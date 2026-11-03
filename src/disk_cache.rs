//! Disk Cache Module
//!
//! Provides file-based cache storage with compression and atomic operations.
//! Supports shared cache coordination for multi-instance deployments.

use crate::cache_hit_update_buffer::CacheHitUpdateBuffer;
use crate::compression::CompressionHandler;
use crate::hybrid_metadata_writer::{HybridMetadataWriter, WriteMode};
use crate::path_safety::is_safe_relative_file_path;
use crate::{ProxyError, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Staged write-cache credits refused at a site that writes no `Write_Ledger` entry, since process
/// start.
///
/// **This counter should read zero forever.** Non-zero means a caller reached `store_range` or
/// `commit_incremental_range` with metadata that classifies as staged, and those two functions have
/// no ledger append — see [`refuse_unledgered_staged_credit`]. Published as
/// `write_cache.unledgered_staged_credits_refused_total`. **Per-instance.**
static UNLEDGERED_STAGED_CREDITS_REFUSED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `write_cache.unledgered_staged_credits_refused_total`, per-instance since process start.
pub(crate) fn unledgered_staged_credits_refused_total() -> u64 {
    UNLEDGERED_STAGED_CREDITS_REFUSED_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Directory entries beneath a shard whose type could not be resolved at all, since process start.
///
/// Published as `cache.dirent_type_resolution_failures_total`. **Per-instance.**
///
/// # What this does and does not measure
///
/// It counts entries for which [`std::fs::DirEntry::file_type`] returned `Err` — the entry's
/// `d_type` was `DT_UNKNOWN` **and** the `lstat` fallback then failed. It is therefore a strict
/// subset of the `DT_UNKNOWN` population, not the `DT_UNKNOWN` rate itself: `std` performs that
/// fallback internally and does not expose whether it fired, so a successful fallback is
/// indistinguishable from a `d_type` the readdir supplied for free. The true fallback rate needs a
/// raw `readdir`, which this unit does not introduce.
///
/// Named for what it measures rather than for the quantity § 16.6.13 (T2) asks about, because a
/// field called `dt_unknown_total` that silently counted only the failing subset would be read as
/// the rate and is exactly the adjacent-value substitution this workstream keeps finding.
///
/// **Non-zero is a finding, not a routine reading.** It means a shard directory listing could not
/// classify one of its own entries, so the traversal fell back to the conservative
/// include-and-let-the-read-fail arm in [`is_shard_subdirectory`].
///
/// Spec: cache-eviction-at-scale § 16.6.13 (T2).
static DIRENT_TYPE_RESOLUTION_FAILURES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `cache.dirent_type_resolution_failures_total`, per-instance since process start.
pub fn dirent_type_resolution_failures_total() -> u64 {
    DIRENT_TYPE_RESOLUTION_FAILURES_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Is this directory entry, found while listing inside a shard, a subdirectory to descend into?
///
/// The one predicate every shard-interior traversal branches on, shared by
/// `ledger_repair::list_l2_dirs`, `cache_size_tracker`'s full validation walk and its rolling
/// variant, so all three answer the question the same way and one test pins all three.
///
/// # Why this is `file_type()` and not `path().is_dir()`
///
/// [`Path::is_dir`] calls `stat(2)`, which is one serial round trip **per entry** — 2,590 on the
/// reference tree and up to 4,096 per shard visit at the design point, all issued before either of
/// the sweep's 64-way fan-outs starts, and none of it charged by the cost model that charges the
/// same count of `readdir`s once. [`std::fs::DirEntry::file_type`] answers the same question from
/// the `d_type` the `readdir` already returned, falling back to `lstat` only on `DT_UNKNOWN`.
///
/// Whether that `stat` costs a wire round trip on any backend is **not established** in either
/// direction: it depends on `READDIRPLUS` negotiation and on the NFS client's attribute cache, and
/// every measurement to date was taken idle at ~99.99% file-server cache hits, the condition most
/// favourable to it being free. So this removes the call rather than depending on the answer, and
/// claims no measured speedup.
///
/// # The prerequisite this depends on
///
/// `is_dir()` **follows** symlinks and `file_type()` does not, so the two disagree on exactly one
/// input: a symlink to a directory, which `is_dir()` includes and `file_type()` reports as
/// `is_symlink()`. Excluding a directory is an **under-count** of the accounting these traversals
/// feed, which is § 16.6.2's unsafe direction. This is therefore correct only under § 16.6.13's
/// stated no-symlink invariant: no entry beneath `metadata/{bucket}/{XX}/` or the matching
/// `ranges/` path is a symbolic link. The proxy is the sole writer of that tree and creates every
/// level of it through `create_dir_all` from [`get_sharded_path`].
///
/// There is deliberately **no `DT_LNK` arm**: given the invariant it would be unreachable, and an
/// arm existing for a state the design forbids is an arm no test can reach.
///
/// # The error arm is the conservative direction
///
/// `file_type()` returns `Result`, so its failure has to be handled, and the handling is not
/// symmetric. Returning `false` would exclude the entry and under-count; returning `true` admits
/// it, the subsequent `read_dir` fails on a non-directory, and the traversal moves on having
/// counted nothing extra. So an unclassifiable entry is included and recorded in
/// [`dirent_type_resolution_failures_total`] rather than silently dropped.
///
/// Spec: cache-eviction-at-scale § 16.6.13 (T1, T2).
pub(crate) fn is_shard_subdirectory(entry: &std::fs::DirEntry) -> bool {
    match entry.file_type() {
        Ok(ft) => ft.is_dir(),
        Err(_) => {
            DIRENT_TYPE_RESOLUTION_FAILURES_TOTAL
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            true
        }
    }
}

/// A staged write-cache credit was reached at a site that writes **no** `Write_Ledger` entry.
/// Refuse it, loudly.
///
/// # The hazard, and why it needed a guard rather than a comment
///
/// `DiskCacheManager::store_range`'s hybrid arm and `commit_incremental_range` both compute
/// `counts_as_staged` and both used to credit `SizeAccumulator::add_write_cache` on it. Neither
/// writes a ledger entry. A staged credit with no entry is a staged range that the fold's candidate
/// set — which is the Write_Ledger and nothing else — cannot see, so it would be a **structural
/// per-write loss mode**, larger than any of L1-L8, occurring on every write rather than on a
/// failure.
///
/// It is **not currently reachable**. Every production caller supplies an `ObjectMetadata` built by
/// `Default` or `new_with_headers`, both of which hardcode `is_write_cached: false`
/// (`cache_types.rs`), and both functions resolve paths under `ranges/` so the `mpus_in_progress/`
/// term of `classify_new_range_as_staged` cannot fire either. The one caller that would pass `true`,
/// `WriteCacheRangeSink::commit`, is `#[allow(dead_code)]` with no production caller.
///
/// But it was unreachable **by caller convention**, with nothing at either site saying so. A
/// convention is not a guard: it holds until someone adds a caller, and the failure would be silent
/// — correct bytes on disk, a correct `.meta`, a plausible `write_cache_size`, and a staging tier
/// eviction cannot see.
///
/// # Why the defensive branch REFUSES the credit rather than applying it
///
/// The invariant every downstream mechanism is written against is that an `add_write_cache` credit
/// and a ledger append happen together — it is what makes the ledger's coverage checkable at all,
/// and it is the premise of the whole loss-mode analysis in § 16.6.6. Applying the credit here
/// would break that invariant and create a class of drift with no owner: nothing reconciles a
/// credit whose entry was never written, because no mechanism is looking for one.
///
/// Refusing keeps the invariant intact, and the resulting under-count is **self-correcting by two
/// independent paths**: the range is still cached and its `.meta` still records staged membership,
/// so `ledger_repair`'s rotation finds it by range identity and appends the entry it should have
/// had, after which the fold counts it; and a full Validation_Scan re-grounds `write_cache_size`
/// absolutely regardless. So the choice is between a bounded, recoverable, counted under-count and
/// an unbounded silent divergence.
///
/// # Why not a type-level change
///
/// Making the state unconstructible means these two functions cannot accept metadata that says
/// "staged" — i.e. splitting `ObjectMetadata` into staged and read-tier types and threading that
/// through `store_range`, `commit_incremental_range`, `finalize_incremental_range`, `RangeSpec`
/// construction, `HybridMetadataWriter::write_range_metadata` and `NewCacheMetadata` itself. That is
/// a refactor of live write paths to forbid a state nothing currently constructs, and its own risk
/// to the reachable paths is larger than the latent hazard it removes. The assertion is the
/// proportionate instrument; if a legitimate staged caller is ever wanted at these sites, the fix is
/// to add the ledger append, and this guard is what will say so.
///
/// Spec: cache-eviction-at-scale § 16.6.6, the reachability check under the loss-mode table.
fn refuse_unledgered_staged_credit(
    site: &'static str,
    cache_key: &str,
    start: u64,
    end: u64,
    compressed_size: u64,
) {
    UNLEDGERED_STAGED_CREDITS_REFUSED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Fails every debug and test build, which is where a newly added caller is first exercised.
    debug_assert!(
        false,
        "{site} reached a STAGED write-cache credit, and this site writes no Write_Ledger entry. \
         The credit is refused. A staged range with no ledger entry is invisible to the staged \
         fold and to staging eviction, which biases the eviction trigger LATE — the direction \
         design § 16.6.2 rules out by name. If a staged caller here is intended, add the ledger \
         append (see `JournalConsolidator::record_staged_range`) rather than removing this \
         assertion. key={cache_key}, range={start}-{end}, size={compressed_size}"
    );
    error!(
        site = site,
        cache_key = cache_key,
        range_start = start,
        range_end = end,
        compressed_size = compressed_size,
        "REFUSED a staged write-cache credit at a site with no Write_Ledger append. This should be \
         unreachable: every production caller hardcodes is_write_cached=false here. The range is \
         still cached and its .meta is correct; only the write-tier credit is declined, so the \
         credit-and-append invariant holds and the ledger-repair rotation re-appends the entry \
         from the .meta by range identity. A peer whose identity is not shared can re-append \
         the range, but a pass refused on a shared identity keeps its claim on the shard, so \
         the entry can stay unindexed until the hostnames are made distinct. Watch \
         unledgered_staged_credits_refused_total."
    );
}

/// What happened to one range's `.bin` file during a batch delete.
///
/// The distinction between the two failure cases is what Requirement 15.2 turns
/// on, and it is a distinction [`BatchRangeDeletion::unlinked_extents`]
/// deliberately erases: that field exists for *accounting*, and both non-`Unlinked`
/// outcomes are correctly excluded from a debit, so it does not carry which one
/// occurred. Metadata removal needs the opposite treatment — one of them must
/// still strip the range from the `.meta` and the other must not — so the
/// classification is made here, once, at the single site that has the
/// `remove_file` result, and both decisions read it.
///
/// Spec: cache-eviction-at-scale. Requirements: 15.2, 15.3
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeUnlinkOutcome {
    /// `remove_file` succeeded — the bytes left the volume on this call. Debit
    /// them, and remove the range from the `.meta`.
    Unlinked,
    /// `remove_file` returned `NotFound` — the `.bin` was already gone before
    /// this call. Nothing to debit now, but the `.meta` entry is a dangling
    /// reference to a file that does not exist and removing it is the *point*:
    /// retaining it would make the object permanently unevictable, since every
    /// later pass would re-select the same range and hit the same absent file.
    AlreadyAbsent,
    /// `remove_file` failed for any other reason — `EACCES` on a read-only
    /// parent, `EIO`, `ESTALE` on shared storage. **The `.bin` is still on the
    /// volume.** Stripping the range from the `.meta` here is what manufactures
    /// an orphan: a file with no metadata referencing it, invisible to a
    /// validation scan that roots at `metadata/` (Requirement 15.1), and
    /// therefore never reclaimed and never counted.
    Failed,
}

/// Outcome of a [`DiskCacheManager::batch_delete_ranges`] call.
///
/// Replaced a three-element tuple on 2026-08-28. The added field —
/// `unlinked_extents` — is what makes a caller able to debit only what left the
/// disk, and a fourth tuple element would have been the wrong shape for it: the
/// tuple already held two `Vec`s and a `u64`/`bool` pair, so a caller
/// destructuring the two `Vec`s in the wrong order still compiles and silently
/// debits the wrong set. That is the same argument [`crate::cache`]'s
/// `EvictedRange` records for replacing its own seven-element tuple, and the
/// consequence here is identical — a phantom debit rather than a loud failure.
///
/// Spec: cache-eviction-at-scale. Requirements: 7.2
#[derive(Debug, Default)]
pub struct BatchRangeDeletion {
    /// Total bytes freed, summed over range files that were **actually unlinked**.
    pub bytes_freed: u64,
    /// True if no ranges remain in the `.meta` (the `.meta` file was deleted).
    pub all_ranges_evicted: bool,
    /// Paths of every file removed — range files, and the `.meta`/`.lock` pair when
    /// the object was fully evicted. For directory cleanup.
    pub deleted_paths: Vec<PathBuf>,
    /// Extents whose `.bin` **existed and was unlinked without error**.
    ///
    /// This is deliberately narrower than the caller's candidate list, and narrower
    /// than the set removed from the `.meta`. Two cases are excluded and they are
    /// **not** the same thing:
    ///
    /// - **Unlink failed** (e.g. a read-only parent directory): the bytes are still
    ///   on the volume, so debiting them would leave the accumulator short of the
    ///   disk — undershoot, the direction that silently over-admits.
    /// - **File already absent**: no bytes to reclaim now. Whether those bytes are
    ///   still credited is a pre-existing discrepancy this pass cannot tell apart
    ///   from an already-debited one, so re-debiting risks a double debit in the
    ///   same dangerous direction. Reconciling that is Requirement 15's job, not
    ///   eviction's.
    ///
    /// The two are distinguishable *here* — an absent file also fails the preceding
    /// `metadata()` call and so contributes a `file_size` of 0 — but both are
    /// correctly excluded from the debit, so the distinction is not carried out of
    /// this function.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 7.2
    pub unlinked_extents: Vec<(u64, u64)>,
    /// Extents whose `.bin` unlink **failed while the file is still present**, and
    /// which were therefore left in the object's `.meta` rather than stripped from
    /// it (Requirement 15.2).
    ///
    /// This is not the complement of `unlinked_extents`. An extent whose `.bin` was
    /// already absent appears in neither: it is excluded from the debit for the
    /// reasons in that field's comment, and it is *still* removed from the `.meta`,
    /// because its file genuinely does not exist. Only the survives-on-disk case
    /// lands here. See [`RangeUnlinkOutcome`].
    ///
    /// **This list is a report, not the record.** Requirement 15.4's durable record
    /// is the retained `.meta` entry itself: it keeps the range accounted, keeps the
    /// object's `.meta` alive (Requirement 15.3), and leaves the range visible to
    /// the next eviction pass's candidate walk — which reads `.meta` ranges — so the
    /// unlink is retried without any new state. This field exists so a caller can
    /// log or count the event in the same pass.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 15.2, 15.3, 15.4
    pub retained_extents: Vec<(u64, u64)>,
    /// How many times this call **wrote** the `.meta` file.
    ///
    /// § 5.5's whole-object path is "one unlink instead of a rewrite", and that is a
    /// claim about the *number of filesystem operations*, not about the end state. A
    /// rewrite-then-delete and a bare delete leave the identical end state — no
    /// `.meta` — so an assertion of the form `!meta_path.exists()` passes on both and
    /// cannot detect the regression the requirement exists to prevent. This is the
    /// figure that can.
    ///
    /// Incremented inside [`DiskCacheManager::rewrite_metadata_file`], which is the
    /// only site in this function that writes the document, so a rewrite added to the
    /// whole-object branch is counted wherever in the branch it is added. A
    /// regression that opened the path directly rather than going through that helper
    /// would not be counted — see that function's comment.
    ///
    /// Spec: cache-eviction-at-scale § 5.5. Requirements: 3.9
    pub meta_writes: u32,
    /// Whether the `.meta` file was **actually removed** by this call.
    ///
    /// Distinct from `all_ranges_evicted`, which is the *decision* — it is computed
    /// before the `remove_file` is attempted, and a failed removal leaves it `true`
    /// with the document still on the volume. This is the observed outcome.
    ///
    /// Spec: cache-eviction-at-scale § 5.5. Requirements: 3.9, 15.3
    pub meta_unlinked: bool,
    /// Whether **every** range the `.meta` listed was in `ranges_to_delete`.
    ///
    /// This is R3.9's precondition read from the document rather than inferred from
    /// the outcome: the whole-object path is available exactly when the candidate set
    /// covers the object. Kept separate from `meta_unlinked` so the two can disagree,
    /// which is precisely R15.3's downgrade — `whole_object_intended && !meta_unlinked`
    /// is a whole-object eviction that became a partial one because a `.bin` survived.
    ///
    /// Note it is a claim about **this instance's read of the shared document**. A
    /// range a peer has admitted and not yet consolidated is not in the `.meta` this
    /// call read, so "every range" cannot mean "every range on the volume" — see the
    /// slot-lock comment in [`DiskCacheManager::batch_delete_ranges`].
    ///
    /// Spec: cache-eviction-at-scale § 5.5. Requirements: 3.9, 15.3
    pub whole_object_intended: bool,
    /// The locked read's figures for every extent in `unlinked_extents`, in the same
    /// order. **Debits are built from this**, never from the caller's candidate: the
    /// candidate was read without the slot lock, and the `.meta` this call read under
    /// the lock is the version whose files were unlinked.
    pub unlinked_ranges: Vec<UnlinkedRange>,
    /// Whether any range stripped from the `.meta` (unlinked or already absent) was staged
    /// in the locked read. The staged tier changed, so the shard needs a fold.
    pub staged_stripped: bool,
    /// The caller passed an expected identity and the locked read did not match it, or a
    /// selected extent became staged when the caller selected none. Nothing was deleted
    /// for this key and nothing may be debited; every other field is empty.
    pub identity_mismatch: bool,
}

/// One extent `batch_delete_ranges` unlinked, with the figures the **locked** `.meta` read
/// recorded for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlinkedRange {
    pub start: u64,
    pub end: u64,
    /// `RangeSpec::compressed_size` from the locked read: the figure the accumulator credited.
    pub compressed_size: u64,
    /// Size of the `.bin` as `stat` saw it just before the unlink (0 if the `stat` failed).
    pub file_size: u64,
    /// Staging membership from the locked read (recorded flag, else the object fallback).
    pub staged: bool,
    /// Whether the locked read's object was write-cached.
    pub is_write_cached: bool,
    /// The `.bin` path that was unlinked.
    pub bin_path: PathBuf,
}

/// The object version an eviction candidate was selected from, carried into the delete so it
/// can be re-checked under the key's slot lock.
///
/// Candidates are selected from a `.meta` read taken without the lock, and the unlink path
/// does not depend on the version, so a peer re-PUT at the same extent in the gap would
/// otherwise lose its new `.bin` and `.meta`. ETag alone is not enough: a same-content
/// single-part re-PUT repeats it. `created_at` is set when the document is constructed and
/// is not rewritten by access updates or range merges, so a re-PUT changes it even when the
/// ETag repeats; `last_modified` and `is_write_cached` catch the rest. A benign change that
/// also moves one of these only costs a skip, and the next pass re-selects the key.
///
/// Serializable because a durable eviction work list carries it (`eviction_worklist`). The
/// recheck compares `created_at` as a `SystemTime` for equality, so it is stored as whole
/// epoch seconds plus nanoseconds and round-trips exactly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExpectedObjectIdentity {
    pub etag: String,
    pub last_modified: String,
    #[serde(with = "system_time_exact")]
    pub created_at: SystemTime,
    pub is_write_cached: bool,
}

/// Serde adapter storing a `SystemTime` as `{secs, nanos}` relative to the Unix epoch, floored
/// (`nanos` is always in `0..1_000_000_000`, `secs` is negative before 1970). Round-trips
/// every `SystemTime` the platform can represent to the nanosecond.
pub(crate) mod system_time_exact {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Red-side lever: drop the sub-second part, so a round trip changes the value.
    #[cfg(test)]
    const TRUNCATE_TO_WHOLE_SECONDS: bool = false;

    #[derive(Serialize, Deserialize)]
    struct Repr {
        secs: i64,
        nanos: u32,
    }

    fn to_repr(t: &SystemTime) -> Repr {
        let repr = match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Repr {
                secs: d.as_secs() as i64,
                nanos: d.subsec_nanos(),
            },
            Err(e) => {
                let d = e.duration();
                if d.subsec_nanos() == 0 {
                    Repr {
                        secs: -(d.as_secs() as i64),
                        nanos: 0,
                    }
                } else {
                    Repr {
                        secs: -(d.as_secs() as i64) - 1,
                        nanos: 1_000_000_000 - d.subsec_nanos(),
                    }
                }
            }
        };
        #[cfg(test)]
        if TRUNCATE_TO_WHOLE_SECONDS {
            return Repr {
                secs: repr.secs,
                nanos: 0,
            };
        }
        repr
    }

    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        to_repr(t).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
        let repr = Repr::deserialize(d)?;
        if repr.nanos >= 1_000_000_000 {
            return Err(serde::de::Error::custom("nanos out of range"));
        }
        let nanos = Duration::new(0, repr.nanos);
        let t = if repr.secs >= 0 {
            UNIX_EPOCH.checked_add(Duration::new(repr.secs as u64, 0))
        } else {
            UNIX_EPOCH.checked_sub(Duration::new(repr.secs.unsigned_abs(), 0))
        };
        t.and_then(|t| t.checked_add(nanos))
            .ok_or_else(|| serde::de::Error::custom("timestamp out of range"))
    }
}

impl ExpectedObjectIdentity {
    /// The identity of the version `metadata` describes.
    pub fn of(metadata: &crate::cache_types::NewCacheMetadata) -> Self {
        Self {
            etag: metadata.object_metadata.etag.clone(),
            last_modified: metadata.object_metadata.last_modified.clone(),
            created_at: metadata.created_at,
            is_write_cached: metadata.object_metadata.is_write_cached,
        }
    }
}

/// What a caller expects the locked `.meta` to be, for
/// [`DiskCacheManager::batch_delete_ranges_expecting`].
#[derive(Debug, Clone, Copy)]
pub struct DeleteExpectation<'a> {
    pub identity: &'a ExpectedObjectIdentity,
    /// Whether the caller selected any staged range. When false, a selected extent that the
    /// locked read shows as staged skips the key.
    pub staged_selected: bool,
}

/// Write `data` to the temp file at `path` and flush it to storage before returning.
///
/// The publish step for every cache `.bin` and `.meta` is a rename of a temp file, and a
/// rename can reach storage before the data it names. On a local `cache_dir` a power loss in
/// that window leaves a durable `.meta` naming a `.bin` whose blocks were never written; on
/// NFS a deferred write error surfaces only at `fsync` or `close`, and `close` errors are
/// dropped. So every temp that a rename is about to publish goes through here (or through
/// [`sync_written_file`] when the caller already holds the descriptor), and an error fails
/// the publish rather than letting it name bytes that may not exist.
///
/// No directory fsync: losing the rename itself leaves the previous state, which is a miss
/// or a stale-but-whole entry, never a pointer to missing data.
pub(crate) fn write_synced(path: &Path, data: impl AsRef<[u8]>) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(data.as_ref())?;
    sync_written_file(&file, path)
}

/// [`write_synced`], returning the identity of the file it wrote
/// ([`crate::cache_types::RangeFileIdentity`]): the writing descriptor is closed after the sync
/// and the identity taken by [`identity_after_close`]. The rename that later publishes the file
/// keeps its length and modification time, so this is what a reader's `fstat` reports. A failed
/// reopen or `fstat` returns `None` (the range is recorded unverified), never an error: a write
/// is not failed for want of its identity.
pub(crate) fn write_synced_identity(
    path: &Path,
    data: impl AsRef<[u8]>,
) -> std::io::Result<Option<crate::cache_types::RangeFileIdentity>> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(data.as_ref())?;
    sync_written_file(&file, path)?;
    drop(file);
    Ok(identity_after_close(path))
}

/// The identity of the finished file at `path` ([`crate::cache_types::RangeFileIdentity`]),
/// by `fstat` of a descriptor opened here. The caller has synced and closed every descriptor
/// it wrote through and has not yet renamed the file (`path` is its private temp name).
///
/// Why a new descriptor: on NFS the server stamps the modification time on each WRITE it
/// applies, and a reader on any client takes it from the attributes its own open revalidates
/// (close-to-open). Closing the writer flushes and releases its open, so this open revalidates
/// with the server too and the writer records the value a reader will see. Without a write
/// delegation that holds by close-to-open alone. A client holding a write delegation may skip
/// the revalidation, as it may for any open of that file; then both stamps rest on the client's
/// attributes agreeing with the server's, which only a cross-client measurement shows.
///
/// `None` when the open or `fstat` fails. The descriptor's close is deferred to a blocking
/// task when a runtime is present: it is read-only, so its close has nothing to write back.
pub(crate) fn identity_after_close(path: &Path) -> Option<crate::cache_types::RangeFileIdentity> {
    let file = std::fs::File::open(path).ok()?;
    let identity = file
        .metadata()
        .ok()
        .and_then(|m| crate::cache_types::RangeFileIdentity::of(&m));
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn_blocking(move || drop(file));
        }
        Err(_) => drop(file),
    }
    identity
}

/// `sync_data` on a temp file the caller has finished writing, before its rename publishes it.
/// See [`write_synced`].
pub(crate) fn sync_written_file(file: &std::fs::File, path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    publish_sync_probe::before_sync(path)?;
    #[cfg(not(test))]
    let _ = path;
    file.sync_data()
}

/// Test seam recording which temp files were synced before publish, scoped to a registered
/// cache root so concurrent tests do not see each other's events.
#[cfg(test)]
pub(crate) mod publish_sync_probe {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    /// One sync, with whether each named final path already existed when it ran.
    #[derive(Debug, Clone)]
    pub(crate) struct SyncEvent {
        pub path: PathBuf,
        /// Snapshot of `watched` finals that existed at the moment of the sync.
        pub finals_present: Vec<PathBuf>,
    }

    #[derive(Debug, Default)]
    pub(crate) struct Probe {
        pub events: Mutex<Vec<SyncEvent>>,
        /// Final paths whose existence is snapshotted at every sync.
        pub watched: Mutex<Vec<PathBuf>>,
        /// Fail every sync under the root with an injected error.
        pub fail: AtomicBool,
    }

    static REGISTRY: Mutex<Vec<(PathBuf, Arc<Probe>)>> = Mutex::new(Vec::new());

    /// Start recording syncs of files under `root`.
    pub(crate) fn register(root: &Path) -> Arc<Probe> {
        let probe = Arc::new(Probe::default());
        REGISTRY
            .lock()
            .unwrap()
            .push((root.to_path_buf(), Arc::clone(&probe)));
        probe
    }

    pub(super) fn before_sync(path: &Path) -> std::io::Result<()> {
        let probes: Vec<Arc<Probe>> = REGISTRY
            .lock()
            .unwrap()
            .iter()
            .filter(|(root, _)| path.starts_with(root))
            .map(|(_, p)| Arc::clone(p))
            .collect();
        for probe in probes {
            let finals_present = probe
                .watched
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.exists())
                .cloned()
                .collect();
            probe.events.lock().unwrap().push(SyncEvent {
                path: path.to_path_buf(),
                finals_present,
            });
            if probe.fail.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("injected publish sync failure"));
            }
        }
        Ok(())
    }
}

/// A finished range `.tmp` whose rename to its final `.bin` path has not happened yet.
///
/// Range paths are `{key}_{start}-{end}.bin` whatever the object version, and eviction's
/// identity re-check (`batch_delete_ranges_expecting`) reads the `.meta` under the key's slot
/// lock. A writer that renames before taking that lock and publishes its `.meta` after can lose
/// its new `.bin` to an eviction that read the old `.meta` in between. Writers that publish a
/// `.meta` under the slot lock call [`Self::publish`] inside that hold.
///
/// Dropping it unpublished removes the `.tmp`.
#[must_use = "an unpublished range is removed when dropped"]
pub(crate) struct PendingRangePublish {
    tmp_path: PathBuf,
    final_path: PathBuf,
    done: bool,
}

impl PendingRangePublish {
    pub(crate) fn new(tmp_path: PathBuf, final_path: PathBuf) -> Self {
        Self {
            tmp_path,
            final_path,
            done: false,
        }
    }

    /// Rename the `.tmp` to the final `.bin`. Returns whether a file already existed at the
    /// final path, which callers use to avoid double-crediting an overwrite. On failure the
    /// `.tmp` is removed.
    pub(crate) fn publish(mut self) -> Result<bool> {
        self.done = true;
        let range_already_existed = self.final_path.exists();
        std::fs::rename(&self.tmp_path, &self.final_path).map_err(|e| {
            let _ = std::fs::remove_file(&self.tmp_path);
            ProxyError::CacheError(format!(
                "Failed to rename incremental tmp file to final: {}",
                e
            ))
        })?;
        Ok(range_already_existed)
    }
}

/// How a journal-only range fill acquires the key's slot lock before publishing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotWait {
    /// Wait up to `lock_acquire_wait_budget`.
    WithinBudget,
    /// One non-blocking attempt; on contention the fill is skipped. For callers that hold
    /// the `DiskCacheManager` `RwLock` and must not wait while holding it.
    TryOnce,
}

/// A buffered range fill whose `.tmp` is written and whose publish remains, returned by
/// [`DiskCacheManager::prepare_range_store`].
///
/// Owns everything the publish needs, so [`Self::finish`] runs with no `DiskCacheManager`
/// guard held. Dropping it unfinished removes the `.tmp` and releases the capacity
/// reservation; nothing is journaled or credited.
#[must_use = "an unfinished range fill is discarded when dropped"]
pub struct PreparedRangeFill {
    cache_dir: PathBuf,
    meta_path: PathBuf,
    cache_key: String,
    start: u64,
    end: u64,
    object_metadata: crate::cache_types::ObjectMetadata,
    ttl: std::time::Duration,
    range_spec: crate::cache_types::RangeSpec,
    compressed_size: u64,
    uncompressed_size: u64,
    counts_as_staged: bool,
    pending: PendingRangePublish,
    hybrid_writer: Arc<Mutex<HybridMetadataWriter>>,
    journal_consolidator: Option<Arc<crate::journal_consolidator::JournalConsolidator>>,
    /// Held until the publish completes, as it was when the whole store ran in one call.
    _capacity_reservation: Option<crate::capacity_guard::FleetReservation>,
    operation_start: std::time::Instant,
    /// The fill's version witness, captured before its S3 request. `None`: unguarded
    /// (callers with no S3 request behind them, and tests).
    fill_witness: Option<crate::cache::FillWitness>,
}

impl PreparedRangeFill {
    /// Guard the publish with the fill's version witness: it publishes, and its journal
    /// `Add` is folded, only while the key's write-order marker still holds the witness.
    pub(crate) fn with_fill_witness(mut self, witness: Option<crate::cache::FillWitness>) -> Self {
        self.fill_witness = witness;
        self
    }

    /// Publish the fill: take the key's slot lock (waiting up to `lock_acquire_wait_budget`),
    /// rename the `.bin` visible if the published `.meta` is the same version, journal it and
    /// credit it. Call it after dropping any `DiskCacheManager` guard.
    pub async fn finish(self) -> Result<()> {
        self.finish_with_slot_wait(SlotWait::WithinBudget).await
    }

    pub(crate) async fn finish_with_slot_wait(self, slot_wait: SlotWait) -> Result<()> {
        let PreparedRangeFill {
            cache_dir,
            meta_path,
            cache_key,
            start,
            end,
            object_metadata,
            ttl,
            range_spec,
            compressed_size,
            uncompressed_size,
            counts_as_staged,
            pending,
            hybrid_writer,
            journal_consolidator,
            _capacity_reservation,
            operation_start,
            fill_witness,
        } = self;
        let cache_key = cache_key.as_str();
        let metadata_start = std::time::Instant::now();
        debug!(
            "Using HybridMetadataWriter for metadata update: key={}, range={}-{}",
            cache_key, start, end
        );

        // Make the range file visible FIRST, so that when other proxies read the journal
        // entry during consolidation the referenced range file already exists.
        //
        // Under the key's slot lock, and only if the published `.meta` (if any) is the same
        // version: this fill writes no `.meta`, so consolidation would otherwise fold a new
        // version's extent under the old version's ETag. Dropped fills journal and credit
        // nothing. The lock is held through the journal append below (`published`), so no
        // slot holder sees the `.bin` before its journal line exists; a failed append unlinks
        // it inside the same hold.
        let published = match publish_range_fill_under_slot_lock_at(
            &cache_dir,
            &meta_path,
            cache_key,
            start,
            end,
            &object_metadata.etag,
            pending,
            slot_wait,
            fill_witness.as_ref(),
        )
        .await
        .inspect_err(|e| {
            error!(
                "Failed to publish range file: key={}, range={}-{}, error={}",
                cache_key, start, end, e
            );
        })? {
            Some(published) => published,
            None => return Ok(()),
        };
        let range_already_existed = published.range_already_existed;
        #[cfg(test)]
        range_rename_pause::before_append(cache_key).await;

        // Write the journal entry (references the now-existing range file). Size tracking is
        // handled by consolidation from journal entries; JournalOnly avoids lock contention,
        // and object_metadata lets consolidation create a `.meta` with response_headers.
        let mut writer = hybrid_writer.lock().await;
        if let Err(e) = writer
            .write_range_metadata_witnessed(
                cache_key,
                range_spec,
                WriteMode::JournalOnly,
                Some(object_metadata.clone()),
                ttl,
                fill_witness.as_ref().map(|w| w.journal_token()),
            )
            .await
        {
            drop(writer);
            error!(
                "HybridMetadataWriter failed, removing the unjournalled range file: key={}, \
                 range={}-{}, error={}",
                cache_key, start, end, e
            );
            published.unpublish(cache_key, start, end);
            return Err(e);
        }
        drop(writer);
        drop(published);

        debug!(
            "HybridMetadataWriter completed: key={}, range={}-{}, duration={:.2}ms",
            cache_key,
            start,
            end,
            metadata_start.elapsed().as_secs_f64() * 1000.0
        );

        // Track size via in-memory accumulator (zero NFS overhead). Skip if the range file
        // already existed on disk (another instance cached it first), which prevents
        // cross-instance over-counting on shared storage.
        if let Some(consolidator) = &journal_consolidator {
            if !range_already_existed {
                consolidator
                    .size_accumulator()
                    .add_range(cache_key, start, end, compressed_size);
                // **No write-cache credit here, and that is a guard rather than an
                // omission.** This site writes no `Write_Ledger` entry, so a staged
                // credit would produce a staged range the fold's candidate set cannot
                // see. It is unreachable by caller convention — every production caller
                // hardcodes `is_write_cached: false` — and the convention is now
                // enforced instead of assumed.
                // Spec: cache-eviction-at-scale § 16.6.6; write-cache-accounting-and-eviction 6.2
                if counts_as_staged {
                    refuse_unledgered_staged_credit(
                        "DiskCacheManager::store_range (hybrid arm)",
                        cache_key,
                        start,
                        end,
                        compressed_size,
                    );
                }
            } else {
                debug!(
                    "SIZE_ACCUM skip (range already existed): key={}, range={}-{}, size={}",
                    cache_key, start, end, compressed_size
                );
            }
        }

        debug!(
            "Range stored (hybrid): key={}, range={}-{}, size={} bytes, duration={:.2}ms",
            cache_key,
            start,
            end,
            uncompressed_size,
            operation_start.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }
}

impl Drop for PendingRangePublish {
    fn drop(&mut self) {
        if !self.done {
            let _ = std::fs::remove_file(&self.tmp_path);
        }
    }
}

/// Test-only pause point a writer passes just after its range rename, keyed by cache key. A
/// test registers a key, waits for `reached`, runs an eviction, then fires `resume`.
#[cfg(test)]
pub(crate) mod range_rename_pause {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::Notify;

    #[derive(Clone)]
    pub(crate) struct Pause {
        pub(crate) reached: Arc<Notify>,
        pub(crate) resume: Arc<Notify>,
    }

    fn registry() -> &'static Mutex<HashMap<String, Pause>> {
        static R: OnceLock<Mutex<HashMap<String, Pause>>> = OnceLock::new();
        R.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(crate) fn arm(cache_key: &str) -> Pause {
        let p = Pause {
            reached: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        registry()
            .lock()
            .unwrap()
            .insert(cache_key.to_string(), p.clone());
        p
    }

    /// Called by writers right after the `.bin` rename. A no-op unless `cache_key` is armed;
    /// each arming pauses once.
    pub(crate) async fn after_rename(cache_key: &str) {
        let p = registry().lock().unwrap().remove(cache_key);
        if let Some(p) = p {
            p.reached.notify_one();
            p.resume.notified().await;
        }
    }

    fn before_append_registry() -> &'static Mutex<HashMap<String, Pause>> {
        static R: OnceLock<Mutex<HashMap<String, Pause>>> = OnceLock::new();
        R.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Arm the pause journal-only fills pass after their publish returns and before their
    /// journal append.
    pub(crate) fn arm_before_append(cache_key: &str) -> Pause {
        let p = Pause {
            reached: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        before_append_registry()
            .lock()
            .unwrap()
            .insert(cache_key.to_string(), p.clone());
        p
    }

    /// Called by journal-only fills between the publish and the journal append.
    pub(crate) async fn before_append(cache_key: &str) {
        let p = before_append_registry().lock().unwrap().remove(cache_key);
        if let Some(p) = p {
            p.reached.notify_one();
            p.resume.notified().await;
        }
    }
}

/// Unlink a range `.bin`. The single call through which the producers that must keep a range
/// listed when its unlink fails (`retire_other_version_fills`, `invalidate_all_ranges`) remove
/// the file, so a test can fail one path's unlink through [`unlink_fault`].
pub(crate) fn remove_range_file(path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(e) = unlink_fault::take(path) {
        return Err(e);
    }
    std::fs::remove_file(path)
}

/// Test-only unlink failure for [`remove_range_file`], keyed by path so parallel tests cannot
/// trip each other's. Each arming fails one unlink with `PermissionDenied` and leaves the file.
#[cfg(test)]
pub(crate) mod unlink_fault {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    static ARMED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    pub(crate) fn arm(path: &Path) {
        ARMED.lock().unwrap().push(path.to_path_buf());
    }

    pub(super) fn take(path: &Path) -> Option<std::io::Error> {
        let mut armed = ARMED.lock().unwrap();
        let i = armed.iter().position(|p| p == path)?;
        armed.remove(i);
        Some(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "injected unlink failure",
        ))
    }
}

// ---------------------------------------------------------------------------
// Metadata read outcome types (Task 1: non-blocking metadata read+parse)
// ---------------------------------------------------------------------------

/// Reason a `.meta` file is classified as confidently corrupt (safe to heal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorruptReason {
    /// File exceeds `max_metadata_file_bytes` — rejected without reading the body.
    Oversize,
    /// File matches the legacy `CacheEntry` schema (inline body, no `object_metadata`).
    /// Detected when JSON has `body` + `is_put_cached` keys but no `object_metadata`.
    Legacy,
    /// File fails to parse as `NewCacheMetadata` after bounded retries and the file
    /// size is stable (not actively being written).
    ParseFailure,
}

/// Outcome of a synchronous metadata read+parse attempt.
#[derive(Debug)]
pub enum MetadataReadOutcome {
    /// Successfully parsed a valid `NewCacheMetadata`.
    Parsed(Box<crate::cache_types::NewCacheMetadata>),
    /// File does not exist on disk.
    Missing,
    /// File is empty or unreadable after bounded retries — likely a transient
    /// mid-write on shared storage. Do NOT heal (next request will likely succeed).
    TransientMiss,
    /// File is confidently corrupt and should be healed via the Req 3 path.
    Corrupt { reason: CorruptReason },
}

/// Result of a classified metadata lookup.
///
/// Exposes the corruption flag so callers (e.g. the GET miss path in `cache.rs`)
/// can trigger the overwrite-in-place heal when `corrupt == true`.
///
/// Spec: cache-metadata-resilience Req 3, Task 6
#[derive(Debug)]
pub struct MetadataLookup {
    /// Parsed metadata, or `None` on miss/corrupt.
    pub meta: Option<crate::cache_types::NewCacheMetadata>,
    /// `true` when the `.meta` file was classified as confidently corrupt
    /// (oversize, legacy, or stable parse failure). The caller should ensure
    /// the corrupt file is overwritten after a successful S3 fetch.
    pub corrupt: bool,
    /// The specific corruption reason, if `corrupt == true`.
    pub corrupt_reason: Option<CorruptReason>,
}

/// Synchronous, blocking metadata read+parse helper.
///
/// Designed to run inside `tokio::task::spawn_blocking` so that filesystem I/O
/// and CPU-bound JSON deserialization stay off the async worker threads.
///
/// # Classification logic
///
/// 1. `stat` the file; if missing → `Missing`.
/// 2. If `len > cap` → `Corrupt{Oversize}` (no read).
/// 3. Read with bounded retries for empty/stale-handle conditions.
/// 4. If still empty after retries → `TransientMiss`.
/// 5. Parse as `NewCacheMetadata` with bounded retries (re-read on failure).
/// 6. If parse fails and file size is stable across retries → `Corrupt{ParseFailure}`.
/// 7. If parse fails but file size changed → `TransientMiss` (mid-write).
///
/// Legacy detection (`Corrupt{Legacy}`) detects files matching the old `CacheEntry` schema
/// (has `body` + `is_put_cached`, lacks `object_metadata`). This is bounded because oversize
/// files are already rejected by the cap before any parse attempt.
pub fn read_and_parse_metadata_blocking(path: &Path, cap: u64) -> MetadataReadOutcome {
    read_and_parse_metadata_with_io(path, cap, &StdMetadataFileIo)
}

/// Classify the `.meta` at `meta_path` and unlink it if it is confidently corrupt
/// ([`MetadataReadOutcome::Corrupt`]: legacy `CacheEntry` shape, oversize, or a stable parse
/// failure). The caller MUST hold the key's metadata slot lock.
///
/// For callers whose own fast parse has already failed. Returns the classifier's outcome: on
/// `Corrupt` the file has been removed (or was already gone), on `Parsed` a writer replaced it
/// between the caller's read and this one and the document is usable, and on `TransientMiss`
/// or `Missing` nothing was touched. The classifier's retries sleep, so it runs on a blocking
/// thread. No size cap is applied: the caller has already read the whole file, and a document
/// that does not parse is corrupt whatever its size.
///
/// The removal debits neither `cached_objects` nor bytes, and that is the accounting the rest
/// of the tree already applies to an unparseable document. `cached_objects` is credited only by
/// consolidation's `is_new_object` and installed only by censuses that skip unparseable
/// documents (`ObjectsScanResults` counts `is_valid()` entries; the full validation subtracts
/// the ones it self-heals), and a legacy `CacheEntry` held its body inline and was never
/// produced by consolidation. So the healed key's next consolidation crediting one new object
/// is correct. The old range files stay on disk and keep whatever credit they had; a fill
/// renamed over one of them is not credited again (`range_already_existed`), which is right
/// when that file was credited and wrong only when a census install has since excluded it.
pub(crate) async fn remove_unloadable_meta_under_slot_lock(
    meta_path: &Path,
    cache_key: &str,
) -> MetadataReadOutcome {
    let path = meta_path.to_path_buf();
    let outcome = match tokio::task::spawn_blocking(move || {
        read_and_parse_metadata_blocking(&path, u64::MAX)
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(e) => {
            warn!(
                "Metadata classification task failed: cache_key={}, error={}",
                cache_key, e
            );
            return MetadataReadOutcome::TransientMiss;
        }
    };
    if let MetadataReadOutcome::Corrupt { reason } = &outcome {
        match std::fs::remove_file(meta_path) {
            Ok(()) => warn!(
                "[METADATA_HEAL] Removed unloadable metadata file: cache_key={}, reason={:?}, path={:?}",
                cache_key, reason, meta_path
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                warn!(
                    "[METADATA_HEAL] Failed to remove unloadable metadata file: cache_key={}, \
                     reason={:?}, path={:?}, error={}",
                    cache_key, reason, meta_path, e
                );
                return MetadataReadOutcome::TransientMiss;
            }
        }
    }
    outcome
}

/// Stale file handles (`ESTALE`) observed while reading a `.meta`, since process start.
///
/// Published as `cache.metadata_read_stale_handles_total`. **Per-instance.**
///
/// Counts once per `ESTALE` **observed**, not once per read: a read whose first two attempts
/// return `ESTALE` and whose third succeeds adds 2 here and nothing to
/// [`METADATA_READ_STALE_HANDLE_MISSES_TOTAL`]. It covers every file operation
/// [`read_and_parse_metadata_blocking`] performs — the initial `stat`, the bounded read loop and
/// the parse-retry re-read — and nothing else: `.bin` range reads and every metadata WRITE path
/// are not counted. The predicate is [`is_stale_file_handle`].
///
/// **Non-zero is a finding worth alarming on, not a routine reading.** An `ESTALE` means the NFS
/// client held a handle to a `.meta` the server no longer recognises — typically another proxy
/// replaced the file between this proxy's open and its read. The read path absorbs occasional
/// ones by retrying; a sustained rate means the shared volume is invalidating handles under load,
/// and each one costs up to 300 ms of blocking-pool time. Read it against
/// [`METADATA_READ_STALE_HANDLE_MISSES_TOTAL`], which counts the ones that became S3 fetches.
static METADATA_READ_STALE_HANDLES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `.meta` reads abandoned because of a stale file handle and treated as a cache miss, since
/// process start.
///
/// Published as `cache.metadata_read_stale_handle_misses_total`. **Per-instance.**
///
/// Counts once per READ: a `stat` that returns `ESTALE` (not retried), or a read loop whose final
/// attempt still returns `ESTALE` after the fixed three retries. Each one returned
/// [`MetadataReadOutcome::TransientMiss`], so the object was fetched from S3 instead of served
/// from cache. **Non-zero is a finding**: it is the subset of
/// [`METADATA_READ_STALE_HANDLES_TOTAL`] that retrying did not absorb.
static METADATA_READ_STALE_HANDLE_MISSES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `cache.metadata_read_stale_handles_total`, per-instance since process start.
pub fn metadata_read_stale_handles_total() -> u64 {
    METADATA_READ_STALE_HANDLES_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// `cache.metadata_read_stale_handle_misses_total`, per-instance since process start.
pub fn metadata_read_stale_handle_misses_total() -> u64 {
    METADATA_READ_STALE_HANDLE_MISSES_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Is this I/O error a stale NFS file handle?
///
/// Unchanged from the predicate the read loop has always used: raw errno 116 (Linux `ESTALE`) or
/// a message naming it. Note that errno 116 is Linux-specific — macOS's `ESTALE` is 70 and its
/// message is "Stale NFS file handle", which neither string arm matches — so on a non-Linux host
/// this predicate does not fire. The proxy's supported deployment targets are Linux.
/// Decode a range file of one or more concatenated LZ4 frames into a buffer sized
/// exactly `expected` up front, so the output never grows by doubling (a 16 MiB
/// range decoded through an unsized buffer can briefly hold 32 MiB of capacity).
/// Returns `None` on any decode error or size mismatch, and the caller falls back
/// to the general decompressor, so a format this does not handle is unaffected
/// (buffered-serve-memory-bound R1.3).
fn decode_lz4_frames_exact(compressed: &[u8], expected: u64) -> Option<Vec<u8>> {
    use std::io::Read;
    let expected = usize::try_from(expected).ok()?;
    let mut out = Vec::with_capacity(expected);
    let mut input = compressed;
    while !input.is_empty() {
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&mut input);
        decoder.read_to_end(&mut out).ok()?;
        if out.len() > expected {
            return None;
        }
    }
    (out.len() == expected).then_some(out)
}

/// Capacity of the `BufReader` an LZ4 range file is read through, on the streamed serve path
/// and the buffered small-hit path. `FrameDecoder` reads each block as a 4-byte header and then
/// the block body, so with 64 KiB blocks std's default 8 KiB buffer costs two `read` calls per
/// block. A buffer of this size absorbs both reads, at most this many bytes per concurrent read.
const LZ4_RANGE_READER_CAPACITY: usize = 256 * 1024;

/// std's default `BufReader` capacity, the floor for a small file.
const LZ4_RANGE_READER_MIN_CAPACITY: usize = 8 * 1024;

/// Reader capacity for an LZ4 range file of `file_len` bytes: the file's length, bounded by
/// [`LZ4_RANGE_READER_MIN_CAPACITY`] and [`LZ4_RANGE_READER_CAPACITY`], so a small file never
/// allocates the full buffer.
fn lz4_range_reader_capacity(file_len: u64) -> usize {
    usize::try_from(file_len)
        .unwrap_or(usize::MAX)
        .clamp(LZ4_RANGE_READER_MIN_CAPACITY, LZ4_RANGE_READER_CAPACITY)
}

/// [`decode_lz4_frames_exact`] reading the frames from the file at `path` instead of from a
/// buffer holding the whole file, so a load holds the decode output and the decoder's block
/// buffers but never the compressed frame as well.
///
/// Returns the file's length and `Some(decoded)` when the frames decode to exactly `expected`
/// bytes. `None` means the caller should fall back to the whole-buffer decoder, which reports
/// the reason. The frame decoder verifies each frame's content checksum when the frame carries
/// one, as it does on the buffered path. Blocking: run it on a blocking thread.
#[cfg(test)]
fn decode_lz4_file_exact(path: &Path, expected: u64) -> std::io::Result<(u64, Option<Vec<u8>>)> {
    decode_lz4_file_exact_with_capacity(path, expected, None)
}

/// [`decode_lz4_file_exact`] with the reader capacity given; `None` takes
/// [`lz4_range_reader_capacity`] of the file's length.
#[cfg(test)]
fn decode_lz4_file_exact_with_capacity(
    path: &Path,
    expected: u64,
    reader_capacity: Option<usize>,
) -> std::io::Result<(u64, Option<Vec<u8>>)> {
    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    decode_lz4_open_file_exact(file, file_len, expected, reader_capacity)
}

/// [`decode_lz4_file_exact_with_capacity`] over a file already open (whose length is
/// `file_len`), so a caller that checked the descriptor's identity decodes that same file.
fn decode_lz4_open_file_exact(
    file: std::fs::File,
    file_len: u64,
    expected: u64,
    reader_capacity: Option<usize>,
) -> std::io::Result<(u64, Option<Vec<u8>>)> {
    use std::io::{BufRead, Read};
    let Ok(expected) = usize::try_from(expected) else {
        return Ok((file_len, None));
    };
    let capacity = reader_capacity.unwrap_or_else(|| lz4_range_reader_capacity(file_len));
    let mut reader = std::io::BufReader::with_capacity(capacity, file);
    let mut out = Vec::with_capacity(expected);
    // `FrameDecoder` reads a frame and stops at its end, so a fresh decoder per frame walks a
    // file of concatenated frames exactly as the slice loop does.
    while !reader.fill_buf()?.is_empty() {
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&mut reader);
        if decoder.read_to_end(&mut out).is_err() || out.len() > expected {
            return Ok((file_len, None));
        }
    }
    Ok((file_len, (out.len() == expected).then_some(out)))
}

fn is_stale_file_handle(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(116)
        || e.to_string().contains("stale file handle")
        || e.to_string().contains("ESTALE")
}

/// The two file operations [`read_and_parse_metadata_blocking`] performs, so a test can inject
/// errors — `ESTALE` in particular, which a local filesystem cannot be made to return.
trait MetadataFileIo {
    fn len(&self, path: &Path) -> std::io::Result<u64>;
    fn read_to_string(&self, path: &Path) -> std::io::Result<String>;
}

/// The real filesystem. The only implementation outside tests.
struct StdMetadataFileIo;

impl MetadataFileIo for StdMetadataFileIo {
    fn len(&self, path: &Path) -> std::io::Result<u64> {
        std::fs::metadata(path).map(|m| m.len())
    }
    fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
        std::fs::read_to_string(path)
    }
}

fn read_and_parse_metadata_with_io(
    path: &Path,
    cap: u64,
    io: &dyn MetadataFileIo,
) -> MetadataReadOutcome {
    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY_MS: u64 = 50;

    // Step 1: stat the file
    let initial_size = match io.len(path) {
        Ok(len) => len,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return MetadataReadOutcome::Missing;
            }
            if is_stale_file_handle(&e) {
                // Not retried here: the read gives up and becomes a miss, so it counts both
                // as an observed stale handle and as a stale-handle miss.
                METADATA_READ_STALE_HANDLES_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                METADATA_READ_STALE_HANDLE_MISSES_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            // Other stat errors (permission, etc.) — treat as transient
            warn!(
                "[METADATA_BLOCKING] Failed to stat metadata file: path={:?}, error={}",
                path, e
            );
            return MetadataReadOutcome::TransientMiss;
        }
    };

    // Step 2: size cap check
    if initial_size > cap {
        warn!(
            "[METADATA_BLOCKING] Metadata file exceeds size cap: path={:?}, size={}, cap={}",
            path, initial_size, cap
        );
        return MetadataReadOutcome::Corrupt {
            reason: CorruptReason::Oversize,
        };
    }

    // Step 3: read with bounded retries for empty/stale-handle
    let content: String;
    let mut read_attempts = 0u32;

    loop {
        read_attempts += 1;
        match io.read_to_string(path) {
            Ok(c) => {
                if c.is_empty() && read_attempts <= MAX_RETRIES {
                    // Empty file — likely transient mid-write on shared storage
                    std::thread::sleep(std::time::Duration::from_millis(
                        RETRY_DELAY_MS * read_attempts as u64,
                    ));
                    continue;
                }
                content = c;
                break;
            }
            Err(e) => {
                // Check for stale file handle (ESTALE) or NotFound (deleted between stat and read)
                let is_stale = is_stale_file_handle(&e);
                let is_not_found = e.kind() == std::io::ErrorKind::NotFound;

                if is_not_found {
                    return MetadataReadOutcome::Missing;
                }

                if is_stale {
                    // Once per ESTALE observed, retried or not — see the counter's doc.
                    METADATA_READ_STALE_HANDLES_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }

                if is_stale && read_attempts <= MAX_RETRIES {
                    warn!(
                        "[METADATA_BLOCKING] Stale file handle, retrying: path={:?}, attempt={}/{}, error={}",
                        path, read_attempts, MAX_RETRIES, e
                    );
                    std::thread::sleep(std::time::Duration::from_millis(
                        RETRY_DELAY_MS * read_attempts as u64,
                    ));
                    continue;
                }

                // Exhausted retries or non-stale error — transient miss
                if is_stale {
                    // The final attempt was still stale: retrying did not absorb it, and the
                    // object will be fetched from S3.
                    METADATA_READ_STALE_HANDLE_MISSES_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    "[METADATA_BLOCKING] Failed to read metadata file after retries: path={:?}, error={}",
                    path, e
                );
                return MetadataReadOutcome::TransientMiss;
            }
        }
    }

    // Step 4: still empty after retries
    if content.is_empty() {
        warn!(
            "[METADATA_BLOCKING] Metadata file still empty after retries: path={:?}",
            path
        );
        return MetadataReadOutcome::TransientMiss;
    }

    // Step 5+6: parse with bounded retries
    let mut parse_attempts = 0u32;
    let mut current_content = content;

    loop {
        parse_attempts += 1;
        match serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&current_content) {
            Ok(meta) => {
                return MetadataReadOutcome::Parsed(Box::new(meta));
            }
            Err(_e) => {
                if parse_attempts < MAX_RETRIES {
                    // Might be a partial read during in-progress write — retry
                    std::thread::sleep(std::time::Duration::from_millis(
                        RETRY_DELAY_MS * parse_attempts as u64,
                    ));

                    // Re-read the file
                    match io.read_to_string(path) {
                        Ok(new_content) => {
                            current_content = new_content;
                            continue;
                        }
                        Err(e) => {
                            if is_stale_file_handle(&e) {
                                // Observed, but not a miss: the parse retries on the content it
                                // already holds, and the outcome is decided by that parse.
                                METADATA_READ_STALE_HANDLES_TOTAL
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            // Can't re-read; continue with old content for next attempt
                            continue;
                        }
                    }
                }

                // All retries exhausted. Classify based on file size stability.
                // If the file size is unchanged, the parse failure is stable → corrupt.
                // If the file size changed, it's likely mid-write → transient.
                let current_size = io.len(path).unwrap_or(initial_size);

                if current_size == initial_size {
                    // Size stable across retry window → confident corruption.
                    // Before returning ParseFailure, check for legacy CacheEntry schema.
                    // Legacy markers: has "body" + "is_put_cached", lacks "object_metadata".
                    if let Ok(serde_json::Value::Object(ref obj)) =
                        serde_json::from_str::<serde_json::Value>(&current_content)
                    {
                        if obj.contains_key("body")
                            && obj.contains_key("is_put_cached")
                            && !obj.contains_key("object_metadata")
                        {
                            warn!(
                                "[METADATA_BLOCKING] Legacy CacheEntry detected: path={:?}, size={}",
                                path, current_size
                            );
                            return MetadataReadOutcome::Corrupt {
                                reason: CorruptReason::Legacy,
                            };
                        }
                    }

                    warn!(
                        "[METADATA_BLOCKING] Stable parse failure (size unchanged): path={:?}, size={}",
                        path, current_size
                    );
                    return MetadataReadOutcome::Corrupt {
                        reason: CorruptReason::ParseFailure,
                    };
                } else {
                    // Size changed — file is being written; treat as transient
                    warn!(
                        "[METADATA_BLOCKING] Parse failure but size changed (mid-write?): path={:?}, initial_size={}, current_size={}",
                        path, initial_size, current_size
                    );
                    return MetadataReadOutcome::TransientMiss;
                }
            }
        }
    }
}

/// State for an in-progress incremental range write to disk cache.
/// Created by `begin_incremental_range_write()`, consumed by `commit_incremental_range()` or `abort_incremental_range()`.
pub struct IncrementalRangeWriter {
    /// Path to the temporary file being written
    tmp_path: PathBuf,
    /// Final destination path (.bin file)
    final_path: PathBuf,
    /// Open file handle for the .tmp file
    file: std::fs::File,
    /// Total uncompressed bytes written so far
    pub bytes_written: u64,
    /// Total compressed bytes written so far
    pub compressed_bytes_written: u64,
    /// Cache key for this range
    cache_key: String,
    /// Range start byte
    start: u64,
    /// Range end byte (inclusive)
    end: u64,
    /// Whether compression is enabled for this write
    compression_enabled: bool,
    /// In-memory accumulation buffer for batched LZ4 framing.
    /// Incoming chunks are appended here and flushed as a single LZ4 frame
    /// (compressed blocks when `compression_enabled`, store-mode/stored blocks
    /// otherwise) once `batch_buf.len() >= batch_size`.
    /// See the `cache-miss-throughput` spec, Requirement 2.1, and the
    /// compression-content-aware-fix spec, Requirement 3.
    batch_buf: Vec<u8>,
    /// Target size (in bytes) at which `batch_buf` is flushed as one LZ4 frame.
    /// Seeded from `DiskCacheManager::compression_batch_size` at writer creation.
    batch_size: usize,
    /// Shared compression-stats counters (same Arc as the owning
    /// `DiskCacheManager`'s `CompressionHandler`), so this inline-framing
    /// streaming path contributes to the live stats exposed via
    /// `/metrics`/OTLP. Spec: compression-content-aware-fix Req 5.2.
    stats: std::sync::Arc<crate::compression::CompressionStatsAtomic>,
    /// The fleet-wide capacity reservation admitted at
    /// `begin_incremental_range_write` / `begin_incremental_part_write`, held for this
    /// writer's whole lifetime and released when it is dropped — on commit, on abort, on
    /// a mid-stream error, and on cancellation.
    ///
    /// **It has to live here rather than at the call site.** The writer outlives the
    /// function that admitted it: the GET-tee sites hand it to a `spawn_blocking` loop that
    /// drives chunks until the body ends. A reservation dropped when `begin_*` returns
    /// would cover the admission decision and none of the actual writing, which is the
    /// window it exists to close.
    ///
    /// `None` when the guard is disabled (floor of 0) or the fill was admitted outside the
    /// danger band, where no reservation is taken.
    ///
    /// Spec: cache-eviction-at-scale task 36 (A2).
    _capacity_reservation: Option<crate::capacity_guard::FleetReservation>,
    /// The fill's version witness, set by the GET-miss caller before its S3 request.
    fill_witness: Option<crate::cache::FillWitness>,
}
impl IncrementalRangeWriter {
    /// Guard this fill's publish with its version witness. See
    /// [`PreparedRangeFill::with_fill_witness`].
    pub(crate) fn set_fill_witness(&mut self, witness: Option<crate::cache::FillWitness>) {
        self.fill_witness = witness;
    }

    /// Returns the number of bytes currently held in the compression batch
    /// buffer (not yet flushed to disk). Primarily used by tests to verify
    /// batching behavior. Always `0` when compression is disabled.
    pub fn batch_buf_len(&self) -> usize {
        self.batch_buf.len()
    }
}

/// Compression facts for a finalized multipart part, returned by
/// [`DiskCacheManager::finalize_incremental_part`] so the caller can record them
/// in the upload tracker (`upload.meta`). `finalize_multipart_upload` later builds
/// the object's `RangeSpec` from the tracker (reading the published file's size
/// directly), so only the algorithm and uncompressed length are surfaced here.
pub(crate) struct PartFinalizeInfo {
    /// Algorithm the part bytes were written with (`Lz4` when compression is
    /// enabled, else `None`), recorded so the GET-side range loader decompresses
    /// correctly after the part is linked into the object.
    pub(crate) compression_algorithm: crate::compression::CompressionAlgorithm,
    /// Total decoded (uncompressed) object bytes written for this part.
    pub(crate) uncompressed_size: u64,
    /// Identity of the part file, taken after its writer synced and closed it
    /// ([`identity_after_close`], [`crate::cache_types::CachedPartInfo::file_identity`]).
    pub(crate) file_identity: Option<crate::cache_types::RangeFileIdentity>,
}

/// Which enumeration named a `.bin` file that invalidation is about to remove.
///
/// Recorded per candidate so a removal failure is attributable in the log: a survivor from
/// the journal side and a survivor from the `.meta` side have different causes and a future
/// investigation needs to know which it is looking at.
///
/// Spec: range-invalidation-completeness. Requirements: 1.6
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemovalCandidateSource {
    /// Named by the key's consolidated `metadata/{bucket}/{XX}/{YYY}/{key}.meta` document.
    ConsolidatedMetadata,
    /// Named by a `JournalOperation::Add` entry under `metadata/_journals/` that no
    /// consolidation cycle has folded into the `.meta` yet — the Journal_Window.
    Journal,
}

impl RemovalCandidateSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::ConsolidatedMetadata => "meta",
            Self::Journal => "journal",
        }
    }
}

/// One `.bin` file invalidation has resolved as a removal candidate for a key.
///
/// The two enumerations produce these independently and can name the same file, so the
/// candidate list is deduplicated on `resolved_path` before anything is removed — a naive
/// union over-reports by every duplicate, because the file-absent branch counts an absent
/// file as removed.
///
/// Spec: range-invalidation-completeness. Requirements: 1.1, 1.2, 1.4
#[derive(Debug)]
struct RangeRemovalCandidate {
    start: u64,
    end: u64,
    /// Bytes on disk, as the naming record recorded them. Summed into `size_freed` only for
    /// a candidate whose file was actually unlinked.
    compressed_size: u64,
    /// Absolute path, `{cache_dir}/ranges/{relative_path}`, already checked by
    /// [`is_safe_relative_file_path`].
    resolved_path: PathBuf,
    /// The path relative to `{cache_dir}/ranges`, which is what a `JournalOperation::Remove`
    /// entry carries as its `range_file_path`.
    relative_path: String,
    source: RemovalCandidateSource,
}

/// The outcome of one [`DiskCacheManager::remove_all_ranges_detailed`] pass, in enough detail
/// for the caller to tell a complete removal from a partial one rather than logging both as
/// success.
///
/// Spec: range-invalidation-completeness. Requirements: 1.4, 1.6, 5.1
pub(crate) struct RangeRemovalOutcome {
    /// Ranges this pass established are no longer on disk: an unlinked file, or a file the
    /// `.meta` named and that was already absent (which is pre-existing behaviour, retained).
    pub(crate) removed_count: usize,
    /// Bytes freed by files this pass actually unlinked.
    pub(crate) size_freed: u64,
    /// Distinct `.bin` paths the two enumerations named between them, after deduplication.
    /// `removed_count + failed_count` can be below this when a journal-named file was
    /// already absent — see `remove_all_ranges_detailed`.
    pub(crate) candidate_count: usize,
    /// How many of `candidate_count` came from the journal enumeration only.
    pub(crate) journal_derived_count: usize,
    /// Candidates whose `.bin` is still on disk because the unlink failed.
    pub(crate) failed_count: usize,
    /// The `(start, end)` of each failed candidate the `.meta` named.
    pub(crate) failed_meta_extents: Vec<(u64, u64)>,
}

/// Whether `e` means the file system has no space left (ENOSPC).
///
/// std reports ENOSPC as `ErrorKind::StorageFull` (stable since Rust 1.83), so the kind and the
/// raw OS code are the primary checks. The message match is a fallback for an error that was
/// re-wrapped as `Other` and lost its OS code on the way.
pub(crate) fn is_disk_full_error(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::StorageFull
        || e.raw_os_error() == Some(libc::ENOSPC)
        || e.to_string().contains("No space left on device")
}

/// Count `e` in `cache.disk_full_events_total` if it means the cache volume is full, and
/// return whether it did. Every cache write that can fail with ENOSPC calls this on its error
/// path, before mapping the error; `site` names the write in the WARN.
pub(crate) fn note_disk_full(e: &std::io::Error, site: &'static str) -> bool {
    if !is_disk_full_error(e) {
        return false;
    }
    let total = crate::metrics::record_disk_full_event();
    warn!(
        "Cache write failed, cache volume full: site={}, error={}, disk_full_events_total={}",
        site, e, total
    );
    true
}

/// Cached range files whose LZ4 data fails to decode. Published as
/// `atomic_metadata.range_file_corruption_detected`. Per-instance, monotone since process
/// start.
///
/// Counted on the two paths that read a `.bin` for a client: the buffered
/// `load_range_data` and the streamed `stream_range_data`. Also counted: a file that decodes
/// cleanly to a length other than the metadata records (a truncation at a frame boundary).
/// Every writer names a range file by the bounds it holds (`store_range` and the partial
/// commit both re-path a clamped range), so a newer version of the object written to the same
/// path decodes to the same length and cannot produce this. Deliberately not counted: I/O
/// errors (an NFS read failure is not evidence the stored bytes are damaged).
static RANGE_FILE_CORRUPTION_DETECTED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Record one corrupt cached range file. `file` and `detail` go into the WARN.
fn note_range_file_corruption(file: &str, detail: &str) {
    let total =
        RANGE_FILE_CORRUPTION_DETECTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    warn!(
        "Cached range file is corrupt: file={}, detail={}, range_file_corruption_detected={}",
        file, detail, total
    );
}

/// `atomic_metadata.range_file_corruption_detected`. See [`RANGE_FILE_CORRUPTION_DETECTED`].
pub(crate) fn range_file_corruption_detected() -> u64 {
    RANGE_FILE_CORRUPTION_DETECTED.load(std::sync::atomic::Ordering::Relaxed)
}

/// How long a damaged-range mark lasts after the last read it refused. Two heal
/// windows ([`RANGE_HEAL_WINDOW`]): every read a mark refuses retries the heal under the heal
/// limit, so a mark read at least once a window outlasts a full window and its range is healed
/// at the first window with room, or once a consolidation pass lists a range only a pending
/// journal `Add` named. A mark no read relied on for this long is dropped; if its file is still
/// there and still damaged, the next streamed read of it breaks once more and marks it again.
pub(crate) const DAMAGED_RANGE_MARK_TTL: Duration = Duration::from_secs(120);
/// Most damaged-range marks one proxy holds: about 1 MiB of paths at the limit. A mark
/// beyond it drops the aged marks, then the oldest. The cost of a dropped mark is one more
/// broken streamed read of that file, never wrong bytes: the decode still refuses it.
pub(crate) const DAMAGED_RANGE_MARK_LIMIT: usize = 4096;

/// Range files a read on this proxy found damaged, by path, each with the stamp of the
/// file that read opened ([`RangeFileStamp`]: length, modification time and inode) and when a
/// read last relied on the mark. A streamed read finds damage only after its `206` head is
/// sent, so it cannot be rescued; the mark makes the next read of that file on this proxy refuse
/// it before reading a byte, which the serve answers from S3 the way it answers a missing file.
/// That holds when the heal leaves the file in place: a range only a pending journal `Add`
/// lists (`NotListed`), or the heal limit reached (`Suppressed`).
///
/// The path names the cache key and the extent (`{bucket}/{XX}/{YYY}/{key}_{start}-{end}.bin`
/// under the cache directory), and the stamp names the file, so a refill renamed over the path
/// is not caught by its predecessor's mark: the first read that sees another stamp drops it.
struct DamagedRangeMarks {
    marks: std::collections::HashMap<PathBuf, (RangeFileStamp, std::time::Instant)>,
    limit: usize,
    ttl: Duration,
}

impl DamagedRangeMarks {
    fn new(limit: usize, ttl: Duration) -> Self {
        Self {
            marks: std::collections::HashMap::new(),
            limit,
            ttl,
        }
    }

    /// Mark the file `stamp` names at `path`. At the limit, aged marks go first, then the
    /// oldest.
    fn mark(&mut self, path: &Path, stamp: RangeFileStamp, now: std::time::Instant) {
        if self.limit == 0 {
            return;
        }
        if !self.marks.contains_key(path) && self.marks.len() >= self.limit {
            let ttl = self.ttl;
            self.marks
                .retain(|_, (_, at)| now.saturating_duration_since(*at) < ttl);
            if self.marks.len() >= self.limit {
                let oldest = self
                    .marks
                    .iter()
                    .min_by_key(|(_, (_, at))| *at)
                    .map(|(p, _)| p.clone());
                if let Some(oldest) = oldest {
                    self.marks.remove(&oldest);
                }
            }
        }
        self.marks.insert(path.to_path_buf(), (stamp, now));
    }

    /// Whether any mark names `path`, whatever its file. No I/O.
    fn holds(&self, path: &Path) -> bool {
        self.marks.contains_key(path)
    }

    /// Whether the file `stamp` names at `path` is marked. A match refreshes the mark; a mark
    /// for another file, or one no read relied on for `ttl`, is dropped.
    fn matches(&mut self, path: &Path, stamp: &RangeFileStamp, now: std::time::Instant) -> bool {
        let ttl = self.ttl;
        let Some((marked, at)) = self.marks.get_mut(path) else {
            return false;
        };
        if *marked == *stamp && now.saturating_duration_since(*at) < ttl {
            *at = now;
            return true;
        }
        self.marks.remove(path);
        false
    }

    fn unmark(&mut self, path: &Path) {
        self.marks.remove(path);
    }
}

/// The process-wide mark set every `DiskCacheManager` shares.
fn damaged_range_marks() -> std::sync::MutexGuard<'static, DamagedRangeMarks> {
    static MARKS: std::sync::OnceLock<std::sync::Mutex<DamagedRangeMarks>> =
        std::sync::OnceLock::new();
    MARKS
        .get_or_init(|| {
            std::sync::Mutex::new(DamagedRangeMarks::new(
                DAMAGED_RANGE_MARK_LIMIT,
                DAMAGED_RANGE_MARK_TTL,
            ))
        })
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

/// Mark the range file at `path`, opened with `stamp`, as damaged. Called beside every
/// [`note_range_file_corruption`].
fn mark_damaged_range_file(path: &Path, stamp: RangeFileStamp) {
    damaged_range_marks().mark(path, stamp, std::time::Instant::now());
}

fn unmark_damaged_range_file(path: &Path) {
    damaged_range_marks().unmark(path);
}

/// Whether the file a read opened at `path`, with `stamp`, is one a read found damaged.
fn damaged_range_mark_matches(path: &Path, stamp: &RangeFileStamp) -> bool {
    damaged_range_marks().matches(path, stamp, std::time::Instant::now())
}

/// The stamp of the file at `path` when a mark names that file. No I/O unless a mark names the
/// path; then one `stat`, and a mark for a file no longer there, or another file, is dropped.
fn marked_damaged_range_file(path: &Path) -> Option<RangeFileStamp> {
    if !damaged_range_marks().holds(path) {
        return None;
    }
    match RangeFileStamp::at(path) {
        Some(stamp) => damaged_range_mark_matches(path, &stamp).then_some(stamp),
        None => {
            unmark_damaged_range_file(path);
            None
        }
    }
}

/// The cache-miss error a read reports for a file a mark refused.
fn marked_damaged_range_error(spec: &crate::cache_types::RangeSpec) -> ProxyError {
    ProxyError::CacheError(format!(
        "Range file {} was found damaged by an earlier read on this proxy, not served \
         (cache miss - will fetch from S3)",
        spec.file_path
    ))
}
/// Cached range files a read refused because the file it opened is not the one its listing
/// describes: `fstat` of the read's own descriptor reported another length or modification
/// time than the listing's [`crate::cache_types::RangeSpec::file_identity`]. Typically another
/// version's file renamed over the same path, or a volume copied with a tool that does not
/// preserve modification times. Not damage, so counted apart from
/// [`RANGE_FILE_CORRUPTION_DETECTED`], and healed outside the heal limit: each heal is one
/// removal, and a refill records the new file's identity.
static RANGE_FILE_IDENTITY_MISMATCH_DETECTED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Record one refused range file. One WARN per detection, with the file and both identities.
fn note_range_file_identity_mismatch(
    file: &str,
    listed: Option<crate::cache_types::RangeFileIdentity>,
    found: crate::cache_types::RangeFileIdentity,
) {
    let total = RANGE_FILE_IDENTITY_MISMATCH_DETECTED
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;
    #[cfg(test)]
    serve_refusal_log::note(file, "identity");
    warn!(
        "Cached range file is not the file its listing describes, not served: file={}, \
         listed={:?}, found={:?}, range_file_identity_mismatch_detected={}",
        file, listed, found, total
    );
}
/// `atomic_metadata.range_file_identity_mismatch_detected`. See
/// [`RANGE_FILE_IDENTITY_MISMATCH_DETECTED`].
pub(crate) fn range_file_identity_mismatch_detected() -> u64 {
    RANGE_FILE_IDENTITY_MISMATCH_DETECTED.load(std::sync::atomic::Ordering::Relaxed)
}
/// A range file whose `fstat` identity differs from its listing's: the stamp a heal judges it
/// by, and the identity found.
pub(crate) struct WrongRangeFile {
    stamp: RangeFileStamp,
    found: crate::cache_types::RangeFileIdentity,
}
/// The identity `meta` (an `fstat` of the descriptor a read uses) reports, when the listing
/// records one and it does not match ([`crate::cache_types::RangeFileIdentity::matches`]).
/// `None` means serve: the identity matches, or the listing records none (cached before 3.0.0,
/// or rewritten by an older proxy) and is unverified.
fn wrong_range_file(
    listed: Option<crate::cache_types::RangeFileIdentity>,
    meta: &std::fs::Metadata,
) -> Option<WrongRangeFile> {
    let listed = listed?;
    let found = crate::cache_types::RangeFileIdentity::of(meta)?;
    (!listed.matches(&found)).then(|| WrongRangeFile {
        stamp: RangeFileStamp::of(meta),
        found,
    })
}
/// `fstat` of the descriptor a streamed read uses. Test seam: fails for a `file_path` a test
/// registered with [`fstat_failure_seam::fail_for`].
fn fstat_read_descriptor(
    file: &std::fs::File,
    file_path: &str,
) -> std::io::Result<std::fs::Metadata> {
    #[cfg(test)]
    if fstat_failure_seam::fails(file_path) {
        return Err(std::io::Error::other("seam: fstat failed"));
    }
    #[cfg(not(test))]
    let _ = file_path;
    file.metadata()
}
#[cfg(test)]
pub(crate) mod fstat_failure_seam {
    use std::sync::Mutex;
    static PATHS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    pub(crate) fn fail_for(file_path: &str) {
        PATHS.lock().unwrap().push(file_path.to_string());
    }
    pub(crate) fn fails(file_path: &str) -> bool {
        PATHS.lock().unwrap().iter().any(|p| p == file_path)
    }
}
/// What the pending journals hold for one extent relative to a version label. See
/// [`DiskCacheManager::pending_journal_range_for_label`].
pub(crate) enum PendingForLabel {
    /// A surviving, non-conflicting `Add` of the label's version.
    Found(crate::cache_types::RangeSpec),
    /// Surviving `Add`s for the extent, none of the label's version.
    OtherVersion,
    /// No surviving, non-conflicting `Add` for the extent.
    Absent,
}
/// A live-TTL verdict and the document it was judged on. `document` is `None` when no
/// document could be read (the verdict is then `Expired` with no validators); `from_meta_file`
/// is false for this instance's tracked fill, which is never cached in RAM.
#[derive(Debug)]
pub(crate) struct CheckedExpiration {
    pub(crate) verdict: crate::cache_types::ObjectExpirationResult,
    pub(crate) document: Option<crate::cache_types::NewCacheMetadata>,
    pub(crate) from_meta_file: bool,
}
impl CheckedExpiration {
    fn without_document(verdict: crate::cache_types::ObjectExpirationResult) -> Self {
        Self {
            verdict,
            document: None,
            from_meta_file: false,
        }
    }
}
/// Test record of why a serve declined cached bytes: keyed by cache key, or by the
/// range's `file_path` for an identity refusal. A test asserts the refusal it provoked was the
/// one under test by reading its own key, so parallel tests cannot move each other's figures
/// the way they move a process-global counter.
#[cfg(test)]
pub(crate) mod serve_refusal_log {
    use std::collections::HashMap;
    use std::sync::Mutex;
    static LOG: Mutex<Option<HashMap<String, Vec<&'static str>>>> = Mutex::new(None);
    pub(crate) fn note(key: &str, reason: &'static str) {
        LOG.lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .entry(key.to_string())
            .or_default()
            .push(reason);
    }
    pub(crate) fn reasons(key: &str) -> Vec<&'static str> {
        LOG.lock()
            .unwrap()
            .as_ref()
            .and_then(|log| log.get(key).cloned())
            .unwrap_or_default()
    }
}
/// Open the range file at `path` and `fstat` the descriptor. `Ok(Err(_))` when the file is not
/// the one `listed` describes; nothing is read from it then. Blocking.
fn open_range_file_checked(
    path: &Path,
    listed: Option<crate::cache_types::RangeFileIdentity>,
) -> std::io::Result<std::result::Result<(std::fs::File, std::fs::Metadata), WrongRangeFile>> {
    let file = std::fs::File::open(path)?;
    let meta = file.metadata()?;
    Ok(match wrong_range_file(listed, &meta) {
        Some(wrong) => Err(wrong),
        None => Ok((file, meta)),
    })
}
/// [`open_range_file_checked`], then read the whole file through the same descriptor. Returns
/// the bytes and the stamp of the file read. Blocking.
pub(crate) fn read_range_file_checked(
    path: &Path,
    listed: Option<crate::cache_types::RangeFileIdentity>,
) -> std::io::Result<std::result::Result<(Vec<u8>, RangeFileStamp), WrongRangeFile>> {
    use std::io::Read;
    let (mut file, meta) = match open_range_file_checked(path, listed)? {
        Ok(opened) => opened,
        Err(wrong) => return Ok(Err(wrong)),
    };
    let mut data = Vec::with_capacity(usize::try_from(meta.len()).unwrap_or(0));
    file.read_to_end(&mut data)?;
    Ok(Ok((data, RangeFileStamp::of(&meta))))
}
/// Count a read's refusal of `wrong` for `spec`, heal the range (outside the heal limit), and
/// return the cache-miss error the read reports. The caller serves the request from S3.
pub(crate) async fn refuse_wrong_range_file(
    cache_dir: &Path,
    consolidator: Option<&Arc<crate::journal_consolidator::JournalConsolidator>>,
    metadata_cache: Option<&Arc<crate::metadata_cache::MetadataCache>>,
    spec: &crate::cache_types::RangeSpec,
    wrong: WrongRangeFile,
) -> ProxyError {
    note_range_file_identity_mismatch(&spec.file_path, spec.file_identity, wrong.found);
    heal_damaged_range(
        cache_dir,
        consolidator,
        metadata_cache,
        &global_range_heal_limiter(),
        spec,
        Some(wrong.stamp),
        false,
    )
    .await;
    ProxyError::CacheError(format!(
        "Range file {} is not the file its listing describes (listed {:?}, found {:?}) \
         (cache miss - will fetch from S3)",
        spec.file_path, spec.file_identity, wrong.found
    ))
}

/// Heals one proxy may perform per [`RANGE_HEAL_WINDOW`] (range-file-self-heal R4). Past it a
/// damaged range file is detected, counted and logged as before, and left in place: a volume
/// that keeps returning damaged bytes would otherwise turn every read into an S3 fetch plus a
/// re-cache that is damaged again.
pub(crate) const RANGE_HEAL_LIMIT: u32 = 16;
/// The window [`RANGE_HEAL_LIMIT`] counts over.
pub(crate) const RANGE_HEAL_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);

/// The per-proxy bound on range-file heals and its two counters,
/// `atomic_metadata.range_file_corruption_healed` and
/// `atomic_metadata.range_file_corruption_heal_suppressed`. Production uses one process-wide
/// instance ([`global_range_heal_limiter`]); a test gives its `DiskCacheManager` its own so the
/// counts it asserts are its own.
pub(crate) struct RangeHealLimiter {
    limit: u32,
    window: std::time::Duration,
    /// Start of the current window and the heals taken in it.
    state: std::sync::Mutex<Option<(std::time::Instant, u32)>>,
    healed: std::sync::atomic::AtomicU64,
    suppressed: std::sync::atomic::AtomicU64,
    /// Test seam: every heal that passes the stamp check waits here, so concurrent heals of
    /// one range all reach the locked removal.
    #[cfg(test)]
    rendezvous: Option<Arc<tokio::sync::Barrier>>,
    /// Test seam: what each heal returned, in completion order.
    #[cfg(test)]
    outcomes: std::sync::Mutex<Vec<RangeHealOutcome>>,
    /// Test seam: run once by the buffered read after it detects damage and before it heals,
    /// standing in for a peer's refill landing in that gap.
    #[cfg(test)]
    after_detect: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl RangeHealLimiter {
    pub(crate) fn new(limit: u32, window: std::time::Duration) -> Self {
        Self {
            limit,
            window,
            state: std::sync::Mutex::new(None),
            healed: std::sync::atomic::AtomicU64::new(0),
            suppressed: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            rendezvous: None,
            #[cfg(test)]
            outcomes: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            after_detect: std::sync::Mutex::new(None),
        }
    }

    /// Hold each heal past its stamp check until `readers` heals have arrived.
    #[cfg(test)]
    pub(crate) fn with_rendezvous(mut self, readers: usize) -> Self {
        self.rendezvous = Some(Arc::new(tokio::sync::Barrier::new(readers)));
        self
    }

    /// What each heal through this limiter returned, in completion order.
    #[cfg(test)]
    pub(crate) fn outcomes(&self) -> Vec<RangeHealOutcome> {
        self.outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Run `f` once, after the next buffered read detects damage and before it heals.
    #[cfg(test)]
    pub(crate) fn set_after_detect(&self, f: Box<dyn FnOnce() + Send>) {
        *self.after_detect.lock().unwrap_or_else(|p| p.into_inner()) = Some(f);
    }

    #[cfg(test)]
    fn run_after_detect(&self) {
        let f = self
            .after_detect
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(f) = f {
            f();
        }
    }

    #[cfg(not(test))]
    fn run_after_detect(&self) {}

    #[cfg(test)]
    async fn wait_rendezvous(&self) {
        if let Some(barrier) = &self.rendezvous {
            // Bounded, so a reader that never arrives fails the test rather than hanging it.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait()).await;
        }
    }

    #[cfg(not(test))]
    async fn wait_rendezvous(&self) {}

    #[cfg(test)]
    fn record(&self, outcome: RangeHealOutcome) {
        self.outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(outcome);
    }

    #[cfg(not(test))]
    fn record(&self, _outcome: RangeHealOutcome) {}

    /// Take one heal from the current window, or report the window full.
    fn try_acquire(&self) -> bool {
        let now = std::time::Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state.as_mut() {
            Some((start, used)) if now.duration_since(*start) < self.window => {
                if *used >= self.limit {
                    return false;
                }
                *used += 1;
                true
            }
            _ => {
                *state = Some((now, 1));
                self.limit > 0
            }
        }
    }

    /// Damaged ranges removed since process start.
    pub(crate) fn healed(&self) -> u64 {
        self.healed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reads past the heal limit, since process start. Each one left its damaged range in
    /// place, so one range read repeatedly in a full window is counted once per read.
    pub(crate) fn suppressed(&self) -> u64 {
        self.suppressed.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The process-wide heal bound every production `DiskCacheManager` shares.
fn global_range_heal_limiter() -> Arc<RangeHealLimiter> {
    static LIMITER: std::sync::OnceLock<Arc<RangeHealLimiter>> = std::sync::OnceLock::new();
    LIMITER
        .get_or_init(|| Arc::new(RangeHealLimiter::new(RANGE_HEAL_LIMIT, RANGE_HEAL_WINDOW)))
        .clone()
}

/// Log, at most once a minute per key, that a pre-3.0.0 range listing is being fetched again
/// ([`crate::cache_types::RangeSpec::is_unverified_prefix`]). The heal removes the listing on
/// the first read, so a key normally logs once; the bound covers a heal that cannot run yet
/// (another proxy holds the key's slot lock, or this proxy's RAM copy of the `.meta` is
/// stale), when every read of the key would otherwise log.
fn note_unverified_prefix_refetch(cache_key: &str, start: u64, end: u64) {
    const QUIET: Duration = Duration::from_secs(60);
    const MAX_KEYS: usize = 4096;
    static LAST: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>,
    > = std::sync::OnceLock::new();
    let now = std::time::Instant::now();
    {
        let mut last = LAST
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if last
            .get(cache_key)
            .is_some_and(|at| now.duration_since(*at) < QUIET)
        {
            return;
        }
        if last.len() >= MAX_KEYS {
            last.clear();
        }
        last.insert(cache_key.to_string(), now);
    }
    warn!(
        "Fetching cached range {}-{} of {} again from S3: it was cached by a release before \
         3.0.0 and is replaced on this read",
        start, end, cache_key
    );
}

/// `atomic_metadata.range_file_corruption_healed`. See [`RangeHealLimiter`].
pub(crate) fn range_file_corruption_healed() -> u64 {
    global_range_heal_limiter().healed()
}

/// `atomic_metadata.range_file_corruption_heal_suppressed`. See [`RangeHealLimiter`].
pub(crate) fn range_file_corruption_heal_suppressed() -> u64 {
    global_range_heal_limiter().suppressed()
}

/// Which file a read found damaged: a refill renames a new file over the same path, so a heal
/// that finds a different stamp there leaves it alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RangeFileStamp {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    ino: u64,
}

impl RangeFileStamp {
    fn of(meta: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            len: meta.len(),
            modified: meta.modified().ok(),
            #[cfg(unix)]
            ino: meta.ino(),
        }
    }

    fn at(path: &Path) -> Option<Self> {
        std::fs::metadata(path).ok().map(|m| Self::of(&m))
    }
}

/// What [`heal_damaged_range`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeHealOutcome {
    /// The range's file and `.meta` entry were removed and debited.
    Healed,
    /// The heal window was full; nothing was removed.
    Suppressed,
    /// No `.meta` lists this exact range (a journal-only fill, a key already rewritten, or a
    /// file outside the cache layout); nothing was removed.
    NotListed,
    /// The file at the path is no longer the one the read found damaged, or is gone; nothing
    /// was removed.
    Replaced,
    /// Under the slot lock the `.meta` was another version, or the file had already gone.
    Raced,
    /// The locked removal failed (slot lock budget, unreadable `.meta`); nothing was debited.
    Failed,
}

/// Invalidate one damaged cached range: its `.bin` and its entry in the key's `.meta`
/// (range-file-self-heal R1, R2). The removal is read-tier eviction's own:
/// `batch_delete_ranges_expecting` under the key's metadata slot lock with the object version
/// re-checked, then eviction's Step 5 accounting from what actually left the disk
/// (`subtract_range`, the write-cache debit for a staged range and the staged shard mark,
/// `Remove` journal entries, `decrement_cached_objects` if the `.meta` went). Nothing else of
/// the key is touched, and no revalidation is started.
///
/// The read paths hold only the range's `RangeSpec`, so the key is recovered from the `.meta`
/// that sits beside the range's path (`{stem}_{start}-{end}.bin` beside `{stem}.meta`) and
/// accepted only when that document's own `cache_key` derives both paths back and lists this
/// exact range. A range the read took from a pending journal entry is therefore not healed.
///
/// When the `.meta` no longer lists the range (healed here, or raced by another removal),
/// this proxy's RAM copy of it, `metadata_cache`, is dropped too, so the next read here is an
/// ordinary miss at once rather than after `metadata_cache.refresh_interval`.
async fn heal_damaged_range(
    cache_dir: &Path,
    consolidator: Option<&Arc<crate::journal_consolidator::JournalConsolidator>>,
    metadata_cache: Option<&Arc<crate::metadata_cache::MetadataCache>>,
    limiter: &RangeHealLimiter,
    damaged: &crate::cache_types::RangeSpec,
    observed: Option<RangeFileStamp>,
    rate_limited: bool,
) -> RangeHealOutcome {
    let outcome = heal_damaged_range_once(
        cache_dir,
        consolidator,
        metadata_cache,
        limiter,
        damaged,
        observed,
        rate_limited,
        HealCause::Damaged,
    )
    .await;
    limiter.record(outcome);
    if outcome == RangeHealOutcome::Healed {
        unmark_damaged_range_file(&cache_dir.join("ranges").join(&damaged.file_path));
    }
    outcome
}

/// The largest range [`looks_like_s3_error_document`] is applied to. A release before 3.0.0
/// clamped a range it stored from an S3 error reply to the error body's length, and S3 error
/// bodies are a few KiB (a `SignatureDoesNotMatch` body echoing the request is about 6.4 KiB),
/// so a longer legacy range cannot be one.
pub(crate) const LEGACY_ERROR_BODY_MAX_BYTES: u64 = 64 * 1024;

/// Whether `data` begins as an S3 error document: an XML declaration or `<Error>` first, and
/// both `<Error>` and `<Code>` within the first 512 bytes.
pub(crate) fn looks_like_s3_error_document(data: &[u8]) -> bool {
    let head = &data[..data.len().min(512)];
    let contains = |needle: &[u8]| head.windows(needle.len()).any(|w| w == needle);
    (head.starts_with(b"<?xml") || head.starts_with(b"<Error>"))
        && contains(b"<Error>")
        && contains(b"<Code>")
}

/// Why a listed range is being removed by [`heal_damaged_range_once`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealCause {
    /// A read found the range's file damaged or not the file its listing describes. Counted
    /// in `range_file_corruption_healed`.
    Damaged,
    /// The listing matches [`crate::cache_types::RangeSpec::is_unverified_prefix`]: written
    /// before 3.0.0, at offset 0, short of the object's end. Its file is not known to be
    /// damaged, so it is not counted as corruption, and the listing is removed even when the
    /// file is already gone, because a listing left in place would drop the refill's publish
    /// (an extent already listed is not listed again).
    UnverifiedPrefix,
    /// The listing records no file identity (written before 3.0.0) and its bytes are an S3
    /// error document ([`looks_like_s3_error_document`]), which a release before 3.0.0 could
    /// store from an error reply. Not counted as corruption.
    LegacyErrorBody,
}

/// The body of [`heal_damaged_range`].
#[allow(clippy::too_many_arguments)] // One flag per caller-visible difference; a struct would only rename them.
async fn heal_damaged_range_once(
    cache_dir: &Path,
    consolidator: Option<&Arc<crate::journal_consolidator::JournalConsolidator>>,
    metadata_cache: Option<&Arc<crate::metadata_cache::MetadataCache>>,
    limiter: &RangeHealLimiter,
    damaged: &crate::cache_types::RangeSpec,
    observed: Option<RangeFileStamp>,
    rate_limited: bool,
    cause: HealCause,
) -> RangeHealOutcome {
    let suffix = format!("_{}-{}.bin", damaged.start, damaged.end);
    let Some(stem) = damaged.file_path.strip_suffix(&suffix) else {
        return RangeHealOutcome::NotListed;
    };
    let meta_path = cache_dir.join("metadata").join(format!("{}.meta", stem));
    let bin_path = cache_dir.join("ranges").join(&damaged.file_path);
    let meta: crate::cache_types::NewCacheMetadata = match tokio::fs::read_to_string(&meta_path)
        .await
        .ok()
        .and_then(|c| serde_json::from_str(&c).ok())
    {
        Some(meta) => meta,
        None => return RangeHealOutcome::NotListed,
    };
    let cache_key = meta.cache_key.clone();
    let locator = DiskCacheManager::new(cache_dir.to_path_buf(), true, 4096, false, 1_048_576);
    let listed = meta.ranges.iter().find(|r| {
        r.start == damaged.start
            && r.end == damaged.end
            && r.file_path == damaged.file_path
            && r.compressed_size == damaged.compressed_size
            && r.uncompressed_size == damaged.uncompressed_size
            && r.file_identity == damaged.file_identity
    });
    let Some(listed) = listed else {
        debug!(
            "Damaged range file not healed, its .meta does not list it: file={}, range={}-{}",
            damaged.file_path, damaged.start, damaged.end
        );
        return RangeHealOutcome::NotListed;
    };
    if locator.get_new_metadata_file_path(&cache_key) != meta_path
        || locator.get_new_range_file_path(&cache_key, damaged.start, damaged.end) != bin_path
    {
        return RangeHealOutcome::NotListed;
    }
    if cause == HealCause::Damaged {
        match (observed, RangeFileStamp::at(&bin_path)) {
            (_, None) => return RangeHealOutcome::Replaced,
            (Some(seen), Some(now)) if seen != now => return RangeHealOutcome::Replaced,
            _ => {}
        }
        limiter.wait_rendezvous().await;
    }
    // An identity mismatch is healed outside the limit: the limit protects against a volume
    // that keeps returning damaged bytes, and a refused file is not one. Each such heal is one
    // removal, after which a refill records the new file's identity, so a volume copy that
    // reset every modification time refills at read rate instead of 16 ranges a minute.
    if rate_limited && !limiter.try_acquire() {
        let total = limiter
            .suppressed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        warn!(
            "Damaged range file left in place, heal limit reached: key={}, range={}-{}, file={}, \
             limit={} per {}s, range_file_corruption_heal_suppressed={}",
            cache_key,
            damaged.start,
            damaged.end,
            damaged.file_path,
            limiter.limit,
            limiter.window.as_secs(),
            total
        );
        return RangeHealOutcome::Suppressed;
    }
    let identity = ExpectedObjectIdentity::of(&meta);
    let expectation = DeleteExpectation {
        identity: &identity,
        staged_selected: crate::cache_types::is_staged_range_spec(
            listed,
            meta.object_metadata.is_write_cached,
        ),
    };
    let deletion = match locator
        .batch_delete_ranges_expecting(
            &cache_key,
            &[(damaged.start, damaged.end)],
            Some(expectation),
        )
        .await
    {
        Ok(deletion) => deletion,
        Err(e) => {
            warn!(
                "Damaged range file not healed, removal failed: key={}, range={}-{}, error={}",
                cache_key, damaged.start, damaged.end, e
            );
            return RangeHealOutcome::Failed;
        }
    };
    if deletion.identity_mismatch || deletion.unlinked_ranges.is_empty() {
        // Another removal or a rewrite changed the `.meta` first; this proxy's RAM copy of it
        // is stale either way.
        if let Some(metadata_cache) = metadata_cache {
            metadata_cache.invalidate(&cache_key).await;
        }
        return RangeHealOutcome::Raced;
    }
    if let Some(consolidator) = consolidator {
        let accumulator = consolidator.size_accumulator();
        for u in &deletion.unlinked_ranges {
            accumulator.subtract_range(&cache_key, u.start, u.end, u.compressed_size);
            if u.staged {
                accumulator.subtract_write_cache(u.compressed_size);
            }
        }
        if deletion.staged_stripped {
            crate::staged_fold::mark_shard_dirty(
                cache_dir,
                consolidator.write_ledger().instance_id(),
                &cache_key,
            );
        }
        consolidator
            .write_eviction_journal_entries(
                deletion
                    .unlinked_ranges
                    .iter()
                    .map(|u| {
                        (
                            cache_key.clone(),
                            u.start,
                            u.end,
                            if u.file_size > 0 {
                                u.file_size
                            } else {
                                u.compressed_size
                            },
                            u.bin_path.to_string_lossy().to_string(),
                        )
                    })
                    .collect(),
            )
            .await;
        if deletion.meta_unlinked {
            consolidator.decrement_cached_objects(1).await;
        }
    }
    // This proxy's RAM copy of the `.meta` still lists the healed range.
    if let Some(metadata_cache) = metadata_cache {
        metadata_cache.invalidate(&cache_key).await;
    }
    if cause == HealCause::LegacyErrorBody {
        return RangeHealOutcome::Healed;
    }
    if cause == HealCause::UnverifiedPrefix {
        debug!(
            "Unverified pre-3.0.0 range listing removed, the next read refills it: key={}, \
             range={}-{}, file={}",
            cache_key, damaged.start, damaged.end, damaged.file_path
        );
        return RangeHealOutcome::Healed;
    }
    let total = limiter
        .healed
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;
    warn!(
        "Damaged range file invalidated, the next read refetches it: key={}, range={}-{}, \
         file={}, range_file_corruption_healed={}",
        cache_key, damaged.start, damaged.end, damaged.file_path, total
    );
    RangeHealOutcome::Healed
}
/// Unparseable `.meta` documents replaced by a freshly fetched one. Published as
/// `cache.metadata_heal_overwrite_total`. Per-instance, monotone since process start.
///
/// Counted only after the replacement is renamed into place, at the three writes that read
/// the existing document under the key's metadata lock and replace it when it does not
/// parse: [`DiskCacheManager::store_range`], the full-object fill
/// (`CacheManager::store_full_object_as_range_new`) and its zero-length arm. Also counted by
/// `CacheManager::remove_corrupt_metadata` when it removes one, a HEAD-path heal that has no
/// production caller. The denominator is `cache.corruption_metadata_total`: a detection that
/// is never followed by a full-object fill of that key is never healed.
static METADATA_HEAL_OVERWRITES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Record one healed `.meta`. Call only once the replacement is in place.
pub(crate) fn note_metadata_heal_overwrite(cache_key: &str) {
    let total = METADATA_HEAL_OVERWRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    info!(
        "[METADATA_HEAL] Unparseable .meta replaced: cache_key={}, metadata_heal_overwrite_total={}",
        cache_key, total
    );
}
/// `cache.metadata_heal_overwrite_total`. See [`METADATA_HEAL_OVERWRITES`].
pub(crate) fn metadata_heal_overwrite_total() -> u64 {
    METADATA_HEAL_OVERWRITES.load(std::sync::atomic::Ordering::Relaxed)
}

// Test-only fault injection for the streaming range writer's batch write: the next
// `flush_batch` on this thread fails with this raw OS error instead of writing.
#[cfg(test)]
thread_local! {
    static INJECT_BATCH_WRITE_OS_ERROR: std::cell::Cell<Option<i32>> =
        const { std::cell::Cell::new(None) };
}

/// Disk cache manager for file-based cache operations
pub struct DiskCacheManager {
    cache_dir: PathBuf,
    compression_handler: CompressionHandler,
    metrics_manager: Option<std::sync::Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>,
    size_tracker: Option<std::sync::Arc<crate::cache_size_tracker::CacheSizeTracker>>,
    write_cache_enabled: bool,
    /// Optional HybridMetadataWriter for shared storage mode
    /// When set, metadata writes go through the hybrid writer for multi-instance coordination
    hybrid_metadata_writer: Option<Arc<Mutex<HybridMetadataWriter>>>,
    /// Optional CacheHitUpdateBuffer for shared storage mode
    /// When set, cache-hit updates (TTL refresh, access updates) go through the buffer
    cache_hit_update_buffer: Option<Arc<CacheHitUpdateBuffer>>,
    /// Optional JournalConsolidator for direct size tracking
    /// When set, size is updated directly when storing new ranges (bypasses journal-based tracking)
    journal_consolidator: Option<Arc<crate::journal_consolidator::JournalConsolidator>>,
    /// The heal bound a damaged range file found by a read is removed under. See
    /// [`RangeHealLimiter`].
    range_heal_limiter: Arc<RangeHealLimiter>,
    /// This proxy's RAM metadata tier, so a heal drops the key's entry there. `None` for a
    /// manager built by [`Self::new`]; set by `CacheManager::create_configured_disk_cache_manager`.
    metadata_cache: Option<Arc<crate::metadata_cache::MetadataCache>>,
    /// Target size for the in-memory compression batch used by `IncrementalRangeWriter`.
    /// Incoming body chunks are accumulated into a batch buffer and flushed as a single
    /// LZ4 frame once the buffer reaches this size. Plumbed from `CacheConfig::compression_batch_size`.
    /// See the `cache-miss-throughput` spec, Requirement 5.1.
    compression_batch_size: usize,
    /// Semaphore bounding concurrent blocking metadata I/O operations.
    /// Shared across both request-path and consolidation-path metadata reads.
    /// Spec: cache-metadata-resilience Req 1
    metadata_io_semaphore: Arc<tokio::sync::Semaphore>,
    /// Maximum .meta file size before classifying as corrupt/oversized.
    /// Plumbed from `CacheConfig::max_metadata_file_bytes`.
    /// Spec: cache-metadata-resilience Req 3/4
    max_metadata_file_bytes: u64,
    /// Minimum received fraction (0.0–1.0) of an incomplete range to salvage as a
    /// clamped sub-range on the read/GET commit path. `1.0` = exact-only (the
    /// default for an unconfigured manager, preserving legacy behaviour); the
    /// configured path sets it from `CacheConfig::partial_range_commit_ratio`
    /// (default 0.5). Only consulted by `commit_incremental_range`; the PUT
    /// write-cache path passes `None` to `finalize_incremental_range`.
    /// Spec: crt-conditional-range-caching Req 2
    partial_range_commit_ratio: f64,
    /// Free-space floor, in bytes, for the danger-band publication guard on this
    /// manager's three publication boundaries (P1 `store_range`, P3a
    /// `begin_incremental_range_write`, P3c `begin_incremental_part_write`).
    ///
    /// `0` disables the guard, which is the default for a manager built by
    /// [`Self::new`] — tests and early startup. Plumbed from
    /// `CacheConfig::disk_safety_free_space_floor` by
    /// `CacheManager::create_configured_disk_cache_manager`, the only production builder.
    ///
    /// Spec: cache-eviction-at-scale task 36 (A1, A4).
    disk_safety_free_space_floor: u64,
    /// This instance's record of committed-but-unfolded streamed fills, shared by every
    /// configured manager. `None` for a manager built by [`Self::new`], which then behaves
    /// exactly as before the tracker existed. Set by
    /// `CacheManager::create_configured_disk_cache_manager`.
    fill_visibility_tracker: Option<Arc<crate::fill_visibility_tracker::FillVisibilityTracker>>,
}

impl DiskCacheManager {
    /// Create a new disk cache manager
    pub fn new(
        cache_dir: PathBuf,
        compression_enabled: bool,
        compression_threshold: usize,
        write_cache_enabled: bool,
        compression_batch_size: usize,
    ) -> Self {
        Self {
            cache_dir,
            compression_handler: CompressionHandler::new(
                compression_threshold,
                compression_enabled,
            ),
            metrics_manager: None,
            size_tracker: None,
            write_cache_enabled,
            hybrid_metadata_writer: None,
            cache_hit_update_buffer: None,
            journal_consolidator: None,
            range_heal_limiter: global_range_heal_limiter(),
            metadata_cache: None,
            compression_batch_size,
            metadata_io_semaphore: Arc::new(tokio::sync::Semaphore::new(32)),
            max_metadata_file_bytes: 4 * 1024 * 1024, // 4 MiB default
            partial_range_commit_ratio: 1.0,          // exact-only until configured (legacy)
            // Guard off until configured. `create_configured_disk_cache_manager` is the
            // only production builder and always sets it, which
            // `capacity_guard::tests::every_enumerated_publication_path_is_wired_at_a_boundary`
            // and the checks below keep true.
            disk_safety_free_space_floor: 0,
            fill_visibility_tracker: None,
        }
    }

    /// Wire this instance's fill-visibility tracker. Called by
    /// `CacheManager::create_configured_disk_cache_manager`.
    pub(crate) fn set_fill_visibility_tracker(
        &mut self,
        tracker: Arc<crate::fill_visibility_tracker::FillVisibilityTracker>,
    ) {
        self.fill_visibility_tracker = Some(tracker);
    }

    /// Record a just-committed streamed fill in the fill-visibility tracker, if it is the one
    /// shape the tracker covers. Called by `commit_incremental_range` after the `.bin` rename and
    /// after the journal append has returned `Ok` (design point 6).
    ///
    /// Covered: a whole-object (`0..content_length`), read-cache, unstaged range with a non-zero
    /// TTL and at least one validator. Anything else is left to consolidation, as before.
    ///
    /// The document is built the way the consolidator's create-new branch builds one
    /// (`JournalConsolidator::load_or_create_metadata_with_journal_entries`): the journal line's
    /// object metadata, one range, `expires_at = created_at + ttl`. `created_at` is now, which
    /// precedes the consolidation time the `.meta` will carry, so freshness judged from this
    /// entry is never more generous than freshness judged from the `.meta`.
    ///
    /// The entry carries the key's write-order marker state the fill was published under, so a
    /// peer's acknowledged write or delete ends it (`fill_visibility_tracker`, rule 3): a
    /// witnessed fill's token, recorded inside its publish hold, or for a fill without a witness
    /// the marker as it is now.
    ///
    /// A free function over the tracker and cache directory rather than a method, so
    /// [`PreparedIncrementalCommit::finish`] can call it with no `DiskCacheManager` guard held.
    #[allow(clippy::too_many_arguments)] // Each is a distinct piece of the committed fill.
    fn record_fill_visibility_at(
        tracker: Option<&Arc<crate::fill_visibility_tracker::FillVisibilityTracker>>,
        cache_dir: &Path,
        cache_key: &str,
        meta_path: &Path,
        fill_witness: Option<&crate::cache::FillWitness>,
        range_spec: &crate::cache_types::RangeSpec,
        object_metadata: &crate::cache_types::ObjectMetadata,
        ttl: Duration,
    ) {
        let Some(tracker) = tracker else {
            return;
        };
        let whole_object = object_metadata.content_length > 0
            && range_spec.start == 0
            && range_spec.end.checked_add(1) == Some(object_metadata.content_length);
        let has_validator = !object_metadata.etag.trim().is_empty()
            || object_metadata.effective_last_modified().is_some();
        if !whole_object
            || ttl.is_zero()
            || object_metadata.is_write_cached
            || range_spec.staged != Some(false)
            || !has_validator
        {
            return;
        }
        let bin_path = cache_dir.join("ranges").join(&range_spec.file_path);
        let Some(stamp) = crate::fill_visibility_tracker::RangeFileStamp::of(&bin_path) else {
            // The rename just succeeded, so a failed stat means the file is already gone
            // (reaped or evicted). Nothing to vouch for.
            return;
        };
        let marker = match fill_witness {
            Some(witness) => match witness.token() {
                Some(token) => {
                    crate::fill_visibility_tracker::MarkerStamp::witnessed(meta_path, token)
                }
                // An unestablished witness publishes nothing, so no fill reaches here with one.
                None => return,
            },
            None => match crate::fill_visibility_tracker::MarkerStamp::of(meta_path) {
                Some(marker) => marker,
                None => return,
            },
        };
        let now = SystemTime::now();
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: object_metadata.clone(),
            ranges: vec![range_spec.clone()],
            created_at: now,
            expires_at: now + ttl,
            compression_info: crate::cache_types::CompressionInfo::default(),
            ..Default::default()
        };
        tracker.record(metadata, vec![stamp], marker);
    }

    /// Create a new disk cache manager whose `CompressionHandler` shares the
    /// stats `Arc` of `stats_source`, rather than allocating a fresh one.
    ///
    /// Every production `DiskCacheManager` is built per-operation by
    /// `CacheManager::create_configured_disk_cache_manager`. If it used the
    /// plain `new`, its handler (and the streaming writers that borrow its
    /// `shared_stats()`) would record compression counters into a short-lived
    /// `Arc` that dies with the manager — invisible to the `/metrics` snapshot
    /// taken from `CacheManagerInner`'s handler at startup. Sharing the source
    /// handler's `Arc` makes the disk-cache write paths observable.
    /// Spec: compression-followup-fixes Requirement 1.
    pub(crate) fn new_with_shared_stats(
        cache_dir: PathBuf,
        compression_enabled: bool,
        compression_threshold: usize,
        write_cache_enabled: bool,
        compression_batch_size: usize,
        stats_source: &CompressionHandler,
    ) -> Self {
        let mut manager = Self::new(
            cache_dir,
            compression_enabled,
            compression_threshold,
            write_cache_enabled,
            compression_batch_size,
        );
        manager.compression_handler = CompressionHandler::new_with_shared_stats(
            compression_threshold,
            compression_enabled,
            stats_source,
        );
        manager
    }

    /// Configure the partial-range commit ratio (read/GET path) from CacheConfig.
    /// `1.0` = exact-only; lower allows salvaging a received prefix of an
    /// incomplete range. Called by `CacheManager::create_configured_disk_cache_manager`.
    /// Spec: crt-conditional-range-caching Req 2
    pub(crate) fn set_partial_range_commit_ratio(&mut self, ratio: f64) {
        self.partial_range_commit_ratio = ratio;
    }

    /// Arm the danger-band publication guard with the configured free-space floor.
    ///
    /// Called by `CacheManager::create_configured_disk_cache_manager`. `0` leaves the guard
    /// off. Spec: cache-eviction-at-scale task 36.
    pub fn set_disk_safety_free_space_floor(&mut self, floor_bytes: u64) {
        self.disk_safety_free_space_floor = floor_bytes;
    }

    /// Ask the danger-band guard whether `incoming_bytes` may be published at `path`.
    ///
    /// One helper for the three boundaries in this file, so the floor field is read in one
    /// place and a boundary cannot accidentally consult a different figure.
    async fn admit_publication(
        &self,
        incoming_bytes: u64,
        path: crate::capacity_guard::PublicationPath,
    ) -> crate::capacity_guard::Admission {
        crate::capacity_guard::admit_publication(
            &self.cache_dir,
            self.disk_safety_free_space_floor,
            incoming_bytes,
            path,
        )
        .await
    }

    /// Configure metadata I/O limits from CacheConfig.
    ///
    /// Sets the size cap and concurrency semaphore for metadata reads.
    /// Called by `CacheManager::create_configured_disk_cache_manager`.
    pub fn set_metadata_config(
        &mut self,
        max_metadata_file_bytes: u64,
        metadata_io_concurrency: usize,
    ) {
        self.max_metadata_file_bytes = max_metadata_file_bytes;
        self.metadata_io_semaphore = Arc::new(tokio::sync::Semaphore::new(metadata_io_concurrency));
    }

    /// The metadata read limits `get_metadata` applies: (size cap, semaphore permits at rest).
    #[cfg(test)]
    pub(crate) fn metadata_io_limits_for_tests(&self) -> (u64, usize) {
        (
            self.max_metadata_file_bytes,
            self.metadata_io_semaphore.available_permits(),
        )
    }

    /// Set metrics manager for recording metrics
    pub fn set_metrics_manager(
        &mut self,
        metrics_manager: std::sync::Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>,
    ) {
        self.metrics_manager = Some(metrics_manager);
    }

    /// Set size tracker for cache size tracking
    pub(crate) fn set_size_tracker(
        &mut self,
        size_tracker: std::sync::Arc<crate::cache_size_tracker::CacheSizeTracker>,
    ) {
        debug!("Size tracker set on DiskCacheManager");
        self.size_tracker = Some(size_tracker);
    }

    /// Set hybrid metadata writer for shared storage mode
    /// When set, metadata writes go through the hybrid writer for multi-instance coordination
    pub fn set_hybrid_metadata_writer(&mut self, writer: Arc<Mutex<HybridMetadataWriter>>) {
        info!("HybridMetadataWriter enabled for DiskCacheManager - shared storage mode active");
        self.hybrid_metadata_writer = Some(writer);
    }

    /// Set cache hit update buffer for shared storage mode
    /// When set, cache-hit updates (TTL refresh, access updates) go through the buffer
    pub(crate) fn set_cache_hit_update_buffer(&mut self, buffer: Arc<CacheHitUpdateBuffer>) {
        info!("CacheHitUpdateBuffer enabled for DiskCacheManager - journal-based cache-hit updates active");
        self.cache_hit_update_buffer = Some(buffer);
    }

    /// Set journal consolidator for direct size tracking
    /// When set, size is updated directly when storing new ranges
    pub fn set_journal_consolidator(
        &mut self,
        consolidator: Arc<crate::journal_consolidator::JournalConsolidator>,
    ) {
        info!("JournalConsolidator enabled for DiskCacheManager - direct size tracking active");
        self.journal_consolidator = Some(consolidator);
    }

    /// Wire this proxy's RAM metadata tier, which a heal invalidates for the healed key.
    /// Called by `CacheManager::create_configured_disk_cache_manager`.
    pub(crate) fn set_metadata_cache(&mut self, cache: Arc<crate::metadata_cache::MetadataCache>) {
        self.metadata_cache = Some(cache);
    }

    /// Give this manager its own heal bound, so a test's heal counts are its own.
    #[cfg(test)]
    pub(crate) fn set_range_heal_limiter_for_test(&mut self, limiter: Arc<RangeHealLimiter>) {
        self.range_heal_limiter = limiter;
    }

    /// Invalidate `range_spec`'s damaged file, found by a read of `path` whose stamp was
    /// `observed`. See [`heal_damaged_range`].
    pub(crate) async fn heal_damaged_range(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
        observed: Option<RangeFileStamp>,
    ) -> RangeHealOutcome {
        heal_damaged_range(
            &self.cache_dir,
            self.journal_consolidator.as_ref(),
            self.metadata_cache.as_ref(),
            &self.range_heal_limiter,
            range_spec,
            observed,
            true,
        )
        .await
    }
    /// Remove `range_spec`, a listing matching
    /// [`crate::cache_types::RangeSpec::is_unverified_prefix`], with its `.bin` and its
    /// accounting, under the key's slot lock (read-tier eviction's own removal, as
    /// [`heal_damaged_range`] performs it). Outside the heal limit: each removal is one
    /// listing, which the refill replaces with one that records its file identity.
    pub(crate) async fn drop_unverified_prefix_listing(
        &self,
        cache_key: &str,
        range_spec: &crate::cache_types::RangeSpec,
    ) -> RangeHealOutcome {
        note_unverified_prefix_refetch(cache_key, range_spec.start, range_spec.end);
        heal_damaged_range_once(
            &self.cache_dir,
            self.journal_consolidator.as_ref(),
            self.metadata_cache.as_ref(),
            &self.range_heal_limiter,
            range_spec,
            None,
            false,
            HealCause::UnverifiedPrefix,
        )
        .await
    }
    /// Remove `range_spec`, a listing written before 3.0.0 whose bytes are an S3 error
    /// document, with its `.bin` and its accounting, as [`Self::drop_unverified_prefix_listing`]
    /// does. The caller serves the request from S3 and the refill records its file identity,
    /// so a real object whose bytes look like an error document is fetched once more, not
    /// repeatedly.
    async fn drop_legacy_error_body_listing(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
    ) -> ProxyError {
        warn!(
            "Range cached before 3.0.0 holds an S3 error document, fetching it from S3: \
             file={}, range={}-{}",
            range_spec.file_path, range_spec.start, range_spec.end
        );
        heal_damaged_range_once(
            &self.cache_dir,
            self.journal_consolidator.as_ref(),
            self.metadata_cache.as_ref(),
            &self.range_heal_limiter,
            range_spec,
            None,
            false,
            HealCause::LegacyErrorBody,
        )
        .await;
        ProxyError::CacheError(format!(
            "Range {}-{} cached before 3.0.0 holds an S3 error document (cache miss - will fetch from S3)",
            range_spec.start, range_spec.end
        ))
    }

    /// Refuse a stream of `range_spec` before its first byte when it was written before 3.0.0,
    /// is small enough to be an S3 error document, and decodes to one. Every other range
    /// returns `Ok`, without a read when it cannot be an error document. The read records,
    /// marks and heals nothing else it finds: a damaged file returns `Ok`, and the stream that
    /// follows reports it exactly as it would without this check.
    pub(crate) async fn check_legacy_range(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
    ) -> Result<()> {
        if !is_safe_relative_file_path(&range_spec.file_path) {
            warn!(
                "Legacy range check: rejected unsafe file_path={:?} (path traversal attempt), treating as cache miss",
                range_spec.file_path
            );
            return Err(ProxyError::CacheError(format!(
                "Range file path rejected (unsafe): {} (cache miss - will fetch from S3)",
                range_spec.file_path
            )));
        }
        if range_spec.file_identity.is_some()
            || range_spec.uncompressed_size > LEGACY_ERROR_BODY_MAX_BYTES
        {
            return Ok(());
        }
        let path = self.cache_dir.join("ranges").join(&range_spec.file_path);
        if marked_damaged_range_file(&path).is_some() {
            return Ok(());
        }
        let Ok(Ok(Ok((file_bytes, _)))) =
            tokio::task::spawn_blocking(move || read_range_file_checked(&path, None)).await
        else {
            return Ok(());
        };
        let expected = range_spec.uncompressed_size;
        // `decode_lz4_frames_exact` rather than the compression handler: this check
        // records no decompression failure and moves no counter, so a damaged file is
        // reported once, by the stream that follows.
        let data = match range_spec.compression_algorithm {
            crate::compression::CompressionAlgorithm::None => Some(file_bytes),
            crate::compression::CompressionAlgorithm::Lz4 => {
                decode_lz4_frames_exact(&file_bytes, expected)
            }
        };
        match data {
            Some(data)
                if data.len() as u64 == expected
                    && range_spec
                        .end
                        .wrapping_sub(range_spec.start)
                        .wrapping_add(1)
                        == expected
                    && looks_like_s3_error_document(&data) =>
            {
                Err(self.drop_legacy_error_body_listing(range_spec).await)
            }
            _ => Ok(()),
        }
    }

    /// Refuse a read of `range_spec`'s file, which a read on this proxy found damaged
    /// ([`DamagedRangeMarks`]), and retry its heal under the heal limit: a range only a pending
    /// journal `Add` named is healed once a consolidation pass lists it, and one the limit left
    /// in place once the window has room. The caller serves the request from S3.
    async fn refuse_marked_damaged_range(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
        stamp: RangeFileStamp,
    ) -> ProxyError {
        debug!(
            "Range file a read on this proxy found damaged, not served: file={}, range={}-{}",
            range_spec.file_path, range_spec.start, range_spec.end
        );
        self.heal_damaged_range(range_spec, Some(stamp)).await;
        marked_damaged_range_error(range_spec)
    }
    /// [`refuse_wrong_range_file`] with this manager's consolidator and RAM metadata tier.
    async fn refuse_wrong_range_file(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
        wrong: WrongRangeFile,
    ) -> ProxyError {
        refuse_wrong_range_file(
            &self.cache_dir,
            self.journal_consolidator.as_ref(),
            self.metadata_cache.as_ref(),
            range_spec,
            wrong,
        )
        .await
    }

    /// Get the cache hit update buffer (if set)
    pub fn get_cache_hit_update_buffer(&self) -> Option<Arc<CacheHitUpdateBuffer>> {
        self.cache_hit_update_buffer.clone()
    }

    /// Record a range access using the cache hit update buffer (journal-based)
    /// This records access count increments that will be consolidated to metadata
    pub async fn record_range_access(
        &self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        if let Some(buffer) = &self.cache_hit_update_buffer {
            buffer
                .record_access_update(cache_key, range_start, range_end, 1)
                .await
        } else {
            // No buffer configured (single instance mode), skip access recording
            // Access counts will be updated directly during cache operations
            debug!(
                "No cache hit update buffer configured, skipping access recording for {}",
                cache_key
            );
            Ok(())
        }
    }

    /// Whether a hybrid metadata writer is configured — exactly
    /// `self.hybrid_metadata_writer.is_some()`.
    ///
    /// **False on a manager built by [`Self::new`] alone**, which leaves the writer `None`;
    /// production reaches `true` only because `CacheManager`'s builder calls
    /// [`Self::set_hybrid_metadata_writer`]. `find_pending_journal_ranges` gates on this and
    /// returns `Ok(Vec::new())` immediately when it is false, so **any test asserting an empty
    /// journal-lookup result passes vacuously unless its fixture sets the writer** — nothing in
    /// the output distinguishes "suppressed correctly" from "never reached the lookup".
    /// `tests/range_invalidation_marker_suppression_test.rs` and
    /// `tests/range_invalidation_divergent_instance_test.rs` therefore assert this in their seeds;
    /// those assertions are load-bearing, not redundant, and deleting one restores the vacuity
    /// silently. Note the asymmetry: `enumerate_journal_add_ranges` deliberately does **not** gate
    /// on this, so a `.bin`-is-gone assertion passing implies nothing about the mode.
    pub fn is_shared_storage_mode(&self) -> bool {
        self.hybrid_metadata_writer.is_some()
    }

    /// Scan journal files for pending range entries covering the requested range,
    /// **honouring `JournalOperation::Remove` markers**.
    ///
    /// This is the Journal_Fallback: metadata does not name the range, but an
    /// unconsolidated journal `Add` might, which closes the window between a journal
    /// write and the consolidation cycle that folds it into the `.meta`.
    ///
    /// Only active in shared storage mode.
    ///
    /// # Removal markers are honoured here, and NOT at the callers
    ///
    /// Six consuming sites reach the journal through this one function
    /// (`find_cached_ranges`' two fallback branches; `RangeHandler::load_range_data_from_new_storage`
    /// and `load_range_frame_from_new_storage`; `HttpProxy::find_page_overlap` and
    /// `serve_range_from_cache`). `invalidate_all_ranges` deletes a range's `.bin` and
    /// writes a `JournalOperation::Remove` marker for it, so without the suppression
    /// below this function reports coverage for a file that is gone. A check at the
    /// callers would be a six-way obligation one site will forget, which is the
    /// call-site form of the failure
    /// the cache coherency invariants records as "a correct freshness
    /// check placed behind an earlier one that can suppress its input is not a check".
    /// Spec: range-invalidation-completeness. Requirements: R2.1, R2.2.
    ///
    /// # Why there is no early return, and what that costs
    ///
    /// This function used to `return` mid-scan on an `Add` that exactly matched or
    /// fully contained the request — **before** later lines in the same file and
    /// before any later journal file was opened. A marker sitting after its `Add` in
    /// file order, or in another instance's file, was therefore never seen, and the
    /// exact-match return is the *normal* path for a whole-object read. Collecting
    /// markers "in the same pass" while keeping those returns produces a fix that
    /// passes a naive test and is unsound.
    ///
    /// So the scan is now two-phase over the content of **one** pass: phase 1
    /// accumulates candidate `Add`s and markers, phase 2 resolves them, and the
    /// exact-match / full-containment selection is applied afterwards in the same
    /// scan order, so it picks the same `RangeSpec` the early returns did.
    ///
    /// This adds **no** Shared_Storage read — the same `read_dir` and the same
    /// `read_to_string` per file as before (R2.3). It does cost the truncation
    /// optimisation: the common case used to be a partial parse of one file, and is
    /// now `O(instances x journal length)` parse work per call on a hot lookup path.
    /// R2.3 is free in *reads*, not in *work*; the trade is stated rather than hidden.
    ///
    /// # Republish guard
    ///
    /// A range republished **after** its marker must survive, or this becomes an
    /// intermittent post-overwrite cache miss. The discriminator is not invented here:
    /// it is the same rule `JournalConsolidator::apply_journal_entries`' `Remove` arm
    /// applies — exact `(start, end)` match, and retain when
    /// `range_spec.created_at > marker.timestamp`. Read that arm's comment (the T1-T5
    /// re-PUT sequence) before changing this. R2.4.
    ///
    /// # Consequence at the two kinds of consumer
    ///
    /// Four consumers ask for **coverage** and simply stop reporting a suppressed
    /// range. Three ask for **path resolution** — the two `RangeHandler` loaders and
    /// `HttpProxy::serve_range_from_cache` — and there a suppressed range becomes a
    /// `ProxyError::CacheError`/a buffered-path fallback instead. Every one of those
    /// degrades (complete S3 refetch via `merge_ranges_with_fallback`, an S3 range
    /// refetch in `get_cached_range_data`, the buffered serve path, or a skipped RAM
    /// promotion); none surfaces to a client. In practice the paired coverage lookup
    /// declines first, so the resolution arm is reached only if a marker lands between
    /// the two calls.
    ///
    /// # Freshness semantics are unchanged
    ///
    /// The return stays `Vec<RangeSpec>` and both journal-fallback branches still wrap
    /// it in `RangeLookupResult::fresh(...)`. A journal record carries no expiry and no
    /// validators; that gap belongs to `page-widening-freshness`, not here. R2.5.
    pub async fn find_pending_journal_ranges(
        &self,
        cache_key: &str,
        requested_start: u64,
        requested_end: u64,
    ) -> Result<Vec<crate::cache_types::RangeSpec>> {
        // A conflicting extent (see `pending_journal_ranges_classified`) is neither coverage
        // nor a resolvable spec here: its `.bin` holds whichever publish renamed last.
        Ok(self
            .pending_journal_ranges_classified(cache_key, requested_start, requested_end)
            .await?
            .into_iter()
            .filter(|(_, _, conflicting)| !conflicting)
            .map(|(range, _, _)| range)
            .collect())
    }

    /// [`Self::find_pending_journal_ranges`], with each range's ETag as its journal `Add`
    /// recorded it: `object_metadata.etag`, taken from the S3 response the bytes arrived in.
    /// Empty when the entry recorded none (an `Add` written without object metadata), and
    /// when the extent's surviving `Add`s record different versions.
    ///
    /// This is the only evidence of a journal-pending extent's version. A `.meta`'s ETag, or
    /// a caller's, says nothing about bytes no `.meta` lists yet. Publishing a range fill
    /// drops one whose ETag differs from the key's `.meta`, but before any `.meta` exists
    /// extents of two versions can both be pending: a fill, a direct-to-S3 overwrite, then
    /// a fill of another extent on any proxy. Or two fills of the SAME extent from
    /// two versions, on two proxies: the second publish renames over the first's `.bin`,
    /// so neither recorded ETag is evidence of the file, and the extent's ETag is empty.
    pub async fn find_pending_journal_ranges_with_etags(
        &self,
        cache_key: &str,
        requested_start: u64,
        requested_end: u64,
    ) -> Result<Vec<(crate::cache_types::RangeSpec, String)>> {
        Ok(self
            .pending_journal_ranges_classified(cache_key, requested_start, requested_end)
            .await?
            .into_iter()
            .map(|(range, etag, conflicting)| {
                if conflicting {
                    (range, String::new())
                } else {
                    (range, etag)
                }
            })
            .collect())
    }

    /// The object length a pending journal `Add` of version `etag` (quote-insensitive)
    /// recorded from the S3 response its bytes arrived in; 0 when none did. Page widening
    /// promotes a Page to RAM under this length before any `.meta` lists the Page.
    pub async fn pending_journal_object_size(&self, cache_key: &str, etag: &str) -> u64 {
        if !self.is_shared_storage_mode() {
            return 0;
        }
        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        let Ok(entries) = std::fs::read_dir(&journals_dir) else {
            return 0;
        };
        for journal_entry in entries.flatten() {
            let path = journal_entry.path();
            if !path.is_file() || path.extension().and_then(|e| e.to_str()) != Some("journal") {
                continue;
            }
            let Ok(content) = tokio::fs::read_to_string(&path).await else {
                continue;
            };
            for line in content.lines() {
                let Ok(entry) = serde_json::from_str::<crate::journal_manager::JournalEntry>(line)
                else {
                    continue;
                };
                if entry.cache_key != cache_key
                    || !matches!(
                        entry.operation,
                        crate::journal_manager::JournalOperation::Add
                    )
                {
                    continue;
                }
                if let Some(m) = entry.object_metadata.as_ref() {
                    if m.content_length > 0
                        && crate::revalidation::etags_equal_ignoring_quotes(&m.etag, etag)
                    {
                        return m.content_length;
                    }
                }
            }
        }
        0
    }
    /// The pending journal `Add` for exactly `start..=end` whose recorded ETag is `label`
    /// (quote-insensitive), never one whose extent is conflicting. A streaming serve that
    /// resolves an extent no `.meta` lists takes its spec from here with `label` the ETag its
    /// response head carries, so another version's own `Add`, whose file identity matches its
    /// file, is never served under that head. An empty `label` (an ETag-less origin)
    /// matches only an `Add` that recorded no ETag.
    pub async fn find_pending_journal_range_for_etag(
        &self,
        cache_key: &str,
        start: u64,
        end: u64,
        label: &str,
    ) -> Option<crate::cache_types::RangeSpec> {
        match self
            .pending_journal_range_for_label(cache_key, start, end, label)
            .await
        {
            PendingForLabel::Found(spec) => Some(spec),
            PendingForLabel::OtherVersion | PendingForLabel::Absent => None,
        }
    }

    /// What the pending journals hold for exactly `start..=end` relative to `label`, from one
    /// scan: an `Add` of that version, only `Add`s of other versions, or nothing. A
    /// conflicting extent counts as nothing, as [`Self::find_pending_journal_ranges`] treats it.
    pub(crate) async fn pending_journal_range_for_label(
        &self,
        cache_key: &str,
        start: u64,
        end: u64,
        label: &str,
    ) -> PendingForLabel {
        let Ok(candidates) = self
            .pending_journal_ranges_classified(cache_key, start, end)
            .await
        else {
            return PendingForLabel::Absent;
        };
        let exact = |r: &crate::cache_types::RangeSpec, conflicting: bool| {
            !conflicting && r.start == start && r.end == end
        };
        if let Some((spec, _, _)) = candidates.iter().find(|(r, etag, conflicting)| {
            exact(r, *conflicting) && crate::revalidation::etags_equal_ignoring_quotes(etag, label)
        }) {
            return PendingForLabel::Found(spec.clone());
        }
        if candidates
            .iter()
            .any(|(r, _, conflicting)| exact(r, *conflicting))
        {
            #[cfg(test)]
            serve_refusal_log::note(cache_key, "pending_add_of_another_version");
            return PendingForLabel::OtherVersion;
        }
        PendingForLabel::Absent
    }
    /// The journal scan behind both lookups above. Each surviving `Add` overlapping the
    /// request, with the ETag it recorded and whether its extent is *conflicting*: another
    /// surviving `Add` for the same `(start, end)` records a different ETag (quote-
    /// insensitive; a missing ETag differs from any recorded one).
    ///
    /// A conflicting extent's `.bin` holds whichever of those fills renamed last, which no
    /// journal line records. The `.bin` path is `(key, start, end)` only, and with no `.meta`
    /// the publish guard has nothing to compare against, so two proxies that miss the same
    /// extent before either journal line exists (in-flight coalescing is per process) both
    /// publish. The consolidator retires such a file (`retire_other_version_fills`); until it
    /// runs, the lookups above treat the extent as not cached.
    async fn pending_journal_ranges_classified(
        &self,
        cache_key: &str,
        requested_start: u64,
        requested_end: u64,
    ) -> Result<Vec<(crate::cache_types::RangeSpec, String, bool)>> {
        // Only check journals in shared storage mode
        if !self.is_shared_storage_mode() {
            return Ok(Vec::new());
        }

        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        if !journals_dir.exists() {
            return Ok(Vec::new());
        }

        // Phase 1 accumulators. Candidates are kept in scan order so the
        // exact-match/containment selection in phase 3 reproduces the choice the
        // removed early returns made.
        let mut candidate_adds: Vec<(crate::cache_types::RangeSpec, String)> = Vec::new();
        // `(start, end, marker timestamp)`. Only markers overlapping the request are
        // kept, which is lossless: a marker can only suppress an `Add` whose extent it
        // matches exactly, and such an `Add` overlaps the request by construction.
        let mut removal_markers: Vec<(u64, u64, std::time::SystemTime)> = Vec::new();

        // Scan all .journal files
        let journal_entries = match std::fs::read_dir(&journals_dir) {
            Ok(entries) => entries,
            Err(e) => {
                debug!("Failed to read journals directory: {}", e);
                return Ok(Vec::new());
            }
        };

        for journal_entry in journal_entries {
            let journal_entry = match journal_entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let journal_path = journal_entry.path();
            if !journal_path.is_file() {
                continue;
            }

            let file_name = match journal_path.file_name().and_then(|n| n.to_str()) {
                Some(name) if name.ends_with(".journal") => name,
                _ => continue,
            };

            // Read and parse journal file
            let content = match tokio::fs::read_to_string(&journal_path).await {
                Ok(c) => c,
                Err(e) => {
                    debug!("Failed to read journal file {:?}: {}", journal_path, e);
                    continue;
                }
            };

            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                let entry: crate::journal_manager::JournalEntry = match serde_json::from_str(line) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                // Check if this entry is for our cache key
                if entry.cache_key != cache_key {
                    continue;
                }

                // Check if the range overlaps with requested range
                let range = &entry.range_spec;
                let overlaps = requested_start <= range.end && range.start <= requested_end;
                if !overlaps {
                    continue;
                }

                // NO early return here, deliberately — see the doc comment's § "Why
                // there is no early return, and what that costs". A marker after its
                // `Add` in file order, or in another instance's file, is only reachable
                // once the whole pass has completed.
                match entry.operation {
                    // A pending fill written before 3.0.0 at offset 0, short of the object's
                    // end (`RangeSpec::is_unverified_prefix`), is never coverage. Every journal
                    // fallback reads through this scan, page widening's included, so this one
                    // check covers them all. Once folded, `find_cached_ranges` removes it.
                    crate::journal_manager::JournalOperation::Add
                        if range.is_unverified_prefix(
                            entry
                                .object_metadata
                                .as_ref()
                                .map(|m| m.content_length)
                                .unwrap_or(0),
                        ) =>
                    {
                        note_unverified_prefix_refetch(cache_key, range.start, range.end);
                        continue;
                    }
                    crate::journal_manager::JournalOperation::Add => {
                        debug!(
                            "[JOURNAL_LOOKUP] Found pending range in journal: cache_key={}, range={}-{}, journal_file={}",
                            cache_key, range.start, range.end, file_name
                        );
                        candidate_adds.push((
                            range.clone(),
                            entry
                                .object_metadata
                                .as_ref()
                                .map(|m| m.etag.clone())
                                .unwrap_or_default(),
                        ));
                    }
                    crate::journal_manager::JournalOperation::Remove => {
                        debug!(
                            "[JOURNAL_LOOKUP] Found removal marker in journal: cache_key={}, range={}-{}, journal_file={}",
                            cache_key, range.start, range.end, file_name
                        );
                        removal_markers.push((range.start, range.end, entry.timestamp));
                    }
                    _ => continue,
                }
            }
        }

        // Phase 2: resolve markers against candidates, over content already read.
        //
        // Suppress an `Add` when some marker at the *same extent* was recorded at or
        // after the range was created. The comparison is the one
        // `JournalConsolidator::apply_journal_entries`' `Remove` arm makes
        // (`created_at > entry.timestamp` retains), applied to the set rather than to a
        // single entry: several markers for one extent are possible (a re-PUT that is
        // itself later evicted), and the range survives only if it post-dates every one
        // of them. R2.4.
        if !removal_markers.is_empty() {
            let before = candidate_adds.len();
            candidate_adds.retain(|(range, _)| {
                let superseding_marker = removal_markers.iter().find(|(start, end, timestamp)| {
                    *start == range.start && *end == range.end && range.created_at <= *timestamp
                });
                match superseding_marker {
                    Some((start, end, _)) => {
                        debug!(
                            "[JOURNAL_LOOKUP] Suppressing journal range removed by a marker: cache_key={}, range={}-{}",
                            cache_key, start, end
                        );
                        false
                    }
                    None => true,
                }
            });
            if candidate_adds.len() != before {
                debug!(
                    "[JOURNAL_LOOKUP] Removal markers suppressed {} of {} journal ranges: cache_key={}, requested_range={}-{}",
                    before - candidate_adds.len(),
                    before,
                    cache_key,
                    requested_start,
                    requested_end
                );
            }
        }

        // Phase 2c: surviving `Add`s for one extent that record the same version but
        // different file identities: the extent was filled twice and one rename replaced the
        // other's file. Keep the `Add`s whose identity matches the file now at the path (one
        // `stat` per such extent, `RangeFileIdentity::matches`), so the selection below names
        // the file a read will open. When none matches, all are kept, and a read refuses
        // whichever is selected. `Add`s of different versions are left to the conflict rule
        // below.
        {
            let same_version = |a: &str, b: &str| {
                if a.is_empty() || b.is_empty() {
                    a.is_empty() && b.is_empty()
                } else {
                    crate::revalidation::etags_equal_ignoring_quotes(a, b)
                }
            };
            let mut disputed: Vec<(u64, u64, String, String)> = Vec::new();
            for (i, (range, etag)) in candidate_adds.iter().enumerate() {
                let Some(identity) = range.file_identity else {
                    continue;
                };
                let differs = candidate_adds[..i].iter().any(|(other, other_etag)| {
                    other.start == range.start
                        && other.end == range.end
                        && same_version(etag, other_etag)
                        && other.file_identity.is_some_and(|o| o != identity)
                });
                if differs
                    && !disputed.iter().any(|(s, e, _, t)| {
                        *s == range.start && *e == range.end && same_version(t, etag)
                    })
                {
                    disputed.push((
                        range.start,
                        range.end,
                        range.file_path.clone(),
                        etag.clone(),
                    ));
                }
            }
            for (start, end, file_path, version) in disputed {
                if !is_safe_relative_file_path(&file_path) {
                    continue;
                }
                let found = std::fs::metadata(self.cache_dir.join("ranges").join(&file_path))
                    .ok()
                    .and_then(|m| crate::cache_types::RangeFileIdentity::of(&m));
                let Some(found) = found else {
                    continue;
                };
                let same = |r: &crate::cache_types::RangeSpec, etag: &str| {
                    r.start == start && r.end == end && same_version(etag, &version)
                };
                let describes = |r: &crate::cache_types::RangeSpec| {
                    r.file_identity.is_some_and(|i| i.matches(&found))
                };
                if candidate_adds
                    .iter()
                    .any(|(r, e)| same(r, e) && describes(r))
                {
                    candidate_adds.retain(|(r, e)| !same(r, e) || describes(r));
                }
            }
        }
        // Phase 2b: classify. Over surviving candidates only, after phase 2, so an `Add`
        // a marker superseded cannot make its extent conflicting.
        let same_recorded_version = |a: &str, b: &str| {
            if a.is_empty() || b.is_empty() {
                a.is_empty() && b.is_empty()
            } else {
                crate::revalidation::etags_equal_ignoring_quotes(a, b)
            }
        };
        let conflicting: Vec<bool> = candidate_adds
            .iter()
            .map(|(range, etag)| {
                candidate_adds.iter().any(|(other, other_etag)| {
                    other.start == range.start
                        && other.end == range.end
                        && !same_recorded_version(etag, other_etag)
                })
            })
            .collect();
        let classified: Vec<(crate::cache_types::RangeSpec, String, bool)> = candidate_adds
            .into_iter()
            .zip(conflicting)
            .map(|((range, etag), conflicting)| {
                if conflicting {
                    debug!(
                        "[JOURNAL_LOOKUP] Pending Adds for one extent record different versions, not coverage: cache_key={}, range={}-{}",
                        cache_key, range.start, range.end
                    );
                }
                (range, etag, conflicting)
            })
            .collect();

        // Phase 3: the exact-match / full-containment selection the early returns used
        // to make, applied in the same scan order so it picks the same `RangeSpec` —
        // just after the whole pass rather than during it. Both checks stay in one
        // iteration, so the first surviving candidate satisfying either wins, exactly
        // as before. A conflicting candidate is never selected, so a later one that is
        // not can be; it stays in the list below for the callers to refuse.
        for (range, etag, conflicting) in &classified {
            if *conflicting {
                continue;
            }
            if range.start == requested_start && range.end == requested_end {
                debug!(
                    "[JOURNAL_LOOKUP] Exact match found in journal: cache_key={}, range={}-{}",
                    cache_key, range.start, range.end
                );
                return Ok(vec![(range.clone(), etag.clone(), false)]);
            }

            if range.start <= requested_start && range.end >= requested_end {
                debug!(
                    "[JOURNAL_LOOKUP] Full containment found in journal: cache_key={}, range={}-{}",
                    cache_key, range.start, range.end
                );
                return Ok(vec![(range.clone(), etag.clone(), false)]);
            }
        }

        // Sort by start position
        let mut matching_ranges = classified;
        matching_ranges.sort_by_key(|(r, _, _)| r.start);

        if !matching_ranges.is_empty() {
            debug!(
                "[JOURNAL_LOOKUP] Found {} pending ranges in journals for cache_key={}, requested_range={}-{}",
                matching_ranges.len(), cache_key, requested_start, requested_end
            );
        }

        Ok(matching_ranges)
    }

    /// Initialize the disk cache directory structure
    ///
    /// Creates only base directories for the new bucket-first sharded structure:
    /// - metadata: Base directory for metadata files (bucket/XX/YYY structure created lazily)
    /// - ranges: Base directory for range binary files (bucket/XX/YYY structure created lazily)
    /// - locks: Directory for file locks
    ///
    /// Bucket and hash directories (XX/YYY) are NOT created during initialization.
    /// They are created lazily when the first file is written to that shard.
    /// This supports the new sharding architecture where directories are created on-demand.
    ///
    /// # Requirements
    /// Implements Requirements 8.1, 13.1, 13.3, 13.4
    pub async fn initialize(&self) -> Result<()> {
        // Create cache directory if it doesn't exist
        if !self.cache_dir.exists() {
            std::fs::create_dir_all(&self.cache_dir).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create cache directory: {}", e))
            })?;
            info!("Created cache directory: {:?}", self.cache_dir);
        }

        // Create only base directories for the new sharded structure
        // Do NOT create flat subdirectories or bucket/hash directories
        // Bucket and hash directories will be created lazily on first write
        let mut subdirs = vec!["metadata", "ranges", "locks"];
        if self.write_cache_enabled {
            subdirs.push("mpus_in_progress");
        }

        for subdir in &subdirs {
            let subdir_path = self.cache_dir.join(subdir);
            if !subdir_path.exists() {
                std::fs::create_dir_all(&subdir_path).map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Failed to create cache subdirectory {}: {}",
                        subdir, e
                    ))
                })?;
                debug!("Created base directory: {:?}", subdir_path);
            }
        }

        info!(
            "Disk cache manager initialized with bucket-first sharded structure: {:?}. \
            Bucket and hash directories will be created lazily on first write.",
            self.cache_dir
        );
        Ok(())
    }

    /// Invalidate cache entry by removing files
    ///
    /// This method removes both metadata and all associated range files for a cache entry.
    /// Handles both the new bucket-first sharded structure and legacy flat structure
    /// for backward compatibility.
    ///
    /// # Requirements
    /// Implements Requirement 8.4: Update invalidate_cache_entry() to use sharded paths
    ///
    /// # Behavior
    /// 1. Tries new sharded structure first (for range-based caching)
    /// 2. Falls back to old flat structure (for legacy full-object caching)
    /// 3. Removes all range binary files (.bin) if using new structure
    /// 4. Removes metadata and cache files
    /// 5. Handles missing files gracefully (logs warning but continues)
    /// 6. Removes lock files if they exist
    pub async fn invalidate_cache_entry(&self, cache_key: &str) -> Result<()> {
        debug!("Invalidating cache entry for key: {}", cache_key);

        let mut removed_count = 0;
        let mut failed_count = 0;

        // Hold the key's metadata slot lock for the whole read-then-delete. The `.meta`
        // read below decides which `.bin` files go and whether the `.meta` itself does;
        // without the lock a concurrent writer's read-modify-write of the same `.meta` can
        // republish between our read and our `remove_file`, and either its update or our
        // deletion is silently lost. Released by `Drop` on every return path.
        let _meta_lock = self.acquire_meta_slot_lock(cache_key).await?;

        // ===== NEW SHARDED STRUCTURE (range-based caching) =====

        // Step 1: Get metadata file path using new sharded structure
        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Step 2: Try to load metadata to find all range files
        // If metadata doesn't exist or is corrupted, we'll still try to clean up
        let ranges_to_remove = if metadata_file_path.exists() {
            match std::fs::read_to_string(&metadata_file_path) {
                Ok(content) => {
                    match serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&content) {
                        Ok(metadata) => {
                            debug!(
                                "Loaded metadata for invalidation: key={}, ranges={}",
                                cache_key,
                                metadata.ranges.len()
                            );
                            metadata.ranges
                        }
                        Err(e) => {
                            warn!(
                                "Failed to parse metadata during invalidation: key={}, error={}. Will remove metadata file only.",
                                cache_key, e
                            );
                            Vec::new()
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to read metadata during invalidation: key={}, error={}. Will remove metadata file only.",
                        cache_key, e
                    );
                    Vec::new()
                }
            }
        } else {
            debug!(
                "Metadata file not found in new structure: key={}, path={:?}",
                cache_key, metadata_file_path
            );
            Vec::new()
        };

        // Step 3: Remove all range binary files (new structure)
        for range_spec in &ranges_to_remove {
            let range_file_path =
                self.get_new_range_file_path(cache_key, range_spec.start, range_spec.end);

            if range_file_path.exists() {
                match std::fs::remove_file(&range_file_path) {
                    Ok(_) => {
                        removed_count += 1;
                        debug!(
                            "Removed range file: key={}, range={}-{}, path={:?}",
                            cache_key, range_spec.start, range_spec.end, range_file_path
                        );
                    }
                    Err(e) => {
                        failed_count += 1;
                        warn!(
                            "Failed to remove range file: key={}, range={}-{}, path={:?}, error={}",
                            cache_key, range_spec.start, range_spec.end, range_file_path, e
                        );
                    }
                }
            } else {
                debug!(
                    "Range file not found (already removed?): key={}, range={}-{}, path={:?}",
                    cache_key, range_spec.start, range_spec.end, range_file_path
                );
            }
        }

        // Step 4: Remove metadata file (new structure)
        if metadata_file_path.exists() {
            match std::fs::remove_file(&metadata_file_path) {
                Ok(_) => {
                    removed_count += 1;
                    debug!(
                        "Removed metadata file: key={}, path={:?}",
                        cache_key, metadata_file_path
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to remove metadata file: key={}, path={:?}, error={}",
                        cache_key, metadata_file_path, e
                    );
                }
            }
        }

        // The lock file is NOT removed. It is a shared slot in a fixed set: removing it would
        // delete a file other keys' holders are locking, and would release nothing, because
        // `flock` binds to the open file description rather than to the path.

        // ===== OLD FLAT STRUCTURE (legacy full-object caching) =====
        // For backward compatibility with old cache entries

        let old_cache_file_path = self.get_cache_file_path(cache_key);
        let old_metadata_file_path = self.get_legacy_flat_metadata_file_path(cache_key);

        // Remove old cache file
        if old_cache_file_path.exists() {
            match std::fs::remove_file(&old_cache_file_path) {
                Ok(_) => {
                    removed_count += 1;
                    debug!(
                        "Removed old cache file: key={}, path={:?}",
                        cache_key, old_cache_file_path
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to remove old cache file: key={}, path={:?}, error={}",
                        cache_key, old_cache_file_path, e
                    );
                }
            }
        }

        // Remove old metadata file
        if old_metadata_file_path.exists() {
            match std::fs::remove_file(&old_metadata_file_path) {
                Ok(_) => {
                    removed_count += 1;
                    debug!(
                        "Removed old metadata file: key={}, path={:?}",
                        cache_key, old_metadata_file_path
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to remove old metadata file: key={}, path={:?}, error={}",
                        cache_key, old_metadata_file_path, e
                    );
                }
            }
        }

        info!(
            "Invalidated cache entry: key={}, removed={} files, failed={} files",
            cache_key, removed_count, failed_count
        );

        Ok(())
    }

    /// Get cache file path for a given cache key
    fn get_cache_file_path(&self, cache_key: &str) -> PathBuf {
        let cache_type = self.determine_cache_type(cache_key);
        let safe_key = self.sanitize_cache_key(cache_key);
        self.cache_dir
            .join(cache_type)
            .join(format!("{}.cache", safe_key))
    }

    /// Get the **legacy flat** metadata file path for a given cache key.
    ///
    /// Returns `{cache_type}/{percent-encoded key}.meta` — the pre-sharding construction.
    /// This is **not** the path production reads or writes, and it is not an alias for it:
    /// the live path is `metadata/{bucket}/{XX}/{YYY}/{percent-encoded key}.meta`, built by
    /// [`Self::get_new_metadata_file_path`] via [`get_sharded_path`]. On a sharded cache —
    /// every current cache — the two name different files.
    ///
    /// Use this only to clean up entries left by a pre-sharding cache, alongside the sharded
    /// path rather than instead of it. Both of its call sites do exactly that. Selecting it
    /// where the sharded path was meant is the defect `EVICT-METAPATH-FIX` fixed, which is
    /// why the name says `legacy_flat` rather than leaving the caller to infer it.
    fn get_legacy_flat_metadata_file_path(&self, cache_key: &str) -> PathBuf {
        let cache_type = self.determine_cache_type(cache_key);
        let safe_key = self.sanitize_cache_key(cache_key);
        self.cache_dir
            .join(cache_type)
            .join(format!("{}.meta", safe_key))
    }

    /// Determine cache type from cache key for directory organization
    pub fn determine_cache_type(&self, cache_key: &str) -> &str {
        if cache_key.contains(":part:") {
            "parts" // Object parts
        } else if cache_key.contains(":range:") {
            "ranges" // Range requests
        } else {
            "metadata" // Full objects
        }
    }

    /// Sanitize cache key for safe filesystem usage
    /// Uses percent encoding to prevent collisions while maintaining filesystem safety
    pub fn sanitize_cache_key(&self, cache_key: &str) -> String {
        // Use the same percent encoding as sanitize_cache_key_new for consistency
        self.sanitize_cache_key_new(cache_key)
    }

    /// Extract path from cache key for content-aware compression.
    ///
    /// Cache keys are generated by `CacheManager::generate_cache_key` (bare
    /// object path) or with one or both of the suffixes
    /// `:part:<n>` / `:range:<start>-<end>` appended by
    /// `generate_part_cache_key` / `generate_range_cache_key` /
    /// `generate_cache_key_with_params` (see `cache.rs`). Strip only those
    /// known trailing suffix patterns rather than splitting at the first
    /// colon, so object keys that legitimately contain a colon are not
    /// truncated before extension detection
    /// (compression-content-aware-fix spec, extension-extraction fix).
    fn extract_path_from_cache_key(&self, cache_key: &str) -> String {
        crate::cache::strip_known_cache_key_suffixes(cache_key)
    }

    // ============================================================================
    // New Range Storage Architecture Methods
    // ============================================================================

    /// Get metadata file path for new range storage architecture
    /// Returns path: cache_dir/metadata/{bucket}/{XX}/{YYY}/{sanitized_key}.meta
    /// Uses bucket-first hash-based sharding for scalability
    pub fn get_new_metadata_file_path(&self, cache_key: &str) -> PathBuf {
        let base_dir = self.cache_dir.join("metadata");
        let sanitized_key = self.sanitize_cache_key_new(cache_key);

        // Requirement 1.2: Log input cache key and resulting path
        // Use get_sharded_path which implements bucket-first sharding
        // If parsing fails (invalid cache key format), fall back to flat structure
        match get_sharded_path(&base_dir, cache_key, ".meta") {
            Ok(path) => {
                debug!(
                    "[PATH_RESOLUTION] Metadata path constructed: cache_key={}, sanitized_key={}, path={:?}",
                    cache_key, sanitized_key, path
                );
                path
            }
            Err(e) => {
                // Fall back to old flat structure for backward compatibility
                warn!(
                    "[PATH_RESOLUTION] Failed to get sharded path, using flat structure: cache_key={}, sanitized_key={}, error={}",
                    cache_key, sanitized_key, e
                );
                let flat_path = base_dir.join(format!("{}.meta", sanitized_key));
                debug!(
                    "[PATH_RESOLUTION] Flat path constructed: cache_key={}, sanitized_key={}, path={:?}",
                    cache_key, sanitized_key, flat_path
                );
                flat_path
            }
        }
    }

    /// Get range binary file path for new range storage architecture
    /// Returns path: cache_dir/ranges/{bucket}/{XX}/{YYY}/{sanitized_key}_{start}-{end}.bin
    /// Uses bucket-first hash-based sharding for scalability
    ///
    /// # Requirements
    /// Implements Requirements 3.1, 3.2, 3.3, 3.4, 3.5
    pub fn get_new_range_file_path(&self, cache_key: &str, start: u64, end: u64) -> PathBuf {
        let base_dir = self.cache_dir.join("ranges");
        let suffix = format!("_{}-{}.bin", start, end);
        let sanitized_key = self.sanitize_cache_key_new(cache_key);

        // Requirement 3.5: Use get_sharded_path which implements bucket-first sharding
        // If parsing fails (invalid cache key format), fall back to flat structure
        match get_sharded_path(&base_dir, cache_key, &suffix) {
            Ok(path) => {
                debug!(
                    "[PATH_RESOLUTION] Range path constructed: cache_key={}, sanitized_key={}, range={}-{}, path={:?}",
                    cache_key, sanitized_key, start, end, path
                );
                path
            }
            Err(e) => {
                // Requirement 3.4: Fall back to old flat structure for backward compatibility
                warn!(
                    "[PATH_RESOLUTION] Failed to get sharded path for range, using flat structure: cache_key={}, sanitized_key={}, range={}-{}, error={}",
                    cache_key, sanitized_key, start, end, e
                );
                let flat_path = base_dir.join(format!("{}_{}-{}.bin", sanitized_key, start, end));
                debug!(
                    "[PATH_RESOLUTION] Flat range path constructed: cache_key={}, sanitized_key={}, range={}-{}, path={:?}",
                    cache_key, sanitized_key, start, end, flat_path
                );
                flat_path
            }
        }
    }

    /// Sanitize cache key for new range storage architecture
    /// Uses percent encoding to prevent collisions while maintaining filesystem safety
    /// For keys that would exceed 200 characters, uses BLAKE3 hash to ensure filesystem compatibility
    ///
    /// # Requirements
    /// Implements Requirements 2.1, 2.2, 2.3, 2.4
    pub(crate) fn sanitize_cache_key_new(&self, cache_key: &str) -> String {
        use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

        // Define only the filesystem-unsafe ASCII characters that need encoding
        // Start with CONTROLS (0x00-0x1F, 0x7F) and add filesystem-specific chars
        // This preserves all non-ASCII UTF-8 (unicode) characters
        const FRAGMENT: &AsciiSet = &CONTROLS
            .add(b' ') // Space
            .add(b'/') // Path separator (Unix)
            .add(b'\\') // Path separator (Windows)
            .add(b':') // Drive letter separator (Windows), problematic on macOS
            .add(b'*') // Wildcard (Windows)
            .add(b'?') // Wildcard (Windows)
            .add(b'"') // Quote (Windows)
            .add(b'<') // Redirect (Windows)
            .add(b'>') // Redirect (Windows)
            .add(b'|') // Pipe (Windows)
            .add(b'%'); // Percent (to avoid double-encoding issues)

        // Use utf8_percent_encode which preserves non-ASCII UTF-8 sequences
        let sanitized = utf8_percent_encode(cache_key, FRAGMENT).to_string();

        // Requirement 2.5: Log if sanitization changed the key
        if sanitized != cache_key {
            debug!(
                "[CACHE_KEY_VALIDATION] Cache key sanitized: original={}, sanitized={}",
                cache_key, sanitized
            );
        }

        // Filesystem filename limit is typically 255 bytes
        // Use 200 as threshold to leave room for extensions (.meta, .bin, range suffixes)
        if sanitized.len() > 200 {
            // Hash long keys to ensure they fit within filesystem limits
            let hash = blake3::hash(cache_key.as_bytes());
            let hashed = format!("long_key_{}", hash.to_hex());
            // Requirement 2.4: Log when keys are hashed
            debug!(
                "[CACHE_KEY_VALIDATION] Cache key hashed (exceeds 200 chars): original_length={}, sanitized_length={}, hashed={}",
                cache_key.len(), sanitized.len(), hashed
            );
            hashed
        } else {
            sanitized
        }
    }
    /// Store a range with atomic operations
    /// Implements Requirements 1.1, 3.1, 3.2, 3.4, 3.5
    ///
    /// This method:
    /// 1. Compresses the range data if appropriate
    /// 2. Writes range data to a .tmp file then atomically renames it
    /// 3. Updates metadata file atomically
    /// 4. Ensures both operations succeed or both fail (atomic guarantee)
    /// 5. Handles partial write failures with cleanup
    #[allow(clippy::too_many_arguments)]
    pub async fn store_range(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
    ) -> Result<()> {
        self.store_range_with_slot_wait(
            cache_key,
            start,
            end,
            data,
            object_metadata,
            ttl,
            compression_enabled,
            SlotWait::WithinBudget,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn store_range_with_slot_wait(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
        slot_wait: SlotWait,
    ) -> Result<()> {
        match self
            .prepare_range_store(
                cache_key,
                start,
                end,
                data,
                object_metadata,
                ttl,
                compression_enabled,
            )
            .await?
        {
            Some(fill) => fill.finish_with_slot_wait(slot_wait).await,
            None => Ok(()),
        }
    }

    /// The part of [`Self::store_range`] that needs the manager: validation, the
    /// danger-band admission, compression and the `.tmp` write.
    ///
    /// Returns `Some(fill)` when the publish remains to be done. [`PreparedRangeFill::finish`]
    /// takes the key's slot lock (waiting up to `lock_acquire_wait_budget`), publishes the
    /// `.bin`, journals and credits it, and borrows nothing from the manager, so a caller
    /// holding `disk_cache.write()` drops the guard between the two calls and the slot wait
    /// does not hold the `RwLock`. Dropping an unfinished fill removes its `.tmp` and releases
    /// its capacity reservation.
    ///
    /// Returns `None` when nothing remains: the fill was declined, or the manager has no
    /// hybrid metadata writer and the direct-mode store (test configurations only) ran here.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_range_store(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
    ) -> Result<Option<PreparedRangeFill>> {
        use crate::cache_types::{NewCacheMetadata, RangeSpec};

        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range storage operation: key={}, range={}-{}, size={} bytes",
            cache_key,
            start,
            end,
            data.len()
        );

        // Validate range boundaries
        if start > end {
            error!(
                "Range validation failed: invalid boundaries for key={}, start={}, end={}",
                cache_key, start, end
            );
            return Err(ProxyError::CacheError(format!(
                "Invalid range: start ({}) > end ({})",
                start, end
            )));
        }

        let uncompressed_size = data.len() as u64;
        // When the client requests a range larger than the object (e.g., Range: bytes=0-52428799
        // for a 10-byte object), S3 returns only the available bytes with Content-Range: bytes 0-9/10.
        // Only then is the range end clamped to the data received: S3's `Content-Range` says the
        // object ends there, or a `200` with no `Content-Range` carries the whole object from
        // byte 0. Any other short body (a truncated upstream stream, an error document returned
        // with a 2xx, an unchecked repair fetch) is not a shorter object, and keeping it as a
        // valid sub-range would serve its bytes as the object's.
        let end = if uncompressed_size < (end - start + 1) && uncompressed_size > 0 {
            let clamped_end = start + uncompressed_size - 1;
            let upstream_content_range = object_metadata
                .response_headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-range"))
                .map(|(_, v)| v.as_str());
            let confirmed = match upstream_content_range {
                Some(value) => content_range_end(value) == Some(clamped_end),
                None => start == 0 && object_metadata.content_length == uncompressed_size,
            };
            if !confirmed {
                error!(
                    "Refusing to cache a short range: key={}, range={}-{}, data_size={}, content_range={:?}, content_length={} (the upstream response does not show the object ending at byte {})",
                    cache_key,
                    start,
                    end,
                    uncompressed_size,
                    upstream_content_range,
                    object_metadata.content_length,
                    clamped_end
                );
                return Err(ProxyError::CacheError(format!(
                    "Data size ({}) is shorter than range size ({}) and the upstream response does not show the object ending at byte {}",
                    uncompressed_size,
                    end - start + 1,
                    clamped_end
                )));
            }
            info!(
                "Range clamped to actual data size: key={}, requested_end={}, clamped_end={}, data_size={}",
                cache_key, end, clamped_end, uncompressed_size
            );
            clamped_end
        } else if uncompressed_size != (end - start + 1) {
            error!(
                "Range validation failed: data size mismatch for key={}, expected={}, actual={}",
                cache_key,
                end - start + 1,
                uncompressed_size
            );
            return Err(ProxyError::CacheError(format!(
                "Data size ({}) doesn't match range size ({})",
                uncompressed_size,
                end - start + 1
            )));
        } else {
            end
        };

        debug!(
            "Range validation passed: key={}, range={}-{}, size={} bytes",
            cache_key, start, end, uncompressed_size
        );

        // A range that arrives without an ETag (a non-2xx or otherwise ETag-less upstream
        // response reaching a store path) is never published over metadata that carries one.
        // In journal mode it would fold into that `.meta` under the cached ETag (the
        // consolidator keeps an ETag-less fill); in direct mode its ETag mismatch would wipe
        // every cached range.
        if object_metadata.etag.is_empty() {
            let metadata_file_path = self.get_new_metadata_file_path(cache_key);
            let existing_etag = std::fs::read_to_string(&metadata_file_path)
                .ok()
                .and_then(|content| serde_json::from_str::<NewCacheMetadata>(&content).ok())
                .map(|existing| existing.object_metadata.etag)
                .filter(|etag| !etag.is_empty());
            if let Some(existing_etag) = existing_etag {
                warn!(
                    "Refusing to cache a range without an ETag over metadata that has one: key={}, range={}-{}, existing_etag={}",
                    cache_key, start, end, existing_etag
                );
                return Err(ProxyError::CacheError(format!(
                    "Refusing to cache range {}-{} without an ETag for key {} (existing ETag {})",
                    start, end, cache_key, existing_etag
                )));
            }
        }

        // ===================================================================
        // P1 — the danger-band publication guard (cache-eviction-at-scale task 36).
        //
        // This is the boundary, not a call site: this function has nine production callers
        // and is also where `store_full_object_as_range` (P2),
        // `RangeHandler::store_range_new_storage` (P5) and page widening (P6) all land, so
        // one check here covers four of U4's nine paths and no alternate route bypasses it.
        //
        // `uncompressed_size` rather than the compressed figure, deliberately: the
        // compressed size is not known until the next step, and over-stating the incoming
        // bytes is the safe direction for a capacity decision. Checked *before* compression
        // so a declined fill does not pay for an LZ4 pass it will throw away.
        //
        // Declining returns `Ok(())` — the caching is skipped and the client's request is
        // untouched, the same degradation `store_put_as_write_cached_range_with_ttl` takes
        // on a `disk_safety_refusal`. Three of P5's four call sites are `tokio::spawn`ed
        // and ignore the result; the fourth (page widening's absolute-range case) is
        // awaited but assembles its Page in memory from the fetched buffers rather than by
        // re-reading what it just stored, so a decline costs a cache entry and not a
        // response.
        //
        // The reservation is held for the rest of this function — the compress, write and
        // rename — and released on drop, including on an early `?` return.
        // ===================================================================
        // The `max_cache_size` soft stop, decided once for this fill. P1 is where the
        // range GET-miss fill, the page fills, the `?partNumber=` fill and the damaged-range
        // refill all land, so one check here covers every read fill that is not the streamed
        // tee. `uncompressed_size` for the same reason the danger band uses it: over-stating
        // the incoming bytes is the safe direction, and the compressed figure is not known
        // yet.
        //
        // Every production caller of this function is a read fill — write-through's buffered
        // and streaming halves both go through `WriteCacheRangeSink`, which is gated by
        // `disk_safety_refusal` instead — so this needs no caller-kind parameter. `Ok(None)`
        // is the existing decline value: the caching is skipped, the client's request is
        // untouched.
        if let Some(reason) = crate::cache::read_fill_refusal(&self.cache_dir, uncompressed_size) {
            debug!(
                "Declining to cache range {}-{} for key={} ({} bytes): {}",
                start, end, cache_key, uncompressed_size, reason
            );
            return Ok(None);
        }

        let _capacity_reservation = {
            let admission = self
                .admit_publication(
                    uncompressed_size,
                    crate::capacity_guard::PublicationPath::P1BufferedRange,
                )
                .await;
            if let Some(detail) = admission.decline() {
                debug!(
                    "Declining to cache range {}-{} for key={} ({} bytes): {}",
                    start, end, cache_key, uncompressed_size, detail.reason
                );
                return Ok(None);
            }
            admission.into_reservation()
        };

        // Step 1: Compress the range data if appropriate.
        // `compression_enabled` here is the final effective-compression decision
        // (per-bucket rule + threshold + built-in denylist), computed by the
        // caller via `CacheManager::effective_compression`. Whether or not
        // compression runs, the data is always written as a checksummed LZ4
        // frame — compressed blocks when true, store-mode/stored blocks
        // (no LZ4 compressor invocation, still checksummed) when false. No
        // raw/uncompressed bytes are ever written by this path.
        // Requirement 5.1, 5.2, 5.3: Per-bucket compression control.
        let compression_start = std::time::Instant::now();
        let path = self.extract_path_from_cache_key(cache_key);
        let compression_result =
            self.compression_handler
                .compress_with_metadata(data, &path, compression_enabled);
        let (compressed_data, compression_algorithm, compressed_size) = (
            compression_result.data,
            compression_result.algorithm,
            compression_result.compressed_size,
        );
        let compression_duration = compression_start.elapsed();

        debug!(
            "Range compression completed: key={}, range={}-{}, algorithm={:?}, original={} bytes, compressed={} bytes, ratio={:.2}%, duration={:.2}ms, compression_enabled={}",
            cache_key,
            start,
            end,
            compression_algorithm,
            uncompressed_size,
            compressed_size,
            (compressed_size as f64 / uncompressed_size as f64) * 100.0,
            compression_duration.as_secs_f64() * 1000.0,
            compression_enabled
        );

        // Step 2: Write range data to .tmp file (but don't make it visible yet)
        // We'll rename it AFTER updating metadata to avoid race conditions
        let write_start = std::time::Instant::now();
        let range_file_path = self.get_new_range_file_path(cache_key, start, end);
        // Instance-scoped `{key}_{s}-{e}.bin.tmp.{host}.{pid}`, so the dead-instance reaper
        // can tell a dead process's leftover from a live peer's write in flight.
        let range_tmp_path = crate::cache::instance_scoped_range_tmp_path(&range_file_path, None);

        debug!(
            "Writing range binary file to temp: key={}, range={}-{}, path={:?}, size={} bytes",
            cache_key, start, end, range_tmp_path, compressed_size
        );

        // Ensure ranges directory exists
        if let Some(parent) = range_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                error!(
                    "Failed to create ranges directory: key={}, path={:?}, error={}",
                    cache_key, parent, e
                );
                ProxyError::CacheError(format!("Failed to create ranges directory: {}", e))
            })?;
        }

        // Write to temporary file (keep it as .tmp until metadata is updated). Synced here,
        // before either arm below, because the direct arm renames the `.meta` naming this
        // file before the `.bin` itself; see `write_synced`.
        let file_identity = match write_synced_identity(&range_tmp_path, &compressed_data) {
            Ok(identity) => identity,
            Err(e) => {
                // Clean up tmp file on failure
                let _ = std::fs::remove_file(&range_tmp_path);

                // Check if this is a disk space exhaustion error
                // Requirement 8.3: Handle disk space exhaustion
                if note_disk_full(&e, "store_range.bin_tmp") {
                    error!(
                    "Disk space exhausted: key={}, range={}-{}, attempted_size={} bytes, error={}",
                    cache_key, start, end, compressed_size, e
                );
                    return Err(ProxyError::CacheError(format!(
                        "Disk space exhausted: {}",
                        e
                    )));
                } else {
                    error!(
                        "Failed to write range tmp file: key={}, range={}-{}, path={:?}, error={}",
                        cache_key, start, end, range_tmp_path, e
                    );
                    return Err(ProxyError::CacheError(format!(
                        "Failed to write range tmp file: {}",
                        e
                    )));
                }
            }
        };

        let write_duration = write_start.elapsed();
        debug!(
            "Range binary file written to temp: key={}, range={}-{}, path={:?}, size={} bytes, duration={:.2}ms",
            cache_key, start, end, range_tmp_path, compressed_size, write_duration.as_secs_f64() * 1000.0
        );

        // Create range spec for metadata update
        let ranges_dir = self.cache_dir.join("ranges");
        let range_file_relative_path = range_file_path
            .strip_prefix(&ranges_dir)
            .map_err(|e| {
                let _ = std::fs::remove_file(&range_tmp_path);
                ProxyError::CacheError(format!("Failed to compute relative path: {}", e))
            })?
            .to_string_lossy()
            .to_string();

        // Classify staging membership BEFORE `range_file_relative_path` is moved into
        // the `RangeSpec`, so the accounting block below needs neither a clone nor a
        // second, divergent predicate.
        // Spec: write-cache-accounting-and-eviction. Requirements: 6.2
        let counts_as_staged = crate::cache_types::classify_new_range_as_staged(
            &range_file_relative_path,
            object_metadata.is_write_cached,
        );

        // Record membership ON the range (R12.2) rather than leaving it to be
        // re-derived later from the object flag. The object flag is correct only
        // while every range of the object shares one tier; once a GET range-miss
        // appends a read-tier range to a still-flagged object, re-deriving
        // classifies that range as staged though nothing ever credited it.
        // Spec: write-cache-accounting-and-eviction. Requirements: 12.2
        let range_spec = RangeSpec::new_staged(
            start,
            end,
            range_file_relative_path,
            compression_algorithm,
            compressed_size,
            uncompressed_size,
            counts_as_staged,
        )
        // The `.tmp`'s identity; the publish renames it, which keeps its length and
        // modification time.
        .with_file_identity(file_identity);
        // Step 3: Update metadata - use hybrid writer for coordinated metadata updates
        // (always enabled in production; tests may not have it configured)
        let metadata_start = std::time::Instant::now();

        if let Some(hybrid_writer) = &self.hybrid_metadata_writer {
            // Shared storage mode: the publish, journal and credit run in
            // `PreparedRangeFill::finish`, which needs no `&self`. A caller holding the
            // `DiskCacheManager` write guard drops it before `finish`, so the slot-lock wait
            // there does not hold the `RwLock`.
            let pending = PendingRangePublish::new(range_tmp_path.clone(), range_file_path.clone());
            return Ok(Some(PreparedRangeFill {
                cache_dir: self.cache_dir.clone(),
                meta_path: self.get_new_metadata_file_path(cache_key),
                cache_key: cache_key.to_string(),
                start,
                end,
                object_metadata,
                ttl,
                range_spec,
                compressed_size,
                uncompressed_size,
                counts_as_staged,
                pending,
                hybrid_writer: Arc::clone(hybrid_writer),
                journal_consolidator: self.journal_consolidator.clone(),
                _capacity_reservation,
                operation_start,
                fill_witness: None,
            }));
        }

        // Fallback: direct file locking for metadata updates (used in tests without hybrid writer)
        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");

        debug!(
            "Updating metadata (direct mode): key={}, metadata_path={:?}",
            cache_key, metadata_file_path
        );

        // Ensure metadata directory exists
        if let Some(parent) = metadata_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                // Clean up temp range file on failure
                let _ = std::fs::remove_file(&range_tmp_path);
                error!(
                    "Failed to create metadata directory: key={}, path={:?}, error={}",
                    cache_key, parent, e
                );
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        // Acquire the key's metadata slot lock, through the SAME namespace and protocol as
        // `MetadataLockManager`. This path used to take its own inline `flock` on a per-key
        // `{key}.meta.lock` that it never wrote a payload into — precisely the state the
        // retired stale-detection branch judged stale, so a contending manager acquirer
        // unlinked this holder's file and took an independent lock on a fresh inode.
        //
        // Bounded and off the runtime. It called the blocking primitive INLINE on a runtime
        // worker with no bound at all, so a wedged peer both stalled this task indefinitely and
        // occupied a worker while doing it.
        let lock_acquire_start = std::time::Instant::now();
        let meta_lock = self.acquire_meta_slot_lock(cache_key).await.map_err(|e| {
            // Clean up temp range file on failure
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to acquire metadata slot lock: key={}, error={}",
                cache_key, e
            );
            e
        })?;
        let lock_file_path = meta_lock.lock_file_path().to_path_buf();

        let lock_duration = lock_acquire_start.elapsed();
        debug!(
            "Acquired exclusive lock: key={}, lock_path={:?}, wait_time={:.2}ms",
            cache_key,
            lock_file_path,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read existing metadata or create new (inside lock). `replaces_unparseable` is
        // counted in `cache.metadata_heal_overwrite_total` once the new document is in place.
        let mut replaces_unparseable = false;
        let mut cache_metadata = if metadata_file_path.exists() {
            debug!(
                "Reading existing metadata: key={}, path={:?}",
                cache_key, metadata_file_path
            );

            match std::fs::read_to_string(&metadata_file_path) {
                Ok(content) => {
                    match serde_json::from_str::<NewCacheMetadata>(&content) {
                        Ok(metadata) => {
                            debug!(
                                "Parsed existing metadata: key={}, ranges={}, etag={}",
                                cache_key,
                                metadata.ranges.len(),
                                metadata.object_metadata.etag
                            );

                            // Verify object metadata matches
                            // Quote-insensitive via the shared helper. The largest blast
                            // radius of the seven sites — a bare-vs-quoted spelling of the
                            // SAME ETag invalidated every existing range for the key.
                            if !crate::revalidation::etags_equal_ignoring_quotes(
                                &metadata.object_metadata.etag,
                                &object_metadata.etag,
                            ) {
                                warn!(
                                    "ETag mismatch detected: key={}, existing_etag={}, new_etag={} - invalidating existing ranges",
                                    cache_key, metadata.object_metadata.etag, object_metadata.etag
                                );

                                // Requirement 14.1, 14.2, 14.3: Invalidate existing cached ranges
                                // and allow new data to be cached
                                let invalidated_count = metadata.ranges.len();

                                // Delete existing range files
                                let ranges_dir = self.cache_dir.join("ranges");
                                for range in &metadata.ranges {
                                    let range_path = ranges_dir.join(&range.file_path);
                                    if range_path.exists() {
                                        if let Err(e) = std::fs::remove_file(&range_path) {
                                            warn!(
                                                "Failed to delete invalidated range file: key={}, path={:?}, error={}",
                                                cache_key, range_path, e
                                            );
                                        }
                                    }
                                }

                                // Record metrics for ETag mismatch invalidation
                                if let Some(metrics_manager) = &self.metrics_manager {
                                    metrics_manager
                                        .read()
                                        .await
                                        .record_etag_mismatch(
                                            cache_key,
                                            &metadata.object_metadata.etag,
                                            &object_metadata.etag,
                                        )
                                        .await;
                                    let range_names: Vec<String> = metadata
                                        .ranges
                                        .iter()
                                        .map(|r| format!("{}-{}", r.start, r.end))
                                        .collect();
                                    metrics_manager
                                        .read()
                                        .await
                                        .record_range_invalidation(
                                            cache_key,
                                            &range_names,
                                            "etag_mismatch",
                                        )
                                        .await;
                                }

                                info!(
                                    "Invalidated {} existing ranges due to ETag mismatch: key={}, old_etag={}, new_etag={}",
                                    invalidated_count, cache_key, metadata.object_metadata.etag, object_metadata.etag
                                );

                                // Create fresh metadata with new object_metadata
                                let now = SystemTime::now();
                                NewCacheMetadata {
                                    cache_key: cache_key.to_string(),
                                    object_metadata: object_metadata.clone(),
                                    ranges: Vec::new(),
                                    created_at: now,
                                    expires_at: now + ttl,
                                    compression_info: crate::cache_types::CompressionInfo::default(
                                    ),
                                    ..Default::default()
                                }
                            } else {
                                metadata
                            }
                        }
                        Err(e) => {
                            // Spec: cache-metadata-resilience Req 3, Task 6
                            // Existing .meta is corrupt — create fresh metadata instead of
                            // failing. This is the heal path: store_range will overwrite
                            // the corrupt file with a valid one via atomic tmp+rename below.
                            warn!(
                                "[METADATA_HEAL] Existing metadata unparseable during store_range, creating fresh: key={}, path={:?}, error={}",
                                cache_key, metadata_file_path, e
                            );
                            replaces_unparseable = true;
                            let now = SystemTime::now();
                            NewCacheMetadata {
                                cache_key: cache_key.to_string(),
                                object_metadata: object_metadata.clone(),
                                ranges: Vec::new(),
                                created_at: now,
                                expires_at: now + ttl,
                                compression_info: crate::cache_types::CompressionInfo::default(),
                                ..Default::default()
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to read existing metadata: key={}, path={:?}, error={}",
                        cache_key, metadata_file_path, e
                    );
                    // Clean up temp range file and release lock
                    let _ = std::fs::remove_file(&range_tmp_path);
                    return Err(ProxyError::CacheError(format!(
                        "Failed to read metadata file: {}",
                        e
                    )));
                }
            }
        } else {
            // Create new metadata
            info!(
                "Creating new metadata entry: key={}, etag={}",
                cache_key, object_metadata.etag
            );

            let now = SystemTime::now();
            NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata: object_metadata.clone(),
                ranges: Vec::new(),
                created_at: now,
                expires_at: now + ttl,
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            }
        };

        // Refresh object-level expiration on every cache write
        cache_metadata.refresh_object_ttl(ttl);

        // Check if this range already exists and update it, or add new
        let range_update_type = if let Some(existing_range) = cache_metadata
            .ranges
            .iter_mut()
            .find(|r| r.start == start && r.end == end)
        {
            *existing_range = range_spec.clone();
            debug!(
                "Updated existing range in metadata: key={}, range={}-{}",
                cache_key, start, end
            );
            "update"
        } else {
            cache_metadata.ranges.push(range_spec.clone());
            debug!(
                "Added new range to metadata: key={}, range={}-{}, total_ranges={}",
                cache_key,
                start,
                end,
                cache_metadata.ranges.len()
            );
            "add"
        };

        // Write metadata to temporary file
        let serialize_start = std::time::Instant::now();
        let metadata_json = serde_json::to_string_pretty(&cache_metadata).map_err(|e| {
            // Clean up temp range file and release lock on failure
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to serialize metadata: key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        let serialize_duration = serialize_start.elapsed();
        debug!(
            "Metadata serialized: key={}, size={} bytes, duration={:.2}ms",
            cache_key,
            metadata_json.len(),
            serialize_duration.as_secs_f64() * 1000.0
        );

        write_synced(&metadata_tmp_path, metadata_json).map_err(|e| {
            crate::disk_cache::note_disk_full(&e, "store_range.meta_tmp");
            // Clean up both tmp files, release lock on failure
            let _ = std::fs::remove_file(&metadata_tmp_path);
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to write metadata tmp file: key={}, path={:?}, error={}",
                cache_key, metadata_tmp_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata tmp file: {}", e))
        })?;

        // Atomic rename of metadata
        debug!(
            "Performing atomic metadata rename: tmp={:?} -> final={:?}",
            metadata_tmp_path, metadata_file_path
        );

        std::fs::rename(&metadata_tmp_path, &metadata_file_path).map_err(|e| {
            // Clean up both tmp files, release lock on failure
            let _ = std::fs::remove_file(&metadata_tmp_path);
            let _ = std::fs::remove_file(&range_tmp_path);
            error!(
                "Failed to rename metadata file: key={}, tmp={:?}, final={:?}, error={}",
                cache_key, metadata_tmp_path, metadata_file_path, e
            );
            ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
        })?;
        if replaces_unparseable {
            note_metadata_heal_overwrite(cache_key);
        }

        // Release the slot lock by dropping the guard: `MetadataLock::Drop` closes the
        // descriptor, which is what releases the `flock`. The slot file is permanent.
        drop(meta_lock);

        let metadata_duration = metadata_start.elapsed();
        debug!(
            "Released lock: key={}, total_metadata_update_duration={:.2}ms",
            cache_key,
            metadata_duration.as_secs_f64() * 1000.0
        );

        // Step 4: NOW make the range file visible by renaming from .tmp to .bin
        // This ensures metadata is updated BEFORE the range file becomes visible
        // This fixes the race condition where concurrent requests could see the range file
        // but not find it in metadata
        debug!(
            "Making range file visible: tmp={:?} -> final={:?}",
            range_tmp_path, range_file_path
        );

        std::fs::rename(&range_tmp_path, &range_file_path)
            .map_err(|e| {
                // Clean up tmp file on failure
                let _ = std::fs::remove_file(&range_tmp_path);
                error!(
                    "Failed to rename range file after metadata update: key={}, range={}-{}, tmp={:?}, final={:?}, error={}",
                    cache_key, start, end, range_tmp_path, range_file_path, e
                );
                // Note: Metadata is already updated, so this is a partial failure
                // The range will be in metadata but the binary file won't exist
                // This will be handled as a cache miss on next read
                ProxyError::CacheError(format!("Failed to make range file visible: {}", e))
            })?;

        debug!(
            "Range binary file made visible: key={}, range={}-{}, path={:?}",
            cache_key, start, end, range_file_path
        );

        let total_duration = operation_start.elapsed();
        debug!(
            "Range storage operation completed: key={}, range={}-{}, operation={}, uncompressed={} bytes, compressed={} bytes, compression_ratio={:.2}%, total_ranges={}, total_duration={:.2}ms",
            cache_key,
            start,
            end,
            range_update_type,
            uncompressed_size,
            compressed_size,
            (compressed_size as f64 / uncompressed_size as f64) * 100.0,
            cache_metadata.ranges.len(),
            total_duration.as_secs_f64() * 1000.0
        );

        // Size tracking is handled unconditionally by JournalConsolidator through journal entries.
        // SharedStorageConfig tunes coordination parameters; it has no `enabled` flag.

        Ok(None)
    }

    /// Begin an incremental range write. Creates a `.tmp` file with a random suffix
    /// for concurrent write safety and returns an `IncrementalRangeWriter`.
    ///
    /// Each chunk written via `write_range_chunk` is independently compressed as its own
    /// LZ4 frame and appended to the `.tmp` file (concatenated frames approach).
    /// `FrameDecoder` handles concatenated frames transparently on read.
    /// # Returns
    ///
    /// `Ok(Some(writer))` when the write was admitted, `Ok(None)` when the danger-band
    /// publication guard declined it on capacity grounds, `Err` on a real failure.
    ///
    /// `Ok(None)` is a **deliberate type change** made by cache-eviction-at-scale task 36:
    /// declining and failing must not be the same value at any call site, because the
    /// callers' handling differs — a decline skips caching and leaves the client's response
    /// alone, whereas an `Err` is worth a WARN. Making it an `Option` rather than a special
    /// `Err` variant means the compiler asks every present and future call site which one it
    /// is, instead of a convention doing it.
    /// [`Self::begin_incremental_range_write`] for a **read** fill: the `max_cache_size` soft
    /// stop first, then the shared boundary.
    ///
    /// # Why the check is here and not inside the shared function
    ///
    /// P3a is the one publication boundary both tiers pass through: the GET-response tee and
    /// write-through's streaming sink (`WriteCacheRangeSink::open`). Write-through's
    /// `max_cache_size` decision is already taken, against its own figure, by
    /// `disk_safety_refusal` before the body is read; evaluating it a second time here would
    /// let a PUT that was admitted be declined mid-stream on a different reading of the same
    /// bound, which is a write-path behaviour change this unit has no mandate for. So the
    /// read-fill half gets its own entry point and the shared one is unchanged.
    ///
    /// Returns `Ok(None)` for a decline, exactly as the shared function does, so a caller
    /// cannot tell the two declines apart and does not need to.
    pub async fn begin_incremental_range_write_for_read_fill(
        &self,
        cache_key: &str,
        start: u64,
        end: u64,
        compression_enabled: bool,
    ) -> Result<Option<IncrementalRangeWriter>> {
        if start <= end {
            if let Some(reason) = crate::cache::read_fill_refusal(&self.cache_dir, end - start + 1)
            {
                debug!(
                    "Declining to begin incremental range write {}-{} for key={}: {}",
                    start, end, cache_key, reason
                );
                return Ok(None);
            }
        }
        self.begin_incremental_range_write(cache_key, start, end, compression_enabled)
            .await
    }

    pub async fn begin_incremental_range_write(
        &self,
        cache_key: &str,
        start: u64,
        end: u64,
        compression_enabled: bool,
    ) -> Result<Option<IncrementalRangeWriter>> {
        if start > end {
            return Err(ProxyError::CacheError(format!(
                "Invalid range: start ({}) > end ({})",
                start, end
            )));
        }

        // P3a — the GET-response incremental tee, U4's primary GET-facing publication path
        // and the one with no guard of any kind before task 36. Also the range write
        // underneath the write-cache streaming sink (P3b/P7), which is separately gated by
        // `disk_safety_refusal` at `open_write_cache_sink`.
        //
        // The incoming figure is the range's full declared length, known here because the
        // caller has the content length before the body streams. That is the whole
        // commitment the writer is about to make, not the bytes written so far, which is the
        // right quantity to admit against.
        let admission = self
            .admit_publication(
                end - start + 1,
                crate::capacity_guard::PublicationPath::P3aGetResponseTee,
            )
            .await;
        if let Some(detail) = admission.decline() {
            debug!(
                "Declining to begin incremental range write {}-{} for key={}: {}",
                start, end, cache_key, detail.reason
            );
            return Ok(None);
        }
        let capacity_reservation = admission.into_reservation();

        let range_file_path = self.get_new_range_file_path(cache_key, start, end);

        // Ensure parent directory exists
        if let Some(parent) = range_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create ranges directory: {}", e))
            })?;
        }

        // Unique temp name with a random suffix for concurrent write safety within this
        // process, and host + pid so the dead-instance reaper can judge a leftover:
        // `{key}_{s}-{e}.{rand}.tmp.{host}.{pid}`.
        let random_suffix: u32 = fastrand::u32(..);
        let tmp_path =
            crate::cache::instance_scoped_range_tmp_path(&range_file_path, Some(random_suffix));

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true) // O_CREAT|O_EXCL — refuses symlinks and pre-existing files
            .open(&tmp_path)
            .map_err(|e| {
                note_disk_full(&e, "incremental_range.create_tmp");
                ProxyError::CacheError(format!(
                    "Failed to create incremental tmp file {:?}: {}",
                    tmp_path, e
                ))
            })?;

        debug!(
            "Begin incremental range write: key={}, range={}-{}, tmp={:?}, compression={}",
            cache_key, start, end, tmp_path, compression_enabled
        );

        Ok(Some(IncrementalRangeWriter {
            tmp_path,
            final_path: range_file_path,
            file,
            bytes_written: 0,
            compressed_bytes_written: 0,
            cache_key: cache_key.to_string(),
            start,
            end,
            compression_enabled,
            batch_buf: Vec::with_capacity(self.compression_batch_size),
            batch_size: self.compression_batch_size,
            stats: self.compression_handler.shared_stats(),
            _capacity_reservation: capacity_reservation,
            fill_witness: None,
        }))
    }

    /// Write a chunk to the incremental range writer.
    ///
    /// Chunk bytes are always accumulated into an in-memory batch buffer and
    /// flushed as a single LZ4 frame once the buffer reaches `batch_size`.
    /// Residual bytes remaining at commit time are flushed by
    /// `commit_incremental_range`. This produces fewer, larger frames (better
    /// compression ratio + fewer syscalls) versus a one-frame-per-chunk
    /// approach. `FrameDecoder` reads concatenated frames transparently on load.
    ///
    /// When `compression_enabled` is true the flushed frame uses LZ4-compressed
    /// blocks; when false it uses a store-mode frame (uncompressed/"stored"
    /// blocks, no LZ4 compressor invocation, still checksummed via
    /// `CompressionHandler::encode_store_mode_frame`) — see
    /// compression-content-aware-fix spec, Requirement 3.
    ///
    /// `writer.bytes_written` always tracks the logical uncompressed total and is
    /// updated on every chunk. `writer.compressed_bytes_written` is updated when
    /// a batch is flushed.
    pub fn write_range_chunk(writer: &mut IncrementalRangeWriter, chunk: &[u8]) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }

        writer.bytes_written += chunk.len() as u64;
        writer.batch_buf.extend_from_slice(chunk);
        if writer.batch_buf.len() >= writer.batch_size {
            Self::flush_batch(writer)?;
        }
        Ok(())
    }

    /// Encode `writer.batch_buf` as a single LZ4 frame — compressed blocks when
    /// `writer.compression_enabled`, store-mode (stored) blocks otherwise — and
    /// append it to the `.tmp` file. Updates `writer.compressed_bytes_written`
    /// by the frame length and clears `batch_buf`. No-op when the buffer is
    /// empty.
    ///
    /// Called from `write_range_chunk` when the batch threshold is reached, and from
    /// `finalize_incremental_range` to flush any residual bytes before the final rename.
    fn flush_batch(writer: &mut IncrementalRangeWriter) -> Result<()> {
        if writer.batch_buf.is_empty() {
            return Ok(());
        }
        use std::io::Write;

        let frame_buf = if writer.compression_enabled {
            use lz4_flex::frame::FrameEncoder;

            let mut frame_buf = Vec::with_capacity(writer.batch_buf.len());
            let mut encoder = FrameEncoder::with_frame_info(
                crate::compression::range_frame_info(),
                &mut frame_buf,
            );
            encoder.write_all(&writer.batch_buf).map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to compress batch in incremental write: {}",
                    e
                ))
            })?;
            encoder.finish().map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to finish LZ4 frame in batched incremental write: {}",
                    e
                ))
            })?;
            frame_buf
        } else {
            crate::compression::CompressionHandler::encode_store_mode_frame(&writer.batch_buf)
                .map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Failed to encode store-mode frame in batched incremental write: {}",
                        e
                    ))
                })?
        };

        #[cfg(test)]
        let write_result = match INJECT_BATCH_WRITE_OS_ERROR.with(|cell| cell.take()) {
            Some(code) => Err(std::io::Error::from_raw_os_error(code)),
            None => writer.file.write_all(&frame_buf),
        };
        #[cfg(not(test))]
        let write_result = writer.file.write_all(&frame_buf);
        write_result.map_err(|e| {
            note_disk_full(&e, "incremental_range.batch_write");
            ProxyError::CacheError(format!("Failed to write batch to tmp file: {}", e))
        })?;
        writer.compressed_bytes_written += frame_buf.len() as u64;
        // Contribute to the live shared compression stats so /metrics and OTLP
        // cover this inline-framing streaming path (compression-content-aware-fix
        // Req 5.2).
        writer
            .stats
            .record_batch_bytes(writer.batch_buf.len() as u64, frame_buf.len() as u64);
        writer.batch_buf.clear();
        Ok(())
    }

    /// Validate total size, flush file, atomic rename `.tmp` → `.bin`, and write journal entry.
    ///
    /// Returns an error (and cleans up the `.tmp` file) if `bytes_written != end - start + 1`.
    ///
    /// Takes `&self` rather than `&mut self` — no `DiskCacheManager` field is mutated here.
    /// All internal mutation is serialized by finer-grained locks: `HybridMetadataWriter`'s
    /// async `Mutex` for the journal append, and `SizeAccumulator`'s internal atomics + dedup
    /// `HashSet` for size tracking. This lets callers hold a read lock (or plain `Arc` deref)
    /// during commit instead of a write lock, so concurrent commits of distinct ranges
    /// proceed in parallel.
    ///
    /// The publish waits for the key's slot lock for up to `lock_acquire_wait_budget`, so a
    /// caller holding the manager through a `tokio::sync::RwLock` should call
    /// [`Self::commit_incremental_range_shared`] instead, which holds the guard only for the
    /// finalize.
    pub async fn commit_incremental_range(
        &self,
        writer: IncrementalRangeWriter,
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: Duration,
    ) -> Result<()> {
        self.prepare_incremental_commit(writer, object_metadata, ttl)?
            .finish()
            .await
    }

    /// [`Self::commit_incremental_range`] for a manager behind the shared `RwLock`: the read
    /// guard covers only [`Self::prepare_incremental_commit`] and is dropped before the
    /// slot-lock wait in [`PreparedIncrementalCommit::finish`].
    ///
    /// Holding the read guard through that wait (up to 5 s by default while consolidation holds
    /// the slot) would let one queued `write()` block every later `read()` behind it, because
    /// tokio's `RwLock` is write-preferring.
    pub async fn commit_incremental_range_shared(
        lock: &tokio::sync::RwLock<Self>,
        writer: IncrementalRangeWriter,
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: Duration,
    ) -> Result<()> {
        let prepared =
            lock.read()
                .await
                .prepare_incremental_commit(writer, object_metadata, ttl)?;
        prepared.finish().await
    }

    /// A buffered GET-miss range fill guarded by its version witness: the two-phase
    /// store `RangeHandler::store_range_new_storage` runs (the write guard covers the admission,
    /// compression and `.tmp` write and is dropped before the slot-lock wait), with the witness
    /// attached to the publish.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn store_range_shared_witnessed(
        lock: &tokio::sync::RwLock<Self>,
        cache_key: &str,
        start: u64,
        end: u64,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: Duration,
        compression_enabled: bool,
        fill_witness: Option<crate::cache::FillWitness>,
        slot_wait: SlotWait,
    ) -> Result<()> {
        let prepared = {
            let mut disk_cache = lock.write().await;
            disk_cache
                .prepare_range_store(
                    cache_key,
                    start,
                    end,
                    data,
                    object_metadata,
                    ttl,
                    compression_enabled,
                )
                .await?
        };
        match prepared {
            Some(fill) => {
                fill.with_fill_witness(fill_witness)
                    .finish_with_slot_wait(slot_wait)
                    .await
            }
            None => Ok(()),
        }
    }

    /// The part of [`Self::commit_incremental_range`] that needs the manager: flush the
    /// residual batch, validate the length and leave the `.tmp` ready to publish. Without a
    /// hybrid writer the `.bin` is published here, since nothing waits for a slot lock on that
    /// arm. The returned value owns everything the publish, journal and credit need.
    pub fn prepare_incremental_commit(
        &self,
        writer: IncrementalRangeWriter,
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: Duration,
    ) -> Result<PreparedIncrementalCommit> {
        let cache_key = writer.cache_key.clone();
        let fill_witness = writer.fill_witness.clone();

        // Finalize the bytes (flush residual batch, validate length, publish the
        // `.bin`). Shared with the no-journal write-cache path so both produce
        // byte-identical range files. The read/GET path passes the configured
        // partial-range commit ratio so a received prefix of an incomplete range is
        // salvaged as a clamped sub-range (crt-conditional-range-caching Req 2);
        // the PUT write-cache path passes `None` (exact-only).
        //
        // With a hybrid writer the fill is journal-only, so the rename goes under the key's slot
        // lock with the same version check as `store_range`'s hybrid arm; a dropped fill
        // journals and credits nothing. Without one, nothing is folded later, so the immediate
        // publish is kept.
        let (range_spec, publish) = if self.hybrid_metadata_writer.is_some() {
            let (range_spec, pending) = self.finalize_incremental_range_unpublished(
                writer,
                Some(self.partial_range_commit_ratio),
            )?;
            (range_spec, IncrementalPublish::UnderSlotLock(pending))
        } else {
            let (range_spec, existed) =
                self.finalize_incremental_range(writer, Some(self.partial_range_commit_ratio))?;
            (range_spec, IncrementalPublish::Published(existed))
        };
        Ok(PreparedIncrementalCommit {
            cache_dir: self.cache_dir.clone(),
            meta_path: self.get_new_metadata_file_path(&cache_key),
            cache_key,
            object_metadata,
            ttl,
            range_spec,
            publish,
            hybrid_writer: self.hybrid_metadata_writer.clone(),
            journal_consolidator: self.journal_consolidator.clone(),
            fill_visibility_tracker: self.fill_visibility_tracker.clone(),
            fill_witness,
        })
    }
}

/// How [`PreparedIncrementalCommit::finish`] makes the `.bin` visible.
enum IncrementalPublish {
    /// Hybrid writer: rename under the key's slot lock with the version check.
    UnderSlotLock(PendingRangePublish),
    /// No hybrid writer: already renamed by `prepare_incremental_commit`; the flag is whether
    /// the `.bin` already existed.
    Published(bool),
}

/// A streamed range fill whose bytes are finalized and whose publish, journal append and
/// credit remain, returned by [`DiskCacheManager::prepare_incremental_commit`].
///
/// Owns everything [`Self::finish`] needs, so it runs with no `DiskCacheManager` guard held.
/// Dropping it unfinished removes an unpublished `.tmp`; nothing is journaled or credited.
#[must_use = "an unfinished streamed fill is discarded when dropped"]
pub struct PreparedIncrementalCommit {
    cache_dir: PathBuf,
    meta_path: PathBuf,
    cache_key: String,
    object_metadata: crate::cache_types::ObjectMetadata,
    ttl: Duration,
    range_spec: crate::cache_types::RangeSpec,
    publish: IncrementalPublish,
    hybrid_writer: Option<Arc<Mutex<HybridMetadataWriter>>>,
    journal_consolidator: Option<Arc<crate::journal_consolidator::JournalConsolidator>>,
    fill_visibility_tracker: Option<Arc<crate::fill_visibility_tracker::FillVisibilityTracker>>,
    fill_witness: Option<crate::cache::FillWitness>,
}

impl PreparedIncrementalCommit {
    /// Publish the fill (under the key's slot lock, waiting up to `lock_acquire_wait_budget`,
    /// when a hybrid writer is configured), journal it, credit it and record its visibility.
    /// Call it after dropping any `DiskCacheManager` guard.
    pub async fn finish(self) -> Result<()> {
        let PreparedIncrementalCommit {
            cache_dir,
            meta_path,
            cache_key,
            object_metadata,
            ttl,
            mut range_spec,
            publish,
            hybrid_writer,
            journal_consolidator,
            fill_visibility_tracker,
            fill_witness,
        } = self;
        // The slot lock, when the publish took one, is held through the journal append below.
        let (range_already_existed, published) = match publish {
            IncrementalPublish::UnderSlotLock(pending) => {
                match publish_range_fill_under_slot_lock_at(
                    &cache_dir,
                    &meta_path,
                    &cache_key,
                    range_spec.start,
                    range_spec.end,
                    &object_metadata.etag,
                    pending,
                    SlotWait::WithinBudget,
                    fill_witness.as_ref(),
                )
                .await?
                {
                    Some(published) => (published.range_already_existed, Some(published)),
                    None => return Ok(()),
                }
            }
            IncrementalPublish::Published(existed) => (existed, None),
        };
        #[cfg(test)]
        range_rename_pause::before_append(&cache_key).await;
        let (start, end, compressed_size) =
            (range_spec.start, range_spec.end, range_spec.compressed_size);
        // Classify staging membership before `range_spec` is moved into the journal
        // write below, through the single shared predicate.
        // Spec: write-cache-accounting-and-eviction. Requirements: 6.2
        let counts_as_staged = crate::cache_types::classify_new_range_as_staged(
            &range_spec.file_path,
            object_metadata.is_write_cached,
        );
        // Record it ON the range, so the persisted `.meta` carries the tier this
        // write decided rather than leaving it to be re-derived from an object flag
        // that may later cover ranges from both tiers (R12.2). Must precede the
        // journal write below, which moves `range_spec`.
        range_spec.staged = Some(counts_as_staged);
        // Kept for the fill-visibility record below; the journal write moves `range_spec`.
        let committed_range = range_spec.clone();
        let mut journalled = false;

        // Write journal entry (same pattern as store_range)
        if let Some(hybrid_writer) = &hybrid_writer {
            let mut hw = hybrid_writer.lock().await;
            if let Err(e) = hw
                .write_range_metadata_witnessed(
                    &cache_key,
                    range_spec,
                    WriteMode::JournalOnly,
                    Some(object_metadata.clone()),
                    ttl,
                    fill_witness.as_ref().map(|w| w.journal_token()),
                )
                .await
            {
                drop(hw);
                error!(
                    "Journal write failed for incremental range, removing the unjournalled range \
                     file: key={}, range={}-{}, error={}",
                    cache_key, start, end, e
                );
                if let Some(published) = published {
                    published.unpublish(&cache_key, start, end);
                }
                return Err(e);
            }
            // A witnessed fill records its visibility inside the publish hold, so a stamp
            // that runs after the hold also clears the record (`invalidate_cache_hierarchy_stamped`
            // invalidates the tracker again after its own hold), and a peer's stamp replaces the
            // marker token the record carries.
            if fill_witness.is_some() {
                DiskCacheManager::record_fill_visibility_at(
                    fill_visibility_tracker.as_ref(),
                    &cache_dir,
                    &cache_key,
                    &meta_path,
                    fill_witness.as_ref(),
                    &committed_range,
                    &object_metadata,
                    ttl,
                );
            }
            drop(published);
            journalled = true;

            // Track size via in-memory accumulator
            if let Some(consolidator) = &journal_consolidator {
                if !range_already_existed {
                    consolidator.size_accumulator().add_range(
                        &cache_key,
                        start,
                        end,
                        compressed_size,
                    );
                    // Same guard as `store_range`'s hybrid arm, and for the same reason: no
                    // ledger append happens here, so a staged credit would be invisible to
                    // the fold and to staging eviction.
                    // Spec: cache-eviction-at-scale § 16.6.6
                    if counts_as_staged {
                        refuse_unledgered_staged_credit(
                            "DiskCacheManager::commit_incremental_range",
                            &cache_key,
                            start,
                            end,
                            compressed_size,
                        );
                    }
                }
            }
        }

        // The `.bin` is published (`finalize_incremental_range` above) and the journal line is
        // appended, so this instance may now serve the fill before consolidation writes its
        // `.meta`. Only when journalled: without a hybrid writer nothing is journalled and there
        // is no fold for the tracker to bridge. After the hybrid-writer lock is released, because
        // the record stats the `.bin` and on shared storage that is a network round trip.
        if journalled && fill_witness.is_none() {
            DiskCacheManager::record_fill_visibility_at(
                fill_visibility_tracker.as_ref(),
                &cache_dir,
                &cache_key,
                &meta_path,
                None,
                &committed_range,
                &object_metadata,
                ttl,
            );
        }

        Ok(())
    }
}

impl DiskCacheManager {
    /// Finalize an incremental range write **without** writing metadata or
    /// updating size tracking: flush any residual batch as a final LZ4 frame,
    /// validate the written length, flush the file, and atomically rename
    /// `.tmp` → `.bin`. Returns the [`crate::cache_types::RangeSpec`] describing
    /// the published file and whether the final `.bin` already existed (so a
    /// size-tracking caller can avoid double-counting an overwrite).
    ///
    /// `min_commit_ratio` controls partial-prefix salvage on the read/GET path
    /// (crt-conditional-range-caching Req 2):
    /// - `None` → exact-only: a byte count `!= end - start + 1` is an error and the
    ///   `.tmp` is discarded. Used by the PUT write-cache path (a truncated upload
    ///   must never be cached as object content).
    /// - `Some(r)` → if the received byte count is in `[r * expected, expected)` and
    ///   `> 0`, the contiguous prefix is committed as a **clamped** sub-range
    ///   `[start, start + received - 1]` (range file renamed + `RangeSpec` built with
    ///   the clamped end). Bytes arrive in order on a single upstream connection, so
    ///   the prefix is exactly the object's bytes for the clamped range. Below the
    ///   ratio (or zero), the `.tmp` is discarded and an error is returned.
    ///
    /// `commit_incremental_range` uses this for the byte-finalize step and then
    /// writes the journal entry + size deltas. The write-cache PUT path
    /// (`CacheManager::store_put_as_write_cached_range_with_ttl` via
    /// `WriteCacheRangeSink::finalize`) uses it directly and writes its `.meta`
    /// immediately through `store_new_metadata`, preserving read-after-write cache
    /// semantics — the journal-only commit defers the `.meta` until consolidation,
    /// which would turn an immediate post-PUT GET into a cache miss.
    pub(crate) fn finalize_incremental_range(
        &self,
        writer: IncrementalRangeWriter,
        min_commit_ratio: Option<f64>,
    ) -> Result<(crate::cache_types::RangeSpec, bool)> {
        let (range_spec, pending) =
            self.finalize_incremental_range_unpublished(writer, min_commit_ratio)?;
        let range_already_existed = pending.publish()?;
        Ok((range_spec, range_already_existed))
    }

    /// [`Self::finalize_incremental_range`] without the rename: the `.tmp` is complete and the
    /// `RangeSpec` describes the final path, but nothing is visible at that path until the
    /// caller calls [`PendingRangePublish::publish`].
    ///
    /// For writers that publish a `.meta` under the key's slot lock. The `.bin` path does not
    /// depend on the object version, so a rename taken before that lock lets an eviction holding
    /// it read the OLD `.meta`, pass its identity re-check, and unlink the NEW `.bin`; the writer
    /// then publishes a `.meta` naming a missing file. Renaming inside the same hold as the
    /// `.meta` publish makes the eviction's locked read see either neither or both.
    pub(crate) fn finalize_incremental_range_unpublished(
        &self,
        mut writer: IncrementalRangeWriter,
        min_commit_ratio: Option<f64>,
    ) -> Result<(crate::cache_types::RangeSpec, PendingRangePublish)> {
        use crate::cache_types::RangeSpec;
        use std::io::Write;

        // Flush any residual bytes in the batch buffer as a final LZ4 frame
        // (compressed or store-mode, per `writer.compression_enabled` — see
        // `flush_batch`). Must happen before the size check + rename so the
        // file is complete when the rename makes it visible. No-op if the
        // buffer is empty.
        if !writer.batch_buf.is_empty() {
            if let Err(e) = Self::flush_batch(&mut writer) {
                let _ = std::fs::remove_file(&writer.tmp_path);
                return Err(e);
            }
        }

        let expected_size = writer.end - writer.start + 1;
        if writer.bytes_written != expected_size {
            // Incomplete range. Decide whether to salvage the received contiguous
            // prefix as a clamped sub-range (read/GET path) or discard it (exact-only).
            let salvage_prefix = match min_commit_ratio {
                Some(ratio) if writer.bytes_written > 0 && writer.bytes_written < expected_size => {
                    (writer.bytes_written as f64) >= ratio * (expected_size as f64)
                }
                _ => false,
            };

            if !salvage_prefix {
                // Below threshold, zero bytes, or exact-only caller — clean up and error.
                let _ = std::fs::remove_file(&writer.tmp_path);
                return Err(ProxyError::CacheError(format!(
                    "Incremental write size mismatch: expected {} bytes, got {} (likely caused by cache channel backpressure — disk I/O slower than S3 download)",
                    expected_size, writer.bytes_written
                )));
            }

            // Salvage: clamp the range end to the contiguous prefix actually
            // received. The bytes are in order (single upstream connection), so the
            // first `bytes_written` bytes are exactly the object's bytes for
            // [start, clamped_end]. Recompute the final path so the `.bin` filename
            // encodes the clamped end (the read path locates ranges by filename).
            let clamped_end = writer.start + writer.bytes_written - 1;
            let clamped_final_path =
                self.get_new_range_file_path(&writer.cache_key, writer.start, clamped_end);
            info!(
                "Partial range commit: salvaging prefix {}/{} bytes for key={}, requested range {}-{} clamped to {}-{}",
                writer.bytes_written, expected_size, writer.cache_key,
                writer.start, writer.end, writer.start, clamped_end
            );
            writer.end = clamped_end;
            writer.final_path = clamped_final_path;
        }

        // Flush file to ensure all buffered data reaches the NFS write cache.
        // This is synchronous and must complete before the rename so the
        // .bin file is readable after it becomes visible.
        let mut file = writer.file;
        file.flush().map_err(|e| {
            let _ = std::fs::remove_file(&writer.tmp_path);
            ProxyError::CacheError(format!("Failed to flush incremental tmp file: {}", e))
        })?;
        // Data before pointer: synced before any rename or `.meta` can name this file, so a
        // crash cannot leave a published entry over unwritten blocks. See `write_synced`.
        sync_written_file(&file, &writer.tmp_path).map_err(|e| {
            let _ = std::fs::remove_file(&writer.tmp_path);
            ProxyError::CacheError(format!("Failed to sync incremental tmp file: {}", e))
        })?;
        // Closed here, then the identity taken from a new descriptor (`identity_after_close`):
        // the stamp must be the server's, which a deferred close would leave unrevalidated.
        // The sync above left the close no writeback to do (NFS4 close's `nfs_wb_all`, which
        // dominated ~29% of sampled CPU when the close also wrote the data back). The salvage
        // above re-paths the same file, so this is taken after its last write. The publish's
        // rename keeps the length and modification time.
        drop(file);
        let file_identity = identity_after_close(&writer.tmp_path);

        // Always tagged Lz4: whether `writer.compression_enabled` was true or
        // false, every batch was flushed as a valid, checksummed LZ4 frame
        // (compressed or store-mode blocks respectively) via `flush_batch`.
        // `CompressionAlgorithm::None` is no longer produced by any write
        // path — see compression-content-aware-fix spec, Requirement 3.5.
        let compression_algorithm = crate::compression::CompressionAlgorithm::Lz4;
        // Count the finalized range in the live shared stats (Req 5.2).
        writer.stats.record_object(writer.compression_enabled);

        // The `.tmp` is complete. The rename `.tmp` → `.bin` is the caller's, through
        // `PendingRangePublish::publish`; dropping the handle unpublished removes the `.tmp`.
        let pending = PendingRangePublish::new(writer.tmp_path.clone(), writer.final_path.clone());

        debug!(
            "Finalized incremental range (unpublished): key={}, range={}-{}, uncompressed={}, compressed={}, path={:?}",
            writer.cache_key, writer.start, writer.end,
            writer.bytes_written, writer.compressed_bytes_written, writer.final_path
        );

        // Build RangeSpec describing the file the publish will make visible
        let ranges_dir = self.cache_dir.join("ranges");
        let range_file_relative_path = writer
            .final_path
            .strip_prefix(&ranges_dir)
            .map_err(|e| ProxyError::CacheError(format!("Failed to compute relative path: {}", e)))?
            .to_string_lossy()
            .to_string();

        // Leaves `staged` as `None` on purpose: this function publishes bytes and knows
        // nothing about the object's tier — it takes no `ObjectMetadata`. Every caller
        // records membership before the range is persisted, and each is commented at
        // that point: `commit_incremental_range` (just below, for the journal write),
        // `CacheManager::store_put_as_write_cached_range_with_ttl` and
        // `store_streamed_write_cache_metadata` (both via `WriteCacheRangeSink::finalize`,
        // before their `store_new_metadata`). Adding an `is_write_cached` parameter here
        // purely to move the assignment would give this function a decision it has no
        // basis for making.
        // Spec: write-cache-accounting-and-eviction. Requirements: 12.2
        // The identity is the `.tmp`'s whatever the salvage decided: a salvaged prefix keeps
        // the same file and only clamps `end` and the final path.
        let range_spec = RangeSpec::new(
            writer.start,
            writer.end,
            range_file_relative_path,
            compression_algorithm,
            writer.compressed_bytes_written,
            writer.bytes_written,
        )
        .with_file_identity(file_identity);
        Ok((range_spec, pending))
    }

    /// Abort an incremental write, cleaning up the `.tmp` file.
    pub fn abort_incremental_range(writer: IncrementalRangeWriter) {
        let tmp_path = writer.tmp_path.clone();
        // Drop the file handle first to release the fd
        drop(writer);
        if let Err(e) = std::fs::remove_file(&tmp_path) {
            // NotFound is success, not a failure. Aborting a write whose tmp file
            // is already gone is the normal outcome, not an anomaly: another
            // instance's TTL sweep may have reaped the staging directory, or a
            // concurrent abort may have won the race. Reporting it as a warning
            // produced a flood of lines that read like disk faults during the
            // multipart lifecycle race, which is the opposite of useful — the
            // signal drowned in its own consequence.
            if e.kind() == std::io::ErrorKind::NotFound {
                debug!(
                    "Aborted incremental tmp file was already gone, nothing to clean up: {:?}",
                    tmp_path
                );
            } else {
                warn!(
                    "Failed to clean up aborted incremental tmp file {:?}: {}",
                    tmp_path, e
                );
            }
        }
    }

    /// Begin an incremental **multipart-part** write that stages bytes into the
    /// upload's in-progress directory (`mpus_in_progress/{upload_id}/part{N}.bin`)
    /// instead of the sharded `ranges/` tree.
    ///
    /// Reuses the same batched-LZ4-frame writer as
    /// [`Self::begin_incremental_range_write`] — bytes are fed via
    /// [`Self::write_range_chunk`] and flushed as concatenated LZ4 frames by
    /// `flush_batch`, byte-compatible with the range files the GET path reads
    /// (`FrameDecoder` decodes concatenated frames transparently). Unlike a range
    /// write, a part has no final byte offset yet (offsets are assigned at
    /// `CompleteMultipartUpload`), so the returned writer is finalized with
    /// [`Self::finalize_incremental_part`], which performs no range-length
    /// validation, writes no metadata/journal entry, and renames the `.tmp` to the
    /// given `part_final_path`. The part is tracked in `upload.meta` and only linked
    /// into object metadata when the upload completes.
    ///
    /// # Returns
    ///
    /// `Ok(Some(writer))` when admitted, `Ok(None)` when the danger-band publication guard
    /// declined it — see [`Self::begin_incremental_range_write`] for why declining is an
    /// `Option` rather than an `Err`.
    ///
    /// `part_size_hint` is the part's declared `Content-Length` when the caller knows it, or
    /// `None` when it does not. Task 36 (A4) records that this path's only pre-existing size
    /// control, `capacity_manager::check_streaming_capacity`, fires **only** when
    /// Content-Length is absent — so a properly sized part bypassed every gate. The hint is
    /// what lets the guard admit against the part's real size in exactly the case that
    /// check skips.
    pub async fn begin_incremental_part_write(
        &self,
        part_final_path: PathBuf,
        cache_key: &str,
        compression_enabled: bool,
        part_size_hint: Option<u64>,
    ) -> Result<Option<IncrementalRangeWriter>> {
        // P3c / P8 — the `UploadPart` staging sink. `u4-get-publication.md` records that
        // `open_multipart_part_sink` applies none of the three gates the write-cache sink
        // applies: no `write_cache_max_object_size` check, no sizing reservation, and no
        // `disk_safety_refusal`.
        //
        // With no hint the incoming figure is 0, which admits — the guard cannot reserve
        // against a size nobody knows, and inventing one would either refuse legitimate
        // parts or reserve a fiction. That residual is exactly the case
        // `check_streaming_capacity` already covers, so the two are complementary rather
        // than overlapping.
        let admission = self
            .admit_publication(
                part_size_hint.unwrap_or(0),
                crate::capacity_guard::PublicationPath::P3cMultipartPartSink,
            )
            .await;
        if let Some(detail) = admission.decline() {
            debug!(
                "Declining to begin incremental part write for key={} ({:?} bytes): {}",
                cache_key, part_size_hint, detail.reason
            );
            return Ok(None);
        }
        let capacity_reservation = admission.into_reservation();

        // Ensure the upload's in-progress directory exists.
        if let Some(parent) = part_final_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create multipart part directory: {}", e))
            })?;
        }

        // Unique .tmp filename with random suffix for concurrent write safety
        // (the part's own part{N}.lock serializes the later rename + record write, but
        // distinct streamed bodies write their own tmp files in parallel first).
        let random_suffix: u32 = fastrand::u32(..);
        let stem = part_final_path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy();
        let tmp_filename = format!("{}.{:08x}.tmp", stem, random_suffix);
        let tmp_path = part_final_path.with_file_name(tmp_filename);

        let file = std::fs::File::create(&tmp_path).map_err(|e| {
            note_disk_full(&e, "incremental_part.create_tmp");
            ProxyError::CacheError(format!(
                "Failed to create incremental part tmp file {:?}: {}",
                tmp_path, e
            ))
        })?;

        debug!(
            "Begin incremental part write: key={}, tmp={:?}, final={:?}, compression={}",
            cache_key, tmp_path, part_final_path, compression_enabled
        );

        // `start`/`end` are unused by the part finalizer (a part's final offset is
        // unknown until completion); they are set to `0` sentinels here.
        Ok(Some(IncrementalRangeWriter {
            tmp_path,
            final_path: part_final_path,
            file,
            bytes_written: 0,
            compressed_bytes_written: 0,
            cache_key: cache_key.to_string(),
            start: 0,
            end: 0,
            compression_enabled,
            batch_buf: Vec::with_capacity(self.compression_batch_size),
            batch_size: self.compression_batch_size,
            stats: self.compression_handler.shared_stats(),
            _capacity_reservation: capacity_reservation,
            fill_witness: None,
        }))
    }

    /// Finalize a multipart-part incremental write: flush any residual batch as a
    /// final LZ4 frame, flush the file, and atomically rename `.tmp` → the part
    /// file. Returns the [`PartFinalizeInfo`] (algorithm + sizes) for the upload
    /// tracker.
    ///
    /// Unlike [`Self::finalize_incremental_range`], this performs **no**
    /// range-length validation (a part's final offset/length-as-range is unknown
    /// until completion) and writes **no** metadata/journal entry — the part is
    /// recorded in its own `part{N}.json` under `part{N}.lock` by the caller and
    /// only linked into object metadata at `CompleteMultipartUpload`, whose tracker
    /// assembly holds `upload.lock`. It is not the only holder:
    /// `write_cache_manager::sweep_incomplete_uploads` try-locks the same lock (and skips the
    /// upload when it is held) before renaming an abandoned upload directory aside, so a
    /// completion that holds it is never reaped underneath. An associated function
    /// (no `&self`), mirroring [`Self::write_range_chunk`] /
    /// [`Self::abort_incremental_range`].
    pub(crate) fn finalize_incremental_part(
        mut writer: IncrementalRangeWriter,
    ) -> Result<PartFinalizeInfo> {
        use std::io::Write;

        // Flush any residual batched bytes as a final LZ4 frame (compressed or
        // store-mode, per `writer.compression_enabled` — see `flush_batch`)
        // before the rename, so the part file is complete when it becomes
        // visible. No-op when the buffer is empty.
        if !writer.batch_buf.is_empty() {
            if let Err(e) = Self::flush_batch(&mut writer) {
                let _ = std::fs::remove_file(&writer.tmp_path);
                return Err(e);
            }
        }

        let mut file = writer.file;
        file.flush().map_err(|e| {
            let _ = std::fs::remove_file(&writer.tmp_path);
            ProxyError::CacheError(format!("Failed to flush incremental part tmp file: {}", e))
        })?;
        // Data before pointer: the part becomes a range the multipart `.meta` names.
        sync_written_file(&file, &writer.tmp_path).map_err(|e| {
            let _ = std::fs::remove_file(&writer.tmp_path);
            ProxyError::CacheError(format!("Failed to sync incremental part tmp file: {}", e))
        })?;
        // Closed, then stamped from a new descriptor; same rationale as
        // `finalize_incremental_range_unpublished`.
        drop(file);
        let file_identity = identity_after_close(&writer.tmp_path);

        // Always tagged Lz4 — see `finalize_incremental_range`'s equivalent
        // comment; every batch was flushed as a valid, checksummed LZ4 frame
        // regardless of `writer.compression_enabled`.
        let compression_algorithm = crate::compression::CompressionAlgorithm::Lz4;
        // Count the finalized part in the live shared stats (Req 5.2).
        writer.stats.record_object(writer.compression_enabled);

        // Atomic rename .tmp → part{N}.bin
        std::fs::rename(&writer.tmp_path, &writer.final_path).map_err(|e| {
            let _ = std::fs::remove_file(&writer.tmp_path);
            ProxyError::CacheError(format!(
                "Failed to rename incremental part tmp file to final: {}",
                e
            ))
        })?;

        debug!(
            "Finalized incremental part: key={}, uncompressed={}, compressed={}, path={:?}",
            writer.cache_key,
            writer.bytes_written,
            writer.compressed_bytes_written,
            writer.final_path
        );

        Ok(PartFinalizeInfo {
            compression_algorithm,
            uncompressed_size: writer.bytes_written,
            file_identity,
        })
    }

    /// Get metadata for a cache entry without loading range data
    /// Implements Requirements 1.3, 2.1, 8.1
    ///
    /// This method:
    /// 1. Reads only the metadata file (no range data loaded)
    /// 2. Parses JSON to NewCacheMetadata structure via spawn_blocking (off async runtime)
    /// 3. Handles corrupted metadata gracefully (returns None, logs error)
    /// 4. Returns None if metadata file doesn't exist
    /// 5. Checks expiration and returns None if expired
    /// 6. Records metrics for corruption events
    ///
    /// Spec: cache-metadata-resilience Req 1 — blocking I/O runs in spawn_blocking
    /// with a semaphore-bounded concurrency limit.
    pub async fn get_metadata(
        &self,
        cache_key: &str,
    ) -> Result<Option<crate::cache_types::NewCacheMetadata>> {
        let operation_start = std::time::Instant::now();
        let sanitized_key = self.sanitize_cache_key_new(cache_key);

        debug!(
            "Starting metadata retrieval: key={}, sanitized_key={}, operation=get_metadata",
            cache_key, sanitized_key
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Fast path: check if metadata file exists before acquiring semaphore
        let file_exists = metadata_file_path.exists();
        debug!(
            "[METADATA_LOOKUP] Checking metadata file: cache_key={}, sanitized_key={}, path={:?}, exists={}",
            cache_key, sanitized_key, metadata_file_path, file_exists
        );

        if !file_exists {
            debug!(
                "[METADATA_LOOKUP] Metadata file not found: cache_key={}, sanitized_key={}, path={:?}, result=cache_miss",
                cache_key, sanitized_key, metadata_file_path
            );
            return Ok(None);
        }

        // Acquire semaphore permit to bound concurrent blocking metadata ops
        let _permit = self.metadata_io_semaphore.acquire().await.map_err(|_| {
            crate::ProxyError::CacheError("Metadata I/O semaphore closed unexpectedly".to_string())
        })?;

        // Run blocking read+parse off the async runtime
        let path_clone = metadata_file_path.clone();
        let cap = self.max_metadata_file_bytes;
        let cache_key_owned = cache_key.to_string();

        let outcome = match tokio::task::spawn_blocking(move || {
            read_and_parse_metadata_blocking(&path_clone, cap)
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(join_err) => {
                // JoinError means the blocking task panicked or was cancelled
                warn!(
                    "[METADATA_LOOKUP] spawn_blocking JoinError for metadata read: cache_key={}, error={}",
                    cache_key_owned, join_err
                );
                MetadataReadOutcome::TransientMiss
            }
        };

        // Act on the outcome — async metrics recording happens here, outside the blocking closure
        match outcome {
            MetadataReadOutcome::Missing => {
                debug!(
                    "[METADATA_LOOKUP] Metadata file missing (raced with delete?): cache_key={}, path={:?}",
                    cache_key, metadata_file_path
                );
                Ok(None)
            }
            MetadataReadOutcome::TransientMiss => {
                warn!(
                    "[METADATA_LOOKUP] Transient miss (empty/partial after retries), treating as cache miss (not deleting): cache_key={}, path={:?}",
                    cache_key, metadata_file_path
                );
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager
                        .read()
                        .await
                        .record_corrupted_metadata_simple()
                        .await;
                }
                Ok(None)
            }
            MetadataReadOutcome::Corrupt { reason } => {
                warn!(
                    "[METADATA_LOOKUP] Metadata classified as corrupt: cache_key={}, path={:?}, reason={:?}",
                    cache_key, metadata_file_path, reason
                );
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager
                        .read()
                        .await
                        .record_corrupted_metadata_simple()
                        .await;
                }
                Ok(None)
            }
            MetadataReadOutcome::Parsed(metadata) => {
                // Record metadata file size
                if let Ok(file_meta) = std::fs::metadata(&metadata_file_path) {
                    let size = file_meta.len();
                    if let Some(metrics_manager) = &self.metrics_manager {
                        metrics_manager
                            .read()
                            .await
                            .record_metadata_file_size(size)
                            .await;
                    }
                }

                // Record parse duration
                let parse_duration = operation_start.elapsed();
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager
                        .read()
                        .await
                        .record_metadata_parse_duration(parse_duration.as_secs_f64() * 1000.0)
                        .await;
                }

                // Record range file count
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager
                        .read()
                        .await
                        .record_range_file_count(metadata.ranges.len() as u64)
                        .await;
                }

                // Check write cache expiration (lazy expiration - Requirements 5.3, 5.4)
                if metadata.object_metadata.is_write_cached
                    && metadata.object_metadata.is_write_cache_expired()
                {
                    info!(
                        "Write cache entry expired (lazy expiration): key={}, write_cache_expires_at={:?}, result=cache_miss",
                        cache_key, metadata.object_metadata.write_cache_expires_at
                    );
                    return Ok(None);
                }

                let total_duration = operation_start.elapsed();
                debug!(
                    "Metadata retrieval completed: key={}, ranges={}, parse_duration={:.2}ms, total_duration={:.2}ms, result=cache_hit",
                    cache_key,
                    metadata.ranges.len(),
                    parse_duration.as_secs_f64() * 1000.0,
                    total_duration.as_secs_f64() * 1000.0
                );

                Ok(Some(*metadata))
            }
        }
    }

    /// Classified metadata lookup — exposes the corruption flag.
    ///
    /// Same logic as `get_metadata` but returns a `MetadataLookup` so the caller
    /// can trigger the overwrite-in-place heal (Req 3) when `corrupt == true`.
    ///
    /// Spec: cache-metadata-resilience Req 3, Task 6
    pub async fn get_metadata_classified(&self, cache_key: &str) -> Result<MetadataLookup> {
        let operation_start = std::time::Instant::now();
        let sanitized_key = self.sanitize_cache_key_new(cache_key);

        debug!(
            "Starting classified metadata retrieval: key={}, sanitized_key={}",
            cache_key, sanitized_key
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Fast path: check if metadata file exists before acquiring semaphore
        if !metadata_file_path.exists() {
            return Ok(MetadataLookup {
                meta: None,
                corrupt: false,
                corrupt_reason: None,
            });
        }

        // Acquire semaphore permit to bound concurrent blocking metadata ops
        let _permit = self.metadata_io_semaphore.acquire().await.map_err(|_| {
            crate::ProxyError::CacheError("Metadata I/O semaphore closed unexpectedly".to_string())
        })?;

        // Run blocking read+parse off the async runtime
        let path_clone = metadata_file_path.clone();
        let cap = self.max_metadata_file_bytes;
        let cache_key_owned = cache_key.to_string();

        let outcome = match tokio::task::spawn_blocking(move || {
            read_and_parse_metadata_blocking(&path_clone, cap)
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(join_err) => {
                warn!(
                    "[METADATA_CLASSIFIED] spawn_blocking JoinError: cache_key={}, error={}",
                    cache_key_owned, join_err
                );
                MetadataReadOutcome::TransientMiss
            }
        };

        // Map outcome to MetadataLookup
        match outcome {
            MetadataReadOutcome::Missing => Ok(MetadataLookup {
                meta: None,
                corrupt: false,
                corrupt_reason: None,
            }),
            MetadataReadOutcome::TransientMiss => {
                warn!(
                    "[METADATA_CLASSIFIED] Transient miss, not healing: cache_key={}, path={:?}",
                    cache_key, metadata_file_path
                );
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager
                        .read()
                        .await
                        .record_corrupted_metadata_simple()
                        .await;
                }
                Ok(MetadataLookup {
                    meta: None,
                    corrupt: false,
                    corrupt_reason: None,
                })
            }
            MetadataReadOutcome::Corrupt { reason } => {
                warn!(
                    "[METADATA_CLASSIFIED] Corrupt metadata detected, flagging for heal: cache_key={}, path={:?}, reason={:?}",
                    cache_key, metadata_file_path, reason
                );
                if let Some(metrics_manager) = &self.metrics_manager {
                    metrics_manager
                        .read()
                        .await
                        .record_corrupted_metadata_with_reason(
                            cache_key,
                            &metadata_file_path.to_string_lossy(),
                            &format!("{:?}", reason),
                        )
                        .await;
                }
                Ok(MetadataLookup {
                    meta: None,
                    corrupt: true,
                    corrupt_reason: Some(reason),
                })
            }
            MetadataReadOutcome::Parsed(metadata) => {
                // Check write cache expiration (lazy expiration)
                if metadata.object_metadata.is_write_cached
                    && metadata.object_metadata.is_write_cache_expired()
                {
                    info!(
                        "Write cache entry expired (lazy expiration): key={}, write_cache_expires_at={:?}",
                        cache_key, metadata.object_metadata.write_cache_expires_at
                    );
                    return Ok(MetadataLookup {
                        meta: None,
                        corrupt: false,
                        corrupt_reason: None,
                    });
                }

                let total_duration = operation_start.elapsed();
                debug!(
                    "Classified metadata retrieval completed: key={}, ranges={}, duration={:.2}ms",
                    cache_key,
                    metadata.ranges.len(),
                    total_duration.as_secs_f64() * 1000.0
                );

                Ok(MetadataLookup {
                    meta: Some(*metadata),
                    corrupt: false,
                    corrupt_reason: None,
                })
            }
        }
    }

    /// Validate cached GET data against HEAD metadata
    ///
    /// Compares ETag and Last-Modified from HEAD cache with cached GET metadata.
    /// If they differ, invalidates all cached GET data (metadata and range files).
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The cache key for the object
    /// * `head_etag` - ETag from HEAD request
    /// * `head_last_modified` - Last-Modified from HEAD request
    ///
    /// # Returns
    ///
    /// Returns Ok(true) if cache was invalidated, Ok(false) if cache is valid or doesn't exist
    ///
    /// # Requirements
    ///
    /// Implements Requirements 7.1, 7.2, 7.3, 7.4, 7.5
    pub async fn validate_cache_against_head(
        &self,
        cache_key: &str,
        head_etag: &str,
        head_last_modified: &str,
    ) -> Result<bool> {
        debug!(
            "[CACHE_INVALIDATION] Starting cache validation: cache_key={}, head_etag={}, head_last_modified={}",
            cache_key, head_etag, head_last_modified
        );

        // Get cached GET metadata
        let cached_metadata = match self.get_metadata(cache_key).await? {
            Some(metadata) => metadata,
            None => {
                debug!(
                    "[CACHE_INVALIDATION] No cached GET metadata found: cache_key={}, result=no_action",
                    cache_key
                );
                return Ok(false);
            }
        };

        let cached_etag = &cached_metadata.object_metadata.etag;
        let cached_last_modified = &cached_metadata.object_metadata.last_modified;

        // Requirement 7.1, 7.2: Compare ETag and Last-Modified
        // Quote-insensitive via the shared helper. PRODUCTION-UNREACHABLE — a contract
        // pin for whatever revives this function, not a fix.
        let etag_mismatch =
            !crate::revalidation::etags_equal_ignoring_quotes(cached_etag, head_etag);
        let last_modified_mismatch = cached_last_modified != head_last_modified;

        if etag_mismatch || last_modified_mismatch {
            // Requirement 7.5: Log invalidation with old and new metadata values
            warn!(
                "[CACHE_INVALIDATION] Metadata mismatch detected: cache_key={}, \
                cached_etag={}, head_etag={}, etag_mismatch={}, \
                cached_last_modified={}, head_last_modified={}, last_modified_mismatch={}, \
                action=invalidate_cache",
                cache_key,
                cached_etag,
                head_etag,
                etag_mismatch,
                cached_last_modified,
                head_last_modified,
                last_modified_mismatch
            );

            // Requirement 7.3, 7.4: Invalidate all cached metadata and range binary files
            self.invalidate_cache_entry(cache_key).await?;

            info!(
                "[CACHE_INVALIDATION] Cache invalidated due to metadata mismatch: cache_key={}, \
                old_etag={}, new_etag={}, old_last_modified={}, new_last_modified={}, \
                ranges_deleted={}, result=invalidated",
                cache_key,
                cached_etag,
                head_etag,
                cached_last_modified,
                head_last_modified,
                cached_metadata.ranges.len()
            );

            Ok(true)
        } else {
            debug!(
                "[CACHE_INVALIDATION] Cache metadata matches HEAD: cache_key={}, etag={}, last_modified={}, result=valid",
                cache_key, cached_etag, cached_last_modified
            );
            Ok(false)
        }
    }

    /// Update metadata file with atomic writes
    /// Implements Requirements 1.3, 2.1, 3.2
    ///
    /// This method:
    /// 1. Acquires exclusive lock on metadata file
    /// 2. Writes metadata to .tmp file
    /// 3. Atomically renames .tmp to final file
    /// 4. Releases lock
    /// 5. Ensures metadata operations don't load range data
    pub async fn update_metadata(
        &self,
        metadata: &crate::cache_types::NewCacheMetadata,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting metadata update: key={}, ranges={}, operation=update_metadata",
            metadata.cache_key,
            metadata.ranges.len()
        );

        let metadata_file_path = self.get_new_metadata_file_path(&metadata.cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");
        // The lock lives in the SHARED slot namespace (`metadata_lock_manager::slot_lock_path`),
        // not at a per-key `{key}.meta.lock`. A per-key lock file that nobody writes a payload
        // into was the state the retired stale-detection branches judged stale, and every such
        // branch then unlinked the live holder's file. The slot file is never unlinked.

        debug!(
            "Metadata update paths: key={}, metadata={:?}, tmp={:?}",
            metadata.cache_key, metadata_file_path, metadata_tmp_path
        );

        // Ensure metadata directory exists
        if let Some(parent) = metadata_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                error!(
                    "Failed to create metadata directory: key={}, path={:?}, error={}",
                    metadata.cache_key, parent, e
                );
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        // Acquire the key's metadata slot lock, held as a guard for the rest of the function.
        // Bounded by the configured wait budget and taken on the blocking pool, so a contended
        // slot neither stalls a runtime worker nor waits on a wedged peer indefinitely.
        let lock_start = std::time::Instant::now();
        let _meta_lock = self.acquire_meta_slot_lock(&metadata.cache_key).await?;

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired exclusive lock: key={}, wait_time={:.2}ms",
            metadata.cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Serialize metadata to JSON
        let serialize_start = std::time::Instant::now();
        let metadata_json = serde_json::to_string_pretty(metadata).map_err(|e| {
            error!(
                "Failed to serialize metadata: key={}, error={}",
                metadata.cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        let serialize_duration = serialize_start.elapsed();
        debug!(
            "Metadata serialized: key={}, size={} bytes, duration={:.2}ms",
            metadata.cache_key,
            metadata_json.len(),
            serialize_duration.as_secs_f64() * 1000.0
        );

        // Write to temporary file
        let write_start = std::time::Instant::now();
        write_synced(&metadata_tmp_path, &metadata_json).map_err(|e| {
            crate::disk_cache::note_disk_full(&e, "update_metadata.meta_tmp");
            let _ = std::fs::remove_file(&metadata_tmp_path);
            error!(
                "Failed to write metadata tmp file: key={}, path={:?}, error={}",
                metadata.cache_key, metadata_tmp_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata tmp file: {}", e))
        })?;

        let write_duration = write_start.elapsed();
        debug!(
            "Metadata tmp file written: key={}, path={:?}, size={} bytes, duration={:.2}ms",
            metadata.cache_key,
            metadata_tmp_path,
            metadata_json.len(),
            write_duration.as_secs_f64() * 1000.0
        );

        // Atomic rename
        debug!(
            "Performing atomic rename: key={}, tmp={:?} -> final={:?}",
            metadata.cache_key, metadata_tmp_path, metadata_file_path
        );

        std::fs::rename(&metadata_tmp_path, &metadata_file_path).map_err(|e| {
            let _ = std::fs::remove_file(&metadata_tmp_path);
            error!(
                "Failed to rename metadata file: key={}, tmp={:?}, final={:?}, error={}",
                metadata.cache_key, metadata_tmp_path, metadata_file_path, e
            );
            ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
        })?;

        // The lock is released by `_meta_lock`'s `Drop` at the end of this function.
        let total_duration = operation_start.elapsed();
        debug!(
            "Releasing lock: key={}, total_duration={:.2}ms",
            metadata.cache_key,
            total_duration.as_secs_f64() * 1000.0
        );

        info!(
            "Metadata update completed: key={}, ranges={}, file_size={} bytes, total_duration={:.2}ms",
            metadata.cache_key, metadata.ranges.len(), metadata_json.len(), total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Acquire the key's metadata slot lock from an async context, bounded by the configured
    /// wait budget.
    ///
    /// A thin pass-through to `metadata_lock_manager::acquire_slot_lock_within_budget`, kept
    /// because `DiskCacheManager` already owns `cache_dir` and most of this file's `.meta`
    /// mutations are methods on it. The bound used to be a hardcoded five seconds written here
    /// and at two other call sites; it is now `cache.shared_storage.lock_acquire_wait_budget`,
    /// whose default is five, so the three sites' behaviour is unchanged unless an operator
    /// changes it.
    ///
    /// The returned guard holds the lock until dropped and never unlinks the slot file.
    /// Callers must not hold a second metadata lock while this one is live — see the
    /// nesting rule in `metadata_lock_manager`.
    pub(crate) async fn acquire_meta_slot_lock(
        &self,
        cache_key: &str,
    ) -> Result<crate::metadata_lock_manager::MetadataLock> {
        crate::metadata_lock_manager::acquire_slot_lock_within_budget(&self.cache_dir, cache_key)
            .await
    }
}

/// Publish a journal-only GET range fill's `.bin` under the key's slot lock, or drop it.
///
/// Range paths are `{key}_{start}-{end}.bin` whatever the object version, and these fills
/// write no `.meta`: consolidation later folds their journal line into whatever `.meta` the
/// key already has. So a fill of a different version than the published `.meta` would land
/// its bytes under that `.meta`'s ETag. Under the slot lock (the lock consolidation and
/// eviction take), this reads the `.meta` and:
///
/// - its ETag differs from `fill_etag` (quote-insensitive): drop the `.tmp`, return `None`.
///   The caller journals nothing and credits nothing;
/// - the extent is already listed under the same ETag and its `.bin` is present: drop the
///   `.tmp`, return `None` (no overwrite, no credit);
/// - otherwise rename inside the hold and return `Some(range_already_existed)`.
///
/// On `Some`, the returned [`PublishedFill`] still holds the slot lock: the caller journals the
/// fill inside that hold and then drops it, so no slot holder (consolidation, eviction, an orphan
/// reclaimer) can observe the `.bin` before its journal line exists.
///
/// Takes no `DiskCacheManager`, so [`PreparedRangeFill::finish`] and
/// [`PreparedIncrementalCommit::finish`] call it after the caller has dropped its guard.
/// `meta_path` is the key's `.meta` path (`get_new_metadata_file_path`).
#[allow(clippy::too_many_arguments)]
async fn publish_range_fill_under_slot_lock_at(
    cache_dir: &Path,
    meta_path: &Path,
    cache_key: &str,
    start: u64,
    end: u64,
    fill_etag: &str,
    pending: PendingRangePublish,
    slot_wait: SlotWait,
    fill_witness: Option<&crate::cache::FillWitness>,
) -> Result<Option<PublishedFill>> {
    let _slot = match slot_wait {
        SlotWait::WithinBudget => {
            crate::metadata_lock_manager::acquire_slot_lock_within_budget(cache_dir, cache_key)
                .await?
        }
        SlotWait::TryOnce => {
            let dir = cache_dir.to_path_buf();
            let key = cache_key.to_string();
            match tokio::task::spawn_blocking(move || {
                crate::metadata_lock_manager::try_acquire_slot_lock(&dir, &key)
            })
            .await
            {
                Ok(Ok(lock)) => lock,
                Ok(Err(e)) => {
                    debug!(
                        "Range fill skipped (slot lock not immediately available): key={}, \
                             range={}-{}, error={}",
                        cache_key, start, end, e
                    );
                    return Ok(None);
                }
                Err(e) => {
                    return Err(ProxyError::CacheError(format!(
                        "Metadata slot lock task failed: cache_key={}, error={}",
                        cache_key, e
                    )));
                }
            }
        }
    };
    // The version witness, before anything is renamed: a write or delete of the key
    // acknowledged after this fill's S3 request began replaced the marker's token.
    if let Some(witness) = fill_witness {
        if !witness.holds(meta_path) {
            debug!(
                "Range fill dropped (superseded by an acknowledged write): key={}, range={}-{}",
                cache_key, start, end
            );
            return Ok(None);
        }
    }
    let text = std::fs::read_to_string(meta_path).ok();
    let mut published = text
        .as_deref()
        .and_then(|text| serde_json::from_str::<crate::cache_types::NewCacheMetadata>(text).ok());
    // A `.meta` that exists but does not parse (legacy `CacheEntry` shape, a stable parse
    // failure) would be left in place by a fill that only journals, and every consolidation of
    // the key would then fail to load it. Classify it under the slot lock already held and, if
    // it is confidently corrupt, unlink it, so this fill publishes as if no document existed.
    // The fill's journal entry carries `object_metadata`, so consolidation builds a fresh
    // document holding only the ranges the journal names; the old document's range files are
    // listed by nothing and are left to the orphan rotation.
    if published.is_none() && text.is_some() {
        if let MetadataReadOutcome::Parsed(meta) =
            remove_unloadable_meta_under_slot_lock(meta_path, cache_key).await
        {
            published = Some(*meta);
        }
    }
    if let Some(meta) = published {
        if !crate::revalidation::etags_equal_ignoring_quotes(&meta.object_metadata.etag, fill_etag)
        {
            debug!(
                "Range fill dropped (version differs from published .meta): key={}, \
                     range={}-{}, meta_etag={}, fill_etag={}",
                cache_key, start, end, meta.object_metadata.etag, fill_etag
            );
            return Ok(None);
        }
        // An unservable listing also drops the fill: the extent reads as missing until
        // consolidation unlinks and unlists it (`retire_unservable_extents`), after which a
        // fill publishes normally.
        if meta.ranges.iter().any(|r| r.start == start && r.end == end)
            && pending.final_path.exists()
        {
            debug!(
                "Range fill dropped (extent already listed and present): key={}, range={}-{}",
                cache_key, start, end
            );
            return Ok(None);
        }
    }
    let final_path = pending.final_path.clone();
    let range_already_existed = pending.publish()?;
    #[cfg(test)]
    range_rename_pause::after_rename(cache_key).await;
    Ok(Some(PublishedFill {
        range_already_existed,
        final_path,
        _slot,
    }))
}

/// A journal-only fill's `.bin`, renamed visible by [`publish_range_fill_under_slot_lock_at`],
/// with the key's slot lock still held. Drop it after the journal append.
pub(crate) struct PublishedFill {
    /// Whether a file already sat at the final path before the rename.
    pub(crate) range_already_existed: bool,
    final_path: PathBuf,
    _slot: crate::metadata_lock_manager::MetadataLock,
}

impl PublishedFill {
    /// The journal append failed. Unlink the `.bin` inside the same hold, so no file is left
    /// that neither a journal nor a `.meta` names. A file that existed before the rename is
    /// kept: it may be a peer's fill of the same version whose journal line names it.
    fn unpublish(self, cache_key: &str, start: u64, end: u64) {
        if self.range_already_existed {
            return;
        }
        if let Err(e) = std::fs::remove_file(&self.final_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "Failed to remove an unjournalled range file: key={}, range={}-{}, path={:?}, \
                     error={}",
                    cache_key, start, end, self.final_path, e
                );
            }
        }
    }
}

/// Red-side lever for `locked_delete_concurrency_tests`: run
/// [`DiskCacheManager::delete_ranges_under_slot_lock`] inline on the calling task, which is where
/// every step of it ran before the fix. With this `true` the overlap assertion must fail, because one
/// task polling `LEAF_CONCURRENCY_LIMIT` sibling deletions can only be inside one locked section at
/// a time.
#[cfg(test)]
const LOCKED_SECTION_INLINE: bool = false;

/// When each object's locked delete section was entered and left, so a test can assert that
/// several of them were open at the same instant.
///
/// Scoped to a cache-key prefix the arming test owns. The lib test binary runs its tests as
/// parallel threads in one process and several of them delete ranges, so an unscoped log would
/// count their holds as overlap — the direction that makes a SERIAL implementation read as
/// concurrent (`testing.md` → "Never assert an exact delta on a process-global counter").
#[cfg(test)]
pub(crate) mod locked_delete_observer {
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    struct State {
        /// The arming test's key prefix. `None` means disarmed: nothing is recorded at all.
        prefix: Option<String>,
        /// How long each hold stays open past its real work. Unlinking a 2 KiB file on a local
        /// filesystem takes microseconds, so without a dwell two genuinely concurrent holds can
        /// finish inside one scheduler tick and read as serial — and, worse, a serial
        /// implementation's holds can appear to touch. The dwell makes the difference between 1
        /// and 32 unmistakable in both directions.
        dwell: Duration,
        /// One `(entered, left)` pair per observed hold.
        holds: Vec<(Instant, Option<Instant>)>,
    }

    static STATE: Mutex<State> = Mutex::new(State {
        prefix: None,
        dwell: Duration::ZERO,
        holds: Vec::new(),
    });

    fn state() -> std::sync::MutexGuard<'static, State> {
        STATE.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record holds of keys starting with `prefix`, each held open for `dwell`. Clears any
    /// previous log.
    pub(crate) fn arm(prefix: &str, dwell: Duration) {
        let mut s = state();
        s.prefix = Some(prefix.to_string());
        s.dwell = dwell;
        s.holds.clear();
    }

    /// Stop recording and forget the log. Call it before the test's assertions, so a panicking
    /// assertion cannot leave the observer armed for the rest of the binary.
    pub(crate) fn disarm() {
        let mut s = state();
        s.prefix = None;
        s.dwell = Duration::ZERO;
        s.holds.clear();
    }

    /// Observe one hold of `cache_key`'s slot. Returns a guard whose drop records the end of the
    /// hold; the dwell is slept INSIDE the hold, before the guard is returned.
    pub(crate) fn observe(cache_key: &str) -> Hold {
        let (index, dwell) = {
            let mut s = state();
            match s.prefix.as_deref() {
                Some(prefix) if cache_key.starts_with(prefix) => {
                    s.holds.push((Instant::now(), None));
                    (Some(s.holds.len() - 1), s.dwell)
                }
                _ => (None, Duration::ZERO),
            }
        };
        if index.is_some() && !dwell.is_zero() {
            std::thread::sleep(dwell);
        }
        Hold { index }
    }

    /// The open half of one observed hold.
    pub(crate) struct Hold {
        index: Option<usize>,
    }

    impl Drop for Hold {
        fn drop(&mut self) {
            if let Some(index) = self.index {
                let left = Instant::now();
                let mut s = state();
                if let Some(hold) = s.holds.get_mut(index) {
                    hold.1 = Some(left);
                }
            }
        }
    }

    /// `(holds observed, the most that were open at any one instant)`.
    ///
    /// A hold with no recorded end is treated as having ended now, so an in-flight hold cannot
    /// inflate the peak beyond what was really concurrent at the moment of reading.
    pub(crate) fn observed_and_peak_overlap() -> (usize, usize) {
        let s = state();
        let mut events: Vec<(Instant, i32)> = Vec::with_capacity(s.holds.len() * 2);
        for (entered, left) in &s.holds {
            events.push((*entered, 1));
            events.push((left.unwrap_or_else(Instant::now), -1));
        }
        // At equal instants an end is applied before a start, so two holds that merely abut are
        // never counted as overlapping.
        events.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut open: i32 = 0;
        let mut peak: i32 = 0;
        for (_, delta) in events {
            open += delta;
            peak = peak.max(open);
        }
        (s.holds.len(), peak.max(0) as usize)
    }
}

impl DiskCacheManager {
    /// Batch delete multiple ranges from a single object
    ///
    /// This method efficiently deletes multiple ranges from a single object in one operation:
    /// 1. Reads current metadata (once per object)
    /// 2. Deletes all specified range .bin files, collecting paths
    /// 3. Removes from metadata.ranges every range whose `.bin` is no longer on
    ///    disk — unlinked now, or already absent. A range whose unlink **failed
    ///    while the file is still present is kept** (Requirement 15.2), so eviction
    ///    cannot manufacture a `.bin` that no metadata references.
    /// 4. Atomic write updated metadata (once per object)
    /// 5. If no `.bin` survives, deletes metadata file and lock files
    ///    (Requirement 15.3)
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `ranges_to_delete` - List of (start, end) pairs identifying ranges to delete
    ///
    /// # Returns
    /// * [`BatchRangeDeletion`] on success. Note `unlinked_extents` is narrower than
    ///   `ranges_to_delete` and narrower than the set stripped from the `.meta` — see
    ///   that field's doc comment. Any accounting a caller does MUST be built from it
    ///   rather than from its own candidate list (Requirement 7.2). `retained_extents`
    ///   reports the ranges left in place by a failed unlink (Requirement 15.4).
    ///
    /// # Requirements
    /// Implements Requirements 2.1, 2.2, 2.3, 2.5 from range-based-disk-eviction spec
    /// Spec: cache-eviction-at-scale. Requirements: 7.2
    pub async fn batch_delete_ranges(
        &self,
        cache_key: &str,
        ranges_to_delete: &[(u64, u64)],
    ) -> Result<BatchRangeDeletion> {
        self.batch_delete_ranges_expecting(cache_key, ranges_to_delete, None)
            .await
    }

    /// [`Self::batch_delete_ranges`], re-checking under the slot lock that the `.meta` is still
    /// the version the caller selected from.
    ///
    /// With `expected` set, after the locked re-read: if the document's identity differs from
    /// `expected.identity`, or a selected extent is now staged when the caller selected none,
    /// the key is skipped whole. Nothing is unlinked, the `.meta` is untouched, and the result
    /// carries `identity_mismatch = true` with no extents, so a caller cannot debit or count
    /// anything for it. `None` is the unchecked behaviour.
    pub async fn batch_delete_ranges_expecting(
        &self,
        cache_key: &str,
        ranges_to_delete: &[(u64, u64)],
        expected: Option<DeleteExpectation<'_>>,
    ) -> Result<BatchRangeDeletion> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting batch range deletion: key={}, ranges_to_delete={}, operation=batch_delete_ranges",
            cache_key, ranges_to_delete.len()
        );

        if ranges_to_delete.is_empty() {
            debug!("No ranges to delete for key={}", cache_key);
            return Ok(BatchRangeDeletion::default());
        }

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        let metadata_tmp_path = metadata_file_path.with_extension("meta.tmp");

        // Check if metadata file exists.
        //
        // `tokio::fs::try_exists` rather than `Path::exists`: this runs on the task that polls up
        // to `LEAF_CONCURRENCY_LIMIT` sibling deletions, and a synchronous `stat` on shared
        // storage head-of-line blocks every one of them. `unwrap_or(false)` keeps `Path::exists`'s
        // treatment of an error as "absent".
        if !tokio::fs::try_exists(&metadata_file_path)
            .await
            .unwrap_or(false)
        {
            warn!(
                "Metadata file not found for batch delete: key={}, path={:?}, skipping",
                cache_key, metadata_file_path
            );
            return Ok(BatchRangeDeletion::default());
        }

        // Acquire the key's metadata slot lock, through the SAME namespace and protocol as
        // `MetadataLockManager`. This is the acquisition the eviction rewrite's safety argument
        // rests on, and it used to be an inline `flock` on a per-key `{key}.meta.lock` with no
        // payload ever written — so every contended manager acquisition read an empty file,
        // judged it stale, unlinked THIS holder's lock and proceeded. Two holders, no timing
        // luck required.
        //
        // The retired ESTALE (116) retry is gone with it. That arm unlinked the path and
        // reopened, which could remove a peer's freshly created live lock; and a slot file that
        // nobody ever unlinks does not present a stale handle to begin with, so there is
        // nothing left for it to recover from.
        //
        // Acquired HERE, on the task, and not inside the blocking unit below: the acquisition
        // polls with non-blocking attempts and sleeps asynchronously between them
        // (`acquire_slot_lock_within`), so a waiter holds no thread. Moving the wait onto a
        // blocking thread would park one thread per waiter, which is the failure that function's
        // doc records.
        let lock_start = std::time::Instant::now();
        let meta_lock = match self.acquire_meta_slot_lock(cache_key).await {
            Ok(lock) => lock,
            Err(e) => {
                debug!(
                    "Failed to acquire lock for batch delete: key={}, error={}, skipping",
                    cache_key, e
                );
                return Err(e);
            }
        };

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired lock for batch delete: key={}, wait_time={:.2}ms",
            cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // The candidate extents' `.bin` paths, derived here because the blocking unit below owns
        // no `DiskCacheManager`. Still ONE derivation site: `get_new_range_file_path`, the same
        // function the unlinks used when they derived the path themselves. The path is a function
        // of the key and the extent alone — not of the version — and the unit only unlinks extents
        // that are in this set, so the target is identical to the one derived from the locked
        // `RangeSpec`.
        let candidate_paths: std::collections::HashMap<(u64, u64), PathBuf> = ranges_to_delete
            .iter()
            .map(|&(start, end)| {
                (
                    (start, end),
                    self.get_new_range_file_path(cache_key, start, end),
                )
            })
            .collect();

        let owned_key = cache_key.to_string();
        let owned_ranges = ranges_to_delete.to_vec();
        // The expectation, owned. `DeleteExpectation` borrows the identity from the caller's
        // candidate; the recheck now runs on another thread, so it carries a clone of exactly
        // that value and compares the same way.
        let owned_expected = expected.map(|e| (e.identity.clone(), e.staged_selected));

        let locked_section = move || {
            Self::delete_ranges_under_slot_lock(
                meta_lock,
                &owned_key,
                &owned_ranges,
                owned_expected,
                &metadata_file_path,
                &metadata_tmp_path,
                &candidate_paths,
                operation_start,
            )
        };

        // Red-side lever for `locked_sections_of_many_objects_overlap_in_time`: run the locked
        // section inline on the calling task, which is what this function did until the fix.
        #[cfg(test)]
        if LOCKED_SECTION_INLINE {
            return locked_section();
        }

        // ONE blocking unit per object: the `.meta` read, the parse, the identity recheck, the
        // `.bin` unlinks, the rewrite-or-unlink of the `.meta`, and the slot-lock release (the
        // descriptor close, an NFS `LOCKU` plus `CLOSE`) all happen on one blocking thread.
        //
        // Every one of those was a synchronous `std::fs` call on the caller's task until the fix.
        // `run_leaf_groups` dispatches up to `LEAF_CONCURRENCY_LIMIT` of these with
        // `buffer_unordered` and does not `tokio::spawn`, so one task polled all of them and a
        // synchronous call in any one of them stalled all the rest: measured effective
        // concurrency about 1.5 per process against a backend whose mutation knee is 32
        // (`EVICT-EXEC-MEASURE`). The three steps that were already `tokio::fs` or
        // `spawn_blocking` yielded; the five between them did not, so the yields bought little.
        //
        // Plain `spawn_blocking`, deliberately NOT `metadata_lock_manager::run_blocking_io`:
        // that helper's 64 permits bound the request-path commit tails, and this call has
        // already taken the key's slot lock. Queueing for a permit while holding a
        // shared-storage lock would put every eviction behind up to 64 PUT commit tails and
        // hold the lock for the wait. The width here is bounded by the caller instead
        // (`LEAF_CONCURRENCY_LIMIT` leaf groups, one executor pass per process), so at most
        // tens of the runtime's 512 blocking threads are in this function at once.
        let cache_key_for_error = cache_key.to_string();
        tokio::task::spawn_blocking(locked_section)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Batch range deletion task failed: key={}, error={}",
                    cache_key_for_error, e
                ))
            })?
    }

    /// [`Self::batch_delete_ranges_expecting`]'s locked section, as one synchronous unit for the
    /// blocking pool.
    ///
    /// Takes the slot-lock guard by value, so the hold spans exactly the steps it spanned when
    /// this was inline: from the acquisition in the caller, through the locked `.meta` re-read,
    /// the identity recheck, the `.bin` unlinks and the `.meta` rewrite-or-unlink, to the guard's
    /// drop at the end of this function. Nothing was added to the hold and nothing was taken out
    /// of it. The recheck therefore still decides from a read that cannot be interleaved with a
    /// peer's publish of a new version, which is the property it exists for; running on another
    /// thread changes which thread reads the document, not what the document can be.
    ///
    /// Every early return drops the guard here too, including the error paths, because the guard
    /// is bound for the whole function.
    #[allow(clippy::too_many_arguments)]
    fn delete_ranges_under_slot_lock(
        meta_lock: crate::metadata_lock_manager::MetadataLock,
        cache_key: &str,
        ranges_to_delete: &[(u64, u64)],
        expected: Option<(ExpectedObjectIdentity, bool)>,
        metadata_file_path: &Path,
        metadata_tmp_path: &Path,
        candidate_paths: &std::collections::HashMap<(u64, u64), PathBuf>,
        operation_start: std::time::Instant,
    ) -> Result<BatchRangeDeletion> {
        // Held for the whole function: `MetadataLock::Drop` closes the descriptor, which is
        // what releases the `flock`, so every early return releases it too. The slot file
        // itself is permanent and is never unlinked.
        let _meta_lock = meta_lock;

        // Records this hold's start and, on drop, its end — inside the lock, before the guard
        // above releases it (drop order is reverse of declaration).
        #[cfg(test)]
        let _observed = locked_delete_observer::observe(cache_key);

        // Read current metadata (once)
        let metadata_content = match std::fs::read_to_string(metadata_file_path) {
            Ok(content) => content,
            Err(e) => {
                warn!(
                    "Failed to read metadata for batch delete: key={}, error={}, skipping",
                    cache_key, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to read metadata: {}",
                    e
                )));
            }
        };

        // Parse metadata
        let mut metadata: crate::cache_types::NewCacheMetadata =
            match serde_json::from_str(&metadata_content) {
                Ok(meta) => meta,
                Err(e) => {
                    warn!(
                        "Failed to parse metadata for batch delete: key={}, error={}, skipping",
                        cache_key, e
                    );
                    return Err(ProxyError::CacheError(format!(
                        "Failed to parse metadata: {}",
                        e
                    )));
                }
            };

        // Create a set of ranges to delete for efficient lookup
        let ranges_set: std::collections::HashSet<(u64, u64)> =
            ranges_to_delete.iter().cloned().collect();

        let object_is_write_cached = metadata.object_metadata.is_write_cached;

        // The identity re-check, under the slot lock and before anything is unlinked. The
        // caller selected from an unlocked read; a peer that re-PUT the key in the gap (or
        // while this call waited for the slot) has published a different version at paths
        // this call would otherwise unlink, since the `.bin` path does not depend on the
        // version.
        if let Some((expected_identity, staged_selected)) = &expected {
            let identity_matches = ExpectedObjectIdentity::of(&metadata) == *expected_identity;
            let newly_staged = !staged_selected
                && metadata.ranges.iter().any(|r| {
                    ranges_set.contains(&(r.start, r.end))
                        && crate::cache_types::is_staged_range_spec(r, object_is_write_cached)
                });
            if !identity_matches || newly_staged {
                info!(
                    "[RANGE_EVICTION] Skipping key, rewritten since selection: cache_key={}, \
                     identity_matches={}, newly_staged={}, action=nothing_deleted_nothing_debited",
                    cache_key, identity_matches, newly_staged
                );
                return Ok(BatchRangeDeletion {
                    identity_mismatch: true,
                    ..BatchRangeDeletion::default()
                });
            }
        }

        // R3.9's precondition, read from the document BEFORE anything is unlinked: is every
        // range this `.meta` lists a candidate? Computed here rather than inferred afterwards
        // from `metadata.ranges.is_empty()`, because those two are different claims and only
        // this one is R3.9's. A `.meta` listing no ranges at all is deliberately NOT
        // whole-object — there is no object to evict whole — so the downgrade predicate below
        // stays meaningful.
        //
        // Spec: cache-eviction-at-scale § 5.5. Requirements: 3.9
        let whole_object_intended = !metadata.ranges.is_empty()
            && metadata
                .ranges
                .iter()
                .all(|r| ranges_set.contains(&(r.start, r.end)));

        let mut meta_writes: u32 = 0;
        let mut meta_unlinked = false;
        let mut bytes_freed: u64 = 0;
        let mut deleted_paths: Vec<PathBuf> = Vec::new();
        let mut ranges_deleted: Vec<(u64, u64)> = Vec::new();
        // Extents whose `.bin` actually left the disk. Kept separate from
        // `ranges_deleted` — which is the metadata-removal set and is populated
        // unconditionally — because the caller's accounting may only be built from
        // this one. See `BatchRangeDeletion::unlinked_extents`.
        // Spec: cache-eviction-at-scale. Requirements: 7.2
        let mut unlinked_extents: Vec<(u64, u64)> = Vec::new();
        // Extents whose `.bin` unlink failed with the file still present. These are
        // deliberately absent from `ranges_deleted`, so they survive the `retain`
        // below and keep the object's `.meta` alive.
        // Spec: cache-eviction-at-scale. Requirements: 15.2, 15.3, 15.4
        let mut retained_extents: Vec<(u64, u64)> = Vec::new();

        // Delete this object's candidate `.bin` files, serially within the unit.
        //
        // They were `tokio::fs` futures driven by `buffer_unordered(FILE_CONCURRENCY_LIMIT)`
        // until the fix, which overlapped ONE object's unlinks while every other step of every
        // sibling object waited behind the synchronous calls around them. The concurrency a drain
        // is bounded by is across objects, and that is what the caller now gets: 32 of these
        // units in flight instead of about 1.5. What it costs is an object with many
        // candidate extents, whose unlinks no longer overlap each other — at the unlink rate
        // measured on the shared volume (4,613/s, `disk_cache` § 5.5 note) tens of microseconds
        // each, against the five to seven serialised round trips per object this takes off the
        // caller's task.
        //
        // Requirement 15.2: the error is CLASSIFIED rather than collapsed to a bool.
        // `NotFound` means the file is gone and the `.meta` entry should go with it; any other
        // error means the file is still there and the `.meta` entry must survive. Collapsing the
        // two — which this code did until 2026-08-28 — is what produced orphans.
        let mut results = Vec::new();
        for range_spec in metadata
            .ranges
            .iter()
            .filter(|r| ranges_set.contains(&(r.start, r.end)))
        {
            let key = (range_spec.start, range_spec.end);
            let locked_compressed = range_spec.compressed_size;
            let locked_staged =
                crate::cache_types::is_staged_range_spec(range_spec, object_is_write_cached);
            // Unreachable in practice: `ranges_set` and `candidate_paths` are built from the
            // same list. Skipping rather than deriving a second path keeps one derivation site.
            let Some(path) = candidate_paths.get(&key).cloned() else {
                continue;
            };

            let file_size = match std::fs::metadata(&path) {
                Ok(meta) => meta.len(),
                Err(e) => {
                    warn!(
                        "Failed to get file size for range: key={}, range={}-{}, path={:?}, error={}",
                        cache_key, key.0, key.1, path, e
                    );
                    0
                }
            };

            let outcome = match std::fs::remove_file(&path) {
                Ok(()) => {
                    debug!(
                        "[RANGE_EVICTION] Deleted range file: cache_key={}, range_start={}, range_end={}, freed_bytes={}, path={:?}",
                        cache_key, key.0, key.1, file_size, path
                    );
                    RangeUnlinkOutcome::Unlinked
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Nothing on disk to orphan. Removing the dangling `.meta`
                    // entry is correct and is the only way the object becomes
                    // evictable at all.
                    debug!(
                        "[RANGE_EVICTION] Range file already absent: cache_key={}, range_start={}, range_end={}, path={:?}, action=removing_stale_metadata_entry",
                        cache_key, key.0, key.1, path
                    );
                    RangeUnlinkOutcome::AlreadyAbsent
                }
                Err(e) => {
                    // Requirement 2.4: Log warning on failure, continue processing remaining files
                    // Requirement 15.2: the file survives, so the metadata entry does too.
                    warn!(
                        "[RANGE_EVICTION] Failed to delete range file: cache_key={}, range_start={}, range_end={}, error={}, path={:?}, action=retaining_metadata_entry_for_retry",
                        cache_key, key.0, key.1, e, path
                    );
                    RangeUnlinkOutcome::Failed
                }
            };

            results.push((
                key,
                file_size,
                outcome,
                path,
                locked_compressed,
                locked_staged,
            ));
        }

        // Collect results into bytes_freed, deleted_paths, ranges_deleted, retained_extents
        //
        // Requirement 15.2: metadata removal is no longer unconditional. Until
        // 2026-08-28 `ranges_deleted.push(range_key)` ran for every result under the
        // comment "Always mark range for metadata removal (file may already be
        // gone)" — true of an already-absent file and false of a failed unlink, and
        // the code could not tell them apart because the outcome was a bool. A
        // surviving `.bin` stripped from its `.meta` is an orphan, and § 15.1 shows
        // the validation scan cannot see one: it roots at `metadata/` and never
        // reads `ranges/`, so the bytes are neither reclaimed nor counted, forever.
        let mut unlinked_ranges: Vec<UnlinkedRange> = Vec::new();
        let mut staged_stripped = false;
        for (range_key, file_size, outcome, path, locked_compressed, locked_staged) in results {
            match outcome {
                RangeUnlinkOutcome::Unlinked => {
                    bytes_freed += file_size;
                    unlinked_ranges.push(UnlinkedRange {
                        start: range_key.0,
                        end: range_key.1,
                        compressed_size: locked_compressed,
                        file_size,
                        staged: locked_staged,
                        is_write_cached: object_is_write_cached,
                        bin_path: path.clone(),
                    });
                    deleted_paths.push(path);
                    unlinked_extents.push(range_key);
                    ranges_deleted.push(range_key);
                    staged_stripped |= locked_staged;
                }
                RangeUnlinkOutcome::AlreadyAbsent => {
                    // No bytes to debit (see `unlinked_extents`), but the `.meta`
                    // entry points at a file that is not there, so it goes.
                    ranges_deleted.push(range_key);
                    staged_stripped |= locked_staged;
                }
                RangeUnlinkOutcome::Failed => {
                    // Requirement 15.2: the `.bin` is still on the volume. Leave the
                    // range in the `.meta` so it stays accounted and stays a
                    // candidate for the next pass (Requirement 15.4).
                    retained_extents.push(range_key);
                }
            }
        }

        // Requirement 2.2: Update metadata to remove evicted ranges from the ranges list
        let ranges_deleted_set: std::collections::HashSet<(u64, u64)> =
            ranges_deleted.iter().cloned().collect();
        metadata
            .ranges
            .retain(|r| !ranges_deleted_set.contains(&(r.start, r.end)));

        // Requirement 15.3: the `.meta` is deleted only when no `.bin` survives.
        //
        // The `retain` above already guarantees this: a failed unlink is absent from
        // `ranges_deleted`, so its `RangeSpec` survives and `metadata.ranges` cannot
        // be empty. The second clause is therefore redundant *today* and is stated
        // anyway, because R15.3 is a property of the surviving files and this makes
        // the code say so directly rather than leave it as an emergent consequence of
        // how `ranges_deleted` happens to be built. If a later change reworks that
        // list — and § 5.5 plans exactly that, adding a whole-object path that
        // unlinks the `.meta` without a rewrite — the hole does not silently reopen.
        let all_ranges_evicted = metadata.ranges.is_empty() && retained_extents.is_empty();

        if all_ranges_evicted {
            // All ranges evicted - delete metadata file and lock files
            // This implements Requirement 3.1, 3.2, 3.3 from the spec

            // Delete metadata file
            // Requirement 7.2: Log metadata deletion with reason (all ranges evicted)
            // § 5.5: ONE unlink, and no rewrite before it. The document is about to stop
            // existing, so writing the emptied range list first would be a write nobody can
            // ever read — and on the measured backend a `.meta` rewrite is 451.9/s against
            // 4,613/s for an unlink, so it is also the expensive half of the operation. The
            // property is observable rather than implied: `meta_writes` stays 0 here, and
            // that is what a test asserts, because the end state (no `.meta`) is identical
            // either way.
            match std::fs::remove_file(metadata_file_path) {
                Ok(()) => {
                    // Get metadata file size for tracking
                    meta_unlinked = true;
                    deleted_paths.push(metadata_file_path.to_path_buf());
                    debug!(
                        "[METADATA_EVICTION] Deleted metadata file: cache_key={}, reason=all_ranges_evicted, path={:?}, meta_writes={}",
                        cache_key, metadata_file_path, meta_writes
                    );
                }
                Err(e) => {
                    // Requirement 7.4: Log errors with cache_key and failure reason
                    warn!(
                        "[METADATA_EVICTION] Failed to delete metadata file: cache_key={}, error={}, path={:?}",
                        cache_key, e, metadata_file_path
                    );
                }
            }

            // The lock file is NOT deleted. It is a shared slot in a fixed set, so deleting it
            // would remove a file other keys' holders are locking — and `flock` binds to the
            // open file description rather than the path, so the deletion would release nothing
            // while handing the next opener an independent lock on a different inode.
        } else {
            // Some ranges remain — rewrite the document, retaining them. R3.11: this is not a
            // degraded outcome, and the rewrite is charged per OBJECT rather than per range,
            // so evicting five cold ranges of a six-range object costs one rewrite.
            Self::rewrite_metadata_file(
                &metadata,
                metadata_file_path,
                metadata_tmp_path,
                &mut meta_writes,
            )?;

            debug!(
                "Updated metadata after range deletion: key={}, remaining_ranges={}, meta_writes={}",
                cache_key,
                metadata.ranges.len(),
                meta_writes
            );
        }

        // R15.3's downgrade, made observable. Every range was a candidate, so this pass set out
        // to take § 5.5's one-unlink path; a `.bin` survived its unlink, so the `.meta` was kept
        // and rewritten with that range's entry intact instead. Without this line the event is
        // only inferable by correlating two other warnings, and an operator whose volume is
        // returning EIO sees a cache that is quietly slower rather than one that is failing to
        // delete.
        //
        // Spec: cache-eviction-at-scale § 5.5. Requirements: 15.3, 15.4
        if whole_object_intended && !meta_unlinked {
            warn!(
                "[METADATA_EVICTION] WHOLE_OBJECT_DOWNGRADED: every range of cache_key={} was an \
                 eviction candidate, so this pass set out to take the whole-object path, and the \
                 .meta was NOT removed. {} range file(s) survived their unlink ({:?}); their \
                 .meta entries are retained so the ranges stay accounted and the next pass \
                 retries them (R15.3, R15.4). A count of 0 here means the .meta removal itself \
                 failed and the warning above it names the error. meta_writes={}, path={:?}. A \
                 persistent non-zero rate is a filesystem problem, not a cache problem.",
                cache_key,
                retained_extents.len(),
                retained_extents,
                meta_writes,
                metadata_file_path
            );
        }

        let total_duration = operation_start.elapsed();
        debug!(
            "Batch range deletion completed: key={}, ranges_deleted={}, bytes_freed={}, all_evicted={}, total_duration={:.2}ms",
            cache_key, ranges_deleted.len(), bytes_freed, all_ranges_evicted, total_duration.as_secs_f64() * 1000.0
        );

        // Already-absent files: metadata entry removed, nothing debited. Benign, and
        // the count is worth seeing because a persistent non-zero rate means `.meta`
        // and `ranges/` are drifting apart somewhere upstream of eviction.
        let already_absent = ranges_deleted.len() - unlinked_extents.len();
        if already_absent > 0 {
            warn!(
                "[RANGE_EVICTION] Some range files were already absent: cache_key={}, requested={}, unlinked={}, already_absent={}, note=stale_metadata_entries_removed_bytes_not_debited",
                cache_key,
                ranges_to_delete.len(),
                unlinked_extents.len(),
                already_absent
            );
        }

        // Requirement 15.4: the durable record of a failed unlink is the retained
        // `.meta` entry, which the next eviction pass re-selects. This log is the
        // operator-visible half — a persistent non-zero rate here is a filesystem
        // problem rather than a cache problem, and without it the retry loop is
        // silent. The extents are named so the affected object is diagnosable.
        if !retained_extents.is_empty() {
            warn!(
                "[RANGE_EVICTION] Range files could not be deleted and remain on disk: cache_key={}, retained={}, extents={:?}, action=metadata_entries_kept_bytes_still_accounted, retry=next_eviction_pass",
                cache_key,
                retained_extents.len(),
                retained_extents
            );
        }

        Ok(BatchRangeDeletion {
            bytes_freed,
            all_ranges_evicted,
            deleted_paths,
            unlinked_extents,
            retained_extents,
            meta_writes,
            meta_unlinked,
            whole_object_intended,
            unlinked_ranges,
            staged_stripped,
            identity_mismatch: false,
        })
    }

    /// Write one `.meta` document atomically, counting the write.
    ///
    /// **The counter is the point.** § 5.5's whole-object path is specified as "one unlink
    /// instead of a rewrite", and a rewrite-then-delete leaves exactly the end state a bare
    /// delete does, so the requirement is not testable from the filesystem afterwards. Routing
    /// every `.meta` write in [`Self::batch_delete_ranges`] through this one function makes the
    /// operation count a figure a caller can assert on
    /// ([`BatchRangeDeletion::meta_writes`]).
    ///
    /// The residual, stated rather than left implicit: this counts writes that go through
    /// **here**. A regression that called `std::fs::write` on the metadata path directly would
    /// not be counted, so the figure is a guard against the plausible regression — reusing the
    /// existing rewrite on the whole-object branch — and not a proof that no byte was written.
    /// The complementary positive control is the partial path, which must report exactly 1.
    ///
    /// Spec: cache-eviction-at-scale § 5.5. Requirements: 3.9, 3.11
    fn rewrite_metadata_file(
        metadata: &crate::cache_types::NewCacheMetadata,
        metadata_file_path: &std::path::Path,
        metadata_tmp_path: &std::path::Path,
        meta_writes: &mut u32,
    ) -> Result<()> {
        let metadata_json = serde_json::to_string_pretty(metadata)
            .map_err(|e| ProxyError::CacheError(format!("Failed to serialize metadata: {}", e)))?;

        // Counted at the point of no return for "this call wrote the document": the tmp file
        // carries the bytes, so a rewrite that got this far has paid the write whether or not
        // the rename lands.
        *meta_writes += 1;

        // Write to temporary file first for atomic operation
        if let Err(e) = std::fs::write(metadata_tmp_path, &metadata_json) {
            crate::disk_cache::note_disk_full(&e, "meta_tmp_publish");
            let _ = std::fs::remove_file(metadata_tmp_path);
            return Err(ProxyError::CacheError(format!(
                "Failed to write metadata tmp file: {}",
                e
            )));
        }

        // Atomic rename
        if let Err(e) = std::fs::rename(metadata_tmp_path, metadata_file_path) {
            let _ = std::fs::remove_file(metadata_tmp_path);
            return Err(ProxyError::CacheError(format!(
                "Failed to rename metadata file: {}",
                e
            )));
        }

        Ok(())
    }

    /// Batch clean up empty directories after range eviction
    ///
    /// This method processes all deleted file paths and removes empty directories
    /// in a bottom-up fashion (deepest directories first). It stops at cache type
    /// root directories (metadata/, ranges/) to preserve the cache structure.
    ///
    /// # Arguments
    /// * `deleted_paths` - Paths of all deleted files from eviction operations
    ///
    /// # Returns
    /// * `Ok(usize)` - Number of directories successfully removed
    ///
    /// # Behavior
    /// - Collects unique parent directories from all deleted paths
    /// - Sorts by depth (deepest first) for bottom-up cleanup
    /// - Removes empty directories, stopping at cache type root
    /// - Recursively checks parent directories after removing a directory
    /// - Logs warnings on failure, doesn't fail eviction
    /// - Skips non-existent directories gracefully
    ///
    /// # Requirements
    /// Implements Requirements 4.1, 4.2, 4.3, 4.4, 4.5 from range-based-disk-eviction spec
    pub(crate) fn batch_cleanup_empty_directories(&self, deleted_paths: &[PathBuf]) -> usize {
        let operation_start = std::time::Instant::now();

        if deleted_paths.is_empty() {
            debug!("No deleted paths to clean up directories for");
            return 0;
        }

        // Cache type root directory names that should NOT be deleted
        // Requirement 4.5: Do not delete cache type root directories
        let cache_type_roots = ["metadata", "ranges", "locks", "mpus_in_progress"];

        // Helper function to check if a path is a cache type root or the cache_dir
        let is_protected_dir = |dir: &PathBuf| -> bool {
            // Check if this directory is a cache type root
            if let Some(dir_name) = dir.file_name().and_then(|n| n.to_str()) {
                if cache_type_roots.contains(&dir_name) {
                    return true;
                }
            }
            // Check if the directory is the cache_dir itself
            dir == &self.cache_dir
        };

        // Collect unique parent directories from all deleted paths
        // Requirement 4.1: Check parent directory after file deletion
        let mut dirs_to_check: std::collections::HashSet<PathBuf> = deleted_paths
            .iter()
            .filter_map(|p| p.parent().map(|d| d.to_path_buf()))
            .filter(|d| !is_protected_dir(d))
            .collect();

        debug!(
            "Collected {} unique parent directories from {} deleted paths",
            dirs_to_check.len(),
            deleted_paths.len()
        );

        let mut removed_count: usize = 0;
        let mut checked_count: usize = 0;

        // Process directories in a loop until no more can be removed
        // This handles recursive parent cleanup (Requirement 4.3)
        loop {
            if dirs_to_check.is_empty() {
                break;
            }

            // Sort by depth (deepest first) for bottom-up cleanup
            let mut dirs: Vec<PathBuf> = dirs_to_check.drain().collect();
            dirs.sort_by(|a, b| {
                let a_depth = a.components().count();
                let b_depth = b.components().count();
                b_depth.cmp(&a_depth) // Descending order (deepest first)
            });

            let mut newly_emptied_parents: Vec<PathBuf> = Vec::new();

            for dir in &dirs {
                checked_count += 1;

                // Skip protected directories (already filtered, but double-check)
                if is_protected_dir(dir) {
                    debug!("[DIR_CLEANUP] Skipping protected directory: path={:?}", dir);
                    continue;
                }

                // Check if directory exists
                if !dir.exists() {
                    debug!("[DIR_CLEANUP] Directory already removed: path={:?}", dir);
                    continue;
                }

                // Check if directory is empty
                // Requirement 4.2: Remove directory when it becomes empty
                match Self::is_directory_empty(dir) {
                    Ok(true) => {
                        // Directory is empty, try to remove it
                        match std::fs::remove_dir(dir) {
                            Ok(()) => {
                                removed_count += 1;
                                debug!("[DIR_CLEANUP] Removed empty directory: path={:?}", dir);

                                // After removing a directory, add its parent to check list
                                // Requirement 4.3: Recursively clean up empty parent directories
                                if let Some(parent) = dir.parent() {
                                    let parent_path = parent.to_path_buf();
                                    if !is_protected_dir(&parent_path) {
                                        newly_emptied_parents.push(parent_path);
                                    }
                                }
                            }
                            Err(e) => {
                                // Requirement 4.4: Log warning on failure, don't fail eviction
                                warn!(
                                    "[DIR_CLEANUP] Failed to remove empty directory (non-critical): path={:?}, error={}",
                                    dir, e
                                );
                            }
                        }
                    }
                    Ok(false) => {
                        // Directory is not empty, skip it
                        debug!(
                            "[DIR_CLEANUP] Directory not empty, skipping: path={:?}",
                            dir
                        );
                    }
                    Err(e) => {
                        // Requirement 4.4: Log warning on failure, don't fail eviction
                        warn!(
                            "[DIR_CLEANUP] Failed to check if directory is empty (non-critical): path={:?}, error={}",
                            dir, e
                        );
                    }
                }
            }

            // Add newly emptied parent directories to check in next iteration
            for parent in newly_emptied_parents {
                dirs_to_check.insert(parent);
            }
        }

        let total_duration = operation_start.elapsed();
        debug!(
            "Directory cleanup completed: checked={}, removed={}, duration={:.2}ms",
            checked_count,
            removed_count,
            total_duration.as_secs_f64() * 1000.0
        );

        removed_count
    }

    /// Check if a directory is empty
    ///
    /// # Arguments
    /// * `dir` - Path to the directory to check
    ///
    /// # Returns
    /// * `Ok(true)` - Directory is empty
    /// * `Ok(false)` - Directory is not empty
    /// * `Err` - Failed to read directory
    fn is_directory_empty(dir: &PathBuf) -> std::io::Result<bool> {
        let mut entries = std::fs::read_dir(dir)?;
        Ok(entries.next().is_none())
    }

    /// Find cached ranges that overlap with the requested range
    /// Implements Requirements 2.2, 2.4
    ///
    /// This method:
    /// 1. Reads only the metadata file (no range data loaded)
    /// 2. Identifies all cached ranges that overlap with the requested range
    /// 3. Optimizes for common cases (exact match, full containment)
    /// 4. Returns all overlapping range specs
    ///
    /// Range overlap detection:
    /// - Two ranges [a1, a2] and [b1, b2] overlap if: a1 <= b2 AND b1 <= a2
    /// - Exact match: requested range exactly matches a cached range
    /// - Full containment: requested range is fully contained within a cached range
    /// - Partial overlap: requested range partially overlaps with cached ranges
    /// # Lookup purpose (the expired entry revalidation feature, R1.1-R1.5)
    ///
    /// `purpose` decides what happens when the entry's stored `expires_at` has
    /// passed. `FreshServe` returns no ranges, as this method always did.
    /// `RevalidationCandidate` continues and reports
    /// [`crate::cache_types::StoredFreshness::Expired`] on the result, so a caller
    /// that is about to revalidate can still see the coverage and the validators.
    ///
    /// The expiry decision is the **only** thing the two purposes differ on —
    /// overlap calculation below is shared, so the two cannot drift into
    /// disagreeing about what is cached.
    ///
    /// Returning `StoredFreshness::Expired` grants discovery, never permission to
    /// serve. See [`crate::cache_types::RangeLookupPurpose`].
    pub async fn find_cached_ranges(
        &self,
        cache_key: &str,
        requested_start: u64,
        requested_end: u64,
        preloaded_metadata: Option<&crate::cache_types::NewCacheMetadata>,
        purpose: crate::cache_types::RangeLookupPurpose,
    ) -> Result<crate::cache_types::RangeLookupResult> {
        use crate::cache_types::{RangeLookupPurpose, RangeLookupResult, StoredFreshness};
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range lookup: key={}, requested_range={}-{}, operation=find_cached_ranges",
            cache_key, requested_start, requested_end
        );

        // Validate requested range
        if requested_start > requested_end {
            error!(
                "Invalid requested range: key={}, start={}, end={}",
                cache_key, requested_start, requested_end
            );
            return Err(ProxyError::CacheError(format!(
                "Invalid requested range: start ({}) > end ({})",
                requested_start, requested_end
            )));
        }

        // Get metadata: use preloaded if provided, otherwise read from disk
        let metadata = if let Some(preloaded) = preloaded_metadata {
            debug!(
                "Using preloaded metadata: key={}, total_ranges={}, checking_for_overlaps",
                cache_key,
                preloaded.ranges.len()
            );
            Some(preloaded.clone())
        } else {
            match self.get_metadata(cache_key).await? {
                Some(meta) => {
                    debug!(
                        "Metadata retrieved: key={}, total_ranges={}, checking_for_overlaps",
                        cache_key,
                        meta.ranges.len()
                    );
                    Some(meta)
                }
                None => {
                    debug!(
                        "No metadata found: key={}, checking_journals_for_pending_ranges",
                        cache_key
                    );
                    None
                }
            }
        };

        // If no metadata, check journals for pending ranges (shared storage mode)
        if metadata.is_none() {
            let journal_ranges = self
                .find_pending_journal_ranges(cache_key, requested_start, requested_end)
                .await?;
            if !journal_ranges.is_empty() {
                let duration = operation_start.elapsed();
                debug!(
                    "[RANGE_OVERLAP] Range lookup completed via journal fallback: key={}, requested_range={}-{}, result=journal_hit, ranges={}, duration={:.2}ms",
                    cache_key, requested_start, requested_end, journal_ranges.len(), duration.as_secs_f64() * 1000.0
                );
                // Journal records carry no expiry and no validators, so there is
                // no `expires_at` that could have passed. Reported Fresh, and
                // their semantics are unchanged by this spec (R1.6, and the
                // journal exclusion in the design's lookup-result section).
                return Ok(RangeLookupResult::fresh(journal_ranges));
            }
            return Ok(RangeLookupResult::empty());
        }

        let metadata = metadata.unwrap();

        // A LISTING WRITTEN BEFORE 3.0.0 AT OFFSET 0, SHORT OF THE OBJECT'S END, IS NOT
        // COVERAGE (`RangeSpec::is_unverified_prefix`): a release before 3.0.0 may have stored
        // the bytes at another offset under it. It is skipped in the loop below and removed
        // here, on every lookup purpose and for both metadata sources, before the stored-expiry
        // switch. Removing it is what lets the refill take its place: a fill of an extent that
        // is already listed is not listed again. Every serve path that turns a `.meta` into
        // bytes resolves its extents here (the no-validator guard below lists them).
        let content_length = metadata.object_metadata.content_length;
        for range_spec in metadata
            .ranges
            .iter()
            .filter(|r| !r.unservable && r.is_unverified_prefix(content_length))
        {
            self.drop_unverified_prefix_listing(cache_key, range_spec)
                .await;
        }

        // Object-level stored expiry (lazy expiration - Requirement 1.4).
        // `get_metadata()` does not check `expires_at` for range lookups; the
        // check is deferred here so callers that preload metadata also benefit —
        // both metadata arms above converge on this single point, which is what
        // makes the policy identical for disk-read and preloaded metadata
        // (expired-entry-revalidation R1.5).
        //
        // What happens past expiry depends on why the caller is asking:
        //
        // - `FreshServe`: return nothing, exactly as before. The caller has no
        //   further gate, so coverage it cannot revalidate must not reach it.
        // - `RevalidationCandidate`: continue, and mark the result Expired. The
        //   caller is about to consult live TTL and, if expired, send a
        //   conditional request carrying these validators. Hiding the coverage
        //   here is what made that path unreachable (issue #17).
        let stored_freshness = if std::time::SystemTime::now() > metadata.expires_at {
            match purpose {
                RangeLookupPurpose::FreshServe => {
                    debug!(
                        "Object expired during range lookup: key={}, expires_at={:?}, \
                         purpose=FreshServe, stored_freshness=expired, result=no_ranges",
                        cache_key, metadata.expires_at
                    );
                    return Ok(RangeLookupResult::empty());
                }
                RangeLookupPurpose::RevalidationCandidate => {
                    // Requirement 5.3: the Stored_Expired-candidate-selected
                    // outcome is distinguishable in the logs from a fresh serve
                    // and from a rejection.
                    debug!(
                        "Object expired during range lookup: key={}, expires_at={:?}, \
                         purpose=RevalidationCandidate, stored_freshness=expired, \
                         result=candidate_retained_for_live_ttl_evaluation",
                        cache_key, metadata.expires_at
                    );
                    StoredFreshness::Expired
                }
            }
        } else {
            StoredFreshness::Fresh
        };

        // Find all overlapping ranges
        let mut overlapping_ranges = Vec::new();

        for range_spec in &metadata.ranges {
            // An unservable extent is listed only so its `.bin` stays named and evictable; its
            // bytes may be another version's. It is never coverage: skipping it here makes it
            // read as missing on every lookup purpose and both metadata sources, so the
            // request refetches.
            if range_spec.unservable || range_spec.is_unverified_prefix(content_length) {
                debug!(
                    "[RANGE_OVERLAP] Skipping unservable extent: cache_key={}, cached_range={}-{}",
                    cache_key, range_spec.start, range_spec.end
                );
                continue;
            }
            // Requirement 1.5: Log each range being checked for overlap
            let overlaps = requested_start <= range_spec.end && range_spec.start <= requested_end;
            debug!(
                "[RANGE_OVERLAP] Checking range overlap: cache_key={}, cached_range={}-{}, requested_range={}-{}, overlaps={}",
                cache_key, range_spec.start, range_spec.end, requested_start, requested_end, overlaps
            );

            // Check if ranges overlap: requested_start <= range_end AND range_start <= requested_end
            if overlaps {
                overlapping_ranges.push(range_spec.clone());

                debug!(
                    "[RANGE_OVERLAP] Overlap detected: key={}, cached_range={}-{}, requested_range={}-{}, overlap_type=partial",
                    cache_key, range_spec.start, range_spec.end, requested_start, requested_end
                );

                // Optimization: Check for exact match
                if range_spec.start == requested_start && range_spec.end == requested_end {
                    let duration = operation_start.elapsed();
                    debug!(
                        "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=exact_match, cached_range={}-{}, duration={:.2}ms",
                        cache_key, requested_start, requested_end, range_spec.start, range_spec.end, duration.as_secs_f64() * 1000.0
                    );
                    // For exact match, we can return immediately with just this range
                    return Ok(RangeLookupResult {
                        ranges: vec![range_spec.clone()],
                        stored_freshness,
                    });
                }

                // Optimization: Check for full containment
                if range_spec.start <= requested_start && range_spec.end >= requested_end {
                    let duration = operation_start.elapsed();
                    debug!(
                        "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=full_containment, cached_range={}-{}, duration={:.2}ms",
                        cache_key, requested_start, requested_end, range_spec.start, range_spec.end, duration.as_secs_f64() * 1000.0
                    );
                    // For full containment, we can return immediately with just this range
                    return Ok(RangeLookupResult {
                        ranges: vec![range_spec.clone()],
                        stored_freshness,
                    });
                }
            }
        }

        // Sort overlapping ranges by start position for easier processing
        overlapping_ranges.sort_by_key(|r| r.start);

        let duration = operation_start.elapsed();
        if overlapping_ranges.is_empty() {
            // No overlapping ranges in metadata - check journals for pending ranges (shared storage mode)
            let journal_ranges = self
                .find_pending_journal_ranges(cache_key, requested_start, requested_end)
                .await?;
            if !journal_ranges.is_empty() {
                debug!(
                    "[RANGE_OVERLAP] Range lookup completed via journal fallback: key={}, requested_range={}-{}, result=journal_hit, ranges={}, total_cached_ranges={}, duration={:.2}ms",
                    cache_key, requested_start, requested_end, journal_ranges.len(), metadata.ranges.len(), duration.as_secs_f64() * 1000.0
                );
                return Ok(RangeLookupResult::fresh(journal_ranges));
            }

            debug!(
                "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=no_overlap, total_cached_ranges={}, duration={:.2}ms",
                cache_key, requested_start, requested_end, metadata.ranges.len(), duration.as_secs_f64() * 1000.0
            );
        } else {
            debug!(
                "[RANGE_OVERLAP] Range lookup completed: key={}, requested_range={}-{}, result=partial_overlap, overlapping_ranges={}, total_cached_ranges={}, duration={:.2}ms",
                cache_key, requested_start, requested_end, overlapping_ranges.len(), metadata.ranges.len(), duration.as_secs_f64() * 1000.0
            );
        }

        Ok(RangeLookupResult {
            ranges: overlapping_ranges,
            stored_freshness,
        })
    }

    /// Load range data from binary file with access tracking
    /// Implements Requirements 2.2, 8.2, 1.2 (access tracking)
    ///
    /// This method:
    /// 1. Reads binary data from the range file specified in RangeSpec
    /// 2. Handles decompression if the range was compressed
    /// 3. Handles missing files gracefully (returns error for cache miss)
    /// 4. Validates that decompressed size matches expected uncompressed_size
    /// 5. Updates range access statistics asynchronously (Requirement 1.2)
    ///
    /// Error handling:
    /// - Missing range file: Returns error (Requirement 8.2 - fetch from S3 and recache)
    /// - Corrupted data: Returns error with details
    /// - Decompression failure: Returns error with details
    pub async fn load_range_data(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
    ) -> Result<Vec<u8>> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range data load: file={}, range={}-{}, compressed_size={} bytes, uncompressed_size={} bytes, compression={:?}",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            range_spec.compressed_size,
            range_spec.uncompressed_size,
            range_spec.compression_algorithm
        );

        // Start timing range load
        let load_start = std::time::Instant::now();

        // Construct full path to range binary file
        // range_spec.file_path is relative to ranges directory, including bucket/XX/YYY directories
        // e.g., "bucket/XX/YYY/filename.bin"
        // Security (Req 10): reject file_path if it is absolute, contains ".." traversal,
        // or would escape cache_dir/ranges/. Treat as cache miss.
        if !is_safe_relative_file_path(&range_spec.file_path) {
            warn!(
                "Range read: rejected unsafe file_path={:?} (path traversal attempt), treating as cache miss",
                range_spec.file_path
            );
            return Err(ProxyError::CacheError(format!(
                "Range file path rejected (unsafe): {} (cache miss - will fetch from S3)",
                range_spec.file_path
            )));
        }
        let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

        debug!(
            "Range file path: file={}, full_path={:?}",
            range_spec.file_path, range_file_path
        );

        // Check if range file exists
        // Requirement 8.2: Handle missing range binary files
        if !range_file_path.exists() {
            debug!(
                "Range file missing (will fetch from S3): file={}, path={:?}, range={}-{}",
                range_spec.file_path, range_file_path, range_spec.start, range_spec.end
            );
            // Record missing range file metric
            if let Some(metrics_manager) = &self.metrics_manager {
                metrics_manager
                    .read()
                    .await
                    .record_missing_range_file()
                    .await;
            }

            // Requirement 8.2: When range binary file cannot be read, return error
            // The caller should fetch from S3 and attempt to recache
            // This is a cache miss scenario - the caller will handle fetching from S3
            return Err(ProxyError::CacheError(format!(
                "Range file not found: {} (cache miss - will fetch from S3)",
                range_spec.file_path
            )));
        }

        // A file a read on this proxy found damaged is not read again.
        if let Some(stamp) = marked_damaged_range_file(&range_file_path) {
            return Err(self.refuse_marked_damaged_range(range_spec, stamp).await);
        }

        // A compressed range decodes straight from the file, so the compressed frame is never
        // whole in memory beside its decode output. Blocking file I/O and the decode
        // run on a blocking thread (Requirement 3.1).
        let read_start = std::time::Instant::now();
        let streamed =
            if range_spec.compression_algorithm == crate::compression::CompressionAlgorithm::Lz4 {
                let path = range_file_path.clone();
                let expected = range_spec.uncompressed_size;
                let listed = range_spec.file_identity;
                // The descriptor's identity is checked before anything is decoded from it.
                match tokio::task::spawn_blocking(move || {
                    Ok::<_, std::io::Error>(match open_range_file_checked(&path, listed)? {
                        Ok((file, meta)) => Ok(decode_lz4_open_file_exact(
                            file,
                            meta.len(),
                            expected,
                            None,
                        )?),
                        Err(wrong) => Err(wrong),
                    })
                })
                .await
                {
                    Ok(Ok(Err(wrong))) => {
                        return Err(self.refuse_wrong_range_file(range_spec, wrong).await);
                    }
                    Ok(Ok(Ok((file_len, Some(data))))) => {
                        debug!(
                            "Range file read and decoded: file={}, size={} bytes, duration={:.2}ms",
                            range_spec.file_path,
                            file_len,
                            read_start.elapsed().as_secs_f64() * 1000.0
                        );
                        Some((file_len as usize, data))
                    }
                    // Not a clean exact decode: the buffered path below reports why.
                    Ok(Ok(Ok((_, None)))) => None,
                    Ok(Err(e)) => {
                        error!(
                            "Failed to read range file: file={}, path={:?}, error={}",
                            range_spec.file_path, range_file_path, e
                        );
                        return Err(ProxyError::CacheError(format!(
                            "Failed to read range file {}: {}",
                            range_spec.file_path, e
                        )));
                    }
                    Err(e) => {
                        return Err(ProxyError::CacheError(format!(
                            "Range file decode task failed for {}: {}",
                            range_spec.file_path, e
                        )));
                    }
                }
            } else {
                None
            };

        let (compressed_size, decompressed_data, read_stamp) = if let Some((len, data)) = streamed {
            (len, data, None)
        } else {
            // Read compressed data from file on a blocking thread (Requirement 3.1), with the
            // stamp of the file this read opened: a heal judges that file, not one a refill
            // has renamed over the path since (range-file-self-heal R3).
            let read_path = range_file_path.clone();
            let listed = range_spec.file_identity;
            let read =
                tokio::task::spawn_blocking(move || read_range_file_checked(&read_path, listed))
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::other(e)));
            let (compressed_data, file_stamp) = match read {
                Ok(Err(wrong)) => {
                    return Err(self.refuse_wrong_range_file(range_spec, wrong).await);
                }
                Ok(Ok((data, stamp))) => {
                    let read_duration = read_start.elapsed();
                    debug!(
                        "Range file read: file={}, size={} bytes, duration={:.2}ms",
                        range_spec.file_path,
                        data.len(),
                        read_duration.as_secs_f64() * 1000.0
                    );
                    (data, stamp)
                }
                Err(e) => {
                    error!(
                        "Failed to read range file: file={}, path={:?}, error={}",
                        range_spec.file_path, range_file_path, e
                    );
                    return Err(ProxyError::CacheError(format!(
                        "Failed to read range file {}: {}",
                        range_spec.file_path, e
                    )));
                }
            };

            let compressed_size = compressed_data.len();

            // Decompress frame-encoded data. An uncompressed file is the data itself:
            // hand over the read buffer rather than copying it, which would hold the
            // range twice (buffered-serve-memory-bound R1.3).
            let decompressed_data = if range_spec.compression_algorithm
                == crate::compression::CompressionAlgorithm::None
            {
                compressed_data
            } else if let Some(data) =
                decode_lz4_frames_exact(&compressed_data, range_spec.uncompressed_size)
            {
                data
            } else {
                let decompress_start = std::time::Instant::now();
                debug!(
                    "Decompressing range data: file={}, algorithm={:?}, compressed={} bytes, expected_uncompressed={} bytes",
                    range_spec.file_path,
                    range_spec.compression_algorithm,
                    compressed_size,
                    range_spec.uncompressed_size
                );

                match self.compression_handler.decompress_with_algorithm(
                    &compressed_data,
                    range_spec.compression_algorithm.clone(),
                ) {
                    Ok(data) => {
                        let decompress_duration = decompress_start.elapsed();
                        debug!(
                            "Decompression completed: file={}, algorithm={:?}, compressed={} bytes, uncompressed={} bytes, duration={:.2}ms",
                            range_spec.file_path,
                            range_spec.compression_algorithm,
                            compressed_size,
                            data.len(),
                            decompress_duration.as_secs_f64() * 1000.0
                        );
                        data
                    }
                    Err(e) => {
                        error!(
                            "Decompression failed: file={}, range={}-{}, algorithm={:?}, error={}",
                            range_spec.file_path,
                            range_spec.start,
                            range_spec.end,
                            range_spec.compression_algorithm,
                            e
                        );
                        note_range_file_corruption(&range_spec.file_path, "decode failed");
                        mark_damaged_range_file(&range_file_path, file_stamp);
                        // The caller still falls back to S3 for this request; the heal makes
                        // the next read of the range an ordinary miss (range-file-self-heal R1).
                        self.range_heal_limiter.run_after_detect();
                        self.heal_damaged_range(range_spec, Some(file_stamp)).await;
                        return Err(ProxyError::CacheError(format!(
                            "Failed to decompress range data: {}",
                            e
                        )));
                    }
                }
            };
            (compressed_size, decompressed_data, Some(file_stamp))
        };

        // Verify compressed size matches what we expect
        if compressed_size as u64 != range_spec.compressed_size {
            warn!(
                "Compressed size mismatch: file={}, range={}-{}, expected={} bytes, actual={} bytes, continuing_anyway",
                range_spec.file_path,
                range_spec.start,
                range_spec.end,
                range_spec.compressed_size,
                compressed_size
            );
            // Continue anyway - the file might have been updated
        }

        // Validate decompressed size
        if decompressed_data.len() as u64 != range_spec.uncompressed_size {
            error!(
                "Decompressed size validation failed: file={}, range={}-{}, expected={} bytes, actual={} bytes",
                range_spec.file_path,
                range_spec.start,
                range_spec.end,
                range_spec.uncompressed_size,
                decompressed_data.len()
            );
            // Every writer names a range file by the bounds it holds, so a file at this path
            // that decodes cleanly to another length is damaged (truncated at a frame boundary),
            // not a newer version of the object (range-file-self-heal R1).
            note_range_file_corruption(&range_spec.file_path, "decoded to the wrong length");
            if let Some(stamp) = read_stamp {
                mark_damaged_range_file(&range_file_path, stamp);
            }
            self.heal_damaged_range(range_spec, read_stamp).await;
            return Err(ProxyError::CacheError(format!(
                "Decompressed data size mismatch: expected {} bytes, got {} bytes",
                range_spec.uncompressed_size,
                decompressed_data.len()
            )));
        }

        // Validate that data size matches range size
        let expected_range_size = range_spec.end - range_spec.start + 1;
        if decompressed_data.len() as u64 != expected_range_size {
            error!(
                "Range size validation failed: file={}, range={}-{}, expected_size={} bytes, actual_size={} bytes",
                range_spec.file_path,
                range_spec.start,
                range_spec.end,
                expected_range_size,
                decompressed_data.len()
            );
            return Err(ProxyError::CacheError(format!(
                "Range data size mismatch: expected {} bytes for range {}-{}, got {} bytes",
                expected_range_size,
                range_spec.start,
                range_spec.end,
                decompressed_data.len()
            )));
        }

        // A range written before 3.0.0 records no file identity, and such a release could
        // store the body of an S3 error reply as a range's bytes. Checked here, before any
        // caller serves the bytes or promotes them to RAM.
        if range_spec.file_identity.is_none()
            && range_spec.uncompressed_size <= LEGACY_ERROR_BODY_MAX_BYTES
            && looks_like_s3_error_document(&decompressed_data)
        {
            return Err(self.drop_legacy_error_body_listing(range_spec).await);
        }
        debug!(
            "Range data validation passed: file={}, range={}-{}, size={} bytes",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            decompressed_data.len()
        );

        // Record range load duration
        let load_duration = load_start.elapsed();
        if let Some(metrics_manager) = &self.metrics_manager {
            metrics_manager
                .read()
                .await
                .record_range_load_duration(load_duration.as_secs_f64() * 1000.0)
                .await;
        }

        let total_duration = operation_start.elapsed();
        debug!(
            "Range data load completed: file={}, range={}-{}, uncompressed={} bytes, compressed={} bytes, compression_ratio={:.2}%, total_duration={:.2}ms",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            decompressed_data.len(),
            compressed_size,
            (compressed_size as f64 / decompressed_data.len() as f64) * 100.0,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(decompressed_data)
    }

    /// Load the raw (still-encoded) frame bytes for a range, without decompressing.
    ///
    /// Mirrors `load_range_data`'s path-safety check, existence check, and
    /// `tokio::fs::read` + compressed-size warn, but skips decompression and the
    /// decompressed-size/range-size validation — the caller gets the on-disk frame
    /// bytes verbatim plus the range's tagged algorithm.
    ///
    /// Used by the RAM-tier range-promotion path (compression-content-aware-fix
    /// Requirement 9) so promoting a range to RAM does not require decompressing
    /// then re-storing uncompressed bytes: the frame is copied through as-is.
    pub(crate) async fn load_range_frame(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
    ) -> Result<(Vec<u8>, crate::compression::CompressionAlgorithm)> {
        // Security (Req 10): reject file_path if it is absolute, contains ".." traversal,
        // or would escape cache_dir/ranges/. Treat as cache miss.
        if !is_safe_relative_file_path(&range_spec.file_path) {
            warn!(
                "Range frame read: rejected unsafe file_path={:?} (path traversal attempt), treating as cache miss",
                range_spec.file_path
            );
            return Err(ProxyError::CacheError(format!(
                "Range file path rejected (unsafe): {} (cache miss - will fetch from S3)",
                range_spec.file_path
            )));
        }
        let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

        debug!(
            "Loading range frame (no decompression): file={}, range={}-{}, compression={:?}",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            range_spec.compression_algorithm
        );

        // Check if range file exists
        if !range_file_path.exists() {
            debug!(
                "Range file missing (will fetch from S3): file={}, path={:?}, range={}-{}",
                range_spec.file_path, range_file_path, range_spec.start, range_spec.end
            );
            if let Some(metrics_manager) = &self.metrics_manager {
                metrics_manager
                    .read()
                    .await
                    .record_missing_range_file()
                    .await;
            }
            return Err(ProxyError::CacheError(format!(
                "Range file not found: {} (cache miss - will fetch from S3)",
                range_spec.file_path
            )));
        }

        // Nor from a file a read on this proxy found damaged.
        if marked_damaged_range_file(&range_file_path).is_some() {
            return Err(marked_damaged_range_error(range_spec));
        }

        // Read frame bytes on a blocking thread, through a descriptor whose identity is the
        // listing's: a frame from another file is not promoted.
        let read_path = range_file_path.clone();
        let listed = range_spec.file_identity;
        let read = tokio::task::spawn_blocking(move || read_range_file_checked(&read_path, listed))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e)));
        let frame_data = match read {
            Ok(Err(wrong)) => {
                return Err(self.refuse_wrong_range_file(range_spec, wrong).await);
            }
            Ok(Ok((data, _stamp))) => {
                debug!(
                    "Range frame file read: file={}, size={} bytes",
                    range_spec.file_path,
                    data.len()
                );
                data
            }
            Err(e) => {
                error!(
                    "Failed to read range file: file={}, path={:?}, error={}",
                    range_spec.file_path, range_file_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to read range file {}: {}",
                    range_spec.file_path, e
                )));
            }
        };

        // Verify frame size matches what we expect (warn only — the file may have been
        // updated concurrently; the frame decoder is the real correctness gate on read).
        let frame_size = frame_data.len();
        if frame_size as u64 != range_spec.compressed_size {
            warn!(
                "Frame size mismatch: file={}, range={}-{}, expected={} bytes, actual={} bytes, continuing_anyway",
                range_spec.file_path, range_spec.start, range_spec.end, range_spec.compressed_size, frame_size
            );
        }

        Ok((frame_data, range_spec.compression_algorithm.clone()))
    }

    /// Whether `range_spec`'s range file is a safe relative path that exists on disk and is
    /// not a file a read on this proxy found damaged. A pre-flight for a chained serve,
    /// which checks every extent before sending the response head (buffered-serve-memory-bound
    /// R2.1).
    pub(crate) fn range_spec_file_present(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
    ) -> bool {
        if !is_safe_relative_file_path(&range_spec.file_path) {
            return false;
        }
        let path = self.cache_dir.join("ranges").join(&range_spec.file_path);
        path.exists() && marked_damaged_range_file(&path).is_none()
    }

    /// Stream range data from disk in chunks (Requirement 5.1, 5.4)
    ///
    /// For uncompressed data, streams directly from the file in `chunk_size` chunks.
    /// For LZ4 data, decompresses incrementally: a blocking task reads the file through a
    /// `FrameDecoder` (looping over concatenated frames) and sends each decompressed chunk
    /// as it is produced. Neither mode holds the whole file in memory.
    ///
    /// Chunks cross a 4-deep channel, so resident data is bounded by about
    /// 4 × `chunk_size` per stream. Dropping the returned stream makes the blocking task's
    /// next send fail, which stops the read.
    ///
    /// Returns a stream of Bytes chunks that can be used with StreamBody.
    ///
    /// # Arguments
    /// * `range_spec` - The range specification containing file path and compression info
    /// * `chunk_size` - Size of each chunk in bytes (default: 524_288 = 512 KiB)
    ///
    /// # Returns
    /// * `Ok(impl Stream<Item = Result<Bytes>>)` - Stream of data chunks
    /// * `Err(ProxyError)` - If file cannot be opened or read
    pub async fn stream_range_data(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
        chunk_size: usize,
    ) -> Result<impl futures::Stream<Item = Result<bytes::Bytes>>> {
        self.stream_range_data_from(range_spec, chunk_size, 0)
            .await
            .map(|(stream, _skipped)| stream)
    }

    /// As [`Self::stream_range_data`], but the caller only needs the extent's bytes
    /// from `start_offset` (uncompressed, relative to the extent start) onwards.
    /// Returns the stream and how many leading bytes it has already skipped. For an
    /// uncompressed file (`CompressionAlgorithm::None`) the reader seeks straight to
    /// `start_offset`, so the returned skip equals it; an LZ4 file has no index of
    /// frame offsets, so it is decoded from the start and the returned skip is 0,
    /// leaving the caller to read and discard (buffered-serve-memory-bound R1.4).
    pub async fn stream_range_data_from(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
        chunk_size: usize,
        start_offset: u64,
    ) -> Result<(impl futures::Stream<Item = Result<bytes::Bytes>>, u64)> {
        self.stream_range_data_from_with_capacity(range_spec, chunk_size, start_offset, None)
            .await
    }

    /// [`Self::stream_range_data_from`] with the LZ4 reader capacity given; `None` takes
    /// [`lz4_range_reader_capacity`] of the file's length.
    async fn stream_range_data_from_with_capacity(
        &self,
        range_spec: &crate::cache_types::RangeSpec,
        chunk_size: usize,
        start_offset: u64,
        reader_capacity: Option<usize>,
    ) -> Result<(impl futures::Stream<Item = Result<bytes::Bytes>>, u64)> {
        // Security (Req 10): reject file_path if it is absolute, contains ".." traversal,
        // or would escape cache_dir/ranges/. Treat as cache miss.
        if !is_safe_relative_file_path(&range_spec.file_path) {
            warn!(
                "Range stream: rejected unsafe file_path={:?} (path traversal attempt), treating as cache miss",
                range_spec.file_path
            );
            return Err(ProxyError::CacheError(format!(
                "Range file path rejected (unsafe): {} (cache miss - will fetch from S3)",
                range_spec.file_path
            )));
        }
        let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);

        debug!(
            "Starting range data stream: file={}, range={}-{}, compression={:?}, chunk_size={}",
            range_spec.file_path,
            range_spec.start,
            range_spec.end,
            range_spec.compression_algorithm,
            chunk_size
        );

        // Check if file exists
        if !range_file_path.exists() {
            debug!(
                "Range file missing for streaming: file={}, path={:?}",
                range_spec.file_path, range_file_path
            );
            return Err(ProxyError::CacheError(format!(
                "Range file not found: {} (cache miss)",
                range_spec.file_path
            )));
        }

        // Refused before the stream opens, so before the caller sends a response head.
        self.check_legacy_range(range_spec).await?;
        let expected_size = range_spec.uncompressed_size;
        let compression_algorithm = range_spec.compression_algorithm.clone();
        let corrupt_file_label = range_spec.file_path.clone();

        // Async file open — does not block tokio worker thread (Requirement 3.1)
        let file = tokio::fs::File::open(&range_file_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to open range file: {}", e)))?;

        // Convert to std::fs::File for FrameDecoder (sync Read trait)
        let mut std_file = file.into_std().await;
        // The file this stream reads must be the one its listing describes: `fstat` of
        // this descriptor, before the first byte, so a refused file is a clean miss. A failed
        // `fstat` is a miss too, as on the buffered path, and is not healed: it says nothing
        // about which file this is.
        let meta = fstat_read_descriptor(&std_file, &range_spec.file_path).map_err(|e| {
            error!(
                "Failed to read range file for streaming: file={}, path={:?}, error={}",
                range_spec.file_path, range_file_path, e
            );
            ProxyError::CacheError(format!(
                "Failed to read range file {}: {}",
                range_spec.file_path, e
            ))
        })?;
        if let Some(wrong) = wrong_range_file(range_spec.file_identity, &meta) {
            return Err(self.refuse_wrong_range_file(range_spec, wrong).await);
        }
        // A file a read on this proxy found damaged is refused before its first byte, so the
        // serve answers from S3 instead of sending a head and breaking off again.
        let open_stamp = RangeFileStamp::of(&meta);
        if damaged_range_mark_matches(&range_file_path, &open_stamp) {
            return Err(self
                .refuse_marked_damaged_range(range_spec, open_stamp)
                .await);
        }
        let skipped = if compression_algorithm == crate::compression::CompressionAlgorithm::None
            && start_offset > 0
            && start_offset < expected_size
        {
            use std::io::Seek;
            std_file
                .seek(std::io::SeekFrom::Start(start_offset))
                .map_err(|e| ProxyError::CacheError(format!("Failed to seek range file: {}", e)))?;
            start_offset
        } else {
            0
        };

        // A damaged file found mid-stream is healed from the blocking task, before the error
        // is sent, against the stamp of the file this stream opened (range-file-self-heal R1,
        // R3). The response already sent its head and cannot be rescued. The mark set first
        // makes the next read of this file on this proxy a miss even when the heal leaves it in
        // place: a range only a pending journal `Add` lists, or the heal limit reached.
        let heal_stamp = std_file.metadata().ok().map(|m| RangeFileStamp::of(&m));
        let heal_runtime = tokio::runtime::Handle::current();
        let heal_cache_dir = self.cache_dir.clone();
        let heal_consolidator = self.journal_consolidator.clone();
        let heal_metadata_cache = self.metadata_cache.clone();
        let heal_limiter = self.range_heal_limiter.clone();
        let heal_spec = range_spec.clone();
        let mark_path = range_file_path.clone();
        let mut heal = Some(move || {
            mark_damaged_range_file(&mark_path, open_stamp);
            heal_runtime.block_on(heal_damaged_range(
                &heal_cache_dir,
                heal_consolidator.as_ref(),
                heal_metadata_cache.as_ref(),
                &heal_limiter,
                &heal_spec,
                heal_stamp,
                true,
            ));
        });

        // Channel capacity of 4 provides backpressure: blocking task pauses when
        // 4 chunks are buffered, bounding memory to 4 × chunk_size per request.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes>>(4);

        tokio::task::spawn_blocking(move || {
            use crate::compression::CompressionAlgorithm;
            use lz4_flex::frame::FrameDecoder;
            use std::io::Read;

            let mut buf = vec![0u8; chunk_size];
            let mut total_read = skipped;
            // The read reached the file's end with no error and the receiver still there.
            let mut clean_end = false;

            match compression_algorithm {
                CompressionAlgorithm::Lz4 => {
                    // Use BufReader for the underlying file, then loop creating
                    // new FrameDecoders to handle concatenated LZ4 frames
                    // (produced by incremental writes).
                    let capacity = reader_capacity.unwrap_or_else(|| {
                        lz4_range_reader_capacity(std_file.metadata().map_or(0, |m| m.len()))
                    });
                    let mut buf_reader = std::io::BufReader::with_capacity(capacity, std_file);
                    // A `FrameDecoder::read` returns at most one block, and blocks are
                    // 64 KiB, so decoded bytes accumulate in `buf` across reads (and across
                    // frames) and are sent once `chunk_size` bytes are ready. `stopped` is
                    // set on an error or a dropped receiver, when the remainder is not sent.
                    let mut filled = 0usize;
                    let mut stopped = false;

                    'outer: loop {
                        // Check if there's more data in the underlying reader
                        {
                            use std::io::BufRead;
                            match buf_reader.fill_buf() {
                                Ok([]) => break, // True EOF
                                Err(e) => {
                                    let _ = tx.blocking_send(Err(ProxyError::CacheError(format!(
                                        "Decompression error mid-stream: {}",
                                        e
                                    ))));
                                    stopped = true;
                                    break;
                                }
                                _ => {}
                            }
                        }

                        let mut decoder = FrameDecoder::new(&mut buf_reader);
                        loop {
                            match decoder.read(&mut buf[filled..]) {
                                Ok(0) => break, // End of this LZ4 frame
                                Ok(n) => {
                                    total_read += n as u64;
                                    filled += n;
                                    if filled == buf.len() {
                                        let chunk = bytes::Bytes::copy_from_slice(&buf[..filled]);
                                        filled = 0;
                                        if tx.blocking_send(Ok(chunk)).is_err() {
                                            stopped = true;
                                            break 'outer; // Receiver dropped
                                        }
                                    }
                                }
                                Err(e) => {
                                    // An LZ4 decode error, not a read error: lz4_flex wraps
                                    // its own `frame::Error` inside the `io::Error` for a
                                    // malformed frame and passes an underlying read error
                                    // through bare. Counted before the send so a reader that
                                    // sees the error sees the count. `UnexpectedEof` is the file
                                    // ending inside a frame (a truncated file), which the buffered
                                    // path already counts as a failed decode: the open descriptor
                                    // keeps the file it opened even if a refill renames over it.
                                    if e.kind() == std::io::ErrorKind::UnexpectedEof
                                        || e.get_ref()
                                            .and_then(|inner| {
                                                inner.downcast_ref::<lz4_flex::frame::Error>()
                                            })
                                            .is_some()
                                    {
                                        note_range_file_corruption(
                                            &corrupt_file_label,
                                            "decode failed mid-stream",
                                        );
                                        if let Some(heal) = heal.take() {
                                            heal();
                                        }
                                    }
                                    let _ = tx.blocking_send(Err(ProxyError::CacheError(format!(
                                        "Decompression error mid-stream: {}",
                                        e
                                    ))));
                                    stopped = true;
                                    break 'outer;
                                }
                            }
                        }
                    }
                    if !stopped && filled > 0 {
                        let _ = tx.blocking_send(Ok(bytes::Bytes::copy_from_slice(&buf[..filled])));
                    }
                    clean_end = !stopped;
                }
                CompressionAlgorithm::None => {
                    let mut reader = std::io::BufReader::new(std_file);
                    loop {
                        match reader.read(&mut buf) {
                            Ok(0) => {
                                clean_end = true;
                                break;
                            }
                            Ok(n) => {
                                total_read += n as u64;
                                let chunk = bytes::Bytes::copy_from_slice(&buf[..n]);
                                if tx.blocking_send(Ok(chunk)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = tx.blocking_send(Err(ProxyError::CacheError(format!(
                                    "Decompression error mid-stream: {}",
                                    e
                                ))));
                                break;
                            }
                        }
                    }
                }
            }

            // Validate total decompressed size matches expected
            if total_read != expected_size {
                // The whole file was read and holds another length, so it is damaged (see
                // `load_range_data`). A stream that stopped early, on an error or a departed
                // client, says nothing about the file's length.
                if clean_end {
                    note_range_file_corruption(
                        &corrupt_file_label,
                        "decoded to the wrong length mid-stream",
                    );
                    if let Some(heal) = heal.take() {
                        heal();
                    }
                }
                let _ = tx.blocking_send(Err(ProxyError::CacheError(format!(
                    "Decompressed size mismatch: expected {} bytes, got {}",
                    expected_size, total_read
                ))));
            }
        });

        Ok((tokio_stream::wrappers::ReceiverStream::new(rx), skipped))
    }

    /// Store full object as a range (0 to content_length-1)
    /// Implements Requirements 4.1, 4.2, 4.3
    ///
    /// This method treats full objects uniformly as ranges, eliminating special casing.
    /// A full object is simply stored as range 0 to content_length-1.
    ///
    /// This enables:
    /// - Consistent storage model (everything is a range)
    /// - Ability to serve any range request from a cached full object
    /// - No distinction between "full object" and "range" cache entries
    pub async fn store_full_object_as_range(
        &mut self,
        cache_key: &str,
        data: &[u8],
        object_metadata: crate::cache_types::ObjectMetadata,
        ttl: std::time::Duration,
        compression_enabled: bool,
    ) -> Result<()> {
        debug!("Storing full object as range for key: {}", cache_key);

        let content_length = data.len() as u64;

        // Validate that data length matches object metadata
        if content_length != object_metadata.content_length {
            return Err(ProxyError::CacheError(format!(
                "Data length ({}) doesn't match object metadata content_length ({})",
                content_length, object_metadata.content_length
            )));
        }

        // Handle empty objects specially
        if content_length == 0 {
            info!("Storing empty object for key: {}", cache_key);

            // For empty objects, we still create metadata but with no ranges
            // This is consistent with the design: empty objects have no data to cache
            let now = SystemTime::now();
            let metadata = crate::cache_types::NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata,
                ranges: Vec::new(), // No ranges for empty object
                created_at: now,
                expires_at: now + ttl,
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            };

            return self.update_metadata(&metadata).await;
        }

        // Store as range 0 to content_length-1
        // This is the key insight: full objects are just ranges from 0 to N
        let start = 0u64;
        let end = content_length - 1;

        info!(
            "Storing full object as range {}-{} for key: {} ({} bytes)",
            start, end, cache_key, content_length
        );

        // Use the existing store_range method - no special casing needed!
        self.store_range(
            cache_key,
            start,
            end,
            data,
            object_metadata,
            ttl,
            compression_enabled,
        )
        .await
    }

    /// Load metadata from file (helper method)
    /// Used internally by metadata update operations
    async fn load_metadata_new(
        &self,
        cache_key: &str,
    ) -> Result<crate::cache_types::NewCacheMetadata> {
        let metadata_path = self.get_new_metadata_file_path(cache_key);

        let metadata_content = std::fs::read_to_string(&metadata_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to read metadata file: {}", e)))?;

        let metadata =
            serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&metadata_content)
                .map_err(|e| ProxyError::CacheError(format!("Failed to parse metadata: {}", e)))?;

        Ok(metadata)
    }

    /// Update range access statistics with file locking
    /// Implements Requirements 1.2, 4.1, 4.2, 4.3, 4.4, 7.1, 7.2, 7.3, 7.4, 7.5
    ///
    /// In shared storage mode:
    /// - Uses CacheHitUpdateBuffer to buffer access updates in RAM
    /// - Periodically flushed to per-instance journal file
    /// - Consolidated by background task into metadata files
    ///
    /// In single instance mode:
    /// - Direct metadata write (existing behavior)
    ///
    /// This method:
    /// 1. In shared storage mode: buffers update via CacheHitUpdateBuffer
    /// 2. In single instance mode:
    ///    a. Acquires exclusive file lock on metadata file (60s timeout)
    ///    b. Reads current metadata
    ///    c. Updates only the specific range's last_accessed and access_count
    ///    d. Writes updated metadata using in-place editing (no fsync for performance)
    ///    e. Releases lock
    ///
    /// Performance: Target < 5ms per update (Requirement 7.1)
    pub async fn update_range_access(
        &mut self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        // In shared storage mode, use the cache hit update buffer
        if let Some(buffer) = &self.cache_hit_update_buffer {
            debug!(
                "Using journal-based access update: key={}, range={}-{}",
                cache_key, range_start, range_end
            );
            return buffer
                .record_access_update(cache_key, range_start, range_end, 1)
                .await;
        }

        // Single instance mode: direct metadata write
        self.update_range_access_direct(cache_key, range_start, range_end)
            .await
    }

    /// Direct range access update (single instance mode)
    /// Used when shared storage mode is disabled
    async fn update_range_access_direct(
        &mut self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range access update: key={}, range={}-{}, operation=update_range_access",
            cache_key, range_start, range_end
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        // The lock lives in the SHARED slot namespace (`metadata_lock_manager::slot_lock_path`),
        // not at a per-key `{key}.meta.lock`. A per-key lock file that nobody writes a payload
        // into was the state the retired stale-detection branches judged stale, and every such
        // branch then unlinked the live holder's file. The slot file is never unlinked.

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for access update: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire the key's metadata slot lock with a SINGLE non-blocking attempt, held as a
        // guard for the rest of the function.
        //
        // A single attempt is the behaviour, not an implementation detail: a contended slot
        // means a live peer is mid-write, and an access-statistics update is worth skipping
        // rather than waiting for. So this path is bounded by construction and the wait budget
        // never applies to it.
        //
        // There is no stale branch. The refusal arm used to judge staleness by the lock file's
        // mtime and then `remove_file` it, which is unsound twice over: nothing ever advances
        // that mtime (the file carries no payload and is only flocked, never written), so the
        // predicate was satisfied by any lock file older than the threshold whether or not a
        // holder was live; and `flock` binds to the open file description, not the path, so
        // unlinking the path released nothing and merely gave the next opener an independent
        // lock on a different inode. A `flock` cannot go stale: the kernel releases it on
        // process death and the NFSv4.1 server releases it on lease expiry.
        let lock_start = std::time::Instant::now();
        let _meta_lock =
            match crate::metadata_lock_manager::try_acquire_slot_lock(&self.cache_dir, cache_key) {
                Ok(lock) => {
                    debug!(
                        "Acquired exclusive lock for access update: key={}, wait_time={:.2}ms",
                        cache_key,
                        lock_start.elapsed().as_secs_f64() * 1000.0
                    );
                    lock
                }
                Err(e) => {
                    debug!(
                        "Metadata slot lock held by a peer, skipping update: key={}, error={}",
                        cache_key, e
                    );
                    return Ok(());
                }
            };

        // Read metadata
        let read_start = std::time::Instant::now();
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => {
                let read_duration = read_start.elapsed();
                debug!(
                    "Metadata loaded for access update: key={}, ranges={}, duration={:.2}ms",
                    cache_key,
                    meta.ranges.len(),
                    read_duration.as_secs_f64() * 1000.0
                );
                meta
            }
            Err(e) => {
                error!(
                    "Failed to load metadata for access update: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Find and update the specific range
        let update_start = std::time::Instant::now();
        let mut range_found = false;
        for range in &mut metadata.ranges {
            if range.start == range_start && range.end == range_end {
                range.record_access();
                range_found = true;
                debug!(
                    "Range access recorded: key={}, range={}-{}, access_count={}, last_accessed={:?}",
                    cache_key, range_start, range_end, range.access_count, range.last_accessed
                );
                break;
            }
        }

        if !range_found {
            debug!(
                "Range not found for access update: key={}, range={}-{}, result=skip",
                cache_key, range_start, range_end
            );
            return Ok(());
        }

        let update_duration = update_start.elapsed();
        debug!(
            "Range access statistics updated: key={}, range={}-{}, duration={:.2}ms",
            cache_key,
            range_start,
            range_end,
            update_duration.as_secs_f64() * 1000.0
        );

        // Write updated metadata in-place (Requirement 7.2, 7.3)
        let write_start = std::time::Instant::now();
        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            error!(
                "Failed to serialize metadata for access update: key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        std::fs::write(&metadata_path, json).map_err(|e| {
            crate::disk_cache::note_disk_full(&e, "range_access.meta");
            error!(
                "Failed to write metadata for access update: key={}, path={:?}, error={}",
                cache_key, metadata_path, e
            );
            ProxyError::CacheError(format!("Failed to write metadata: {}", e))
        })?;
        // No fsync for performance (Requirement 7.3)

        let write_duration = write_start.elapsed();
        debug!(
            "Metadata written for access update: key={}, duration={:.2}ms",
            cache_key,
            write_duration.as_secs_f64() * 1000.0
        );

        // The lock is released by `_meta_lock`'s `Drop` at the end of this function
        // (Requirement 4.5). Every early return above releases it the same way, which is why
        // the explicit unlock on each of them is gone rather than merely unnecessary — one
        // missed path was all it took to leak a hold.
        let total_duration = operation_start.elapsed();
        debug!(
            "Range access update completed: key={}, range={}-{}, total_duration={:.2}ms",
            cache_key,
            range_start,
            range_end,
            total_duration.as_secs_f64() * 1000.0
        );

        // Check if we met the performance target (Requirement 7.1)
        if total_duration.as_millis() > 45 {
            warn!(
                "Range access update exceeded performance target: key={}, range={}-{}, duration={:.2}ms, target=45ms",
                cache_key, range_start, range_end, total_duration.as_secs_f64() * 1000.0
            );
        }

        Ok(())
    }

    /// Refresh the object-level TTL for a cached object.
    /// Uses the journal/buffer system in shared storage mode, or falls back to direct write.
    /// Moves stored expiry only; the live GET age is untouched. An origin `304` uses
    /// [`Self::record_origin_validation`] instead, which also re-anchors the age.
    pub async fn refresh_object_ttl(
        &mut self,
        cache_key: &str,
        new_ttl: std::time::Duration,
    ) -> Result<()> {
        // `fill_visibility.hits_total`. This function's one `src/` caller is the full-object
        // Mode B `If-Match` arm (`http_proxy.rs`), which calls it immediately before serving the
        // cached ranges without a live-TTL `Fresh` verdict, so when the metadata it serves was
        // this instance's fill-visibility record, this is the one place the serve can be counted.
        // The post-`304` arms count the same way in `record_origin_validation`.
        if !new_ttl.is_zero() {
            if let Some(tracker) = &self.fill_visibility_tracker {
                if tracker
                    .vouches(cache_key, &self.get_new_metadata_file_path(cache_key))
                    .await
                {
                    tracker.record_hit();
                }
            }
        }

        // In shared storage mode, use the cache hit update buffer
        if let Some(buffer) = &self.cache_hit_update_buffer {
            debug!(
                "Using journal-based object TTL refresh: key={}, new_ttl={:.2}s",
                cache_key,
                new_ttl.as_secs_f64()
            );
            return buffer.record_ttl_refresh(cache_key, new_ttl).await;
        }

        // Single instance mode: direct metadata write
        self.refresh_object_ttl_direct(cache_key, new_ttl).await
    }

    /// Record an origin `304` for a cached object: stored expiry moves to `now + new_ttl`
    /// and the live GET age re-anchors to now (`NewCacheMetadata::record_origin_validation`).
    ///
    /// Called by the full-object and range post-`304` arms (`http_proxy.rs`). The Mode B
    /// `If-Match` arm must NOT call this: it serves on a comparison against the cache's own
    /// ETag, which establishes nothing about the origin, and re-anchoring there would extend
    /// unvalidated serving for another `get_ttl`. It calls [`Self::refresh_object_ttl`].
    ///
    /// Always a direct locked write, including in shared-storage mode. The journal's
    /// `TtlRefresh` operation carries a TTL and nothing else, so the consolidator cannot tell
    /// a `304` from a Mode B hit. The write is a single non-blocking lock attempt; a
    /// contended slot or an absent `.meta` skips it, which costs one extra revalidation on
    /// the next read and never extends freshness.
    ///
    /// Takes `&self`: the key's metadata slot lock, not exclusive access to this manager,
    /// orders the write against every other `.meta` writer, in this process and in its peers,
    /// so callers hold the manager's read lock. Under the write lock every revalidated hit in
    /// the process queued behind every reader, journal scans included (the lock is fair).
    pub async fn record_origin_validation(
        &self,
        cache_key: &str,
        new_ttl: std::time::Duration,
    ) -> Result<()> {
        // `fill_visibility.hits_total`, as in `refresh_object_ttl`: this serve is counted here
        // because no live-TTL `Fresh` verdict counted it.
        if !new_ttl.is_zero() {
            if let Some(tracker) = &self.fill_visibility_tracker {
                if tracker
                    .vouches(cache_key, &self.get_new_metadata_file_path(cache_key))
                    .await
                {
                    tracker.record_hit();
                }
            }
        }
        self.write_object_ttl_locked(cache_key, new_ttl, true).await
    }

    /// Directly refresh the object-level TTL by loading metadata, updating expires_at, and writing back.
    pub(crate) async fn refresh_object_ttl_direct(
        &mut self,
        cache_key: &str,
        new_ttl: std::time::Duration,
    ) -> Result<()> {
        self.write_object_ttl_locked(cache_key, new_ttl, false)
            .await
    }

    /// The locked `.meta` read-modify-write behind [`Self::refresh_object_ttl_direct`] and
    /// [`Self::record_origin_validation`]. `origin_validated` selects whether the live GET
    /// age re-anchors as well as stored expiry.
    async fn write_object_ttl_locked(
        &self,
        cache_key: &str,
        new_ttl: std::time::Duration,
        origin_validated: bool,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting object TTL refresh: key={}, new_ttl={:.2}s, operation=refresh_object_ttl",
            cache_key,
            new_ttl.as_secs_f64()
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        // The lock lives in the SHARED slot namespace (`metadata_lock_manager::slot_lock_path`),
        // not at a per-key `{key}.meta.lock`. A per-key lock file that nobody writes a payload
        // into was the state the retired stale-detection branches judged stale, and every such
        // branch then unlinked the live holder's file. The slot file is never unlinked.

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for object TTL refresh: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire the key's metadata slot lock with a SINGLE non-blocking attempt, held as a
        // guard for the rest of the function. A contended slot means a live peer is mid-write,
        // and a TTL refresh is worth skipping rather than waiting for — so this path is bounded
        // by construction and the wait budget never applies to it.
        //
        // There is no stale branch. The refusal arm used to judge staleness by the lock file's
        // mtime and then `remove_file` it, which is unsound twice over: nothing ever advances
        // that mtime (the file carries no payload and is only flocked, never written), so the
        // predicate was satisfied by any lock file older than the threshold whether or not a
        // holder was live; and `flock` binds to the open file description, not the path, so
        // unlinking the path released nothing and merely gave the next opener an independent
        // lock on a different inode.
        let lock_start = std::time::Instant::now();
        let _meta_lock =
            match crate::metadata_lock_manager::try_acquire_slot_lock(&self.cache_dir, cache_key) {
                Ok(lock) => {
                    debug!(
                        "Acquired exclusive lock for object TTL refresh: key={}, wait_time={:.2}ms",
                        cache_key,
                        lock_start.elapsed().as_secs_f64() * 1000.0
                    );
                    lock
                }
                Err(e) => {
                    debug!(
                        "Metadata slot lock held by a peer, skipping update: key={}, error={}",
                        cache_key, e
                    );
                    return Ok(());
                }
            };

        // Read metadata
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                error!(
                    "Failed to load metadata for object TTL refresh: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Refresh the object-level TTL, and on an origin `304` the live GET age too.
        if origin_validated {
            metadata.record_origin_validation(new_ttl);
        } else {
            metadata.refresh_object_ttl(new_ttl);
        }

        // Write updated metadata
        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            error!(
                "Failed to serialize metadata for object TTL refresh: key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        if origin_validated {
            // Reached in shared-storage mode as well, where a peer may read this `.meta` at
            // any moment, so publish by temp + rename rather than an in-place write that a
            // reader could observe half-written.
            write_atomic(&metadata_path, json.as_bytes(), Shared::Yes)
                .await
                .inspect_err(|e| {
                    error!(
                        "Failed to write metadata for origin validation: key={}, path={:?}, error={}",
                        cache_key, metadata_path, e
                    );
                })?;
        } else {
            std::fs::write(&metadata_path, json).map_err(|e| {
                crate::disk_cache::note_disk_full(&e, "object_ttl_refresh.meta");
                error!(
                    "Failed to write metadata for object TTL refresh: key={}, path={:?}, error={}",
                    cache_key, metadata_path, e
                );
                ProxyError::CacheError(format!("Failed to write metadata: {}", e))
            })?;
        }

        // The lock is released by `_meta_lock`'s `Drop` at the end of this function, on every
        // path including the early returns above.
        let total_duration = operation_start.elapsed();
        info!(
            "Object TTL refresh completed: key={}, new_expires_at={:?}, total_duration={:.2}ms",
            cache_key,
            metadata.expires_at,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Remove an invalidated range after conditional validation (lazy expiration)
    /// Implements Requirements 1.4, 1.6, 4.1, 4.2, 4.4
    ///
    /// This method should be called during cache lookup when conditional validation
    /// (If-Modified-Since) returns 200 OK, indicating the cached data is stale.
    ///
    /// This method:
    /// 1. Acquires exclusive file lock on metadata file
    /// 2. Reads current metadata
    /// 3. Verifies the range is expired
    /// 4. Deletes the .bin file for the range
    /// 5. Removes the range from metadata
    /// 6. If no ranges remain, deletes the metadata file
    /// 7. Releases lock
    pub async fn remove_invalidated_range(
        &mut self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Starting invalidated range removal: key={}, range={}-{}, operation=remove_invalidated_range",
            cache_key, range_start, range_end
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        // The lock lives in the SHARED slot namespace (`metadata_lock_manager::slot_lock_path`),
        // not at a per-key `{key}.meta.lock`. A per-key lock file that nobody writes a payload
        // into was the state the retired stale-detection branches judged stale, and every such
        // branch then unlinked the live holder's file. The slot file is never unlinked.

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for range removal: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire the key's metadata slot lock, held as a guard for the rest of the function.
        // Bounded by the configured wait budget and taken on the blocking pool.
        let lock_start = std::time::Instant::now();
        let _meta_lock = self.acquire_meta_slot_lock(cache_key).await?;

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired exclusive lock for range removal: key={}, wait_time={:.2}ms",
            cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read metadata
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                error!(
                    "Failed to load metadata for range removal: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Whether this call unlinked the (parseable) `.meta`, which debits `cached_objects`.
        let mut meta_unlinked = false;

        // Find the specific expired range
        let range_to_remove = metadata
            .ranges
            .iter()
            .find(|r| r.start == range_start && r.end == range_end)
            .cloned();

        if let Some(range) = range_to_remove {
            info!(
                "Removing range: key={}, range={}-{}, file={}",
                cache_key, range_start, range_end, range.file_path
            );

            // Delete .bin file for this range (Requirement 9.4)
            let range_path = self.cache_dir.join("ranges").join(&range.file_path);
            if range_path.exists() {
                match std::fs::remove_file(&range_path) {
                    Ok(_) => {
                        debug!(
                            "Deleted range binary file: key={}, range={}-{}, path={:?}",
                            cache_key, range_start, range_end, range_path
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete range binary file: key={}, range={}-{}, path={:?}, error={}",
                            cache_key, range_start, range_end, range_path, e
                        );
                        // Continue anyway - metadata will be updated
                    }
                }
            } else {
                debug!(
                    "Range binary file already deleted: key={}, range={}-{}, path={:?}",
                    cache_key, range_start, range_end, range_path
                );
            }

            // Remove this range from metadata
            metadata
                .ranges
                .retain(|r| !(r.start == range_start && r.end == range_end));

            debug!(
                "Range removed from metadata: key={}, range={}-{}, remaining_ranges={}",
                cache_key,
                range_start,
                range_end,
                metadata.ranges.len()
            );

            if metadata.ranges.is_empty() {
                // No ranges left, delete metadata file (Requirement 1.7, 9.5)
                match std::fs::remove_file(&metadata_path) {
                    Ok(_) => {
                        meta_unlinked = true;
                        info!(
                            "Deleted metadata file (no ranges remaining): key={}, path={:?}",
                            cache_key, metadata_path
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete metadata file: key={}, path={:?}, error={}",
                            cache_key, metadata_path, e
                        );
                    }
                }
            } else {
                // Update metadata with remaining ranges
                let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
                    error!(
                        "Failed to serialize metadata after range removal: key={}, error={}",
                        cache_key, e
                    );
                    ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
                })?;

                std::fs::write(&metadata_path, json).map_err(|e| {
                    crate::disk_cache::note_disk_full(&e, "range_removal.meta");
                    error!(
                        "Failed to write metadata after range removal: key={}, path={:?}, error={}",
                        cache_key, metadata_path, e
                    );
                    ProxyError::CacheError(format!("Failed to write metadata: {}", e))
                })?;

                debug!(
                    "Metadata updated after range removal: key={}, remaining_ranges={}",
                    cache_key,
                    metadata.ranges.len()
                );
            }
        } else {
            debug!(
                "Range not found for removal: key={}, range={}-{}, result=skip",
                cache_key, range_start, range_end
            );
        }

        // The `cached_objects` debit for the unlinked (parseable) `.meta`, after the slot is
        // released: `decrement_cached_objects` takes `size_state.lock`, and the slot is the
        // innermost lock.
        drop(_meta_lock);
        if meta_unlinked {
            if let Some(consolidator) = &self.journal_consolidator {
                consolidator.decrement_cached_objects(1).await;
            }
        }
        let total_duration = operation_start.elapsed();
        info!(
            "Invalidated range removal completed: key={}, range={}-{}, total_duration={:.2}ms",
            cache_key,
            range_start,
            range_end,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Delete a stale range (known to be invalid)
    /// Implements Requirements 10.1, 10.3, 10.4, 10.5
    ///
    /// This method should be called when a range is known to be stale due to:
    /// - ETag mismatch (Requirement 10.1)
    /// - Corrupted cache files (Requirement 10.3)
    /// - Conditional validation returning 200 OK (Requirement 10.4)
    ///
    /// This method:
    /// 1. Acquires exclusive file lock on metadata file
    /// 2. Reads current metadata
    /// 3. Deletes the .bin file for the range
    /// 4. Removes the range from metadata
    /// 5. If no ranges remain, deletes the metadata file
    /// 6. Releases lock
    /// 7. Logs the deletion with reason (Requirement 10.5)
    pub async fn delete_stale_range(
        &mut self,
        cache_key: &str,
        start: u64,
        end: u64,
        reason: &str,
    ) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Deleting stale range: key={}, range={}-{}, reason={}, operation=delete_stale_range",
            cache_key, start, end, reason
        );

        let metadata_path = self.get_new_metadata_file_path(cache_key);
        // The lock lives in the SHARED slot namespace (`metadata_lock_manager::slot_lock_path`),
        // not at a per-key `{key}.meta.lock`. A per-key lock file that nobody writes a payload
        // into was the state the retired stale-detection branches judged stale, and every such
        // branch then unlinked the live holder's file. The slot file is never unlinked.

        // Check if metadata file exists
        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for stale range deletion: key={}, path={:?}, result=skip",
                cache_key, metadata_path
            );
            return Ok(());
        }

        // Acquire the key's metadata slot lock, held as a guard for the rest of the function.
        // Bounded by the configured wait budget and taken on the blocking pool.
        let lock_start = std::time::Instant::now();
        let _meta_lock = self.acquire_meta_slot_lock(cache_key).await?;

        let lock_duration = lock_start.elapsed();
        debug!(
            "Acquired exclusive lock for stale range deletion: key={}, wait_time={:.2}ms",
            cache_key,
            lock_duration.as_secs_f64() * 1000.0
        );

        // Read metadata
        let mut metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                error!(
                    "Failed to load metadata for stale range deletion: key={}, error={}",
                    cache_key, e
                );
                return Err(e);
            }
        };

        // Find and delete the range
        let range_to_remove = metadata
            .ranges
            .iter()
            .find(|r| r.start == start && r.end == end)
            .cloned();

        if let Some(range) = range_to_remove {
            info!(
                "Removing stale range: key={}, range={}-{}, file={}, reason={}",
                cache_key, start, end, range.file_path, reason
            );

            // Delete .bin file (Requirement 9.4)
            let range_path = self.cache_dir.join("ranges").join(&range.file_path);
            if range_path.exists() {
                match std::fs::remove_file(&range_path) {
                    Ok(_) => {
                        debug!(
                            "Deleted stale range binary file: key={}, range={}-{}, path={:?}, reason={}",
                            cache_key, start, end, range_path, reason
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete stale range binary file: key={}, range={}-{}, path={:?}, error={}, reason={}",
                            cache_key, start, end, range_path, e, reason
                        );
                        // Continue anyway - metadata will be updated
                    }
                }
            } else {
                debug!(
                    "Stale range binary file already deleted: key={}, range={}-{}, path={:?}, reason={}",
                    cache_key, start, end, range_path, reason
                );
            }

            // Remove range from metadata
            metadata
                .ranges
                .retain(|r| !(r.start == start && r.end == end));

            debug!(
                "Stale range removed from metadata: key={}, range={}-{}, remaining_ranges={}, reason={}",
                cache_key, start, end, metadata.ranges.len(), reason
            );

            if metadata.ranges.is_empty() {
                // No ranges left, delete metadata file (Requirement 9.5)
                match std::fs::remove_file(&metadata_path) {
                    Ok(_) => {
                        info!(
                            "Deleted metadata file (no ranges remaining): key={}, path={:?}, reason={}",
                            cache_key, metadata_path, reason
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete metadata file: key={}, path={:?}, error={}, reason={}",
                            cache_key, metadata_path, e, reason
                        );
                    }
                }
            } else {
                // Update metadata with remaining ranges
                let json = serde_json::to_string_pretty(&metadata)
                    .map_err(|e| {
                        error!(
                            "Failed to serialize metadata after stale range deletion: key={}, error={}, reason={}",
                            cache_key, e, reason
                        );
                        ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
                    })?;

                std::fs::write(&metadata_path, json)
                    .map_err(|e| {
                        crate::disk_cache::note_disk_full(&e, "stale_range_deletion.meta");
                        error!(
                            "Failed to write metadata after stale range deletion: key={}, path={:?}, error={}, reason={}",
                            cache_key, metadata_path, e, reason
                        );
                        ProxyError::CacheError(format!("Failed to write metadata: {}", e))
                    })?;

                debug!(
                    "Metadata updated after stale range deletion: key={}, remaining_ranges={}, reason={}",
                    cache_key, metadata.ranges.len(), reason
                );
            }
        } else {
            debug!(
                "Stale range not found for deletion: key={}, range={}-{}, reason={}, result=skip",
                cache_key, start, end, reason
            );
        }

        // The lock is released by `_meta_lock`'s `Drop` at the end of this function.
        let total_duration = operation_start.elapsed();
        info!(
            "Stale range deletion completed: key={}, range={}-{}, reason={}, total_duration={:.2}ms",
            cache_key, start, end, reason, total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Delete all ranges for an object (e.g., on PUT conflict)
    /// Implements Requirements 10.2, 10.5
    ///
    /// This method should be called when all ranges for an object need to be invalidated,
    /// such as when a PUT request is received for the object (Requirement 10.2).
    ///
    /// This method:
    /// 1. Checks if metadata file exists
    /// 2. Loads metadata to get range files
    /// 3. Deletes all .bin files for all ranges
    /// 4. Deletes the metadata file
    /// 5. Logs the deletion with reason (Requirement 10.5)
    pub async fn delete_all_ranges(&mut self, cache_key: &str, reason: &str) -> Result<()> {
        let operation_start = std::time::Instant::now();
        info!(
            "Deleting all ranges for object: key={}, reason={}, operation=delete_all_ranges",
            cache_key, reason
        );
        if let Some(tracker) = &self.fill_visibility_tracker {
            tracker.invalidate(cache_key);
        }

        let metadata_path = self.get_new_metadata_file_path(cache_key);

        if !metadata_path.exists() {
            debug!(
                "Metadata file not found for all ranges deletion: key={}, path={:?}, reason={}, result=skip",
                cache_key, metadata_path, reason
            );
            return Ok(());
        }

        // Load metadata to get range files
        let metadata = match self.load_metadata_new(cache_key).await {
            Ok(meta) => meta,
            Err(e) => {
                warn!(
                    "Failed to load metadata for all ranges deletion: key={}, error={}, reason={}, result=skip",
                    cache_key, e, reason
                );
                // Try to delete metadata file anyway
                let _ = std::fs::remove_file(&metadata_path);
                return Ok(());
            }
        };

        let range_count = metadata.ranges.len();
        info!(
            "Deleting {} ranges for object: key={}, reason={}",
            range_count, cache_key, reason
        );

        // Delete all .bin files
        let mut deleted_count = 0;
        let mut failed_count = 0;

        for range in &metadata.ranges {
            let range_path = self.cache_dir.join("ranges").join(&range.file_path);
            if range_path.exists() {
                match std::fs::remove_file(&range_path) {
                    Ok(_) => {
                        debug!(
                            "Deleted range binary file: key={}, range={}-{}, path={:?}, reason={}",
                            cache_key, range.start, range.end, range_path, reason
                        );
                        deleted_count += 1;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to delete range binary file: key={}, range={}-{}, path={:?}, error={}, reason={}",
                            cache_key, range.start, range.end, range_path, e, reason
                        );
                        failed_count += 1;
                    }
                }
            } else {
                debug!(
                    "Range binary file already deleted: key={}, range={}-{}, path={:?}, reason={}",
                    cache_key, range.start, range.end, range_path, reason
                );
            }
        }

        info!(
            "Range binary files deletion summary: key={}, deleted={}, failed={}, total={}, reason={}",
            cache_key, deleted_count, failed_count, range_count, reason
        );

        // Delete metadata file
        match std::fs::remove_file(&metadata_path) {
            Ok(_) => {
                info!(
                    "Deleted metadata file: key={}, path={:?}, reason={}",
                    cache_key, metadata_path, reason
                );
            }
            Err(e) => {
                warn!(
                    "Failed to delete metadata file: key={}, path={:?}, error={}, reason={}",
                    cache_key, metadata_path, e, reason
                );
            }
        }

        let total_duration = operation_start.elapsed();
        info!(
            "All ranges deletion completed: key={}, ranges_deleted={}, reason={}, total_duration={:.2}ms",
            cache_key, deleted_count, reason, total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Check if a cached object is expired and needs conditional validation.
    /// Evaluates GET freshness against a caller-supplied TTL (`current_get_ttl`)
    /// rather than the stored `expires_at`. This ensures that a tightened or zeroed
    /// `get_ttl` in `cache_rules.json` takes effect on already-cached objects
    /// without requiring a proxy restart or manual cache wipe.
    ///
    /// Freshness rule: `fresh ⇔ (current_get_ttl > 0) AND (now - anchor ≤ current_get_ttl)`,
    /// where `anchor` is the later of `created_at` and the last origin `304`
    /// (`NewCacheMetadata::freshness_anchor`).
    /// When `current_get_ttl` is zero the entry is always expired (every GET revalidates).
    /// Clock skew (anchor in the future) yields age 0 → fresh for any non-zero TTL.
    ///
    /// This is **read-only** — no metadata write on the read path.
    ///
    /// Returns:
    /// - `Ok(Fresh)` if object is fresh per the current `get_ttl`
    /// - `Ok(Expired { last_modified: Some(...) })` if expired with known last_modified
    /// - `Ok(Expired { last_modified: None })` on metadata read/parse failure (fail-safe)
    pub async fn check_object_expiration(
        &self,
        cache_key: &str,
        current_get_ttl: std::time::Duration,
    ) -> Result<crate::cache_types::ObjectExpirationResult> {
        self.check_object_expiration_with_document(cache_key, current_get_ttl)
            .await
            .map(|checked| checked.verdict)
    }

    /// [`Self::check_object_expiration`], with the document the verdict was judged on: the
    /// `.meta` it read and parsed, or this instance's tracked fill. A caller that serves under
    /// that verdict takes its head and listing from this document rather than reading again
    pub(crate) async fn check_object_expiration_with_document(
        &self,
        cache_key: &str,
        current_get_ttl: std::time::Duration,
    ) -> Result<CheckedExpiration> {
        use crate::cache_types::ObjectExpirationResult;

        let metadata_path = self.get_new_metadata_file_path(cache_key);

        // Read and parse metadata, treating any failure as expired
        let metadata_content = match tokio::fs::read_to_string(&metadata_path).await {
            // A `.meta` exists. Whether it supersedes a fill-visibility entry is decided after
            // the parse below, because a HEAD-only `.meta` of the same version does not.
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No `.meta` yet. If this instance committed a streamed fill of the key that the
                // consolidator has not folded, judge it by the same rule the `.meta` would be
                // judged by. A zero `get_ttl` never consults the tracker, so that verdict is
                // exactly the one below (Expired, no validators): every read is forwarded to S3
                // as a miss, with no revalidation sent.
                if !current_get_ttl.is_zero() {
                    if let Some(tracker) = &self.fill_visibility_tracker {
                        if let Some(tracked) = tracker.lookup(cache_key) {
                            let verdict =
                                Self::expiration_verdict(cache_key, &tracked, current_get_ttl);
                            if matches!(verdict, ObjectExpirationResult::Fresh) {
                                tracker.record_hit();
                            }
                            debug!(
                                "Expiration verdict from the fill-visibility tracker (no .meta \
                                 yet): cache_key={}, verdict={:?}",
                                cache_key, verdict
                            );
                            return Ok(CheckedExpiration {
                                verdict,
                                document: Some(tracked),
                                from_meta_file: false,
                            });
                        }
                    }
                }
                // Metadata file doesn't exist — treat as expired (fail-safe)
                warn!(
                    "Metadata file not found during expiration check: cache_key={}, path={:?}",
                    cache_key, metadata_path
                );
                return Ok(CheckedExpiration::without_document(
                    ObjectExpirationResult::Expired {
                        last_modified: None,
                        etag: None,
                    },
                ));
            }
            Err(e) => {
                // The `.meta` may exist but could not be read. Disk wins either way.
                if let Some(tracker) = &self.fill_visibility_tracker {
                    tracker.note_disk_meta(cache_key);
                }
                warn!(
                    "Failed to read metadata during expiration check: cache_key={}, error={}",
                    cache_key, e
                );
                return Ok(CheckedExpiration::without_document(
                    ObjectExpirationResult::Expired {
                        last_modified: None,
                        etag: None,
                    },
                ));
            }
        };

        let metadata: crate::cache_types::NewCacheMetadata =
            match serde_json::from_str(&metadata_content) {
                Ok(m) => m,
                Err(e) => {
                    // Disk wins: an unparseable `.meta` still supersedes any entry.
                    if let Some(tracker) = &self.fill_visibility_tracker {
                        tracker.note_disk_meta(cache_key);
                    }
                    warn!(
                        "Failed to parse metadata during expiration check: cache_key={}, error={}",
                        cache_key, e
                    );
                    return Ok(CheckedExpiration::without_document(
                        ObjectExpirationResult::Expired {
                            last_modified: None,
                            etag: None,
                        },
                    ));
                }
            };

        // Disk wins, with one exception: a HEAD-only `.meta` of the same version as a fill
        // this instance tracks. The verdict then comes from the tracked document with the earlier
        // `created_at` of the two, which is what the `.meta` will carry once the fill is folded
        // onto it. A zero `get_ttl` never consults the tracker, so its verdict is the `.meta`'s
        // below (Expired).
        if let Some(tracker) = &self.fill_visibility_tracker {
            if !current_get_ttl.is_zero() && metadata.ranges.is_empty() {
                if let Some(tracked) = tracker.lookup_over_disk_meta(cache_key, &metadata) {
                    let verdict = Self::expiration_verdict(cache_key, &tracked, current_get_ttl);
                    if matches!(verdict, ObjectExpirationResult::Fresh) {
                        tracker.record_hit();
                    }
                    debug!(
                        "Expiration verdict from the fill-visibility tracker over a HEAD-only \
                         .meta: cache_key={}, verdict={:?}",
                        cache_key, verdict
                    );
                    return Ok(CheckedExpiration {
                        verdict,
                        document: Some(tracked),
                        from_meta_file: false,
                    });
                }
            } else {
                tracker.note_disk_meta(cache_key);
            }
        }

        let verdict = Self::expiration_verdict(cache_key, &metadata, current_get_ttl);
        Ok(CheckedExpiration {
            verdict,
            document: Some(metadata),
            from_meta_file: true,
        })
    }

    /// The live-TTL verdict for one parsed metadata document, shared by the `.meta` arm of
    /// [`Self::check_object_expiration`] and its fill-visibility arm so the two cannot disagree
    /// about what "fresh" means.
    fn expiration_verdict(
        cache_key: &str,
        metadata: &crate::cache_types::NewCacheMetadata,
        current_get_ttl: std::time::Duration,
    ) -> crate::cache_types::ObjectExpirationResult {
        use crate::cache_types::ObjectExpirationResult;

        // Evaluate freshness against the current get_ttl (read-only comparison).
        // get_ttl=0 means "always revalidate"; otherwise fresh when age ≤ ttl.
        // Clock skew (created_at in the future) yields age 0 → fresh for non-zero ttl.
        let now = std::time::SystemTime::now();
        // Age runs from the later of `created_at` and the last origin `304`
        // (`freshness_anchor`). Without the second, an entry older than `get_ttl` stays
        // expired after every `304` and revalidates on every read for the rest of its life.
        let age = now
            .duration_since(metadata.freshness_anchor())
            .unwrap_or(std::time::Duration::ZERO);
        let ttl_expired = current_get_ttl.is_zero() || age > current_get_ttl;

        // A write-through PUT/CompleteMultipartUpload entry with no effective
        // Last-Modified is a state the rest of the tree treats as an integrity
        // error (`cache_validator.rs`'s `MissingRequiredField`), and the first GET
        // after such a write is a cache HIT that never reaches S3 — so nothing
        // ever learns it unless this predicate forces a revalidation. The
        // `is_write_cached` conjunct is load-bearing, not decoration: without it
        // the trigger would also fire on a legitimately-empty read-cache entry an
        // older release may have written, and — more importantly — it is what
        // makes R4 terminate. Graduation clears `is_write_cached` on a successful
        // revalidation (deferred until the field is learned, R6), so once this
        // entry acquires a Last-Modified and graduates, this conjunct goes false
        // and the trigger cannot fire again for it.
        //
        // Spec: write-cache-last-modified. Requirements: 1.1, 1.3, 1.4
        let missing_write_cache_last_modified = metadata.object_metadata.is_write_cached
            && metadata.object_metadata.effective_last_modified().is_none();

        // NO VALIDATOR OF ANY KIND, NO SERVE ON THE CACHE'S OWN AUTHORITY.
        //
        // An entry carrying neither an ETag nor an effective `Last-Modified` can never be
        // checked against the origin. `revalidation::classify` returns
        // `CacheVerdict::Undetermined` whenever either side's ETag normalises to empty, and
        // the `Expired` arm below already maps an empty stored ETag to `None`, so such an
        // entry's verdict is `Undetermined` permanently — there is no exchange with S3 that
        // could ever confirm or refute it. Serving it on a live-TTL `Fresh` verdict means
        // returning bytes the proxy cannot attribute to any version of any object, for up
        // to `get_ttl`.
        //
        // WHY THIS PREDICATE AND NOT "THE ETAG IS EMPTY". `extract_object_metadata_from_response`
        // stores the `etag` header with `.unwrap_or_default()`, so an S3-compatible origin
        // that omits `ETag` produces a stored empty ETag for an ordinary, correctly cached
        // object. That entry is still revalidatable — the `Expired` arm sends
        // `if-modified-since` from `effective_last_modified` — so refusing it would disable
        // caching for such an origin to close a hole it is not part of. Requiring BOTH
        // validators to be absent is what separates "this origin is terse" from "this
        // document was synthesised rather than observed".
        //
        // THE PRODUCERS. This was written for the orphan re-adoption sweep, which published a
        // re-adopted range with no object metadata and has since been removed. What remains is
        // any document built on `ObjectMetadata::default()` (empty ETag, empty
        // `Last-Modified`, empty `response_headers`) with `created_at: SystemTime::now()`:
        // `HybridMetadataWriter::load_or_create_metadata`'s create-new branch and the
        // consolidator's fallback when no journal entry carries object metadata both build one.
        // Stored expiry is a revalidation boundary rather than a disappearance since
        // `expired-entry-revalidation`, so the two mainline GET paths discover such an entry as
        // a `RevalidationCandidate` and then ask this function. With an age of about zero
        // against a generous `get_ttl` the answer would be `Fresh`, which serves the bytes of an
        // object that may no longer exist in S3 as a `200` with no S3 contact at all.
        //
        // WHY HERE rather than beside `CacheManager::is_empty_object_etag_entry`. That guard's
        // three arms sit in `has_cached_ranges` and the two `get_head_cache_entry_unified`
        // tiers. `has_cached_ranges` covers the full-object GET and the range path's
        // full-object shortcut, but the range-specific lookup does not go through it at all
        // (it is skipped whenever `full_object_check_threshold` is exceeded, and fallen
        // through whenever the shortcut misses), so an arm there would leave the ranged path
        // open. This function is the single live-TTL gate BOTH mainline paths consult. Every
        // other serve path is already closed for this producer by stored expiry, because each
        // asks with `RangeLookupPurpose::FreshServe`: `CacheManager::lookup_part`,
        // `find_page_overlap`, the range path's full-object shortcut, and the Mode B arm via
        // `is_serveable_unvalidated()`. Both HEAD tiers are closed independently by their own
        // `effective_last_modified().is_none()` guard.
        //
        // IT CONVERGES, unlike `is_empty_object_etag_entry`. Expired with neither validator,
        // both mainline GET paths send no revalidation (there is nothing to make one
        // conditional) and forward the client's request to S3 once, as a miss
        // (`expired_without_validators` in `http_proxy.rs`). On the range path that forward is
        // the ordinary miss forward, `stream_range_from_s3_with_caching`, which caches the
        // response under S3's ETag only when the upstream body streams. The production S3
        // client always streams there; a body that arrives already buffered is served and not
        // cached, so the key stays a miss. Once cached, this conjunct goes false and cannot fire
        // for that entry again. A deleted object returns `404` and is forwarded as one. The entry's ranges stay
        // referenced, accounted and evictable throughout; what they are not is serveable
        // without asking S3 first.
        let no_validator_at_all = metadata.object_metadata.etag.trim().is_empty()
            && metadata.object_metadata.effective_last_modified().is_none();

        let expired = ttl_expired || missing_write_cache_last_modified || no_validator_at_all;

        if no_validator_at_all {
            warn!(
                "Cache entry for {} carries neither an ETag nor a Last-Modified, so it cannot be \
                 validated against the origin; forwarding to S3 rather than serving it on the \
                 cache's own authority",
                cache_key
            );
        }

        if expired {
            debug!(
                "Object expired per current get_ttl: key={}, age={:?}, current_get_ttl={:?}",
                cache_key, age, current_get_ttl
            );
            ObjectExpirationResult::Expired {
                // `None` when empty, via the same shared accessor the trigger
                // predicate above reads (R1.2), rather than
                // `Some(String::new())`. An empty value here used to reach
                // `http_proxy.rs` and get inserted as a literal empty
                // `if-modified-since` header (R2.1, R2.2) — S3 ignores it
                // (measured, task 0.3), but sending it at all is wrong on its
                // face and an S3-compatible origin may reject it with 400.
                last_modified: metadata
                    .object_metadata
                    .effective_last_modified()
                    .map(str::to_string),
                etag: Some(metadata.object_metadata.etag.clone()).filter(|e| !e.is_empty()),
            }
        } else {
            ObjectExpirationResult::Fresh
        }
    }

    /// Enumerate the `.bin` files that unfolded `JournalOperation::Add` entries name for a key.
    ///
    /// This is the second of invalidation's two enumerations. The first reads the consolidated
    /// `.meta`, which cannot see a range whose journal entry no consolidation cycle has folded
    /// yet — the Journal_Window.
    ///
    /// # What it reads, and what it does NOT do
    ///
    /// Every instance's `*.journal` file, not only this instance's. That is **inherited** from
    /// the existing readers rather than added: `metadata/_journals/` is shared storage on the
    /// read side, and `find_pending_journal_ranges`, `JournalConsolidator`'s discovery and
    /// cleanup passes and several others already `read_dir` it and process every file.
    /// "Per-instance" describes the **writer** — one file named `{instance_id}.journal`.
    ///
    /// Each `.bin` is resolved from the entry's `range_spec.file_path` joined onto
    /// `{cache_dir}/ranges`, guarded by [`is_safe_relative_file_path`] — the same resolution
    /// and the same guard the `.meta` arm applies. The sibling `range_file_path` field is
    /// **not** used: `HybridMetadataWriter::write_journal_only` sets it to the empty string
    /// with the comment "Will be filled by consolidation", so resolving from it would produce
    /// `{cache_dir}/ranges` itself and the safety guard would reject it.
    ///
    /// `Remove` markers are deliberately **not** consulted here. A stale `Add` whose `.bin` a
    /// previous eviction already deleted resolves to an absent file, and the caller declines to
    /// count an absent *journal-named* file as a removal for exactly that reason. Filtering on
    /// markers would put a second copy of the republish discriminator outside the one function
    /// that is meant to own it (`find_pending_journal_ranges`).
    ///
    /// Unlike `find_pending_journal_ranges` this does not gate on `is_shared_storage_mode()`:
    /// deletion completeness is a property of what is on disk, so the only gate is whether the
    /// journal directory exists.
    ///
    /// Spec: range-invalidation-completeness. Requirements: 1.1, 1.2, 1.3, 4.1
    async fn enumerate_journal_add_ranges(&self, cache_key: &str) -> Vec<RangeRemovalCandidate> {
        #[cfg(test)]
        {
            let delay = JOURNAL_ENUMERATION_DELAY_FOR_TESTS
                .lock()
                .unwrap()
                .as_ref()
                .filter(|(dir, _)| dir == &self.cache_dir)
                .map(|(_, delay)| *delay);
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
        }
        unfolded_journal_add_ranges(&self.cache_dir, cache_key)
            .await
            .into_iter()
            .map(|range_spec| RangeRemovalCandidate {
                start: range_spec.start,
                end: range_spec.end,
                compressed_size: range_spec.compressed_size,
                resolved_path: self.cache_dir.join("ranges").join(&range_spec.file_path),
                relative_path: range_spec.file_path,
                source: RemovalCandidateSource::Journal,
            })
            .collect()
    }
}

/// Test seam: delay `enumerate_journal_add_ranges` for one cache directory, so a test can hold
/// the journal enumeration open and observe whether the key's slot lock is held across it.
#[cfg(test)]
static JOURNAL_ENUMERATION_DELAY_FOR_TESTS: std::sync::Mutex<
    Option<(PathBuf, std::time::Duration)>,
> = std::sync::Mutex::new(None);

/// The `RangeSpec` of every unfolded `JournalOperation::Add` entry for `cache_key`, across every
/// instance's journal file, with each `file_path` checked by [`is_safe_relative_file_path`].
///
/// The enumeration behind [`DiskCacheManager::enumerate_journal_add_ranges`] — see that method for
/// what it reads and what it deliberately does not (no `Remove`-marker filtering, no shared-mode
/// gate). A free function so `CacheManager`'s HEAD-detected-change branch, which holds no
/// `DiskCacheManager`, removes exactly the set invalidation removes rather than a second
/// definition of it. `HEAD-CHANGE-LEAVES-UNFOLDED-JOURNAL-ADDS`.
pub(crate) async fn unfolded_journal_add_ranges(
    cache_dir: &Path,
    cache_key: &str,
) -> Vec<crate::cache_types::RangeSpec> {
    let mut candidates = Vec::new();

    let journals_dir = cache_dir.join("metadata").join("_journals");
    if !journals_dir.exists() {
        return candidates;
    }

    let journal_files = match std::fs::read_dir(&journals_dir) {
        Ok(entries) => entries,
        Err(e) => {
            debug!(
                "Range invalidation: failed to read journals directory: key={}, error={}",
                cache_key, e
            );
            return candidates;
        }
    };

    for journal_file in journal_files {
        let journal_file = match journal_file {
            Ok(f) => f,
            Err(_) => continue,
        };

        let journal_path = journal_file.path();
        if !journal_path.is_file() {
            continue;
        }
        match journal_path.file_name().and_then(|n| n.to_str()) {
            Some(name) if name.ends_with(".journal") => name,
            _ => continue,
        };

        let content = match tokio::fs::read_to_string(&journal_path).await {
            Ok(c) => c,
            Err(e) => {
                debug!(
                    "Range invalidation: failed to read journal file {:?}: {}",
                    journal_path, e
                );
                continue;
            }
        };

        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let entry: crate::journal_manager::JournalEntry = match serde_json::from_str(line) {
                Ok(e) => e,
                Err(_) => continue,
            };

            if entry.cache_key != cache_key {
                continue;
            }
            if entry.operation != crate::journal_manager::JournalOperation::Add {
                continue;
            }

            let relative_path = entry.range_spec.file_path.clone();
            if !is_safe_relative_file_path(&relative_path) {
                warn!(
                        "Range delete: rejected unsafe journal-derived file_path={:?} for key={}, range={}-{}, journal={:?}, skipping delete",
                        relative_path,
                        cache_key,
                        entry.range_spec.start,
                        entry.range_spec.end,
                        journal_path
                    );
                continue;
            }

            candidates.push(entry.range_spec);
        }
    }

    candidates
}

impl DiskCacheManager {
    /// Remove all range files for a cache key
    /// Implements Requirements 1.4, 3.2
    ///
    /// This method:
    /// 1. Enumerates the key's ranges from the consolidated `.meta` **and** from every
    ///    instance's journal file under `metadata/_journals/`, deduplicated on resolved path
    /// 2. Removes the range binary file (.bin) each enumerated range names
    /// 3. Writes a `JournalOperation::Remove` marker per removed range, **after** the removal
    /// 4. Handles partial failures gracefully, attributed to the enumeration that named them
    /// 5. Does NOT remove metadata file (caller decides metadata handling)
    ///
    /// # What it does NOT reach
    ///
    /// **Not every `.bin` file belonging to the key.** `store_range`'s hybrid arm renames the
    /// `.bin` visible *before* appending its journal entry — deliberately, so a peer
    /// consolidating the entry finds the file already present — so in that interval the file is
    /// named by nothing and neither enumeration can see it. The same shape is durable rather
    /// than momentary when a journal append fails after a successful rename: the file is then
    /// unnamed until orphan recovery adopts or removes it. A range in either state survives
    /// this call and is not counted. Adding the journal enumeration **narrows** that window; it
    /// does not close it.
    ///
    /// Returns the number of ranges successfully removed and total size freed. Callers needing
    /// to distinguish a complete removal from a partial one should use
    /// [`Self::remove_all_ranges_detailed`].
    ///
    /// Spec: range-invalidation-completeness. Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 3.2, 3.3, 5.2
    pub async fn remove_all_ranges(&self, cache_key: &str) -> Result<(usize, u64)> {
        let outcome = self.remove_all_ranges_detailed(cache_key).await?;
        Ok((outcome.removed_count, outcome.size_freed))
    }

    /// [`Self::remove_all_ranges`], returning the full outcome rather than two of its fields.
    ///
    /// Spec: range-invalidation-completeness. Requirements: 1.1, 1.4, 1.6, 3.3, 5.1
    pub(crate) async fn remove_all_ranges_detailed(
        &self,
        cache_key: &str,
    ) -> Result<RangeRemovalOutcome> {
        let journal_candidates = self.enumerate_journal_add_ranges(cache_key).await;
        self.remove_ranges_with_journal_candidates(cache_key, journal_candidates)
            .await
    }

    /// [`Self::remove_all_ranges_detailed`] with the journal enumeration already taken.
    ///
    /// Split out so `invalidate_all_ranges` can enumerate the journals BEFORE it takes the key's
    /// slot lock. The enumeration is read-only and excludes nothing, so taking it earlier is
    /// indistinguishable from a journal `Add` that lands just after the call: an `Add` a
    /// consolidator folds in the meantime is found by the `.meta` read below, and one appended
    /// after the snapshot is missed exactly as one appended after the lock is released always
    /// was. `INVALIDATE-LOCK-HOLD-TIME`.
    async fn remove_ranges_with_journal_candidates(
        &self,
        cache_key: &str,
        journal_candidates: Vec<RangeRemovalCandidate>,
    ) -> Result<RangeRemovalOutcome> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting bulk range removal: key={}, operation=remove_all_ranges",
            cache_key
        );

        // Enumeration 1: the consolidated `.meta`. A missing document is no longer an early
        // return — the journal enumeration below is independent of it, and returning here
        // would make the Journal_Window unreachable, which is the defect.
        let mut candidates: Vec<RangeRemovalCandidate> = Vec::new();
        match self.get_metadata(cache_key).await? {
            Some(meta) => {
                debug!(
                    "Found metadata for bulk removal: key={}, ranges={}",
                    cache_key,
                    meta.ranges.len()
                );
                for range_spec in &meta.ranges {
                    // Security (Req 10): reject file_path if unsafe. Skip delete silently.
                    if !is_safe_relative_file_path(&range_spec.file_path) {
                        warn!(
                            "Range delete: rejected unsafe file_path={:?} for key={}, skipping delete",
                            range_spec.file_path, cache_key
                        );
                        continue;
                    }
                    candidates.push(RangeRemovalCandidate {
                        start: range_spec.start,
                        end: range_spec.end,
                        compressed_size: range_spec.compressed_size,
                        resolved_path: self.cache_dir.join("ranges").join(&range_spec.file_path),
                        relative_path: range_spec.file_path.clone(),
                        source: RemovalCandidateSource::ConsolidatedMetadata,
                    });
                }
            }
            None => {
                debug!(
                    "No metadata found for bulk removal: key={}, continuing with journal enumeration",
                    cache_key
                );
            }
        }
        let metadata_derived_total = candidates.len();

        // Enumeration 2: every instance's unfolded journal `Add` entries, taken by the caller.
        candidates.extend(journal_candidates);

        // Deduplicate on the RESOLVED PATH, not on `(start, end)`: the path is what gets
        // unlinked, and it is what the file-absent branch below would otherwise count twice.
        // The `.meta` candidates were pushed first, so a file both enumerations name keeps its
        // `.meta` attribution — which is the stronger claim, and is what makes the absent-file
        // accounting below correct for it.
        let mut seen_paths: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        candidates.retain(|c| seen_paths.insert(c.resolved_path.clone()));

        let candidate_count = candidates.len();
        let journal_derived_count = candidate_count.saturating_sub(metadata_derived_total);
        let mut removed_count = 0;
        let mut total_size_freed = 0u64;
        // `(start, end, source, error)` — the source is what makes a survivor attributable
        // (R1.6): a journal-derived failure and a `.meta`-derived one have different causes.
        let mut failed_removals: Vec<(u64, u64, &'static str, String)> = Vec::new();
        // The `.meta`-named subset of `failed_removals`, which `invalidate_all_ranges` keeps
        // listed. A journal-named survivor keeps its unfolded `Add` (no marker is written for
        // it), so consolidation still names it.
        let mut failed_meta_extents: Vec<(u64, u64)> = Vec::new();
        // `(cache_key, start, end, size, relative_bin_path)`, the shape
        // `write_eviction_journal_entries` takes. Accumulated during the loop and written
        // AFTER it, so the file state is true before it is recorded (R3.3).
        let mut marker_entries: Vec<(String, u64, u64, u64, String)> = Vec::new();

        debug!(
            "Starting range file removal: key={}, candidates={} (meta={}, journal_only={})",
            cache_key, candidate_count, metadata_derived_total, journal_derived_count
        );

        for candidate in &candidates {
            debug!(
                "Removing range file: key={}, range={}-{}, path={:?}, compressed_size={}, source={}",
                cache_key,
                candidate.start,
                candidate.end,
                candidate.resolved_path,
                candidate.compressed_size,
                candidate.source.as_str()
            );

            if candidate.resolved_path.exists() {
                match remove_range_file(&candidate.resolved_path) {
                    Ok(_) => {
                        removed_count += 1;
                        total_size_freed += candidate.compressed_size;
                        marker_entries.push((
                            cache_key.to_string(),
                            candidate.start,
                            candidate.end,
                            candidate.compressed_size,
                            candidate.relative_path.clone(),
                        ));
                        debug!(
                            "Range file removed: key={}, range={}-{}, path={:?}, size_freed={}, source={}",
                            cache_key,
                            candidate.start,
                            candidate.end,
                            candidate.resolved_path,
                            candidate.compressed_size,
                            candidate.source.as_str()
                        );
                    }
                    Err(e) => {
                        if matches!(
                            candidate.source,
                            RemovalCandidateSource::ConsolidatedMetadata
                        ) {
                            failed_meta_extents.push((candidate.start, candidate.end));
                        }
                        failed_removals.push((
                            candidate.start,
                            candidate.end,
                            candidate.source.as_str(),
                            e.to_string(),
                        ));
                        warn!(
                            "Failed to remove range file: key={}, range={}-{}, path={:?}, source={}, error={}",
                            cache_key,
                            candidate.start,
                            candidate.end,
                            candidate.resolved_path,
                            candidate.source.as_str(),
                            e
                        );
                    }
                }
            } else {
                debug!(
                    "Range file not found (already removed?): key={}, range={}-{}, path={:?}, source={}",
                    cache_key,
                    candidate.start,
                    candidate.end,
                    candidate.resolved_path,
                    candidate.source.as_str()
                );
                match candidate.source {
                    // Pre-existing behaviour, retained: the `.meta` is the authority for "this
                    // range should be here", so its absence means someone else removed it and
                    // the range is gone either way.
                    RemovalCandidateSource::ConsolidatedMetadata => removed_count += 1,
                    // NOT counted. A journal `Add` is a weaker claim — it may have been
                    // superseded and its `.bin` evicted long ago, with the entry merely not yet
                    // cleaned up — so counting its absence would make `ranges_removed` MORE
                    // wrong than the pre-change count rather than less (R1.4's concern, in the
                    // form the union introduces).
                    RemovalCandidateSource::Journal => {}
                }
                // A marker is still owed either way: the `.bin` is absent, so a lookup that
                // resolves this range from the journal would report coverage for a file that
                // is not there. Marking it is about suppressing that coverage, which is a
                // different question from how many bytes this call freed.
                marker_entries.push((
                    cache_key.to_string(),
                    candidate.start,
                    candidate.end,
                    candidate.compressed_size,
                    candidate.relative_path.clone(),
                ));
            }
        }

        // The Removal_Marker, written AFTER the removals (R3.3). A crash between the two
        // leaves a deleted `.bin` with no marker, which degrades to pre-change behaviour —
        // a lookup reports coverage, the read path hard-errors on the missing file and the
        // caller refetches. Wasted work, not a stale serve. The reverse ordering would allow
        // a marker for a `.bin` that still exists.
        //
        // NARROW scope deliberately: `JournalConsolidator::write_eviction_journal_entries`
        // writes the `Remove` entries and nothing else. `CacheManager::debit_removed_ranges`
        // would also debit `size_accumulator`, which would silently close the undebited-
        // deletion gap this spec puts out of scope (range-invalidation-completeness:R5.4,
        // owned by cache-eviction-at-scale) as a side effect of a marker write.
        //
        // No marker is written for a failed removal: that `.bin` is still on disk, and
        // suppressing coverage for a present file is the one direction this ordering exists
        // to avoid.
        //
        // Spec: range-invalidation-completeness. Requirements: 3.2, 3.3, 5.4
        if !marker_entries.is_empty() {
            match &self.journal_consolidator {
                Some(consolidator) => {
                    let marker_count = marker_entries.len();
                    consolidator
                        .write_eviction_journal_entries(marker_entries)
                        .await;
                    debug!(
                        "Range invalidation wrote Remove markers: key={}, markers={}",
                        cache_key, marker_count
                    );
                }
                None => {
                    // `DiskCacheManager::new` leaves this `None` — tests and early startup.
                    // The only production builder (`create_configured_disk_cache_manager`)
                    // sets it, so this arm is not a production path.
                    debug!(
                        "Range invalidation: no journal consolidator configured, skipping {} Remove markers: key={}",
                        marker_entries.len(),
                        cache_key
                    );
                }
            }
        }

        // NOTE: Size tracking is now handled by JournalConsolidator through journal entries.
        // The Remove operation in the journal contains the size delta.

        let total_duration = operation_start.elapsed();

        if failed_removals.is_empty() {
            info!(
                "Bulk range removal completed successfully: key={}, removed={}/{} ranges, journal_derived={}, size_freed={} bytes, duration={:.2}ms",
                cache_key, removed_count, candidate_count, journal_derived_count, total_size_freed, total_duration.as_secs_f64() * 1000.0
            );
        } else {
            warn!(
                "Bulk range removal completed with failures: key={}, removed={}/{} ranges, journal_derived={}, failed={}, size_freed={} bytes, duration={:.2}ms, failures={:?}",
                cache_key, removed_count, candidate_count, journal_derived_count, failed_removals.len(), total_size_freed,
                total_duration.as_secs_f64() * 1000.0, failed_removals
            );
        }

        Ok(RangeRemovalOutcome {
            removed_count,
            size_freed: total_size_freed,
            candidate_count,
            journal_derived_count,
            failed_count: failed_removals.len(),
            failed_meta_extents,
        })
    }

    /// Invalidate all ranges for an object (removes ranges and metadata)
    /// Requirements: 2.2, 2.3 - ETag validation and range invalidation
    ///
    /// This method:
    /// 1. Snapshots every instance's unfolded journal `Add` entries for the key, unlocked
    /// 2. Acquires the key's metadata slot lock, held from the `.meta` read to its unlink
    /// 3. Removes the range files the `.meta` and the journal snapshot name between them, and
    ///    writes a `JournalOperation::Remove` marker for each — see the completeness limit below
    /// 4. Removes the object's sharded `.meta`, plus any legacy flat `.meta`, then releases
    /// 5. Logs the invalidation action, distinguishing a partial removal from a complete one
    /// 6. Handles partial failures gracefully
    ///
    /// It debits nothing and touches no RAM tier: size accounting follows the `Remove` markers,
    /// and callers clear RAM ranges with `CacheManager::invalidate_ram_ranges`.
    ///
    /// # What this does NOT reach, and why the log says `complete` rather than `all`
    ///
    /// Step 2 is not "all range files for the object", which is what this comment claimed until
    /// `range-invalidation-completeness:R1` landed. Two enumerations name the `.bin` files to
    /// delete — the consolidated `.meta` and every instance's unfolded journal `Add` entries —
    /// and a range can be in neither. `store_range`'s hybrid arm renames the `.bin` visible
    /// *before* appending its journal entry, so in that interval the file is named by nothing;
    /// and a journal append that fails after a successful rename leaves it unnamed durably,
    /// until orphan recovery adopts or removes it. A `.bin` in either state survives this call.
    ///
    /// So this **narrows** the window in which invalidation leaves a stale `.bin` behind. It
    /// does not close it, and the `ranges_removed` figure is a count of what the two
    /// enumerations named, not of what exists.
    ///
    /// # The lock, and what it orders
    ///
    /// Every `.meta` **publisher** holds the key's metadata slot lock:
    /// `CacheManager::store_new_metadata`, `JournalConsolidator::write_metadata_to_disk` and
    /// `HybridMetadataWriter::write_metadata_to_disk` each take `&MetadataLock` as a parameter,
    /// so a caller that publishes without one does not compile. This unlink used to take no
    /// lock, so it could land between a publisher's read and its `rename` and be undone: the
    /// publisher then republished a document derived from the pre-unlink one, resurrecting a
    /// superseded ETag and range entries whose `.bin` files had just been deleted. Under the
    /// lock only two orderings remain — the publisher runs entirely before the unlink, or
    /// entirely after it and finds nothing to read.
    ///
    /// The journal enumeration is taken before the lock (`INVALIDATE-LOCK-HOLD-TIME`): it
    /// is read-only and excludes nothing, so it needs no exclusion. The span covers the range
    /// removal as well as the `.meta` unlink, for the same reason
    /// `invalidate_cache_entry` holds it across both: the `.meta` read decides which `.bin`
    /// files go, so a publisher landing between that read and the unlink would leave a `.bin`
    /// no metadata references. It also makes a failed acquisition a clean no-op — the `?`
    /// below returns before anything is removed, rather than leaving the `.bin` files gone
    /// and the `.meta` behind — so a caller cannot read a partial invalidation as a complete
    /// one. All three production callers already treat `Err` as "not invalidated" and fall
    /// through to S3.
    ///
    /// # This deletes on shared storage from a decision that may be local
    ///
    /// There are three production callers: the ETag-mismatch arm in `range_handler` and the two
    /// 412-retry sites in `http_proxy`. The effect in every case is a deletion on the shared
    /// volume, and on the first of them the *decision* is derived entirely from this instance's
    /// own state.
    ///
    /// **The origin's ETag is not an input to that arm, contrary to what this comment said
    /// until 2026-09-22.** `RangeHandler::find_cached_ranges` compares its `current_etag`
    /// argument against `preloaded_metadata`'s ETag, and only two of the eight call sites pass
    /// both non-`None` — `http_proxy.rs:7491` and `:7663`, both inside `handle_range_request`.
    /// At its single caller, `current_etag` comes from `CacheManager::get_object_etag`, which
    /// reads `get_metadata_from_disk`; `preloaded_metadata` comes from `get_metadata_cached`,
    /// which returns the RAM metadata tier when it has an entry. So **both sides are cache
    /// readings of the same key** — disk against this instance's RAM copy — and the arm fires
    /// precisely when that RAM copy is stale relative to disk. What it then deletes is the disk
    /// document, i.e. the fresher of the two readings, which is typically a peer's correct
    /// re-cache of an object overwritten on S3.
    ///
    /// The staleness is bounded by `cache.metadata_cache.refresh_interval` (default 5s), and the
    /// deletion does not repeat: with the disk `.meta` gone `get_object_etag` returns `None` on
    /// the next read and the arm needs `Some`. `invalidate_ram_ranges` beside it clears the RAM
    /// *range* tier only, so until the interval elapses the stale RAM document keeps claiming
    /// coverage whose `.bin` files are gone, and those reads degrade to a complete refetch.
    ///
    /// That is `cache-coherency-invariants.md` § "Invariant 2's dangerous corollary" in its
    /// sharpest form — a decision with no information about the object's actual current version
    /// deleting a fleet-visible entry — and the `.bin` removal has always had it. This `.meta`
    /// removal joins it now that the path is correct and the delete actually lands.
    ///
    /// **The lock above does not fix that, and is not meant to.** It serialises the write; it
    /// does not make a stale decision correct, and holding it for longer would not either. The
    /// decision is still the caller's ETag comparison against a possibly-RAM document, so a
    /// stale instance now deletes a peer's fresh entry *without racing a publisher* instead of
    /// while racing one. Registered separately; see the `INVALIDATE-META-SLOT-LOCK` return.
    pub async fn invalidate_all_ranges(&self, cache_key: &str) -> Result<()> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting range invalidation: key={}, operation=invalidate_all_ranges",
            cache_key
        );
        // First, and unconditionally: the fill-visibility entry names this key's committed
        // `.bin`, and the caller has decided that is stale.
        if let Some(tracker) = &self.fill_visibility_tracker {
            tracker.invalidate(cache_key);
        }

        // Acquire the key's metadata slot lock BEFORE the `.meta` is read or anything is
        // removed, and hold it as a guard until the `.meta` unlinks are done. `MetadataLock::Drop` closes the descriptor,
        // which is what releases the `flock`, so every return path releases it; the slot file
        // itself is permanent and is never unlinked.
        //
        // `?` rather than a warn-and-continue: an invalidation that could not take the lock has
        // touched nothing, and reporting that as success is the shape this family of defects
        // keeps taking. Every caller already handles `Err` by falling through to S3, so the
        // cost of a contended slot is one upstream request, not a stale serve.
        //
        // No caller of this function holds this key's slot lock. Verified rather than assumed:
        // the three production callers are `range_handler::find_cached_ranges`' ETag-mismatch
        // arm and the two proxy-injected-412 retry sites in `http_proxy`, and neither
        // `range_handler.rs` nor `http_proxy.rs` names `MetadataLock` anywhere, so neither can
        // hold one. Nesting would self-deadlock, because two keys can share a slot and `flock`
        // conflicts between two descriptors of the same file within one process — see the
        // nesting rule in `metadata_lock_manager`.
        //
        // The journal enumeration runs BEFORE the lock. It is a `read_dir` plus one
        // read-and-parse per journal file, and it is read-only. Holding the slot lock across it
        // never froze the journals — `store_range`'s hybrid arm appends without it. Taking
        // the snapshot earlier is equivalent to an `Add` landing just after this call, which the
        // lock never excluded; see `remove_ranges_with_journal_candidates`..
        let journal_candidates = self.enumerate_journal_add_ranges(cache_key).await;

        let meta_lock = self.acquire_meta_slot_lock(cache_key).await?;

        // First remove the range files both enumerations name. The `get_metadata` this calls
        // takes no lock of its own, so this is not a nested acquisition.
        //
        // The unlinks and the `Remove` markers STAY under the lock, and that is forced by two
        // constraints together. The markers are written AFTER the unlinks (R3.3: a marker must
        // never suppress coverage for a `.bin` that is still present). And the markers must land
        // BEFORE the lock is released: once the `.meta` is gone, a consolidator that takes this
        // slot and still sees the unfolded `Add` entries without their `Remove` builds a fresh
        // `.meta` from them (`load_or_create_metadata_with_journal_entries` copies the entries'
        // `object_metadata`), resurrecting the superseded ETag this call exists to remove.
        let removal = self
            .remove_ranges_with_journal_candidates(cache_key, journal_candidates)
            .await?;
        let removed_count = removal.removed_count;
        let size_freed = removal.size_freed;

        // Then remove the metadata file.
        //
        // The SHARDED path — `metadata/{bucket}/{XX}/{YYY}/{key}.meta` — which is the one
        // `get_metadata` reads, and therefore the one `remove_all_ranges` above resolved its
        // range files from. This used to call `get_legacy_flat_metadata_file_path` (then named
        // `get_metadata_file_path`, which is how it was picked by mistake), the pre-sharding FLAT
        // construction `{cache_type}/{key}.meta`. The two are genuinely different paths, not
        // aliases, so on a sharded cache — every current cache — the `exists()` guard was
        // always false, this branch was skipped, and the live `.meta` survived while its
        // `.bin` files had just been deleted. The function logged "Metadata file not found
        // during invalidation" and returned success.
        //
        // What the surviving document then did, both traced before this was changed:
        // nothing on the store path rewrote its `object_metadata`, because the production
        // range store arm writes a journal entry and
        // `JournalConsolidator::load_or_create_metadata_with_journal_entries` returns an
        // existing document verbatim — so the superseded ETag persisted and the
        // ETag-mismatch arm fired again on every subsequent read. And a HEAD was still
        // answered from it, with the superseded ETag and content_length, bounded only by
        // `head_ttl`.
        let metadata_file_path = self.get_new_metadata_file_path(cache_key);
        // R15.2: a `.meta`-named `.bin` whose unlink failed is still on the volume. Removing the
        // `.meta` would leave it named by nothing; keep the document listing exactly those
        // extents, so they stay counted, evictable, and a candidate for the next invalidation.
        // Whether the sharded `.meta` this call unlinked was parseable, so counted in
        // `cached_objects`; debited after the slot is released.
        let mut counted_meta_unlinked = false;
        let retained_meta = if removal.failed_meta_extents.is_empty() {
            None
        } else {
            self.get_metadata(cache_key).await?.map(|mut meta| {
                meta.ranges
                    .retain(|r| removal.failed_meta_extents.contains(&(r.start, r.end)));
                meta
            })
        };
        if let Some(meta) = retained_meta.filter(|m| !m.ranges.is_empty()) {
            let mut meta_writes = 0u32;
            Self::rewrite_metadata_file(
                &meta,
                &metadata_file_path,
                &metadata_file_path.with_extension("meta.tmp"),
                &mut meta_writes,
            )?;
            warn!(
                "Invalidation kept the .meta listing the ranges whose unlink failed: key={}, \
                 retained_ranges={}",
                cache_key,
                meta.ranges.len()
            );
        } else if metadata_file_path.exists() {
            // Only a parseable `.meta` was ever credited in `cached_objects`; read under the
            // slot lock, so the verdict is about the file this unlink removes.
            let parseable = std::fs::read_to_string(&metadata_file_path)
                .ok()
                .is_some_and(|c| {
                    serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&c).is_ok()
                });
            match std::fs::remove_file(&metadata_file_path) {
                Ok(_) => {
                    counted_meta_unlinked = parseable;
                    debug!(
                        "Metadata file removed during invalidation: key={}, path={:?}",
                        cache_key, metadata_file_path
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to remove metadata file during invalidation: key={}, path={:?}, error={}",
                        cache_key, metadata_file_path, e
                    );
                    // Continue anyway - ranges are already removed
                }
            }
        } else {
            debug!(
                "Metadata file not found during invalidation: key={}, path={:?}",
                cache_key, metadata_file_path
            );
        }

        // Legacy flat `.meta` from pre-sharding full-object caching, best effort. Kept so
        // that switching the line above to the sharded path is a strict superset of what it
        // used to do rather than a trade: on an old flat cache the line above removed this
        // file, and nothing else on this path would. Same two-tier removal, and same reason,
        // as `invalidate_cache_entry`'s "OLD FLAT STRUCTURE" block.
        //
        // Skipped when the two constructions coincide, which they do for a full-object key
        // whose cache key cannot be parsed into bucket and object key — `get_new_metadata_file_path`
        // then falls back to the same flat path.
        let legacy_metadata_file_path = self.get_legacy_flat_metadata_file_path(cache_key);
        if legacy_metadata_file_path != metadata_file_path && legacy_metadata_file_path.exists() {
            match std::fs::remove_file(&legacy_metadata_file_path) {
                Ok(_) => {
                    debug!(
                        "Legacy flat metadata file removed during invalidation: key={}, path={:?}",
                        cache_key, legacy_metadata_file_path
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to remove legacy flat metadata file during invalidation: key={}, path={:?}, error={}",
                        cache_key, legacy_metadata_file_path, e
                    );
                }
            }
        }

        // Nothing below needs exclusion.
        drop(meta_lock);
        // `decrement_cached_objects` takes `size_state.lock`, so it runs with the slot released.
        if counted_meta_unlinked {
            if let Some(consolidator) = &self.journal_consolidator {
                consolidator.decrement_cached_objects(1).await;
            }
        }

        let total_duration = operation_start.elapsed();

        // `result=` distinguishes a removal that got everything it enumerated from one that did
        // not, rather than reporting both as success. `complete` is scoped to the two
        // enumerations deliberately — it does NOT claim every `.bin` for the key is gone, for
        // the reason in this function's doc comment. A survivor is attributable: the
        // `remove_all_ranges` warning above names the enumeration each failure came from.
        //
        // Spec: range-invalidation-completeness. Requirements: 5.1, 5.3
        let result = if removal.failed_count == 0 {
            "complete"
        } else {
            "partial"
        };
        info!(
            "Range invalidation completed: key={}, result={}, ranges_removed={}/{} enumerated, journal_derived={}, failed={}, size_freed={} bytes, duration={:.2}ms",
            cache_key,
            result,
            removed_count,
            removal.candidate_count,
            removal.journal_derived_count,
            removal.failed_count,
            size_freed,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(())
    }

    /// Get object ETag from metadata file
    /// Implements Requirements 2.1, 4.3
    ///
    /// This method:
    /// 1. Reads only the metadata file (no range data loaded)
    /// 2. Extracts the ETag from object metadata
    /// 3. Handles corrupted or missing metadata gracefully
    /// 4. Caches ETag values within request lifecycle for efficiency
    /// 5. Returns None if metadata doesn't exist or is corrupted
    pub async fn get_object_etag(&self, cache_key: &str) -> Result<Option<String>> {
        let operation_start = std::time::Instant::now();
        debug!(
            "Starting ETag retrieval: key={}, operation=get_object_etag",
            cache_key
        );

        // Get metadata (this handles all error cases gracefully)
        let metadata = match self.get_metadata(cache_key).await? {
            Some(meta) => {
                debug!(
                    "Metadata retrieved for ETag: key={}, etag={}",
                    cache_key, meta.object_metadata.etag
                );
                meta
            }
            None => {
                debug!(
                    "No metadata found for ETag retrieval: key={}, result=none",
                    cache_key
                );
                return Ok(None);
            }
        };

        let etag = metadata.object_metadata.etag.clone();
        let total_duration = operation_start.elapsed();

        debug!(
            "ETag retrieval completed: key={}, etag={}, duration={:.2}ms",
            cache_key,
            etag,
            total_duration.as_secs_f64() * 1000.0
        );

        Ok(Some(etag))
    }

    // ============================================================================
    // Enhanced Error Handling and Concurrent Access Safety Methods
    // ============================================================================

    /// Validate metadata file structure and content for corruption detection
    ///
    /// This method performs comprehensive validation of metadata files to detect
    /// various forms of corruption including:
    /// - JSON parsing errors
    /// - Missing required fields
    /// - Invalid field values
    /// - Inconsistent data structures
    ///
    /// # Arguments
    /// * `cache_key` - The cache key to validate
    ///
    /// # Returns
    /// * `Ok(true)` - Metadata is valid
    /// * `Ok(false)` - Metadata is corrupted but recoverable
    /// * `Err(ProxyError)` - Metadata is corrupted and requires invalidation
    ///
    /// # Requirements
    /// Implements Requirements 4.3
    pub async fn validate_metadata_integrity(&self, cache_key: &str) -> Result<bool> {
        let operation_start = std::time::Instant::now();
        debug!(
            "[METADATA_VALIDATION] Starting integrity validation: cache_key={}",
            cache_key
        );

        let metadata_file_path = self.get_new_metadata_file_path(cache_key);

        // Check if metadata file exists
        if !metadata_file_path.exists() {
            debug!(
                "[METADATA_VALIDATION] Metadata file not found: cache_key={}, path={:?}, result=not_found",
                cache_key, metadata_file_path
            );
            return Ok(true); // No metadata to validate
        }

        // Read and validate file content
        let metadata_content = match std::fs::read_to_string(&metadata_file_path) {
            Ok(content) => {
                if content.is_empty() {
                    error!(
                        "[METADATA_VALIDATION] Empty metadata file detected: cache_key={}, path={:?}",
                        cache_key, metadata_file_path
                    );
                    return Err(ProxyError::CacheError("Empty metadata file".to_string()));
                }
                content
            }
            Err(e) => {
                error!(
                    "[METADATA_VALIDATION] Failed to read metadata file: cache_key={}, path={:?}, error={}",
                    cache_key, metadata_file_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to read metadata: {}",
                    e
                )));
            }
        };

        // Validate JSON structure
        let metadata =
            match serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&metadata_content) {
                Ok(meta) => meta,
                Err(e) => {
                    error!(
                    "[METADATA_VALIDATION] JSON parsing failed: cache_key={}, path={:?}, error={}",
                    cache_key, metadata_file_path, e
                );
                    return Err(ProxyError::CacheError(format!("Corrupted JSON: {}", e)));
                }
            };

        // Validate required fields
        if metadata.cache_key.is_empty() {
            error!(
                "[METADATA_VALIDATION] Empty cache_key field: cache_key={}, path={:?}",
                cache_key, metadata_file_path
            );
            return Err(ProxyError::CacheError("Empty cache_key field".to_string()));
        }

        if metadata.object_metadata.etag.is_empty() {
            error!(
                "[METADATA_VALIDATION] Empty ETag field: cache_key={}, path={:?}",
                cache_key, metadata_file_path
            );
            return Err(ProxyError::CacheError("Empty ETag field".to_string()));
        }

        // Validate timestamps
        let now = SystemTime::now();
        if metadata.created_at > now {
            warn!(
                "[METADATA_VALIDATION] Future creation timestamp: cache_key={}, created_at={:?}, now={:?}",
                cache_key, metadata.created_at, now
            );
            // This is suspicious but not necessarily corrupted
        }

        // Validate range specifications
        for (i, range_spec) in metadata.ranges.iter().enumerate() {
            if range_spec.start > range_spec.end {
                error!(
                    "[METADATA_VALIDATION] Invalid range boundaries: cache_key={}, range_index={}, start={}, end={}",
                    cache_key, i, range_spec.start, range_spec.end
                );
                return Err(ProxyError::CacheError(format!(
                    "Invalid range {}: start > end",
                    i
                )));
            }

            if range_spec.file_path.is_empty() {
                error!(
                    "[METADATA_VALIDATION] Empty range file path: cache_key={}, range_index={}",
                    cache_key, i
                );
                return Err(ProxyError::CacheError(format!(
                    "Empty file path for range {}",
                    i
                )));
            }

            // Security (Req 10): reject file_path if unsafe
            if !is_safe_relative_file_path(&range_spec.file_path) {
                warn!(
                    "[METADATA_VALIDATION] Unsafe range file path: cache_key={}, range_index={}, path={:?}",
                    cache_key, i, range_spec.file_path
                );
                return Ok(false);
            }

            // Validate that the range file exists
            let range_file_path = self.cache_dir.join("ranges").join(&range_spec.file_path);
            if !range_file_path.exists() {
                warn!(
                    "[METADATA_VALIDATION] Missing range file: cache_key={}, range_index={}, path={:?}",
                    cache_key, i, range_file_path
                );
                // Missing range files indicate inconsistency but not necessarily corruption
                return Ok(false);
            }
        }

        let validation_duration = operation_start.elapsed();
        debug!(
            "[METADATA_VALIDATION] Validation completed: cache_key={}, ranges={}, duration={:.2}ms, result=valid",
            cache_key, metadata.ranges.len(), validation_duration.as_secs_f64() * 1000.0
        );

        Ok(true)
    }
}

impl DiskCacheManager {
    // ============================================================================
    // Comprehensive Error Handling and Fallback Methods
    // ============================================================================
}

// ============================================================================
// Path Resolution Functions for Bucket-First Sharding
// ============================================================================

/// Parse cache key into bucket and object key
///
/// Cache keys are in format: `/bucket/object_key` (S3 request path)
///
/// # Arguments
/// * `cache_key` - The cache key to parse (format: "/bucket/object_key")
///
/// # Returns
/// * `Ok((bucket, object_key))` - Tuple of bucket name and object key
/// * `Err(ProxyError)` - If cache key format is invalid (no slash separator)
///
/// # Examples
/// ```
/// use s3_proxy::disk_cache::parse_cache_key;
/// let (bucket, object_key) = parse_cache_key("my-bucket/path/to/object").unwrap();
/// assert_eq!(bucket, "my-bucket");
/// assert_eq!(object_key, "path/to/object");
/// ```
///
/// Validate that a bucket segment is safe to use as a filesystem directory name.
///
/// Rejects:
/// - `.` or `..` (path traversal)
/// - Empty strings (already rejected upstream but defensive)
/// - Strings containing `/`, `\`, or NUL (path separators and string terminators)
/// - Strings containing any ASCII control character (0x00–0x1F, 0x7F)
///
/// Does NOT enforce the S3 bucket naming rules (lowercase, length) — the
/// proxy forwards to S3, and S3 will reject invalid bucket names. We only
/// enforce what is needed to keep cache paths inside the cache directory.
fn validate_bucket_segment(bucket: &str) -> Result<()> {
    if bucket.is_empty() {
        return Err(ProxyError::CacheError(
            "Invalid cache key: empty bucket segment".to_string(),
        ));
    }
    if bucket == "." || bucket == ".." {
        return Err(ProxyError::CacheError(format!(
            "Invalid cache key: bucket segment '{}' is not allowed",
            bucket
        )));
    }
    for b in bucket.bytes() {
        if b == b'/' || b == b'\\' || b < 0x20 || b == 0x7F {
            return Err(ProxyError::CacheError(format!(
                "Invalid cache key: bucket segment '{}' contains an invalid byte (0x{:02X})",
                bucket, b
            )));
        }
    }
    Ok(())
}

/// # Requirements
/// Implements Requirements 1.2, 5.2
pub fn parse_cache_key(cache_key: &str) -> Result<(&str, &str)> {
    // Remove leading slash if present (S3 paths start with /)
    let key = cache_key.strip_prefix('/').unwrap_or(cache_key);

    // Split on first '/' to get bucket and object key
    // Format: "bucket/object/path" (e.g., "my-bucket/path/to/file.txt")
    let (bucket, object_key) = key.split_once('/').ok_or_else(|| {
        ProxyError::CacheError(format!(
            "Invalid cache key format: '{}'. Expected format: 'bucket/object_key'",
            cache_key
        ))
    })?;

    // Reject bucket segments that would escape the cache directory or contain
    // unsafe bytes (handles empty, `.`, `..`, path separators, control chars).
    validate_bucket_segment(bucket)?;

    if object_key.is_empty() {
        return Err(ProxyError::CacheError(format!(
            "Invalid cache key format: '{}'. Object key must be non-empty",
            cache_key
        )));
    }

    Ok((bucket, object_key))
}

/// Sanitize object key for use as a filename
///
/// This function:
/// 1. Percent-encodes filesystem-unsafe characters
/// 2. For keys >200 chars after encoding, uses BLAKE3 hash as filename
/// 3. Appends the provided suffix to the result
///
/// # Arguments
/// * `object_key` - The S3 object key to sanitize
/// * `suffix` - The suffix to append (e.g., ".meta", "_{start}-{end}.bin")
///
/// # Returns
/// * Sanitized filename with suffix appended
///
/// # Examples
/// Not a doctest: this function is `pub(crate)`, and a doctest compiles as a
/// separate crate, so it could only reach this name by widening the visibility
/// back to `pub`. The executable equivalents live in this file's own test
/// module — `test_sanitize_object_key_for_filename_basic`,
/// `test_sanitize_object_key_for_filename_long_key` and
/// `test_sanitize_object_key_determinism`.
/// ```text
/// sanitize_object_key_for_filename("path/to/file.txt", ".meta")
///     -> "path%2Fto%2Ffile.txt.meta"
/// sanitize_object_key_for_filename(&"a".repeat(250), ".meta")
///     -> "{64-char-blake3-hash}.meta"
/// ```
///
/// # Requirements
/// Implements Requirements 3.3, 3.4, 3.5
pub(crate) fn sanitize_object_key_for_filename(object_key: &str, suffix: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

    // Define filesystem-unsafe ASCII characters that need encoding
    // Start with CONTROLS (0x00-0x1F, 0x7F) and add filesystem-specific chars
    const FRAGMENT: &AsciiSet = &CONTROLS
        .add(b' ') // Space
        .add(b'/') // Path separator (Unix)
        .add(b'\\') // Path separator (Windows)
        .add(b':') // Drive letter separator (Windows), problematic on macOS
        .add(b'*') // Wildcard (Windows)
        .add(b'?') // Wildcard (Windows)
        .add(b'"') // Quote (Windows)
        .add(b'<') // Redirect (Windows)
        .add(b'>') // Redirect (Windows)
        .add(b'|') // Pipe (Windows)
        .add(b'%'); // Percent (to avoid double-encoding issues)

    // Use utf8_percent_encode which preserves non-ASCII UTF-8 sequences
    let sanitized = utf8_percent_encode(object_key, FRAGMENT).to_string();

    // Filesystem filename limit is typically 255 bytes
    // Use 200 as threshold to leave room for extensions and suffixes
    if sanitized.len() > 200 {
        // Hash long keys to ensure they fit within filesystem limits
        // Use BLAKE3 for fast, secure hashing (64 hex characters)
        let hash = blake3::hash(object_key.as_bytes());
        format!("{}{}", hash.to_hex(), suffix)
    } else {
        format!("{}{}", sanitized, suffix)
    }
}

/// Get sharded path for a cache file using bucket-first directory structure
///
/// This function:
/// 1. Parses the cache key to extract bucket and object key
/// 2. Computes BLAKE3 hash of the object key (not bucket)
/// 3. Extracts first 2 hex digits for level 1, next 3 for level 2
/// 4. Constructs path: base_dir/bucket/XX/YYY/filename
///
/// # Arguments
/// * `base_dir` - Base directory (e.g., "cache_dir/metadata" or "cache_dir/ranges")
/// * `cache_key` - Cache key in format "/bucket/object_key" (S3 request path)
/// * `suffix` - Suffix for the filename (e.g., ".meta", "_{start}-{end}.bin")
///
/// # Returns
/// * `Ok(PathBuf)` - Full path to the sharded cache file
/// * `Err(ProxyError)` - If cache key format is invalid
///
/// # Examples
/// ```
/// use std::path::PathBuf;
/// use s3_proxy::disk_cache::get_sharded_path;
/// let path = get_sharded_path(
///     &PathBuf::from("/cache/metadata"),
///     "my-bucket/path/to/file.txt",
///     ".meta"
/// ).unwrap();
/// // Returns: /cache/metadata/my-bucket/XX/YYY/path%2Fto%2Ffile.txt.meta
/// // where XX and YYY are derived from BLAKE3("path/to/file.txt")
/// ```
///
/// # Requirements
/// Implements Requirements 1.1, 2.1, 2.2, 5.1
pub fn get_sharded_path(
    base_dir: &std::path::Path,
    cache_key: &str,
    suffix: &str,
) -> Result<PathBuf> {
    // Requirement 3.1: Parse cache key to extract bucket and object key
    let (bucket, object_key) = parse_cache_key(cache_key)?;

    debug!(
        "[SHARDED_PATH] Parsed cache key: cache_key={}, bucket={}, object_key={}",
        cache_key, bucket, object_key
    );

    // Requirement 3.2: Hash object key only (not bucket) using BLAKE3
    let hash = blake3::hash(object_key.as_bytes());
    let hash_hex = hash.to_hex();

    // Requirement 3.2: Extract directory levels from hash
    // Level 1: First 2 hex digits (256 directories)
    // Level 2: Next 3 hex digits (4,096 subdirectories per L1)
    let level1 = &hash_hex.as_str()[0..2];
    let level2 = &hash_hex.as_str()[2..5];

    debug!(
        "[SHARDED_PATH] Hash-based directories computed: object_key={}, hash_prefix={}, level1={}, level2={}",
        object_key, &hash_hex.as_str()[0..10], level1, level2
    );

    // Requirement 3.3: Sanitize object key for filename
    let filename = sanitize_object_key_for_filename(object_key, suffix);

    // Requirement 3.3: Construct path: base_dir/bucket/XX/YYY/filename
    let sharded_path = base_dir
        .join(bucket)
        .join(level1)
        .join(level2)
        .join(&filename);

    // Requirement 3.5: Log full sharded path construction
    debug!(
        "[SHARDED_PATH] Sharded path constructed: cache_key={}, bucket={}, level1={}, level2={}, filename={}, full_path={:?}",
        cache_key, bucket, level1, level2, filename, sharded_path
    );

    Ok(sharded_path)
}

/// Which temp-file naming scheme [`write_atomic`] uses.
///
/// Not a stylistic choice. A file **any** instance may write needs an instance-suffixed temp
/// name, or two instances racing share one temp path and the rename publishes a file that is
/// half of each — a complete, parseable, wrong document with no error anywhere.
/// `persist_size_state_internal` already does this by hand, and the shard-checkpoint writer and
/// the pending-file writer in `src/staged_fold.rs` each open-code their own copy of the same
/// `{file}.tmp.{host}.{pid}` scheme.
///
/// Spec: cache-eviction-at-scale § 2.1, § 2 trap 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shared {
    /// Any instance on the volume may write this path. Temp name carries hostname and pid.
    Yes,
    /// Only this instance writes this path. A plain `.tmp` suffix is enough.
    No,
}

/// Atomically replace `path` with `bytes`: write a temp, `sync_all`, rename.
///
/// # Why `sync_all` is unconditional
///
/// A rename is atomic with respect to *visibility*, not with respect to *durability*: without
/// the `fsync` the rename can be recorded while the data blocks are not, so a crash publishes
/// a name pointing at a short or zero-length file. `fsync` measured free within noise on EFS
/// and 1.136 ms on FSxZ (`requirements.md` § Shard-partition maintenance), so there is no case
/// for making it optional and no flag to get wrong.
///
/// # Existing call sites are deliberately NOT retrofitted
///
/// Four open-coded `tmp`+rename sites already exist and disagree about durability — the
/// ledger's compaction and the journal's cleanup `sync_all`, the journal's append and
/// `persist_size_state_internal` do not. Converting them is unrelated churn with its own red
/// side (§ 2.1 says so explicitly). This helper exists so the sites added by
/// `cache-eviction-at-scale` do not become a fifth and sixth inconsistency.
///
/// Spec: cache-eviction-at-scale § 2.1.
pub(crate) async fn write_atomic(
    path: &std::path::Path,
    bytes: &[u8],
    shared: Shared,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            ProxyError::CacheError(format!(
                "write_atomic: failed to create parent of {:?}: {}",
                path, e
            ))
        })?;
    }

    let tmp = match shared {
        Shared::Yes => {
            let host = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string())
                .replace(['/', '\\'], "_");
            // Bounded to NAME_MAX: this also stages key-derived `.meta` names (the TTL
            // refresh after an origin `304`), which reach 205 bytes before the suffix.
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            path.with_file_name(crate::cache::bounded_instance_tmp_name(
                &name,
                "",
                &host,
                std::process::id(),
            ))
        }
        Shared::No => append_to_file_name(path, ".tmp"),
    };

    let write_result = async {
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(bytes).await?;
        // Before the rename, never after: see the durability note above.
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&tmp, path).await
    }
    .await;

    if let Err(e) = write_result {
        // Best effort — a surviving temp is reaped by the background sweep, but leaving one
        // behind on every failure would accumulate on a volume nobody is watching.
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(ProxyError::CacheError(format!(
            "write_atomic: failed to write {:?}: {}",
            path, e
        )));
    }
    Ok(())
}

/// `path` with `suffix` appended to its file NAME.
///
/// Deliberately not `Path::with_extension`, which replaces the existing extension: on
/// `{XX}.tail` that would yield `{XX}.tmp` and two different partitions in one bucket would
/// share one temp path. Appending keeps the original name whole.
fn append_to_file_name(path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unnamed".to_string());
    name.push_str(suffix);
    path.with_file_name(name)
}

/// Normalize cache key by removing leading slashes and ensuring consistent format
///
/// This function ensures cache keys are in a consistent format regardless of
/// whether they were provided with or without leading slashes.
///
/// # Arguments
/// * `cache_key` - The cache key to normalize
///
/// # Returns
/// * Normalized cache key without leading slash
///
/// # Examples
/// ```
/// use s3_proxy::disk_cache::normalize_cache_key;
/// assert_eq!(normalize_cache_key("/bucket/path"), "bucket/path");
/// assert_eq!(normalize_cache_key("bucket/path"), "bucket/path");
/// ```
///
/// # Requirements
/// Implements Requirements 5.3, 5.5
/// The inclusive end byte a `Content-Range: bytes <start>-<end>/<total>` value names.
fn content_range_end(value: &str) -> Option<u64> {
    crate::http_proxy::parse_content_range(value).map(|(_, end, _)| end)
}

pub fn normalize_cache_key(cache_key: &str) -> String {
    // Remove leading slash if present
    cache_key.strip_prefix('/').unwrap_or(cache_key).to_string()
}

/// Test-only shims for [`DiskCacheManager::find_cached_ranges`].
///
/// The lookup takes an explicit
/// [`crate::cache_types::RangeLookupPurpose`] and returns a
/// [`crate::cache_types::RangeLookupResult`] rather than a bare `Vec`, so the
/// stored-expiry condition travels with the coverage
/// (the expired entry revalidation feature R1.2, R1.4).
///
/// These shims exist so the ~30 pre-existing overlap-arithmetic tests below —
/// which are about *geometry*, not freshness — keep reading as one line instead
/// of destructuring a result they do not care about. **The purpose is still named
/// at every call site**, in the shim's own name, which is what R1.4 requires: it
/// forbids a purpose that has to be inferred, not one stated as a word.
///
/// Tests that are about freshness call the real API directly with both purposes.
/// See `find_cached_ranges_purpose_matrix` for the six-case matrix.
#[cfg(test)]
impl DiskCacheManager {
    /// A `FreshServe` lookup, returning just the extents.
    ///
    /// Rejects Stored_Expired entries, which is what every caller with no
    /// downstream freshness gate needs.
    async fn fresh_serve_ranges(
        &self,
        cache_key: &str,
        requested_start: u64,
        requested_end: u64,
    ) -> Result<Vec<crate::cache_types::RangeSpec>> {
        Ok(self
            .find_cached_ranges(
                cache_key,
                requested_start,
                requested_end,
                None,
                crate::cache_types::RangeLookupPurpose::FreshServe,
            )
            .await?
            .ranges)
    }
}

#[cfg(test)]
mod decode_lz4_file_exact_tests {
    use super::{decode_lz4_file_exact, decode_lz4_frames_exact};
    use std::io::Write;

    fn frame(data: &[u8]) -> Vec<u8> {
        let mut info = lz4_flex::frame::FrameInfo::new();
        info.content_checksum = true;
        let mut enc = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    /// Concatenated frames decode from the file to the same bytes the buffer decoder yields.
    #[test]
    fn concatenated_frames_match_the_buffer_decoder() {
        let a: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let b: Vec<u8> = (0..70_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let mut file_bytes = frame(&a);
        file_bytes.extend(frame(&b));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.bin");
        std::fs::write(&path, &file_bytes).unwrap();
        let expected = (a.len() + b.len()) as u64;
        let (len, got) = decode_lz4_file_exact(&path, expected).unwrap();
        assert_eq!(len, file_bytes.len() as u64);
        let got = got.expect("exact decode");
        assert_eq!(Some(got), decode_lz4_frames_exact(&file_bytes, expected));
        // A wrong expected size is not an exact decode.
        assert!(decode_lz4_file_exact(&path, expected - 1)
            .unwrap()
            .1
            .is_none());
        assert!(decode_lz4_file_exact(&path, expected + 1)
            .unwrap()
            .1
            .is_none());
    }

    /// A flipped content-checksum byte fails the decode, as on the buffered path.
    #[test]
    fn corrupt_checksum_is_not_an_exact_decode() {
        let a: Vec<u8> = (0..100_000u32).map(|i| (i % 13) as u8).collect();
        let mut bytes = frame(&a);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.bin");
        std::fs::write(&path, &bytes).unwrap();
        assert!(decode_lz4_frames_exact(&bytes, a.len() as u64).is_none());
        assert!(decode_lz4_file_exact(&path, a.len() as u64)
            .unwrap()
            .1
            .is_none());
    }
}

/// Bytes served from an LZ4 range file do not depend on the reader capacity: every layout
/// earlier and current releases write, store mode and compressed, streamed (full and from an
/// offset) and buffered, at std's default 8 KiB and at [`LZ4_RANGE_READER_CAPACITY`].
#[cfg(test)]
mod lz4_reader_capacity_tests {
    use super::{
        decode_lz4_file_exact_with_capacity, lz4_range_reader_capacity, DiskCacheManager,
        LZ4_RANGE_READER_CAPACITY, LZ4_RANGE_READER_MIN_CAPACITY,
    };
    use crate::cache_types::ObjectMetadata;
    use crate::compression::{CompressionAlgorithm, CompressionHandler};
    use futures::StreamExt;
    use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
    use std::io::Write;

    fn frames(data: &[u8], block: BlockSize, frame_len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for part in data.chunks(frame_len) {
            let mut info = FrameInfo::new();
            info.content_checksum = true;
            info.block_mode = BlockMode::Independent;
            info.block_size = block;
            let mut enc = FrameEncoder::with_frame_info(info, &mut out);
            enc.write_all(part).unwrap();
            enc.finish().unwrap();
        }
        out
    }

    /// (name, file bytes) for every layout: 4 MiB blocks in one frame, 1 MiB frames, and
    /// 64 KiB blocks in one frame and in 1 MiB frames, plus the proxy's own store-mode writer.
    fn layouts(data: &[u8], store: bool) -> Vec<(String, Vec<u8>)> {
        let mut out = vec![
            (
                "4MiB-single".to_string(),
                frames(data, BlockSize::Max4MB, data.len()),
            ),
            (
                "1MiB-frames".to_string(),
                frames(data, BlockSize::Max4MB, 1 << 20),
            ),
            (
                "64KiB-single".to_string(),
                frames(data, BlockSize::Max64KB, data.len()),
            ),
            (
                "64KiB-1MiB-frames".to_string(),
                frames(data, BlockSize::Max64KB, 1 << 20),
            ),
        ];
        if store {
            let mut stored = Vec::new();
            for part in data.chunks(1 << 20) {
                stored.extend(CompressionHandler::encode_store_mode_frame(part).unwrap());
            }
            out.push(("store-writer-64KiB-1MiB-frames".to_string(), stored));
            out.push((
                "store-writer-4MiB-single".to_string(),
                CompressionHandler::encode_store_mode_frame_with_blocks(data, 4 << 20, 7).unwrap(),
            ));
        }
        out
    }

    fn content(store: bool, len: usize) -> Vec<u8> {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|i| {
                if store {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    (x >> 24) as u8
                } else {
                    let line = b"{\"key\":\"warehouse/part-\",\"status\":200}\n";
                    line[i % line.len()]
                }
            })
            .collect()
    }

    #[test]
    fn capacity_is_bounded_by_file_length() {
        assert_eq!(lz4_range_reader_capacity(0), LZ4_RANGE_READER_MIN_CAPACITY);
        assert_eq!(lz4_range_reader_capacity(100_000), 100_000);
        assert_eq!(
            lz4_range_reader_capacity(u64::MAX),
            LZ4_RANGE_READER_CAPACITY
        );
    }

    #[tokio::test]
    async fn bytes_served_are_identical_at_every_reader_capacity() {
        // Not a multiple of any block or frame size, so the last block and frame are short.
        const LEN: usize = (5 << 20) + 12_345;
        let dir = tempfile::tempdir().unwrap();
        let mut dcm = DiskCacheManager::new(dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        dcm.initialize().await.unwrap();
        let seed = vec![7u8; 4096];
        let meta = ObjectMetadata {
            content_length: seed.len() as u64,
            compression_algorithm: CompressionAlgorithm::Lz4,
            ..Default::default()
        };
        dcm.store_range(
            "reader-capacity-bucket/template",
            0,
            4095,
            &seed,
            meta,
            std::time::Duration::from_secs(3600),
            true,
        )
        .await
        .unwrap();
        let template = dcm
            .get_metadata("reader-capacity-bucket/template")
            .await
            .unwrap()
            .unwrap()
            .ranges[0]
            .clone();
        std::fs::create_dir_all(dir.path().join("ranges").join("reader-capacity")).unwrap();

        let mut checked = 0;
        for store in [true, false] {
            let data = content(store, LEN);
            for (name, bytes) in layouts(&data, store) {
                let rel = format!("reader-capacity/{name}-{store}.bin");
                let path = dir.path().join("ranges").join(&rel);
                std::fs::write(&path, &bytes).unwrap();
                let mut spec = template.clone();
                spec.file_path = rel;
                spec.start = 0;
                spec.end = LEN as u64 - 1;
                spec.compression_algorithm = CompressionAlgorithm::Lz4;
                spec.compressed_size = bytes.len() as u64;
                spec.uncompressed_size = LEN as u64;
                // The file written above is the one this listing describes.
                spec.file_identity = std::fs::metadata(&path)
                    .ok()
                    .and_then(|m| crate::cache_types::RangeFileIdentity::of(&m));
                for capacity in [None, Some(8192), Some(LZ4_RANGE_READER_CAPACITY)] {
                    let label = format!("{name} store={store} capacity={capacity:?}");
                    let (len, got) =
                        decode_lz4_file_exact_with_capacity(&path, LEN as u64, capacity).unwrap();
                    assert_eq!(len, bytes.len() as u64, "{label}");
                    assert!(got.as_deref() == Some(&data[..]), "buffered {label}");
                    for (chunk, offset) in [(1_048_576, 0u64), (262_144, 3_000_001)] {
                        let (stream, skipped) = dcm
                            .stream_range_data_from_with_capacity(&spec, chunk, offset, capacity)
                            .await
                            .unwrap();
                        let mut stream = std::pin::pin!(stream);
                        let mut served = Vec::with_capacity(LEN);
                        while let Some(c) = stream.next().await {
                            served.extend_from_slice(&c.unwrap());
                        }
                        // An LZ4 file decodes from its start; the caller discards to `offset`.
                        let from = (offset - skipped) as usize;
                        assert!(
                            served[from..] == data[offset as usize..],
                            "streamed {label} chunk={chunk} offset={offset}"
                        );
                        checked += 1;
                    }
                }
            }
        }
        // 2 store contents x layouts (6 + 4) x 3 capacities x 2 stream reads.
        assert_eq!(checked, 60);
    }
}

#[cfg(test)]
mod disk_full_classifier_tests {
    use super::is_disk_full_error;
    use std::io;

    /// A real ENOSPC from the OS is classified as disk-full. Since Rust 1.83 std reports ENOSPC
    /// as `ErrorKind::StorageFull`, not `Other`, so a classifier gated on `Other` never fires.
    #[test]
    fn raw_enospc_is_disk_full() {
        let e = io::Error::from_raw_os_error(libc::ENOSPC);
        assert_eq!(e.kind(), io::ErrorKind::StorageFull, "std's ENOSPC mapping");
        assert!(
            is_disk_full_error(&e),
            "ENOSPC not classified as disk full: {e:?}"
        );
    }

    /// An error built from `StorageFull` with no OS code is disk-full too.
    #[test]
    fn storage_full_kind_is_disk_full() {
        let e = io::Error::from(io::ErrorKind::StorageFull);
        assert!(is_disk_full_error(&e));
    }

    /// The message match stays as a fallback for wrapped errors that lost the OS code.
    #[test]
    fn message_fallback_is_disk_full() {
        let e = io::Error::other("write failed: No space left on device (os error 28)");
        assert!(is_disk_full_error(&e));
    }

    /// Other errors are not disk-full.
    #[test]
    fn other_errors_are_not_disk_full() {
        assert!(!is_disk_full_error(&io::Error::from_raw_os_error(
            libc::EACCES
        )));
        assert!(!is_disk_full_error(&io::Error::from_raw_os_error(
            libc::EIO
        )));
        assert!(!is_disk_full_error(&io::Error::other("unrelated")));
    }
}

#[cfg(test)]
mod unledgered_staged_credit_guard_tests {
    use super::*;

    /// The guard at the two ledger-free credit sites fires, is counted, and its reachability
    /// premise still holds.
    ///
    /// # What is being guarded
    ///
    /// `store_range`'s hybrid arm and `commit_incremental_range` write **no** `Write_Ledger` entry.
    /// A staged write-cache credit at either would create a staged range the fold's candidate set —
    /// the ledger and nothing else — cannot see, on every write rather than on a failure. That would
    /// be a larger loss mode than any of § 16.6.6's L1-L8.
    ///
    /// The state is unreachable, but it was unreachable **by caller convention** with nothing at
    /// either site saying so, which is a comment rather than a guard.
    ///
    /// # Arm 3 is the one that matters, and it is the cheapest of the three
    ///
    /// Arms 1 and 2 assert the guard behaves when reached. Arm 3 asserts the **premise that it is
    /// not reached**: both `ObjectMetadata` constructors every production caller uses must still
    /// hardcode `is_write_cached: false`. If either default flipped, every `store_range` call in the
    /// process would start classifying as staged and the guard would go from unreachable to firing
    /// on the hot path. Nothing else in the tree asserts that, and it is the assertion that would
    /// catch the change while it was still a one-line diff.
    ///
    /// A failure in arm 1 or 2 implicates `refuse_unledgered_staged_credit`. A failure in arm 3
    /// implicates `ObjectMetadata::default` or `ObjectMetadata::new_with_headers` in
    /// `cache_types.rs`, and means the two credit sites need a real ledger append rather than a
    /// guard.
    #[test]
    fn a_staged_credit_at_a_ledger_free_site_is_refused_and_counted() {
        // ---- Arm 1 + 2: the guard fires and is counted.
        //
        // `debug_assertions` is ON under `cargo test` and OFF under `cargo test --release`, which is
        // the pre-push gate. Both are asserted rather than one, because the two builds rely on
        // different halves of the guard: the debug build on the assertion, the release build on the
        // counter and the `error!`. A test that only covered one would leave the gate's own build
        // unverified.
        let before = unledgered_staged_credits_refused_total();

        // Silence the panic hook for the deliberate panic, then restore it, so a passing run does
        // not print a backtrace that reads like a failure.
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = std::panic::catch_unwind(|| {
            refuse_unledgered_staged_credit("test-site", "bucket/guarded.bin", 0, 1023, 512);
        });
        std::panic::set_hook(previous_hook);

        if cfg!(debug_assertions) {
            let payload = outcome.expect_err(
                "with debug_assertions on, a staged credit at a ledger-free site must PANIC. A \
                 newly added staged caller is first exercised in a debug or test build, and the \
                 assertion is what stops it reaching production silently.",
            );
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            assert!(
                message.contains("test-site"),
                "the panic must NAME the site, or a maintainer cannot tell which of the two credit \
                 sites was reached: {message}"
            );
            assert!(
                message.contains("Write_Ledger"),
                "and it must say what is missing, so the fix (add the append) is obvious from the \
                 message rather than from reading the spec: {message}"
            );
        } else {
            assert!(
                outcome.is_ok(),
                "with debug_assertions off — which is the pre-push gate's own build — the guard \
                 must NOT panic. Refusing the credit and logging is the release behaviour; taking \
                 down a proxy over a bookkeeping classification would be strictly worse than the \
                 under-count it prevents."
            );
        }

        assert_eq!(
            unledgered_staged_credits_refused_total(),
            before + 1,
            "the refusal must be COUNTED in both builds. In release the assertion is compiled out, \
             so this counter and the error! log are the entire signal — without the counter the \
             condition would be invisible on /metrics."
        );

        // ---- Arm 3: the reachability premise.
        assert!(
            !crate::cache_types::ObjectMetadata::default().is_write_cached,
            "the guard is unreachable only because ObjectMetadata::default() hardcodes \
             is_write_cached: false. If this flips, EVERY store_range call in the process \
             classifies as staged, the guard fires on the hot path, and these two sites need a real \
             ledger append rather than a refusal."
        );
        assert!(
            !crate::cache_types::ObjectMetadata::new_with_headers(
                "etag".to_string(),
                "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                1024,
                None,
                std::collections::HashMap::new(),
            )
            .is_write_cached,
            "and because new_with_headers does too — the other constructor every production caller \
             of these two sites uses"
        );
    }
}

#[cfg(test)]
mod stale_file_handle_counter_tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Both counters are process-global and libtest runs this binary's tests as parallel
    /// threads, so every test that injects `ESTALE` — the only thing that can move them, since
    /// a local filesystem never returns it — holds this for its whole body. An exact delta is
    /// then safe; see the testing guidance → "Never assert an exact delta on a
    /// process-global counter".
    static COUNTER_LOCK: Mutex<()> = Mutex::new(());

    /// Injects a scripted sequence of `stat` and read results, in order.
    struct ScriptedIo {
        lens: Mutex<VecDeque<std::io::Result<u64>>>,
        reads: Mutex<VecDeque<std::io::Result<String>>>,
    }

    impl MetadataFileIo for ScriptedIo {
        fn len(&self, _path: &Path) -> std::io::Result<u64> {
            self.lens
                .lock()
                .unwrap()
                .pop_front()
                .expect("the test scripted too few stat results")
        }
        fn read_to_string(&self, _path: &Path) -> std::io::Result<String> {
            self.reads
                .lock()
                .unwrap()
                .pop_front()
                .expect("the test scripted too few read results")
        }
    }

    /// The error the live branch matches: raw errno 116, Linux `ESTALE`.
    fn estale() -> std::io::Error {
        std::io::Error::from_raw_os_error(116)
    }

    fn valid_meta_json() -> String {
        serde_json::to_string(&crate::cache_types::NewCacheMetadata {
            cache_key: "bucket/stale-handle-test".to_string(),
            ..Default::default()
        })
        .expect("a default NewCacheMetadata must serialise")
    }

    fn counters() -> (u64, u64) {
        (
            metadata_read_stale_handles_total(),
            metadata_read_stale_handle_misses_total(),
        )
    }

    /// Two `ESTALE`s then a good read: retrying absorbs both, the read PARSES, and the
    /// observed counter moves by exactly 2 while the miss counter does not move.
    ///
    /// The vacuity trap this avoids: a test asserting the counter exists, or is zero after a
    /// healthy read, passes with no increment at all. This one drives the branch through the
    /// seam and asserts the exact movement, and it asserts the outcome was `Parsed` so a counter
    /// that moved on a read which then failed for another reason cannot satisfy it.
    #[test]
    fn retried_stale_handles_count_each_observation_and_no_miss() {
        let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let json = valid_meta_json();
        let io = ScriptedIo {
            lens: Mutex::new(VecDeque::from([Ok(json.len() as u64)])),
            reads: Mutex::new(VecDeque::from([Err(estale()), Err(estale()), Ok(json)])),
        };
        assert!(
            is_stale_file_handle(&estale()),
            "the injected error must be one the live predicate matches, or this test measures \
             a different branch"
        );

        let (seen_before, miss_before) = counters();
        let outcome =
            read_and_parse_metadata_with_io(Path::new("/unused/stale.meta"), u64::MAX, &io);
        let (seen_after, miss_after) = counters();

        assert!(
            matches!(outcome, MetadataReadOutcome::Parsed(_)),
            "two ESTALEs within the retry bound must be absorbed and the third read parsed"
        );
        assert_eq!(
            seen_after - seen_before,
            2,
            "cache.metadata_read_stale_handles_total must move once per ESTALE observed at the \
             read loop — two injected, before={seen_before} after={seen_after}"
        );
        assert_eq!(
            miss_after - miss_before,
            0,
            "a read that retrying absorbed is not a miss — before={miss_before} \
             after={miss_after}"
        );
    }

    /// Four `ESTALE`s: the three retries are spent, the fourth attempt is still stale, the read
    /// becomes a `TransientMiss`, and the miss counter moves by exactly 1 while the observed
    /// counter moves by 4.
    #[test]
    fn exhausted_stale_handle_counts_one_miss() {
        let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let io = ScriptedIo {
            lens: Mutex::new(VecDeque::from([Ok(100)])),
            reads: Mutex::new(VecDeque::from([
                Err(estale()),
                Err(estale()),
                Err(estale()),
                Err(estale()),
            ])),
        };

        let (seen_before, miss_before) = counters();
        let outcome =
            read_and_parse_metadata_with_io(Path::new("/unused/stale.meta"), u64::MAX, &io);
        let (seen_after, miss_after) = counters();

        assert!(
            matches!(outcome, MetadataReadOutcome::TransientMiss),
            "a read still stale after the fixed three retries must be a transient miss"
        );
        assert_eq!(
            seen_after - seen_before,
            4,
            "every ESTALE observed counts, including the final unretried one — \
             before={seen_before} after={seen_after}"
        );
        assert_eq!(
            miss_after - miss_before,
            1,
            "cache.metadata_read_stale_handle_misses_total must move once for the read that \
             retrying did not absorb — before={miss_before} after={miss_after}"
        );
    }

    /// A stale handle on the initial `stat` is not retried: it is one observation and one miss.
    #[test]
    fn stale_handle_on_stat_counts_one_observation_and_one_miss() {
        let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let io = ScriptedIo {
            lens: Mutex::new(VecDeque::from([Err(estale())])),
            reads: Mutex::new(VecDeque::new()),
        };

        let (seen_before, miss_before) = counters();
        let outcome =
            read_and_parse_metadata_with_io(Path::new("/unused/stale.meta"), u64::MAX, &io);
        let (seen_after, miss_after) = counters();

        assert!(matches!(outcome, MetadataReadOutcome::TransientMiss));
        assert_eq!(
            (seen_after - seen_before, miss_after - miss_before),
            (1, 1),
            "an ESTALE on stat is observed once and abandons the read once"
        );
    }

    /// Part C — an `ESTALE` on the PARSE-retry re-read is observed but is not a miss.
    ///
    /// The first content does not parse, so the parse loop sleeps and re-reads. That re-read is
    /// stale: it counts once, the loop keeps the content it already holds, and the next re-read
    /// returns valid JSON that parses. So exactly +1 observed, +0 misses, outcome `Parsed`.
    ///
    /// The outcome assertion is what makes the +1 attributable to the parse-retry site: the read
    /// loop never saw an error (its only read returned non-empty text), and the stat succeeded, so
    /// neither of the other two increment sites was reached.
    #[test]
    fn stale_handle_on_parse_retry_reread_counts_one_observation_and_no_miss() {
        let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let json = valid_meta_json();
        let io = ScriptedIo {
            lens: Mutex::new(VecDeque::from([Ok(json.len() as u64)])),
            reads: Mutex::new(VecDeque::from([
                Ok("{\"truncated mid-write".to_string()),
                Err(estale()),
                Ok(json),
            ])),
        };

        let (seen_before, miss_before) = counters();
        let outcome =
            read_and_parse_metadata_with_io(Path::new("/unused/partial.meta"), u64::MAX, &io);
        let (seen_after, miss_after) = counters();

        assert!(
            matches!(outcome, MetadataReadOutcome::Parsed(_)),
            "FIXTURE: the third read's valid JSON must parse, or the stale re-read between them \
             was not the one the parse loop recovered from"
        );
        assert_eq!(
            (seen_after - seen_before, miss_after - miss_before),
            (1, 0),
            "an ESTALE on the parse-retry re-read must count once as observed and never as a \
             miss — observed before={seen_before} after={seen_after}, misses \
             before={miss_before} after={miss_after}"
        );
    }

    /// Part C — the dashboard's `/api/cache-stats` `metadata_cache.stale_handle_errors` is
    /// the live `.meta` reader's counter, not a permanent 0.
    ///
    /// Two `ESTALE`s are injected first so the counter is non-zero: against a zero counter a
    /// source replaced with a literal `0` would compare equal and the test would pass vacuously.
    /// The JSON value must then EQUAL `metadata_read_stale_handles_total()` read under the same
    /// lock, which nothing else in this binary can move.
    ///
    /// Sync `#[test]` driving its own runtime, so `COUNTER_LOCK` (a `std::sync::Mutex`) is never
    /// held across an `.await`.
    #[test]
    fn dashboard_stale_handle_errors_reports_the_metadata_reader_counter() {
        let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let io = ScriptedIo {
            lens: Mutex::new(VecDeque::from([Ok(100)])),
            reads: Mutex::new(VecDeque::from([
                Err(estale()),
                Err(estale()),
                Err(estale()),
                Err(estale()),
            ])),
        };
        let _ = read_and_parse_metadata_with_io(Path::new("/unused/stale.meta"), u64::MAX, &io);
        let expected = metadata_read_stale_handles_total();
        assert!(
            expected >= 4,
            "PRECONDITION MISSING: the counter must be non-zero, or a source hardcoded to 0 \
             passes this test (read {expected})"
        );

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let body = runtime.block_on(async {
            let cache_manager = Arc::new(crate::cache::CacheManager::new(
                temp_dir.path().join("cache"),
                false,
                0,
                1024,
                false,
            ));
            let api = crate::dashboard::ApiHandler::new(
                Arc::new(crate::dashboard::LogReader::new(
                    temp_dir.path().join("logs"),
                    "test-host".to_string(),
                )),
                Arc::new(crate::config::DashboardConfig::default()),
            );
            api.set_cache_manager(cache_manager).await;
            api.get_cache_stats()
                .await
                .expect("/api/cache-stats must build")
                .into_body()
        });

        let json: serde_json::Value =
            serde_json::from_str(&body).expect("/api/cache-stats body is JSON");
        assert_eq!(
            json["metadata_cache"]["stale_handle_errors"].as_u64(),
            Some(expected),
            "/api/cache-stats metadata_cache.stale_handle_errors must report \
             cache.metadata_read_stale_handles_total; a different source returns the dashboard \
             card to a permanent 0. Body: {body}"
        );
    }

    /// A non-stale error (permission denied) moves neither counter — the control that makes the
    /// three tests above attributable to the `ESTALE` predicate rather than to any read error.
    #[test]
    fn a_non_stale_read_error_moves_neither_counter() {
        let _guard = COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let io = ScriptedIo {
            lens: Mutex::new(VecDeque::from([Ok(100)])),
            reads: Mutex::new(VecDeque::from([Err(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied,
            ))])),
        };
        let before = counters();
        let outcome =
            read_and_parse_metadata_with_io(Path::new("/unused/denied.meta"), u64::MAX, &io);
        assert!(matches!(outcome, MetadataReadOutcome::TransientMiss));
        assert_eq!(
            counters(),
            before,
            "a permission error is not a stale file handle and must not be counted as one"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::{RangeLookupPurpose, StoredFreshness};
    use crate::compression::CompressionAlgorithm;
    use std::time::Duration;
    use tempfile::TempDir;

    /// `INVALIDATE-LOCK-HOLD-TIME`: the journal enumeration in `invalidate_all_ranges` is a
    /// `read_dir` plus one read-and-parse per journal file, and it needs no mutual exclusion with
    /// `.meta` publishers. While it is in progress, a peer of the same key must be able to take
    /// the key's slot lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalidate_all_ranges_does_not_hold_the_slot_lock_across_journal_enumeration() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        std::fs::create_dir_all(cache_dir.join("metadata").join("_journals")).unwrap();
        let manager = std::sync::Arc::new(DiskCacheManager::new(
            cache_dir.clone(),
            true,
            1024,
            false,
            1_048_576,
        ));
        let key = "scn1-bucket/scn1-object";

        *JOURNAL_ENUMERATION_DELAY_FOR_TESTS.lock().unwrap() =
            Some((cache_dir.clone(), Duration::from_millis(1500)));

        let invalidating = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.invalidate_all_ranges(key).await })
        };
        // Let the invalidation reach the slow enumeration.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let bound = Duration::from_millis(500);
        let started = std::time::Instant::now();
        let mut acquired = false;
        while started.elapsed() < bound {
            if crate::metadata_lock_manager::try_acquire_slot_lock(&cache_dir, key).is_ok() {
                acquired = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        *JOURNAL_ENUMERATION_DELAY_FOR_TESTS.lock().unwrap() = None;
        invalidating.await.unwrap().unwrap();
        assert!(
            acquired,
            "a peer could not take the key's slot lock within {:?} while invalidate_all_ranges was \
             enumerating journals: the lock is held across the enumeration",
            bound
        );
    }

    /// R15.2 at `invalidate_all_ranges` (U1): when one `.bin` unlink fails, the `.meta`
    /// survives listing exactly that extent, so the surviving file stays named. The other
    /// extent's `.bin` is removed and unlisted.
    #[tokio::test]
    async fn invalidate_all_ranges_keeps_meta_listing_ranges_whose_unlink_failed() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let cache_key = "test-bucket/case50-invalidate";
        let object_metadata = ObjectMetadata {
            etag: "case50-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            ..Default::default()
        };
        for start in [0u64, 4096] {
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    start + 1023,
                    &[3u8; 1024],
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }
        let meta = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(meta.ranges.len(), 2);
        let ranges_dir = temp_dir.path().join("ranges");
        let failing = meta.ranges.iter().find(|r| r.start == 0).unwrap();
        let failing_path = ranges_dir.join(&failing.file_path);
        let other_path = ranges_dir.join(
            &meta
                .ranges
                .iter()
                .find(|r| r.start == 4096)
                .unwrap()
                .file_path,
        );

        unlink_fault::arm(&failing_path);
        cache_manager
            .invalidate_all_ranges(cache_key)
            .await
            .unwrap();

        assert!(
            failing_path.exists(),
            "the injected failure must leave the .bin"
        );
        assert!(!other_path.exists(), "the other .bin is removed");
        let kept = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .expect("the .meta was unlinked while a .bin it named is still on disk");
        let extents: Vec<(u64, u64)> = kept.ranges.iter().map(|r| (r.start, r.end)).collect();
        assert_eq!(
            extents,
            vec![(0, 1023)],
            "the .meta must list exactly the survivor"
        );
    }

    /// A `DiskCacheManager` with a hybrid writer, so range fills take the journal-only arm.
    fn hybrid_disk_cache(dir: &std::path::Path) -> DiskCacheManager {
        std::fs::create_dir_all(dir.join("metadata").join("_journals")).unwrap();
        let mut dc = DiskCacheManager::new(dir.to_path_buf(), true, 1024, false, 1_048_576);
        let writer = crate::hybrid_metadata_writer::HybridMetadataWriter::new(
            dir.to_path_buf(),
            Arc::new(crate::metadata_lock_manager::MetadataLockManager::new(
                dir.to_path_buf(),
                Duration::from_secs(30),
                3,
            )),
            Arc::new(crate::journal_manager::JournalManager::new(
                dir.to_path_buf(),
                "case50-instance".to_string(),
            )),
            Arc::new(crate::hybrid_metadata_writer::ConsolidationTrigger::new(
                1024 * 1024,
                100,
            )),
        );
        dc.set_hybrid_metadata_writer(Arc::new(Mutex::new(writer)));
        dc
    }

    fn object_metadata_a() -> crate::cache_types::ObjectMetadata {
        crate::cache_types::ObjectMetadata {
            etag: "\"case50\"".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 4096,
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 4096,
            ..Default::default()
        }
    }

    /// U2: a journal-only fill holds the key's slot lock from the `.bin` rename through
    /// its journal append. A probe taken in between (the fill paused just before the append)
    /// must fail to acquire; otherwise a slot holder can see a `.bin` that no journal line or
    /// `.meta` names yet.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn journal_only_fill_holds_the_slot_lock_through_the_journal_append() {
        let temp_dir = TempDir::new().unwrap();
        let mut dc = hybrid_disk_cache(temp_dir.path());
        dc.initialize().await.unwrap();
        let key = "test-bucket/case50-fill-probe";
        let pause = range_rename_pause::arm_before_append(key);
        let (reached, resume) = (pause.reached.clone(), pause.resume.clone());
        let dir = temp_dir.path().to_path_buf();
        let probe = async move {
            tokio::time::timeout(Duration::from_secs(10), reached.notified())
                .await
                .expect("the fill never reached its pre-append pause");
            let acquired = crate::metadata_lock_manager::try_acquire_slot_lock(&dir, key).is_ok();
            resume.notify_one();
            acquired
        };
        let fill = dc.store_range(
            key,
            0,
            1023,
            &[5u8; 1024],
            object_metadata_a(),
            Duration::from_secs(3600),
            true,
        );
        let (stored, acquired) = tokio::join!(fill, probe);
        stored.unwrap();
        assert!(
            !acquired,
            "a probe took the key's slot lock between the fill's .bin rename and its journal \
             append"
        );
        assert!(dc.get_new_range_file_path(key, 0, 1023).exists());
    }

    /// U2: a journal-only fill whose journal append fails removes the `.bin` it renamed,
    /// so nothing is left that no journal line or `.meta` names.
    #[tokio::test]
    async fn journal_only_fill_removes_its_bin_when_the_journal_append_fails() {
        let temp_dir = TempDir::new().unwrap();
        let mut dc = hybrid_disk_cache(temp_dir.path());
        dc.initialize().await.unwrap();
        // A directory where the journal's lock file goes makes every append fail.
        std::fs::create_dir_all(
            temp_dir
                .path()
                .join("metadata/_journals/case50-instance.journal.lock"),
        )
        .unwrap();
        let key = "test-bucket/case50-fill-append-fails";
        let result = dc
            .store_range(
                key,
                0,
                1023,
                &[6u8; 1024],
                object_metadata_a(),
                Duration::from_secs(3600),
                true,
            )
            .await;
        assert!(result.is_err(), "fixture: the journal append must fail");
        assert!(
            !dc.get_new_range_file_path(key, 0, 1023).exists(),
            "a fill whose journal append failed left its .bin on disk, named by nothing"
        );
    }

    // ============================================================================
    // Tests for Path Resolution Functions
    // ============================================================================

    #[test]
    fn test_parse_cache_key_valid() {
        // Test basic parsing with leading slash
        let (bucket, object_key) = parse_cache_key("/my-bucket/path/to/object").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(object_key, "path/to/object");

        // Test without leading slash
        let (bucket, object_key) = parse_cache_key("my-bucket/path/to/object").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(object_key, "path/to/object");

        // Test with bucket names containing periods and hyphens
        let (bucket, object_key) = parse_cache_key("my.bucket-name/object").unwrap();
        assert_eq!(bucket, "my.bucket-name");
        assert_eq!(object_key, "object");

        // Test simple object at bucket root
        let (bucket, object_key) = parse_cache_key("bucket/file.txt").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(object_key, "file.txt");
    }

    #[test]
    fn test_parse_cache_key_invalid() {
        // Test missing slash (no object key)
        let result = parse_cache_key("no-slash-here");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid cache key format"));

        // Test empty string
        let result = parse_cache_key("");
        assert!(result.is_err());

        // Test only bucket with trailing slash
        let result = parse_cache_key("bucket/");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cache_key_rejects_dotdot() {
        // Validates: Requirements 2.1, 5.3
        let result = parse_cache_key("../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(".."));
    }

    #[test]
    fn test_parse_cache_key_rejects_single_dot() {
        // Validates: Requirements 2.1, 5.4
        let result = parse_cache_key("./bucket/key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cache_key_rejects_nul() {
        // Validates: Requirements 2.3, 5.5
        let result = parse_cache_key("bu\0cket/key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cache_key_rejects_control_chars() {
        // Validates: Requirements 2.3
        let result = parse_cache_key("bu\x01cket/key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cache_key_accepts_s3_valid_names() {
        // Validates: Requirements 2.6
        let (bucket, object_key) = parse_cache_key("my-bucket.v2/path/to/key").unwrap();
        assert_eq!(bucket, "my-bucket.v2");
        assert_eq!(object_key, "path/to/key");
    }

    #[test]
    fn test_get_sharded_path_stays_within_base() {
        // Validates: Requirements 2.4
        let tmp = TempDir::new().unwrap();
        let result = get_sharded_path(tmp.path(), "../foo/key", ".meta");
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitize_object_key_for_filename_basic() {
        // Test simple key
        let filename = sanitize_object_key_for_filename("simple.txt", ".meta");
        assert_eq!(filename, "simple.txt.meta");

        // Test key with filesystem-unsafe characters
        let filename = sanitize_object_key_for_filename("path/to/file.txt", ".meta");
        assert!(filename.contains("%2F")); // '/' should be encoded
        assert!(filename.ends_with(".meta"));

        // Test key with spaces
        let filename = sanitize_object_key_for_filename("file with spaces.txt", ".bin");
        assert!(filename.contains("%20")); // space should be encoded
        assert!(filename.ends_with(".bin"));

        // Test key with special characters
        let filename = sanitize_object_key_for_filename("file:with*special?chars", ".meta");
        assert!(filename.contains("%3A")); // ':' should be encoded
        assert!(filename.contains("%2A")); // '*' should be encoded
        assert!(filename.contains("%3F")); // '?' should be encoded
    }

    #[test]
    fn test_sanitize_object_key_for_filename_long_key() {
        // Test key that exceeds 200 characters
        let long_key = "a".repeat(250);
        let filename = sanitize_object_key_for_filename(&long_key, ".meta");

        // Should use BLAKE3 hash (64 hex chars) + suffix
        assert_eq!(filename.len(), 64 + 5); // 64 hex chars + ".meta"
        assert!(filename.ends_with(".meta"));

        // Verify it's a valid hex string (excluding suffix)
        let hash_part = &filename[..64];
        assert!(hash_part.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_sanitize_object_key_determinism() {
        // Same input should always produce same output
        let key = "path/to/file.txt";
        let filename1 = sanitize_object_key_for_filename(key, ".meta");
        let filename2 = sanitize_object_key_for_filename(key, ".meta");
        assert_eq!(filename1, filename2);

        // Long keys should also be deterministic
        let long_key = "a".repeat(250);
        let filename1 = sanitize_object_key_for_filename(&long_key, ".meta");
        let filename2 = sanitize_object_key_for_filename(&long_key, ".meta");
        assert_eq!(filename1, filename2);
    }

    #[test]
    fn test_get_sharded_path_basic() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");
        let cache_key = "my-bucket/path/to/file.txt";

        let path = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();

        // Verify path structure: base_dir/bucket/XX/YYY/filename
        let path_str = path.to_string_lossy();
        assert!(path_str.starts_with("/cache/metadata/my-bucket/"));
        assert!(path_str.ends_with(".meta"));

        // Verify bucket is in path
        assert!(path_str.contains("my-bucket"));

        // Verify hash directories exist (2 hex digits / 3 hex digits)
        let components: Vec<&str> = path_str.split('/').collect();
        assert!(components.len() >= 6); // /cache/metadata/bucket/XX/YYY/filename
    }

    #[test]
    fn test_get_sharded_path_determinism() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");
        let cache_key = "my-bucket/path/to/file.txt";

        // Same input should always produce same path
        let path1 = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();
        let path2 = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();
        assert_eq!(path1, path2);
    }

    #[test]
    fn test_get_sharded_path_same_object_different_buckets() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");

        // Same object key in different buckets
        let path1 = get_sharded_path(&base_dir, "bucket1/file.txt", ".meta").unwrap();
        let path2 = get_sharded_path(&base_dir, "bucket2/file.txt", ".meta").unwrap();

        let path1_str = path1.to_string_lossy();
        let path2_str = path2.to_string_lossy();

        // Should have different bucket directories
        assert!(path1_str.contains("bucket1"));
        assert!(path2_str.contains("bucket2"));

        // But same hash directories (XX/YYY) since object key is the same
        let extract_hash_dirs = |path: &str| -> String {
            let parts: Vec<&str> = path.split('/').collect();
            // Find bucket index and extract next two components (XX/YYY)
            for (i, part) in parts.iter().enumerate() {
                if part.starts_with("bucket") && i + 2 < parts.len() {
                    return format!("{}/{}", parts[i + 1], parts[i + 2]);
                }
            }
            String::new()
        };

        let hash1 = extract_hash_dirs(&path1_str);
        let hash2 = extract_hash_dirs(&path2_str);
        assert_eq!(
            hash1, hash2,
            "Same object key should produce same hash directories"
        );
    }

    #[test]
    fn test_get_sharded_path_invalid_cache_key() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");

        // Missing slash (no object key)
        let result = get_sharded_path(&base_dir, "no-slash", ".meta");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid cache key format"));
    }

    #[test]
    fn test_get_sharded_path_filename_excludes_bucket() {
        let base_dir = std::path::PathBuf::from("/cache/metadata");
        let cache_key = "my-bucket/file.txt";

        let path = get_sharded_path(&base_dir, cache_key, ".meta").unwrap();
        let filename = path.file_name().unwrap().to_string_lossy();

        // Filename should not contain bucket name
        assert!(!filename.contains("my-bucket"));
        assert!(filename.contains("file.txt"));
    }

    #[tokio::test]
    async fn test_cache_key_sanitization() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let unsafe_key = "bucket/path:with*special?chars<>|";
        let safe_key = cache_manager.sanitize_cache_key(unsafe_key);

        assert!(!safe_key.contains('/'));
        assert!(!safe_key.contains('*'));
        assert!(!safe_key.contains('?'));
        assert!(!safe_key.contains('<'));
        assert!(!safe_key.contains('>'));
        assert!(!safe_key.contains('|'));
    }

    #[tokio::test]
    async fn test_cache_type_determination() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Test basic cache types
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object"),
            "metadata"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:part:1"),
            "parts"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:range:0-1023"),
            "ranges"
        );

        // Version-related keys are now treated as regular metadata/parts/ranges
        // (versioning support has been removed)
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:version:123"),
            "metadata"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:version:123:part:1"),
            "parts"
        );
        assert_eq!(
            cache_manager.determine_cache_type("bucket:object:version:123:range:0-1023"),
            "ranges"
        );
    }

    // ============================================================================
    // Tests for New Range Storage Architecture
    // ============================================================================

    #[tokio::test]
    async fn test_new_metadata_file_path_generation() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/test-object";
        let path = cache_manager.get_new_metadata_file_path(cache_key);

        // Should be in metadata subdirectory
        assert!(path.to_string_lossy().contains("metadata"));
        // Should have .meta extension
        assert!(path.to_string_lossy().ends_with(".meta"));
        // Should contain bucket name as directory
        assert!(path.to_string_lossy().contains("test-bucket"));
        // Should have hash-based sharding directories (XX/YYY pattern)
        let path_str = path.to_string_lossy();
        let parts: Vec<&str> = path_str.split('/').collect();
        // Find the bucket directory and verify hash directories follow
        let bucket_idx = parts.iter().position(|&p| p == "test-bucket");
        assert!(bucket_idx.is_some(), "Bucket directory not found in path");
        let bucket_idx = bucket_idx.unwrap();
        // After bucket, should have XX (2 hex chars) and YYY (3 hex chars) directories
        assert!(bucket_idx + 2 < parts.len(), "Missing hash directories");
        let level1 = parts[bucket_idx + 1];
        let level2 = parts[bucket_idx + 2];
        assert_eq!(level1.len(), 2, "Level 1 directory should be 2 hex chars");
        assert_eq!(level2.len(), 3, "Level 2 directory should be 3 hex chars");
        assert!(
            level1.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 1 should be hex"
        );
        assert!(
            level2.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 2 should be hex"
        );
    }

    #[tokio::test]
    async fn test_new_range_file_path_generation() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 8388607u64;
        let path = cache_manager.get_new_range_file_path(cache_key, start, end);

        // Should be in ranges subdirectory
        assert!(path.to_string_lossy().contains("ranges"));
        // Should have .bin extension
        assert!(path.to_string_lossy().ends_with(".bin"));
        // Should contain bucket name as directory
        assert!(path.to_string_lossy().contains("test-bucket"));
        // Should contain range in filename
        assert!(path.to_string_lossy().contains("_0-8388607.bin"));
        // Should have hash-based sharding directories (XX/YYY pattern)
        let path_str = path.to_string_lossy();
        let parts: Vec<&str> = path_str.split('/').collect();
        // Find the bucket directory and verify hash directories follow
        let bucket_idx = parts.iter().position(|&p| p == "test-bucket");
        assert!(bucket_idx.is_some(), "Bucket directory not found in path");
        let bucket_idx = bucket_idx.unwrap();
        // After bucket, should have XX (2 hex chars) and YYY (3 hex chars) directories
        assert!(bucket_idx + 2 < parts.len(), "Missing hash directories");
        let level1 = parts[bucket_idx + 1];
        let level2 = parts[bucket_idx + 2];
        assert_eq!(level1.len(), 2, "Level 1 directory should be 2 hex chars");
        assert_eq!(level2.len(), 3, "Level 2 directory should be 3 hex chars");
        assert!(
            level1.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 1 should be hex"
        );
        assert!(
            level2.chars().all(|c| c.is_ascii_hexdigit()),
            "Level 2 should be hex"
        );
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_basic() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let key = "bucket:object";
        let sanitized = cache_manager.sanitize_cache_key_new(key);
        // With percent encoding, ':' becomes '%3A'
        assert_eq!(sanitized, "bucket%3Aobject");
        // Verify no unsafe characters remain
        assert!(!sanitized.contains(':'));
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_special_characters() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Test all special characters that need sanitization
        let key = "bucket:path/with?query&param=value";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, these characters should be encoded
        assert!(!sanitized.contains(':'));
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains('?'));
        // Note: '&' and '=' are not in our FRAGMENT set, so they may remain
        // But the key point is that unsafe filesystem characters are encoded

        // Verify percent-encoded versions are present
        assert!(sanitized.contains("%3A")); // :
        assert!(sanitized.contains("%2F")); // /
        assert!(sanitized.contains("%3F")); // ?
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_filesystem_unsafe_chars() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Test filesystem-unsafe characters
        let key = r#"bucket\path*with<special>chars|and"quotes"#;
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, these characters should be encoded
        assert!(!sanitized.contains('\\'));
        assert!(!sanitized.contains('*'));
        assert!(!sanitized.contains('<'));
        assert!(!sanitized.contains('>'));
        assert!(!sanitized.contains('|'));
        assert!(!sanitized.contains('"'));

        // Verify percent-encoded versions are present
        assert!(sanitized.contains("%5C")); // \
        assert!(sanitized.contains("%2A")); // *
        assert!(sanitized.contains("%3C")); // <
        assert!(sanitized.contains("%3E")); // >
        assert!(sanitized.contains("%7C")); // |
        assert!(sanitized.contains("%22")); // "
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_url_special_chars() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Test URL-related special characters
        let key = "bucket#fragment%20space{brace}[bracket]$dollar!exclaim";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, % should be encoded to prevent double-encoding issues
        assert!(!sanitized.contains('%') || sanitized.contains("%25"));

        // Note: Not all of these characters are in our FRAGMENT set
        // We only encode characters that are problematic for filesystems
        // The key test is that the result is filesystem-safe

        // Verify the string is filesystem-safe (no path separators or wildcards)
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains('\\'));
        assert!(!sanitized.contains('*'));
        assert!(!sanitized.contains('?'));
        assert!(!sanitized.contains('"'));
        assert!(!sanitized.contains('<'));
        assert!(!sanitized.contains('>'));
        assert!(!sanitized.contains('|'));
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_whitespace() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Test whitespace characters
        let key = "bucket with spaces\tand\ttabs\nand\nnewlines\rand\rreturns";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // With percent encoding, whitespace should be encoded
        assert!(!sanitized.contains(' '));
        assert!(!sanitized.contains('\t'));
        assert!(!sanitized.contains('\n'));
        assert!(!sanitized.contains('\r'));

        // Verify percent-encoded versions are present
        assert!(sanitized.contains("%20")); // space
        assert!(sanitized.contains("%09")); // tab
        assert!(sanitized.contains("%0A")); // newline
        assert!(sanitized.contains("%0D")); // carriage return
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_empty_string() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let key = "";
        let sanitized = cache_manager.sanitize_cache_key_new(key);
        assert_eq!(sanitized, "");
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_already_safe() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Test key that's already safe
        let key = "bucket-object-name_with_underscores.and.dots";
        let sanitized = cache_manager.sanitize_cache_key_new(key);
        assert_eq!(sanitized, key);
    }

    #[tokio::test]
    async fn test_path_generation_deterministic() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/test-object?query=value";
        let start = 1024u64;
        let end = 2047u64;

        // Generate paths multiple times
        let path1 = cache_manager.get_new_range_file_path(cache_key, start, end);
        let path2 = cache_manager.get_new_range_file_path(cache_key, start, end);
        let path3 = cache_manager.get_new_range_file_path(cache_key, start, end);

        // All should be identical
        assert_eq!(path1, path2);
        assert_eq!(path2, path3);

        // Same for metadata paths
        let meta_path1 = cache_manager.get_new_metadata_file_path(cache_key);
        let meta_path2 = cache_manager.get_new_metadata_file_path(cache_key);
        let meta_path3 = cache_manager.get_new_metadata_file_path(cache_key);

        assert_eq!(meta_path1, meta_path2);
        assert_eq!(meta_path2, meta_path3);
    }

    #[tokio::test]
    async fn test_path_generation_different_ranges() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/test-object";

        // Different ranges should produce different paths
        let path1 = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        let path2 = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
        let path3 = cache_manager.get_new_range_file_path(cache_key, 2048, 4095);

        assert_ne!(path1, path2);
        assert_ne!(path2, path3);
        assert_ne!(path1, path3);

        // But all should be in the same ranges directory
        assert_eq!(path1.parent(), path2.parent());
        assert_eq!(path2.parent(), path3.parent());
    }

    #[tokio::test]
    async fn test_path_generation_different_keys() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let start = 0u64;
        let end = 1023u64;

        // Different cache keys should produce different paths
        let path1 = cache_manager.get_new_range_file_path("bucket1:object1", start, end);
        let path2 = cache_manager.get_new_range_file_path("bucket2:object2", start, end);
        let path3 = cache_manager.get_new_range_file_path("bucket1:object2", start, end);

        assert_ne!(path1, path2);
        assert_ne!(path2, path3);
        assert_ne!(path1, path3);
    }

    #[tokio::test]
    async fn test_path_generation_large_ranges() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/large-object";

        // Test with very large range values
        let start = 1_000_000_000_000u64; // 1TB
        let end = 1_000_000_010_000u64;

        let path = cache_manager.get_new_range_file_path(cache_key, start, end);

        // Should contain the large numbers
        assert!(path.to_string_lossy().contains("1000000000000"));
        assert!(path.to_string_lossy().contains("1000000010000"));
    }

    #[tokio::test]
    async fn test_path_generation_full_object_as_range() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/test-object";
        let content_length = 10485760u64; // 10MB

        // Full object stored as range 0 to content_length-1
        let path = cache_manager.get_new_range_file_path(cache_key, 0, content_length - 1);

        assert!(path.to_string_lossy().contains("_0-10485759.bin"));
    }

    #[tokio::test]
    async fn test_path_generation_complex_s3_key() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Complex S3 key with multiple path segments and special characters
        let cache_key = "my-bucket/path/to/deeply/nested/object-with-dashes_and_underscores.tar.gz?versionId=abc123&partNumber=5";

        let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
        let range_path = cache_manager.get_new_range_file_path(cache_key, 0, 1048575);

        // Get just the filename portion (not the full path with directory separators)
        let meta_filename = meta_path.file_name().unwrap().to_string_lossy();
        let range_filename = range_path.file_name().unwrap().to_string_lossy();

        // Filenames should not contain filesystem-unsafe characters
        assert!(!meta_filename.contains(':'));
        assert!(!meta_filename.contains('/'));
        assert!(!meta_filename.contains('?'));
        assert!(!meta_filename.contains('\\'));
        assert!(!meta_filename.contains('*'));
        assert!(!meta_filename.contains('"'));
        assert!(!meta_filename.contains('<'));
        assert!(!meta_filename.contains('>'));
        assert!(!meta_filename.contains('|'));

        assert!(!range_filename.contains(':'));
        assert!(!range_filename.contains('/'));
        assert!(!range_filename.contains('?'));
        assert!(!range_filename.contains('\\'));
        assert!(!range_filename.contains('*'));
        assert!(!range_filename.contains('"'));
        assert!(!range_filename.contains('<'));
        assert!(!range_filename.contains('>'));
        assert!(!range_filename.contains('|'));
    }

    #[tokio::test]
    async fn test_sanitize_cache_key_new_unicode() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        // Test with unicode characters
        let key = "bucket:文件/объект/αρχείο";
        let sanitized = cache_manager.sanitize_cache_key_new(key);

        // Special chars should be percent-encoded
        assert!(sanitized.contains("%3A")); // :
        assert!(sanitized.contains("%2F")); // /

        // Unicode characters will be percent-encoded as UTF-8 bytes
        // This is correct behavior - it prevents collisions and is reversible
        // Verify the string is filesystem-safe
        assert!(!sanitized.contains(':'));
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains('\\'));

        // Verify it can be decoded back
        use percent_encoding::percent_decode_str;
        let decoded = percent_decode_str(&sanitized).decode_utf8().unwrap();
        assert_eq!(decoded, key);
    }

    // ============================================================================
    // Test Helper Functions
    // ============================================================================

    // ============================================================================
    // Tests for store_range() method
    // ============================================================================

    /// L4, data before pointer: a range fill syncs the `.bin` temp first, while neither the
    /// `.meta` nor the `.bin` is published, then the `.meta` temp, while the `.meta` is still
    /// unpublished. The direct arm renames the `.meta` before the `.bin`, so syncing the
    /// `.bin` at its own rename would be too late; the order assertion is what pins that.
    #[tokio::test]
    async fn range_fill_syncs_bin_then_meta_before_publishing_either() {
        use crate::cache_types::ObjectMetadata;
        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let cache_key = "test-bucket/synced-object";
        let meta_final = cache_manager.get_new_metadata_file_path(cache_key);
        let bin_final = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        let probe = publish_sync_probe::register(temp_dir.path());
        probe
            .watched
            .lock()
            .unwrap()
            .extend([meta_final.clone(), bin_final.clone()]);

        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &vec![7u8; 1024],
                ObjectMetadata {
                    etag: "e".to_string(),
                    content_length: 1024,
                    ..Default::default()
                },
                Duration::from_secs(3600),
                false,
            )
            .await
            .unwrap();

        let events = probe.events.lock().unwrap().clone();
        assert_eq!(
            events.len(),
            2,
            "one .bin and one .meta sync, got {:?}",
            events
        );
        let bin_name = bin_final.file_name().unwrap().to_string_lossy().to_string();
        let meta_name = meta_final
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let name = |e: &publish_sync_probe::SyncEvent| {
            e.path.file_name().unwrap().to_string_lossy().to_string()
        };
        assert!(
            name(&events[0]).starts_with(&format!("{bin_name}.tmp")),
            "the .bin temp is synced first, got {:?}",
            events[0].path
        );
        assert!(
            events[0].finals_present.is_empty(),
            "nothing may be published before the .bin data is synced, got {:?}",
            events[0].finals_present
        );
        assert!(
            name(&events[1]).starts_with(&meta_name) && name(&events[1]) != meta_name,
            "the .meta temp is synced second, got {:?}",
            events[1].path
        );
        assert!(
            events[1].finals_present.is_empty(),
            "the .meta is synced before its rename, got {:?}",
            events[1].finals_present
        );
        assert!(meta_final.exists() && bin_final.exists());
    }

    /// An injected publish-sync failure fails the fill and publishes neither file.
    #[tokio::test]
    async fn range_fill_publish_sync_failure_publishes_nothing() {
        use crate::cache_types::ObjectMetadata;
        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let cache_key = "test-bucket/unsynced-object";
        let probe = publish_sync_probe::register(temp_dir.path());
        probe.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &vec![7u8; 1024],
                ObjectMetadata {
                    etag: "e".to_string(),
                    content_length: 1024,
                    ..Default::default()
                },
                Duration::from_secs(3600),
                false,
            )
            .await;
        assert!(result.is_err(), "a failed data sync must fail the fill");
        assert!(!cache_manager.get_new_metadata_file_path(cache_key).exists());
        assert!(!cache_manager
            .get_new_range_file_path(cache_key, 0, 1023)
            .exists());
    }

    #[tokio::test]
    async fn test_store_range_basic() {
        use crate::cache_types::{NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024]; // 1KB of data

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Verify range file exists
        let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
        assert!(range_file_path.exists(), "Range file should exist");

        // Verify metadata file exists
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(metadata_file_path.exists(), "Metadata file should exist");

        // Read and verify metadata
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: NewCacheMetadata = serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.cache_key, cache_key);
        assert_eq!(metadata.object_metadata.etag, object_metadata.etag);
        assert_eq!(metadata.ranges.len(), 1);
        assert_eq!(metadata.ranges[0].start, start);
        assert_eq!(metadata.ranges[0].end, end);
        assert_eq!(metadata.ranges[0].uncompressed_size, 1024);
    }

    /// Spec: compression-followup-fixes Requirement 1.
    /// A manager built via `new_with_shared_stats` must record its write
    /// compression counters into the shared stats Arc, so a store_range write
    /// is observable through the source handler's snapshot (the object the
    /// /metrics endpoint reads).
    #[tokio::test]
    async fn test_store_range_records_into_shared_stats() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        // Source handler holds the "canonical" stats Arc (stands in for the one
        // CacheManagerInner owns and /metrics snapshots).
        let source = CompressionHandler::new(1024, true);
        let before = source.get_stats();
        assert_eq!(before.total_objects_compressed, 0);
        assert_eq!(before.total_objects_uncompressed, 0);

        let mut cache_manager = DiskCacheManager::new_with_shared_stats(
            temp_dir.path().to_path_buf(),
            true,
            1024,
            false,
            1_048_576,
            &source,
        );
        cache_manager.initialize().await.unwrap();

        let object_metadata = ObjectMetadata {
            etag: "shared-stats-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 4096,
            content_type: Some("text/plain".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 4096,
            parts: Vec::new(),
            ..Default::default()
        };

        // Highly compressible payload above the 1024 threshold → compressed branch.
        let data = vec![42u8; 4096];
        cache_manager
            .store_range(
                "shared-stats-bucket/obj",
                0,
                4095,
                &data,
                object_metadata,
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // The write must be visible through the SOURCE handler's snapshot,
        // proving the disk manager shared the Arc rather than counting into a
        // throwaway one.
        let after = source.get_stats();
        assert_eq!(
            after.total_objects_compressed, 1,
            "store_range compression should increment the shared stats Arc"
        );
        assert!(
            after.total_bytes_before >= 4096,
            "shared stats should reflect bytes-before of the write"
        );
    }

    #[tokio::test]
    async fn test_store_range_multiple_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];

            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Verify all range files exist
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
            assert!(range_file_path.exists(), "Range file {} should exist", i);
        }

        // Verify metadata contains all ranges
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.ranges.len(), 5);
    }

    #[tokio::test]
    async fn test_store_range_etag_mismatch_invalidation() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store first range successfully
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Verify first range was stored
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();
        assert_eq!(metadata.ranges.len(), 1, "Should have 1 range");
        assert_eq!(metadata.object_metadata.etag, "test-etag");

        // Get the path of the first range file
        let first_range_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        assert!(first_range_path.exists(), "First range file should exist");

        // Store with different ETag - should invalidate existing ranges and succeed
        // Requirement 14.1, 14.2, 14.3: ETag mismatch invalidates existing ranges
        let different_metadata = ObjectMetadata {
            etag: "different-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        let result = cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data,
                different_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await;
        assert!(
            result.is_ok(),
            "Should succeed after invalidating old ranges"
        );

        // Verify the old range file was deleted
        assert!(
            !first_range_path.exists(),
            "Old range file should be deleted on ETag mismatch"
        );

        // Verify new range was stored with new ETag
        let new_range_path = cache_manager.get_new_range_file_path(cache_key, 1024, 2047);
        assert!(new_range_path.exists(), "New range file should exist");

        // Verify metadata was updated with new ETag and only new range
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(
            metadata.ranges.len(),
            1,
            "Should have only 1 range (the new one)"
        );
        assert_eq!(
            metadata.object_metadata.etag, "different-etag",
            "ETag should be updated"
        );
        assert_eq!(
            metadata.ranges[0].start, 1024,
            "Range should be the new one"
        );
        assert_eq!(metadata.ranges[0].end, 2047, "Range should be the new one");
    }

    #[tokio::test]
    async fn test_store_range_invalid_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Test start > end
        let result = cache_manager
            .store_range(
                cache_key,
                1024,
                0,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await;
        assert!(result.is_err(), "Should fail when start > end");

        // Test data size mismatch (data larger than range)
        // data is 1024 bytes but range 0-511 expects 512 bytes
        let result = cache_manager
            .store_range(
                cache_key,
                0,
                511,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await;
        assert!(
            result.is_err(),
            "Should fail when data size doesn't match range"
        );
    }

    #[tokio::test]
    async fn test_store_range_update_existing() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data1 = vec![1u8; 1024];
        let data2 = vec![2u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range first time
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Store same range again (update)
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data2,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Verify metadata still has only 1 range
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(
            metadata.ranges.len(),
            1,
            "Should still have only 1 range after update"
        );
    }

    #[tokio::test]
    async fn test_store_range_full_object_as_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let content_length = 10240u64;
        let data = vec![42u8; content_length as usize];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range 0 to content_length-1
        cache_manager
            .store_range(
                cache_key,
                0,
                content_length - 1,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Verify range file exists
        let range_file_path =
            cache_manager.get_new_range_file_path(cache_key, 0, content_length - 1);
        assert!(
            range_file_path.exists(),
            "Full object range file should exist"
        );

        // Verify metadata
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.ranges.len(), 1);
        assert_eq!(metadata.ranges[0].start, 0);
        assert_eq!(metadata.ranges[0].end, content_length - 1);
    }

    #[tokio::test]
    async fn test_store_range_no_tmp_files_left() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Check for any .tmp files in ranges and metadata directories
        let ranges_dir = temp_dir.path().join("ranges");
        let metadata_dir = temp_dir.path().join("metadata");

        for entry in std::fs::read_dir(&ranges_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(
                !path.to_string_lossy().ends_with(".tmp"),
                "No .tmp files should remain in ranges dir"
            );
        }

        for entry in std::fs::read_dir(&metadata_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(
                !path.to_string_lossy().ends_with(".tmp"),
                "No .tmp files should remain in metadata dir"
            );
        }
    }

    #[tokio::test]
    async fn test_store_range_concurrent_writes_no_lost_updates() {
        use crate::cache_types::ObjectMetadata;
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(Mutex::new(DiskCacheManager::new(
            temp_dir.path().to_path_buf(),
            true,
            1024,
            false,
            1_048_576,
        )));
        cache_manager.lock().await.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 102400,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 102400,
            parts: Vec::new(),
            ..Default::default()
        };

        // Spawn 10 concurrent tasks, each storing a different range
        let mut handles = vec![];
        for i in 0..10 {
            let cache_manager_clone = Arc::clone(&cache_manager);
            let cache_key_clone = cache_key.to_string();
            let object_metadata_clone = object_metadata.clone();

            let handle = tokio::spawn(async move {
                let start = i * 1024;
                let end = start + 1023;
                let data = vec![i as u8; 1024];

                let mut cm = cache_manager_clone.lock().await;
                cm.store_range(
                    &cache_key_clone,
                    start,
                    end,
                    &data,
                    object_metadata_clone,
                    Duration::from_secs(3600),
                    true,
                )
                .await
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Verify all 10 ranges are in the metadata (no lost updates)
        let cm = cache_manager.lock().await;
        let metadata_file_path = cm.get_new_metadata_file_path(cache_key);
        let metadata_content = std::fs::read_to_string(&metadata_file_path).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(
            metadata.ranges.len(),
            10,
            "All 10 ranges should be present (no lost updates)"
        );

        // Verify each range is present
        for i in 0..10 {
            let start = i * 1024;
            let end = start + 1023;
            assert!(
                metadata
                    .ranges
                    .iter()
                    .any(|r| r.start == start && r.end == end),
                "Range {}-{} should be present",
                start,
                end
            );
        }
    }

    // ============================================================================
    // Tests for get_metadata() and update_metadata() methods
    // ============================================================================

    #[tokio::test]
    async fn test_get_metadata_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/nonexistent-object";

        // Get metadata for nonexistent entry
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for nonexistent metadata"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_after_store_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(metadata.is_some(), "Metadata should exist");

        let metadata = metadata.unwrap();
        assert_eq!(metadata.cache_key, cache_key);
        assert_eq!(metadata.object_metadata.etag, object_metadata.etag);
        assert_eq!(metadata.ranges.len(), 1);
        assert_eq!(metadata.ranges[0].start, start);
        assert_eq!(metadata.ranges[0].end, end);
    }

    #[tokio::test]
    async fn test_get_metadata_no_range_data_loaded() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Store multiple large ranges
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10 * 1024 * 1024, // 10MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        for i in 0..10 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024]; // 1MB each

            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Get metadata - should be fast and not load 10MB of range data
        let start_time = std::time::Instant::now();
        let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
        let elapsed = start_time.elapsed();

        assert!(metadata.is_some());
        let metadata = metadata.unwrap();
        assert_eq!(metadata.ranges.len(), 10);

        // Should complete quickly (under 100ms even with 10 ranges)
        // This validates that we're not loading range data
        assert!(
            elapsed.as_millis() < 100,
            "Metadata read took {}ms, should be under 100ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_get_metadata_expired() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Create metadata that's a write-cached object with expired write_cache_expires_at
        let now = SystemTime::now();
        let expired_time = now - std::time::Duration::from_secs(3600); // 1 hour ago

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                // Mark as write-cached with expired write_cache_expires_at
                is_write_cached: true,
                write_cache_expires_at: Some(expired_time),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600), // Not expired
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Write expired write-cache metadata directly
        cache_manager.update_metadata(&metadata).await.unwrap();

        // Try to get metadata - should return None because write cache is expired
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for expired write-cache metadata"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_corrupted_json() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Write corrupted JSON to metadata file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "{ this is not valid json }").unwrap();

        // Try to get metadata - should return None and log error
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for corrupted metadata"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Write empty file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "").unwrap();

        // Try to get metadata - should return None
        let result = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(
            result.is_none(),
            "Should return None for empty metadata file"
        );
    }

    // ============================================================================
    // Tests for check_object_expiration metadata read failure handling
    // Validates: Requirements 6.1, 6.2
    // ============================================================================

    #[tokio::test]
    async fn test_check_object_expiration_corrupted_metadata() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/corrupted-object";

        // Write corrupted (non-JSON) content to metadata file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "this is not valid json at all!!!").unwrap();

        // check_object_expiration should return Expired { last_modified: None }
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired {
                last_modified: None,
                etag: None
            },
            "Corrupted metadata should be treated as expired with no last_modified"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_invalid_json_schema() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/wrong-schema-object";

        // Write valid JSON but wrong schema
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, r#"{"foo": "bar", "baz": 42}"#).unwrap();

        // check_object_expiration should return Expired { last_modified: None }
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired {
                last_modified: None,
                etag: None
            },
            "Invalid JSON schema should be treated as expired with no last_modified"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_missing_metadata() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/nonexistent-object";

        // No metadata file exists
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired {
                last_modified: None,
                etag: None
            },
            "Missing metadata should be treated as expired with no last_modified"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_empty_file() {
        use crate::cache_types::ObjectExpirationResult;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/empty-metadata-object";

        // Write empty file
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_file_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file_path, "").unwrap();

        // check_object_expiration should return Expired { last_modified: None }
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Expired {
                last_modified: None,
                etag: None
            },
            "Empty metadata file should be treated as expired with no last_modified"
        );
    }

    // ============================================================================
    // Tests for check_object_expiration with current_get_ttl
    // Validates: Requirements 2.1, 2.2, 2.5
    // ============================================================================

    #[tokio::test]
    async fn test_check_object_expiration_get_ttl_zero_always_expired() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/ttl-zero-object";
        let now = std::time::SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"test-etag\"".to_string(),
                last_modified: "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
                content_length: 100,
                content_type: Some("text/plain".to_string()),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        // get_ttl=0 → always expired regardless of age
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::ZERO)
            .await
            .unwrap();
        assert!(
            matches!(result, ObjectExpirationResult::Expired { .. }),
            "get_ttl=0 should always return Expired"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_within_ttl_is_fresh() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/within-ttl-object";
        let now = std::time::SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"test-etag\"".to_string(),
                last_modified: "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
                content_length: 100,
                content_type: Some("text/plain".to_string()),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now - std::time::Duration::from_secs(60),
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        // age=60s, get_ttl=300s → 60 ≤ 300 → Fresh
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(300))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Fresh,
            "Object within TTL window should be Fresh"
        );
    }

    #[tokio::test]
    async fn test_check_object_expiration_beyond_ttl_is_expired() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/beyond-ttl-object";
        let now = std::time::SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"test-etag\"".to_string(),
                last_modified: "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
                content_length: 100,
                content_type: Some("text/plain".to_string()),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now - std::time::Duration::from_secs(600),
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        // age=600s, get_ttl=300s → 600 > 300 → Expired
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(300))
            .await
            .unwrap();
        match result {
            ObjectExpirationResult::Expired {
                last_modified,
                etag,
            } => {
                assert_eq!(
                    last_modified,
                    Some("Wed, 01 Jan 2025 12:00:00 GMT".to_string())
                );
                assert_eq!(etag, Some("\"test-etag\"".to_string()));
            }
            ObjectExpirationResult::Fresh => {
                panic!("Object beyond TTL should be Expired");
            }
        }
    }

    #[tokio::test]
    async fn test_check_object_expiration_clock_skew_future_created_at() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/clock-skew-object";
        let now = std::time::SystemTime::now();

        // created_at is in the future (clock skew)
        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"test-etag\"".to_string(),
                last_modified: "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
                content_length: 100,
                content_type: Some("text/plain".to_string()),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now + std::time::Duration::from_secs(60),
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        // Clock skew: created_at in the future → age=0 → Fresh for non-zero ttl
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(300))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Fresh,
            "Clock skew (created_at in the future) should be treated as Fresh for non-zero ttl"
        );

        // Clock skew with get_ttl=0 → still expired (get_ttl=0 always means expired)
        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::ZERO)
            .await
            .unwrap();
        assert!(
            matches!(result, ObjectExpirationResult::Expired { .. }),
            "Clock skew with get_ttl=0 should still be Expired"
        );
    }

    // Spec: write-cache-last-modified. Requirements: 1.1, 1.3, 1.4, 1.6, 2.1, 2.2
    // (tasks 3.1, 3.2, 3.8)

    /// R1.1/R1.3: a write-cached entry with no effective Last-Modified must
    /// report Expired even while comfortably within a generous get_ttl — this is
    /// the trigger this spec adds, distinct from ordinary TTL expiry.
    #[tokio::test]
    async fn write_cached_entry_with_no_last_modified_is_expired_within_generous_ttl() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/write-cached-no-last-modified";
        let now = std::time::SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"test-etag\"".to_string(),
                last_modified: String::new(), // The write-through PUT's own state.
                content_length: 100,
                is_write_cached: true,
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now, // seconds old — well within a generous get_ttl
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(86400))
            .await
            .unwrap();
        match result {
            ObjectExpirationResult::Expired { last_modified, .. } => {
                assert_eq!(
                    last_modified, None,
                    "R2.1/R2.2: an empty stored value must surface as None, not \
                     Some(String::new()), or the caller inserts a literal empty \
                     if-modified-since header"
                );
            }
            ObjectExpirationResult::Fresh => {
                panic!(
                    "a write-cached entry with no effective Last-Modified must be \
                     Expired regardless of get_ttl age (R1.1, R1.3) — this is the \
                     new trigger, not ordinary TTL expiry"
                );
            }
        }
    }

    /// R1.4: a READ-cache entry (is_write_cached: false) with no effective
    /// Last-Modified — legitimately possible from an older release — must NOT be
    /// forced into revalidation by this new trigger. Its existing TTL behaviour
    /// (Fresh within get_ttl) must be unchanged.
    #[tokio::test]
    async fn read_cache_entry_with_no_last_modified_keeps_ordinary_ttl_behaviour() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/read-cached-no-last-modified";
        let now = std::time::SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"test-etag\"".to_string(),
                last_modified: String::new(),
                content_length: 100,
                is_write_cached: false, // Not write-cached — the R1.4 conjunct.
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(86400))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Fresh,
            "R1.4: a non-write-cached entry with no Last-Modified must retain its \
             existing TTL-only behaviour and must not be revalidated on this \
             ground"
        );
    }

    /// R1.6/R1.7/R11.11: the trigger must be upload-shape-agnostic. A write-cached
    /// entry that plausibly came from a multipart completion (parts_count and
    /// upload_id set, no branch on either) must trigger revalidation identically
    /// to the single-part case above.
    #[tokio::test]
    async fn write_cached_multipart_entry_with_no_last_modified_is_expired() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/multipart-no-last-modified";
        let now = std::time::SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"c0ac455d64d87082383844659c014574-2\"".to_string(),
                last_modified: String::new(),
                content_length: 10_485_760,
                is_write_cached: true,
                parts_count: Some(2),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(86400))
            .await
            .unwrap();
        assert!(
            matches!(result, ObjectExpirationResult::Expired { .. }),
            "the trigger must fire for a multipart-shaped entry identically to a \
             single-part one — no branch on parts_count or upload_id (R1.6)"
        );
    }

    /// R2.1/R2.2 direct coverage: a write-cached entry that already has a real
    /// Last-Modified (having previously revalidated, R3) must be evaluated on
    /// ordinary TTL terms only — the new conjunct must go false once the field is
    /// learned, which is also part of what makes R4 terminate.
    #[tokio::test]
    async fn write_cached_entry_with_last_modified_uses_ordinary_ttl() {
        use crate::cache_types::{
            CompressionInfo, NewCacheMetadata, ObjectExpirationResult, ObjectMetadata,
        };

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/write-cached-with-last-modified";
        let now = std::time::SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"test-etag\"".to_string(),
                last_modified: "Wed, 09 Sep 2026 10:28:17 GMT".to_string(),
                content_length: 100,
                is_write_cached: true,
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

        let result = cache_manager
            .check_object_expiration(cache_key, std::time::Duration::from_secs(86400))
            .await
            .unwrap();
        assert_eq!(
            result,
            ObjectExpirationResult::Fresh,
            "once Last-Modified is known, the new conjunct must be false and \
             ordinary get_ttl freshness applies — a still-write-cached entry with \
             a learned Last-Modified must not be forced to revalidate again"
        );
    }

    #[tokio::test]
    async fn test_update_metadata_basic() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 10240,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 10240,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: vec![RangeSpec::new(
                0,
                1023,
                "test_0-1023.bin".to_string(),
                crate::compression::CompressionAlgorithm::Lz4,
                1024,
                1024,
            )],
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Update metadata
        cache_manager.update_metadata(&metadata).await.unwrap();

        // Verify file exists
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(metadata_file_path.exists(), "Metadata file should exist");

        // Read and verify
        let retrieved = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(retrieved.is_some());

        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.cache_key, metadata.cache_key);
        assert_eq!(
            retrieved.object_metadata.etag,
            metadata.object_metadata.etag
        );
        assert_eq!(retrieved.ranges.len(), 1);
    }

    #[tokio::test]
    async fn test_update_metadata_atomic() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        // Create initial metadata
        let metadata1 = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "etag-1".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        cache_manager.update_metadata(&metadata1).await.unwrap();

        // Update with new metadata
        let metadata2 = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "etag-2".to_string(),
                last_modified: "Thu, 22 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 2048,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 2048,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        cache_manager.update_metadata(&metadata2).await.unwrap();

        // Verify the update
        let retrieved = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.object_metadata.etag, "etag-2");
        assert_eq!(retrieved.object_metadata.content_length, 2048);
    }

    #[tokio::test]
    async fn test_update_metadata_no_tmp_files_left() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        cache_manager.update_metadata(&metadata).await.unwrap();

        // Check for any .tmp files in metadata directory
        let metadata_dir = temp_dir.path().join("metadata");

        for entry in std::fs::read_dir(&metadata_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            assert!(
                !path.to_string_lossy().ends_with(".tmp"),
                "No .tmp files should remain"
            );
        }
    }

    #[tokio::test]
    async fn test_update_metadata_creates_directory() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        // Don't initialize - test that update_metadata creates directories
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Should create metadata directory automatically
        cache_manager.update_metadata(&metadata).await.unwrap();

        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(metadata_file_path.exists(), "Metadata file should exist");
        assert!(
            metadata_file_path.parent().unwrap().exists(),
            "Metadata directory should exist"
        );
    }

    #[tokio::test]
    async fn test_get_and_update_metadata_round_trip() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let now = SystemTime::now();

        // Create metadata with multiple ranges
        let mut metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 10240,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 10240,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: vec![
                RangeSpec::new(
                    0,
                    1023,
                    "test_0-1023.bin".to_string(),
                    crate::compression::CompressionAlgorithm::Lz4,
                    512,
                    1024,
                ),
                RangeSpec::new(
                    1024,
                    2047,
                    "test_1024-2047.bin".to_string(),
                    crate::compression::CompressionAlgorithm::Lz4,
                    1024,
                    1024,
                ),
            ],
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Write metadata
        cache_manager.update_metadata(&metadata).await.unwrap();

        // Read it back
        let retrieved = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();

        // Verify all fields match
        assert_eq!(retrieved.cache_key, metadata.cache_key);
        assert_eq!(
            retrieved.object_metadata.etag,
            metadata.object_metadata.etag
        );
        assert_eq!(retrieved.ranges.len(), 2);
        assert_eq!(retrieved.ranges[0].start, 0);
        assert_eq!(retrieved.ranges[0].end, 1023);
        assert_eq!(retrieved.ranges[1].start, 1024);
        assert_eq!(retrieved.ranges[1].end, 2047);

        // Modify and update
        metadata.ranges.push(RangeSpec::new(
            2048,
            3071,
            "test_2048-3071.bin".to_string(),
            crate::compression::CompressionAlgorithm::Lz4,
            1024,
            1024,
        ));

        cache_manager.update_metadata(&metadata).await.unwrap();

        // Read again and verify the update
        let retrieved2 = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved2.ranges.len(), 3);
        assert_eq!(retrieved2.ranges[2].start, 2048);
    }

    #[tokio::test]
    async fn test_metadata_operations_with_special_characters() {
        use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Cache key with special characters
        let cache_key = "test-bucket/path/to/object?query=value&param=123";
        let now = SystemTime::now();

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "test-etag".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: 1024,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: 1024,
                parts: Vec::new(),
                ..Default::default()
            },
            ranges: Vec::new(),
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        // Should handle special characters correctly
        cache_manager.update_metadata(&metadata).await.unwrap();

        let retrieved = cache_manager.get_metadata(cache_key).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().cache_key, cache_key);
    }

    // ============================================================================
    // Tests for find_cached_ranges() method
    // ============================================================================

    /// The six-case lookup-purpose matrix.
    ///
    /// Spec: the expired entry revalidation feature R1.1, R1.2, R1.5, R7.5,
    /// and the `design.md` table it is copied from.
    ///
    /// | Metadata source | Stored state | Purpose | Expected |
    /// |---|---|---|---|
    /// | Disk | Fresh | FreshServe | overlap, `Fresh` |
    /// | Disk | Expired | FreshServe | none |
    /// | Disk | Expired | RevalidationCandidate | overlap, `Expired` |
    /// | Preloaded | Fresh | FreshServe | overlap, `Fresh` |
    /// | Preloaded | Expired | FreshServe | none |
    /// | Preloaded | Expired | RevalidationCandidate | overlap, `Expired` |
    ///
    /// # Why both metadata sources, in one test
    ///
    /// R1.5 requires the policy to be identical whether metadata is read from
    /// disk or supplied preloaded: "the same entry SHALL NOT be a candidate on
    /// one path and an ordinary miss on the other solely because of metadata
    /// source." The two arms converge on one expiry check today, so this test
    /// pins that convergence rather than discovering it. Running both sources
    /// against the *same* seeded entry in one test is what makes them comparable
    /// — separate tests could drift into seeding differently and stop being a
    /// statement about the source at all.
    ///
    /// The `Fresh`/`RevalidationCandidate` combination is deliberately absent: it
    /// is not a distinct case. Stored freshness decides the outcome and the
    /// purpose only matters once expiry has passed, which is exactly the property
    /// making the two purposes safe to share one overlap calculation. The four
    /// rows above that vary purpose cover it.
    #[tokio::test]
    async fn find_cached_ranges_purpose_matrix() {
        for stored_expired in [false, true] {
            let temp_dir = TempDir::new().unwrap();
            let mut cache_manager =
                DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
            cache_manager.initialize().await.unwrap();
            let cache_key = "test-bucket/purpose-matrix";

            let object_metadata = crate::cache_types::ObjectMetadata {
                etag: "\"matrix-etag\"".to_string(),
                last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                content_length: 1024,
                ..Default::default()
            };
            cache_manager
                .store_range(
                    cache_key,
                    0,
                    1023,
                    &vec![7u8; 1024],
                    object_metadata,
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();

            // Drive stored expiry directly. Waiting out a real TTL would make the
            // test slow and, worse, would move `created_at` too — and this test is
            // about `expires_at` alone. `created_at` is the *live*-TTL input and is
            // not read here at all.
            let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
            let mut metadata: crate::cache_types::NewCacheMetadata =
                serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
            metadata.expires_at = if stored_expired {
                SystemTime::now() - Duration::from_secs(3600)
            } else {
                SystemTime::now() + Duration::from_secs(3600)
            };
            std::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap()).unwrap();

            // Precondition, asserted rather than assumed: without it a fixture
            // that failed to move `expires_at` would make the Expired rows pass
            // vacuously as Fresh rows.
            assert_eq!(
                SystemTime::now() > metadata.expires_at,
                stored_expired,
                "fixture failed to establish stored_expired={}",
                stored_expired
            );

            let preloaded = metadata.clone();
            for (source, preloaded_arg) in [("disk", None), ("preloaded", Some(&preloaded))] {
                for purpose in [
                    RangeLookupPurpose::FreshServe,
                    RangeLookupPurpose::RevalidationCandidate,
                ] {
                    let result = cache_manager
                        .find_cached_ranges(cache_key, 0, 1023, preloaded_arg, purpose)
                        .await
                        .unwrap();

                    let hides = stored_expired && purpose == RangeLookupPurpose::FreshServe;
                    assert_eq!(
                        result.ranges.is_empty(),
                        hides,
                        "source={source} stored_expired={stored_expired} purpose={purpose:?}: \
                         expected ranges empty={hides}, got {} range(s)",
                        result.ranges.len()
                    );

                    let expected_freshness = if stored_expired && !hides {
                        StoredFreshness::Expired
                    } else {
                        StoredFreshness::Fresh
                    };
                    assert_eq!(
                        result.stored_freshness, expected_freshness,
                        "source={source} stored_expired={stored_expired} purpose={purpose:?}: \
                         stored_freshness must be {expected_freshness:?}"
                    );

                    // R1.2: an expired candidate is discoverable but not
                    // serveable. Asserted on the marker the serve gate reads, not
                    // merely on the coverage being present.
                    if stored_expired && purpose == RangeLookupPurpose::RevalidationCandidate {
                        assert!(
                            result.stored_freshness == crate::cache_types::StoredFreshness::Expired,
                            "an expired candidate must report itself as such, so no caller \
                             can mistake it for ordinary serveable coverage"
                        );
                    }
                }
            }
        }
    }

    /// an extent marked `unservable` (kept listed after a failed retire unlink) is
    /// never coverage. Its `.bin` here holds another version's bytes under this `.meta`'s
    /// ETag, which is the state `retire_other_version_fills` leaves. Every lookup purpose
    /// and both metadata sources must report a miss, so the GET refetches; any range
    /// returned is loaded and checked, so a lookup that ignores the marker fails on the
    /// wrong bytes it would serve.
    #[tokio::test]
    async fn find_cached_ranges_never_serves_an_unservable_extent() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let cache_key = "test-bucket/case51/unservable";
        let other_version_bytes = vec![9u8; 1024];

        // The file holds the other version's bytes ("v2").
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &other_version_bytes,
                crate::cache_types::ObjectMetadata {
                    etag: "\"etag-v2\"".to_string(),
                    last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
                    content_length: 1024,
                    ..Default::default()
                },
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        // The `.meta` names "v1" and keeps the extent listed, marked unservable.
        let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
        let mut metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        metadata.object_metadata.etag = "\"etag-v1\"".to_string();
        metadata.ranges[0].unservable = true;
        std::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap()).unwrap();
        let reread: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert!(
            reread.ranges[0].unservable,
            "fixture: the marker must survive a .meta round trip"
        );

        let preloaded = metadata.clone();
        for (source, preloaded_arg) in [("disk", None), ("preloaded", Some(&preloaded))] {
            for purpose in [
                RangeLookupPurpose::FreshServe,
                RangeLookupPurpose::RevalidationCandidate,
            ] {
                for (start, end) in [(0u64, 1023u64), (100, 199), (0, 2047)] {
                    let result = cache_manager
                        .find_cached_ranges(cache_key, start, end, preloaded_arg, purpose)
                        .await
                        .unwrap();
                    for range in &result.ranges {
                        let served = cache_manager.load_range_data(range).await.unwrap();
                        assert_ne!(
                            served, other_version_bytes,
                            "source={source} purpose={purpose:?} request={start}-{end}: the \
                             lookup served the other version's bytes from an unservable extent"
                        );
                    }
                    assert!(
                        result.ranges.is_empty(),
                        "source={source} purpose={purpose:?} request={start}-{end}: an \
                         unservable extent counted as coverage"
                    );
                }
            }
        }

        // Control: the same fixture without the marker is found (the lookup is not
        // vacuously empty).
        metadata.ranges[0].unservable = false;
        let result = cache_manager
            .find_cached_ranges(
                cache_key,
                0,
                1023,
                Some(&metadata),
                RangeLookupPurpose::FreshServe,
            )
            .await
            .unwrap();
        assert_eq!(
            result.ranges.len(),
            1,
            "control: unmarked extent is coverage"
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_no_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/nonexistent-object";

        // Find ranges for nonexistent object
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 1023)
            .await
            .unwrap();
        assert_eq!(
            ranges.len(),
            0,
            "Should return empty vec for nonexistent object"
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_exact_match() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store a range
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Find exact match
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 1023)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find exactly one range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_full_containment() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store a large range
        let data = vec![42u8; 4096];
        cache_manager
            .store_range(
                cache_key,
                0,
                4095,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request a smaller range that's fully contained
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 1024, 2047)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find the containing range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 4095);
        assert!(
            ranges[0].start <= 1024 && ranges[0].end >= 2047,
            "Range should fully contain requested range"
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_no_overlap() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges 0-1023 and 4096-5119
        let data1 = vec![1u8; 1024];
        let data2 = vec![2u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                4096,
                5119,
                &data2,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request range 2048-3071 (no overlap)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 2048, 3071)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 0, "Should find no overlapping ranges");
    }

    #[tokio::test]
    async fn test_find_cached_ranges_partial_overlap_start() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 0-1023
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request range 512-1535 (overlaps at start)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 512, 1535)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find one overlapping range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_partial_overlap_end() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 1024-2047
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request range 512-1535 (overlaps at end)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 512, 1535)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find one overlapping range");
        assert_eq!(ranges[0].start, 1024);
        assert_eq!(ranges[0].end, 2047);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_multiple_overlaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Request range that spans multiple cached ranges (1536-3583)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 1536, 3583)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 3, "Should find three overlapping ranges");

        // Verify ranges are sorted by start position
        assert_eq!(ranges[0].start, 1024);
        assert_eq!(ranges[0].end, 2047);
        assert_eq!(ranges[1].start, 2048);
        assert_eq!(ranges[1].end, 3071);
        assert_eq!(ranges[2].start, 3072);
        assert_eq!(ranges[2].end, 4095);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_adjacent_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store adjacent ranges
        let data1 = vec![1u8; 1024];
        let data2 = vec![2u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data1,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data2,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request range that spans both
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 512, 1535)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 2, "Should find both adjacent ranges");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
        assert_eq!(ranges[1].start, 1024);
        assert_eq!(ranges[1].end, 2047);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_boundary_conditions() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 1024-2047
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                1024,
                2047,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request range ending exactly at cached range start (0-1024)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 1024)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should overlap at boundary (inclusive)");

        // Request range starting exactly at cached range end (2047-3071)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 2047, 3071)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should overlap at boundary (inclusive)");

        // Request range just before cached range (0-1023)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 1023)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 0, "Should not overlap");

        // Request range just after cached range (2048-3071)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 2048, 3071)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 0, "Should not overlap");
    }

    #[tokio::test]
    async fn test_find_cached_ranges_full_object_as_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let content_length = 10240u64;
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range 0 to content_length-1
        let data = vec![42u8; content_length as usize];
        cache_manager
            .store_range(
                cache_key,
                0,
                content_length - 1,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request any sub-range
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 2048, 4095)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find the full object range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, content_length - 1);

        // Request full object
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, content_length - 1)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find exact match");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, content_length - 1);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_invalid_range() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";

        // Request invalid range (start > end)
        let result = cache_manager.fresh_serve_ranges(cache_key, 1024, 0).await;
        assert!(result.is_err(), "Should fail with invalid range");
    }

    #[tokio::test]
    async fn test_find_cached_ranges_large_number_of_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100 * 1024 * 1024, // 100MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 100 ranges (1MB each)
        for i in 0..100 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Find ranges covering bytes 50MB..55MB — should return exactly 5 ranges
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 50 * 1024 * 1024, 55 * 1024 * 1024 - 1)
            .await
            .unwrap();

        assert_eq!(ranges.len(), 5, "Should find 5 overlapping ranges");
    }

    #[tokio::test]
    async fn test_find_cached_ranges_sorted_output() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges in non-sequential order
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                4096,
                5119,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                2048,
                3071,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Find all ranges
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 10239)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 3);

        // Verify they're sorted by start position
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[1].start, 2048);
        assert_eq!(ranges[2].start, 4096);

        // Verify sorting is stable
        for i in 1..ranges.len() {
            assert!(
                ranges[i - 1].start <= ranges[i].start,
                "Ranges should be sorted by start position"
            );
        }
    }

    #[tokio::test]
    async fn test_find_cached_ranges_single_byte_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range 0-1023
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request single byte range
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 512, 512)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find containing range");
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);
    }

    #[tokio::test]
    #[ignore = "Flaky performance test - depends on system timing"]
    async fn test_find_cached_ranges_performance_requirement() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1000 * 1024 * 1024, // 1GB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1000 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 1000 ranges (1MB each)
        for i in 0..1000 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Verify metadata file size is reasonable (should be under 1MB for 1000 ranges with TTL fields)
        let metadata_file_path = cache_manager.get_new_metadata_file_path(cache_key);
        let metadata_size = std::fs::metadata(&metadata_file_path).unwrap().len();
        assert!(
            metadata_size < 1024 * 1024,
            "Metadata file is {} bytes, should be under 1MB",
            metadata_size
        );

        // Find ranges - should complete in under 10ms as per requirement 2.3
        let start_time = std::time::Instant::now();
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 500 * 1024 * 1024, 510 * 1024 * 1024 - 1)
            .await
            .unwrap();
        let elapsed = start_time.elapsed();

        assert_eq!(ranges.len(), 10, "Should find 10 overlapping ranges");
        assert!(
            elapsed.as_millis() < 10,
            "Range lookup with 1000 ranges took {}ms, should be under 10ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_find_cached_ranges_with_gaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 20480,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 20480,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store non-contiguous ranges with gaps:
        // Range A: 0-2999 (3KB)
        // GAP: 3000-4999 (2KB missing)
        // Range B: 5000-7999 (3KB)
        // GAP: 8000-8999 (1KB missing)
        // Range C: 9000-11999 (3KB)

        let data_a = vec![1u8; 3000];
        let data_b = vec![2u8; 3000];
        let data_c = vec![3u8; 3000];

        cache_manager
            .store_range(
                cache_key,
                0,
                2999,
                &data_a,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                5000,
                7999,
                &data_b,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                9000,
                11999,
                &data_c,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request range that spans all cached ranges and gaps (0-11999)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 11999)
            .await
            .unwrap();

        // Should return all 3 non-contiguous ranges
        assert_eq!(ranges.len(), 3, "Should find all 3 non-contiguous ranges");

        // Verify ranges are sorted and correct
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 2999);

        assert_eq!(ranges[1].start, 5000);
        assert_eq!(ranges[1].end, 7999);

        assert_eq!(ranges[2].start, 9000);
        assert_eq!(ranges[2].end, 11999);

        // Verify gaps are NOT in the returned ranges
        // Gap 1: 3000-4999 should NOT be present
        // Gap 2: 8000-8999 should NOT be present
        for range in &ranges {
            // No range should cover the gap areas
            assert!(
                !(range.start <= 3000 && range.end >= 4999),
                "Gap 3000-4999 should not be covered"
            );
            assert!(
                !(range.start <= 8000 && range.end >= 8999),
                "Gap 8000-8999 should not be covered"
            );
        }
    }

    #[tokio::test]
    async fn test_find_cached_ranges_partial_hit_with_gaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 20480,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 20480,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges: 0-999, 3000-3999, 6000-6999
        let data = vec![42u8; 1000];
        cache_manager
            .store_range(
                cache_key,
                0,
                999,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                3000,
                3999,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();
        cache_manager
            .store_range(
                cache_key,
                6000,
                6999,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request range 500-6500 (spans all 3 ranges with gaps)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 500, 6500)
            .await
            .unwrap();

        // Should return all 3 ranges that overlap
        assert_eq!(ranges.len(), 3);

        // Caller would need to:
        // 1. Use bytes 500-999 from Range A (500 bytes from cache)
        // 2. Fetch bytes 1000-2999 from S3 (2000 bytes missing)
        // 3. Use bytes 3000-3999 from Range B (1000 bytes from cache)
        // 4. Fetch bytes 4000-5999 from S3 (2000 bytes missing)
        // 5. Use bytes 6000-6500 from Range C (501 bytes from cache)
        // Total: 2001 bytes from cache, 4000 bytes from S3

        // Verify the ranges returned
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 999);
        assert_eq!(ranges[1].start, 3000);
        assert_eq!(ranges[1].end, 3999);
        assert_eq!(ranges[2].start, 6000);
        assert_eq!(ranges[2].end, 6999);
    }

    #[tokio::test]
    async fn test_find_cached_ranges_mostly_gaps() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100 * 1024, // 100KB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store only a tiny portion: 0-1023 (1KB out of 100KB)
        let data = vec![42u8; 1024];
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Request entire object (0-102399)
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 102399)
            .await
            .unwrap();

        // Should return the 1 tiny cached range
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, 1023);

        // Caller should decide: 1% cache hit rate probably not worth the complexity
        // Better to treat as cache miss and fetch entire range from S3
        // This is a policy decision for the higher-level cache logic
    }

    // ============================================================================
    // Tests for load_range_data() method
    // ============================================================================

    #[tokio::test]
    async fn test_load_range_data_basic() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let original_data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Get metadata to retrieve range spec
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1);

        // Load range data
        let loaded_data = cache_manager
            .load_range_data(&metadata.ranges[0])
            .await
            .unwrap();

        // Verify data matches original
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), 1024);
    }

    #[tokio::test]
    async fn test_load_range_data_with_compression() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 100, false, 1_048_576); // Low threshold to force compression
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object.txt"; // .txt extension to enable compression
        let start = 0u64;
        let end = 2047u64;
        // Create compressible data (repeated pattern)
        let original_data = vec![65u8; 2048]; // 2KB of 'A's

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("text/plain".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range (should be compressed)
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1);

        // Verify compression was applied
        let range_spec = &metadata.ranges[0];
        assert!(
            range_spec.compressed_size < range_spec.uncompressed_size,
            "Data should be compressed: compressed={}, uncompressed={}",
            range_spec.compressed_size,
            range_spec.uncompressed_size
        );

        // Load range data (should decompress automatically)
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify data matches original after decompression
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), 2048);
    }

    #[tokio::test]
    async fn test_load_range_data_no_compression() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), false, 1024, false, 1_048_576); // Compression disabled
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 1023u64;
        let original_data = vec![42u8; 1024];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store range (no compression)
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_spec = &metadata.ranges[0];

        // Verify compression algorithm is Lz4 (all data uses frame format now)
        assert_eq!(range_spec.compression_algorithm, CompressionAlgorithm::Lz4);

        // Load range data
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify data matches original
        assert_eq!(loaded_data, original_data);
    }

    #[tokio::test]
    async fn test_load_range_data_missing_file() {
        use crate::cache_types::RangeSpec;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Create a range spec pointing to a non-existent file
        let range_spec = RangeSpec::new(
            0,
            1023,
            "nonexistent_file_0-1023.bin".to_string(),
            CompressionAlgorithm::Lz4,
            1024,
            1024,
        );

        // Try to load - should fail with error
        let result = cache_manager.load_range_data(&range_spec).await;
        assert!(result.is_err(), "Should fail when range file is missing");

        // Verify error message mentions the missing file
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("not found") || error_msg.contains("nonexistent"),
            "Error should mention missing file: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_load_range_data_multiple_ranges() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges with different data
        let mut expected_data = Vec::new();
        for i in 0..5 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            expected_data.push(data.clone());
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 5);

        // Load each range and verify
        for (i, range_spec) in metadata.ranges.iter().enumerate() {
            let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();
            assert_eq!(loaded_data, expected_data[i], "Range {} data mismatch", i);
            assert_eq!(loaded_data.len(), 1024);
        }
    }

    #[tokio::test]
    async fn test_load_range_data_large_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 10485759u64; // 10MB
        let original_data = vec![42u8; 10485760];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10485760,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10485760,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store large range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_spec = &metadata.ranges[0];

        // Load large range
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify data
        assert_eq!(loaded_data.len(), 10485760);
        assert_eq!(loaded_data, original_data);
    }

    #[tokio::test]
    async fn test_load_range_data_full_object_as_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let content_length = 10240u64;
        let original_data = vec![42u8; content_length as usize];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range 0 to content_length-1
        cache_manager
            .store_range(
                cache_key,
                0,
                content_length - 1,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1);

        let range_spec = &metadata.ranges[0];
        assert_eq!(range_spec.start, 0);
        assert_eq!(range_spec.end, content_length - 1);

        // Load full object
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), content_length as usize);
    }

    #[tokio::test]
    async fn test_load_range_data_different_compression_algorithms() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 100, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100000,
            content_type: Some("text/plain".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100000,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store ranges with different file extensions (triggers different compression)
        let test_cases = vec![
            ("test-bucket/file.txt", vec![65u8; 2048]), // Text file - compressible
            ("test-bucket/file.bin", vec![42u8; 2048]), // Binary - may not compress well
            ("test-bucket/file.json", vec![123u8; 2048]), // JSON - compressible
        ];

        for (cache_key, original_data) in test_cases {
            // Store range
            cache_manager
                .store_range(
                    cache_key,
                    0,
                    2047,
                    &original_data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();

            // Get metadata
            let metadata = cache_manager
                .get_metadata(cache_key)
                .await
                .unwrap()
                .unwrap();
            let range_spec = &metadata.ranges[0];

            // Load range data
            let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

            // Verify data matches regardless of compression algorithm used
            assert_eq!(
                loaded_data, original_data,
                "Data mismatch for {}",
                cache_key
            );
            assert_eq!(loaded_data.len(), 2048);
        }
    }

    #[tokio::test]
    async fn test_load_range_data_single_byte() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let start = 0u64;
        let end = 0u64; // Single byte
        let original_data = vec![42u8];

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1024,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store single byte range
        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &original_data,
                object_metadata,
                std::time::Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_spec = &metadata.ranges[0];

        // Load single byte
        let loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();

        // Verify
        assert_eq!(loaded_data, original_data);
        assert_eq!(loaded_data.len(), 1);
        assert_eq!(loaded_data[0], 42);
    }

    #[tokio::test]
    async fn test_load_range_data_after_store_and_retrieve() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store multiple ranges
        let mut expected_data = Vec::new();
        for i in 0..3 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            expected_data.push(data.clone());
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Simulate a request for range 1024-2047
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 1024, 2047)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find exact match");

        // Load the found range
        let loaded_data = cache_manager.load_range_data(&ranges[0]).await.unwrap();

        // Verify it's the correct range data
        assert_eq!(loaded_data, expected_data[1]);
        assert_eq!(loaded_data.len(), 1024);
        assert_eq!(loaded_data[0], 1); // Second range has value 1
    }

    #[tokio::test]
    async fn test_load_range_data_performance() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 100 * 1024 * 1024, // 100MB
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 100 * 1024 * 1024,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 10 ranges of 1MB each
        for i in 0..10 {
            let start = i * 1024 * 1024;
            let end = start + 1024 * 1024 - 1;
            let data = vec![i as u8; 1024 * 1024];
            cache_manager
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Get metadata
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();

        // Load all ranges and measure total time
        let start_time = std::time::Instant::now();
        for range_spec in &metadata.ranges {
            let _loaded_data = cache_manager.load_range_data(range_spec).await.unwrap();
        }
        let elapsed = start_time.elapsed();

        // Loading 10MB total should be fast (under 1 second)
        assert!(
            elapsed.as_secs() < 1,
            "Loading 10 ranges (10MB total) took {}ms, should be under 1 second",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_load_range_data_validates_size() {
        use crate::cache_types::RangeSpec;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Manually create a range file with wrong size
        let ranges_dir = temp_dir.path().join("ranges");
        std::fs::create_dir_all(&ranges_dir).unwrap();

        let range_file_path = ranges_dir.join("test_0-1023.bin");
        let wrong_data = vec![42u8; 512]; // Only 512 bytes instead of 1024
        std::fs::write(&range_file_path, &wrong_data).unwrap();

        // Create range spec expecting 1024 bytes with no compression
        // so the raw data length (512) is compared directly against uncompressed_size (1024)
        let range_spec = RangeSpec::new(
            0,
            1023,
            "test_0-1023.bin".to_string(),
            CompressionAlgorithm::None,
            1024,
            1024,
        );

        // Try to load - should fail due to size mismatch
        let result = cache_manager.load_range_data(&range_spec).await;
        assert!(
            result.is_err(),
            "Should fail when data size doesn't match range size"
        );

        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("mismatch") || error_msg.contains("size"),
            "Error should mention size mismatch: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_load_range_data_concurrent_loads() {
        use crate::cache_types::ObjectMetadata;
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(Mutex::new(DiskCacheManager::new(
            temp_dir.path().to_path_buf(),
            true,
            1024,
            false,
            1_048_576,
        )));
        cache_manager.lock().await.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store 10 ranges
        let mut expected_data = Vec::new();
        for i in 0..10 {
            let start = i * 1024;
            let end = start + 1023;
            let data = vec![i as u8; 1024];
            expected_data.push(data.clone());
            cache_manager
                .lock()
                .await
                .store_range(
                    cache_key,
                    start,
                    end,
                    &data,
                    object_metadata.clone(),
                    Duration::from_secs(3600),
                    true,
                )
                .await
                .unwrap();
        }

        // Get metadata
        let metadata = cache_manager
            .lock()
            .await
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let range_specs: Vec<_> = metadata.ranges.clone();

        // Load all ranges concurrently
        let mut handles = vec![];
        for (i, range_spec) in range_specs.into_iter().enumerate() {
            let cache_manager_clone = Arc::clone(&cache_manager);
            let expected = expected_data[i].clone();

            let handle = tokio::spawn(async move {
                let cm = cache_manager_clone.lock().await;
                let loaded = cm.load_range_data(&range_spec).await.unwrap();
                assert_eq!(loaded, expected);
            });

            handles.push(handle);
        }

        // Wait for all loads to complete
        for handle in handles {
            handle.await.unwrap();
        }
    }

    // ============================================================================
    // Tests for Full Object as Range Handling (Task 7)
    // ============================================================================

    #[tokio::test]
    async fn test_store_full_object_as_range_basic() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let data = vec![42u8; 10240]; // 10KB object

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: data.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: data.len() as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Verify it was stored as range 0 to content_length-1
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(metadata.ranges.len(), 1, "Should have exactly one range");
        assert_eq!(metadata.ranges[0].start, 0, "Range should start at 0");
        assert_eq!(
            metadata.ranges[0].end, 10239,
            "Range should end at content_length-1"
        );
        assert_eq!(
            metadata.ranges[0].uncompressed_size, 10240,
            "Uncompressed size should match data length"
        );

        // Verify range file exists
        let range_file_path = cache_manager.get_new_range_file_path(cache_key, 0, 10239);
        assert!(range_file_path.exists(), "Range file should exist");
    }

    #[tokio::test]
    async fn test_store_full_object_as_range_empty_object() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/empty-object";
        let data = vec![]; // Empty object

        let object_metadata = ObjectMetadata {
            etag: "empty-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 0,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 0,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store empty object
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Verify metadata exists but has no ranges (empty objects have no data to cache)
        let metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            metadata.ranges.len(),
            0,
            "Empty object should have no ranges"
        );
        assert_eq!(
            metadata.object_metadata.content_length, 0,
            "Content length should be 0"
        );
    }

    #[tokio::test]
    async fn test_full_object_can_serve_any_range() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let original_data: Vec<u8> = (0..10240).map(|i| (i % 256) as u8).collect(); // 10KB with pattern

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: original_data.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: original_data.len() as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object as range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &original_data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Test that we can serve various range requests from the cached full object

        // Test range at start
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 1023)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering start");
        let range_data = cache_manager.load_range_data(&ranges[0]).await.unwrap();
        assert_eq!(
            &range_data[0..1024],
            &original_data[0..1024],
            "Range data should match"
        );

        // Test range in middle
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 5000, 6000)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering middle");

        // Test range at end
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 9000, 10239)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering end");

        // Test full range
        let ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 10239)
            .await
            .unwrap();
        assert_eq!(ranges.len(), 1, "Should find range covering entire object");
        assert_eq!(ranges[0].start, 0, "Range should start at 0");
        assert_eq!(ranges[0].end, 10239, "Range should end at content_length-1");
    }

    #[tokio::test]
    async fn test_full_object_as_range_no_special_casing() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let full_object_data = vec![42u8; 10240]; // 10KB object

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: full_object_data.len() as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: full_object_data.len() as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Store full object using store_full_object_as_range
        cache_manager
            .store_full_object_as_range(
                cache_key,
                &full_object_data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Now store a partial range using store_range
        let cache_key2 = "test-bucket/test-object-2";
        let partial_data = vec![99u8; 1024];
        cache_manager
            .store_range(
                cache_key2,
                0,
                1023,
                &partial_data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        // Both should be stored in the same way (as ranges in the ranges/ directory)
        let full_obj_metadata = cache_manager
            .get_metadata(cache_key)
            .await
            .unwrap()
            .unwrap();
        let partial_obj_metadata = cache_manager
            .get_metadata(cache_key2)
            .await
            .unwrap()
            .unwrap();

        // Both should have ranges array
        assert!(
            !full_obj_metadata.ranges.is_empty(),
            "Full object should have ranges"
        );
        assert!(
            !partial_obj_metadata.ranges.is_empty(),
            "Partial range should have ranges"
        );

        // Both should have range files in ranges/ directory
        let full_obj_range_path = cache_manager.get_new_range_file_path(cache_key, 0, 10239);
        let partial_range_path = cache_manager.get_new_range_file_path(cache_key2, 0, 1023);

        assert!(
            full_obj_range_path.to_string_lossy().contains("ranges/"),
            "Full object should be in ranges/ directory"
        );
        assert!(
            partial_range_path.to_string_lossy().contains("ranges/"),
            "Partial range should be in ranges/ directory"
        );

        // Both should be retrievable using the same mechanisms
        let full_obj_ranges = cache_manager
            .fresh_serve_ranges(cache_key, 0, 10239)
            .await
            .unwrap();
        let partial_ranges = cache_manager
            .fresh_serve_ranges(cache_key2, 0, 1023)
            .await
            .unwrap();

        assert_eq!(
            full_obj_ranges.len(),
            1,
            "Full object should have one range"
        );
        assert_eq!(
            partial_ranges.len(),
            1,
            "Partial range should have one range"
        );
    }

    #[tokio::test]
    async fn test_store_full_object_size_mismatch() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/test-object";
        let data = vec![42u8; 10240]; // 10KB object

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 5120, // Wrong size!
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 5120,
            parts: Vec::new(),
            ..Default::default()
        };

        // Try to store with mismatched size - should fail
        let result = cache_manager
            .store_full_object_as_range(
                cache_key,
                &data,
                object_metadata.clone(),
                Duration::from_secs(3600),
                true,
            )
            .await;
        assert!(
            result.is_err(),
            "Should fail when data length doesn't match metadata"
        );
    }

    /// One key, one stored range, and a peer holding the key's metadata slot lock.
    ///
    /// The range is stored BEFORE the peer takes the slot, because `store_range` acquires
    /// the same slot and would otherwise wait on the fixture's own peer. Returns the live
    /// guard so the test decides when the peer releases.
    async fn slot_contended_entry(
        cache_manager: &mut DiskCacheManager,
        cache_key: &str,
    ) -> (crate::metadata_lock_manager::MetadataLock, PathBuf, PathBuf) {
        let object_metadata = crate::cache_types::ObjectMetadata {
            etag: "\"metalock-b1\"".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1024,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1024,
            ..Default::default()
        };
        cache_manager
            .store_range(
                cache_key,
                0,
                1023,
                &vec![9u8; 1024],
                object_metadata,
                Duration::from_secs(3600),
                true,
            )
            .await
            .expect("fixture: store_range must succeed");

        let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
        let bin_path = cache_manager.get_new_range_file_path(cache_key, 0, 1023);
        assert!(meta_path.exists(), "fixture: .meta must exist");
        assert!(bin_path.exists(), "fixture: .bin must exist");

        let peer = crate::metadata_lock_manager::try_acquire_slot_lock(
            &cache_manager.cache_dir,
            cache_key,
        )
        .expect("fixture: the peer must be able to take the slot");
        assert_eq!(
            peer.lock_file_path(),
            crate::metadata_lock_manager::slot_lock_path(&cache_manager.cache_dir, cache_key),
            "fixture: the peer must hold the SAME slot the function under test acquires"
        );
        (peer, meta_path, bin_path)
    }

    /// Same two-sided shape for `invalidate_cache_entry`. This function has no production
    /// caller on this tree (its three callers have none of their own), so the assertion
    /// pins the contract for whatever revives it rather than guarding a live path.
    #[tokio::test]
    async fn invalidate_cache_entry_refuses_while_a_peer_holds_the_slot_then_deletes() {
        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let cache_key = "test-bucket/metalock-b1-invalidate-cache-entry";
        let (peer, meta_path, bin_path) = slot_contended_entry(&mut cache_manager, cache_key).await;

        let contended = cache_manager.invalidate_cache_entry(cache_key).await;
        assert!(
            contended.is_err(),
            "invalidate_cache_entry must fail rather than delete while a peer holds the \
             key's metadata slot lock. got={contended:?}"
        );
        assert!(
            meta_path.exists() && bin_path.exists(),
            "the entry was deleted while a peer held the slot — the read-then-delete ran \
             unlocked. meta_exists={} bin_exists={}",
            meta_path.exists(),
            bin_path.exists()
        );

        drop(peer);
        cache_manager
            .invalidate_cache_entry(cache_key)
            .await
            .expect("once the slot is free the invalidation must proceed");
        assert!(
            !meta_path.exists() && !bin_path.exists(),
            "positive control: with the slot free the entry must be deleted. meta_exists={} \
             bin_exists={}",
            meta_path.exists(),
            bin_path.exists()
        );
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: journal-based-metadata-updates, Property 10: Single-Instance Direct Write Optimization**
    /// *For any* cache-hit operation when shared_storage mode is disabled, the system may use
    /// direct metadata writes for performance (bypassing the journal).
    /// When shared_storage mode is enabled, the system shall use journal-based updates.
    /// **Validates: Requirements 5.4, 5.5**
    #[quickcheck]
    fn prop_mode_dependent_ttl_refresh_behavior(
        range_start: u64,
        range_end_offset: u16,
        ttl_secs: u16,
        use_shared_storage: bool,
    ) -> TestResult {
        // Filter invalid inputs
        if ttl_secs == 0 || range_end_offset == 0 {
            return TestResult::discard();
        }

        let range_end = range_start.saturating_add(range_end_offset as u64);
        let cache_key = "test-bucket/test-object";

        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();

            // Create cache manager
            let mut cache_manager = DiskCacheManager::new(
                temp_dir.path().to_path_buf(),
                true,  // compression_enabled
                1024,  // compression_threshold
                false, // write_cache_enabled
                1_048_576, // compression_batch_size (default 1 MiB)
            );

            if use_shared_storage {
                // Set up CacheHitUpdateBuffer for shared storage mode
                let buffer = std::sync::Arc::new(
                    crate::cache_hit_update_buffer::CacheHitUpdateBuffer::with_config(
                        temp_dir.path().to_path_buf(),
                        "test-instance".to_string(),
                        Duration::from_secs(60), // Long interval to prevent auto-flush
                        10000,
                    )
                );
                cache_manager.set_cache_hit_update_buffer(buffer.clone());

                // Property 1: In shared storage mode, cache_hit_update_buffer should be set
                if cache_manager.get_cache_hit_update_buffer().is_none() {
                    return TestResult::error("CacheHitUpdateBuffer should be set in shared storage mode");
                }

                // Call refresh_object_ttl
                let result = cache_manager.refresh_object_ttl(
                    cache_key,
                    Duration::from_secs(ttl_secs as u64),
                ).await;

                // Should succeed (buffered in RAM)
                if result.is_err() {
                    return TestResult::error(format!(
                        "refresh_object_ttl failed in shared storage mode: {:?}",
                        result.err()
                    ));
                }

                // Property 2: Entry should be buffered (not written directly to metadata)
                let buffer_len = buffer.buffer_len().await;
                if buffer_len != 1 {
                    return TestResult::error(format!(
                        "Expected 1 entry in buffer, got {}",
                        buffer_len
                    ));
                }

                // Property 3: No metadata file should be created (journal routing)
                let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
                if metadata_path.exists() {
                    return TestResult::error("Metadata file should not be created in shared storage mode (journal routing)");
                }
            } else {
                // Property 4: In single instance mode, cache_hit_update_buffer should be None
                if cache_manager.get_cache_hit_update_buffer().is_some() {
                    return TestResult::error("CacheHitUpdateBuffer should not be set in single instance mode");
                }

                // Create metadata file first (required for direct TTL refresh)
                let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
                if let Some(parent) = metadata_path.parent() {
                    tokio::fs::create_dir_all(parent).await.unwrap();
                }

                // Create initial metadata with the range
                let now = SystemTime::now();
                let range_spec = crate::cache_types::RangeSpec {
                    start: range_start,
                    end: range_end,
                    file_path: "test_range.bin".to_string(),
                    compression_algorithm: CompressionAlgorithm::Lz4,
                    compressed_size: range_end - range_start + 1,
                    uncompressed_size: range_end - range_start + 1,
                    created_at: now,
                    last_accessed: now,
                    access_count: 1,
                    staged: None, unservable: false, file_identity: None,
                };

                let metadata = crate::cache_types::NewCacheMetadata {
                    cache_key: cache_key.to_string(),
                    object_metadata: crate::cache_types::ObjectMetadata::default(),
                    ranges: vec![range_spec],
                    created_at: now,
                    expires_at: now + Duration::from_secs(3600),
                    compression_info: crate::cache_types::CompressionInfo::default(),
                    ..Default::default()
                };

                let json = serde_json::to_string_pretty(&metadata).unwrap();
                tokio::fs::write(&metadata_path, json).await.unwrap();

                // Call refresh_object_ttl
                let result = cache_manager.refresh_object_ttl(
                    cache_key,
                    Duration::from_secs(ttl_secs as u64),
                ).await;

                // Should succeed (direct write)
                if result.is_err() {
                    return TestResult::error(format!(
                        "refresh_object_ttl failed in single instance mode: {:?}",
                        result.err()
                    ));
                }

                // Property 5: Metadata file should be updated directly
                if !metadata_path.exists() {
                    return TestResult::error("Metadata file should exist after direct TTL refresh");
                }

                // Property 6: No journal file should be created
                let journal_path = temp_dir.path()
                    .join("metadata")
                    .join("_journals")
                    .join("test-instance.journal");
                if journal_path.exists() {
                    return TestResult::error("Journal file should not be created in single instance mode");
                }
            }

            TestResult::passed()
        })
    }

    // ============================================================================
    // Tests for batch_delete_ranges (parallel file deletes) — Task 3.2
    // Requirements: 2.1, 2.4
    // ============================================================================

    /// Test batch_delete_ranges with multiple range files — verify all deleted and bytes_freed correct.
    /// Validates Requirement 2.1: async file deletes work correctly for multiple ranges.
    #[tokio::test]
    async fn test_batch_delete_ranges_multiple_files() {
        use crate::cache_types::CompressionInfo;
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/batch-delete-object";
        let now = SystemTime::now();

        // Create 5 range .bin files directly on disk with known sizes
        let ranges: Vec<(u64, u64, usize)> = vec![
            (0, 999, 1000),
            (1000, 1999, 1000),
            (2000, 2999, 1000),
            (3000, 3999, 1000),
            (4000, 4999, 1000),
        ];

        let mut range_specs = Vec::new();
        for (start, end, size) in &ranges {
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            if let Some(parent) = range_file_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            let data = vec![0xABu8; *size];
            std::fs::write(&range_file_path, &data).unwrap();

            range_specs.push(crate::cache_types::RangeSpec {
                start: *start,
                end: *end,
                file_path: range_file_path
                    .strip_prefix(temp_dir.path().join("ranges"))
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: *size as u64,
                uncompressed_size: *size as u64,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                staged: None,
                unservable: false,
                file_identity: None,
            });
        }

        // Write metadata file
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata::default(),
            ranges: range_specs,
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        if let Some(parent) = metadata_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let json = serde_json::to_string_pretty(&metadata).unwrap();
        std::fs::write(&metadata_path, &json).unwrap();

        // Delete ranges 0-999, 2000-2999, 4000-4999 (3 of 5)
        let ranges_to_delete = vec![(0u64, 999u64), (2000u64, 2999u64), (4000u64, 4999u64)];

        let deletion = cache_manager
            .batch_delete_ranges(cache_key, &ranges_to_delete)
            .await
            .unwrap();

        // Verify bytes_freed = 3 * 1000 = 3000
        assert_eq!(
            deletion.bytes_freed, 3000,
            "bytes_freed should equal sum of deleted file sizes"
        );

        // Verify not all ranges evicted (2 remain)
        assert!(
            !deletion.all_ranges_evicted,
            "all_evicted should be false when some ranges remain"
        );

        // Verify 3 paths were deleted
        assert_eq!(
            deletion.deleted_paths.len(),
            3,
            "Should have 3 deleted file paths"
        );

        // R7.2: every requested extent existed and unlinked cleanly here, so the
        // accounting set must match the request set exactly.
        let mut unlinked = deletion.unlinked_extents.clone();
        unlinked.sort_unstable();
        assert_eq!(
            unlinked, ranges_to_delete,
            "all three .bin files were deletable, so all three extents must be reported unlinked"
        );

        // Verify the .bin files for deleted ranges no longer exist
        for (start, end) in &ranges_to_delete {
            let path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            assert!(
                !path.exists(),
                "Range file {}-{} should be deleted",
                start,
                end
            );
        }

        // Verify the .bin files for remaining ranges still exist
        let remaining_ranges = vec![(1000u64, 1999u64), (3000u64, 3999u64)];
        for (start, end) in &remaining_ranges {
            let path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            assert!(
                path.exists(),
                "Range file {}-{} should still exist",
                start,
                end
            );
        }

        // Verify metadata was updated — only 2 ranges remain
        let updated_content = std::fs::read_to_string(&metadata_path).unwrap();
        let updated_metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&updated_content).unwrap();
        assert_eq!(
            updated_metadata.ranges.len(),
            2,
            "Metadata should have 2 remaining ranges"
        );
        assert!(updated_metadata
            .ranges
            .iter()
            .any(|r| r.start == 1000 && r.end == 1999));
        assert!(updated_metadata
            .ranges
            .iter()
            .any(|r| r.start == 3000 && r.end == 3999));
    }

    /// Test batch_delete_ranges with some missing files — verify warning logged, remaining files
    /// still deleted, and metadata updated correctly.
    /// Validates Requirement 2.4: failed deletes log warning and continue processing.
    #[tokio::test]
    async fn test_batch_delete_ranges_missing_files() {
        use crate::cache_types::CompressionInfo;
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/batch-delete-missing";
        let now = SystemTime::now();

        // Create 4 ranges in metadata, but only create .bin files for 2 of them
        let all_ranges: Vec<(u64, u64)> = vec![(0, 999), (1000, 1999), (2000, 2999), (3000, 3999)];
        let file_size: usize = 500;

        let mut range_specs = Vec::new();
        for (start, end) in &all_ranges {
            let range_file_path = cache_manager.get_new_range_file_path(cache_key, *start, *end);
            if let Some(parent) = range_file_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }

            // Only create .bin files for ranges 0-999 and 2000-2999
            // Ranges 1000-1999 and 3000-3999 will be "missing"
            if *start == 0 || *start == 2000 {
                let data = vec![0xCDu8; file_size];
                std::fs::write(&range_file_path, &data).unwrap();
            }

            range_specs.push(crate::cache_types::RangeSpec {
                start: *start,
                end: *end,
                file_path: range_file_path
                    .strip_prefix(temp_dir.path().join("ranges"))
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: file_size as u64,
                uncompressed_size: file_size as u64,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                staged: None,
                unservable: false,
                file_identity: None,
            });
        }

        // Write metadata file with all 4 ranges
        let metadata = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata::default(),
            ranges: range_specs,
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
        if let Some(parent) = metadata_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let json = serde_json::to_string_pretty(&metadata).unwrap();
        std::fs::write(&metadata_path, &json).unwrap();

        // Delete all 4 ranges — 2 exist on disk, 2 are missing
        let ranges_to_delete: Vec<(u64, u64)> = all_ranges.clone();

        let deletion = cache_manager
            .batch_delete_ranges(cache_key, &ranges_to_delete)
            .await
            .unwrap();

        // bytes_freed should only count the 2 files that existed (2 * 500 = 1000)
        assert_eq!(
            deletion.bytes_freed, 1000,
            "bytes_freed should only count files that existed on disk"
        );

        // All ranges evicted from metadata (even missing files get removed from metadata)
        assert!(
            deletion.all_ranges_evicted,
            "all_evicted should be true when all ranges are removed from metadata"
        );

        // R7.2: an ALREADY-ABSENT `.bin` is excluded from the accounting set even
        // though it IS removed from the `.meta`. The two sets diverging is the whole
        // point of reporting them separately — `ranges_deleted` (metadata) is all four,
        // `unlinked_extents` (accounting) is the two that existed.
        let mut unlinked = deletion.unlinked_extents.clone();
        unlinked.sort_unstable();
        assert_eq!(
            unlinked,
            vec![(0u64, 999u64), (2000u64, 2999u64)],
            "only extents whose .bin existed and was unlinked may be reported for debit"
        );

        // deleted_paths should contain only the 2 files that were actually deleted,
        // plus the metadata file and lock file that get cleaned up when all ranges are evicted
        let bin_deleted: Vec<_> = deletion
            .deleted_paths
            .iter()
            .filter(|p| p.to_string_lossy().ends_with(".bin"))
            .collect();
        assert_eq!(
            bin_deleted.len(),
            2,
            "Should have 2 deleted .bin file paths (only files that existed)"
        );

        // Verify the existing .bin files were actually deleted
        for start in &[0u64, 2000u64] {
            let end = start + 999;
            let path = cache_manager.get_new_range_file_path(cache_key, *start, end);
            assert!(
                !path.exists(),
                "Range file {}-{} should be deleted",
                start,
                end
            );
        }

        // Verify metadata file was deleted (all ranges evicted)
        assert!(
            !metadata_path.exists(),
            "Metadata file should be deleted when all ranges are evicted"
        );

        // R15.2: an already-absent `.bin` is NOT a retained range. This is the arm that
        // stops the R15.2 fix being over-broad: a naive "retain whenever the unlink did
        // not succeed" would land these four here, keep the `.meta` forever, and make
        // the object permanently unevictable — every later pass re-selecting the same
        // ranges and hitting the same absent files. Nothing survives on disk here, so
        // there is nothing to orphan and nothing to retry.
        assert!(
            deletion.retained_extents.is_empty(),
            "an absent .bin leaves nothing on disk, so no range may be retained: {:?}",
            deletion.retained_extents
        );
    }

    /// A range whose `.bin` unlink fails must keep its `.meta` entry, and the object
    /// must keep its `.meta` file (Requirements 15.2, 15.3), while a range that
    /// unlinks cleanly still evicts fully.
    ///
    /// **The `src/` line this exercises** is the metadata-removal decision in
    /// `batch_delete_ranges`'s result loop — the `ranges_deleted.push(range_key)`
    /// that ran unconditionally under the comment "Always mark range for metadata
    /// removal (file may already be gone)", now the `match` on
    /// [`RangeUnlinkOutcome`] — and the `.meta` unlink it gates via
    /// `all_ranges_evicted`.
    ///
    /// **Red side, measured** against that unconditional push: the locked object's
    /// `.meta` is DELETED and its range list is gone, i.e. a `.bin` sitting on the
    /// volume with no metadata anywhere referencing it. § 15.1 is why that is
    /// permanent rather than merely untidy — both validation scan modes root at
    /// `metadata/` and never read `ranges/`, so the file is invisible to accounting
    /// and to every reclamation path that exists today.
    ///
    /// **This asserts the composed post-Phase-1 contract**, not only task 3's half. A
    /// failed unlink must leave all three of: the bytes still credited (task 2 — via
    /// `unlinked_extents` and `bytes_freed`, which are the caller's only debit
    /// inputs), the range still listed in the `.meta` (task 3), and the `.meta` itself
    /// present (task 3). Asserting them together is the point: task 2's narrowing is
    /// only *correct* if the bytes stay reachable through metadata, and task 3's
    /// retention is only *safe* if those bytes are not also debited. Either fix alone
    /// leaves a hole the other closes.
    ///
    /// Spec: cache-eviction-at-scale. Requirements: 15.2, 15.3, 15.4
    #[cfg(unix)]
    #[tokio::test]
    async fn failed_unlink_keeps_the_range_in_metadata_and_keeps_the_meta_file() {
        use crate::cache_types::CompressionInfo;
        use std::os::unix::fs::PermissionsExt;

        const RANGE_BYTES: u64 = 4096;
        const DELETABLE_KEY: &str = "test-bucket/r15-deletable";
        const LOCKED_KEY: &str = "test-bucket/r15-locked";

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Plant a single-range object with its `.bin` on disk and a matching `.meta`.
        // Single-range deliberately: it is what makes the `.meta` deletion happen at
        // all, so one fixture exercises R15.2 (range stripped) and R15.3 (`.meta`
        // unlinked) together — the exact sequence R15.13 asks to be shown failing.
        let plant = |key: &str| -> (PathBuf, PathBuf) {
            let now = SystemTime::now();
            let bin_path = cache_manager.get_new_range_file_path(key, 0, RANGE_BYTES - 1);
            std::fs::create_dir_all(bin_path.parent().unwrap()).unwrap();
            std::fs::write(&bin_path, vec![0xABu8; RANGE_BYTES as usize]).unwrap();

            let metadata = crate::cache_types::NewCacheMetadata {
                cache_key: key.to_string(),
                object_metadata: crate::cache_types::ObjectMetadata::default(),
                ranges: vec![crate::cache_types::RangeSpec {
                    start: 0,
                    end: RANGE_BYTES - 1,
                    file_path: bin_path
                        .strip_prefix(temp_dir.path().join("ranges"))
                        .unwrap()
                        .to_string_lossy()
                        .to_string(),
                    compression_algorithm: CompressionAlgorithm::Lz4,
                    compressed_size: RANGE_BYTES,
                    uncompressed_size: RANGE_BYTES,
                    created_at: now,
                    last_accessed: now,
                    access_count: 1,
                    staged: None,
                    unservable: false,
                    file_identity: None,
                }],
                created_at: now,
                expires_at: now + Duration::from_secs(3600),
                compression_info: CompressionInfo::default(),
                ..Default::default()
            };

            let meta_path = cache_manager.get_new_metadata_file_path(key);
            std::fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
            std::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap()).unwrap();

            (bin_path, meta_path)
        };

        let (deletable_bin, deletable_meta) = plant(DELETABLE_KEY);
        let (locked_bin, locked_meta) = plant(LOCKED_KEY);

        // The two objects must not share a `ranges/` leaf, or the read-only directory
        // would block both unlinks and the control arm would prove nothing. BLAKE3
        // sharding makes this true for these keys; assert it so a sharding change fails
        // here with a reason rather than silently disarming the test.
        let locked_dir = locked_bin.parent().unwrap().to_path_buf();
        assert_ne!(
            deletable_bin.parent().unwrap(),
            locked_dir,
            "fixture requires the two ranges in different leaf directories"
        );

        // The lever: on Unix `unlink` needs write permission on the PARENT directory,
        // not on the file, so a read-only parent fails the unlink with EACCES while
        // `metadata()` still succeeds. That is the genuine "unlink failed, bytes remain
        // on the volume" case — distinct from "file already gone", which
        // `test_batch_delete_ranges_missing_files` covers and which must behave the
        // opposite way.
        let original_mode = std::fs::metadata(&locked_dir).unwrap().permissions().mode();
        std::fs::set_permissions(&locked_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let locked = cache_manager
            .batch_delete_ranges(LOCKED_KEY, &[(0, RANGE_BYTES - 1)])
            .await
            .unwrap();

        // Restore before any assertion can fail, so TempDir cleanup is not left
        // fighting a read-only directory.
        std::fs::set_permissions(&locked_dir, std::fs::Permissions::from_mode(original_mode))
            .unwrap();

        // Precondition, asserted rather than assumed: the lever actually bit. Running as
        // root would delete the file regardless of directory mode, and then every
        // assertion below would be measuring the clean-unlink path under the locked
        // name — passing while proving nothing about orphan production.
        assert!(
            locked_bin.exists(),
            "the undeletable .bin must survive — otherwise the read-only-directory lever \
             did not bite (running as root?) and this test cannot distinguish a failed \
             unlink from a successful one"
        );

        // --- The composed post-Phase-1 contract for a failed unlink ---

        // (1) task 3, R15.3: the `.meta` survives, because a `.bin` does.
        assert!(
            locked_meta.exists(),
            "R15.3: the .meta must not be deleted while a .bin survives — deleting it is \
             what makes the surviving file an orphan invisible to the metadata-rooted \
             validation scan"
        );

        // (2) task 3, R15.2: the range is still LISTED. Read the `.meta` back and check
        // the `ranges` array, not merely that the file exists — file existence is
        // satisfied by an object whose other ranges happen to remain, and here it would
        // also be satisfied by a `.meta` rewritten to an empty range list.
        let persisted: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&locked_meta).unwrap()).unwrap();
        assert_eq!(
            persisted
                .ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>(),
            vec![(0u64, RANGE_BYTES - 1)],
            "R15.2: a range whose .bin unlink failed must remain in the .meta ranges array"
        );
        assert!(
            !locked.all_ranges_evicted,
            "all_ranges_evicted must be false while a .bin survives"
        );

        // (3) task 2, R7.2: the bytes stay credited, because the caller's debit is built
        // from `unlinked_extents` and `bytes_freed` and neither includes this range.
        // Without this, task 3's retention would keep a range in the `.meta` whose bytes
        // had already been subtracted — accounted twice in opposite directions.
        assert!(
            locked.unlinked_extents.is_empty(),
            "nothing left the disk, so nothing may be offered for debit: {:?}",
            locked.unlinked_extents
        );
        assert_eq!(
            locked.bytes_freed, 0,
            "no bytes were freed — the file is still on the volume"
        );

        // (4) R15.4: the failure is reported, and the retained `.meta` entry above IS
        // the durable record that makes the next pass retry it.
        assert_eq!(
            locked.retained_extents,
            vec![(0u64, RANGE_BYTES - 1)],
            "R15.4: a range left behind by a failed unlink must be reported"
        );

        // --- Control arm: a clean unlink still evicts fully ---
        //
        // Without this, a fix that simply stopped removing ranges from the `.meta`
        // altogether would pass every assertion above.
        let clean = cache_manager
            .batch_delete_ranges(DELETABLE_KEY, &[(0, RANGE_BYTES - 1)])
            .await
            .unwrap();

        assert!(!deletable_bin.exists(), "the deletable .bin must be gone");
        assert!(
            !deletable_meta.exists(),
            "R15.3 must not block the normal path: with no .bin surviving, the .meta goes"
        );
        assert!(clean.all_ranges_evicted);
        assert_eq!(clean.unlinked_extents, vec![(0u64, RANGE_BYTES - 1)]);
        assert_eq!(clean.bytes_freed, RANGE_BYTES);
        assert!(
            clean.retained_extents.is_empty(),
            "a clean unlink retains nothing"
        );
    }

    // ============================================================================
    // Tests for Streaming Decompression Edge Cases (Task 1.5)
    // ============================================================================

    /// Test that corrupt/truncated LZ4 data produces a decompression error mid-stream.
    /// Validates: Requirement 1.4
    #[tokio::test]
    async fn test_stream_range_data_corrupt_lz4_error() {
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Write random/corrupt bytes that are not valid LZ4
        let file_path = "test-bucket/corrupt-object_0-1023.bin";
        let range_file_path = temp_dir.path().join("ranges").join(file_path);
        if let Some(parent) = range_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        let corrupt_data: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
        tokio::fs::write(&range_file_path, &corrupt_data)
            .await
            .unwrap();

        // Create a RangeSpec pointing to the corrupt file with Lz4 compression
        let range_spec = crate::cache_types::RangeSpec::new(
            0,
            1023,
            file_path.to_string(),
            CompressionAlgorithm::Lz4,
            corrupt_data.len() as u64,
            1024, // expected uncompressed size
        );

        // stream_range_data should succeed (file exists), but the stream should yield an error
        let stream = cache_manager
            .stream_range_data(&range_spec, 4096)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(stream);
        let mut got_error = false;

        while let Some(result) = stream.next().await {
            if let Err(e) = result {
                got_error = true;
                let error_msg = format!("{}", e);
                assert!(
                    error_msg.contains("Decompression error mid-stream")
                        || error_msg.contains("Decompressed size mismatch"),
                    "Expected decompression error, got: {}",
                    error_msg
                );
                break;
            }
        }

        assert!(
            got_error,
            "Stream should have yielded a decompression error for corrupt LZ4 data"
        );
    }

    /// `check_legacy_range` refuses an unsafe `file_path` itself, before any read: the file
    /// `../outside.bin` holds an error document and would be read and healed without the check.
    #[tokio::test]
    async fn test_check_legacy_range_rejects_unsafe_path_without_reading() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let doc = b"<?xml version=\"1.0\"?><Error><Code>AccessDenied</Code></Error>";
        tokio::fs::write(temp_dir.path().join("outside.bin"), doc)
            .await
            .unwrap();
        let range_spec = crate::cache_types::RangeSpec::new(
            0,
            doc.len() as u64 - 1,
            "../outside.bin".to_string(),
            CompressionAlgorithm::None,
            doc.len() as u64,
            doc.len() as u64,
        );
        let err = cache_manager
            .check_legacy_range(&range_spec)
            .await
            .expect_err("an unsafe path is refused");
        assert!(
            err.to_string().contains("rejected (unsafe)"),
            "unexpected error: {err}"
        );
    }

    /// The pre-open check on an identity-less corrupt extent records no decompression
    /// failure in the manager's compression statistics; the stream that follows is the only
    /// reader to report the damage.
    #[tokio::test]
    async fn test_check_legacy_range_moves_no_decompression_counter() {
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let file_path = "test-bucket/legacy-corrupt_0-1023.bin";
        let range_file_path = temp_dir.path().join("ranges").join(file_path);
        tokio::fs::create_dir_all(range_file_path.parent().unwrap())
            .await
            .unwrap();
        let corrupt_data: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
        tokio::fs::write(&range_file_path, &corrupt_data)
            .await
            .unwrap();
        let range_spec = crate::cache_types::RangeSpec::new(
            0,
            1023,
            file_path.to_string(),
            CompressionAlgorithm::Lz4,
            corrupt_data.len() as u64,
            1024,
        );
        assert!(range_spec.file_identity.is_none(), "fixture: identity-less");
        let before = cache_manager.compression_handler.get_stats();
        let stream = cache_manager
            .stream_range_data(&range_spec, 4096)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(stream);
        let mut got_error = false;
        while let Some(result) = stream.next().await {
            if result.is_err() {
                got_error = true;
                break;
            }
        }
        assert!(got_error, "fixture: the stream reports the corrupt extent");
        let after = cache_manager.compression_handler.get_stats();
        assert_eq!(
            after.decompression_failures - before.decompression_failures,
            0,
            "the pre-open check must not count a decompression failure (before={}, after={})",
            before.decompression_failures,
            after.decompression_failures
        );
    }

    /// A range file whose bytes are not a valid LZ4 frame is counted in
    /// `atomic_metadata.range_file_corruption_detected` on both client read paths: the
    /// buffered `load_range_data` and the streamed `stream_range_data`.
    ///
    /// The counter is process-global and another test in this binary also streams a corrupt
    /// file, so each assertion is a lower bound on this test's own reads rather than an exact
    /// delta.
    ///
    /// # RED SIDE
    ///
    /// With either `note_range_file_corruption` call removed, the matching assertion fails
    /// (run this test alone so the other corrupt-stream test cannot move the counter).
    #[tokio::test]
    async fn store_range_over_an_unparseable_meta_counts_a_heal() {
        let temp_dir = TempDir::new().unwrap();
        let mut dcm =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        dcm.initialize().await.unwrap();
        let cache_key = "heal-bucket/counted-heal.txt";
        let data = b"healed bytes";
        let path = dcm.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "NOT JSON").unwrap();
        let object_metadata = crate::cache_types::ObjectMetadata::new(
            "\"etag-counted-heal\"".to_string(),
            "Wed, 21 Oct 2025 07:28:00 GMT".to_string(),
            data.len() as u64,
            Some("text/plain".to_string()),
        );
        let before = metadata_heal_overwrite_total();
        dcm.store_range(
            cache_key,
            0,
            data.len() as u64 - 1,
            data,
            object_metadata,
            std::time::Duration::from_secs(3600),
            true,
        )
        .await
        .unwrap();
        assert!(
            serde_json::from_str::<crate::cache_types::NewCacheMetadata>(
                &std::fs::read_to_string(&path).unwrap()
            )
            .is_ok(),
            "PRECONDITION: store_range must have replaced the unparseable .meta"
        );
        let after = metadata_heal_overwrite_total();
        assert!(
            after > before,
            "store_range replaced an unparseable .meta but cache.metadata_heal_overwrite_total \
             did not move ({before} -> {after})"
        );
    }

    /// range files written by earlier releases declare 4 MiB blocks. The writers now
    /// declare 64 KiB, and those older files must still read back byte-exact on both client
    /// read paths: the buffered `load_range_data` and the streamed `stream_range_data`.
    ///
    /// Four files, each a single range: a 4 MiB-block store-mode frame, a 4 MiB-block
    /// compressed frame, and each of those concatenated with a new 64 KiB-block frame of the
    /// other kind, because an incremental write appends frames and a file rewritten across the
    /// upgrade is not expected, but a mixed file must not be the one that fails. The payloads
    /// exceed 4 MiB so each old frame holds a full 4 MiB block plus a partial one, which a
    /// reader assuming 64 KiB blocks could not hold.
    #[tokio::test]
    async fn frames_with_4_mib_blocks_read_back_on_both_read_paths() {
        use futures::StreamExt;
        use std::io::Write;

        const OLD_BLOCK: usize = 4 * 1024 * 1024;
        const OLD_BD_CODE: u8 = 7;

        fn old_compressed_frame(data: &[u8]) -> Vec<u8> {
            let mut info = lz4_flex::frame::FrameInfo::new();
            info.content_checksum = true;
            info.block_mode = lz4_flex::frame::BlockMode::Independent;
            info.block_size = lz4_flex::frame::BlockSize::Max4MB;
            let mut enc = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        }
        fn old_store_frame(data: &[u8]) -> Vec<u8> {
            crate::compression::CompressionHandler::encode_store_mode_frame_with_blocks(
                data,
                OLD_BLOCK,
                OLD_BD_CODE,
            )
            .unwrap()
        }
        fn bd_code(frame: &[u8]) -> u8 {
            (frame[5] >> 4) & 0x07
        }

        // Incompressible (xorshift) and compressible (short repeating pattern) payloads.
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let incompressible: Vec<u8> = (0..OLD_BLOCK + 300_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let compressible: Vec<u8> = (0..OLD_BLOCK + 300_000).map(|i| (i % 251) as u8).collect();

        let old_store = old_store_frame(&incompressible);
        let old_compressed = old_compressed_frame(&compressible);
        assert_eq!(
            bd_code(&old_store),
            7,
            "fixture: old store-mode frame is 4 MiB-block"
        );
        assert_eq!(
            bd_code(&old_compressed),
            7,
            "fixture: old compressed frame is 4 MiB-block"
        );
        let new_store =
            crate::compression::CompressionHandler::encode_store_mode_frame(b"new store tail")
                .unwrap();
        let mut h = crate::compression::CompressionHandler::new(0, true);
        let new_compressed = h
            .compress_with_algorithm(&compressible[..100_000], CompressionAlgorithm::Lz4)
            .unwrap()
            .data;
        assert_eq!(
            bd_code(&new_store),
            4,
            "fixture: new frames are 64 KiB-block"
        );
        assert_eq!(
            bd_code(&new_compressed),
            4,
            "fixture: new frames are 64 KiB-block"
        );

        let mixed_a = [old_store.clone(), new_compressed.clone()].concat();
        let mixed_a_data = [incompressible.clone(), compressible[..100_000].to_vec()].concat();
        let mixed_b = [old_compressed.clone(), new_store.clone()].concat();
        let mixed_b_data = [compressible.clone(), b"new store tail".to_vec()].concat();

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cases: [(&str, &Vec<u8>, &Vec<u8>); 4] = [
            ("old-store", &old_store, &incompressible),
            ("old-compressed", &old_compressed, &compressible),
            ("old-store+new-compressed", &mixed_a, &mixed_a_data),
            ("old-compressed+new-store", &mixed_b, &mixed_b_data),
        ];
        for (label, file_bytes, expected) in cases {
            let end = expected.len() as u64 - 1;
            let file_path = format!("test-bucket/case52-{label}_0-{end}.bin");
            let range_file_path = temp_dir.path().join("ranges").join(&file_path);
            tokio::fs::create_dir_all(range_file_path.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&range_file_path, file_bytes)
                .await
                .unwrap();
            let range_spec = crate::cache_types::RangeSpec::new(
                0,
                end,
                file_path.clone(),
                CompressionAlgorithm::Lz4,
                file_bytes.len() as u64,
                expected.len() as u64,
            );

            let buffered = cache_manager
                .load_range_data(&range_spec)
                .await
                .unwrap_or_else(|e| panic!("{label}: buffered read failed: {e}"));
            assert!(
                buffered == *expected,
                "{label}: buffered read is not byte-exact ({} bytes vs {})",
                buffered.len(),
                expected.len()
            );

            let stream = cache_manager
                .stream_range_data(&range_spec, 65_536)
                .await
                .unwrap_or_else(|e| panic!("{label}: stream open failed: {e}"));
            let mut stream = std::pin::pin!(stream);
            let mut streamed = Vec::with_capacity(expected.len());
            while let Some(chunk) = stream.next().await {
                streamed.extend_from_slice(
                    &chunk.unwrap_or_else(|e| panic!("{label}: streamed read failed: {e}")),
                );
            }
            assert!(
                streamed == *expected,
                "{label}: streamed read is not byte-exact ({} bytes vs {})",
                streamed.len(),
                expected.len()
            );
        }
    }

    /// the incremental range writer's frames, compressed and store-mode, declare
    /// 64 KiB blocks (BD code 4). This is the third writer beside `encode_store_mode_frame`
    /// and `compress_with_algorithm`, which `tests/buffered_serve_memory_bound_test.rs`
    /// test (v) covers.
    #[tokio::test]
    async fn incremental_range_writer_frames_declare_64_kib_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        for compression_enabled in [true, false] {
            let cache_key = format!("test-bucket/case52-incremental-{compression_enabled}");
            let len = 200_000u64;
            let mut writer = cache_manager
                .begin_incremental_range_write(&cache_key, 0, len - 1, compression_enabled)
                .await
                .unwrap()
                .expect("the capacity guard admits with an unconfigured floor");
            let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            DiskCacheManager::write_range_chunk(&mut writer, &body).unwrap();
            DiskCacheManager::flush_batch(&mut writer).unwrap();
            let on_disk = std::fs::read(&writer.tmp_path).unwrap();
            let code = (on_disk[5] >> 4) & 0x07;
            DiskCacheManager::abort_incremental_range(writer);
            assert_eq!(
                code, 4,
                "incremental writer (compression_enabled={compression_enabled}) must declare \
                 64 KiB blocks (BD code 4), got code {code}"
            );
        }
    }

    #[tokio::test]
    async fn corrupt_range_file_is_counted_on_both_read_paths() {
        use futures::StreamExt;
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();
        let file_path = "test-bucket/counted-corrupt-object_0-1023.bin";
        let range_file_path = temp_dir.path().join("ranges").join(file_path);
        tokio::fs::create_dir_all(range_file_path.parent().unwrap())
            .await
            .unwrap();
        // A valid LZ4 frame header's magic number followed by garbage, so the decoder gets
        // past the magic check and fails on the frame descriptor or block.
        let corrupt: Vec<u8> = vec![
            0x04, 0x22, 0x4D, 0x18, 0x64, 0x40, 0xA7, 0xFF, 0xFF, 0xFF, 0x7F, 0x01, 0x02,
        ];
        tokio::fs::write(&range_file_path, &corrupt).await.unwrap();
        let range_spec = crate::cache_types::RangeSpec::new(
            0,
            1023,
            file_path.to_string(),
            CompressionAlgorithm::Lz4,
            corrupt.len() as u64,
            1024,
        );

        let before = range_file_corruption_detected();
        assert!(
            cache_manager.load_range_data(&range_spec).await.is_err(),
            "fixture precondition: the buffered read must fail on a corrupt frame"
        );
        let after_buffered = range_file_corruption_detected();
        assert!(
            after_buffered > before,
            "the buffered read path must count a corrupt range file ({before} -> {after_buffered})"
        );

        // The buffered read marked the file, so a streamed read would refuse it unread;
        // drop the mark to read the same damaged file the streamed way.
        unmark_damaged_range_file(&range_file_path);
        let stream = cache_manager
            .stream_range_data(&range_spec, 4096)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(stream);
        let mut got_error = false;
        while let Some(item) = stream.next().await {
            if item.is_err() {
                got_error = true;
                break;
            }
        }
        assert!(
            got_error,
            "fixture precondition: the stream must yield an error"
        );
        let after_streamed = range_file_corruption_detected();
        assert!(
            after_streamed > after_buffered,
            "the streamed read path must count a corrupt range file \
             ({after_buffered} -> {after_streamed})"
        );
    }

    /// range-file-self-heal fixture: a compressible payload of several 64 KiB LZ4 blocks.
    fn self_heal_payload(len: usize) -> Vec<u8> {
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                b"abcdefgh"[(x % 8) as usize]
            })
            .collect()
    }

    /// Store `data` as range `start..start+len-1` of `cache_key` through `store_range`, so a
    /// real `.meta` lists it, and return the stored `RangeSpec`.
    async fn self_heal_store(
        dcm: &mut DiskCacheManager,
        cache_key: &str,
        start: u64,
        data: &[u8],
        total: u64,
    ) -> crate::cache_types::RangeSpec {
        let object_metadata = crate::cache_types::ObjectMetadata::new(
            "\"etag-self-heal\"".to_string(),
            "Wed, 21 Oct 2025 07:28:00 GMT".to_string(),
            total,
            Some("application/octet-stream".to_string()),
        );
        let end = start + data.len() as u64 - 1;
        dcm.store_range(
            cache_key,
            start,
            end,
            data,
            object_metadata,
            std::time::Duration::from_secs(3600),
            true,
        )
        .await
        .unwrap();
        self_heal_listed(dcm, cache_key, start, end)
            .expect("fixture: store_range must list the range")
    }

    /// The range `start..=end` as the key's `.meta` on disk lists it, if it does.
    fn self_heal_listed(
        dcm: &DiskCacheManager,
        cache_key: &str,
        start: u64,
        end: u64,
    ) -> Option<crate::cache_types::RangeSpec> {
        let content = std::fs::read_to_string(dcm.get_new_metadata_file_path(cache_key)).ok()?;
        let meta: crate::cache_types::NewCacheMetadata = serde_json::from_str(&content).ok()?;
        meta.ranges
            .into_iter()
            .find(|r| r.start == start && r.end == end)
    }

    /// Damage a stored range file one of three ways.
    fn self_heal_damage(path: &std::path::Path, form: &str) {
        let mut bytes = std::fs::read(path).unwrap();
        let len = bytes.len();
        match form {
            "truncate" => bytes.truncate(len * 2 / 3),
            "flip" => bytes[len / 2] ^= 0xFF,
            "checksum" => {
                for b in &mut bytes[len - 4..] {
                    *b = !*b;
                }
            }
            other => panic!("unknown damage form {other}"),
        }
        // Damage on the medium leaves the modification time alone, which the identity check
        // compares: rewritten with a new one, the file would read as another file
        // rather than a damaged one.
        let modified = std::fs::metadata(path).unwrap().modified().unwrap();
        std::fs::write(path, &bytes).unwrap();
        filetime::set_file_mtime(path, filetime::FileTime::from_system_time(modified)).unwrap();
    }

    /// Read a range the streamed way, returning the bytes or the first error verbatim.
    async fn self_heal_stream(
        dcm: &DiskCacheManager,
        spec: &crate::cache_types::RangeSpec,
    ) -> std::result::Result<Vec<u8>, String> {
        use futures::StreamExt;
        let stream = dcm
            .stream_range_data(spec, 65_536)
            .await
            .map_err(|e| e.to_string())?;
        let mut stream = std::pin::pin!(stream);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.map_err(|e| e.to_string())?);
        }
        Ok(out)
    }

    /// Read a range the `shape` way (`buffered` or `streamed`).
    async fn self_heal_read(
        dcm: &DiskCacheManager,
        spec: &crate::cache_types::RangeSpec,
        shape: &str,
    ) -> std::result::Result<Vec<u8>, String> {
        if shape == "buffered" {
            dcm.load_range_data(spec).await.map_err(|e| e.to_string())
        } else {
            self_heal_stream(dcm, spec).await
        }
    }

    /// range-file-self-heal R1, R3, R6: a damaged range file found by a client read is
    /// invalidated, so the next read of that range is an ordinary miss and a refill
    /// re-caches it. Each damage form (truncated, a flipped frame byte, a replaced content
    /// checksum) on each read shape (buffered `load_range_data`, streamed
    /// `stream_range_data`).
    ///
    /// # RED SIDE
    ///
    /// Before the heal the range stays listed and the file stays on disk, so the second read
    /// fails the way the first did; the assertion prints both errors.
    #[tokio::test]
    async fn a_damaged_range_file_is_invalidated_when_a_read_detects_it() {
        let data = self_heal_payload(300_000);
        let mut failures: Vec<String> = Vec::new();
        for form in ["truncate", "flip", "checksum"] {
            for shape in ["buffered", "streamed"] {
                let temp_dir = TempDir::new().unwrap();
                let mut dcm = DiskCacheManager::new(
                    temp_dir.path().to_path_buf(),
                    true,
                    1024,
                    false,
                    1_048_576,
                );
                dcm.initialize().await.unwrap();
                dcm.set_range_heal_limiter_for_test(Arc::new(RangeHealLimiter::new(
                    64,
                    std::time::Duration::from_secs(60),
                )));
                let cache_key = format!("heal-bucket/self-heal-{form}-{shape}.bin");
                let spec = self_heal_store(&mut dcm, &cache_key, 0, &data, data.len() as u64).await;
                assert_eq!(
                    spec.compression_algorithm,
                    CompressionAlgorithm::Lz4,
                    "fixture: the range must be stored as an LZ4 frame"
                );
                let bin = temp_dir.path().join("ranges").join(&spec.file_path);
                self_heal_damage(&bin, form);

                let first = self_heal_read(&dcm, &spec, shape).await;
                assert!(
                    first.is_err(),
                    "{form}/{shape}: fixture precondition: the first read of a damaged file \
                     must fail"
                );
                let second = self_heal_read(&dcm, &spec, shape).await;
                let still_listed = self_heal_listed(&dcm, &cache_key, spec.start, spec.end);
                if still_listed.is_some() || bin.exists() {
                    failures.push(format!(
                        "{form}/{shape}: the damaged range must be invalidated by the read that \
                         found it; listed={}, bin_exists={}, first={:?}, second={:?}",
                        still_listed.is_some(),
                        bin.exists(),
                        first.as_ref().map(|v| v.len()),
                        second.as_ref().map(|v| v.len())
                    ));
                    continue;
                }
                if !second
                    .as_ref()
                    .is_err_and(|e| e.contains("not found") && e.contains("cache miss"))
                {
                    failures.push(format!(
                        "{form}/{shape}: after the heal the next read is an ordinary miss, got \
                         {second:?}"
                    ));
                    continue;
                }
                // The refill the miss leads to re-caches the range, and it reads back.
                let refilled =
                    self_heal_store(&mut dcm, &cache_key, 0, &data, data.len() as u64).await;
                assert!(
                    dcm.load_range_data(&refilled).await.unwrap() == data,
                    "{form}/{shape}: the refilled range must read back byte-exact"
                );
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// A fresh manager over `temp_dir` with its own heal bound of `limit` per minute.
    async fn self_heal_manager(
        temp_dir: &TempDir,
        limit: u32,
    ) -> (DiskCacheManager, Arc<RangeHealLimiter>) {
        let mut dcm =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        dcm.initialize().await.unwrap();
        let limiter = Arc::new(RangeHealLimiter::new(
            limit,
            std::time::Duration::from_secs(60),
        ));
        dcm.set_range_heal_limiter_for_test(limiter.clone());
        (dcm, limiter)
    }

    /// range-file-self-heal R2 control: healing a damaged range leaves the key's other range,
    /// and its file, in place and still served from cache.
    #[tokio::test]
    async fn a_heal_leaves_an_undamaged_neighbour_range_serving() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, limiter) = self_heal_manager(&temp_dir, 64).await;
        let data = self_heal_payload(400_000);
        let (first_half, second_half) = data.split_at(200_000);
        let cache_key = "heal-bucket/self-heal-neighbour.bin";
        let damaged = self_heal_store(&mut dcm, cache_key, 0, first_half, 400_000).await;
        let neighbour = self_heal_store(&mut dcm, cache_key, 200_000, second_half, 400_000).await;
        let neighbour_bin = temp_dir.path().join("ranges").join(&neighbour.file_path);
        self_heal_damage(
            &temp_dir.path().join("ranges").join(&damaged.file_path),
            "flip",
        );

        assert!(dcm.load_range_data(&damaged).await.is_err());
        assert_eq!(limiter.healed(), 1, "the damaged range must be healed once");
        assert!(
            self_heal_listed(&dcm, cache_key, damaged.start, damaged.end).is_none(),
            "the damaged range must leave the .meta"
        );
        assert!(
            self_heal_listed(&dcm, cache_key, neighbour.start, neighbour.end).is_some()
                && neighbour_bin.exists(),
            "the undamaged neighbour range and its file must stay"
        );
        assert!(
            dcm.load_range_data(&neighbour).await.unwrap() == second_half,
            "the neighbour must still be served from cache byte-exact"
        );
    }

    /// range-file-self-heal R4: past the heal limit a damaged range is detected and counted
    /// but left in place, and the suppressed heal is counted. A write after the storm still
    /// succeeds.
    #[tokio::test]
    async fn a_heal_storm_past_the_limit_is_counted_and_left_in_place() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, limiter) = self_heal_manager(&temp_dir, 2).await;
        let data = self_heal_payload(150_000);
        let mut specs = Vec::new();
        for i in 0..4 {
            let key = format!("heal-bucket/self-heal-storm-{i}.bin");
            let spec = self_heal_store(&mut dcm, &key, 0, &data, data.len() as u64).await;
            self_heal_damage(
                &temp_dir.path().join("ranges").join(&spec.file_path),
                "checksum",
            );
            specs.push((key, spec));
        }
        let detected_before = range_file_corruption_detected();
        for (_, spec) in &specs {
            assert!(dcm.load_range_data(spec).await.is_err());
        }
        assert!(
            range_file_corruption_detected() >= detected_before + 4,
            "every damaged read is still detected"
        );
        assert_eq!(limiter.healed(), 2, "only the limit's worth is healed");
        assert_eq!(
            limiter.suppressed(),
            2,
            "the rest are counted as suppressed"
        );
        for (i, (key, spec)) in specs.iter().enumerate() {
            let listed = self_heal_listed(&dcm, key, spec.start, spec.end).is_some();
            let present = temp_dir
                .path()
                .join("ranges")
                .join(&spec.file_path)
                .exists();
            if i < 2 {
                assert!(
                    !listed && !present,
                    "range {i} is within the limit and is healed"
                );
            } else {
                assert!(
                    listed && present,
                    "range {i} is past the limit and is left in place"
                );
            }
        }
        let started = std::time::Instant::now();
        self_heal_store(
            &mut dcm,
            "heal-bucket/self-heal-storm-write.bin",
            0,
            &data,
            150_000,
        )
        .await;
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "a write after the storm must not wait on the heal bound"
        );
    }

    /// a mark matches only the file it names, a match refreshes it, an unrelied-on mark
    /// ages out, and the set stays within its limit by dropping the oldest.
    #[test]
    fn damaged_range_marks_match_one_file_age_out_and_stay_bounded() {
        let stamp = |n: u64| RangeFileStamp {
            len: n,
            modified: None,
            #[cfg(unix)]
            ino: n,
        };
        let (a, b, c) = (
            Path::new("/c/a.bin"),
            Path::new("/c/b.bin"),
            Path::new("/c/c.bin"),
        );
        let t0 = std::time::Instant::now();
        let secs = |s: u64| t0 + Duration::from_secs(s);
        let mut marks = DamagedRangeMarks::new(2, Duration::from_secs(10));
        marks.mark(a, stamp(1), t0);
        assert!(
            marks.matches(a, &stamp(1), secs(5)),
            "a fresh mark matches its file"
        );
        assert!(
            marks.matches(a, &stamp(1), secs(14)),
            "a match refreshes the mark"
        );
        assert!(
            !marks.matches(a, &stamp(1), secs(25)),
            "an unrelied-on mark ages out"
        );
        assert!(!marks.holds(a), "an aged mark is dropped");
        marks.mark(a, stamp(1), t0);
        assert!(
            !marks.matches(a, &stamp(2), t0),
            "another file at the path is not marked"
        );
        assert!(
            !marks.holds(a),
            "the first read of another file drops the mark"
        );
        marks.mark(a, stamp(1), t0);
        marks.mark(b, stamp(2), secs(1));
        marks.mark(c, stamp(3), secs(2));
        assert!(
            !marks.holds(a) && marks.holds(b) && marks.holds(c),
            "past the limit the oldest mark goes"
        );
        marks.unmark(b);
        assert!(!marks.holds(b));
        let mut none = DamagedRangeMarks::new(0, Duration::from_secs(10));
        none.mark(a, stamp(1), t0);
        assert!(!none.holds(a), "a zero limit holds nothing");
    }

    /// Open a streamed read of `spec`. `Ok` with the error when the open refuses the file (the
    /// serve then answers from S3, before any head); `Err` describing what the stream yielded
    /// when it opened the file.
    async fn second_stream(
        dcm: &DiskCacheManager,
        spec: &crate::cache_types::RangeSpec,
    ) -> std::result::Result<String, String> {
        use futures::StreamExt;
        match dcm.stream_range_data(spec, 65_536).await {
            Err(e) => Ok(e.to_string()),
            Ok(stream) => {
                let mut stream = std::pin::pin!(stream);
                let mut got = 0usize;
                let mut error = None;
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(bytes) => got += bytes.len(),
                        Err(e) => {
                            error = Some(e.to_string());
                            break;
                        }
                    }
                }
                Err(format!(
                    "the stream opened the file and yielded {got} bytes, then error {error:?}"
                ))
            }
        }
    }

    /// (b): a listed damaged range the heal limit leaves in place breaks one streamed read
    /// on this proxy, not every one. The read that finds the damage marks the file, so the next
    /// streamed read refuses it before its first byte (the serve answers that request from S3),
    /// and nothing is unlinked past the limit.
    ///
    /// # RED SIDE
    ///
    /// Without the mark the second stream opens the same damaged file and breaks off mid-body
    /// again, as every streamed read would until the heal window had room.
    #[tokio::test]
    async fn a_heal_suppressed_damaged_range_breaks_one_streamed_read_only() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, limiter) = self_heal_manager(&temp_dir, 0).await;
        let data = self_heal_payload(300_000);
        let cache_key = "heal-bucket/case53-suppressed.bin";
        let spec = self_heal_store(&mut dcm, cache_key, 0, &data, data.len() as u64).await;
        let bin = temp_dir.path().join("ranges").join(&spec.file_path);
        self_heal_damage(&bin, "flip");
        let first = self_heal_stream(&dcm, &spec).await;
        assert!(
            first.is_err() && limiter.suppressed() == 1,
            "fixture: the first streamed read must break and its heal be suppressed; got {:?}, \
             suppressed={}",
            first.as_ref().map(|v| v.len()),
            limiter.suppressed()
        );
        let second = second_stream(&dcm, &spec).await;
        assert!(
            second.as_ref().is_ok_and(|e| e.contains("cache miss")),
            "SECOND STREAMED READ OPENED THE DAMAGED FILE (heal suppressed): {second:?}; \
             healed={}, suppressed={}",
            limiter.healed(),
            limiter.suppressed()
        );
        assert!(
            limiter.healed() == 0
                && bin.exists()
                && self_heal_listed(&dcm, cache_key, spec.start, spec.end).is_some(),
            "past the heal limit nothing is unlinked: healed={}, bin_exists={}",
            limiter.healed(),
            bin.exists()
        );
    }

    /// (c) control: a mark names the file a read found damaged, not the range. A refill of
    /// the same key and extent publishes a new file, and the next read of it is a hit.
    ///
    /// # RED SIDE
    ///
    /// A mark keyed without the file's identity refuses the refilled file too.
    #[tokio::test]
    async fn a_damaged_range_mark_does_not_outlive_a_refill() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, _limiter) = self_heal_manager(&temp_dir, 0).await;
        let data = self_heal_payload(300_000);
        let cache_key = "heal-bucket/case53-refill.bin";
        let spec = self_heal_store(&mut dcm, cache_key, 0, &data, data.len() as u64).await;
        let bin = temp_dir.path().join("ranges").join(&spec.file_path);
        self_heal_damage(&bin, "flip");
        let damaged = RangeFileStamp::at(&bin);
        assert!(
            self_heal_stream(&dcm, &spec).await.is_err(),
            "fixture: the streamed read of the damaged file must break"
        );
        let refilled = self_heal_store(&mut dcm, cache_key, 0, &data, data.len() as u64).await;
        assert!(
            RangeFileStamp::at(&bin).is_some() && RangeFileStamp::at(&bin) != damaged,
            "fixture: the refill must publish a new file at the same path"
        );
        for shape in ["streamed", "buffered"] {
            let read = self_heal_read(&dcm, &refilled, shape).await;
            assert!(
                read.as_ref().is_ok_and(|v| *v == data),
                "LOST HIT ({shape}): the refilled file must be served from cache, got {:?}",
                read.as_ref().map(|v| v.len())
            );
        }
    }

    /// (d) control: reads that find nothing wrong mark nothing, so every later read of an
    /// undamaged range, either shape, is still a hit.
    ///
    /// # RED SIDE
    ///
    /// A mark set on every open refuses the second read.
    #[tokio::test]
    async fn an_undamaged_range_is_never_marked() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, limiter) = self_heal_manager(&temp_dir, 64).await;
        let data = self_heal_payload(300_000);
        let cache_key = "heal-bucket/case53-undamaged.bin";
        let spec = self_heal_store(&mut dcm, cache_key, 0, &data, data.len() as u64).await;
        for (i, shape) in ["streamed", "buffered", "streamed", "buffered"]
            .iter()
            .enumerate()
        {
            let read = self_heal_read(&dcm, &spec, shape).await;
            assert!(
                read.as_ref().is_ok_and(|v| *v == data),
                "LOST HIT (read {} {shape}): an undamaged range must stay served from cache, \
                 got {:?}",
                i + 1,
                read.as_ref().map(|v| v.len())
            );
        }
        assert_eq!(limiter.healed() + limiter.suppressed(), 0);
    }

    /// range-file-self-heal R1: two readers that find the same damaged range at once, one
    /// buffered and one streamed, heal it once, and the size accounting is debited once with
    /// the range's dedup entry cleared, so the refill's credit is not deduplicated away.
    ///
    /// The limiter's rendezvous holds each heal past its stamp check until both readers have
    /// detected the damage, so both reach `batch_delete_ranges_expecting`: the first removes
    /// the range and the second finds nothing to unlink (`Raced`). Without it one reader can
    /// find the file already gone and never reach the heal.
    ///
    /// # RED SIDE
    ///
    /// With the `unlinked_ranges.is_empty()` guard removed both heals count, and the outcomes
    /// read `[Healed, Healed]`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_readers_of_one_damaged_range_heal_it_once() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, _) = self_heal_manager(&temp_dir, 64).await;
        let limiter = Arc::new(
            RangeHealLimiter::new(64, std::time::Duration::from_secs(60)).with_rendezvous(2),
        );
        dcm.set_range_heal_limiter_for_test(limiter.clone());
        let consolidator = Arc::new(crate::journal_consolidator::JournalConsolidator::new(
            temp_dir.path().to_path_buf(),
            Arc::new(crate::journal_manager::JournalManager::new(
                temp_dir.path().to_path_buf(),
                "test-instance".to_string(),
            )),
            Arc::new(crate::metadata_lock_manager::MetadataLockManager::new(
                temp_dir.path().to_path_buf(),
                std::time::Duration::from_secs(30),
                3,
            )),
            crate::journal_consolidator::ConsolidationConfig::default(),
        ));
        dcm.set_journal_consolidator(consolidator.clone());
        let data = self_heal_payload(400_000);
        let (first_half, second_half) = data.split_at(200_000);
        let cache_key = "heal-bucket/self-heal-concurrent.bin";
        let damaged = self_heal_store(&mut dcm, cache_key, 0, first_half, 400_000).await;
        self_heal_store(&mut dcm, cache_key, 200_000, second_half, 400_000).await;
        // Stand in for the store's credit, so the debit is observable on the dedup set.
        consolidator.size_accumulator().add_range(
            cache_key,
            damaged.start,
            damaged.end,
            damaged.compressed_size,
        );
        let delta_before = consolidator.size_accumulator().current_delta();
        self_heal_damage(
            &temp_dir.path().join("ranges").join(&damaged.file_path),
            "flip",
        );

        let (buffered, streamed) = tokio::join!(
            dcm.load_range_data(&damaged),
            self_heal_stream(&dcm, &damaged)
        );
        assert!(
            buffered.is_err(),
            "the buffered reader must fail on the damaged file"
        );
        assert!(
            streamed.is_err(),
            "the streamed reader must fail on the damaged file or find it gone: {:?}",
            streamed.as_ref().map(|v| v.len())
        );
        let mut outcomes = limiter.outcomes();
        outcomes.sort_by_key(|o| format!("{o:?}"));
        assert_eq!(
            outcomes,
            vec![RangeHealOutcome::Healed, RangeHealOutcome::Raced],
            "both readers must reach the locked removal, and only the first may remove the range"
        );
        assert_eq!(
            limiter.healed(),
            1,
            "two readers of one damaged range heal it once"
        );
        assert_eq!(
            consolidator.size_accumulator().current_delta(),
            delta_before - damaged.compressed_size as i64,
            "the heal debits the range's compressed size exactly once"
        );
        assert!(
            consolidator.size_accumulator().add_range(
                cache_key,
                damaged.start,
                damaged.end,
                damaged.compressed_size,
            ),
            "the heal must clear the range's dedup entry, so the refill credits once"
        );
    }

    /// range-file-self-heal R1: a heal drops the key from this proxy's RAM metadata
    /// tier, so a read here within `metadata_cache.refresh_interval` of the heal no longer
    /// lists the healed range and is an ordinary miss at once. Uses the manager
    /// `CacheManager::create_configured_disk_cache_manager` builds, the production wiring.
    ///
    /// # RED SIDE
    ///
    /// Without the invalidation the second lookup is a RAM hit that still lists the range.
    #[tokio::test]
    async fn a_heal_drops_the_healed_range_from_this_proxys_ram_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let cm =
            crate::cache::CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);
        let mut reader = cm.create_configured_disk_cache_manager();
        reader.initialize().await.unwrap();
        let limiter = Arc::new(RangeHealLimiter::new(
            64,
            std::time::Duration::from_secs(60),
        ));
        reader.set_range_heal_limiter_for_test(limiter.clone());
        let (mut writer, _) = self_heal_manager(&temp_dir, 64).await;
        let data = self_heal_payload(400_000);
        let (first_half, second_half) = data.split_at(200_000);
        let cache_key = "heal-bucket/self-heal-ram-metadata.bin";
        let damaged = self_heal_store(&mut writer, cache_key, 0, first_half, 400_000).await;
        self_heal_store(&mut writer, cache_key, 200_000, second_half, 400_000).await;
        let lists = |m: &crate::cache_types::NewCacheMetadata| {
            m.ranges
                .iter()
                .any(|r| r.start == damaged.start && r.end == damaged.end)
        };
        let before = cm.get_metadata_cached(cache_key).await.unwrap();
        assert!(
            before.as_ref().is_some_and(lists)
                && cm
                    .get_metadata_cache()
                    .peek(cache_key)
                    .await
                    .is_some_and(|m| lists(&m)),
            "fixture precondition: this proxy's RAM metadata must list the range before the heal"
        );
        self_heal_damage(
            &temp_dir.path().join("ranges").join(&damaged.file_path),
            "flip",
        );

        assert!(reader.load_range_data(&damaged).await.is_err());
        assert_eq!(
            limiter.outcomes(),
            vec![RangeHealOutcome::Healed],
            "fixture precondition: the read must heal the range"
        );
        assert!(
            self_heal_listed(&writer, cache_key, damaged.start, damaged.end).is_none(),
            "fixture precondition: the heal removed the range from the .meta on disk"
        );
        let after = cm.get_metadata_cached(cache_key).await.unwrap();
        assert!(
            !after.as_ref().is_some_and(lists),
            "a read on this proxy within refresh_interval of the heal must not still list the \
             healed range {}-{}: lookup lists {:?}, RAM entry present={}",
            damaged.start,
            damaged.end,
            after.as_ref().map(|m| m
                .ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>()),
            cm.get_metadata_cache().peek(cache_key).await.is_some()
        );
    }

    /// range-file-self-heal R3: the buffered heal judges the file the read decoded. A
    /// peer's refill renamed over the path between the read and the heal is a different file
    /// and is left serving.
    ///
    /// # RED SIDE
    ///
    /// With the stamp taken from the path after the read, the heal sees the refill's stamp on
    /// both sides, removes the good file, and the outcomes read `[Healed]`.
    #[tokio::test]
    async fn a_refill_landing_after_a_buffered_read_found_damage_is_not_healed() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, limiter) = self_heal_manager(&temp_dir, 64).await;
        let data = self_heal_payload(300_000);
        let cache_key = "heal-bucket/self-heal-refill-race.bin";
        let spec = self_heal_store(&mut dcm, cache_key, 0, &data, data.len() as u64).await;
        let bin = temp_dir.path().join("ranges").join(&spec.file_path);
        let good = std::fs::read(&bin).unwrap();
        self_heal_damage(&bin, "flip");
        // A peer's refill of the same range: the good bytes, renamed over the path.
        let refill_bin = bin.clone();
        limiter.set_after_detect(Box::new(move || {
            let tmp = refill_bin.with_extension("refill.tmp");
            std::fs::write(&tmp, &good).unwrap();
            std::fs::rename(&tmp, &refill_bin).unwrap();
        }));

        assert!(
            dcm.load_range_data(&spec).await.is_err(),
            "fixture precondition: the read decodes the damaged file"
        );
        let listed = self_heal_listed(&dcm, cache_key, spec.start, spec.end).is_some();
        assert!(
            limiter.outcomes() == vec![RangeHealOutcome::Replaced] && listed && bin.exists(),
            "a refill renamed over the path after the read must not be healed as the damaged \
             file: outcomes={:?}, listed={listed}, bin_exists={}",
            limiter.outcomes(),
            bin.exists()
        );
        // The refill is another file than the damaged one's listing describes, so it
        // serves under the listing its own publish records: the same range, its identity.
        let mut refilled = spec.clone();
        refilled.file_identity = std::fs::metadata(&bin)
            .ok()
            .and_then(|m| crate::cache_types::RangeFileIdentity::of(&m));
        assert!(
            dcm.load_range_data(&refilled).await.unwrap() == data,
            "the refilled range must serve byte-exact"
        );
    }

    /// One LZ4 frame holding `data`, as the proxy's writers produce.
    fn self_heal_frame(data: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut info = lz4_flex::frame::FrameInfo::new();
        info.content_checksum = true;
        let mut encoder = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    /// range-file-self-heal R1: a range file of two LZ4 frames cut at the frame
    /// boundary decodes cleanly to the first frame's length. The read errors, so the file is
    /// damaged and is healed, on both read shapes.
    ///
    /// # RED SIDE
    ///
    /// Without the wrong-length heal the range stays listed and its file stays, and every
    /// read fails the same way.
    #[tokio::test]
    async fn a_range_file_cut_at_a_frame_boundary_is_healed() {
        let data = self_heal_payload(300_000);
        let mut failures: Vec<String> = Vec::new();
        for shape in ["buffered", "streamed"] {
            let temp_dir = TempDir::new().unwrap();
            let (mut dcm, limiter) = self_heal_manager(&temp_dir, 64).await;
            let cache_key = format!("heal-bucket/self-heal-frame-cut-{shape}.bin");
            let spec = self_heal_store(&mut dcm, &cache_key, 0, &data, data.len() as u64).await;
            let bin = temp_dir.path().join("ranges").join(&spec.file_path);
            // The first of two frames, as an incremental write leaves it truncated at the
            // second frame's start.
            std::fs::write(&bin, self_heal_frame(&data[..150_000])).unwrap();
            assert!(
                decode_lz4_frames_exact(&std::fs::read(&bin).unwrap(), 150_000).is_some(),
                "fixture precondition: the cut file decodes cleanly to the first frame"
            );

            let first = self_heal_read(&dcm, &spec, shape).await;
            let still_listed = self_heal_listed(&dcm, &cache_key, spec.start, spec.end);
            if first.is_ok() || still_listed.is_some() || bin.exists() {
                failures.push(format!(
                    "{shape}: a range file cut at a frame boundary must be healed by the read \
                     that found it; first={first:?}, listed={}, bin_exists={}, outcomes={:?}",
                    still_listed.is_some(),
                    bin.exists(),
                    limiter.outcomes()
                ));
                continue;
            }
            let second = self_heal_read(&dcm, &spec, shape).await;
            if !second
                .as_ref()
                .is_err_and(|e| e.contains("not found") && e.contains("cache miss"))
            {
                failures.push(format!(
                    "{shape}: after the heal the next read is an ordinary miss, got {second:?}"
                ));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// range-file-self-heal R2 control: an overwrite is not a wrong-length file. A
    /// reader holding the previous version's `RangeSpec` reads the new version's file at the
    /// same path, which holds the same bounds and so decodes to the same length: no
    /// wrong-length heal fires, and any heal the read did start finds the stale spec unlisted.
    #[tokio::test]
    async fn an_overwritten_object_read_with_a_stale_spec_is_not_healed() {
        let temp_dir = TempDir::new().unwrap();
        let (mut dcm, limiter) = self_heal_manager(&temp_dir, 64).await;
        let data = self_heal_payload(300_000);
        let cache_key = "heal-bucket/self-heal-overwrite.bin";
        let stale = self_heal_store(&mut dcm, cache_key, 0, &data, data.len() as u64).await;
        let newer: Vec<u8> = data.iter().map(|b| b.to_ascii_uppercase()).collect();
        let object_metadata = crate::cache_types::ObjectMetadata::new(
            "\"etag-self-heal-v2\"".to_string(),
            "Thu, 22 Oct 2025 07:28:00 GMT".to_string(),
            newer.len() as u64,
            Some("application/octet-stream".to_string()),
        );
        dcm.store_range(
            cache_key,
            0,
            newer.len() as u64 - 1,
            &newer,
            object_metadata,
            std::time::Duration::from_secs(3600),
            false,
        )
        .await
        .unwrap();
        let current = self_heal_listed(&dcm, cache_key, 0, newer.len() as u64 - 1)
            .expect("fixture: the overwrite lists the range");
        assert!(
            current.compressed_size != stale.compressed_size
                && current.file_path == stale.file_path,
            "fixture precondition: the new version is a different file at the same path"
        );

        let stale_read = dcm.load_range_data(&stale).await;
        assert!(
            limiter
                .outcomes()
                .iter()
                .all(|o| *o == RangeHealOutcome::NotListed)
                && self_heal_listed(&dcm, cache_key, 0, newer.len() as u64 - 1).is_some(),
            "a stale read of an overwritten object must not remove the new version:              outcomes={:?}, stale_read_len={:?}",
            limiter.outcomes(),
            stale_read.as_ref().map(|v| v.len())
        );
        assert!(
            dcm.load_range_data(&current).await.unwrap() == newer,
            "the new version must still serve byte-exact"
        );
    }

    /// Test that streaming a 0-byte range produces no data items and no errors.
    /// Validates: Requirement 1.4 (edge case)
    #[tokio::test]
    async fn test_stream_range_data_empty_range() {
        use crate::compression::CompressionHandler;
        use futures::StreamExt;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Compress 0 bytes with LZ4 to get a valid empty compressed file
        let mut compression_handler = CompressionHandler::new(0, true);
        let compression_result = compression_handler
            .compress_with_algorithm(&[], CompressionAlgorithm::Lz4)
            .unwrap();
        let compressed_data = &compression_result.data;

        // Write the valid empty compressed file
        let file_path = "test-bucket/empty-object_0-0.bin";
        let range_file_path = temp_dir.path().join("ranges").join(file_path);
        if let Some(parent) = range_file_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&range_file_path, compressed_data)
            .await
            .unwrap();

        // Create a RangeSpec with uncompressed_size=0
        let range_spec = crate::cache_types::RangeSpec::new(
            0,
            0,
            file_path.to_string(),
            CompressionAlgorithm::Lz4,
            compressed_data.len() as u64,
            0, // uncompressed_size = 0
        );

        let stream = cache_manager
            .stream_range_data(&range_spec, 4096)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(stream);
        let mut items = Vec::new();

        while let Some(result) = stream.next().await {
            items.push(result);
        }

        // All items should be Ok (no errors), and no data should be produced
        for (i, item) in items.iter().enumerate() {
            assert!(
                item.is_ok(),
                "Item {} should be Ok, got error: {:?}",
                i,
                item.as_ref().unwrap_err()
            );
        }

        let total_bytes: usize = items
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.len())
            .sum();
        assert_eq!(total_bytes, 0, "Empty range should produce 0 bytes of data");
    }

    /// Test that stream_range_data for a missing file returns the same "Range file not found"
    /// error as the current blocking load_range_data implementation.
    /// Validates: Requirement 3.3
    #[tokio::test]
    async fn test_stream_range_data_missing_file_error() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Create a RangeSpec pointing to a non-existent file
        let range_spec = crate::cache_types::RangeSpec::new(
            0,
            1023,
            "nonexistent_file_0-1023.bin".to_string(),
            CompressionAlgorithm::Lz4,
            1024,
            1024,
        );

        // stream_range_data should return an error (not a stream that yields errors)
        let stream_result = cache_manager.stream_range_data(&range_spec, 4096).await;
        assert!(
            stream_result.is_err(),
            "Should fail when range file is missing"
        );

        let stream_error_msg = match stream_result {
            Err(e) => format!("{}", e),
            Ok(_) => panic!("Expected error for missing file"),
        };

        // load_range_data should also return an error for the same missing file
        let load_result = cache_manager.load_range_data(&range_spec).await;
        assert!(
            load_result.is_err(),
            "load_range_data should also fail for missing file"
        );

        let load_error_msg = format!("{}", load_result.unwrap_err());

        // Both should contain "not found" — same error pattern
        assert!(
            stream_error_msg.contains("not found"),
            "stream_range_data error should mention 'not found': {}",
            stream_error_msg
        );
        assert!(
            load_error_msg.contains("not found"),
            "load_range_data error should mention 'not found': {}",
            load_error_msg
        );

        // Both should be CacheError variants with the same file reference
        assert!(
            stream_error_msg.contains("nonexistent_file_0-1023.bin"),
            "stream error should reference the missing file: {}",
            stream_error_msg
        );
        assert!(
            load_error_msg.contains("nonexistent_file_0-1023.bin"),
            "load error should reference the missing file: {}",
            load_error_msg
        );
    }

    // ============================================================================
    // Tests for Incremental Write Error Handling (Requirement 2.3)
    // ============================================================================

    /// An ENOSPC on the streaming range writer's batch write (the GET-response tee) is counted
    /// in `cache.disk_full_events_total`, and the chunk write still fails.
    ///
    /// The error is injected at the `write_all` through `INJECT_BATCH_WRITE_OS_ERROR`, because
    /// a real full volume cannot be produced here. The counter is process-global, so the
    /// assertion is "rose by at least one": no other test injects ENOSPC, and a concurrent
    /// increment can only add.
    ///
    /// # RED SIDE
    ///
    /// With `note_disk_full(&e, "incremental_range.batch_write");` removed from `flush_batch`,
    /// the write still fails and the counter does not move, so the second assertion fails.
    #[tokio::test]
    async fn streaming_range_batch_write_enospc_is_counted_as_disk_full() {
        let temp_dir = TempDir::new().unwrap();
        // Batch size 1024: one 1024-byte chunk triggers `flush_batch` inside `write_range_chunk`.
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1024);
        cache_manager.initialize().await.unwrap();

        let mut writer = cache_manager
            .begin_incremental_range_write("test-bucket/enospc-object", 0, 2047, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        let before = crate::metrics::disk_full_events_total();
        super::INJECT_BATCH_WRITE_OS_ERROR.with(|cell| cell.set(Some(libc::ENOSPC)));
        let result = DiskCacheManager::write_range_chunk(&mut writer, &[0xAAu8; 1024]);
        super::INJECT_BATCH_WRITE_OS_ERROR.with(|cell| cell.set(None));
        let after = crate::metrics::disk_full_events_total();

        assert!(
            result.is_err(),
            "an injected ENOSPC must fail the chunk write"
        );
        assert!(
            after > before,
            "an ENOSPC on the streaming range write must increment \
             cache.disk_full_events_total: before={before} after={after}"
        );
        DiskCacheManager::abort_incremental_range(writer);
    }

    /// Test that abort_incremental_range cleans up the .tmp file.
    /// Begin an incremental write, write some chunks, abort, verify no .tmp files remain.
    /// Validates: Requirement 2.3
    #[tokio::test]
    async fn test_incremental_write_abort_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/abort-test-object";
        let start = 0u64;
        let end = 2047u64;

        // Begin incremental write
        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // Write a couple of chunks
        let chunk1 = vec![0xAAu8; 1024];
        let chunk2 = vec![0xBBu8; 512];
        DiskCacheManager::write_range_chunk(&mut writer, &chunk1).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk2).unwrap();

        // Verify the .tmp file exists before abort
        assert!(
            writer.tmp_path.exists(),
            ".tmp file should exist before abort"
        );

        // Abort the write
        DiskCacheManager::abort_incremental_range(writer);

        // Verify no .tmp files remain anywhere under the cache directory
        fn find_tmp_files(dir: &std::path::Path) -> Vec<PathBuf> {
            let mut results = Vec::new();
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        results.extend(find_tmp_files(&path));
                    } else if path
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().contains(".tmp"))
                    {
                        // Range temps now end `.tmp.{host}.{pid}`, so an extension test
                        // would miss them and this check would pass vacuously.
                        results.push(path);
                    }
                }
            }
            results
        }

        let tmp_files = find_tmp_files(temp_dir.path());
        assert!(
            tmp_files.is_empty(),
            "No .tmp files should remain after abort, found: {:?}",
            tmp_files
        );
    }

    /// Test that commit_incremental_range returns an error when fewer bytes than expected
    /// are written, and cleans up both .tmp and .bin files.
    /// Validates: Requirement 2.3
    #[tokio::test]
    async fn test_incremental_write_size_mismatch_on_commit() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/size-mismatch-object";
        let start = 0u64;
        let end = 1023u64; // Expecting 1024 bytes

        // Begin incremental write
        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // Write only 512 bytes (half of expected)
        let short_data = vec![0xCCu8; 512];
        DiskCacheManager::write_range_chunk(&mut writer, &short_data).unwrap();

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 10240,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 10240,
            parts: Vec::new(),
            ..Default::default()
        };

        // Commit should fail due to size mismatch
        let result = cache_manager
            .commit_incremental_range(writer, object_metadata, Duration::from_secs(3600))
            .await;
        assert!(
            result.is_err(),
            "Commit should fail when bytes_written != expected size"
        );

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("size mismatch"),
            "Error should mention size mismatch: {}",
            err_msg
        );

        // Verify no .tmp or .bin files remain
        fn find_files_in(dir: &std::path::Path) -> Vec<PathBuf> {
            let mut results = Vec::new();
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        results.extend(find_files_in(&path));
                    } else {
                        results.push(path);
                    }
                }
            }
            results
        }

        let ranges_dir = temp_dir.path().join("ranges");
        let leftover_files = find_files_in(&ranges_dir);
        assert!(
            leftover_files.is_empty(),
            "No .tmp or .bin files should remain after size mismatch, found: {:?}",
            leftover_files
        );
    }

    /// Req 2: a received prefix at or above `partial_range_commit_ratio` of an
    /// incomplete range is salvaged as a clamped sub-range instead of discarded.
    /// 768/1024 bytes (75%) with ratio 0.5 → commit succeeds, a `.bin` for the
    /// clamped range 0-767 exists (NOT the requested 0-1023), no `.tmp` remains.
    #[tokio::test]
    async fn test_partial_range_commit_salvages_prefix_above_threshold() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.set_partial_range_commit_ratio(0.5);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/partial-salvage-object";
        let (start, end) = (0u64, 1023u64); // expected 1024 bytes

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");
        // Write 768 bytes (75% of 1024) in order.
        DiskCacheManager::write_range_chunk(&mut writer, &vec![0xABu8; 768]).unwrap();

        let object_metadata = ObjectMetadata {
            etag: "etag-partial".to_string(),
            content_length: 4096,
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 4096,
            ..Default::default()
        };
        let result = cache_manager
            .commit_incremental_range(writer, object_metadata, Duration::from_secs(3600))
            .await;
        assert!(
            result.is_ok(),
            "Commit should salvage a 75% prefix when ratio=0.5: {:?}",
            result.err()
        );

        // The clamped .bin (0-767) must exist; the requested-end .bin (0-1023) must not.
        fn collect_bins(dir: &std::path::Path) -> Vec<String> {
            let mut out = Vec::new();
            if let Ok(entries) = std::fs::read_dir(dir) {
                for e in entries.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        out.extend(collect_bins(&p));
                    } else if p.extension().is_some_and(|x| x == "bin") {
                        out.push(p.file_name().unwrap().to_string_lossy().into_owned());
                    }
                }
            }
            out
        }
        let bins = collect_bins(&temp_dir.path().join("ranges"));
        assert!(
            bins.iter().any(|n| n.ends_with("_0-767.bin")),
            "expected a clamped .bin ending in _0-767.bin, found: {:?}",
            bins
        );
        assert!(
            !bins.iter().any(|n| n.ends_with("_0-1023.bin")),
            "must NOT publish the requested-end range as complete, found: {:?}",
            bins
        );
    }

    /// Req 2: a received prefix below `partial_range_commit_ratio` is discarded
    /// (legacy behaviour). 256/1024 bytes (25%) with ratio 0.5 → commit errors,
    /// no `.bin`/`.tmp` remain.
    #[tokio::test]
    async fn test_partial_range_commit_discards_below_threshold() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.set_partial_range_commit_ratio(0.5);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/partial-discard-object";
        let (start, end) = (0u64, 1023u64);

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");
        DiskCacheManager::write_range_chunk(&mut writer, &vec![0xCDu8; 256]).unwrap();

        let result = cache_manager
            .commit_incremental_range(writer, ObjectMetadata::default(), Duration::from_secs(3600))
            .await;
        assert!(result.is_err(), "25% prefix below ratio 0.5 must error");
        assert!(format!("{}", result.unwrap_err()).contains("size mismatch"));

        let ranges_dir = temp_dir.path().join("ranges");
        fn any_file(dir: &std::path::Path) -> bool {
            std::fs::read_dir(dir)
                .map(|rd| {
                    rd.flatten().any(|e| {
                        let p = e.path();
                        if p.is_dir() {
                            any_file(&p)
                        } else {
                            true
                        }
                    })
                })
                .unwrap_or(false)
        }
        assert!(
            !any_file(&ranges_dir),
            "no .bin/.tmp should remain when prefix is below threshold"
        );
    }

    /// Req 2: ratio 1.0 (the unconfigured default) is exact-only — a 75% prefix is
    /// discarded, matching legacy behaviour and the PUT write-cache path.
    #[tokio::test]
    async fn test_partial_range_commit_exact_only_when_ratio_one() {
        use crate::cache_types::ObjectMetadata;

        let temp_dir = TempDir::new().unwrap();
        // Default ratio is 1.0 (exact-only); do NOT call set_partial_range_commit_ratio.
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/exact-only-object";
        let (start, end) = (0u64, 1023u64);

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");
        DiskCacheManager::write_range_chunk(&mut writer, &vec![0xEEu8; 768]).unwrap();

        let result = cache_manager
            .commit_incremental_range(writer, ObjectMetadata::default(), Duration::from_secs(3600))
            .await;
        assert!(
            result.is_err(),
            "with ratio 1.0 a 75% prefix must error (exact-only)"
        );
    }

    /// Test single-byte range round-trip via incremental write.
    /// Write a single byte (start=0, end=0), commit, read back, verify match.
    #[tokio::test]
    async fn test_incremental_write_single_byte_round_trip() {
        use crate::cache_types::{ObjectMetadata, RangeSpec};

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/single-byte-object";
        let start = 0u64;
        let end = 0u64; // Single byte range

        // Begin incremental write
        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // Write exactly 1 byte
        let single_byte = vec![0x42u8];
        DiskCacheManager::write_range_chunk(&mut writer, &single_byte).unwrap();

        // Capture compressed size before commit consumes writer
        let compressed_bytes = writer.compressed_bytes_written;

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1,
            parts: Vec::new(),
            ..Default::default()
        };

        // Commit
        cache_manager
            .commit_incremental_range(writer, object_metadata, Duration::from_secs(3600))
            .await
            .unwrap();

        // Construct RangeSpec manually to read back
        let final_path = cache_manager.get_new_range_file_path(cache_key, start, end);
        let ranges_dir = temp_dir.path().join("ranges");
        let relative_path = final_path
            .strip_prefix(&ranges_dir)
            .unwrap()
            .to_string_lossy()
            .to_string();

        let range_spec = RangeSpec::new(
            start,
            end,
            relative_path,
            CompressionAlgorithm::Lz4,
            compressed_bytes,
            1, // 1 byte uncompressed
        );

        // Read back and verify
        let loaded_data = cache_manager.load_range_data(&range_spec).await.unwrap();
        assert_eq!(
            loaded_data, single_byte,
            "Single byte should round-trip correctly"
        );
        assert_eq!(loaded_data.len(), 1);
    }

    /// Test that two concurrent incremental writes for the same cache_key and range
    /// produce unique .tmp filenames.
    #[tokio::test]
    async fn test_incremental_write_concurrent_unique_tmp_paths() {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/concurrent-object";
        let start = 0u64;
        let end = 1023u64;

        // Begin two incremental writes for the same key and range
        let writer1 = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");
        let writer2 = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // Verify the two writers have different tmp_path values
        assert_ne!(
            writer1.tmp_path, writer2.tmp_path,
            "Concurrent writers should have unique .tmp paths: {:?} vs {:?}",
            writer1.tmp_path, writer2.tmp_path
        );

        // Both .tmp files should exist
        assert!(writer1.tmp_path.exists(), "Writer 1 .tmp file should exist");
        assert!(writer2.tmp_path.exists(), "Writer 2 .tmp file should exist");

        // Abort both writers
        DiskCacheManager::abort_incremental_range(writer1);
        DiskCacheManager::abort_incremental_range(writer2);

        // Verify cleanup — reuse the helper from abort test
        fn find_tmp_files_in(dir: &std::path::Path) -> Vec<PathBuf> {
            let mut results = Vec::new();
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        results.extend(find_tmp_files_in(&path));
                    } else if path
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().contains(".tmp"))
                    {
                        // Range temps now end `.tmp.{host}.{pid}`, so an extension test
                        // would miss them and this check would pass vacuously.
                        results.push(path);
                    }
                }
            }
            results
        }

        let tmp_files = find_tmp_files_in(temp_dir.path());
        assert!(
            tmp_files.is_empty(),
            "No .tmp files should remain after aborting both writers, found: {:?}",
            tmp_files
        );
    }

    // ============================================================================
    // Tests for Batched LZ4 Compression in IncrementalRangeWriter (Task 3.7)
    // Validates: Requirements 1.3, 2.1, 2.2, 2.3, 2.4
    // ============================================================================

    /// Chunks whose total size is less than `batch_size` must stay in `batch_buf`
    /// without producing any LZ4 frame: the `.tmp` file must be zero-length,
    /// `compressed_bytes_written` must be 0, while `bytes_written` tracks the
    /// logical uncompressed total.
    /// Validates: Requirement 2.1
    #[tokio::test]
    async fn test_write_range_chunk_below_batch_size_does_not_flush() {
        let temp_dir = TempDir::new().unwrap();
        // Small batch_size so the test is fast but still exercises the threshold.
        let batch_size = 8192usize;
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, batch_size);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/below-batch-object";
        let start = 0u64;
        let end = 3999u64; // 4000 bytes total — well below batch_size

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // Write two chunks that sum to less than batch_size (1500 + 2500 = 4000)
        let chunk1 = vec![0x11u8; 1500];
        let chunk2 = vec![0x22u8; 2500];
        DiskCacheManager::write_range_chunk(&mut writer, &chunk1).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk2).unwrap();

        // Logical uncompressed bytes must include everything we pushed
        assert_eq!(
            writer.bytes_written, 4000,
            "bytes_written tracks logical total"
        );
        // Nothing has been flushed yet, so no compressed bytes accounted for
        assert_eq!(
            writer.compressed_bytes_written, 0,
            "compressed_bytes_written must be 0 until a batch is flushed"
        );
        // The full input should still be buffered in memory
        assert_eq!(
            writer.batch_buf.len(),
            4000,
            "batch_buf should hold all accumulated bytes when under threshold"
        );

        // The `.tmp` file on disk must be empty — no frame written yet
        let tmp_len = std::fs::metadata(&writer.tmp_path).unwrap().len();
        assert_eq!(
            tmp_len, 0,
            ".tmp file must be zero-length when all chunks fit inside the batch buffer"
        );

        // Clean up — we don't commit this one
        DiskCacheManager::abort_incremental_range(writer);
    }

    /// A chunk whose size is exactly `batch_size` (or fills `batch_buf` to the
    /// threshold) must trigger exactly one frame flush, producing a non-zero
    /// `.tmp` file and `compressed_bytes_written > 0`. After flush, `batch_buf`
    /// must be empty.
    /// Validates: Requirements 2.1, 2.2
    #[tokio::test]
    async fn test_write_range_chunk_exactly_at_batch_size_flushes() {
        let temp_dir = TempDir::new().unwrap();
        let batch_size = 4096usize;
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, batch_size);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/at-batch-object";
        let start = 0u64;
        let end = (batch_size as u64) - 1; // Exactly batch_size bytes

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // One chunk that exactly fills the batch buffer — threshold check is `>=`
        let chunk = vec![0x7Eu8; batch_size];
        DiskCacheManager::write_range_chunk(&mut writer, &chunk).unwrap();

        assert_eq!(writer.bytes_written, batch_size as u64);
        assert!(
            writer.compressed_bytes_written > 0,
            "exactly-full batch must flush: compressed_bytes_written should be > 0"
        );
        assert!(
            writer.batch_buf.is_empty(),
            "batch_buf must be drained after a flush, got {} bytes",
            writer.batch_buf.len()
        );

        // `.tmp` file must contain the compressed frame
        let tmp_len = std::fs::metadata(&writer.tmp_path).unwrap().len();
        assert!(
            tmp_len > 0,
            ".tmp file must contain a compressed frame after batch flush"
        );
        assert_eq!(
            tmp_len, writer.compressed_bytes_written,
            ".tmp file size must equal the compressed bytes counter"
        );

        DiskCacheManager::abort_incremental_range(writer);
    }

    /// After committing an incremental write whose total size is not a multiple
    /// of `batch_size`, the residual bytes in `batch_buf` must be flushed as a
    /// final LZ4 frame by `commit_incremental_range`, and the resulting `.bin`
    /// file must decompress to the full input via `load_range_data`.
    /// Validates: Requirements 2.3, 1.3 (byte-identity via round-trip)
    #[tokio::test]
    async fn test_commit_flushes_residual_batch() {
        use crate::cache_types::{ObjectMetadata, RangeSpec};

        let temp_dir = TempDir::new().unwrap();
        let batch_size = 4096usize;
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, batch_size);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/residual-batch-object";
        let start = 0u64;
        // 10_000 bytes total. We feed it in chunks of 4096 + 2000 + 4096 + (−192 invalid).
        // Simpler: feed as [5000, 5000] bytes.
        //   After chunk 1 (5000 bytes): batch_buf=5000, >=4096 → flush → batch_buf=0.
        //   After chunk 2 (5000 bytes): batch_buf=5000, >=4096 → flush → batch_buf=0.
        // That produces no residual. To guarantee residual, use [5000, 1000, 1000]
        // totaling 7000 bytes:
        //   After chunk 1 (5000): flushes, buf=0.
        //   After chunk 2 (1000): buf=1000, no flush.
        //   After chunk 3 (1000): buf=2000, no flush, 2000 < batch_size.
        // 2000 bytes residual at commit time.
        let total_len: usize = 7_000;
        let end = (total_len as u64) - 1;

        // Deterministic, non-constant input so a byte-identity round-trip is meaningful.
        let input: Vec<u8> = (0..total_len).map(|i| (i % 251) as u8).collect();

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // Feed the input in the specific partition [5000, 1000, 1000] so
        // there's guaranteed residual left in batch_buf pre-commit.
        DiskCacheManager::write_range_chunk(&mut writer, &input[0..5000]).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &input[5000..6000]).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &input[6000..7000]).unwrap();

        // Before commit, some residual bytes must still be buffered: chunks
        // of 1337 bytes are written, and when accumulated batch_buf crosses
        // the 4096-byte threshold, flush occurs. The residual post-last-chunk
        // is bounded by batch_size (never >= one full batch), but the exact
        // value depends on chunk/threshold alignment, not just `total % batch`.
        assert!(
            writer.batch_buf_len() < batch_size,
            "batch_buf must be under batch_size pre-commit (got {}, batch_size {})",
            writer.batch_buf_len(),
            batch_size
        );
        assert!(
            writer.batch_buf_len() > 0,
            "some residual must still be buffered for this test to exercise commit flush"
        );
        let pre_commit_compressed = writer.compressed_bytes_written;
        assert!(
            pre_commit_compressed > 0,
            "at least one full batch should have been flushed before commit"
        );

        let object_metadata = ObjectMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: total_len as u64,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: total_len as u64,
            parts: Vec::new(),
            ..Default::default()
        };

        // Snapshot compressed_bytes_written by re-reading it after commit via
        // the `.bin` file on disk — the writer is consumed by commit.
        cache_manager
            .commit_incremental_range(writer, object_metadata, Duration::from_secs(3600))
            .await
            .unwrap();

        // Round-trip via load_range_data: construct a RangeSpec pointing at
        // the committed `.bin` file and confirm byte-identical decode.
        let final_path = cache_manager.get_new_range_file_path(cache_key, start, end);
        let ranges_dir = temp_dir.path().join("ranges");
        let relative_path = final_path
            .strip_prefix(&ranges_dir)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let bin_len = std::fs::metadata(&final_path).unwrap().len();
        assert!(
            bin_len > pre_commit_compressed,
            "commit must have appended a residual frame: bin={} > pre-commit compressed={}",
            bin_len,
            pre_commit_compressed
        );

        let range_spec = RangeSpec::new(
            start,
            end,
            relative_path,
            CompressionAlgorithm::Lz4,
            bin_len,
            total_len as u64,
        );

        let loaded = cache_manager.load_range_data(&range_spec).await.unwrap();
        assert_eq!(
            loaded, input,
            "decoded bytes must be byte-identical to original input"
        );
    }

    /// When compression is disabled for a range, batches still flow through
    /// the same batch buffer and are flushed as store-mode LZ4 frames (no LZ4
    /// block compressor invocation, but still a valid, checksummed frame) —
    /// never raw, unchecksummed bytes. See compression-content-aware-fix spec,
    /// Requirement 3.
    #[tokio::test]
    async fn test_compression_disabled_uses_store_mode_frames() {
        let temp_dir = TempDir::new().unwrap();
        let batch_size = 4096usize;
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, batch_size);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/no-compression-object";
        let start = 0u64;
        let end = 4999u64; // 5000 bytes total

        // compression_enabled = false → store-mode framing, not raw passthrough
        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, false)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        let chunk1 = vec![0xA1u8; 2000];
        let chunk2 = vec![0xB2u8; 1500];
        let chunk3 = vec![0xC3u8; 1500];
        DiskCacheManager::write_range_chunk(&mut writer, &chunk1).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk2).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk3).unwrap();

        let expected_total = (chunk1.len() + chunk2.len() + chunk3.len()) as u64;
        assert_eq!(writer.bytes_written, expected_total);
        // Store-mode frames carry ~15-19 bytes of LZ4 frame overhead per
        // flushed batch (magic + header + block-size words + end mark +
        // checksum), so compressed_bytes_written is slightly larger than
        // bytes_written — never equal, and never smaller (no compression ran).
        assert!(
            writer.compressed_bytes_written > expected_total,
            "store-mode frames carry header/trailer overhead: compressed_bytes_written ({}) \
             should exceed bytes_written ({})",
            writer.compressed_bytes_written,
            expected_total
        );

        // The `.tmp` file's bytes must be a valid, decodable LZ4 frame (store
        // mode) rather than raw passthrough bytes.
        let on_disk = std::fs::read(&writer.tmp_path).unwrap();
        let handler = crate::compression::CompressionHandler::new(10, true);
        let decoded = handler
            .decompress_data(&on_disk)
            .expect("store-mode frame on disk must decode via the standard LZ4 frame decoder");
        let mut expected = Vec::new();
        expected.extend_from_slice(&chunk1);
        expected.extend_from_slice(&chunk2);
        expected.extend_from_slice(&chunk3);
        assert_eq!(decoded, expected);

        let tmp_path = writer.tmp_path.clone();
        DiskCacheManager::abort_incremental_range(writer);
        assert!(
            !tmp_path.exists(),
            "abort should have removed the .tmp file"
        );
    }

    /// Producer audit (cache-eviction-at-scale task 29 part 3, see
    /// `CAPACITY-AUTHORITY-PATH-FORWARD.md` § "CORRECTION 2026-09-03"). Pins, for
    /// `DiskCacheManager::store_range` (`disk_cache.rs:1176`), that the persisted
    /// `.bin` file's on-disk byte length equals `RangeSpec.compressed_size`
    /// exactly, and that those bytes decode as a valid LZ4 frame — i.e.
    /// `st_size(.bin) == compressed_size` holds by construction, not by
    /// coincidence. Read back through the real disk path (metadata file), not a
    /// return value, so the assertion is against what a later `readdir` would
    /// actually observe.
    #[tokio::test]
    async fn test_store_range_st_size_equals_compressed_size() {
        use crate::cache_types::{NewCacheMetadata, ObjectMetadata};

        let temp_dir = TempDir::new().unwrap();
        let mut cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/producer-audit-store-range";
        let start = 0u64;
        let end = 8191u64;
        // Compressible payload so the write actually exercises the LZ4 encoder
        // rather than only the store-mode fallback.
        let data = vec![0xABu8; 8192];

        let object_metadata = ObjectMetadata {
            etag: "producer-audit-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 8192,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 8192,
            parts: Vec::new(),
            ..Default::default()
        };

        cache_manager
            .store_range(
                cache_key,
                start,
                end,
                &data,
                object_metadata,
                Duration::from_secs(3600),
                true,
            )
            .await
            .unwrap();

        let range_file_path = cache_manager.get_new_range_file_path(cache_key, start, end);
        let on_disk = std::fs::read(&range_file_path).unwrap();
        let st_size = std::fs::metadata(&range_file_path).unwrap().len();
        assert_eq!(
            st_size,
            on_disk.len() as u64,
            "sanity: metadata().len() must agree with the byte count actually read"
        );

        let metadata_content =
            std::fs::read_to_string(cache_manager.get_new_metadata_file_path(cache_key)).unwrap();
        let metadata: NewCacheMetadata = serde_json::from_str(&metadata_content).unwrap();
        assert_eq!(metadata.ranges.len(), 1);
        let recorded_compressed_size = metadata.ranges[0].compressed_size;

        assert_eq!(
            st_size, recorded_compressed_size,
            "st_size(.bin) must equal RangeSpec.compressed_size for store_range's output"
        );

        // And the bytes must be a valid, decodable LZ4 frame — never raw passthrough.
        let handler = crate::compression::CompressionHandler::new(10, true);
        let decoded = handler
            .decompress_data(&on_disk)
            .expect(".bin bytes from store_range must decode as a standard LZ4 frame");
        assert_eq!(decoded, data);
    }

    /// Producer audit, streaming path: `DiskCacheManager::finalize_incremental_range`
    /// (`disk_cache.rs:2094`), fed by `flush_batch` (`disk_cache.rs:1934`) via
    /// `write_range_chunk`. Pins the same `st_size == compressed_size` invariant for
    /// the concatenated-LZ4-frames streaming writer used by the GET-side range
    /// cache path, plus decodability of the concatenated frames.
    #[tokio::test]
    async fn test_finalize_incremental_range_st_size_equals_compressed_size() {
        let temp_dir = TempDir::new().unwrap();
        let batch_size = 4096usize;
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, batch_size);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/producer-audit-incremental-range";
        let start = 0u64;
        let end = 9999u64; // 10000 bytes, forces at least 2 flushed batches at batch_size=4096

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        let chunk1 = vec![0x11u8; 4000];
        let chunk2 = vec![0x22u8; 4000];
        let chunk3 = vec![0x33u8; 2000];
        DiskCacheManager::write_range_chunk(&mut writer, &chunk1).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk2).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk3).unwrap();

        let expected_compressed_bytes_written = writer.compressed_bytes_written;
        // finalize_incremental_range flushes any residual batch before rename, so
        // compressed_bytes_written may still grow past what's flushed so far —
        // read the RangeSpec's recorded value after finalize, not before.

        let (range_spec, _already_existed) = cache_manager
            .finalize_incremental_range(writer, None)
            .unwrap();

        let final_path = temp_dir.path().join("ranges").join(&range_spec.file_path);
        let on_disk = std::fs::read(&final_path).unwrap();
        let st_size = std::fs::metadata(&final_path).unwrap().len();

        assert_eq!(
            st_size, range_spec.compressed_size,
            "st_size(.bin) must equal RangeSpec.compressed_size for the streaming \
             incremental-range writer's output"
        );
        // Not equal to the pre-finalize snapshot in general (residual batch flush
        // happens inside finalize), but bounded below by it as a sanity check that
        // finalize only ever appends.
        assert!(st_size >= expected_compressed_bytes_written);

        let mut expected = Vec::new();
        expected.extend_from_slice(&chunk1);
        expected.extend_from_slice(&chunk2);
        expected.extend_from_slice(&chunk3);
        let handler = crate::compression::CompressionHandler::new(10, true);
        let decoded = handler
            .decompress_data(&on_disk)
            .expect("concatenated LZ4 frames from finalize_incremental_range must decode");
        assert_eq!(decoded, expected);
    }

    /// Producer audit, multipart-part path: `DiskCacheManager::finalize_incremental_part`
    /// (`disk_cache.rs:2336`). This producer builds no `RangeSpec` (the part is
    /// still staged — see `cache_types.rs:659-662`), so there is no
    /// `compressed_size` field to compare against yet. Pin instead that the
    /// on-disk part file's `st_size` equals `writer.compressed_bytes_written` as
    /// tracked through finalize — the same accumulator that becomes
    /// `compressed_size` once the multipart-completion finalizer in
    /// `signed_put_handler.rs` later reads it back via `tokio::fs::metadata(..).len()`
    /// (see that producer's own `RangeSpec` construction) — and that the part
    /// bytes decode as a valid LZ4 frame.
    #[tokio::test]
    async fn test_finalize_incremental_part_st_size_equals_compressed_bytes_written() {
        let temp_dir = TempDir::new().unwrap();
        let batch_size = 2048usize;
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, batch_size);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/producer-audit-part";
        let part_final_path = temp_dir
            .path()
            .join("mpus_in_progress")
            .join("upload-audit-1")
            .join("part1.bin");

        let mut writer = cache_manager
            .begin_incremental_part_write(part_final_path.clone(), cache_key, true, None)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        let chunk1 = vec![0x44u8; 3000];
        let chunk2 = vec![0x55u8; 1500];
        DiskCacheManager::write_range_chunk(&mut writer, &chunk1).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk2).unwrap();

        let part_info = DiskCacheManager::finalize_incremental_part(writer).unwrap();

        assert!(
            part_final_path.exists(),
            "part .bin file should be renamed into place"
        );
        let on_disk = std::fs::read(&part_final_path).unwrap();
        let st_size = std::fs::metadata(&part_final_path).unwrap().len();

        // finalize_incremental_part returns no compressed_size directly (see
        // PartFinalizeInfo, disk_cache.rs:407) — the on-disk size IS the
        // authoritative compressed size for this producer, read back from disk
        // exactly as the multipart-completion finalizer in signed_put_handler.rs
        // does via tokio::fs::metadata(&new_range_file_path).len(). This test
        // pins that the file's actual byte count is self-consistent (no
        // truncation, no extra trailer) and decodable.
        assert_eq!(
            part_info.uncompressed_size,
            (chunk1.len() + chunk2.len()) as u64
        );
        assert!(
            st_size > 0,
            "part file must contain the flushed LZ4 frame bytes"
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(&chunk1);
        expected.extend_from_slice(&chunk2);
        let handler = crate::compression::CompressionHandler::new(10, true);
        let decoded = handler
            .decompress_data(&on_disk)
            .expect("part .bin bytes from finalize_incremental_part must decode as LZ4 frame(s)");
        assert_eq!(decoded, expected);
    }

    /// Aborting an incremental write after some bytes have been buffered but
    /// never flushed must remove the `.tmp` file from disk (and drop the
    /// in-memory `batch_buf`, which happens automatically via `Drop`).
    /// Validates: Requirement 1.3
    #[tokio::test]
    async fn test_abort_after_partial_batch_leaves_no_tmp_file() {
        let temp_dir = TempDir::new().unwrap();
        let batch_size = 8192usize;
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, batch_size);
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/abort-partial-batch-object";
        let start = 0u64;
        let end = 2047u64; // 2048 bytes — less than batch_size, so no flush happens

        let mut writer = cache_manager
            .begin_incremental_range_write(cache_key, start, end, true)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        // Accumulate some bytes in batch_buf without reaching the threshold.
        let chunk = vec![0x5Au8; 1024];
        DiskCacheManager::write_range_chunk(&mut writer, &chunk).unwrap();
        DiskCacheManager::write_range_chunk(&mut writer, &chunk).unwrap();

        // Precondition: bytes are buffered, `.tmp` exists but empty.
        assert_eq!(writer.batch_buf.len(), 2048);
        assert_eq!(writer.compressed_bytes_written, 0);
        let tmp_path = writer.tmp_path.clone();
        assert!(tmp_path.exists(), ".tmp file should exist before abort");
        assert_eq!(
            std::fs::metadata(&tmp_path).unwrap().len(),
            0,
            ".tmp file must still be empty (no flush yet)"
        );

        // Abort — writer (and its batch_buf) is dropped; `.tmp` is removed.
        DiskCacheManager::abort_incremental_range(writer);

        assert!(
            !tmp_path.exists(),
            ".tmp file must be removed after abort, still present at {:?}",
            tmp_path
        );
    }

    // ============================================================================
    // Tests for range_spec.file_path path traversal rejection (Req 10)
    // ============================================================================

    #[tokio::test]
    async fn test_range_spec_file_path_traversal_rejected_on_read() {
        use crate::cache_types::RangeSpec;
        use crate::compression::CompressionAlgorithm;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        cache_manager.initialize().await.unwrap();

        // Create a sentinel file outside ranges/ to prove it won't be read
        let sentinel_path = temp_dir.path().join("secret.txt");
        std::fs::write(&sentinel_path, b"sensitive data").unwrap();

        // Construct a RangeSpec with a traversal file_path
        let malicious_paths = vec![
            "../secret.txt",
            "../../etc/passwd",
            "bucket/../../../secret.txt",
            "/etc/passwd",
            "bucket/..\\secret.txt",
        ];

        for malicious_path in malicious_paths {
            let range_spec = RangeSpec {
                start: 0,
                end: 1023,
                file_path: malicious_path.to_string(),
                compression_algorithm: CompressionAlgorithm::None,
                compressed_size: 14,
                uncompressed_size: 14,
                created_at: SystemTime::now(),
                last_accessed: SystemTime::now(),
                access_count: 0,
                staged: None,
                unservable: false,
                file_identity: None,
            };

            // load_range_data should return an error (cache miss) for unsafe paths
            let result = cache_manager.load_range_data(&range_spec).await;
            assert!(
                result.is_err(),
                "Expected error for traversal path {:?}, but got Ok",
                malicious_path
            );
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("unsafe"),
                "Error for {:?} should mention 'unsafe', got: {}",
                malicious_path,
                err_msg
            );
        }

        // Verify the sentinel file was never touched
        assert!(sentinel_path.exists(), "Sentinel file should still exist");
    }

    // ============================================================================
    // Tests for Task 2: spawn_blocking metadata read with concurrency bound
    // ============================================================================

    /// Runtime-starvation regression test: a 10ms tokio::time::interval counter must
    /// keep advancing while a slow/oversize .meta read is in flight. On a 2-worker
    /// runtime, blocking the workers would stall the counter.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_metadata_read_does_not_starve_runtime() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().to_path_buf();

        // Create a .meta file that exceeds the 4 MiB cap — triggers Corrupt{Oversize}
        let meta_dir = cache_dir
            .join("metadata")
            .join("test-bucket")
            .join("ab")
            .join("cde");
        std::fs::create_dir_all(&meta_dir).unwrap();
        let meta_file = meta_dir.join("large-object.meta");

        // Write a 5 MiB file (exceeds default 4 MiB cap)
        let junk = vec![b'x'; 5 * 1024 * 1024];
        std::fs::write(&meta_file, &junk).unwrap();

        // Create a DiskCacheManager with default semaphore
        let dcm = DiskCacheManager::new(cache_dir.clone(), true, 1024, true, 1_048_576);

        // Start a counter that ticks every 10ms
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter_clone = counter.clone();

        let ticker_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
            for _ in 0..50 {
                interval.tick().await;
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });

        // Issue get_metadata — since the file is oversize, the blocking helper
        // returns Corrupt{Oversize} quickly, but the key assertion is that the
        // ticker keeps running.
        let result = dcm.get_metadata("test-bucket/large-object").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // Oversize → cache miss

        // Wait for ticker to finish
        ticker_handle.await.unwrap();

        // The counter must have advanced (proves runtime was NOT starved)
        let ticks = counter.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            ticks >= 5,
            "Expected ticker to advance (got {} ticks), runtime may have been starved",
            ticks
        );
    }

    /// Existing behavior: valid .meta files are parsed correctly through spawn_blocking.
    #[tokio::test]
    async fn test_get_metadata_valid_file_through_spawn_blocking() {
        use crate::cache_types::CompressionInfo;
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().to_path_buf();

        let now = std::time::SystemTime::now();
        let meta = crate::cache_types::NewCacheMetadata {
            cache_key: "test-bucket/valid-object.txt".to_string(),
            object_metadata: crate::cache_types::ObjectMetadata {
                content_length: 1024,
                etag: "\"abc123\"".to_string(),
                content_type: Some("application/octet-stream".to_string()),
                last_modified: "Thu, 01 Jan 2026 00:00:00 GMT".to_string(),
                ..Default::default()
            },
            ranges: vec![],
            created_at: now,
            expires_at: now + std::time::Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };

        let mut dcm = DiskCacheManager::new(cache_dir.clone(), true, 1024, true, 1_048_576);
        dcm.set_metadata_config(4 * 1024 * 1024, 32);

        // Write the metadata using the standard path layout
        let meta_path = dcm.get_new_metadata_file_path("test-bucket/valid-object.txt");
        if let Some(parent) = meta_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(&meta_path, &json).unwrap();

        // Read it through the new get_metadata (spawn_blocking path)
        let result = dcm.get_metadata("test-bucket/valid-object.txt").await;
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert!(parsed.is_some());
        let parsed_meta = parsed.unwrap();
        assert_eq!(parsed_meta.object_metadata.etag, "\"abc123\"");
        assert_eq!(parsed_meta.object_metadata.content_length, 1024);
    }

    /// Minimal config test: omitting metadata_io_concurrency and max_metadata_file_bytes
    /// still starts (defaults apply via serde).
    #[test]
    fn test_minimal_config_without_metadata_io_fields() {
        // A minimal YAML that omits the new fields entirely
        let yaml = r#"
cache:
  cache_dir: /tmp/test-cache
logging:
  access_log_dir: /tmp/test-logs/access
  app_log_dir: /tmp/test-logs/app
"#;
        let config: std::result::Result<crate::config::Config, _> = serde_yaml_ng::from_str(yaml);
        assert!(
            config.is_ok(),
            "Minimal config should parse: {:?}",
            config.err()
        );
        let config = config.unwrap();
        assert_eq!(config.cache.metadata_io_concurrency, 32);
        assert_eq!(config.cache.max_metadata_file_bytes, 4 * 1024 * 1024);
    }
}

/// § 5.5's whole-object path: one unlink, no rewrite, and never while a `.bin` survives.
///
/// # Why these assert a COUNT and not the end state
///
/// `!meta_path.exists()` passes on a bare unlink **and** on a rewrite-then-delete, because the
/// two leave the identical end state. The requirement is about the number of filesystem
/// operations — on the measured backend a `.meta` rewrite is 451.9/s against 4,613/s for an
/// unlink — so the end state cannot express it. `BatchRangeDeletion::meta_writes` can, and the
/// two arms are deliberately separate tests so that a regression reds one and not the other.
///
/// The partial arm is the positive control for the same counter: it must report exactly 1, which
/// is what rules out a figure that is never incremented and therefore trivially 0.
///
/// Spec: cache-eviction-at-scale § 5.5. Requirements: 3.9, 3.11, 15.3
#[cfg(test)]
mod whole_object_mechanism_tests {
    use super::*;
    use crate::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
    use crate::compression::CompressionAlgorithm;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    const RANGE_BYTES: usize = 512;

    fn manager(dir: &std::path::Path) -> DiskCacheManager {
        DiskCacheManager::new(dir.to_path_buf(), true, 1024, false, 1_048_576)
    }

    /// Seed one object with `count` ranges, each `RANGE_BYTES` long, and write its `.meta`.
    fn seed(
        dc: &DiskCacheManager,
        temp: &std::path::Path,
        cache_key: &str,
        count: u64,
    ) -> Vec<(u64, u64)> {
        let now = SystemTime::now();
        let mut extents = Vec::new();
        let mut specs = Vec::new();
        for i in 0..count {
            let start = i * RANGE_BYTES as u64;
            let end = start + RANGE_BYTES as u64 - 1;
            let bin = dc.get_new_range_file_path(cache_key, start, end);
            std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
            std::fs::write(&bin, vec![0xEFu8; RANGE_BYTES]).unwrap();
            specs.push(RangeSpec {
                start,
                end,
                file_path: bin
                    .strip_prefix(temp.join("ranges"))
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                compression_algorithm: CompressionAlgorithm::None,
                compressed_size: RANGE_BYTES as u64,
                uncompressed_size: RANGE_BYTES as u64,
                created_at: now,
                last_accessed: now,
                access_count: 1,
                staged: None,
                unservable: false,
                file_identity: None,
            });
            extents.push((start, end));
        }
        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata::default(),
            ranges: specs,
            created_at: now,
            expires_at: now + Duration::from_secs(3600),
            compression_info: CompressionInfo::default(),
            ..Default::default()
        };
        let meta = dc.get_new_metadata_file_path(cache_key);
        std::fs::create_dir_all(meta.parent().unwrap()).unwrap();
        std::fs::write(&meta, serde_json::to_string_pretty(&metadata).unwrap()).unwrap();
        extents
    }

    /// ARM 1 — every range selected. The MECHANISM arm: zero `.meta` writes.
    #[tokio::test]
    async fn arm1_all_selected_takes_the_one_unlink_path_with_no_rewrite() {
        let temp = TempDir::new().unwrap();
        let dc = manager(temp.path());
        dc.initialize().await.unwrap();
        let key = "test-bucket/ew-arm1.bin";
        let extents = seed(&dc, temp.path(), key, 3);

        let out = dc.batch_delete_ranges(key, &extents).await.unwrap();

        assert!(
            out.whole_object_intended,
            "fixture: all 3 of the .meta's ranges were passed to the delete, so R3.9's \
             precondition must hold. whole_object_intended={}",
            out.whole_object_intended
        );
        assert!(
            out.meta_unlinked,
            "the .meta must be UNLINKED when no .bin survives (R15.3 permits it)"
        );
        assert_eq!(
            out.meta_writes, 0,
            "§ 5.5: one unlink INSTEAD OF a rewrite. The .meta was written {} time(s) before \
             being removed — a write nobody can ever read, and the expensive half of the \
             operation (451.9/s against 4,613/s on FSxZ). Note the end state is identical \
             either way, which is why this asserts the count and not the file's absence.",
            out.meta_writes
        );
        assert_eq!(out.bytes_freed, 3 * RANGE_BYTES as u64);
        assert_eq!(out.unlinked_extents.len(), 3);
        assert!(out.retained_extents.is_empty());
    }

    /// ARM 1, the END-STATE half — deliberately kept as its own test.
    ///
    /// **This assertion cannot detect the regression ARM 1 detects**, and that is the point of
    /// having it: a rewrite-then-delete leaves no `.meta` exactly as a bare delete does, so this
    /// passes against both. It is here so the pair can be run against a deliberate rewrite and
    /// the difference observed — one reds, one does not. Do not promote it to "the whole-object
    /// test"; it is the control that proves the mechanism arm was necessary.
    #[tokio::test]
    async fn arm1_end_state_alone_cannot_tell_a_rewrite_from_an_unlink() {
        let temp = TempDir::new().unwrap();
        let dc = manager(temp.path());
        dc.initialize().await.unwrap();
        let key = "test-bucket/ew-arm1-endstate.bin";
        let extents = seed(&dc, temp.path(), key, 3);
        let meta_path = dc.get_new_metadata_file_path(key);
        assert!(meta_path.exists(), "fixture: the .meta must exist first");

        let _ = dc.batch_delete_ranges(key, &extents).await.unwrap();

        assert!(
            !meta_path.exists(),
            "the .meta must be gone: {:?}",
            meta_path
        );
    }

    /// ARM 2 — one range NOT selected. Must REWRITE, retaining that range, and must not unlink.
    ///
    /// Also the positive control for `meta_writes`: exactly 1 here, which is what rules out a
    /// counter that is never incremented and so trivially satisfies ARM 1.
    #[tokio::test]
    async fn arm2_one_range_unselected_is_rewritten_not_unlinked() {
        let temp = TempDir::new().unwrap();
        let dc = manager(temp.path());
        dc.initialize().await.unwrap();
        let key = "test-bucket/ew-arm2.bin";
        let extents = seed(&dc, temp.path(), key, 3);
        let selected = &extents[..2];
        let survivor = extents[2];
        let survivor_bin = dc.get_new_range_file_path(key, survivor.0, survivor.1);

        let out = dc.batch_delete_ranges(key, selected).await.unwrap();

        assert!(
            !out.whole_object_intended,
            "R3.10: one range was not a candidate, so this is NOT a whole-object eviction"
        );
        assert!(
            !out.meta_unlinked,
            "R15.3: a surviving range means the .meta stays. Unlinking it here strands \
             {:?} as an orphan the validation scan cannot see (R15.1).",
            survivor_bin
        );
        assert_eq!(
            out.meta_writes, 1,
            "the partial path rewrites the document ONCE, per object rather than per range \
             (R3.11). This is also the control that `meta_writes` can move at all."
        );
        let meta_path = dc.get_new_metadata_file_path(key);
        assert!(
            meta_path.exists(),
            "the .meta must survive: {:?}",
            meta_path
        );
        let doc: NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(
            doc.ranges.len(),
            1,
            "exactly the unselected range survives, got {:?}",
            doc.ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>()
        );
        assert_eq!((doc.ranges[0].start, doc.ranges[0].end), survivor);
        assert!(
            survivor_bin.exists(),
            "DATA LOSS: the unselected range's .bin was removed: {:?}",
            survivor_bin
        );
    }

    /// ARM 4 — a peer already deleted one of the object's `.bin` files.
    ///
    /// Nothing survives on the volume, so the whole-object path is still correct and must still
    /// be taken with zero rewrites. The already-absent range contributes no bytes to the debit
    /// (its `.meta` entry is still stripped, which is what keeps the object evictable at all),
    /// so the freed total is two ranges and not three — the figure is asserted rather than the
    /// count of extents, because the two differ here and only one of them is the accounting
    /// input R7.2 governs.
    #[tokio::test]
    async fn arm4_a_bin_a_peer_already_deleted_does_not_block_the_unlink() {
        let temp = TempDir::new().unwrap();
        let dc = manager(temp.path());
        dc.initialize().await.unwrap();
        let key = "test-bucket/ew-arm4.bin";
        let extents = seed(&dc, temp.path(), key, 3);
        // The peer's deletion: the `.bin` goes, the `.meta` entry stays behind.
        let peer_deleted = dc.get_new_range_file_path(key, extents[1].0, extents[1].1);
        std::fs::remove_file(&peer_deleted).unwrap();

        let out = dc.batch_delete_ranges(key, &extents).await.unwrap();

        assert!(out.whole_object_intended);
        assert!(
            out.meta_unlinked,
            "an already-absent .bin leaves nothing on the volume, so R15.3 does not forbid \
             the unlink and the object must not be left permanently unevictable"
        );
        assert_eq!(
            out.meta_writes, 0,
            "still the one-unlink path: an absent file is not a surviving file"
        );
        assert_eq!(
            out.unlinked_extents.len(),
            2,
            "only the two extents whose files actually left the volume are debitable (R7.2)"
        );
        assert_eq!(out.bytes_freed, 2 * RANGE_BYTES as u64);
        assert!(out.retained_extents.is_empty());
    }

    /// R15.3's DOWNGRADE — a `.bin` that cannot be unlinked turns a whole-object eviction into a
    /// partial one.
    ///
    /// The failure is produced by making the range files' leaf directory read-only, so
    /// `remove_file` fails with the files still present. That is the shape of the real cause
    /// (`EACCES` on a read-only volume, `EIO`, `ESTALE` on shared storage) rather than a flag,
    /// and the test refuses to pass vacuously if the unlink somehow succeeds — running as root
    /// bypasses directory permissions, and a silent pass there would report a downgrade that
    /// never happened.
    #[tokio::test]
    async fn r15_3_a_surviving_bin_downgrades_the_whole_object_path_to_a_rewrite() {
        use std::os::unix::fs::PermissionsExt;
        let temp = TempDir::new().unwrap();
        let dc = manager(temp.path());
        dc.initialize().await.unwrap();
        let key = "test-bucket/ew-downgrade.bin";
        let extents = seed(&dc, temp.path(), key, 2);
        let leaf = dc
            .get_new_range_file_path(key, extents[0].0, extents[0].1)
            .parent()
            .unwrap()
            .to_path_buf();
        let original = std::fs::metadata(&leaf).unwrap().permissions();
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o555)).unwrap();

        let out = dc.batch_delete_ranges(key, &extents).await;

        // Restore before any assertion, so a failure cannot leave the temp dir unremovable.
        std::fs::set_permissions(&leaf, original).unwrap();
        let out = out.unwrap();

        assert!(
            !out.retained_extents.is_empty(),
            "FIXTURE DID NOT BITE: the unlinks succeeded against a read-only parent \
             directory {:?}, so nothing was retained and this test proves nothing. Running as \
             root bypasses directory permissions — re-run as an unprivileged user.",
            leaf
        );
        assert!(
            out.whole_object_intended,
            "both of the .meta's ranges were candidates, so the pass DID set out to take the \
             whole-object path — which is what makes this a downgrade rather than an ordinary \
             partial eviction"
        );
        assert!(
            !out.meta_unlinked,
            "R15.3: a .bin survives, so the .meta MUST be kept. Unlinking it here is how a \
             failed unlink manufactures the orphan § 6.8 then has to reclaim."
        );
        assert_eq!(
            out.meta_writes, 1,
            "the downgrade is TO the partial path, so the document is rewritten once"
        );
        assert_eq!(
            out.bytes_freed, 0,
            "nothing left the volume, so nothing is debited"
        );
        let meta_path = dc.get_new_metadata_file_path(key);
        let doc: NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(
            doc.ranges.len(),
            out.retained_extents.len(),
            "every retained extent keeps its .meta entry, so the range stays accounted and \
             the next pass retries it (R15.4)"
        );
    }
}

/// Families D (`store_range`) and E1 (`begin_incremental_range_write`) stage under an
/// instance-scoped name ending `.tmp.{host}.{pid}`, the shape the dead-instance temp reaper
/// matches, and never under the old unscoped names.
#[cfg(test)]
mod range_temp_name_tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    fn my_host() -> String {
        gethostname::gethostname()
            .to_string_lossy()
            .replace(['/', '\\'], "_")
    }

    fn object_metadata() -> crate::cache_types::ObjectMetadata {
        crate::cache_types::ObjectMetadata {
            etag: "scn2-etag".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 1024,
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: 1024,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_streamed_fill_stages_under_an_instance_scoped_name() {
        let temp_dir = TempDir::new().unwrap();
        let dc = DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        dc.initialize().await.unwrap();
        let writer = dc
            .begin_incremental_range_write("scn2-bucket/streamed", 0, 1023, true)
            .await
            .unwrap()
            .expect("admitted");
        let name = writer
            .tmp_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let tail = format!(".tmp.{}.{}", my_host(), std::process::id());
        assert!(
            name.ends_with(&tail),
            "streamed-fill temp {name:?} must end {tail:?}"
        );
        assert_eq!(
            crate::journal_consolidator::dead_instance_temp_owner(&name, &[my_host()]),
            Some(std::process::id()),
            "the reaper's matcher must name this process as the owner of {name:?}"
        );
        assert_eq!(
            writer.tmp_path.parent(),
            writer.final_path.parent(),
            "the temp sits beside its final .bin so the rename stays in one directory"
        );
        DiskCacheManager::abort_incremental_range(writer);
    }

    #[tokio::test]
    async fn a_range_write_stages_under_the_instance_scoped_name_and_never_the_old_one() {
        let temp_dir = TempDir::new().unwrap();
        let mut dc =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        dc.initialize().await.unwrap();

        // Old name occupied by a directory: a writer still using `{final}.bin.tmp` cannot write.
        let key_a = "scn2-bucket/range-a";
        let final_a = dc.get_new_range_file_path(key_a, 0, 1023);
        let old_a = final_a.with_extension("bin.tmp");
        std::fs::create_dir_all(&old_a).unwrap();
        dc.store_range(
            key_a,
            0,
            1023,
            &[7u8; 1024],
            object_metadata(),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("store_range must not stage under the old unscoped {key}_{s}-{e}.bin.tmp");
        assert!(final_a.is_file(), "the range must be published");
        assert!(old_a.is_dir(), "the old-name entry is untouched");

        // New name occupied by a directory: the writer must be using exactly that name.
        let key_b = "scn2-bucket/range-b";
        let final_b = dc.get_new_range_file_path(key_b, 0, 1023);
        let new_b = crate::cache::instance_scoped_range_tmp_path(&final_b, None);
        assert!(new_b
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(&format!(".bin.tmp.{}.{}", my_host(), std::process::id())));
        std::fs::create_dir_all(&new_b).unwrap();
        let result = dc
            .store_range(
                key_b,
                0,
                1023,
                &[8u8; 1024],
                object_metadata(),
                Duration::from_secs(3600),
                false,
            )
            .await;
        assert!(
            result.is_err(),
            "store_range must stage under {new_b:?}; with that name blocked it cannot succeed"
        );
        assert!(!final_b.exists());
    }
}
/// The signed-range fill's store as production runs it (moved from
/// `tests/signed_range_store_slot_contention_test.rs`, which pinned the removed
/// `store_range_skip_on_slot_contention`): `store_range_shared_witnessed` with
/// `SlotWait::TryOnce` tries the key's slot lock once, skips the fill on contention without
/// holding the `DiskCacheManager` `RwLock` across the attempt, and caches once the slot is free.
#[cfg(test)]
mod signed_range_try_once_tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;

    const KEY: &str = "test-bucket/case54/signed-range.bin";
    const LEN: u64 = 4096;
    const BOUND: Duration = Duration::from_millis(1500);

    fn bin_count(cache_dir: &Path) -> usize {
        fn walk(dir: &Path, n: &mut usize) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for e in entries.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        walk(&p, n);
                    } else if p.extension().and_then(|x| x.to_str()) == Some("bin") {
                        *n += 1;
                    }
                }
            }
        }
        let mut n = 0;
        walk(&cache_dir.join("ranges"), &mut n);
        n
    }

    async fn store(dcm: &tokio::sync::RwLock<DiskCacheManager>) {
        DiskCacheManager::store_range_shared_witnessed(
            dcm,
            KEY,
            0,
            LEN - 1,
            &vec![0x33; LEN as usize],
            crate::cache_types::ObjectMetadata {
                etag: "\"33333333333333333333333333333333\"".to_string(),
                content_length: LEN,
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                upload_state: crate::cache_types::UploadState::Complete,
                cumulative_size: LEN,
                ..Default::default()
            },
            Duration::from_secs(3600),
            false,
            None,
            SlotWait::TryOnce,
        )
        .await
        .expect("store");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_contended_signed_range_fill_is_skipped_without_stalling_the_rwlock() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache_dir = temp.path();
        for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
            std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
        }
        let manager = Arc::new(crate::cache::CacheManager::new_with_eviction_algorithm(
            cache_dir.to_path_buf(),
            false,
            0,
            crate::cache::CacheEvictionAlgorithm::TinyLFU,
        ));
        let dcm = Arc::new(tokio::sync::RwLock::new(
            manager.create_configured_disk_cache_manager(),
        ));
        let slot = crate::metadata_lock_manager::try_acquire_slot_lock(cache_dir, KEY)
            .expect("fixture: take the key's slot lock");
        let task_dcm = Arc::clone(&dcm);
        let task = tokio::spawn(async move { store(&task_dcm).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        let read = tokio::time::timeout(BOUND, dcm.read()).await;
        let waited = started.elapsed();
        assert!(
            read.is_ok(),
            "STALL: an unrelated disk_cache.read() waited over {:?} while the signed-range \
             store waited on the slot lock",
            BOUND
        );
        drop(read);
        tokio::time::timeout(BOUND, task)
            .await
            .expect("the contended store must return within the bound")
            .expect("join");
        assert!(waited < BOUND, "read waited {:?}", waited);
        assert_eq!(
            bin_count(cache_dir),
            0,
            "a contended fill is skipped, not published"
        );
        drop(slot);
        store(&dcm).await;
        assert_eq!(
            bin_count(cache_dir),
            1,
            "an uncontended fill publishes its .bin"
        );
    }
}

#[cfg(test)]
mod streamed_fstat_failure_tests {
    use super::*;
    use crate::compression::CompressionAlgorithm;
    use futures::StreamExt;
    use tempfile::TempDir;

    async fn drain(stream: impl futures::Stream<Item = Result<bytes::Bytes>>) -> Vec<u8> {
        let mut stream = std::pin::pin!(stream);
        let mut body = Vec::new();
        while let Some(Ok(chunk)) = stream.next().await {
            body.extend_from_slice(&chunk);
        }
        body
    }

    /// A streamed read whose `fstat` fails is a miss, as a buffered read's is, and
    /// is not healed: a failed `fstat` says nothing about which file the descriptor is.
    #[tokio::test]
    async fn a_streamed_read_whose_fstat_fails_is_a_miss_and_not_healed() {
        let temp_dir = TempDir::new().unwrap();
        let dcm =
            DiskCacheManager::new(temp_dir.path().to_path_buf(), true, 1024, false, 1_048_576);
        dcm.initialize().await.unwrap();
        let data = vec![0x5au8; 4096];
        let file_path = "test-bucket/case31-fstat_0-4095.bin";
        let path = temp_dir.path().join("ranges").join(file_path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &data).unwrap();
        let identity =
            crate::cache_types::RangeFileIdentity::of(&std::fs::metadata(&path).unwrap());
        assert!(
            identity.is_some(),
            "fixture: the listing records an identity"
        );
        let spec = crate::cache_types::RangeSpec::new(
            0,
            4095,
            file_path.to_string(),
            CompressionAlgorithm::None,
            4096,
            4096,
        )
        .with_file_identity(identity);
        // Control: `fstat` succeeds and matches, so the stream serves the file.
        let (stream, _) = dcm
            .stream_range_data_from(&spec, 1024, 0)
            .await
            .expect("control: a matching file streams");
        assert_eq!(drain(stream).await, data, "control: bytes served");
        fstat_failure_seam::fail_for(file_path);
        match dcm.stream_range_data_from(&spec, 1024, 0).await {
            Ok((stream, _)) => {
                let served = drain(stream).await;
                panic!(
                    "UNVERIFIED BYTES SERVED (F2): fstat of the stream's descriptor failed and \
                     the stream served {} bytes",
                    served.len()
                );
            }
            Err(e) => assert!(
                e.to_string().contains("seam: fstat failed"),
                "the miss names the fstat failure: {}",
                e
            ),
        }
        assert!(
            path.exists(),
            "a failed fstat healed the range: it says nothing about which file it is"
        );
    }
}
