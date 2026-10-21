//! Metadata Lock Manager
//!
//! Mutual exclusion for `.meta` read-modify-write on shared storage, over a fixed set of lock
//! slots that are created on first use and never unlinked.
//!
//! The protocol is a payload-free `flock` and nothing else: no lock-file contents, no fsync, no
//! read-back, no fence epoch, no heartbeat, no staleness inference, and no lock breaking.
//! Liveness comes from the kernel releasing a `flock` on process death and from the NFSv4.1
//! server releasing it on lease expiry. There is no such thing as a stale `flock`, so a lock
//! that is still held is held by someone live, and breaking it would remove exclusion rather
//! than restore it.
//!
//! # Lease expiry is a safety boundary as well as a liveness mechanism
//!
//! Everything above is a statement about the lock. None of it is a statement about the
//! publish, and that gap is the protocol's one unfenced edge.
//!
//! The `flock` is held on the slot file. The publish it protects writes
//! `{key}.meta.tmp.{host}.{pid}` and then renames it onto `{key}.meta`, which are path
//! operations on other files. Nothing ties the rename's validity to the lock still being held,
//! so a rename issued after the server has released the lock still lands.
//!
//! Reaching that state requires a partition longer than the NFSv4.1 lease AND a peer publish
//! inside it. Instance A acquires the slot lock and reads the `.meta`. A's connection to the
//! shared volume is cut for longer than the lease, and the server releases A's lock. Instance B
//! acquires the same slot, publishes, releases. A reconnects and renames the document it read
//! before the partition, discarding B's publish. A believed throughout that it held the lock,
//! nothing refused its rename, and neither side logs an error.
//!
//! Five callers do not publish by tmp-and-rename. `CacheManager::evict_range` and
//! `DiskCacheManager::{update_range_access_direct, refresh_object_ttl_direct,
//! remove_invalidated_range, delete_stale_range}` rewrite `{key}.meta` in place under the slot
//! lock, with no temp file. The clobber above applies to them unchanged. They also carry a
//! failure mode the rename shape cannot: `std::fs::write` truncates before it writes, so a
//! write that fails partway leaves a zero-length `.meta` where a failed rename would have left
//! the previous document intact. That needs no partition and no peer, only a failed write.
//! The read path absorbs both. `disk_cache::read_and_parse_metadata_blocking` classifies an
//! empty or size-changing document `TransientMiss`, which is an ordinary miss, and
//! `CacheManager::remove_corrupt_metadata` re-parses under the lock before deleting. Of the
//! five, only `remove_invalidated_range` is reachable in production.
//!
//! Nothing detects this, and the reason is structural rather than an oversight. Once `flock`
//! returns, no code performs any further operation on the locked descriptor: the slot file
//! carries no payload, `MetadataLock` exposes no handle to the `File`, and no caller
//! re-validates the lock before publishing. There is therefore no operation left on which a
//! lost lock could surface an error. Only a fence applied at publish time would catch it, and
//! the protocol has none; the fence epoch that once existed was removed because no writer read
//! it before publishing.
//!
//! This is a property of the protocol, not of any one caller. Every slot-locked
//! read-modify-write on the volume is exposed, including
//! `JournalConsolidator::consolidate_object_with_files`.
//!
//! Both statements are true at once, and a reader who takes either alone reaches the wrong
//! conclusion: a `flock` never goes stale, and a lock that never goes stale is still not a
//! fence over the publish it protects — whether that publish is a `rename` or an in-place
//! `write`.
//!
//! # Bounding a wait, without inferring liveness from it
//!
//! Two bounds exist, and they bound different disciplines rather than duplicating one:
//!
//! - **A wait budget**, `cache.shared_storage.lock_acquire_wait_budget`, bounds the *budgeted*
//!   acquisition — `acquire_slot_lock_within_budget`, which polls `try_lock` with an async
//!   sleep between attempts. On expiry the caller gives up having touched nothing.
//! - **A retry count**, `cache.shared_storage.lock_max_retries`, bounds the *retrying*
//!   acquisition — `MetadataLockManager::acquire_lock`, which reattempts a non-blocking
//!   acquisition with backoff.
//!
//! Neither is a lock expiry. Giving up after waiting is not a statement about the holder;
//! taking the lock after waiting would be, and that is what the retired `locks/{key}.lock`
//! sentinel did. A budget that stole the lock on expiry would reintroduce exactly the lost
//! update this protocol replaced, so the word "timeout" is avoided throughout: the value is
//! how long a caller is willing to wait, not how long a holder is allowed to hold.
//!
//! Slots are shared, so two distinct keys can hash to one. See `slot_for_cache_key`.
//!
//! # What one acquisition costs on the volume
//!
//! Three filesystem operations: an `open(2)` of the slot file (`O_CREAT`, no truncate), the
//! `flock(2)`, and the `close(2)` that releases it. The slot directory's `create_dir_all` — a
//! `mkdir(2)` that fails `EEXIST` plus a `stat(2)` — runs once per directory per process, and
//! again only when an open finds the directory gone. Until it ran on every acquisition, so
//! an acquisition was five operations, not three.
//!
//! # Lock ordering
//!
//! Three file-lock namespaces can be held together by a single writer, and a writer that
//! needs more than one takes them in this order and releases them in reverse.
//!
//! **This is not an enumeration of the volume's lock namespaces.** There are at least
//! eleven, and an earlier version of this note said "three ... exist on the cache volume",
//! which a reader takes as complete. The others — `locks/global_eviction.lock`,
//! `locks/initialization.lock`, `size_tracking/{validation,size_state,global_consolidation}.lock`,
//! `{journal}.journal.lock`, `capacity_guard`'s `_reservations.lock`, and the retired legacy
//! `{key}.meta.lock` that `disk_cache` still sweeps — are not per-key, which is why an
//! ordering note aimed at per-key deadlock leaves them out. Only the completeness claim was
//! wrong; the order below is sound and is obeyed.
//!
//! Note also that the journal-append lock is taken *inside* a slot-lock section in places
//! (`CacheManager::debit_removed_ranges` reaches the consolidator's journal append), so it
//! is ordered after the slot lock without appearing in the list:
//!
//! 1. `mpus_in_progress/{upload_id}/upload.lock` — the multipart upload's own lock, held
//!    by the signed-write path for the life of a **completion**. It has not guarded a part
//!    write since 2.6.0: a part's finalize-and-record critical section takes the per-part
//!    `mpus_in_progress/{upload_id}/part{N}.lock` instead
//!    (`SignedPutHandler::record_part_blocking` records why), which is a fourth namespace
//!    and is likewise not per-key.
//! 2. the metadata slot lock in this module — innermost, and the only one that guards a
//!    `.meta` read-modify-write.
//!
//! A third namespace sat between the two until it was retired: `locks/{key}.lock`, a
//! per-key write sentinel written as a `create_new(true)` JSON file rather than a `flock`.
//! It excluded nothing — no `.meta` writer observed it — and its only real effect was to
//! make `CacheManager::is_cache_entry_active` report a key busy while a publication held
//! it. Nothing creates or reads one now.
//!
//! **Never hold two metadata slot locks at once**, even for two different keys: two keys
//! can share a slot, and `flock` conflicts between two descriptors of the same file even
//! within one process, so the second acquisition deadlocks against the first roughly once
//! in `META_LOCK_SLOTS` requests. A function that mutates two keys' metadata acquires for
//! the first, releases, then acquires for the second — see
//! `CacheManager::invalidate_cache_hierarchy` for the shape, and
//! `nested_meta_lock_acquisition_is_refused` for the assertion that a nested acquisition
//! is refused rather than granted.

use crate::{ProxyError, Result};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tracing::{debug, error};

/// Slot-lock acquisitions granted, process-wide since start.
///
/// Counts acquisitions that produced a `MetadataLock` guard, which is every acquisition
/// made through this module. The counters live here rather than on `MetricsManager`
/// because the acquisition primitives are synchronous free functions with no metrics
/// handle to reach — some of them run inside blocking-pool closures — and this module
/// holds no `MetricsManager` reference at all. Same channel as
/// `crate::tee_stream::backpressure_events_total_get`.
static SLOT_ACQUISITIONS_SUCCESSFUL: AtomicU64 = AtomicU64::new(0);

/// Slot-lock acquisition ATTEMPTS refused, process-wide since start.
///
/// One per refused attempt, not one per giving-up caller: `MetadataLockManager::
/// acquire_lock` retries, so a contended acquisition that succeeds on its third
/// attempt adds two here and one to `SLOT_ACQUISITIONS_SUCCESSFUL`. That makes this
/// the contention signal and `SLOT_RETRY_EXHAUSTED` the failure signal, and it is why
/// this counter is always greater than or equal to that one.
static SLOT_ACQUISITIONS_FAILED: AtomicU64 = AtomicU64::new(0);

/// Callers that exhausted `lock_max_retries` and gave up, process-wide since start.
///
/// This is the retry-exhaustion event, and it is **not** a timeout: the protocol has no
/// timeout and never infers liveness from elapsed time, so what bounds a caller's wait
/// is the retry count. See `MetadataLockManager::acquire_lock`.
static SLOT_RETRY_EXHAUSTED: AtomicU64 = AtomicU64::new(0);

/// Test seam: make the next `n` `run_locked_blocking` calls for a key return the
/// retry-exhaustion error without trying the lock, as a slot held through every retry would.
#[cfg(test)]
pub(crate) mod forced_retry_exhaustion {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    fn registry() -> &'static Mutex<HashMap<String, u32>> {
        static R: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();
        R.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(crate) fn arm(cache_key: &str, n: u32) {
        registry().lock().unwrap().insert(cache_key.to_string(), n);
    }

    pub(crate) fn take(cache_key: &str) -> bool {
        let mut r = registry().lock().unwrap();
        match r.get_mut(cache_key) {
            Some(n) if *n > 0 => {
                *n -= 1;
                if *n == 0 {
                    r.remove(cache_key);
                }
                true
            }
            _ => false,
        }
    }
}

/// Cumulative microseconds that slot locks were held, process-wide since start.
///
/// Accumulated in microseconds and published in milliseconds so that a critical
/// section shorter than a millisecond still contributes rather than truncating to
/// zero — most `.meta` read-modify-write sections are a few milliseconds on shared
/// storage, but not all of them are.
static SLOT_HOLD_TIME_US_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Blocking acquisitions that gave up because the wait budget expired, process-wide.
///
/// The failure signal for `acquire_slot_lock_within_budget`. Distinct from
/// `SLOT_RETRY_EXHAUSTED`, which is the same event on the *retrying* path: this one
/// counts a caller bounded by elapsed time, that one a caller bounded by attempt
/// count, and a deployment normally produces both.
///
/// Expiry is a give-up, never a steal. The lock stays with whoever holds it.
static SLOT_WAIT_BUDGET_EXPIRED: AtomicU64 = AtomicU64::new(0);

/// Wait budget for a blocking slot-lock acquisition, in milliseconds.
///
/// Seeded once from `cache.shared_storage.lock_acquire_wait_budget` by
/// `set_slot_acquire_wait_budget`, and read by `acquire_slot_lock_within_budget`. It lives
/// here as a process-wide value rather than on `MetadataLockManager` for the same reason the
/// counters above do: the acquisition primitives are synchronous free functions, some of them
/// running inside blocking-pool closures, with no handle to reach a config through.
///
/// The initial value is the shipped default rather than zero, so a process that never calls
/// the setter — every unit test, and any future caller that forgets — gets the documented
/// budget instead of an instantly-expiring one.
static SLOT_ACQUIRE_WAIT_BUDGET_MS: AtomicU64 = AtomicU64::new(5_000);

/// Slot-lock acquisitions granted since process start.
pub fn metadata_lock_acquisitions_successful() -> u64 {
    SLOT_ACQUISITIONS_SUCCESSFUL.load(Ordering::Relaxed)
}

/// Slot-lock acquisition attempts refused since process start.
///
/// Attempts, not callers — see `SLOT_ACQUISITIONS_FAILED`.
pub fn metadata_lock_acquisitions_failed() -> u64 {
    SLOT_ACQUISITIONS_FAILED.load(Ordering::Relaxed)
}

/// Callers that exhausted their retries and gave up since process start.
pub fn metadata_lock_retry_exhausted_total() -> u64 {
    SLOT_RETRY_EXHAUSTED.load(Ordering::Relaxed)
}

/// Cumulative milliseconds slot locks were held since process start.
///
/// A cumulative **sum**, not a mean. Divide by
/// `metadata_lock_acquisitions_successful` for a mean hold time, accepting that the
/// divisor also counts any lock still held at the moment of the read.
pub fn metadata_lock_hold_time_ms_total() -> u64 {
    SLOT_HOLD_TIME_US_TOTAL.load(Ordering::Relaxed) / 1_000
}

/// Blocking acquisitions that gave up because the wait budget expired, since process start.
pub fn metadata_lock_wait_budget_expired_total() -> u64 {
    SLOT_WAIT_BUDGET_EXPIRED.load(Ordering::Relaxed)
}

/// Set the process-wide wait budget for blocking slot-lock acquisitions.
///
/// Called once during configuration load. Later calls simply replace the value; nothing
/// caches it, so a change takes effect on the next acquisition.
pub fn set_slot_acquire_wait_budget(budget: Duration) {
    SLOT_ACQUIRE_WAIT_BUDGET_MS.store(budget.as_millis() as u64, Ordering::Relaxed);
}

/// The process-wide wait budget for blocking slot-lock acquisitions.
pub fn slot_acquire_wait_budget() -> Duration {
    Duration::from_millis(SLOT_ACQUIRE_WAIT_BUDGET_MS.load(Ordering::Relaxed))
}

/// Number of metadata lock slots.
///
/// The lock namespace is a fixed set of files, created on first use and **never unlinked
/// by anyone**. A cache key maps to a slot by hash, so two distinct keys can share one —
/// see `slot_for_cache_key` for the collision arithmetic and its consequence.
pub const META_LOCK_SLOTS: u64 = 4096;

/// Directory holding the fixed slot set, relative to `metadata/`.
pub const META_LOCK_DIR: &str = "_meta_locks";

/// Map a cache key to its lock slot.
///
/// BLAKE3 over the key, first 8 bytes read little-endian, modulo `META_LOCK_SLOTS`.
/// BLAKE3 is already this crate's sharding hash (`disk_cache::get_sharded_path`), so the
/// distribution is uniform for the same reason that one is.
///
/// # Slot collisions are latency, never corruption
///
/// Two keys sharing a slot serialise against each other. Each holder still gets genuine
/// mutual exclusion — it simply excludes one more key than it strictly needs to. The
/// critical section is a single `.meta` read-modify-write, so the cost of a collision is
/// one such section's duration, and the protocol's correctness does not depend on `N`.
pub fn slot_for_cache_key(cache_key: &str) -> u64 {
    let digest = blake3::hash(cache_key.as_bytes());
    let bytes = digest.as_bytes();
    let mut first_eight = [0u8; 8];
    first_eight.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(first_eight) % META_LOCK_SLOTS
}

// `open_slot_file_for` used to live here: it handed a caller the slot path and an open
// descriptor so the caller could `flock` and unlock it itself, without ever constructing a
// `MetadataLock`. Its eight callers were the whole reason the published lock figures covered 25
// of 33 acquisitions, and they now hold guards, so it has no callers and is gone.
//
// It is deliberately not kept "in case someone needs it". `open_slot_file` below is private and
// `MetadataLock::new` is private, so with the public helper removed there is no way to lock a
// slot from outside this module except through an acquisition function that counts it. That is
// what keeps the funnel complete — a property of the module's surface rather than of anyone
// remembering.

/// Absolute path of the slot file for a cache key.
pub fn slot_lock_path(cache_dir: &Path, cache_key: &str) -> PathBuf {
    cache_dir
        .join("metadata")
        .join(META_LOCK_DIR)
        .join(format!("{:04}.lock", slot_for_cache_key(cache_key)))
}

/// Test seam: `create_dir_all` calls made for each slot directory.
#[cfg(test)]
static SLOT_DIR_CREATE_CALLS_FOR_TESTS: std::sync::Mutex<
    Option<std::collections::HashMap<PathBuf, u64>>,
> = std::sync::Mutex::new(None);

/// `create_dir_all` for a slot directory, counted under test.
fn create_slot_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    {
        *SLOT_DIR_CREATE_CALLS_FOR_TESTS
            .lock()
            .unwrap()
            .get_or_insert_with(Default::default)
            .entry(dir.to_path_buf())
            .or_insert(0) += 1;
    }
    std::fs::create_dir_all(dir)
}

/// Slot directories this process has already created or found present.
///
/// The directory is created once per volume and never removed by this crate, so re-running
/// `create_dir_all` on every acquisition bought nothing but a `mkdir(2)` that fails `EEXIST` and
/// the `stat(2)` that follows it. Cached per directory rather than as one flag, because a process
/// can hold more than one `cache_dir` (every test does).
static KNOWN_SLOT_DIRS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<PathBuf>>> =
    std::sync::OnceLock::new();

fn known_slot_dirs() -> &'static std::sync::Mutex<std::collections::HashSet<PathBuf>> {
    KNOWN_SLOT_DIRS.get_or_init(Default::default)
}

/// Create `dir` if this process has not already, tolerating a peer that created it first.
fn ensure_slot_dir(dir: &Path, force: bool) -> Result<()> {
    if !force && known_slot_dirs().lock().unwrap().contains(dir) {
        return Ok(());
    }
    if let Err(e) = create_slot_dir(dir) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(ProxyError::CacheError(format!(
                "Failed to create metadata lock slot directory: path={:?}, error={}",
                dir, e
            )));
        }
    }
    known_slot_dirs().lock().unwrap().insert(dir.to_path_buf());
    Ok(())
}

fn open_slot_file_once(lock_file_path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(lock_file_path)
}

/// Open (creating if absent) the slot file for a lock acquisition.
///
/// Three properties are load-bearing on shared storage:
///
/// - The slot directory is created once per directory per process, tolerating
///   `AlreadyExists` because a peer may create it between our check and our call, and then
///   remembered, so an acquisition does not pay a `mkdir(2)` + `stat(2)` every time.
/// - An `ENOENT` at open means the directory was removed after we created it (an operator
///   wiping the volume, a test tearing down). The directory is recreated and the open retried
///   once, so a deleted directory costs one extra `mkdir` rather than an error.
/// - `create(true).truncate(false)`, so both instances converge on ONE inode instead of one
///   truncating the file the other is locking. Truncation would also be pointless: the slot
///   carries no payload.
fn open_slot_file(lock_file_path: &Path, cache_key: &str) -> Result<File> {
    let parent = lock_file_path.parent();
    if let Some(parent) = parent {
        ensure_slot_dir(parent, false)?;
    }

    let opened = match open_slot_file_once(lock_file_path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && parent.is_some() => {
            let parent = parent.expect("checked above");
            debug!(
                "Metadata lock slot directory vanished, recreating: path={:?}",
                parent
            );
            ensure_slot_dir(parent, true)?;
            open_slot_file_once(lock_file_path)
        }
        other => other,
    };

    opened.map_err(|e| {
        error!(
            "Failed to open metadata lock slot: cache_key={}, path={:?}, error={}",
            cache_key, lock_file_path, e
        );
        ProxyError::CacheError(format!(
            "Failed to open metadata lock slot: path={:?}, error={}",
            lock_file_path, e
        ))
    })
}

/// Acquire the slot lock for `cache_key` with a single non-blocking attempt.
///
/// This is the one acquisition primitive. `MetadataLockManager` wraps it for retries, and
/// `disk_cache` calls it directly, so both share one namespace and one protocol.
///
/// The slot directory is created on this process's first use of it, tolerating a peer that
/// created it first, and recreated if an open finds it gone; the slot file is opened `create(true).truncate(false)` so two instances racing to create
/// it converge on one inode rather than one truncating the other's.
///
/// No payload is written, nothing is fsynced, nothing is read back, and the file is never
/// unlinked. A held lock is released by closing the descriptor; the kernel does that on
/// process death and the NFSv4.1 server does it on lease expiry, which is the whole of the
/// liveness story and not the whole story. Server-side release on lease expiry is also a safety
/// boundary, because the `rename` a caller publishes with is not fenced by this lock: see the
/// module's "Lease expiry is a safety boundary as well as a liveness mechanism".
///
/// # Nesting is forbidden
///
/// Holding two metadata locks at once can self-deadlock, because two keys may share a slot
/// and `flock` conflicts between two descriptors of the same file even within one process.
/// Acquire sequentially, never nested. `nested_meta_lock_acquisition_is_refused` asserts the
/// refusal so a nested acquisition fails loudly instead of hanging.
pub fn try_acquire_slot_lock(cache_dir: &Path, cache_key: &str) -> Result<MetadataLock> {
    let lock_file_path = slot_lock_path(cache_dir, cache_key);
    let lock_file = open_slot_file(&lock_file_path, cache_key)?;

    match fs4::FileExt::try_lock(&lock_file) {
        Ok(()) => {
            SLOT_ACQUISITIONS_SUCCESSFUL.fetch_add(1, Ordering::Relaxed);
            debug!(
                "Acquired metadata slot lock: cache_key={}, path={:?}",
                cache_key, lock_file_path
            );
            Ok(MetadataLock::new(
                lock_file_path,
                lock_file,
                cache_key.to_string(),
            ))
        }
        Err(fs4::TryLockError::WouldBlock) => {
            SLOT_ACQUISITIONS_FAILED.fetch_add(1, Ordering::Relaxed);
            Err(ProxyError::LockContention(format!(
                "Metadata slot lock is held: cache_key={}, path={:?}",
                cache_key, lock_file_path
            )))
        }
        Err(fs4::TryLockError::Error(e)) => {
            SLOT_ACQUISITIONS_FAILED.fetch_add(1, Ordering::Relaxed);
            Err(ProxyError::CacheError(format!(
                "Failed to lock metadata slot: cache_key={}, path={:?}, error={}",
                cache_key, lock_file_path, e
            )))
        }
    }
}

/// Acquire the slot lock for `cache_key`, blocking until it is granted.
///
/// The blocking counterpart of `try_acquire_slot_lock`, for synchronous call sites that
/// previously took their own inline blocking `flock`. Same namespace, same slot file, same
/// absence of ceremony.
///
/// # This function is UNBOUNDED
///
/// It waits in the kernel for as long as the holder holds, parking its thread. No production
/// path uses it: async callers use `acquire_slot_lock_within_budget`, which polls with
/// non-blocking attempts so a waiter holds no thread, at one shared-storage round trip per
/// attempt.
///
/// Test-only: `acquire_slot_lock_within` was its last production caller and now
/// polls instead. Tests still use it to hold a slot as a peer would.
#[cfg(test)]
pub(crate) fn acquire_slot_lock_blocking(
    cache_dir: &Path,
    cache_key: &str,
) -> Result<MetadataLock> {
    let lock_file_path = slot_lock_path(cache_dir, cache_key);
    let lock_file = open_slot_file(&lock_file_path, cache_key)?;

    fs4::FileExt::lock(&lock_file).map_err(|e| {
        SLOT_ACQUISITIONS_FAILED.fetch_add(1, Ordering::Relaxed);
        ProxyError::CacheError(format!(
            "Failed to lock metadata slot: cache_key={}, path={:?}, error={}",
            cache_key, lock_file_path, e
        ))
    })?;

    SLOT_ACQUISITIONS_SUCCESSFUL.fetch_add(1, Ordering::Relaxed);
    debug!(
        "Acquired metadata slot lock (blocking): cache_key={}, path={:?}",
        cache_key, lock_file_path
    );
    Ok(MetadataLock::new(
        lock_file_path,
        lock_file,
        cache_key.to_string(),
    ))
}

/// Acquire the slot lock for `cache_key` from an async context, bounded by the configured
/// wait budget.
///
/// This is the acquisition every async caller should use. It does two things the raw blocking
/// primitive cannot: it waits without holding a runtime worker or a blocking-pool thread
/// (see [`acquire_slot_lock_within`]), so a contended slot stalls neither unrelated requests
/// nor its own holder; and it bounds the wait, so a wedged peer cannot hang a request path
/// indefinitely.
///
/// # A budget, not a timeout
///
/// Expiry returns `ProxyError::LockContention` and the caller has touched nothing. It does
/// **not** conclude the holder is dead, and it does not take the lock. That distinction is the
/// whole protocol: the retired per-key sentinel inferred death from elapsed time and broke the
/// lock, which produced two holders and the lost update the lock exists to prevent.
///
/// Retryable, and worth retrying: the condition is transient by construction, because the
/// holder is live and will release.
pub(crate) async fn acquire_slot_lock_within_budget(
    cache_dir: &Path,
    cache_key: &str,
) -> Result<MetadataLock> {
    acquire_slot_lock_within(cache_dir, cache_key, slot_acquire_wait_budget()).await
}

/// First pause between two `try_lock` attempts in [`acquire_slot_lock_within`]. Slot holds
/// are a few milliseconds on shared storage, so the first retry comes soon.
const SLOT_POLL_RETRY_MIN: Duration = Duration::from_millis(2);
/// Longest pause between two attempts: at most 20 shared-storage round trips per second per
/// waiter, and a grant noticed within 50 ms of the release.
const SLOT_POLL_RETRY_MAX: Duration = Duration::from_millis(50);

/// One non-blocking attempt for [`acquire_slot_lock_within`]: `Ok(None)` while the slot is
/// held. Unlike [`try_acquire_slot_lock`] it does not count a refusal, so a budgeted wait
/// moves the counters exactly as the queued `flock` it replaced did: one success when granted,
/// one budget expiry when not.
fn try_slot_lock_uncounted(cache_dir: &Path, cache_key: &str) -> Result<Option<MetadataLock>> {
    let lock_file_path = slot_lock_path(cache_dir, cache_key);
    let lock_file = open_slot_file(&lock_file_path, cache_key)?;
    match fs4::FileExt::try_lock(&lock_file) {
        Ok(()) => {
            SLOT_ACQUISITIONS_SUCCESSFUL.fetch_add(1, Ordering::Relaxed);
            debug!(
                "Acquired metadata slot lock (budgeted): cache_key={}, path={:?}",
                cache_key, lock_file_path
            );
            Ok(Some(MetadataLock::new(
                lock_file_path,
                lock_file,
                cache_key.to_string(),
            )))
        }
        Err(fs4::TryLockError::WouldBlock) => Ok(None),
        Err(fs4::TryLockError::Error(e)) => {
            SLOT_ACQUISITIONS_FAILED.fetch_add(1, Ordering::Relaxed);
            Err(ProxyError::CacheError(format!(
                "Failed to lock metadata slot: cache_key={}, path={:?}, error={}",
                cache_key, lock_file_path, e
            )))
        }
    }
}

/// `acquire_slot_lock_within_budget` with the budget supplied explicitly.
///
/// Exists so a test can drive an acquisition to expiry deterministically rather than depending
/// on process-wide state that another test in the same binary may have changed.
///
/// # No waiter ever parks a blocking thread
///
/// Each attempt is a non-blocking `try_lock` on a blocking thread that returns at once;
/// between attempts the task sleeps asynchronously (2 ms doubling to 50 ms), and the budget
/// bounds the whole wait. A queued kernel `flock` here would park one blocking-pool thread
/// per waiter, and `tokio::time::timeout` abandons a `spawn_blocking` task without cancelling
/// the kernel wait, so every expiry left a thread parked until the holder released. Several
/// holders await `tokio::fs` while they hold a slot (the multipart completion, the
/// `disk_cache` and `cache` publish paths, orphan reclamation). With enough waiters across
/// the 4096 slots to fill the blocking pool, a holder's next `tokio::fs` call queued behind
/// threads parked on its own slot, and nothing could release. Polling holds a pool thread
/// for one `open` plus one `try_lock` only.
///
/// The cost is the queued `flock`'s FIFO handoff: a waiter notices a release on its next
/// attempt rather than at once, and a busy slot is won by whichever waiter polls first.
///
/// An attempt in flight when the budget expires may still win; its guard is returned into a
/// dropped `JoinHandle` and `Drop` releases it at once, so the slot is held for a moment by a
/// caller that has already given up. That costs no correctness: the guard cannot outlive the
/// dropped handle.
pub async fn acquire_slot_lock_within(
    cache_dir: &Path,
    cache_key: &str,
    budget: Duration,
) -> Result<MetadataLock> {
    let poll = async {
        let mut backoff = SLOT_POLL_RETRY_MIN;
        loop {
            let owned_dir = cache_dir.to_path_buf();
            let owned_key = cache_key.to_string();
            let attempt = tokio::task::spawn_blocking(move || {
                try_slot_lock_uncounted(&owned_dir, &owned_key)
            })
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Metadata slot lock task failed: cache_key={}, error={}",
                    cache_key, e
                ))
            })??;
            if let Some(lock) = attempt {
                return Ok(lock);
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(SLOT_POLL_RETRY_MAX);
        }
    };

    match tokio::time::timeout(budget, poll).await {
        Ok(result) => result,
        Err(_) => {
            SLOT_WAIT_BUDGET_EXPIRED.fetch_add(1, Ordering::Relaxed);
            Err(ProxyError::LockContention(format!(
                "Metadata slot lock not acquired within the {}ms wait budget: cache_key={}",
                budget.as_millis(),
                cache_key
            )))
        }
    }
}

/// Blocking-pool threads the request-path commit tails may occupy at once, per process.
///
/// The runtime keeps tokio's default `max_blocking_threads` of 512, and that pool also carries
/// every `tokio::fs` call, the streamed-PUT drain and finalize threads, the size-state and
/// ledger writers and the consolidator. A commit tail holds its thread for roughly ten
/// shared-storage round trips, tens of milliseconds on EFS, so 64 threads carry several
/// hundred commits a second, well above the 62 per second per proxy design point, and leave 448
/// threads for everything else. Without the bound, a burst of concurrent PUTs could take the
/// whole pool and queue every other `tokio::fs` call behind it.
pub(crate) const BLOCKING_COMMIT_SLOTS: usize = 64;

fn blocking_commit_slots() -> &'static std::sync::Arc<tokio::sync::Semaphore> {
    static SLOTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    SLOTS.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(BLOCKING_COMMIT_SLOTS)))
}

/// Run synchronous shared-storage work `f` on the blocking pool, holding one of the
/// [`BLOCKING_COMMIT_SLOTS`] permits for as long as `f` runs.
///
/// The permit moves into the blocking task, so a caller that stops waiting (a cancelled
/// request) cannot free it while `f` is still running: the bound counts threads, not waiters.
/// `f` must not wait on anything that a permit holder could be waiting for; the callers are
/// leaf transactions that take at most one non-blocking slot lock.
pub(crate) async fn run_blocking_io<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let permit = blocking_commit_slots()
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| ProxyError::CacheError(format!("Blocking commit slots closed: {}", e)))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await
    .map_err(|e| ProxyError::CacheError(format!("Blocking commit task failed: {}", e)))
}

/// Release `lock` on the blocking pool: its `close` is a shared-storage round trip, and dropping
/// the guard on a runtime worker holds that worker for it. If the blocking task cannot run, the
/// guard is dropped with the failed task, so the lock is released either way.
///
/// Plain `spawn_blocking`, not [`run_blocking_io`]: one `close` holds its thread for one round
/// trip, so it needs no share of the [`BLOCKING_COMMIT_SLOTS`] bound, and taking one would make
/// every release wait behind up to 64 full commit tails while the slot stays held.
pub(crate) async fn release_off_worker(lock: MetadataLock) {
    let _ = tokio::task::spawn_blocking(move || drop(lock)).await;
}

/// Represents an acquired metadata lock.
///
/// The lock is held for as long as this value lives: `Drop` closes the descriptor and does
/// nothing else. It does **not** unlink the slot file — the slot set is fixed and shared, so
/// unlinking it would remove a file other holders are locking, which is the defect this
/// protocol replaced.
#[derive(Debug)]
pub struct MetadataLock {
    /// Path to the slot file
    lock_file_path: PathBuf,
    /// Open file handle (keeps lock active; closing it releases the lock)
    _lock_file: File,
    /// Cache key being locked
    cache_key: String,
    /// When the lock was acquired
    acquired_at: SystemTime,
}

impl MetadataLock {
    /// Create a new metadata lock
    fn new(lock_file_path: PathBuf, lock_file: File, cache_key: String) -> Self {
        Self {
            lock_file_path,
            _lock_file: lock_file,
            cache_key,
            acquired_at: SystemTime::now(),
        }
    }

    /// Get the cache key for this lock
    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }

    /// Get the lock file path
    pub fn lock_file_path(&self) -> &Path {
        &self.lock_file_path
    }

    /// Get when the lock was acquired
    pub fn acquired_at(&self) -> SystemTime {
        self.acquired_at
    }
}

impl Drop for MetadataLock {
    fn drop(&mut self) {
        // The guard's lifetime IS the hold, because `Drop` is what releases the lock —
        // so this is the critical section's duration and not an approximation of it.
        // `elapsed()` fails only if the wall clock stepped backwards during the hold,
        // in which case the hold contributes zero rather than a wrapped value.
        SLOT_HOLD_TIME_US_TOTAL.fetch_add(
            self.acquired_at().elapsed().unwrap_or_default().as_micros() as u64,
            Ordering::Relaxed,
        );

        // Unlock explicitly, then close `_lock_file`: a child forked while the slot file was
        // open keeps the flock alive after the close otherwise. The slot file itself is
        // permanent.
        crate::journal_consolidator::unlock_lock_file(&self._lock_file, "metadata slot lock");
        debug!(
            "Released metadata slot lock: cache_key={}, path={:?}",
            self.cache_key, self.lock_file_path
        );
    }
}

/// How [`MetadataLockManager::run_locked_blocking_logged`] logs giving up after
/// `lock_max_retries`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockExhaustionLog {
    /// ERROR, the default: the caller gives up on the operation.
    Error,
    /// debug: the caller retries the operation and logs its own failures at a bounded rate.
    Debug,
}

/// Metadata lock manager over the fixed slot set.
///
/// Adds a retry loop with exponential backoff on top of `try_acquire_slot_lock`. It holds no
/// lock state of its own — acquisition depends on `cache_dir` and the cache key only — which
/// is why `disk_cache` can call the free function directly and share this namespace without
/// being handed a manager.
pub struct MetadataLockManager {
    /// Cache directory path
    cache_dir: PathBuf,
    /// Maximum retry attempts
    max_retries: u32,
    /// Base backoff duration for exponential backoff
    base_backoff: Duration,
    /// Maximum backoff duration
    max_backoff: Duration,
}

impl MetadataLockManager {
    /// Create a new metadata lock manager.
    ///
    /// The second parameter is accepted for call-site compatibility and unused. This manager's
    /// bound is `max_retries`; the wait budget belongs to the blocking path and is a
    /// process-wide value set by `set_slot_acquire_wait_budget`, because the blocking
    /// primitive is a free function with no manager to reach.
    pub fn new(cache_dir: PathBuf, _lock_acquire_wait_budget: Duration, max_retries: u32) -> Self {
        Self {
            cache_dir,
            max_retries,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }

    /// Acquire the key's metadata slot lock, retrying with exponential backoff.
    ///
    /// A contended slot is retried; there is no stale-lock branch, because a `flock` cannot go
    /// stale — the kernel releases it on process death and the NFSv4.1 server releases it on
    /// lease expiry. A lock that stays held is held by someone live, and breaking it would
    /// remove exclusion rather than restore it.
    ///
    /// Holding the lock is not the same as being fenced while publishing. A caller that renames
    /// a `.meta` under this lock can still have lost the lock to a lease expiry it has no way to
    /// observe: see the module's "Lease expiry is a safety boundary as well as a liveness
    /// mechanism".
    ///
    /// What bounds the wait is `lock_max_retries`, not elapsed time, so giving up here is
    /// **retry exhaustion** and is counted by `metadata_lock_retry_exhausted_total`. There is
    /// no timeout to count.
    ///
    /// Each attempt (one `open` plus one non-blocking `try_lock`) runs on the blocking pool, so
    /// a slow shared-storage `open` does not hold a runtime worker. The guard is returned to the
    /// caller, which releases it wherever it drops it.
    pub async fn acquire_lock(&self, cache_key: &str) -> Result<MetadataLock> {
        let mut attempt = 0;

        while attempt <= self.max_retries {
            let dir = self.cache_dir.clone();
            let key = cache_key.to_string();
            let outcome = tokio::task::spawn_blocking(move || try_acquire_slot_lock(&dir, &key))
                .await
                .map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Metadata slot lock task failed: cache_key={}, error={}",
                        cache_key, e
                    ))
                })?;
            match outcome {
                Ok(lock) => {
                    debug!(
                        "Successfully acquired metadata lock: cache_key={}, attempt={}",
                        cache_key,
                        attempt + 1
                    );
                    return Ok(lock);
                }
                Err(e) => {
                    if attempt == self.max_retries {
                        SLOT_RETRY_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                        error!(
                            "Failed to acquire metadata lock after {} attempts: cache_key={}, error={}",
                            self.max_retries + 1, cache_key, e
                        );
                        return Err(e);
                    }

                    let backoff = self.calculate_backoff(attempt);
                    debug!(
                        "Lock acquisition failed, retrying: cache_key={}, attempt={}, backoff={:.2}ms, error={}",
                        cache_key, attempt + 1, backoff.as_secs_f64() * 1000.0, e
                    );

                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
            }
        }

        unreachable!("Loop should have returned or errored")
    }

    /// Try to acquire the key's metadata slot lock with a single attempt (no retries).
    ///
    /// Returns `Ok(lock)` if acquired, `Err` if the slot is held. Used by journal
    /// consolidation, where skipping a contended key and retrying next cycle is cheaper than
    /// waiting.
    pub async fn try_acquire_lock(&self, cache_key: &str) -> Result<MetadataLock> {
        try_acquire_slot_lock(&self.cache_dir, cache_key)
    }

    /// Acquire the key's slot lock, run `critical` under it and release it, all on ONE
    /// blocking-pool thread behind a [`run_blocking_io`] permit.
    ///
    /// For a synchronous shared-storage transaction on the request path (the streamed PUT's
    /// commit tail): the `open` and `try_lock`, every `std::fs` call `critical` makes, and the
    /// `close` that releases the lock all run off the runtime workers, so a slow volume stalls
    /// the PUT and not `/health`.
    ///
    /// `critical` receives the guard by value and must drop it before it returns; the guard
    /// cannot leave the closure unless `critical` returns it, and no caller does.
    ///
    /// The wait has `acquire_lock`'s shape and bound: each attempt is one non-blocking
    /// `try_lock`; a refused attempt hands `critical` back to the async side, which sleeps the
    /// same backoff without holding a thread or a permit and retries, and `lock_max_retries`
    /// bounds the whole wait. A queued `flock` inside the closure would park a pool thread and a
    /// permit for as long as a peer held the slot, which is the wedge in a new place.
    ///
    /// Never call this while holding another metadata lock: see the never-hold-two rule.
    pub(crate) async fn run_locked_blocking<T, F>(&self, cache_key: &str, critical: F) -> Result<T>
    where
        F: FnOnce(MetadataLock) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.run_locked_blocking_logged(cache_key, LockExhaustionLog::Error, critical)
            .await
    }

    /// [`Self::run_locked_blocking`], with the log level of retry exhaustion chosen by the
    /// caller. A caller that retries the whole transaction itself and rate-limits its
    /// own failure log (the background write-stamp retry) passes [`LockExhaustionLog::Debug`],
    /// so a volume that refuses every lock does not produce one ERROR per attempt. Exhaustion
    /// is counted in `metadata_lock_retry_exhausted_total` either way.
    pub(crate) async fn run_locked_blocking_logged<T, F>(
        &self,
        cache_key: &str,
        exhaustion_log: LockExhaustionLog,
        critical: F,
    ) -> Result<T>
    where
        F: FnOnce(MetadataLock) -> T + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(test)]
        if forced_retry_exhaustion::take(cache_key) {
            return Err(ProxyError::LockContention(format!(
                "test seam: forced slot-lock retry exhaustion: cache_key={}",
                cache_key
            )));
        }
        let mut pending = critical;
        let mut attempt = 0;
        loop {
            let dir = self.cache_dir.clone();
            let key = cache_key.to_string();
            let outcome = run_blocking_io(move || match try_acquire_slot_lock(&dir, &key) {
                Ok(lock) => Ok(pending(lock)),
                Err(e) => Err((pending, e)),
            })
            .await?;
            match outcome {
                Ok(value) => {
                    debug!(
                        "Ran locked blocking section: cache_key={}, attempt={}",
                        cache_key,
                        attempt + 1
                    );
                    return Ok(value);
                }
                Err((back, e)) => {
                    if attempt == self.max_retries {
                        SLOT_RETRY_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                        match exhaustion_log {
                            LockExhaustionLog::Error => error!(
                                "Failed to acquire metadata lock after {} attempts: cache_key={}, error={}",
                                self.max_retries + 1, cache_key, e
                            ),
                            LockExhaustionLog::Debug => debug!(
                                "Failed to acquire metadata lock after {} attempts: cache_key={}, error={}",
                                self.max_retries + 1, cache_key, e
                            ),
                        }
                        return Err(e);
                    }
                    let backoff = self.calculate_backoff(attempt);
                    debug!(
                        "Lock acquisition failed, retrying: cache_key={}, attempt={}, backoff={:.2}ms, error={}",
                        cache_key, attempt + 1, backoff.as_secs_f64() * 1000.0, e
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                    pending = back;
                }
            }
        }
    }

    /// [`Self::run_locked_blocking`] with a single attempt and no retry: the slot lock's `open`
    /// and `try_lock`, `critical` and the release all run on ONE blocking-pool thread behind a
    /// [`run_blocking_io`] permit. A held slot returns `Err(ProxyError::LockContention)` without
    /// running `critical`.
    ///
    /// For request-path side effects that are cheaper to skip than to wait for (the HEAD
    /// metadata store), where `try_acquire_lock` would otherwise do the `open` and `flock` on a
    /// runtime worker. Once the permit is held the blocking task exists, so a caller that stops
    /// waiting does not stop `critical`.
    pub(crate) async fn try_run_locked_blocking<T, F>(
        &self,
        cache_key: &str,
        critical: F,
    ) -> Result<T>
    where
        F: FnOnce(MetadataLock) -> T + Send + 'static,
        T: Send + 'static,
    {
        let dir = self.cache_dir.clone();
        let key = cache_key.to_string();
        run_blocking_io(move || try_acquire_slot_lock(&dir, &key).map(critical)).await?
    }

    /// Path of the slot lock file for a cache key.
    ///
    /// Test-only: production callers go through `slot_lock_path` or the acquisition functions
    /// directly. Kept because the one-namespace test asserts the manager and the free function
    /// resolve to the same path.
    #[cfg(test)]
    fn get_lock_file_path(&self, cache_key: &str) -> PathBuf {
        slot_lock_path(&self.cache_dir, cache_key)
    }

    /// Get metadata file path for a cache key
    ///
    /// Test-only: the slot protocol never needs the `.meta` path, but the malformed-key
    /// rejection it inherits from `get_sharded_path` is worth keeping asserted.
    ///
    /// Returns `ProxyError::CacheError` if `cache_key` is malformed (missing
    /// `bucket/object` separator or containing a rejected bucket segment).
    #[cfg(test)]
    fn get_metadata_file_path(&self, cache_key: &str) -> Result<PathBuf> {
        let base_dir = self.cache_dir.join("metadata");
        crate::disk_cache::get_sharded_path(&base_dir, cache_key, ".meta")
    }

    /// Calculate exponential backoff with jitter
    fn calculate_backoff(&self, attempt: u32) -> Duration {
        // Exponential backoff: base_backoff * 2^attempt
        let exponential_backoff = self.base_backoff * (1u32 << attempt.min(10)); // Cap at 2^10
        let capped_backoff = exponential_backoff.min(self.max_backoff);

        // Add jitter (±25% of the backoff time)
        let jitter_range = capped_backoff.as_millis() / 4; // 25%
        let jitter = fastrand::u64(0..=jitter_range as u64 * 2) as i64 - jitter_range as i64;
        let jittered_millis = (capped_backoff.as_millis() as i64 + jitter).max(0) as u64;

        Duration::from_millis(jittered_millis)
    }
}

/// Get the current time in milliseconds since UNIX epoch
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn slot_dir_create_calls(dir: &Path) -> u64 {
        SLOT_DIR_CREATE_CALLS_FOR_TESTS
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|m| m.get(dir).copied())
            .unwrap_or(0)
    }

    /// `SLOT-LOCK-CREATE-DIR-ALL-HOIST`: the slot directory is created once, not on every
    /// acquisition. Counted per directory, so parallel tests in other temp dirs cannot move it.
    #[test]
    fn slot_directory_is_created_once_not_per_acquisition() {
        let temp_dir = TempDir::new().unwrap();
        let slot_dir = temp_dir.path().join("metadata").join(META_LOCK_DIR);
        let acquisitions = 50;
        for i in 0..acquisitions {
            let lock = try_acquire_slot_lock(temp_dir.path(), &format!("scn1-bucket/key-{i}"))
                .expect("uncontended acquisition");
            drop(lock);
        }
        assert_eq!(
            slot_dir_create_calls(&slot_dir),
            1,
            "create_dir_all ran {} times for {} acquisitions of one slot directory",
            slot_dir_create_calls(&slot_dir),
            acquisitions
        );
    }

    /// The hoist must not make a deleted slot directory fatal: an acquisition after the
    /// directory is removed recreates it and succeeds.
    #[test]
    fn a_deleted_slot_directory_is_recreated_on_the_next_acquisition() {
        let temp_dir = TempDir::new().unwrap();
        let slot_dir = temp_dir.path().join("metadata").join(META_LOCK_DIR);
        drop(try_acquire_slot_lock(temp_dir.path(), "scn1-bucket/a").unwrap());
        std::fs::remove_dir_all(&slot_dir).unwrap();
        let lock = try_acquire_slot_lock(temp_dir.path(), "scn1-bucket/a")
            .expect("acquisition after the slot directory was deleted");
        assert!(lock.lock_file_path().exists());
        drop(lock);
        let blocking = acquire_slot_lock_blocking(temp_dir.path(), "scn1-bucket/b")
            .expect("blocking acquisition after recreation");
        drop(blocking);
    }

    #[test]
    fn test_metadata_lock_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            MetadataLockManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(30), 3);

        assert_eq!(manager.cache_dir, temp_dir.path());
        assert_eq!(manager.max_retries, 3);
    }

    #[tokio::test]
    async fn test_lock_acquisition_basic() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            MetadataLockManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(30), 3);

        let cache_key = "test-bucket/test-object";
        let lock = manager.acquire_lock(cache_key).await.unwrap();

        assert_eq!(lock.cache_key(), cache_key);
        assert!(lock.lock_file_path().exists());

        // The slot file carries NO payload. Asserted, because a payload is exactly what the
        // retired protocol mistook for a liveness record — and an empty file is what its
        // stale-detection branch then judged stale.
        let content = std::fs::read_to_string(lock.lock_file_path()).unwrap();
        assert!(
            content.is_empty(),
            "the slot file must stay empty. content={content:?}"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_file_path_rejects_malformed() {
        // Validates: Requirements 3.4, 5.6
        let temp_dir = TempDir::new().unwrap();
        let manager =
            MetadataLockManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(30), 3);

        let result = manager.get_metadata_file_path("noslash");
        assert!(
            result.is_err(),
            "Malformed cache key must return Err, got Ok"
        );
    }

    // =========================================================================
    // Slot protocol: exclusion, permanence, and the nesting rule
    // =========================================================================

    /// Find a second cache key that hashes to the same slot as `first`.
    ///
    /// Used by the collision and nesting tests. With 4096 slots a match turns up within a
    /// few thousand candidates, so the search is bounded and fast.
    fn colliding_key(first: &str) -> String {
        let target = slot_for_cache_key(first);
        for i in 0..1_000_000u64 {
            let candidate = format!("test-bucket/collide-{i}");
            if candidate != first && slot_for_cache_key(&candidate) == target {
                return candidate;
            }
        }
        panic!("no colliding key found for {first} — is the slot function degenerate?");
    }

    #[test]
    fn slot_function_is_in_range_and_stable() {
        for i in 0..2_000u64 {
            let key = format!("bucket/object-{i}");
            let slot = slot_for_cache_key(&key);
            assert!(slot < META_LOCK_SLOTS, "slot {slot} out of range");
            assert_eq!(
                slot,
                slot_for_cache_key(&key),
                "the slot function must be deterministic"
            );
        }
    }

    /// R1, the defect this protocol replaced.
    ///
    /// # The branch this targets
    ///
    /// The retired protocol's contended path was: `try_lock` returns `WouldBlock`, then
    /// `is_lock_stale` reads an EMPTY payload and returns `Ok(true)`, then `break_stale_lock`
    /// unlinks the live holder's file and a fresh `create(true).truncate(true)` open takes an
    /// independent lock on a new inode. Two holders, neither able to tell.
    ///
    /// The empty payload was not an edge case: `disk_cache::batch_delete_ranges` flocked the
    /// same path and never wrote one, so it was the steady state of every eviction rewrite.
    ///
    /// This asserts the OPPOSITE outcome — the contender is REFUSED — and asserts the
    /// precondition (an empty lock file) rather than assuming it, because a fixture whose
    /// holder writes a payload cannot reach the arm that carried the defect.
    #[test]
    fn a_contended_slot_with_an_empty_lock_file_refuses_the_contender() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/contended-object";

        let holder = try_acquire_slot_lock(temp_dir.path(), cache_key)
            .expect("fixture: the first acquisition must succeed");

        // The precondition, ASSERTED: an empty payload is what the retired stale-detection
        // branch judged stale, and it is the state a payload-writing fixture cannot reach.
        let content = std::fs::read_to_string(holder.lock_file_path()).unwrap();
        assert!(
            content.is_empty(),
            "fixture: the lock file must be EMPTY. content={content:?}"
        );

        let contender = try_acquire_slot_lock(temp_dir.path(), cache_key);
        assert!(
            matches!(contender, Err(ProxyError::LockContention(_))),
            "a live holder must exclude the contender. Under the retired protocol the \
             contender read the empty payload, judged it stale, unlinked the live holder's \
             file and acquired a second independent lock. got={contender:?}"
        );

        assert!(
            holder.lock_file_path().exists(),
            "the refused contender must not have unlinked the holder's slot file"
        );
    }

    /// R3's runtime half: the slot file OUTLIVES its holder.
    ///
    /// `Drop` closes the descriptor and nothing else. The retired `Drop` unlinked in its body,
    /// and because Rust drops fields after the body, the unlink preceded the flock release.
    #[test]
    fn dropping_a_lock_releases_it_without_unlinking_the_slot() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/dropped-object";

        let path = {
            let lock = try_acquire_slot_lock(temp_dir.path(), cache_key).unwrap();
            lock.lock_file_path().to_path_buf()
        };

        assert!(
            path.exists(),
            "the slot file must persist after its holder is dropped: the slot set is fixed \
             and shared, so unlinking it would remove a file other holders lock"
        );

        let again = try_acquire_slot_lock(temp_dir.path(), cache_key);
        assert!(
            again.is_ok(),
            "closing the descriptor must release the lock, got={again:?}"
        );
    }

    /// A slot collision serialises two DISTINCT keys. That is the cost of the design, and it
    /// is latency rather than corruption — asserted here so the trade-off is executable.
    #[test]
    fn two_keys_sharing_a_slot_exclude_each_other() {
        let temp_dir = TempDir::new().unwrap();
        let first = "test-bucket/collision-anchor";
        let second = colliding_key(first);
        assert_eq!(
            slot_for_cache_key(first),
            slot_for_cache_key(&second),
            "fixture: the two keys must share a slot"
        );

        let _held = try_acquire_slot_lock(temp_dir.path(), first).unwrap();
        let other = try_acquire_slot_lock(temp_dir.path(), &second);
        assert!(
            matches!(other, Err(ProxyError::LockContention(_))),
            "a colliding key must WAIT rather than proceed — exclusion is over-broad by \
             design, never absent. got={other:?}"
        );
    }

    /// R4, the nesting rule.
    ///
    /// Two keys can share a slot, so holding two metadata locks at once can self-deadlock.
    /// `flock` conflicts between two descriptors of the same file even within one process, so
    /// a nested acquisition is REFUSED rather than granted — which makes the failure loud and
    /// bounded instead of a hang. This fires if a nested acquisition is introduced.
    #[test]
    fn nested_meta_lock_acquisition_is_refused() {
        let temp_dir = TempDir::new().unwrap();
        let outer_key = "test-bucket/nesting-outer";
        let inner_key = colliding_key(outer_key);

        let _outer = try_acquire_slot_lock(temp_dir.path(), outer_key).unwrap();

        // The deliberate nesting: a second acquisition while the first is still held.
        let inner = try_acquire_slot_lock(temp_dir.path(), &inner_key);
        assert!(
            matches!(inner, Err(ProxyError::LockContention(_))),
            "a nested acquisition on a colliding slot must be refused, not granted. Granting \
             it would mean two holders of one slot inside one process. got={inner:?}"
        );
    }

    #[tokio::test]
    async fn the_manager_and_the_free_function_share_one_namespace() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            MetadataLockManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(30), 0);
        let cache_key = "test-bucket/one-namespace";

        let inline = try_acquire_slot_lock(temp_dir.path(), cache_key).unwrap();
        assert_eq!(
            inline.lock_file_path(),
            manager.get_lock_file_path(cache_key),
            "disk_cache's direct acquisition and the manager's must resolve to ONE path, or \
             they are two protocols in two namespaces excluding nobody"
        );

        let via_manager = manager.try_acquire_lock(cache_key).await;
        assert!(
            via_manager.is_err(),
            "an inline holder must exclude a manager acquirer: this is the exclusion the \
             retired protocol did not provide. got={:?}",
            via_manager.err()
        );
    }

    #[test]
    fn slots_are_distinct_paths_under_one_fixed_directory() {
        let temp_dir = TempDir::new().unwrap();
        let a = slot_lock_path(temp_dir.path(), "bucket/a");
        let expected_dir = temp_dir.path().join("metadata").join(META_LOCK_DIR);
        assert_eq!(a.parent().unwrap(), expected_dir);
        assert_eq!(
            a.extension().and_then(|e| e.to_str()),
            Some("lock"),
            "slot files must keep the .lock suffix"
        );

        let mut seen = std::collections::BTreeSet::new();
        for i in 0..500u64 {
            seen.insert(slot_lock_path(temp_dir.path(), &format!("bucket/k{i}")));
        }
        assert!(
            seen.len() > 1,
            "all keys collapsed to one slot — the hash is not distributing"
        );
    }

    /// `n` cache keys that all hash to distinct slots.
    fn keys_on_distinct_slots(n: usize) -> Vec<String> {
        let mut slots = std::collections::BTreeSet::new();
        let mut keys = Vec::with_capacity(n);
        for i in 0..1_000_000u64 {
            let key = format!("case62-bucket/k{i}");
            if slots.insert(slot_for_cache_key(&key)) {
                keys.push(key);
                if keys.len() == n {
                    return keys;
                }
            }
        }
        panic!("could not find {n} keys on distinct slots");
    }

    /// a slot holder that awaits `tokio::fs` completes while more contended slots
    /// than blocking-pool threads have waiters.
    ///
    /// Eight peers hold eight other slots and eight budgeted waiters queue on them, on a
    /// runtime whose blocking pool has 4 threads. The holder of a ninth slot then needs one
    /// pool thread for a `tokio::fs` read. A per-slot gate would still park one thread per
    /// contended slot, which is why the bound has to be "no waiter parks a thread" rather
    /// than "one waiter per slot".
    ///
    /// # RED SIDE
    ///
    /// With `acquire_slot_lock_within` restored to a blocking `flock` on the pool, the eight
    /// waiters park all 4 threads and the holder's read never runs.
    #[test]
    fn a_slot_holder_awaiting_tokio_fs_completes_while_waiters_outnumber_the_blocking_pool() {
        let temp_dir = TempDir::new().unwrap();
        let keys = keys_on_distinct_slots(9);
        let peers: Vec<MetadataLock> = keys[1..]
            .iter()
            .map(|k| try_acquire_slot_lock(temp_dir.path(), k).unwrap())
            .collect();
        let probe_file = temp_dir.path().join("probe.txt");
        std::fs::write(&probe_file, b"probe").unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let dir = temp_dir.path().to_path_buf();
        let outcome = rt.block_on(async move {
            let holder_lock = acquire_slot_lock_within(&dir, &keys[0], Duration::from_secs(10))
                .await
                .expect("FIXTURE: the holder's slot is uncontended");
            for k in &keys[1..] {
                let d = dir.clone();
                let k = k.clone();
                tokio::spawn(async move {
                    let _ = acquire_slot_lock_within(&d, &k, Duration::from_secs(10)).await;
                });
            }
            // Let the waiters reach their slots before the holder needs the pool.
            tokio::time::sleep(Duration::from_millis(300)).await;
            tokio::time::timeout(Duration::from_secs(3), async move {
                let body = tokio::fs::read(&probe_file).await;
                drop(holder_lock);
                body
            })
            .await
        });
        drop(peers);
        rt.shutdown_background();
        assert!(
            matches!(&outcome, Ok(Ok(body)) if body.as_slice() == b"probe"),
            "STALLED: a slot holder's tokio::fs read did not finish within 3 s while 8 slot \
             waiters faced a 4-thread blocking pool: {:?}",
            outcome
        );
    }
}
#[cfg(test)]
mod explicit_unlock_tests {
    use super::*;
    use crate::journal_consolidator::explicit_unlock_tests::{child_holding, peer_can_lock};
    use tempfile::TempDir;
    /// dropping a `MetadataLock` frees the slot flock while a child holds the descriptor.
    ///
    /// # RED SIDE
    ///
    /// With the `unlock_lock_file` call in `Drop for MetadataLock` removed, the final assertion
    /// fails.
    #[test]
    fn a_dropped_metadata_lock_frees_the_flock_while_a_child_holds_the_descriptor() {
        let tmp = TempDir::new().unwrap();
        let key = "test-bucket/case63-key";
        let lock = try_acquire_slot_lock(tmp.path(), key).unwrap();
        let path = lock.lock_file_path().to_path_buf();
        let mut child = child_holding(&lock._lock_file);
        assert!(!peer_can_lock(&path), "fixture: the slot lock must be held");
        drop(lock);
        let freed = peer_can_lock(&path);
        let _ = child.kill();
        let _ = child.wait();
        assert!(
            freed,
            "dropping a MetadataLock must unlock despite the child's copy of the descriptor"
        );
    }
}
