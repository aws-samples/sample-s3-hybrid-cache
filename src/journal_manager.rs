//! Journal Manager Module
//!
//! Provides write-ahead journaling for metadata resilience in the atomic metadata writes system.
//! Supports per-instance journal files co-located with metadata files and integration with
//! multipart upload cleanup.

use crate::cache_types::RangeSpec;
use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;
use tracing::{debug, error};

/// Red-side lever (a): restore the whole-file read-modify-write append (read the
/// journal, write `.journal.tmp`, rename over it).
#[cfg(test)]
const RMW_APPEND_LEVER: bool = false;

/// Red-side lever (b): skip the torn-tail newline guard in the `O_APPEND` path.
#[cfg(test)]
const SKIP_TORN_TAIL_GUARD_LEVER: bool = false;

/// How many times [`lock_journal_lock_file`] reopens a lock file that was unlinked between
/// its open and its lock before giving up.
const JOURNAL_LOCK_REOPEN_ATTEMPTS: usize = 4;

/// True when `lock_path` still names the inode `file` was opened on.
///
/// A lock file can be unlinked by `cleanup_stale_lock_files` (only while that cleanup holds
/// its flock) after another task opened it but before that task locked it. The late locker
/// then holds a flock on a dead inode that no later opener can see, so it must detect the
/// unlink and reopen.
pub(crate) fn lock_file_still_named(file: &std::fs::File, lock_path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (file.metadata(), std::fs::metadata(lock_path)) {
            (Ok(held), Ok(named)) => held.dev() == named.dev() && held.ino() == named.ino(),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (file, lock_path);
        true
    }
}

/// Open `lock_path` (creating it) and take its flock, returning the held file.
///
/// Every holder of a `.journal.lock` goes through here, so a lock file unlinked by the
/// stale-lock cleanup cannot split mutual exclusion across two inodes: after the flock is
/// granted, the path is re-checked to still name the locked inode, and a mismatch drops the
/// lock and reopens.
///
/// `blocking = false` returns `Ok(None)` when another holder has the lock (or the lock
/// call itself fails), which callers treat as busy. `blocking = true` waits. Either mode
/// returns `Ok(None)` if the file kept being unlinked for every attempt. An open failure is
/// `Err`.
pub(crate) fn lock_journal_lock_file(
    lock_path: &Path,
    blocking: bool,
) -> std::io::Result<Option<std::fs::File>> {
    for _ in 0..JOURNAL_LOCK_REOPEN_ATTEMPTS {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(lock_path)?;
        if blocking {
            fs4::FileExt::lock(&file)?;
        } else {
            match fs4::FileExt::try_lock(&file) {
                Ok(()) => {}
                Err(fs4::TryLockError::WouldBlock) => return Ok(None),
                Err(fs4::TryLockError::Error(e)) => {
                    debug!(
                        "Journal lock acquisition I/O error: path={:?}, error={}",
                        lock_path, e
                    );
                    return Ok(None);
                }
            }
        }
        if lock_file_still_named(&file, lock_path) {
            return Ok(Some(file));
        }
        debug!(
            "Journal lock file was unlinked between open and lock, reopening: path={:?}",
            lock_path
        );
    }
    Ok(None)
}

/// Append newline-terminated `lines` to `{instance_id}.journal` in `journals_dir` under its
/// `.journal.lock`, for a writer outside [`JournalManager`] (the cache-hit buffer flush).
///
/// The lock is tried `attempts` times, `retry_delay` apart. A [`JournalManager`] append holds
/// it for one short write, so a brief retry nearly always lands on the primary journal; a
/// consolidator cleanup holds it across a whole-journal read and rewrite, so when every
/// attempt finds it busy the lines go to a uniquely named `{instance_id}:{nanos}.journal`
/// instead, as [`JournalManager::append_range_entry`] does. Writing the primary without the
/// lock would let cleanup's rename discard the lines it had not read.
///
/// With `sync`, the written file is `fdatasync`ed before the lock is released; a sync
/// failure is logged and not returned, because the bytes are already readable.
/// Returns the path the lines were written to.
pub(crate) async fn append_lines_locked_or_fresh(
    journals_dir: &Path,
    instance_id: &str,
    lines: &str,
    attempts: usize,
    retry_delay: std::time::Duration,
    sync: bool,
) -> std::io::Result<PathBuf> {
    let primary = journals_dir.join(format!("{}.journal", instance_id));
    let lock_path = primary.with_extension("journal.lock");
    for attempt in 0..attempts.max(1) {
        if attempt > 0 {
            tokio::time::sleep(retry_delay).await;
        }
        let Some(_lock) = lock_journal_lock_file(&lock_path, false)? else {
            continue;
        };
        JournalManager::append_lines_to_primary(&primary, lines).await?;
        if sync {
            sync_data_best_effort(&primary).await;
        }
        refresh_ready_record(journals_dir, instance_id);
        return Ok(primary);
    }
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let fresh = journals_dir.join(format!("{}:{}.journal", instance_id, nanos));
    debug!(
        "Journal lock busy after {} attempts, writing fresh journal: path={:?}",
        attempts, fresh
    );
    tokio::fs::write(&fresh, lines).await?;
    if sync {
        sync_data_best_effort(&fresh).await;
    }
    refresh_ready_record(journals_dir, instance_id);
    Ok(fresh)
}

/// Read `cache_key`'s entries from one journal file, in file order.
///
/// Each key a consolidation cycle processes reads the journal files discovery listed for it,
/// and a cache lookup whose range no `.meta` lists yet reads every journal file
/// (`DiskCacheManager::pending_journal_ranges_classified` and its siblings),
/// up to `KEY_CONCURRENCY_LIMIT` keys at once. Loading each file whole and parsing every line
/// held one copy of the journal per key in flight and cost a JSON parse per line per key, so a
/// journal that had fallen behind took memory and CPU in proportion to its size times the key
/// count, and fell further behind; on the lookup path that parse ran on the runtime's worker
/// threads, once per lookup. This reads the file a line at a time on the blocking pool
/// and parses only the lines that contain the key's JSON string: memory is one line, and a
/// line for another key costs a substring search.
///
/// The filter cannot drop one of the key's lines: every writer serializes entries with
/// `serde_json`, which encodes a given string the same way every time, so each of the key's
/// lines contains that encoding, quotes included. A line that contains it for another reason
/// is parsed and compared on `cache_key` as before. Unparseable lines are skipped, as before,
/// and so is a line that is not valid UTF-8, where the whole-file read skipped the file.
pub(crate) async fn read_entries_for_key(
    journal_file: &Path,
    cache_key: &str,
) -> std::io::Result<Vec<JournalEntry>> {
    let path = journal_file.to_path_buf();
    let key = cache_key.to_string();
    tokio::task::spawn_blocking(move || read_entries_for_key_blocking(&path, &key))
        .await
        .map_err(std::io::Error::other)?
}

fn read_entries_for_key_blocking(
    path: &Path,
    cache_key: &str,
) -> std::io::Result<Vec<JournalEntry>> {
    let reader = std::io::BufReader::with_capacity(64 * 1024, std::fs::File::open(path)?);
    scan_lines_for_key(reader, path, cache_key)
}

/// The filtered line scan of [`read_entries_for_key`], over `reader` from wherever it stands.
/// A first line that starts mid-line is a fragment, which does not parse and is skipped.
fn scan_lines_for_key(
    mut reader: impl std::io::BufRead,
    path: &Path,
    cache_key: &str,
) -> std::io::Result<Vec<JournalEntry>> {
    let needle = serde_json::to_string(cache_key).ok();
    let mut buf = Vec::new();
    let mut entries = Vec::new();
    loop {
        buf.clear();
        if reader.read_until(b'\n', &mut buf)? == 0 {
            break;
        }
        let Ok(line) = std::str::from_utf8(&buf) else {
            debug!("Skipping journal line that is not UTF-8: file={:?}", path);
            continue;
        };
        let line = line.trim_end_matches(['\n', '\r']);
        if line.trim().is_empty() || needle.as_deref().is_some_and(|n| !line.contains(n)) {
            continue;
        }
        match serde_json::from_str::<JournalEntry>(line) {
            Ok(entry) => {
                if entry.cache_key == cache_key {
                    entries.push(entry);
                }
            }
            Err(e) => {
                // Parse errors can occur during concurrent writes on shared storage
                debug!(
                    "Failed to parse journal entry: file={:?}, error={}",
                    path, e
                );
            }
        }
    }
    Ok(entries)
}

/// Where discovery found one key's journal lines, file by file. Built by the consolidator's
/// discovery pass and read by [`JournalManager::get_entries_at_offsets`] under the key's lock.
#[derive(Debug, Clone, Default)]
pub(crate) struct KeyLines {
    pub files: Vec<FileLines>,
}

/// One journal file's share of a [`KeyLines`].
#[derive(Debug, Clone)]
pub(crate) struct FileLines {
    /// Shared by every key with lines in the file: one allocation per file per cycle.
    pub path: Arc<Path>,
    /// (device, inode) when discovery opened the file.
    pub identity: (u64, u64),
    /// The file's length when discovery opened it, or the end of the key's last recorded line
    /// if that is further (a line appended while discovery read the file).
    pub len_at_discovery: u64,
    /// (offset, length) of each of the key's lines, ascending; the length includes the newline.
    pub lines: Vec<(u64, u64)>,
}

impl KeyLines {
    /// Record one of the key's lines at `span` in the file at `path`. Discovery reads one file
    /// at a time, so a line in the file of the last [`FileLines`] extends it.
    pub(crate) fn record(
        &mut self,
        path: &Arc<Path>,
        identity: (u64, u64),
        len_at_discovery: u64,
        span: (u64, u64),
    ) {
        let end = span.0 + span.1;
        match self.files.last_mut() {
            Some(file) if Arc::ptr_eq(&file.path, path) => {
                file.lines.push(span);
                file.len_at_discovery = file.len_at_discovery.max(end);
            }
            _ => self.files.push(FileLines {
                path: Arc::clone(path),
                identity,
                len_at_discovery: len_at_discovery.max(end),
                lines: vec![span],
            }),
        }
    }
}

/// (device, inode) of an open file, compared by [`JournalManager::get_entries_at_offsets`] to
/// tell the file discovery read from one renamed over it since.
pub(crate) fn file_identity(md: &std::fs::Metadata) -> (u64, u64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (md.dev(), md.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = md;
        (0, 0)
    }
}

pub(crate) fn read_exact_at(
    file: &std::fs::File,
    buf: &mut [u8],
    offset: u64,
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
    }
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek};
        let mut f = file;
        f.seek(std::io::SeekFrom::Start(offset))?;
        f.read_exact(buf)
    }
}

/// The entry in one recorded line, if it parses as a complete [`JournalEntry`] of `cache_key`.
fn parse_entry_for_key(buf: &[u8], cache_key: &str, path: &Path) -> Option<JournalEntry> {
    let Ok(line) = std::str::from_utf8(buf) else {
        debug!("Skipping journal line that is not UTF-8: file={:?}", path);
        return None;
    };
    let line = line.trim_end_matches(['\n', '\r']);
    if line.trim().is_empty() {
        return None;
    }
    match serde_json::from_str::<JournalEntry>(line) {
        Ok(entry) if entry.cache_key == cache_key => Some(entry),
        Ok(_) => None,
        Err(e) => {
            debug!(
                "Failed to parse journal entry: file={:?}, error={}",
                path, e
            );
            None
        }
    }
}

/// Two of a key's recorded lines whose gap is below this many bytes are read with one positioned
/// read (R2.9). 64 KiB is one default `BufReader` buffer, the unit the filtered scan already reads
/// in: a gap below it costs at most that many discarded bytes per kept line, while each read
/// saved is one call and, on NFS, can be one READ round trip.
const SPAN_BATCH_GAP: u64 = 64 * 1024;

/// Longest one batched read, in bytes: 1 MiB, the `rsize` Amazon EFS mounts use, so a batch is
/// at most one NFS READ. A key's read reuses one buffer, which `Vec` growth keeps under twice
/// this however busy the key (a single line longer than this is read whole), so the
/// `KEY_CONCURRENCY_LIMIT` (32) keys of a cycle hold under 64 MiB of it, outside
/// `max_inflight_buffer_bytes`. `consolidation_journal_offset_index_cost_test` (its busy arms)
/// measures the cycle's figure at 32 busy keys and asserts that bound.
const SPAN_BATCH_MAX: u64 = 1024 * 1024;

/// Span reads ([`read_key_lines_blocking`] calls made for a cycle's keys) running now, and the
/// most that have run at once since [`reset_span_reads_peak`]. Read by
/// `consolidation_journal_offset_index_cost_test`, an integration test that cannot see
/// `cfg(test)` items; two relaxed atomic operations per key read.
static SPAN_READS_IN_FLIGHT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPAN_READS_PEAK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Most span reads that ran at once since the last [`reset_span_reads_peak`].
#[doc(hidden)]
pub fn span_reads_peak() -> u64 {
    SPAN_READS_PEAK.load(std::sync::atomic::Ordering::Relaxed)
}
/// Set the peak back to the reads running now.
#[doc(hidden)]
pub fn reset_span_reads_peak() {
    SPAN_READS_PEAK.store(
        SPAN_READS_IN_FLIGHT.load(std::sync::atomic::Ordering::Relaxed),
        std::sync::atomic::Ordering::Relaxed,
    );
}
#[cfg(test)]
thread_local! {
    /// Positioned reads [`read_key_lines_blocking`] made on this thread.
    static POSITIONED_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The body of [`JournalManager::get_entries_at_offsets`], on a blocking thread.
fn read_key_lines_blocking(cache_key: &str, files: &[FileLines]) -> Vec<JournalEntry> {
    let mut entries = Vec::new();
    let mut buf = Vec::new();
    let mut from_spans = Vec::new();
    for file_lines in files {
        let path = &*file_lines.path;
        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) => {
                debug!("Failed to read journal file: file={:?}, error={}", path, e);
                continue;
            }
        };
        // The spans hold only while this is the file discovery read and it is no shorter: a
        // rewrite renames a new file over the path, and a truncate shrinks it. `len_now` is the
        // file's length when it is unchanged.
        let len_now = file
            .metadata()
            .ok()
            .filter(|md| {
                file_identity(md) == file_lines.identity && md.len() >= file_lines.len_at_discovery
            })
            .map(|md| md.len());
        let unchanged = len_now.is_some();
        // On an unchanged file every recorded span is one of the key's lines, so a span that
        // yields none (past the end, a fragment, another key's line) means the file changed in
        // place: truncated, then regrown to at least its discovery length. A newer line of the
        // key can then sit at a span while an older one sits where no span reaches, so the entries
        // taken from this file's spans are discarded and the file is rescanned: a file's
        // entries come from its spans or from one rescan, never both.
        from_spans.clear();
        let mut spans_hold = unchanged;
        // Nearby spans are read together (R2.9): one positioned read covers a run of spans whose
        // gaps are each below `SPAN_BATCH_GAP`, up to `SPAN_BATCH_MAX` bytes, and each span is
        // then cut from the buffer at its recorded offset and length. The bytes between spans are
        // discarded unread, so a span is accepted exactly as when read alone.
        let lines = &file_lines.lines;
        let mut next = 0;
        'batches: while unchanged && next < lines.len() {
            let base = lines[next].0;
            let mut end = base + lines[next].1;
            let mut last = next + 1;
            while let Some(&(offset, len)) = lines.get(last) {
                if offset.saturating_sub(end) >= SPAN_BATCH_GAP
                    || offset + len - base > SPAN_BATCH_MAX
                {
                    break;
                }
                end = offset + len;
                last += 1;
            }
            buf.resize((end - base) as usize, 0);
            #[cfg(test)]
            POSITIONED_READS.with(|c| c.set(c.get() + 1));
            if let Err(e) = read_exact_at(&file, &mut buf, base) {
                debug!(
                    "Journal line read failed, rescanning the file: file={:?}, offset={}, len={}, error={}",
                    path, base, end - base, e
                );
                spans_hold = false;
                break;
            }
            for &(offset, len) in &lines[next..last] {
                let at = (offset - base) as usize;
                match parse_entry_for_key(&buf[at..at + len as usize], cache_key, path) {
                    Some(entry) => from_spans.push(entry),
                    None => {
                        debug!(
                            "Journal line no longer holds the key's entry, rescanning the file: file={:?}, offset={}",
                            path, offset
                        );
                        spans_hold = false;
                        break 'batches;
                    }
                }
            }
            next = last;
        }
        if spans_hold {
            entries.append(&mut from_spans);
            // Lines appended since discovery (R2.10): scan from the discovery length to the end
            // for the key's lines, as the whole-file read did. Every span ends at or before
            // `len_at_discovery`, so no line comes from both. A line that straddles it (one
            // discovery saw half of) starts the scan as a fragment, is skipped, and is read by
            // the next cycle.
            if let Some(len_now) = len_now.filter(|&n| n > file_lines.len_at_discovery) {
                use std::io::{Seek, SeekFrom};
                let mut tail = &file;
                let found = tail
                    .seek(SeekFrom::Start(file_lines.len_at_discovery))
                    .and_then(|_| {
                        scan_lines_for_key(
                            std::io::BufReader::with_capacity(64 * 1024, tail),
                            path,
                            cache_key,
                        )
                    });
                match found {
                    Ok(found) => entries.extend(found),
                    Err(e) => debug!(
                        "Failed to read journal lines appended after discovery: file={:?}, from={}, to={}, error={}",
                        path, file_lines.len_at_discovery, len_now, e
                    ),
                }
            }
        } else {
            match read_entries_for_key_blocking(path, cache_key) {
                Ok(found) => entries.extend(found),
                Err(e) => debug!("Failed to read journal file: file={:?}, error={}", path, e),
            }
        }
    }
    entries.sort_by_key(|a| a.timestamp);
    entries
}

/// After an append to a journal in `journals_dir` (`{cache_dir}/metadata/_journals`), keep
/// `instance_id`'s `.ready` record current: see
/// [`crate::staged_authority::refresh_ready_record_after_journal_append`]. Never waits.
fn refresh_ready_record(journals_dir: &Path, instance_id: &str) {
    if let Some(cache_dir) = journals_dir.parent().and_then(Path::parent) {
        crate::staged_authority::refresh_ready_record_after_journal_append(cache_dir, instance_id);
    }
}

async fn sync_data_best_effort(path: &Path) {
    let result = match tokio::fs::File::open(path).await {
        Ok(f) => f.sync_data().await,
        Err(e) => Err(e),
    };
    if let Err(e) = result {
        error!(
            "Failed to fdatasync journal file: path={:?}, error={} \
             (bytes are in page cache and readable; sync is best-effort)",
            path, e
        );
    }
}

/// The earlier append, kept only so lever (a) can show the inode guard going red.
#[cfg(test)]
async fn rmw_append_for_red_side(journal_path: &Path, lines: &str) -> std::io::Result<()> {
    let temp_path = journal_path.with_extension("journal.tmp");
    let existing = tokio::fs::read_to_string(journal_path)
        .await
        .unwrap_or_default();
    tokio::fs::write(&temp_path, format!("{}{}", existing, lines)).await?;
    tokio::fs::rename(&temp_path, journal_path).await
}

/// Journal entry for write-ahead logging of metadata operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JournalEntry {
    pub timestamp: SystemTime,
    pub instance_id: String,
    pub cache_key: String,
    pub range_spec: RangeSpec,
    pub operation: JournalOperation,
    pub range_file_path: String,
    pub metadata_version: u64, // For conflict resolution
    /// New TTL in seconds (for TtlRefresh operation)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_ttl_secs: Option<u64>,
    /// Object-level TTL in seconds (for Add operations, used by journal consolidation to set expires_at)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_ttl_secs: Option<u64>,
    /// Access count increment (for AccessUpdate operation)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_increment: Option<u64>,
    /// Object metadata from S3 response (for Add operations when .meta doesn't exist yet)
    /// Contains response_headers needed for serving cached responses with correct headers
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<crate::cache_types::ObjectMetadata>,
    /// A GET-miss range fill's version witness: the write-order marker token its key
    /// held before the fill's S3 request (empty when none could be established). The
    /// consolidator folds an `Add` carrying one only while the marker still holds it. Absent on
    /// every other entry and on entries written by earlier versions, which fold as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill_witness: Option<String>,
}

/// Types of journal operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JournalOperation {
    /// Add a new range to the cache
    Add,
    /// Update an existing range
    Update,
    /// Remove a range from the cache
    Remove,
    /// Refresh TTL for a cached range (cache-hit operation)
    TtlRefresh,
    /// Update access count and last_accessed timestamp (cache-hit operation)
    AccessUpdate,
    /// An entry graduated out of the write (staging) tier on its first read.
    ///
    /// Carries no range mutation — the `.meta` transition was already performed
    /// synchronously by `CacheManager::refresh_write_cache_ttl` (it has to be, so a
    /// `get_ttl: 0` object is correctly expired on that same GET). What this entry
    /// carries is the **accounting**: `range_spec.compressed_size` holds the entry's
    /// staged compressed size, which the consolidator subtracts from
    /// `SizeState::write_cache_size` while leaving `total_size` unchanged — the bytes
    /// are still on disk, they have simply moved from the write tier to the read tier.
    ///
    /// It exists as a journal entry rather than a direct accumulator debit because the
    /// decrement must be **exactly once fleet-wide** (Requirement 1.2) and the
    /// accumulator is per-instance. Consolidation applies it under the per-key metadata
    /// lock and records `ObjectMetadata::graduation_accounted`, so concurrent
    /// graduations on two proxies collapse to one decrement.
    ///
    /// # Mixed-version fleets
    ///
    /// `JournalOperation` is externally tagged, so an instance running a release without
    /// this variant fails to deserialise a line containing it. Every parse site treats a
    /// malformed line as skippable (`debug!` and continue) rather than fatal, so
    /// consolidation degrades to "the entry is ignored" rather than breaking — but a
    /// graduation consolidated only by an old instance is silently lost, and its bytes
    /// stay in `write_cache_size` until the next full Validation_Scan re-grounds it.
    /// Acceptable, and self-healing, but worth knowing during a rolling upgrade.
    ///
    /// Spec: write-cache-accounting-and-eviction. Requirements: 1.1, 1.2, 1.3
    Graduation,
}

/// Journal manager for write-ahead logging of metadata operations
pub struct JournalManager {
    cache_dir: PathBuf,
    instance_id: String,
    /// Mutex to serialize journal append operations within this instance.
    /// Prevents concurrent appends from overwriting each other (Bug 4 fix).
    append_mutex: Mutex<()>,
}

impl JournalManager {
    /// Create a new journal manager
    pub fn new(cache_dir: PathBuf, instance_id: String) -> Self {
        Self {
            cache_dir,
            instance_id,
            append_mutex: Mutex::new(()),
        }
    }

    /// Append complete, newline-terminated `lines` to the primary journal at `journal_path`.
    ///
    /// The caller MUST hold the journal's `.journal.lock` flock: cleanup takes the same
    /// lock before it truncates or rewrites the file by rename, so opening by path under
    /// the lock always reaches the current inode, and no cleanup can replace the file
    /// between the open and the write.
    ///
    /// One `O_APPEND` `write_all` of the whole buffer, so the cost is the size of the
    /// new lines, not of the journal. If the file's last byte is not `\n` (a torn tail
    /// left by a crash mid-append), a `\n` is written first so the new lines cannot
    /// glue onto the torn fragment and become unparseable with it. A spurious `\n`
    /// is a blank line, which every reader skips. No fsync, as before.
    async fn append_lines_to_primary(journal_path: &Path, lines: &str) -> std::io::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

        #[cfg(test)]
        if RMW_APPEND_LEVER {
            return rmw_append_for_red_side(journal_path, lines).await;
        }

        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(journal_path)
            .await?;

        let len = file.metadata().await?.len();
        let mut needs_separator = false;
        if len > 0 {
            file.seek(std::io::SeekFrom::End(-1)).await?;
            let mut last = [0u8; 1];
            file.read_exact(&mut last).await?;
            needs_separator = last[0] != b'\n';
        }
        #[cfg(test)]
        if SKIP_TORN_TAIL_GUARD_LEVER {
            needs_separator = false;
        }

        let buf = if needs_separator {
            let mut b = String::with_capacity(lines.len() + 1);
            b.push('\n');
            b.push_str(lines);
            b
        } else {
            lines.to_string()
        };
        // O_APPEND: the write lands at end-of-file whatever the seek above left.
        file.write_all(buf.as_bytes()).await?;
        file.flush().await?;
        Ok(())
    }

    /// Append a journal entry with enhanced error handling.
    ///
    /// Uses a mutex to serialize appends within this instance (Bug 4 fix).
    /// Uses non-blocking file lock to coordinate with cleanup operations.
    /// If lock is busy (cleanup in progress), creates a fresh journal file
    /// to avoid blocking cache writes (Bug 5 fix).
    pub async fn append_range_entry(&self, cache_key: &str, entry: JournalEntry) -> Result<()> {
        // Create parent directories if they don't exist
        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        if let Err(e) = tokio::fs::create_dir_all(&journals_dir).await {
            error!(
                "Failed to create journal directory: cache_key={}, path={:?}, error={}",
                cache_key, journals_dir, e
            );
            return Err(ProxyError::CacheError(format!(
                "Failed to create journal directory: {}",
                e
            )));
        }

        // Serialize the entry before acquiring locks (minimize lock hold time)
        let json_entry = serde_json::to_string(&entry).map_err(|e| {
            error!(
                "Failed to serialize journal entry: cache_key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize journal entry: {}", e))
        })?;

        // Acquire mutex to serialize journal appends within this instance.
        // This prevents the lost update problem where concurrent appends
        // read the same content and overwrite each other's entries.
        let _guard = self.append_mutex.lock().await;

        // Primary journal path
        let primary_journal_path = journals_dir.join(format!("{}.journal", self.instance_id));
        let lock_path = primary_journal_path.with_extension("journal.lock");

        // Try to acquire file lock (non-blocking); held until `_lock_file` drops.
        let _lock_file = lock_journal_lock_file(&lock_path, false).map_err(|e| {
            ProxyError::CacheError(format!("Failed to open journal lock file: {}", e))
        })?;
        let got_lock = _lock_file.is_some();

        let journal_path = if got_lock {
            // Got lock - use primary journal file
            primary_journal_path
        } else {
            // Lock busy (cleanup in progress) - create fresh journal file
            // Use timestamp to ensure uniqueness
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let fresh_path =
                journals_dir.join(format!("{}:{}.journal", self.instance_id, timestamp));
            debug!(
                "Journal lock busy, using fresh journal: cache_key={}, path={:?}",
                cache_key, fresh_path
            );
            fresh_path
        };

        // Primary journal (lock held): O_APPEND the one line, O(entry) not O(journal).
        // Fresh journal (lock busy): a uniquely named new file holding just this entry.
        if got_lock {
            let line = format!("{}\n", json_entry);
            if let Err(e) = Self::append_lines_to_primary(&journal_path, &line).await {
                crate::disk_cache::note_disk_full(&e, "journal.append");
                error!(
                    "Failed to append to journal file: cache_key={}, path={:?}, error={}",
                    cache_key, journal_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to append to journal file: {}",
                    e
                )));
            }
        } else {
            // Fresh journal file - just write the single entry
            let content = format!("{}\n", json_entry);
            if let Err(e) = tokio::fs::write(&journal_path, &content).await {
                crate::disk_cache::note_disk_full(&e, "journal.append_fresh");
                error!(
                    "Failed to write fresh journal file: cache_key={}, path={:?}, error={}",
                    cache_key, journal_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to write fresh journal file: {}",
                    e
                )));
            }
        }

        debug!(
            "Appended journal entry: cache_key={}, operation={:?}, path={:?}, fresh={}",
            cache_key, entry.operation, journal_path, !got_lock
        );
        refresh_ready_record(&journals_dir, &self.instance_id);

        Ok(())
    }

    /// Append multiple journal entries for a single cache key in one file operation.
    /// Serializes all entries, acquires the append mutex once, and writes them
    /// in a single `O_APPEND` write.
    pub async fn append_range_entries_batch(
        &self,
        cache_key: &str,
        entries: Vec<JournalEntry>,
    ) -> Result<()> {
        // No-op for empty entries
        if entries.is_empty() {
            return Ok(());
        }

        // Create parent directories if they don't exist
        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        if let Err(e) = tokio::fs::create_dir_all(&journals_dir).await {
            error!(
                "Failed to create journal directory: cache_key={}, path={:?}, error={}",
                cache_key, journals_dir, e
            );
            return Err(ProxyError::CacheError(format!(
                "Failed to create journal directory: {}",
                e
            )));
        }

        // Serialize all entries to JSON lines BEFORE acquiring any locks (minimize lock hold time)
        let mut serialized_lines = String::new();
        for entry in &entries {
            let json_entry = serde_json::to_string(entry).map_err(|e| {
                error!(
                    "Failed to serialize journal entry: cache_key={}, error={}",
                    cache_key, e
                );
                ProxyError::CacheError(format!("Failed to serialize journal entry: {}", e))
            })?;
            serialized_lines.push_str(&json_entry);
            serialized_lines.push('\n');
        }

        // Acquire mutex to serialize journal appends within this instance.
        // This prevents the lost update problem where concurrent appends
        // read the same content and overwrite each other's entries.
        let _guard = self.append_mutex.lock().await;

        // Primary journal path
        let primary_journal_path = journals_dir.join(format!("{}.journal", self.instance_id));
        let lock_path = primary_journal_path.with_extension("journal.lock");

        // Try to acquire file lock (non-blocking); held until `_lock_file` drops.
        let _lock_file = lock_journal_lock_file(&lock_path, false).map_err(|e| {
            ProxyError::CacheError(format!("Failed to open journal lock file: {}", e))
        })?;
        let got_lock = _lock_file.is_some();

        let journal_path = if got_lock {
            // Got lock - use primary journal file
            primary_journal_path
        } else {
            // Lock busy (cleanup in progress) - create fresh journal file
            // Use timestamp to ensure uniqueness
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let fresh_path =
                journals_dir.join(format!("{}:{}.journal", self.instance_id, timestamp));
            debug!(
                "Journal lock busy, using fresh journal for batch: cache_key={}, path={:?}, entry_count={}",
                cache_key, fresh_path, entries.len()
            );
            fresh_path
        };

        // Primary journal (lock held): one O_APPEND write of all lines.
        // Fresh journal (lock busy): a uniquely named new file holding just these entries.
        if got_lock {
            if let Err(e) = Self::append_lines_to_primary(&journal_path, &serialized_lines).await {
                crate::disk_cache::note_disk_full(&e, "journal.batch_append");
                error!(
                    "Failed to append to journal file: cache_key={}, path={:?}, error={}",
                    cache_key, journal_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to append to journal file: {}",
                    e
                )));
            }
        } else {
            // Fresh journal file - write all entries
            if let Err(e) = tokio::fs::write(&journal_path, &serialized_lines).await {
                crate::disk_cache::note_disk_full(&e, "journal.batch_append_fresh");
                error!(
                    "Failed to write fresh journal file: cache_key={}, path={:?}, error={}",
                    cache_key, journal_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to write fresh journal file: {}",
                    e
                )));
            }
        }

        debug!(
            "Appended batch journal entries: cache_key={}, count={}, path={:?}, fresh={}",
            cache_key,
            entries.len(),
            journal_path,
            !got_lock
        );
        refresh_ready_record(&journals_dir, &self.instance_id);

        Ok(())
    }

    /// Find all per-instance journal files that may contain entries for a cache key
    ///
    /// Since we now use per-instance journals (not per-object), this returns all
    /// instance journal files in the _journals directory. The caller must filter
    /// entries by cache_key when reading.
    pub(crate) async fn find_all_journal_files(&self, _cache_key: &str) -> Result<Vec<PathBuf>> {
        let journals_dir = self.cache_dir.join("metadata").join("_journals");

        if !journals_dir.exists() {
            return Ok(Vec::new());
        }

        let mut journal_files = Vec::new();
        let entries = std::fs::read_dir(&journals_dir).map_err(|e| {
            ProxyError::CacheError(format!("Failed to read journal directory: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                ProxyError::CacheError(format!("Failed to read directory entry: {}", e))
            })?;

            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                // Per-instance journals are named {instance_id}.journal
                if file_name.ends_with(".journal") {
                    journal_files.push(path);
                }
            }
        }

        debug!(
            "Found {} per-instance journal files: files={:?}",
            journal_files.len(),
            journal_files
        );

        Ok(journal_files)
    }

    /// Get all journal entries from all instances for a cache key
    ///
    /// Reads from per-instance journals: `metadata/_journals/{instance_id}.journal`
    /// Both HybridMetadataWriter and CacheHitUpdateBuffer now use this format.
    /// Like `get_all_entries_for_cache_key` but reads only the provided files instead of
    /// scanning the entire journal directory. Use this when the caller already has a
    /// pre-built index (e.g. from `discover_pending_cache_keys_indexed`) to avoid
    /// redundant full-directory scans on every key during a consolidation cycle.
    // The consolidator reads by offset (`get_entries_at_offsets`); this stays as the tests'
    // entry point to `read_entries_for_key` over a file list.
    #[cfg(test)]
    pub(crate) async fn get_entries_from_files(
        &self,
        cache_key: &str,
        journal_files: &[std::path::PathBuf],
    ) -> Result<Vec<JournalEntry>> {
        let mut all_entries = Vec::new();

        for journal_file in journal_files {
            match read_entries_for_key(journal_file, cache_key).await {
                Ok(entries) => all_entries.extend(entries),
                Err(e) => {
                    debug!(
                        "Failed to read journal file: file={:?}, error={}",
                        journal_file, e
                    );
                }
            }
        }

        all_entries.sort_by_key(|a| a.timestamp);

        debug!(
            "Retrieved {} journal entries for cache_key={} from {} journal files (indexed)",
            all_entries.len(),
            cache_key,
            journal_files.len()
        );

        Ok(all_entries)
    }

    /// Read `cache_key`'s entries from the lines discovery recorded for it, in timestamp order.
    /// The consolidator calls this under the key's lock, in place of reading every journal file
    /// the key appears in, so a key's read costs its own lines rather than the whole journal.
    ///
    /// Why this returns what the whole-file read returned, less only lines it will see later:
    ///
    /// - An entry returned is a pending entry of this key: its line was read under the key's
    ///   lock, parsed as one complete `JournalEntry` (`serde_json` rejects trailing bytes, so a
    ///   span that no longer covers exactly one line does not parse), and its `cache_key`
    ///   equals the key.
    /// - Spans less than 64 KiB apart are read with one positioned read of at most 1 MiB and cut
    ///   back to their recorded offsets and lengths (R2.9), so batching changes how many reads a
    ///   key costs and nothing about which lines are accepted.
    /// - The spans are used only while the file has the identity and at least the length
    ///   discovery saw, and only if every span yields an entry of the key. A rewrite (a new
    ///   file renamed over the path), a truncate, a span past the end of the file, or a span
    ///   that yields no entry of the key (the file was truncated and regrown to at least its
    ///   discovery length) makes the read discard what it took from that file's spans and rescan the file
    ///   with the filtered line scan. A file's entries come from its spans or from one rescan,
    ///   never both, and the spans are disjoint, so no line is returned twice.
    /// - On an unchanged file longer than at discovery, the lines from the discovery length to
    ///   the end are scanned for the key's (R2.10), as the whole-file read did. Every span ends
    ///   at or before that length, so no line comes from both.
    /// - A line no span covers stays in the journal, because cleanup removes only the lines a
    ///   cycle applied, and the next cycle's discovery records it. One case goes undetected: a file
    ///   truncated and regrown so that every span lands on one of the key's new lines while an
    ///   older new line of the key sits where no span reaches. That line is applied a cycle
    ///   late; only a per-span content check recorded at discovery would catch it.
    pub(crate) async fn get_entries_at_offsets(
        &self,
        cache_key: &str,
        key_lines: &KeyLines,
    ) -> Result<Vec<JournalEntry>> {
        let key = cache_key.to_string();
        let files = key_lines.files.clone();
        let entries = tokio::task::spawn_blocking(move || {
            use std::sync::atomic::Ordering::Relaxed;
            let running = SPAN_READS_IN_FLIGHT.fetch_add(1, Relaxed) + 1;
            SPAN_READS_PEAK.fetch_max(running, Relaxed);
            let entries = read_key_lines_blocking(&key, &files);
            SPAN_READS_IN_FLIGHT.fetch_sub(1, Relaxed);
            entries
        })
        .await
        .map_err(|e| ProxyError::CacheError(format!("Journal read task failed: {}", e)))?;
        debug!(
            "Retrieved {} journal entries for cache_key={} from {} journal files (offset index)",
            entries.len(),
            cache_key,
            key_lines.files.len()
        );
        Ok(entries)
    }

    pub async fn get_all_entries_for_cache_key(
        &self,
        cache_key: &str,
    ) -> Result<Vec<JournalEntry>> {
        let mut all_entries = Vec::new();

        // Read from all per-instance journal files
        let journal_files = self.find_all_journal_files(cache_key).await?;
        for journal_file in &journal_files {
            match read_entries_for_key(journal_file, cache_key).await {
                Ok(entries) => all_entries.extend(entries),
                Err(e) => {
                    // Stale file handle and other transient errors are expected on shared storage
                    debug!(
                        "Failed to read journal file: file={:?}, error={}",
                        journal_file, e
                    );
                }
            }
        }

        // Sort entries by timestamp for proper ordering
        all_entries.sort_by_key(|a| a.timestamp);

        debug!(
            "Retrieved {} journal entries for cache_key={} from {} journal files",
            all_entries.len(),
            cache_key,
            journal_files.len()
        );

        Ok(all_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionAlgorithm;
    use tempfile::TempDir;

    fn create_test_range_spec() -> RangeSpec {
        let now = SystemTime::now();
        RangeSpec {
            start: 0,
            end: 8388607,
            file_path: "test_range_0-8388607.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 8388608,
            uncompressed_size: 8388608,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        }
    }

    fn entry_a(cache_key: &str, start: u64) -> JournalEntry {
        let mut range_spec = create_test_range_spec();
        range_spec.start = start;
        range_spec.end = start + 99;
        JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "case61".to_string(),
            cache_key: cache_key.to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: String::new(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        }
    }

    // ---- Offset index (release 3.0.1 R2): one test per case of the safety argument on
    // `JournalManager::get_entries_at_offsets`. Discovery is the consolidator's own pass.

    fn offset_index_journal(dir: &Path) -> PathBuf {
        let journals = dir.join("metadata").join("_journals");
        std::fs::create_dir_all(&journals).unwrap();
        journals.join("offset-index.journal")
    }

    fn offset_index_line(entry: &JournalEntry) -> Vec<u8> {
        let mut line = serde_json::to_vec(entry).unwrap();
        line.push(b'\n');
        line
    }

    async fn offset_index_discover(
        dir: &Path,
        max_keys: usize,
    ) -> crate::journal_consolidator::DiscoveryResult {
        let jm = Arc::new(JournalManager::new(
            dir.to_path_buf(),
            "offset-index".to_string(),
        ));
        let lm = Arc::new(crate::metadata_lock_manager::MetadataLockManager::new(
            dir.to_path_buf(),
            std::time::Duration::from_secs(30),
            3,
        ));
        crate::journal_consolidator::JournalConsolidator::new(
            dir.to_path_buf(),
            jm,
            lm,
            crate::journal_consolidator::ConsolidationConfig::default(),
        )
        .discover_pending_cache_keys_indexed_capped(max_keys)
        .await
        .unwrap()
    }

    fn offset_index_starts(entries: &[JournalEntry]) -> Vec<u64> {
        entries.iter().map(|e| e.range_spec.start).collect()
    }

    /// Replace a journal's contents in place, keeping its inode: cleanup's `set_len(0)` of a
    /// fully applied primary journal followed by appends.
    fn offset_index_truncate_and_append(path: &Path, bytes: &[u8]) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_len(0).unwrap();
        f.write_all(bytes).unwrap();
    }

    /// The offset read returns exactly each key's entries in order over PR #23's fixture (a key
    /// another key extends, a key named in another field, a key needing JSON escapes, an
    /// unparseable line and a non-UTF-8 line), and after the journal is truncated in place and
    /// re-appended to the same length with another key's line where the key's line was, it
    /// returns none of that key's entries. Red side: drop the `cache_key` comparison in
    /// `parse_entry_for_key`.
    #[tokio::test]
    async fn offset_index_reads_return_exactly_the_keys_entries() {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let plain = "bkt/obj";
        let longer = "bkt/obj-2";
        let escaped = "bkt/\"quoted\"\\back\tslash/é";
        let mut mention = entry_a("bkt/other", 0);
        mention.range_file_path = plain.to_string();
        let plain_first = offset_index_line(&entry_a(plain, 0));
        let mut rest = Vec::new();
        rest.extend(offset_index_line(&entry_a(longer, 0)));
        rest.extend(offset_index_line(&mention));
        rest.extend(offset_index_line(&entry_a(escaped, 0)));
        rest.extend(format!("{{\"cache_key\":\"{plain}\", not json\n").into_bytes());
        rest.extend([b'{', 0xff, 0xfe, b'}', b'\n']);
        rest.extend(offset_index_line(&entry_a(plain, 100)));
        rest.extend(offset_index_line(&entry_a(escaped, 100)));
        std::fs::write(&path, [plain_first.clone(), rest.clone()].concat()).unwrap();

        let index = offset_index_discover(temp.path(), 0).await.key_index;
        let jm = JournalManager::new(temp.path().to_path_buf(), "reader".to_string());
        for (key, starts) in [
            (plain, vec![0, 100]),
            (longer, vec![0]),
            (escaped, vec![0, 100]),
            ("bkt/other", vec![0]),
        ] {
            let entries = jm.get_entries_at_offsets(key, &index[key]).await.unwrap();
            assert!(
                entries.iter().all(|e| e.cache_key == key),
                "offset read returned another key's entry for {key:?}"
            );
            assert_eq!(offset_index_starts(&entries), starts, "entries for {key:?}");
        }

        // Same inode, same length, another key's line at `plain`'s first span.
        let swapped = String::from_utf8(plain_first.clone()).unwrap().replacen(
            "\"cache_key\":\"bkt/obj\"",
            "\"cache_key\":\"bkt/oth\"",
            1,
        );
        assert_eq!(swapped.len(), plain_first.len());
        assert_ne!(swapped.as_bytes(), &plain_first[..]);
        offset_index_truncate_and_append(&path, &[swapped.into_bytes(), rest].concat());
        let entries = jm
            .get_entries_at_offsets(plain, &index[plain])
            .await
            .unwrap();
        assert!(
            entries.iter().all(|e| e.cache_key == plain),
            "offset read returned another key's line from a recorded span: {:?}",
            entries.iter().map(|e| &e.cache_key).collect::<Vec<_>>()
        );
        assert_eq!(offset_index_starts(&entries), vec![100]);
    }

    /// A rewrite renamed over the journal between discovery and the read (a new inode, the same
    /// length, the key's lines moved) makes the read rescan the file and return the key's
    /// current lines. Red side: skip the identity comparison in `read_key_lines_blocking`.
    #[tokio::test]
    async fn offset_index_read_rescans_a_journal_a_rewrite_replaced() {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let (k1, k2) = (entry_a("bkt/k", 0), entry_a("bkt/k", 100));
        let (x1, x2) = (entry_a("bkt/x", 0), entry_a("bkt/x", 100));
        let line = offset_index_line;
        std::fs::write(&path, [line(&x1), line(&k1), line(&x2), line(&k2)].concat()).unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;

        let tmp = path.with_extension("journal.tmp");
        std::fs::write(&tmp, [line(&k1), line(&k2), line(&x1), line(&x2)].concat()).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        assert_ne!(
            crate::journal_manager::file_identity(&std::fs::metadata(&path).unwrap()),
            index["bkt/k"].files[0].identity,
            "the rename must have replaced the inode"
        );

        let jm = JournalManager::new(temp.path().to_path_buf(), "reader".to_string());
        let entries = jm
            .get_entries_at_offsets("bkt/k", &index["bkt/k"])
            .await
            .unwrap();
        assert!(entries.iter().all(|e| e.cache_key == "bkt/k"));
        assert_eq!(offset_index_starts(&entries), vec![0, 100]);
    }

    /// A journal truncated in place and re-appended between discovery and the read, shorter
    /// than at discovery: the read returns the key's line once and no other key's line, and a
    /// line appended after the read is returned by the next cycle. Red side: remove both the
    /// length comparison and the discard of the spans' entries before a rescan in
    /// `read_key_lines_blocking` (either alone keeps it green: the length check rescans before
    /// any span is read, and without it the span past the end discards the first span's copy
    /// before the rescan).
    #[tokio::test]
    async fn offset_index_read_after_truncate_and_reappend_returns_each_line_once() {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let (k1, k2, k3) = (
            entry_a("bkt/k", 0),
            entry_a("bkt/k", 100),
            entry_a("bkt/k", 200),
        );
        let x1 = entry_a("bkt/x", 0);
        let line = offset_index_line;
        std::fs::write(&path, [line(&k1), line(&x1), line(&k2)].concat()).unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;

        offset_index_truncate_and_append(&path, &line(&k1));
        let jm = JournalManager::new(temp.path().to_path_buf(), "reader".to_string());
        let entries = jm
            .get_entries_at_offsets("bkt/k", &index["bkt/k"])
            .await
            .unwrap();
        assert!(entries.iter().all(|e| e.cache_key == "bkt/k"));
        assert_eq!(
            offset_index_starts(&entries),
            vec![0],
            "the key's one remaining line, once"
        );

        // Appended after this cycle's discovery: the next cycle's discovery finds it.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .and_then(|mut f| std::io::Write::write_all(&mut f, &line(&k3)))
            .unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;
        let entries = jm
            .get_entries_at_offsets("bkt/k", &index["bkt/k"])
            .await
            .unwrap();
        assert_eq!(offset_index_starts(&entries), vec![0, 200]);
    }

    /// An entry with every timestamp fixed, so entries that differ only in a letter of the key
    /// and a three-digit start serialize to lines of the same length.
    fn offset_index_fixed(cache_key: &str, start: u64, nanos: u32) -> JournalEntry {
        let at =
            std::time::UNIX_EPOCH + std::time::Duration::new(1_800_000_000, 100_000_000 + nanos);
        let mut entry = entry_a(cache_key, start);
        entry.timestamp = at;
        entry.range_spec.created_at = at;
        entry.range_spec.last_accessed = at;
        entry
    }

    /// A line of the key appended to an indexed file after discovery, with another key's line
    /// after it, is read in the same cycle, once, and the other key's line is not (R2.10).
    /// Red side: the read before this fix, which read only the recorded spans.
    #[tokio::test]
    async fn offset_index_read_returns_a_line_appended_after_discovery() {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let line = offset_index_line;
        let x1 = line(&offset_index_fixed("bkt/x", 300, 0));
        let a = line(&offset_index_fixed("bkt/k", 100, 1));
        let b = line(&offset_index_fixed("bkt/k", 200, 2));
        let x2 = line(&offset_index_fixed("bkt/x", 400, 3));
        std::fs::write(&path, [&x1[..], &a[..]].concat()).unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[&b[..], &x2[..]].concat()).unwrap();
        }
        let jm = JournalManager::new(temp.path().to_path_buf(), "reader".to_string());
        let entries = jm
            .get_entries_at_offsets("bkt/k", &index["bkt/k"])
            .await
            .unwrap();
        assert!(entries.iter().all(|e| e.cache_key == "bkt/k"));
        assert_eq!(
            offset_index_starts(&entries),
            vec![100, 200],
            "the key's line appended after discovery is read in the same cycle, once"
        );
    }
    /// A busy key (R2.9): 300 of its lines, each followed by three other keys' lines, read with
    /// the positioned reads its layout gives, and every line returned once, in order. The key's
    /// lines span less than `SPAN_BATCH_MAX` and no gap reaches `SPAN_BATCH_GAP`, so the layout
    /// gives one read. A range hit continuously writes about 720 lines an
    /// hour per instance. Red sides: one positioned read per recorded line; a size rule of half
    /// `SPAN_BATCH_MAX` (2 reads, which the earlier `reads * 10 <= 300` bound passed).
    #[tokio::test]
    async fn offset_index_busy_key_read_batches_nearby_lines() {
        const KEY_LINES: u64 = 300;
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let mut journal = Vec::new();
        for i in 0..KEY_LINES {
            journal.extend(offset_index_line(&offset_index_fixed(
                "bkt/k",
                100 + i,
                i as u32 * 4,
            )));
            for (n, other) in ["bkt/x", "bkt/y", "bkt/z"].iter().enumerate() {
                journal.extend(offset_index_line(&offset_index_fixed(
                    other,
                    100 + i,
                    i as u32 * 4 + n as u32 + 1,
                )));
            }
        }
        std::fs::write(&path, &journal).unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;
        let files = &index["bkt/k"].files;
        assert_eq!(
            files.iter().map(|f| f.lines.len() as u64).sum::<u64>(),
            KEY_LINES
        );
        POSITIONED_READS.with(|c| c.set(0));
        let entries = read_key_lines_blocking("bkt/k", files);
        let reads = POSITIONED_READS.with(|c| c.get());
        assert_eq!(
            offset_index_starts(&entries),
            (100..100 + KEY_LINES).collect::<Vec<_>>(),
            "every one of the key's lines, once, in order"
        );
        let spans: Vec<(u64, u64)> = files.iter().flat_map(|f| f.lines.iter().copied()).collect();
        let span = spans.last().map_or(0, |&(o, l)| o + l) - spans[0].0;
        let widest_gap = spans
            .windows(2)
            .map(|w| w[1].0 - (w[0].0 + w[0].1))
            .max()
            .unwrap_or(0);
        assert!(
            files.len() == 1 && span <= SPAN_BATCH_MAX && widest_gap < SPAN_BATCH_GAP,
            "fixture: one file, a span of {} bytes within SPAN_BATCH_MAX and gaps of at most {} \
             bytes, below SPAN_BATCH_GAP",
            span,
            widest_gap
        );
        println!(
            "busy key: {} positioned reads for {} lines ({} journal bytes, the key's lines span {} \
             bytes, widest gap {} bytes, layout gives 1)",
            reads,
            KEY_LINES,
            journal.len(),
            span,
            widest_gap
        );
        assert_eq!(
            reads,
            1,
            "a busy key's read must batch nearby lines: {} positioned reads for {} lines ({} \
             journal bytes), where its layout gives 1",
            reads,
            KEY_LINES,
            journal.len()
        );
    }
    /// A line of `len` bytes, newline included, that is no journal entry: discovery skips it as
    /// it skips a torn line, so it only sets the gap between two of the key's lines.
    fn offset_index_pad(len: usize) -> Vec<u8> {
        let mut pad = format!("{{\"pad\":\"{}\"}}", "x".repeat(len - 11)).into_bytes();
        pad.push(b'\n');
        assert_eq!(pad.len(), len);
        pad
    }

    /// Write `parts` as one journal, discover it, and read `bkt/k`'s lines. Returns the spans
    /// discovery recorded, the starts the read returned, and the positioned reads it made.
    async fn offset_index_read_layout(parts: &[Vec<u8>]) -> (Vec<(u64, u64)>, Vec<u64>, u64) {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        std::fs::write(&path, parts.concat()).unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;
        let files = &index["bkt/k"].files;
        let spans = files.iter().flat_map(|f| f.lines.iter().copied()).collect();
        POSITIONED_READS.with(|c| c.set(0));
        let entries = read_key_lines_blocking("bkt/k", files);
        let reads = POSITIONED_READS.with(|c| c.get());
        (spans, offset_index_starts(&entries), reads)
    }

    /// R2.9's gap rule at its edge: of three of the key's lines, the first
    /// two `SPAN_BATCH_GAP - 1` bytes apart share a read, and the third, `SPAN_BATCH_GAP` bytes
    /// after the second, takes its own: exactly 2 positioned reads. Red side: the split taken only
    /// above `SPAN_BATCH_GAP` (1 read).
    #[tokio::test]
    async fn offset_index_read_splits_a_batch_at_span_batch_gap() {
        let gap = SPAN_BATCH_GAP;
        let k = |start: u64, n: u32| offset_index_line(&offset_index_fixed("bkt/k", start, n));
        let parts = [
            k(100, 0),
            offset_index_pad(gap as usize - 1),
            k(200, 1),
            offset_index_pad(gap as usize),
            k(300, 2),
        ];
        let l = parts[0].len() as u64;
        let (spans, starts, reads) = offset_index_read_layout(&parts).await;
        assert_eq!(
            spans,
            vec![(0, l), (l + gap - 1, l), (2 * l + 2 * gap - 1, l)],
            "fixture: the key's lines sit where they were laid out"
        );
        assert_eq!(
            starts,
            vec![100, 200, 300],
            "every one of the key's lines, once, in order"
        );
        println!(
            "gap layout: {} positioned reads for 3 lines, gaps {} and {} bytes",
            reads,
            gap - 1,
            gap
        );
        assert_eq!(
            reads, 2,
            "gaps of SPAN_BATCH_GAP - 1 and SPAN_BATCH_GAP bytes give 2 positioned \
             reads; made {}",
            reads
        );
    }

    /// R2.9's size rule: 40 of the key's lines with 60,000-byte gaps, each
    /// inside `SPAN_BATCH_GAP`, span about 2.4 MiB, so no one read may cover them. A batch based at
    /// a line takes each later line whose end is within `SPAN_BATCH_MAX` of the base, `per_batch`
    /// lines, so the reads are `ceil(40 / per_batch)`. Red side: no size rule (1 read).
    #[tokio::test]
    async fn offset_index_read_splits_a_batch_at_span_batch_max() {
        const LINES: u64 = 40;
        const PAD: u64 = 60_000;
        let mut parts = Vec::new();
        for i in 0..LINES {
            if i > 0 {
                parts.push(offset_index_pad(PAD as usize));
            }
            parts.push(offset_index_line(&offset_index_fixed(
                "bkt/k",
                100 + i,
                i as u32,
            )));
        }
        let l = parts[0].len() as u64;
        let stride = l + PAD;
        let (spans, starts, reads) = offset_index_read_layout(&parts).await;
        assert_eq!(
            spans,
            (0..LINES).map(|i| (i * stride, l)).collect::<Vec<_>>(),
            "fixture: the key's lines sit where they were laid out"
        );
        assert_eq!(
            starts,
            (100..100 + LINES).collect::<Vec<_>>(),
            "every one of the key's lines, once, in order"
        );
        let per_batch = (SPAN_BATCH_MAX - l) / stride + 1;
        let expected = LINES.div_ceil(per_batch);
        assert!(
            expected > 1,
            "fixture: the lines span more than SPAN_BATCH_MAX"
        );
        println!(
            "max layout: {} positioned reads for {} lines {} bytes apart ({} lines per batch, {} \
             expected)",
            reads, LINES, stride, per_batch, expected
        );
        assert_eq!(
            reads, expected,
            "{} lines {} bytes apart give {} positioned reads of at most \
             SPAN_BATCH_MAX; made {}",
            LINES, stride, expected, reads
        );
    }

    /// R2.9's size rule at its edge: 18 of the key's lines, every gap
    /// 62,000 bytes or the last one's remainder (all below `SPAN_BATCH_GAP`), laid out so the last
    /// line ends exactly at base + `SPAN_BATCH_MAX`. A line ending there is within the limit: 1
    /// positioned read. The same layout with the last gap one byte longer ends one byte past it:
    /// 2 reads. Red side: the size rule taken at `>=` (the first layout then takes 2 reads).
    #[tokio::test]
    async fn offset_index_read_batches_a_line_ending_exactly_at_span_batch_max() {
        const LINES: usize = 18;
        const PAD: usize = 62_000;
        let line =
            |i: usize| offset_index_line(&offset_index_fixed("bkt/k", 100 + i as u64, i as u32));
        let total_lines: usize = (0..LINES).map(|i| line(i).len()).sum();
        let layout = |extra: usize| {
            let last_gap = SPAN_BATCH_MAX as usize + extra - total_lines - (LINES - 2) * PAD;
            assert!(
                last_gap >= 11 && (last_gap as u64) < SPAN_BATCH_GAP,
                "fixture: the last gap {} is a pad that is below SPAN_BATCH_GAP",
                last_gap
            );
            let mut parts = Vec::new();
            for i in 0..LINES {
                if i > 0 {
                    parts.push(offset_index_pad(if i == LINES - 1 {
                        last_gap
                    } else {
                        PAD
                    }));
                }
                parts.push(line(i));
            }
            parts
        };
        let (spans, starts, at_max) = offset_index_read_layout(&layout(0)).await;
        let (last_offset, last_len) = *spans.last().unwrap();
        assert_eq!(
            last_offset + last_len,
            SPAN_BATCH_MAX,
            "fixture: the last line ends exactly at base + SPAN_BATCH_MAX"
        );
        assert_eq!(starts.len(), LINES, "every one of the key's lines, once");
        let (spans, _, past_max) = offset_index_read_layout(&layout(1)).await;
        let (last_offset, last_len) = *spans.last().unwrap();
        assert_eq!(
            last_offset + last_len,
            SPAN_BATCH_MAX + 1,
            "fixture: the last line ends one byte past base + SPAN_BATCH_MAX"
        );
        println!(
            "max edge: {} positioned reads with the last line ending at SPAN_BATCH_MAX, {} one \
             byte past it",
            at_max, past_max
        );
        assert_eq!(
            (at_max, past_max),
            (1, 2),
            "a line ending exactly at SPAN_BATCH_MAX shares the read (1), one \
             ending a byte past it takes its own (2); made {} and {}",
            at_max,
            past_max
        );
    }
    /// A journal truncated in place and regrown past its discovery length, with the key's newer
    /// line B at a recorded span, another key's line at the other span, and the key's older line
    /// A where no span reaches: the read returns A and B, A first, so A is not applied a cycle
    /// after B. Red side: the read as built before the fix, which
    /// kept the entries of the spans that still parsed and rescanned nothing.
    #[tokio::test]
    async fn offset_index_read_after_truncate_and_regrow_keeps_the_keys_order() {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let line = offset_index_line;
        let a = line(&offset_index_fixed("bkt/k", 100, 1));
        let b = line(&offset_index_fixed("bkt/k", 200, 2));
        let x1 = line(&offset_index_fixed("bkt/x", 300, 0));
        let x2 = line(&offset_index_fixed("bkt/x", 400, 3));
        let x3 = line(&offset_index_fixed("bkt/x", 500, 4));
        assert!(
            [&b, &x1, &x2, &x3].iter().all(|l| l.len() == a.len()),
            "the fixture's lines must be the same length to align with the spans"
        );
        std::fs::write(&path, [&x1[..], &a[..], &b[..]].concat()).unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;
        // Spans now land on x2 and B; A sits at offset 0, which no span covers, and x3 takes
        // the file past its discovery length, so the identity and length checks both pass.
        offset_index_truncate_and_append(&path, &[&a[..], &x2[..], &b[..], &x3[..]].concat());
        let jm = JournalManager::new(temp.path().to_path_buf(), "reader".to_string());
        let entries = jm
            .get_entries_at_offsets("bkt/k", &index["bkt/k"])
            .await
            .unwrap();
        assert!(entries.iter().all(|e| e.cache_key == "bkt/k"));
        assert_eq!(
            offset_index_starts(&entries),
            vec![100, 200],
            "the key's lines A and B, A first, after a truncate and regrow"
        );
    }
    /// Two distinct entries of the key from one instance with an equal timestamp and range but
    /// different operations both come back from the read. Red side: the
    /// dedupe on `(start, end, timestamp, instance_id)` that `read_key_lines_blocking` had.
    #[tokio::test]
    async fn offset_index_read_returns_distinct_entries_sharing_a_timestamp() {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let add = offset_index_fixed("bkt/k", 100, 1);
        let mut access = add.clone();
        access.operation = JournalOperation::AccessUpdate;
        std::fs::write(
            &path,
            [offset_index_line(&add), offset_index_line(&access)].concat(),
        )
        .unwrap();
        let index = offset_index_discover(temp.path(), 0).await.key_index;
        let jm = JournalManager::new(temp.path().to_path_buf(), "reader".to_string());
        let entries = jm
            .get_entries_at_offsets("bkt/k", &index["bkt/k"])
            .await
            .unwrap();
        let operations: Vec<JournalOperation> =
            entries.iter().map(|e| e.operation.clone()).collect();
        assert_eq!(
            operations,
            vec![JournalOperation::Add, JournalOperation::AccessUpdate],
            "both entries sharing a timestamp and range"
        );
    }
    /// With the discovery cap reached inside a file, a key already indexed keeps its later
    /// lines in that file, so its read returns them in the same cycle, as the whole-file read
    /// did; no key past the cap is indexed, and the file's entry count is complete. Red side:
    /// stop reading the file at the cap, as 3.0.0's discovery did.
    #[tokio::test]
    async fn offset_index_cap_inside_a_file_keeps_indexed_keys_later_lines() {
        let temp = TempDir::new().unwrap();
        let path = offset_index_journal(temp.path());
        let line = offset_index_line;
        std::fs::write(
            &path,
            [
                line(&entry_a("bkt/a", 0)),
                line(&entry_a("bkt/b", 0)),
                line(&entry_a("bkt/c", 0)),
                line(&entry_a("bkt/a", 100)),
            ]
            .concat(),
        )
        .unwrap();
        let discovery = offset_index_discover(temp.path(), 2).await;
        let jm = JournalManager::new(temp.path().to_path_buf(), "reader".to_string());
        let entries = jm
            .get_entries_at_offsets("bkt/a", &discovery.key_index["bkt/a"])
            .await
            .unwrap();
        assert_eq!(offset_index_starts(&entries), vec![0, 100]);
        assert!(!discovery.key_index.contains_key("bkt/c"));
        assert_eq!(discovery.key_index.len(), 2);
        assert_eq!(discovery.file_entry_counts.get(&path), Some(&4));
    }

    /// Throughput guard: an append writes only its own line into the existing file.
    /// The whole-file RMW rewrote the journal into a new inode on every append, so
    /// per-append cost grew with journal size; an unchanged inode and a length that
    /// grows by exactly the new line pin O(entry) cost. Red side: `RMW_APPEND_LEVER`.
    #[tokio::test]
    async fn appends_extend_the_primary_journal_in_place() {
        use std::os::unix::fs::MetadataExt;
        let temp = TempDir::new().unwrap();
        let jm = JournalManager::new(temp.path().to_path_buf(), "case61".to_string());
        let key = "bkt/case61/in-place";
        jm.append_range_entry(key, entry_a(key, 0)).await.unwrap();
        let path = jm
            .cache_dir
            .join("metadata")
            .join("_journals")
            .join(format!("{}.journal", jm.instance_id));
        let ino = std::fs::metadata(&path).unwrap().ino();
        for i in 1..200u64 {
            let entry = entry_a(key, i * 100);
            let line_len = serde_json::to_string(&entry).unwrap().len() as u64 + 1;
            let before = std::fs::metadata(&path).unwrap().len();
            jm.append_range_entry(key, entry).await.unwrap();
            let meta = std::fs::metadata(&path).unwrap();
            assert_eq!(
                meta.ino(),
                ino,
                "append {} replaced the journal file (whole-file rewrite)",
                i
            );
            assert_eq!(meta.len(), before + line_len, "append {} length", i);
        }
        assert!(!path.with_extension("journal.tmp").exists());
        assert_eq!(
            jm.get_all_entries_for_cache_key(key).await.unwrap().len(),
            200
        );
    }

    /// Torn-line tolerance: a partial last line is skipped by every reader in this
    /// module, is seen exactly once when completed, and a crash-torn tail that is never
    /// completed does not swallow the next append. Red side: `SKIP_TORN_TAIL_GUARD_LEVER`.
    #[tokio::test]
    async fn torn_tail_is_skipped_completed_once_and_never_glued() {
        use std::io::Write;
        let temp = TempDir::new().unwrap();
        let jm = JournalManager::new(temp.path().to_path_buf(), "case61".to_string());
        let key = "bkt/case61/torn";
        jm.append_range_entry(key, entry_a(key, 0)).await.unwrap();
        let path = jm
            .cache_dir
            .join("metadata")
            .join("_journals")
            .join(format!("{}.journal", jm.instance_id));
        let raw_append = |s: &str| {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(s.as_bytes()).unwrap();
        };
        async fn counts(jm: &JournalManager, key: &str, path: &Path) -> (usize, usize, usize) {
            let files = vec![path.to_path_buf()];
            (
                jm.get_all_entries_for_cache_key(key).await.unwrap().len(),
                jm.get_all_entries_for_cache_key(key).await.unwrap().len(),
                jm.get_entries_from_files(key, &files).await.unwrap().len(),
            )
        }

        // An in-progress append: half a line, no newline yet.
        let second = serde_json::to_string(&entry_a(key, 100)).unwrap();
        let (head, tail) = second.split_at(second.len() / 2);
        raw_append(head);
        assert_eq!(
            counts(&jm, key, &path).await,
            (1, 1, 1),
            "torn tail must be skipped"
        );

        // The append completes: the line is seen, once.
        raw_append(&format!("{}\n", tail));
        assert_eq!(
            counts(&jm, key, &path).await,
            (2, 2, 2),
            "completed line seen once"
        );

        // A crash-torn tail that never completes, then a normal append.
        let third = serde_json::to_string(&entry_a(key, 200)).unwrap();
        raw_append(&third[..third.len() / 2]);
        jm.append_range_entry(key, entry_a(key, 300)).await.unwrap();
        assert_eq!(
            counts(&jm, key, &path).await,
            (3, 3, 3),
            "the append after a torn tail was glued onto it and lost"
        );
    }

    /// The per-key reads parse only lines that contain the key's JSON string, so they must
    /// still return exactly the key's entries, in file order: none of a key that the key is a
    /// prefix of, none of a key that merely appears in another field, every entry of a key
    /// whose JSON form needs escapes, and the key's lines on either side of a line that does not
    /// parse or is not UTF-8. Red side: drop the `entry.cache_key == cache_key` comparison in
    /// `read_entries_for_key_blocking`, or search for the raw key rather than its JSON form.
    #[tokio::test]
    async fn per_key_reads_return_exactly_the_keys_entries() {
        use std::io::Write;
        let temp = TempDir::new().unwrap();
        let jm = JournalManager::new(temp.path().to_path_buf(), "case-filter".to_string());
        let plain = "bkt/obj";
        let longer = "bkt/obj-2";
        let escaped = "bkt/\"quoted\"\\back\tslash/é";
        let mut mention = entry_a("bkt/other", 0);
        mention.range_file_path = plain.to_string();

        let path = jm
            .cache_dir
            .join("metadata")
            .join("_journals")
            .join(format!("{}.journal", jm.instance_id));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(&path).unwrap();
        let mut line = |bytes: &[u8]| {
            f.write_all(bytes).unwrap();
            f.write_all(b"\n").unwrap();
        };
        let json = |e: &JournalEntry| serde_json::to_string(e).unwrap().into_bytes();
        line(&json(&entry_a(plain, 0)));
        line(&json(&entry_a(longer, 0)));
        line(&json(&mention));
        line(&json(&entry_a(escaped, 0)));
        line(format!("{{\"cache_key\":\"{plain}\", not json").as_bytes());
        line(&[b'{', 0xff, 0xfe, b'}']);
        line(&json(&entry_a(plain, 100)));
        line(&json(&entry_a(escaped, 100)));
        drop(f);

        let files = vec![path.clone()];
        for (key, starts) in [
            (plain, vec![0, 100]),
            (longer, vec![0]),
            (escaped, vec![0, 100]),
        ] {
            for (reader, entries) in [
                (
                    "get_entries_from_files",
                    jm.get_entries_from_files(key, &files).await,
                ),
                (
                    "get_all_entries_for_cache_key",
                    jm.get_all_entries_for_cache_key(key).await,
                ),
            ] {
                let entries = entries.unwrap();
                assert!(
                    entries.iter().all(|e| e.cache_key == key),
                    "{reader} returned another key's entry for {key:?}"
                );
                assert_eq!(
                    entries
                        .iter()
                        .map(|e| e.range_spec.start)
                        .collect::<Vec<_>>(),
                    starts,
                    "{reader} entries for {key:?}"
                );
            }
        }
    }

    #[test]
    fn test_journal_entry_serialization() {
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/test-object".to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "test_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        };

        // Test serialization
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.is_empty());

        // Test deserialization
        let deserialized: JournalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.cache_key, entry.cache_key);
        assert_eq!(deserialized.operation, entry.operation);
        assert_eq!(deserialized.metadata_version, entry.metadata_version);
    }

    #[test]
    fn test_journal_entry_serialization_with_ttl_refresh() {
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/test-object".to_string(),
            range_spec,
            operation: JournalOperation::TtlRefresh,
            range_file_path: String::new(),
            metadata_version: 0,
            new_ttl_secs: Some(7200),
            object_ttl_secs: None,
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        };

        // Test serialization
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("TtlRefresh"));
        assert!(json.contains("7200"));

        // Test deserialization
        let deserialized: JournalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.operation, JournalOperation::TtlRefresh);
        assert_eq!(deserialized.new_ttl_secs, Some(7200));
        assert_eq!(deserialized.access_increment, None);
    }

    #[test]
    fn test_journal_entry_serialization_with_access_update() {
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/test-object".to_string(),
            range_spec,
            operation: JournalOperation::AccessUpdate,
            range_file_path: String::new(),
            metadata_version: 0,
            new_ttl_secs: None,
            object_ttl_secs: None,
            access_increment: Some(5),
            object_metadata: None,
            fill_witness: None,
        };

        // Test serialization
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("AccessUpdate"));
        assert!(json.contains("5"));

        // Test deserialization
        let deserialized: JournalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.operation, JournalOperation::AccessUpdate);
        assert_eq!(deserialized.new_ttl_secs, None);
        assert_eq!(deserialized.access_increment, Some(5));
    }

    #[tokio::test]
    async fn test_journal_manager_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager =
            JournalManager::new(temp_dir.path().to_path_buf(), "test-instance".to_string());

        let cache_key = "test-bucket/test-object";
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "test_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        };

        // Test append
        journal_manager
            .append_range_entry(cache_key, entry.clone())
            .await
            .unwrap();

        // Test get pending entries
        let entries = journal_manager
            .get_all_entries_for_cache_key(cache_key)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cache_key, cache_key);
        assert_eq!(entries[0].operation, JournalOperation::Add);

        // Test find all journal files
        let journal_files = journal_manager
            .find_all_journal_files(cache_key)
            .await
            .unwrap();
        assert_eq!(journal_files.len(), 1);
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: journal-based-metadata-updates, Property 4: Journal Entry Serialization Round-Trip**
    /// *For any* valid JournalEntry (including new TtlRefresh and AccessUpdate operations),
    /// serializing to JSON and deserializing back shall produce an equivalent entry.
    /// **Validates: Requirements 1.4**
    #[quickcheck]
    fn prop_journal_entry_serialization_round_trip(
        instance_id_seed: u8,
        cache_key_seed: u8,
        operation_type: u8,
        range_start: u64,
        range_end_offset: u64,
        new_ttl_secs: Option<u64>,
        access_increment: Option<u64>,
    ) -> TestResult {
        // Generate valid range (end must be >= start)
        let range_end = range_start.saturating_add(range_end_offset);

        // Generate operation type (0-4 for Add, Update, Remove, TtlRefresh, AccessUpdate)
        let operation = match operation_type % 5 {
            0 => JournalOperation::Add,
            1 => JournalOperation::Update,
            2 => JournalOperation::Remove,
            3 => JournalOperation::TtlRefresh,
            4 => JournalOperation::AccessUpdate,
            _ => unreachable!(),
        };

        // For TtlRefresh, ensure new_ttl_secs is set; for AccessUpdate, ensure access_increment is set
        let (final_ttl, final_increment) = match operation {
            JournalOperation::TtlRefresh => (Some(new_ttl_secs.unwrap_or(3600)), None),
            JournalOperation::AccessUpdate => (None, Some(access_increment.unwrap_or(1))),
            _ => (None, None),
        };

        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: range_start,
            end: range_end,
            file_path: format!("test_range_{}-{}.bin", range_start, range_end),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: range_end.saturating_sub(range_start).saturating_add(1),
            uncompressed_size: range_end.saturating_sub(range_start).saturating_add(1),
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        };

        let entry = JournalEntry {
            timestamp: now,
            instance_id: format!("instance-{}", instance_id_seed),
            cache_key: format!("bucket-{}/object-{}", cache_key_seed, cache_key_seed),
            range_spec,
            operation: operation.clone(),
            range_file_path: format!("test_range_{}-{}.bin", range_start, range_end),
            metadata_version: 1,
            new_ttl_secs: final_ttl,
            object_ttl_secs: match operation {
                JournalOperation::Add => Some(3600),
                _ => None,
            },
            access_increment: final_increment,
            object_metadata: None,
            fill_witness: None,
        };

        // Serialize to JSON
        let json = match serde_json::to_string(&entry) {
            Ok(j) => j,
            Err(_) => return TestResult::failed(),
        };

        // Deserialize back
        let deserialized: JournalEntry = match serde_json::from_str(&json) {
            Ok(e) => e,
            Err(_) => return TestResult::failed(),
        };

        // Verify equality of key fields
        if deserialized.instance_id != entry.instance_id {
            return TestResult::failed();
        }
        if deserialized.cache_key != entry.cache_key {
            return TestResult::failed();
        }
        if deserialized.operation != entry.operation {
            return TestResult::failed();
        }
        if deserialized.range_spec.start != entry.range_spec.start {
            return TestResult::failed();
        }
        if deserialized.range_spec.end != entry.range_spec.end {
            return TestResult::failed();
        }
        if deserialized.new_ttl_secs != entry.new_ttl_secs {
            return TestResult::failed();
        }
        if deserialized.access_increment != entry.access_increment {
            return TestResult::failed();
        }

        TestResult::passed()
    }
}
