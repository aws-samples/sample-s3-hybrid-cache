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
use tracing::{debug, error, info, warn};

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
    cleanup_integration: Option<Arc<MultipartCleanupIntegration>>,
    /// Mutex to serialize journal append operations within this instance.
    /// Prevents concurrent appends from overwriting each other (Bug 4 fix).
    append_mutex: Mutex<()>,
}

/// Integration with multipart upload cleanup system
pub struct MultipartCleanupIntegration {
    cache_dir: PathBuf,
}

impl MultipartCleanupIntegration {
    /// Create a new multipart cleanup integration
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    /// Clean up journal entries for a specific cache key during multipart upload cleanup
    pub(crate) async fn cleanup_journal_entries_for_cache_key(
        &self,
        cache_key: &str,
    ) -> Result<u64> {
        debug!(
            "Starting journal cleanup for multipart upload: cache_key={}",
            cache_key
        );

        let mut removed_count = 0;
        let journals_dir = self.cache_dir.join("metadata").join("_journals");

        if !journals_dir.exists() {
            debug!(
                "No journal directory exists during cleanup: cache_key={}",
                cache_key
            );
            return Ok(0);
        }

        // Find all per-instance journal files in the flat _journals directory
        let entries = match std::fs::read_dir(&journals_dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    "Failed to read journal directory during cleanup: cache_key={}, error={}",
                    cache_key, e
                );
                return Ok(0);
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!(
                        "Failed to read journal directory entry: cache_key={}, error={}",
                        cache_key, e
                    );
                    continue;
                }
            };

            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            // Check if this is a per-instance journal file
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.ends_with(".journal") {
                    // Remove journal entries for this cache key from this instance journal file
                    match self
                        .remove_entries_from_journal_file(&path, cache_key)
                        .await
                    {
                        Ok(count) => {
                            removed_count += count;
                            if count > 0 {
                                debug!(
                                    "Removed {} journal entries from instance journal: cache_key={}, file={:?}",
                                    count, cache_key, path
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to remove journal entries from instance journal: cache_key={}, file={:?}, error={}",
                                cache_key, path, e
                            );
                        }
                    }
                }
            }
        }

        info!(
            "Completed journal cleanup for multipart upload: cache_key={}, removed_entries={}",
            cache_key, removed_count
        );

        Ok(removed_count)
    }

    /// Remove entries for a specific cache key from a journal file.
    ///
    /// Reachable only from [`JournalManager::cleanup_for_multipart`], which nothing in the
    /// production request path calls; it is kept for the tests that drive it. It still obeys
    /// the journal protocol: the read, filter and rewrite happen under the journal's
    /// `.journal.lock` (blocking, off the runtime), and the rewrite is a temp file renamed
    /// over the journal, so an append holding the lock is never overwritten and a crash
    /// never leaves a half-written journal. Lines are kept verbatim, including ones this
    /// build cannot parse.
    async fn remove_entries_from_journal_file(
        &self,
        journal_path: &Path,
        cache_key: &str,
    ) -> Result<u64> {
        let path = journal_path.to_path_buf();
        let key = cache_key.to_string();
        tokio::task::spawn_blocking(move || Ok(remove_entries_under_lock(&path, &key)))
            .await
            .map_err(|e| ProxyError::CacheError(format!("Journal rewrite task failed: {}", e)))?
    }
}

/// The body of [`MultipartCleanupIntegration::remove_entries_from_journal_file`], run on a
/// blocking thread while it holds the journal's flock. Returns the number of lines removed;
/// every failure is logged and counts as zero, as before.
fn remove_entries_under_lock(journal_path: &Path, cache_key: &str) -> u64 {
    if !journal_path.exists() {
        return 0;
    }
    let lock_path = journal_path.with_extension("journal.lock");
    let _lock = match lock_journal_lock_file(&lock_path, true) {
        Ok(Some(f)) => f,
        Ok(None) => {
            debug!(
                "Journal lock kept being unlinked during cleanup: file={:?}",
                journal_path
            );
            return 0;
        }
        Err(e) => {
            debug!(
                "Failed to lock journal file during cleanup: file={:?}, error={}",
                journal_path, e
            );
            return 0;
        }
    };

    // Read under the lock, so no append can land between this read and the rename below.
    let content = match std::fs::read_to_string(journal_path) {
        Ok(c) => c,
        Err(e) => {
            // Stale file handle and other transient errors are expected on shared storage
            debug!(
                "Failed to read journal file during cleanup: file={:?}, error={}",
                journal_path, e
            );
            return 0;
        }
    };

    let mut kept = String::with_capacity(content.len());
    let mut removed = 0u64;
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<JournalEntry>(line) {
            Ok(entry) if entry.cache_key == cache_key => removed += 1,
            Ok(_) => {
                kept.push_str(line);
                kept.push('\n');
            }
            Err(e) => {
                // Kept verbatim: a newer build's entry, or a torn tail.
                debug!(
                    "Unparseable journal line kept during cleanup: file={:?}, error={}",
                    journal_path, e
                );
                kept.push_str(line);
                kept.push('\n');
            }
        }
    }
    if removed == 0 {
        return 0;
    }

    let is_fresh_journal = journal_path
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(crate::eviction_worklist::is_fresh_journal_name);
    if kept.is_empty() && is_fresh_journal {
        // A fresh journal is never appended to again; remove it outright.
        if let Err(e) = std::fs::remove_file(journal_path) {
            warn!(
                "Failed to remove empty journal file: file={:?}, error={}",
                journal_path, e
            );
        }
        return removed;
    }

    if kept.is_empty() {
        // Truncate the emptied primary in place and keep it, so the lock file never outlives
        // its journal and its instance stays in the readiness roster. Appenders write only
        // under the lock held here, so no append lands between the read above and this
        // truncate. No `.journal.tmp` is created: a peer's orphan evidence reads one as
        // indeterminate.
        let truncate = (|| -> std::io::Result<()> {
            let f = std::fs::OpenOptions::new().write(true).open(journal_path)?;
            f.set_len(0)?;
            f.sync_all()
        })();
        if let Err(e) = truncate {
            warn!(
                "Failed to truncate emptied journal file: file={:?}, error={}",
                journal_path, e
            );
        }
        return removed;
    }
    // Primary journals are kept so the lock file never outlives its journal.
    let tmp_path = journal_path.with_extension("journal.tmp");
    let rewrite = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(kept.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp_path, journal_path)
    })();
    if let Err(e) = rewrite {
        crate::disk_cache::note_disk_full(&e, "journal.rewrite");
        warn!(
            "Failed to write filtered journal file: file={:?}, error={}",
            journal_path, e
        );
        let _ = std::fs::remove_file(&tmp_path);
    }
    removed
}

impl JournalManager {
    /// Create a new journal manager
    pub fn new(cache_dir: PathBuf, instance_id: String) -> Self {
        let cleanup_integration = Some(Arc::new(MultipartCleanupIntegration::new(
            cache_dir.clone(),
        )));

        Self {
            cache_dir,
            instance_id,
            cleanup_integration,
            append_mutex: Mutex::new(()),
        }
    }

    /// Get journal file path for this instance (single per-instance journal)
    ///
    /// All journal entries from this instance go to a single file:
    /// `metadata/_journals/{instance_id}.journal`
    ///
    /// This matches the format used by CacheHitUpdateBuffer and simplifies
    /// journal consolidation by avoiding per-object journal files.
    pub(crate) fn get_journal_path(&self, _cache_key: &str) -> Result<PathBuf> {
        let journal_path = self
            .cache_dir
            .join("metadata")
            .join("_journals")
            .join(format!("{}.journal", self.instance_id));

        debug!(
            "Journal path constructed: instance_id={}, path={:?}",
            self.instance_id, journal_path
        );

        Ok(journal_path)
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

    /// Get all pending journal entries for a cache key
    pub async fn get_pending_entries(&self, cache_key: &str) -> Result<Vec<JournalEntry>> {
        let journal_path = self.get_journal_path(cache_key)?;

        if !journal_path.exists() {
            return Ok(Vec::new());
        }

        let content = tokio::fs::read_to_string(&journal_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to read journal file: {}", e)))?;

        let mut entries = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
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
                        "Failed to parse journal entry: cache_key={}, line={}, error={}",
                        cache_key, line, e
                    );
                }
            }
        }

        debug!(
            "Retrieved {} pending journal entries: cache_key={}, path={:?}",
            entries.len(),
            cache_key,
            journal_path
        );

        Ok(entries)
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

    /// Clean up journal entries for a multipart upload
    pub async fn cleanup_for_multipart(&self, cache_key: &str) -> Result<()> {
        if let Some(cleanup_integration) = &self.cleanup_integration {
            cleanup_integration
                .cleanup_journal_entries_for_cache_key(cache_key)
                .await?;
        } else {
            warn!(
                "No cleanup integration available for journal cleanup: cache_key={}",
                cache_key
            );
        }
        Ok(())
    }

    /// Get all journal entries from all instances for a cache key
    ///
    /// Reads from per-instance journals: `metadata/_journals/{instance_id}.journal`
    /// Both HybridMetadataWriter and CacheHitUpdateBuffer now use this format.
    /// Like `get_all_entries_for_cache_key` but reads only the provided files instead of
    /// scanning the entire journal directory. Use this when the caller already has a
    /// pre-built index (e.g. from `discover_pending_cache_keys_indexed`) to avoid
    /// redundant full-directory scans on every key during a consolidation cycle.
    pub(crate) async fn get_entries_from_files(
        &self,
        cache_key: &str,
        journal_files: &[std::path::PathBuf],
    ) -> Result<Vec<JournalEntry>> {
        let mut all_entries = Vec::new();

        for journal_file in journal_files {
            match tokio::fs::read_to_string(journal_file).await {
                Ok(content) => {
                    for line in content.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<JournalEntry>(line) {
                            Ok(entry) => {
                                if entry.cache_key == cache_key {
                                    all_entries.push(entry);
                                }
                            }
                            Err(e) => {
                                debug!(
                                    "Failed to parse journal entry: file={:?}, error={}",
                                    journal_file, e
                                );
                            }
                        }
                    }
                }
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

    pub async fn get_all_entries_for_cache_key(
        &self,
        cache_key: &str,
    ) -> Result<Vec<JournalEntry>> {
        let mut all_entries = Vec::new();

        // Read from all per-instance journal files
        let journal_files = self.find_all_journal_files(cache_key).await?;
        for journal_file in &journal_files {
            match tokio::fs::read_to_string(journal_file).await {
                Ok(content) => {
                    for line in content.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<JournalEntry>(line) {
                            Ok(entry) => {
                                if entry.cache_key == cache_key {
                                    all_entries.push(entry);
                                }
                            }
                            Err(e) => {
                                // Parse errors can occur during concurrent writes on shared storage
                                debug!(
                                    "Failed to parse journal entry: file={:?}, error={}",
                                    journal_file, e
                                );
                            }
                        }
                    }
                }
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
        let path = jm.get_journal_path(key).unwrap();
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
        assert_eq!(jm.get_pending_entries(key).await.unwrap().len(), 200);
    }

    /// The multipart rewrite waits for an appender holding `.journal.lock`, then reads the
    /// journal including that append, so the appended line survives the rewrite. Before
    /// the rewrite took no lock and a `tokio::fs::write` could overwrite the append.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_rewrite_racing_an_append_loses_no_line() {
        let temp = TempDir::new().unwrap();
        let jm = JournalManager::new(temp.path().to_path_buf(), "case38".to_string());
        let gone = "bkt/case38/gone";
        let kept = "bkt/case38/kept";
        jm.append_range_entry(gone, entry_a(gone, 0)).await.unwrap();
        let path = jm.get_journal_path(gone).unwrap();
        let lock_path = path.with_extension("journal.lock");

        // An appender holds the lock and has not written yet.
        let held = lock_journal_lock_file(&lock_path, true)
            .unwrap()
            .expect("fixture: the appender must take the lock");
        let integration = MultipartCleanupIntegration::new(temp.path().to_path_buf());
        let rewrite_path = path.clone();
        let rewrite = tokio::spawn(async move {
            integration
                .remove_entries_from_journal_file(&rewrite_path, gone)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !rewrite.is_finished(),
            "the rewrite ran while an appender held the journal lock"
        );

        // The appender's write, under its hold, then release.
        let line = format!("{}\n", serde_json::to_string(&entry_a(kept, 0)).unwrap());
        JournalManager::append_lines_to_primary(&path, &line)
            .await
            .unwrap();
        drop(held);

        assert_eq!(rewrite.await.unwrap().unwrap(), 1, "removed count");
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(
            content.contains(kept),
            "the append racing the rewrite was lost"
        );
        assert!(!content.contains(gone), "the target key must be removed");
        assert!(!path.with_extension("journal.tmp").exists());
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
        let path = jm.get_journal_path(key).unwrap();
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
                jm.get_pending_entries(key).await.unwrap().len(),
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
            .get_pending_entries(cache_key)
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

    #[tokio::test]
    async fn test_multipart_cleanup_integration() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/test-object";
        // `JournalManager::new` wires a `MultipartCleanupIntegration` for this cache_dir.
        let journal_manager =
            JournalManager::new(temp_dir.path().to_path_buf(), "test-instance".to_string());

        // Add some journal entries
        let range_spec = create_test_range_spec();
        let entry1 = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec: range_spec.clone(),
            operation: JournalOperation::Add,
            range_file_path: "test_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        };

        let entry2 = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/other-object".to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "other_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            fill_witness: None,
        };

        journal_manager
            .append_range_entry(cache_key, entry1)
            .await
            .unwrap();
        journal_manager
            .append_range_entry("test-bucket/other-object", entry2)
            .await
            .unwrap();

        // Test cleanup for multipart
        journal_manager
            .cleanup_for_multipart(cache_key)
            .await
            .unwrap();

        // Verify that entries for cache_key are removed but others remain
        let remaining_entries = journal_manager
            .get_pending_entries(cache_key)
            .await
            .unwrap();
        assert_eq!(remaining_entries.len(), 0);

        let other_entries = journal_manager
            .get_pending_entries("test-bucket/other-object")
            .await
            .unwrap();
        assert_eq!(other_entries.len(), 1);
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
    /// Removing a key's last entries truncates a primary journal in place, keeping its file and
    /// inode with no `.journal.tmp`, and deletes a fresh journal outright.
    #[test]
    fn removing_the_last_entries_truncates_a_primary_in_place_and_deletes_a_fresh_journal() {
        use std::os::unix::fs::MetadataExt;
        let temp = TempDir::new().unwrap();
        let journals = temp.path().join("metadata").join("_journals");
        std::fs::create_dir_all(&journals).unwrap();
        let key = "bkt/emptied";
        let line = format!("{}\n", serde_json::to_string(&entry_a(key, 0)).unwrap());
        let primary = journals.join("host-a:42.journal");
        let fresh = journals.join("host-a:42:1700000000000000000.journal");
        std::fs::write(&primary, &line).unwrap();
        std::fs::write(&fresh, &line).unwrap();
        let inode = std::fs::metadata(&primary).unwrap().ino();
        assert_eq!(remove_entries_under_lock(&primary, key), 1);
        assert_eq!(remove_entries_under_lock(&fresh, key), 1);
        let after = std::fs::metadata(&primary).expect("the emptied primary journal is kept");
        assert_eq!(after.len(), 0, "the primary journal is emptied");
        assert_eq!(
            after.ino(),
            inode,
            "the primary journal is truncated in place, not replaced"
        );
        assert!(
            !primary.with_extension("journal.tmp").exists(),
            "no .journal.tmp is left"
        );
        assert!(!fresh.exists(), "a fresh journal is deleted");
    }
}
