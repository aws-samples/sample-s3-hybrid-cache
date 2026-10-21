//! Node-local log of pending write-order stamps.
//!
//! An acknowledged write or delete stamps its key's write-order marker (`{object}.wtok`) on the
//! shared cache volume before its response returns. When that stamp fails, because the volume
//! is refusing or timing out locks (a file-system failover, say), the write is still
//! acknowledged and the stamp is retried in the background until it lands
//! (`CacheManager::spawn_write_stamp_retry`). This log makes that retry durable: one record per
//! such write, appended and fsynced on the node-local volume before the write's response
//! returns. A restart replays it, and the retry's in-memory queue spills to it at its bound
//! instead of dropping the stamp.
//!
//! Format: one JSON object per line,
//! `{"op":"pending"|"landed","dir":<cache_dir>,"key":<cache_key>,"token":<hex>,"t":<unix secs>}`.
//! A key is pending while its newest `pending` record has no `landed` record naming the same
//! token. `landed` records are appended without an fsync: losing one only re-stamps the key
//! after a restart, which can only turn a hit into a miss.
//!
//! The file is rewritten (a temp file in the same directory, fsync, rename) when it is opened
//! and whenever `landed` records since the last rewrite reach the number of pending keys (at
//! least [`COMPACT_MIN_LANDED`]) or leave none pending, so it holds about one line per pending
//! key. An unparseable line, such as a torn final append, is skipped and counted, never fatal.
//!
//! The directory must be node-local, never the shared cache volume: the log exists for the time
//! that volume is refusing writes. A directory inside `cache_dir`, or whose own file system is
//! not a known node-local type, is refused at startup ([`local_state_dir_refusal`]), and
//! the proxy then runs in memory only, as it does when the directory cannot be used at all
//! (`write_stamp_log_memory_only`). A local-disk cache on the root volume, beside the default
//! directory, keeps the log. One process owns a directory at a time (an exclusive `flock` on
//! `pending-stamps.lock`).
//!
//! A cache directory is matched by its canonical path, so a record written under one
//! spelling of it (a symlink, a trailing slash) replays under another.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error};

/// The log's file name inside the configured directory.
pub const LOG_FILE_NAME: &str = "pending-stamps.log";
const TMP_FILE_NAME: &str = "pending-stamps.log.tmp";
const LOCK_FILE_NAME: &str = "pending-stamps.lock";
/// Fewest `landed` records that trigger a rewrite while keys are still pending.
pub(crate) const COMPACT_MIN_LANDED: usize = 1024;
/// Least seconds between two ERRORs for failed log writes.
const WRITE_FAILURE_ERROR_INTERVAL_SECS: u64 = 60;

/// Keys pending in every open log, now.
static LOG_PENDING_KEYS: AtomicU64 = AtomicU64::new(0);
/// Pending records re-queued from the log at startup.
static LOG_REPLAYED: AtomicU64 = AtomicU64::new(0);
/// Unparseable lines skipped while reading the log at startup.
static LOG_TORN: AtomicU64 = AtomicU64::new(0);
/// Failed appends of a pending record (the log could not be written or synced, or is
/// unavailable).
static LOG_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);
static LOG_WRITE_FAILURE_LAST_ERROR: AtomicU64 = AtomicU64::new(0);
/// Open logs running in memory only: their directory was refused or unusable at startup.
static LOG_MEMORY_ONLY: AtomicU64 = AtomicU64::new(0);

/// Keys pending in the node-local log now (gauge).
pub(crate) fn log_pending_keys() -> u64 {
    LOG_PENDING_KEYS.load(Ordering::Relaxed)
}

/// Pending records re-queued from the node-local log at startup, since process start.
pub(crate) fn log_replayed_total() -> u64 {
    LOG_REPLAYED.load(Ordering::Relaxed)
}

/// Unparseable log lines skipped at startup, since process start.
pub(crate) fn log_torn_records_total() -> u64 {
    LOG_TORN.load(Ordering::Relaxed)
}

/// Pending records that could not be written to the node-local log, since process start.
pub(crate) fn log_write_failures_total() -> u64 {
    LOG_WRITE_FAILURES.load(Ordering::Relaxed)
}

/// Pending-stamp logs running in memory only now: `1` on a proxy whose
/// `local_state_dir` was refused or could not be used at startup, so a restart before a pending
/// stamp lands drops it.
pub(crate) fn log_memory_only() -> u64 {
    LOG_MEMORY_ONLY.load(Ordering::Relaxed)
}

pub(crate) fn note_replayed(n: usize) {
    LOG_REPLAYED.fetch_add(n as u64, Ordering::Relaxed);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Op {
    Pending,
    Landed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    op: Op,
    dir: String,
    key: String,
    token: String,
    t: u64,
}

/// A pending stamp read back from the log: its cache directory, key and newest token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingStamp {
    pub(crate) dir: String,
    pub(crate) key: String,
    pub(crate) token: String,
}

/// The pending stamps a log holds, oldest first, and the unparseable lines it skipped.
#[derive(Debug, Default)]
pub(crate) struct LogContents {
    pub(crate) pending: Vec<PendingStamp>,
    pub(crate) torn: u64,
}

#[derive(Debug)]
struct FileState {
    /// Append handle; `None` when the log could not be opened.
    file: Option<File>,
    /// Held for the process's lifetime: the exclusive `flock` on the lock file.
    _lock: Option<File>,
    /// A failed append may have left a partial line: the next append starts a new one.
    needs_newline: bool,
    landed_since_compaction: usize,
    /// Sequence of the last pending record this process appended, in log order.
    seq: u64,
}

/// Why [`StampBacklog::open`] could not open a log, and what the log held.
#[derive(Debug)]
pub(crate) struct OpenFailure {
    pub(crate) error: std::io::Error,
    /// The pending stamps a readable log held, for the caller to replay and guard in memory.
    /// Empty when the log could not be read, or another process owns the directory.
    pub(crate) contents: LogContents,
}

/// One process's node-local pending-stamp log. See the module comment.
#[derive(Debug)]
pub(crate) struct StampBacklog {
    dir: PathBuf,
    path: PathBuf,
    file: Mutex<FileState>,
    /// Pending keys, as a fingerprint of `cache_dir \0 cache_key`, each mapped to a fingerprint
    /// of its newest pending token. A fingerprint collision can only make a key read as pending
    /// (a miss); replay and compaction read the full records from the file.
    live: Mutex<HashMap<u64, u64>>,
    /// Each cache-directory spelling seen, mapped to its canonical path.
    dir_ids: Mutex<HashMap<String, String>>,
    /// The log could not be used or was refused: appends fail and pending stamps live in memory
    /// only.
    memory_only: bool,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn fingerprint(parts: &[&str]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for p in parts {
        p.hash(&mut h);
    }
    h.finish()
}

fn key_fp(dir_id: &str, key: &str) -> u64 {
    fingerprint(&[dir_id, key])
}

/// The absolute form of `path` with `.` components and trailing slashes dropped, without
/// touching the file system.
fn lexical(path: &Path) -> PathBuf {
    std::path::absolute(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .components()
        .collect()
}

/// A cache directory's identity: its canonical path, or its lexical form when it cannot be
/// resolved.
fn canonical_dir(dir: &str) -> String {
    std::fs::canonicalize(dir)
        .unwrap_or_else(|_| lexical(Path::new(dir)))
        .display()
        .to_string()
}

/// `path`'s nearest existing ancestor, and the components below it that do not exist yet.
fn nearest_existing(path: &Path) -> (PathBuf, PathBuf) {
    let mut anchor = lexical(path);
    let mut missing = Vec::new();
    while !anchor.exists() {
        match (
            anchor.file_name().map(|n| n.to_os_string()),
            anchor.parent(),
        ) {
            (Some(name), Some(parent)) => {
                missing.push(name);
                anchor = parent.to_path_buf();
            }
            _ => break,
        }
    }
    (anchor, missing.into_iter().rev().collect())
}

#[cfg(test)]
thread_local! {
    /// Test-only: apply the file-system-type refusal to calls from this thread. Off by default in
    /// tests, whose temporary directories sit on whatever file system the test host uses.
    pub(crate) static ENFORCE_FS_TYPE_CHECK: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

fn fs_type_check_enabled() -> bool {
    #[cfg(test)]
    if !ENFORCE_FS_TYPE_CHECK.with(|c| c.get()) {
        return false;
    }
    true
}

#[cfg(test)]
thread_local! {
    /// Test-only: the `statfs` type [`fs_type`] reports for calls from this thread.
    /// `Some(None)` simulates an unreadable type; `None` reads the real one.
    pub(crate) static FS_TYPE_OVERRIDE: std::cell::Cell<Option<Option<u32>>> =
        const { std::cell::Cell::new(None) };
}

/// `statfs` `f_type` values of the file systems a `local_state_dir` may be on: local
/// file systems that no other host can mount. This is an allowlist: a type not named here,
/// including a network or cluster file system this list has never heard of (GPFS, BeeGFS,
/// OrangeFS and the like), is refused, as is a type that cannot be read. Values are the kernel's
/// `include/uapi/linux/magic.h` (`EXT4_SUPER_MAGIC`, `XFS_SUPER_MAGIC`, `BTRFS_SUPER_MAGIC`,
/// `TMPFS_MAGIC`, `OVERLAYFS_SUPER_MAGIC`, `F2FS_SUPER_MAGIC`), except ZFS, which OpenZFS defines
/// as `ZFS_SUPER_MAGIC` in `include/sys/fs/zfs.h` and which the kernel header does not carry.
pub(crate) const LOCAL_FS_TYPES: &[(u32, &str)] = &[
    (0x0000_EF53, "ext2/ext3/ext4"), // EXT4_SUPER_MAGIC (shared by ext2, ext3 and ext4)
    (0x5846_5342, "XFS"),            // XFS_SUPER_MAGIC
    (0x9123_683E, "btrfs"),          // BTRFS_SUPER_MAGIC
    (0x0102_1994, "tmpfs"),          // TMPFS_MAGIC
    (0x794C_7630, "overlayfs"),      // OVERLAYFS_SUPER_MAGIC
    (0x2FC1_2FC1, "ZFS"),            // ZFS_SUPER_MAGIC (OpenZFS)
    (0xF2F5_2010, "f2fs"),           // F2FS_SUPER_MAGIC
];

/// The name of a known node-local file system type, or `None` for any other type.
pub(crate) fn local_fs_name(f_type: u32) -> Option<&'static str> {
    LOCAL_FS_TYPES
        .iter()
        .find(|(t, _)| *t == f_type)
        .map(|(_, name)| *name)
}

/// The `statfs` `f_type` of the file system holding `path`, or `None` when it cannot be read.
fn fs_type(path: &Path) -> Option<u32> {
    #[cfg(test)]
    if let Some(t) = FS_TYPE_OVERRIDE.with(|c| c.get()) {
        return t;
    }
    statfs_type(path)
}

#[cfg(target_os = "linux")]
fn statfs_type(path: &Path) -> Option<u32> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: `c_path` is NUL-terminated and `st` is a writable `statfs` buffer, which the call
    // fills when it returns 0.
    let rc = unsafe { libc::statfs(c_path.as_ptr(), st.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `statfs` returned 0, so it initialised the buffer.
    let st = unsafe { st.assume_init() };
    // The magic numbers are 32-bit; `f_type`'s width and sign vary by architecture.
    Some((st.f_type as u64 & 0xFFFF_FFFF) as u32)
}

/// Off Linux the type is not read, so [`refusal_by_fs_type`] refuses a same-device directory as
/// unknown.
#[cfg(not(target_os = "linux"))]
fn statfs_type(_path: &Path) -> Option<u32> {
    None
}

/// Why `state_dir` must not hold this proxy's pending-stamp log, or `None` when it may. The log exists for the time the shared cache volume refuses
/// writes, so it must not be on that volume, and a directory another proxy can also open is never
/// used. Refused: a directory inside `cache_dir` (canonicalised, so a symlink into it counts); or
/// one whose own file system, read with `statfs` on the directory's nearest existing ancestor,
/// is not in [`LOCAL_FS_TYPES`] or cannot be read. The directory's own type is judged, not
/// `cache_dir`'s, so a second shared mount and a symlinked subdirectory of `cache_dir` are
/// refused too. A directory on the same local file system as `cache_dir` (a local-disk cache on
/// the root volume) is accepted: no other host can mount it. A directory that does not exist yet
/// is judged by its nearest existing ancestor.
pub(crate) fn local_state_dir_refusal(cache_dir: &Path, state_dir: &Path) -> Option<String> {
    let (anchor, missing) = nearest_existing(state_dir);
    let state = std::fs::canonicalize(&anchor)
        .unwrap_or_else(|_| anchor.clone())
        .join(missing);
    let cache = std::fs::canonicalize(cache_dir).unwrap_or_else(|_| lexical(cache_dir));
    if state.starts_with(&cache) {
        return Some(format!("it is inside cache_dir {:?}", cache_dir));
    }
    if !fs_type_check_enabled() {
        return None;
    }
    refusal_by_fs_type(cache_dir, &anchor)
}

/// The file-system-type half of [`local_state_dir_refusal`] on Linux: `anchor`'s own `statfs`
/// type must be in [`LOCAL_FS_TYPES`].
#[cfg(target_os = "linux")]
fn refusal_by_fs_type(_cache_dir: &Path, anchor: &Path) -> Option<String> {
    match fs_type(anchor) {
        Some(t) if local_fs_name(t).is_some() => None,
        Some(t) => Some(format!(
            "its file system (type 0x{t:08X}) is not a known node-local type ({}), so it may be \
             storage other hosts can mount",
            LOCAL_FS_TYPES
                .iter()
                .map(|(_, n)| *n)
                .collect::<Vec<_>>()
                .join(", ")
        )),
        None => Some("the type of its file system could not be read".to_string()),
    }
}

/// Off Linux the type is not read, so the rule is the one shipped there: a directory on
/// `cache_dir`'s device is refused as of unknown type, and one on another device is accepted.
#[cfg(not(target_os = "linux"))]
fn refusal_by_fs_type(cache_dir: &Path, anchor: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    let (Ok(s), Ok(c)) = (std::fs::metadata(anchor), std::fs::metadata(cache_dir)) else {
        return None;
    };
    if s.dev() != c.dev() {
        return None;
    }
    match fs_type(cache_dir) {
        Some(t) if local_fs_name(t).is_some() => None,
        _ => Some(format!(
            "it is on the same file system as cache_dir {:?} (device {}), whose type could not \
             be read",
            cache_dir,
            c.dev()
        )),
    }
}

fn token_fp(token: &str) -> u64 {
    fingerprint(&[token])
}

/// Read a log, newest token per key winning and a `landed` record clearing the key when it
/// names that token. A missing file is an empty log.
fn read_log(path: &Path) -> std::io::Result<LogContents> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(LogContents::default()),
        Err(e) => return Err(e),
    };
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut seq: u64 = 0;
    let mut latest: HashMap<(String, String), (u64, String)> = HashMap::new();
    let mut by_token: HashMap<String, (String, String)> = HashMap::new();
    let mut torn = 0u64;
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let text = line.strip_suffix(b"\n").unwrap_or(&line);
        if text.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        let record: Record = match serde_json::from_slice(text) {
            Ok(r) => r,
            Err(_) => {
                // A torn append (the process or host died mid-write, or the volume filled):
                // skipped and counted, never fatal.
                torn += 1;
                continue;
            }
        };
        seq += 1;
        match record.op {
            Op::Pending => {
                by_token.insert(
                    record.token.clone(),
                    (record.dir.clone(), record.key.clone()),
                );
                latest.insert((record.dir, record.key), (seq, record.token));
            }
            Op::Landed => {
                // Matched by token: a token names one record, so the landing clears it
                // whichever spelling of the cache directory either record carries.
                if let Some(k) = by_token.remove(&record.token) {
                    if latest.get(&k).is_some_and(|(_, t)| *t == record.token) {
                        latest.remove(&k);
                    }
                }
            }
        }
    }
    let mut pending: Vec<(u64, PendingStamp)> = latest
        .into_iter()
        .map(|((dir, key), (seq, token))| (seq, PendingStamp { dir, key, token }))
        .collect();
    pending.sort_by_key(|(seq, _)| *seq);
    Ok(LogContents {
        pending: pending.into_iter().map(|(_, p)| p).collect(),
        torn,
    })
}

fn encode(op: Op, dir: &str, key: &str, token: &str) -> Vec<u8> {
    let mut line = serde_json::to_vec(&Record {
        op,
        dir: dir.to_string(),
        key: key.to_string(),
        token: token.to_string(),
        t: now_secs(),
    })
    .unwrap_or_default();
    line.push(b'\n');
    line
}

/// Rewrite the log to hold exactly `pending`: a temp file in the same directory, fsynced, then
/// renamed over the log and the directory fsynced.
fn rewrite(dir: &Path, path: &Path, pending: &[PendingStamp]) -> std::io::Result<()> {
    let tmp = dir.join(TMP_FILE_NAME);
    {
        let mut f = File::create(&tmp)?;
        let mut buf = Vec::new();
        for p in pending {
            buf.extend(encode(Op::Pending, &p.dir, &p.key, &p.token));
        }
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

fn open_append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

impl StampBacklog {
    /// Open (creating) the log in `dir`, take its lock, read it and rewrite it compacted.
    /// Returns the log and every pending stamp it held, oldest first. An `Err` means the log
    /// cannot be used; the caller runs with [`Self::unavailable`], replaying and guarding in
    /// memory what the failure says a readable log held: a full or read-only root
    /// volume refuses the rewrite or the lock file, not the read.
    pub(crate) fn open(dir: &Path) -> Result<(Self, LogContents), OpenFailure> {
        let path = dir.join(LOG_FILE_NAME);
        let fail = |error: std::io::Error, read: Option<LogContents>| {
            let contents = read.unwrap_or_else(|| {
                let c = read_log(&path).unwrap_or_default();
                LOG_TORN.fetch_add(c.torn, Ordering::Relaxed);
                c
            });
            OpenFailure { error, contents }
        };
        if let Err(e) = std::fs::create_dir_all(dir) {
            return Err(fail(e, None));
        }
        let lock = match OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(LOCK_FILE_NAME))
        {
            Ok(lock) => lock,
            Err(e) => return Err(fail(e, None)),
        };
        match fs4::FileExt::try_lock(&lock) {
            Ok(()) => {}
            Err(fs4::TryLockError::WouldBlock) => {
                // Another process owns the directory and replays its own log.
                return Err(OpenFailure {
                    error: std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!("another process holds {:?}", dir.join(LOCK_FILE_NAME)),
                    ),
                    contents: LogContents::default(),
                });
            }
            Err(fs4::TryLockError::Error(e)) => return Err(fail(e, None)),
        }
        let contents = match read_log(&path) {
            Ok(contents) => contents,
            Err(error) => {
                return Err(OpenFailure {
                    error,
                    contents: LogContents::default(),
                })
            }
        };
        LOG_TORN.fetch_add(contents.torn, Ordering::Relaxed);
        if let Err(e) = rewrite(dir, &path, &contents.pending) {
            return Err(fail(e, Some(contents)));
        }
        let file = match open_append(&path) {
            Ok(file) => file,
            Err(e) => return Err(fail(e, Some(contents))),
        };
        let backlog = Self::new(dir, Some(file), Some(lock), &contents);
        Ok((backlog, contents))
    }

    fn new(dir: &Path, file: Option<File>, lock: Option<File>, contents: &LogContents) -> Self {
        let memory_only = file.is_none();
        let backlog = Self {
            dir: dir.to_path_buf(),
            path: dir.join(LOG_FILE_NAME),
            file: Mutex::new(FileState {
                file,
                _lock: lock,
                needs_newline: false,
                landed_since_compaction: 0,
                seq: 0,
            }),
            live: Mutex::new(HashMap::new()),
            dir_ids: Mutex::new(HashMap::new()),
            memory_only,
        };
        let live: HashMap<u64, u64> = contents
            .pending
            .iter()
            .map(|p| (backlog.live_key(&p.dir, &p.key), token_fp(&p.token)))
            .collect();
        LOG_PENDING_KEYS.fetch_add(live.len() as u64, Ordering::Relaxed);
        if memory_only {
            LOG_MEMORY_ONLY.fetch_add(1, Ordering::Relaxed);
        }
        *backlog.lock_live() = live;
        backlog
    }

    /// A log that could not be opened, or whose directory was refused: every append fails and
    /// is counted, and the proxy runs in memory only. `contents`, what a readable log
    /// held, is guarded here until each stamp lands.
    pub(crate) fn unavailable(dir: &Path, contents: &LogContents) -> Self {
        Self::new(dir, None, None, contents)
    }

    /// Whether this log runs in memory only. Production reads the process-wide
    /// [`log_memory_only`].
    #[cfg(test)]
    pub(crate) fn memory_only(&self) -> bool {
        self.memory_only
    }

    /// The canonical identity of a cache-directory spelling, resolved once per spelling.
    fn dir_id(&self, dir: &str) -> String {
        let mut ids = self.dir_ids.lock().unwrap_or_else(|p| p.into_inner());
        ids.entry(dir.to_string())
            .or_insert_with(|| canonical_dir(dir))
            .clone()
    }

    /// Whether two spellings name the same cache directory.
    pub(crate) fn same_cache_dir(&self, a: &str, b: &str) -> bool {
        a == b || self.dir_id(a) == self.dir_id(b)
    }

    fn live_key(&self, dir: &str, key: &str) -> u64 {
        key_fp(&self.dir_id(dir), key)
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    fn lock_file(&self) -> std::sync::MutexGuard<'_, FileState> {
        self.file.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn lock_live(&self) -> std::sync::MutexGuard<'_, HashMap<u64, u64>> {
        self.live.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// [`Self::append_pending_batch`] for one key.
    #[cfg(test)]
    pub(crate) fn append_pending(
        &self,
        cache_dir: &str,
        cache_key: &str,
    ) -> Result<(String, u64), ()> {
        self.append_pending_batch(cache_dir, &[cache_key.to_string()])
            .pop()
            .flatten()
            .ok_or(())
    }

    /// Append and fsync a pending record for each of `cache_keys`, each with a fresh token,
    /// before the write's response returns. Blocking. One write and one fsync cover the whole
    /// batch, so handing many keys to the retry costs one node-local sync, not one per
    /// key. Returns each key's token and its record's sequence in log order, in
    /// `cache_keys`' order: two failed writes of one key can append in one order and reach the
    /// retry in the other, and the retry keeps the record with the higher sequence. `None` for
    /// every key (each counted, ERROR at most once a minute) when the log cannot be written:
    /// the stamps are then retried from memory only.
    pub(crate) fn append_pending_batch(
        &self,
        cache_dir: &str,
        cache_keys: &[String],
    ) -> Vec<Option<(String, u64)>> {
        if cache_keys.is_empty() {
            return Vec::new();
        }
        let tokens: Vec<String> = cache_keys
            .iter()
            .map(|_| uuid::Uuid::new_v4().simple().to_string())
            .collect();
        let mut lines = Vec::new();
        for (cache_key, token) in cache_keys.iter().zip(&tokens) {
            lines.extend_from_slice(&encode(Op::Pending, cache_dir, cache_key, token));
        }
        let mut st = self.lock_file();
        let st = &mut *st;
        if st.needs_newline {
            lines.insert(0, b'\n');
        }
        let result = match st.file.as_mut() {
            None => Err(std::io::Error::other(
                "the log could not be opened at startup, or its directory was refused",
            )),
            Some(f) => f.write_all(&lines).and_then(|_| f.sync_data()),
        };
        match result {
            Ok(()) => {
                st.needs_newline = false;
                let mut out = Vec::with_capacity(cache_keys.len());
                // Under the file lock, so `live` holds each key's newest token in log order.
                let mut live = self.lock_live();
                for (cache_key, token) in cache_keys.iter().zip(tokens) {
                    st.seq += 1;
                    let previous =
                        live.insert(self.live_key(cache_dir, cache_key), token_fp(&token));
                    if previous.is_none() {
                        LOG_PENDING_KEYS.fetch_add(1, Ordering::Relaxed);
                    }
                    out.push(Some((token, st.seq)));
                }
                out
            }
            Err(e) => {
                st.needs_newline = st.file.is_some();
                for cache_key in cache_keys {
                    note_write_failure(&self.path, cache_key, &e);
                }
                vec![None; cache_keys.len()]
            }
        }
    }

    /// Record that a stamp covering the pending record with `token` landed. Blocking, no fsync.
    /// Compacts the log when enough have landed.
    pub(crate) fn append_landed(&self, cache_dir: &str, cache_key: &str, token: &str) {
        let mut line = encode(Op::Landed, cache_dir, cache_key, token);
        let mut st = self.lock_file();
        let st = &mut *st;
        // A memory-only log has no file, but its replayed keys still clear.
        let has_file = st.file.is_some();
        if let Some(f) = st.file.as_mut() {
            if st.needs_newline {
                line.insert(0, b'\n');
            }
            match f.write_all(&line) {
                Ok(()) => st.needs_newline = false,
                Err(e) => {
                    st.needs_newline = true;
                    debug!(
                        "Failed to append a landed record to {:?}: cache_key={}, error={}",
                        self.path, cache_key, e
                    );
                }
            }
        }
        let live_now = {
            let mut live = self.lock_live();
            let k = self.live_key(cache_dir, cache_key);
            if live.get(&k) == Some(&token_fp(token)) {
                live.remove(&k);
                LOG_PENDING_KEYS.fetch_sub(1, Ordering::Relaxed);
            }
            live.len()
        };
        if !has_file {
            return;
        }
        st.landed_since_compaction += 1;
        if live_now == 0 || st.landed_since_compaction >= live_now.max(COMPACT_MIN_LANDED) {
            self.compact_locked(st);
        }
    }

    /// Rewrite the log to its pending records. Caller holds the file lock.
    fn compact_locked(&self, st: &mut FileState) {
        let outcome = (|| -> std::io::Result<LogContents> {
            if let Some(f) = st.file.as_mut() {
                f.flush()?;
            }
            let contents = read_log(&self.path)?;
            rewrite(&self.dir, &self.path, &contents.pending)?;
            st.file = Some(open_append(&self.path)?);
            Ok(contents)
        })();
        match outcome {
            Ok(contents) => {
                st.needs_newline = false;
                st.landed_since_compaction = 0;
                let rebuilt: HashMap<u64, u64> = contents
                    .pending
                    .iter()
                    .map(|p| (self.live_key(&p.dir, &p.key), token_fp(&p.token)))
                    .collect();
                let mut live = self.lock_live();
                LOG_PENDING_KEYS.fetch_sub(live.len() as u64, Ordering::Relaxed);
                LOG_PENDING_KEYS.fetch_add(rebuilt.len() as u64, Ordering::Relaxed);
                *live = rebuilt;
            }
            Err(e) => {
                // The log stays as it was, appendable; the next landing tries again.
                debug!("Failed to compact {:?}: {}", self.path, e);
            }
        }
    }

    /// Pending stamps of `cache_dir` in the log now, oldest first, and the sequence of the last
    /// record appended when it was read: every stamp returned is at least as new, in log
    /// order, as any append up to that sequence. Blocking. A memory-only log returns the stamps
    /// it still guards.
    pub(crate) fn pending_for(&self, cache_dir: &str) -> (Vec<PendingStamp>, u64) {
        let st = self.lock_file();
        let seq = st.seq;
        let memory_only = st.file.is_none();
        let pending = match read_log(&self.path) {
            Ok(c) => c.pending,
            Err(e) => {
                debug!("Failed to read {:?}: {}", self.path, e);
                Vec::new()
            }
        };
        let pending = pending
            .into_iter()
            .filter(|p| self.same_cache_dir(&p.dir, cache_dir))
            .filter(|p| {
                !memory_only
                    || self.lock_live().get(&self.live_key(&p.dir, &p.key))
                        == Some(&token_fp(&p.token))
            })
            .collect();
        (pending, seq)
    }

    /// Whether the log holds a pending stamp for `cache_key` of `cache_dir`.
    pub(crate) fn contains(&self, cache_dir: &str, cache_key: &str) -> bool {
        self.lock_live()
            .contains_key(&self.live_key(cache_dir, cache_key))
    }

    /// Test-only: close the log's file and lock, as a process exit does.
    #[cfg(test)]
    pub(crate) fn release_for_restart(&self) {
        let mut st = self.lock_file();
        st.file = None;
        st._lock = None;
    }

    /// Keys pending in this log now.
    #[cfg(test)]
    pub(crate) fn pending_count(&self) -> usize {
        self.lock_live().len()
    }
}

impl Drop for StampBacklog {
    fn drop(&mut self) {
        let n = self.lock_live().len() as u64;
        LOG_PENDING_KEYS.fetch_sub(n, Ordering::Relaxed);
        if self.memory_only {
            LOG_MEMORY_ONLY.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

fn note_write_failure(path: &Path, cache_key: &str, e: &std::io::Error) {
    let failures = LOG_WRITE_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    let now = now_secs().max(1);
    let prev = LOG_WRITE_FAILURE_LAST_ERROR.load(Ordering::Relaxed);
    let due = prev == 0 || now >= prev + WRITE_FAILURE_ERROR_INTERVAL_SECS;
    if due
        && LOG_WRITE_FAILURE_LAST_ERROR
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        error!(
            "Cannot write the node-local pending-stamp log {:?}: a write whose write-order stamp \
             failed is retried from memory only, so a restart before its stamp lands, or a full \
             in-memory queue, lets an older version of the key be served until its TTL: \
             cache_key={}, write_failures_total={}, error={}",
            path, cache_key, failures, e
        );
    } else {
        debug!(
            "Pending-stamp log write failed: cache_key={}, error={}",
            cache_key, e
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newest_token_wins_and_a_landed_record_clears_only_its_token() {
        let dir = tempfile::TempDir::new().unwrap();
        let (log, contents) = StampBacklog::open(dir.path()).unwrap();
        assert!(contents.pending.is_empty());
        let (t1, s1) = log.append_pending("/c", "b/k").unwrap();
        let (t2, s2) = log.append_pending("/c", "b/k").unwrap();
        assert!(s2 > s1, "sequences follow log order");
        log.append_pending("/c", "b/other").unwrap();
        log.append_landed("/c", "b/k", &t1);
        assert!(
            log.contains("/c", "b/k"),
            "an older token's landing leaves the newer one"
        );
        drop(log);
        let (log, contents) = StampBacklog::open(dir.path()).unwrap();
        let keys: Vec<(&str, &str)> = contents
            .pending
            .iter()
            .map(|p| (p.key.as_str(), p.token.as_str()))
            .collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&("b/k", t2.as_str())));
        log.append_landed("/c", "b/k", &t2);
        assert!(!log.contains("/c", "b/k"));
        assert_eq!(log.pending_count(), 1);
    }

    #[test]
    fn a_batch_appends_every_key_in_order_with_one_record_each() {
        let dir = tempfile::TempDir::new().unwrap();
        let (log, _) = StampBacklog::open(dir.path()).unwrap();
        let keys: Vec<String> = (0..5).map(|i| format!("b/k{}", i)).collect();
        let out = log.append_pending_batch("/c", &keys);
        assert_eq!(out.len(), 5);
        let seqs: Vec<u64> = out.iter().map(|r| r.as_ref().unwrap().1).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5], "one record per key, in order");
        for k in &keys {
            assert!(
                log.contains("/c", k),
                "{} must be pending after the batch",
                k
            );
        }
        let (single, seq) = log.append_pending("/c", "b/k9").unwrap();
        assert_eq!(seq, 6);
        assert!(!single.is_empty());
        let (pending, _) = log.pending_for("/c");
        assert_eq!(pending.len(), 6);
    }

    #[test]
    fn landings_compact_the_log_to_its_pending_records() {
        let dir = tempfile::TempDir::new().unwrap();
        let (log, _) = StampBacklog::open(dir.path()).unwrap();
        let tokens: Vec<String> = (0..10)
            .map(|i| log.append_pending("/c", &format!("b/k{i}")).unwrap().0)
            .collect();
        for (i, t) in tokens.iter().enumerate() {
            log.append_landed("/c", &format!("b/k{i}"), t);
        }
        let text = std::fs::read_to_string(dir.path().join(LOG_FILE_NAME)).unwrap();
        assert!(
            text.is_empty(),
            "nothing pending must leave an empty log: {text:?}"
        );
        assert!(!dir.path().join(TMP_FILE_NAME).exists());
    }

    #[test]
    fn a_second_process_cannot_open_the_same_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let (_held, _) = StampBacklog::open(dir.path()).unwrap();
        assert!(StampBacklog::open(dir.path()).is_err());
    }

    #[test]
    fn an_unavailable_log_counts_each_failed_append() {
        let before = log_write_failures_total();
        let log =
            StampBacklog::unavailable(Path::new("/nonexistent/case28"), &LogContents::default());
        assert!(log.append_pending("/c", "b/k").is_err());
        assert!(log_write_failures_total() > before);
        assert!(log.memory_only());
    }

    /// a rewrite that fails at open (as a full or read-only volume refuses its temp
    /// file) still hands back every record the log held.
    #[test]
    fn a_failed_rewrite_at_open_keeps_what_the_log_held() {
        let dir = tempfile::TempDir::new().unwrap();
        {
            let (log, _) = StampBacklog::open(dir.path()).unwrap();
            log.append_pending("/c", "b/k1").unwrap();
            log.append_pending("/c", "b/k2").unwrap();
        }
        std::fs::create_dir(dir.path().join(TMP_FILE_NAME)).unwrap();
        let failure = StampBacklog::open(dir.path()).expect_err("fixture: the rewrite must fail");
        let keys: Vec<&str> = failure
            .contents
            .pending
            .iter()
            .map(|p| p.key.as_str())
            .collect();
        assert_eq!(
            keys,
            ["b/k1", "b/k2"],
            "a failed rewrite discarded a readable log"
        );
        let log = StampBacklog::unavailable(dir.path(), &failure.contents);
        assert!(log.memory_only() && log.contains("/c", "b/k1") && log.contains("/c", "b/k2"));
        let token = failure.contents.pending[0].token.clone();
        log.append_landed("/c", "b/k1", &token);
        assert!(
            !log.contains("/c", "b/k1"),
            "a memory-only landing clears its key"
        );
        let (reload, _) = log.pending_for("/c");
        assert_eq!(
            reload.len(),
            1,
            "a memory-only reload returns only what is still guarded"
        );
    }

    /// a record written under one spelling of the cache directory replays, is
    /// guarded and lands under another.
    #[test]
    fn a_cache_dir_respelled_across_a_restart_still_matches() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache = tempfile::TempDir::new().unwrap();
        let real = cache.path().display().to_string();
        let link = dir.path().join("cache-link");
        std::os::unix::fs::symlink(cache.path(), &link).unwrap();
        let link = format!("{}/", link.display());
        let token = {
            let (log, _) = StampBacklog::open(&dir.path().join("state")).unwrap();
            log.append_pending(&real, "b/k").unwrap().0
        };
        let (log, contents) = StampBacklog::open(&dir.path().join("state")).unwrap();
        assert!(log.same_cache_dir(&contents.pending[0].dir, &link));
        assert!(log.contains(&link, "b/k"), "the respelled key is guarded");
        assert_eq!(log.pending_for(&link).0.len(), 1);
        log.append_landed(&link, "b/k", &token);
        assert!(!log.contains(&real, "b/k") && log.pending_count() == 0);
        drop(log);
        let (_, contents) = StampBacklog::open(&dir.path().join("state")).unwrap();
        assert!(
            contents.pending.is_empty(),
            "the landing clears the record in the file"
        );
    }

    /// the file-system-type rule through the test seam.
    fn refusal_with_type(cache: &Path, state: &Path, f_type: Option<u32>) -> Option<String> {
        ENFORCE_FS_TYPE_CHECK.with(|c| c.set(true));
        FS_TYPE_OVERRIDE.with(|c| c.set(Some(f_type)));
        let r = local_state_dir_refusal(cache, state);
        FS_TYPE_OVERRIDE.with(|c| c.set(None));
        ENFORCE_FS_TYPE_CHECK.with(|c| c.set(false));
        r
    }

    #[test]
    fn every_allowlisted_type_is_accepted() {
        let cache = tempfile::TempDir::new().unwrap();
        let state = tempfile::TempDir::new().unwrap();
        for (t, name) in LOCAL_FS_TYPES {
            assert_eq!(
                refusal_with_type(cache.path(), state.path(), Some(*t)),
                None,
                "{name} (0x{t:08X}) is a node-local type and must be accepted"
            );
        }
        assert_eq!(LOCAL_FS_TYPES.len(), 7, "the allowlist names seven types");
    }

    #[test]
    fn a_type_off_the_allowlist_is_refused_with_its_hex() {
        let cache = tempfile::TempDir::new().unwrap();
        let state = tempfile::TempDir::new().unwrap();
        for (t, what) in [
            (0x0000_6969u32, "NFS"),
            (0x0BD0_0BD0, "Lustre"),
            (0x4750_4653, "GPFS"),
            (0xDEAD_BEEF, "an unknown type"),
        ] {
            let r = refusal_with_type(cache.path(), state.path(), Some(t));
            assert!(
                r.as_deref()
                    .is_some_and(|r| r.contains(&format!("0x{t:08X}"))
                        && r.contains("not a known node-local type")),
                "{what} must be refused with its type in hex: {r:?}"
            );
        }
    }

    #[test]
    fn an_unreadable_type_is_refused() {
        let cache = tempfile::TempDir::new().unwrap();
        let state = tempfile::TempDir::new().unwrap();
        let r = refusal_with_type(cache.path(), state.path(), None);
        assert!(
            r.as_deref()
                .is_some_and(|r| r.contains("could not be read")),
            "an unreadable type must be refused: {r:?}"
        );
    }

    #[test]
    fn inside_cache_dir_is_refused_and_so_is_a_symlink_into_it() {
        let cache = tempfile::TempDir::new().unwrap();
        let elsewhere = tempfile::TempDir::new().unwrap();
        let inside = cache.path().join("state");
        std::fs::create_dir_all(&inside).unwrap();
        let link = elsewhere.path().join("link");
        std::os::unix::fs::symlink(&inside, &link).unwrap();
        for dir in [inside.clone(), link.join("nested")] {
            let r = refusal_with_type(cache.path(), &dir, Some(0xEF53));
            assert!(
                r.as_deref().is_some_and(|r| r.contains("inside cache_dir")),
                "{dir:?} resolves inside cache_dir and must be refused: {r:?}"
            );
        }
    }

    /// the directory's OWN type decides, not `cache_dir`'s, so a shared state directory is
    /// refused wherever `cache_dir` is. The fixture's `cache_dir` and state directory share one
    /// device, and the type is shared-looking (GPFS), which accepted.
    #[test]
    fn a_gpfs_state_dir_on_the_cache_device_is_refused() {
        use std::os::unix::fs::MetadataExt;
        let cache = tempfile::TempDir::new().unwrap();
        let state = tempfile::TempDir::new().unwrap();
        assert_eq!(
            std::fs::metadata(cache.path()).unwrap().dev(),
            std::fs::metadata(state.path()).unwrap().dev(),
            "fixture: one device"
        );
        let r = refusal_with_type(cache.path(), state.path(), Some(0x4750_4653));
        assert!(
            r.is_some(),
            "a GPFS-typed directory on cache_dir's device must be refused"
        );
    }
}
