//! The durable eviction work list: document types, storage layout, and write-once I/O.
//!
//! Spec: `cache-eviction-at-scale` § 16.7.1 (DR-EVICT) and `tasks.md` task 40.
//!
//! This module holds the document types, the write-once I/O, the generation fence, the capability
//! gate and the driver hooks the consolidation cycle schedules. The author that writes chunks is
//! `CacheManager::author_eviction_worklist` and the executor that reads them is
//! `CacheManager::run_eviction_executor_pass`.
//!
//! # Layout
//!
//! Under `metadata/_shard_state/eviction_worklist/{generation}/{bucket}/`:
//!
//! | File | Written by | Rule |
//! |---|---|---|
//! | `{XX}.chunk` | the author, once | published by an exclusive hard link; there is no API that rewrites one |
//! | `{XX}.done` | the executor that finished the chunk, once | same publish; the first finisher wins |
//! | `{XX}.progress.{instance}` | the executor holding the chunk's lease | appended, one [`ProgressRecord`] line per batch, only after that batch's flush succeeded |
//!
//! `{XX}` is the L1 shard. A chunk is never rewritten because a rewritten list is the shape
//! that already lost 24 of 25 entries once in this workstream (design § 16.7.1, "Never rewrite
//! a list"). A finished chunk is marked by a separate `.done` file, not by editing the chunk.
//!
//! # Chunk document
//!
//! One file of newline-terminated JSON lines. Line 1 is the [`ChunkHeader`]; every following
//! line is one [`WorkItem`].
//!
//! - `schema`, `generation`, `author`, `authored_at` (epoch seconds).
//! - `item_count` and `bytes_total` (the sum of the items' `size`).
//! - `bytes_to_free`: the authoring pass's shortfall, the same on every chunk of a generation.
//! - `checksum`: the BLAKE3 hex digest of the header serialized with `checksum` set to the
//!   empty string, followed by every item line including its newline. It covers the header as
//!   well as the items, so a flipped bit in `generation` or `author` is caught too.
//!
//! A reader that finds any mismatch (truncation, a flipped bit, a count or byte-total that does
//! not add up, an unknown schema, a generation that is not the one the path names) returns
//! [`ChunkRead::Torn`]. The executor treats a torn chunk as absent: it does nothing, which is
//! the safe direction (R3.8).
//!
//! # Items carry the object identity
//!
//! The delete path rechecks the object's identity under the key's slot lock
//! ([`ExpectedObjectIdentity`]). An item without one would delete unchecked, so the identity is
//! part of the item schema, not optional, and the executor refuses an item that lacks it.
//!
//! # Generation fence and capability gate
//!
//! `eviction_generation.json` names the current generation. It is written only by the author,
//! under `global_eviction.lock`, after every chunk of that generation is published. A per-directory
//! latch ([`latch_generation`]) only moves forward: a reader that has seen generation G never acts
//! on G-1, and a torn, absent or lower read leaves it where it was. [`generation_admits`] is the
//! executor's question. [`fleet_is_worklist_capable`] says whether every instance that wrote a
//! journal recently has published a `.ready` record with `evict_worklist_capable` set; the roster
//! is the journal directory because a binary without this feature publishes no record at all.
//!
//! # Publish
//!
//!
//! The document is written to a temp file whose name carries the hostname, the pid and a
//! process-local counter (`Shared::Yes` discipline: two instances, or two tasks in one process,
//! never share a temp path), synced, then published with `link(tmp, path)`. `link` fails with
//! `EEXIST` if the target exists, atomically, so a second author of the same path loses rather
//! than replaces. `rename` would silently replace the first author's chunk.
//!
//! A `link` that executed and then reports `EEXIST` or `EREMOTEIO` (an NFS retransmit) reads as
//! `Published` within the same call, because the temp and the final are one inode. The same
//! file reads as `AlreadyExists { identical: true }` only when a caller publishes again, since
//! by then the temp is gone. A stat or read that fails with anything but `NotFound` is retried
//! and then returned as `Err`; it is never read as "a peer's file" or "different bytes".

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::disk_cache::ExpectedObjectIdentity;
use crate::path_safety::is_safe_path_component;
use crate::staged_fold::Shard;
use crate::{ProxyError, Result};
use tracing::warn;

/// Schema version of the chunk header and the done-marker.
pub(crate) const SCHEMA: u32 = 1;

/// Directory under `metadata/_shard_state/` holding work-list generations.
const WORKLIST_DIR: &str = "eviction_worklist";

/// Red-side lever: a stat or read error is a verdict again (absent, "not my inode", "different
/// bytes") instead of being retried and then returned as `Err`.
#[cfg(test)]
const STAT_ERROR_IS_VERDICT: bool = false;

/// Red-side lever: the byte compare reports identical always.
#[cfg(test)]
const PUBLISH_IGNORES_BYTES: bool = false;

/// Red-side lever: publish by `write_atomic` (rename over), which replaces an existing chunk.
#[cfg(test)]
const PUBLISH_BY_RENAME_OVER: bool = false;

/// Disambiguates temp names within one process.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------------------------
// Document types
// ---------------------------------------------------------------------------------------------

/// One eviction decision: delete `[start, end]` of `cache_key`, if the object is still the
/// version `identity` describes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkItem {
    pub cache_key: String,
    pub start: u64,
    pub end: u64,
    /// The object version the author selected this extent from.
    pub identity: ExpectedObjectIdentity,
    /// The extent was a staged range when selected.
    pub staged: bool,
    pub is_write_cached: bool,
    /// On-disk (compressed) bytes of the extent.
    pub compressed_size: u64,
    /// The candidate's size, the figure the author counted toward its target.
    pub size: u64,
}

/// The first line of a chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChunkHeader {
    pub schema: u32,
    pub generation: u64,
    pub author: String,
    /// Unix seconds.
    pub authored_at: u64,
    pub item_count: u64,
    pub bytes_total: u64,
    /// The authoring pass's shortfall: what the whole generation was selected to free, the same
    /// figure on every chunk of it. `0` on a chunk written without one (a test fixture).
    ///
    /// Required, not `#[serde(default)]`: with a default, a bit flip in this key's name parses as
    /// an ignored unknown field plus the default, re-serializes to the original header, and passes
    /// the checksum (`every_single_bit_flip_reads_as_torn`). No chunk predates the field.
    pub bytes_to_free: u64,
    /// See the module documentation.
    pub checksum: String,
}

/// A decoded chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Chunk {
    pub header: ChunkHeader,
    pub items: Vec<WorkItem>,
}

/// Why a chunk file was not accepted. Every variant means "treat the chunk as absent".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TornReason {
    /// The file exists but could not be read.
    Unreadable,
    /// No newline-terminated first line.
    MissingHeader,
    /// The first line did not parse as a header.
    BadHeader,
    /// The schema is not one this binary understands.
    UnknownSchema,
    /// The header names a different generation than the path.
    GenerationMismatch,
    /// The stored checksum is not the checksum of the content.
    ChecksumMismatch,
    /// An item line did not parse, or the item section does not end in a newline.
    BadItem,
    /// `item_count` is not the number of item lines.
    CountMismatch,
    /// `bytes_total` is not the sum of the items' `size`.
    BytesMismatch,
}

/// The result of reading a chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChunkRead {
    /// No file at the path.
    Absent,
    /// A file that failed validation.
    Torn(TornReason),
    /// A valid chunk.
    Chunk(Chunk),
}

/// The result of a write-once publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
    /// This call created the file.
    Published,
    /// A file was already there before this call, and was left untouched: a peer's, or an
    /// earlier attempt by this process whose reply was lost. `identical`
    /// says whether its bytes equal what this call tried to publish. A caller that re-calls after
    /// an `Err`, or after a crash, reads `Published` or `AlreadyExists`; `identical: true` if
    /// this process's earlier attempt landed, `identical: false` if a peer's did. A reply lost inside
    /// one call reads as `Published` when the inode proves this call linked it.
    AlreadyExists { identical: bool },
}

/// The `.done` marker: the chunk at this path has been fully executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DoneMarker {
    pub schema: u32,
    pub generation: u64,
    /// The instance that finished the chunk.
    pub executor: String,
    /// Unix seconds.
    pub completed_at: u64,
    /// The checksum of the chunk this marker closes.
    pub chunk_checksum: String,
}

/// The result of reading a done-marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DoneRead {
    Absent,
    /// Bytes were read and are unparseable or of an unknown schema.
    Torn,
    /// The read failed with an I/O error other than `NotFound` (`EIO`, `ESTALE`, `EISDIR`). It
    /// says nothing about whether a marker exists, so it is never read as finished.
    Unreadable,
    Done(DoneMarker),
}

/// Red-side lever: a re-confirmed torn done-marker reads as "not finished" again.
#[cfg(test)]
const TORN_DONE_IS_UNFINISHED: bool = false;

/// Red-side lever: an unreadable done-marker reads as finished, as `Torn` does.
#[cfg(test)]
const UNREADABLE_DONE_IS_FINISHED: bool = false;

impl DoneRead {
    /// Whether the chunk this marker closes counts as executed, for the author's drain guard and
    /// for the executor's skip. Ask it of a [`read_done_settled`] result, never of one read.
    ///
    /// `Done` is finished and `Absent` is not. **A `Torn` that survived a re-read is finished.**
    /// A file reaches a done path only through [`publish_done`], which links a temp that was
    /// fully written and synced, and only after the executor's final accumulator flush. So a file
    /// at the path proves the chunk was executed even when its bytes cannot be read back; the
    /// damage is in the marker, not in the work. Reading it as "not finished" instead stalls
    /// eviction fleet-wide, because the author writes no new generation while any chunk of the
    /// current one is unfinished (supersession option B), and nothing can repair a write-once
    /// marker. If the file is a foreign write and the chunk was not executed, its unexecuted
    /// extents are still on disk and in the `.meta`, so the next generation selects them again:
    /// under-eviction for one generation, never a lost byte or a double debit.
    ///
    /// **`Unreadable` is not finished.** An I/O error proves no file was read, so it cannot carry
    /// the argument above: two `EIO`s on an absent marker would otherwise skip an unexecuted chunk
    /// and let the author treat the generation as drained while a peer is mid-chunk. The cost of
    /// reading it as unfinished is a deferral: the author waits and the executor re-reads on its
    /// next pass.
    pub(crate) fn is_finished(&self) -> bool {
        match self {
            DoneRead::Done(_) => true,
            DoneRead::Absent => false,
            #[cfg(test)]
            DoneRead::Unreadable if UNREADABLE_DONE_IS_FINISHED => true,
            DoneRead::Unreadable => false,
            #[cfg(test)]
            DoneRead::Torn if TORN_DONE_IS_UNFINISHED => false,
            DoneRead::Torn => true,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------------------------

fn item_section(items: &[WorkItem]) -> Vec<u8> {
    let mut out = Vec::new();
    for item in items {
        // Serializing a struct of strings, integers and bools cannot fail.
        out.extend(serde_json::to_vec(item).unwrap_or_default());
        out.push(b'\n');
    }
    out
}

fn header_line(header: &ChunkHeader) -> Vec<u8> {
    serde_json::to_vec(header).unwrap_or_default()
}

fn checksum_of(header: &ChunkHeader, items: &[u8]) -> String {
    let mut cleared = header.clone();
    cleared.checksum = String::new();
    let mut hasher = blake3::Hasher::new();
    hasher.update(&header_line(&cleared));
    hasher.update(b"\n");
    hasher.update(items);
    hasher.finalize().to_hex().to_string()
}

fn encode_parts(header: &ChunkHeader, items: &[u8]) -> Vec<u8> {
    let mut out = header_line(header);
    out.push(b'\n');
    out.extend_from_slice(items);
    out
}

impl Chunk {
    /// Build a chunk with no `bytes_to_free`, for test fixtures.
    #[cfg(test)]
    pub(crate) fn new(
        generation: u64,
        author: &str,
        authored_at: u64,
        items: Vec<WorkItem>,
    ) -> Self {
        Self::new_for_pass(generation, author, authored_at, 0, items)
    }

    /// Build a chunk carrying the authoring pass's `bytes_to_free` in the header. The count, byte
    /// total and checksum are computed here, never supplied.
    pub(crate) fn new_for_pass(
        generation: u64,
        author: &str,
        authored_at: u64,
        bytes_to_free: u64,
        items: Vec<WorkItem>,
    ) -> Self {
        let section = item_section(&items);
        let mut header = ChunkHeader {
            schema: SCHEMA,
            generation,
            author: author.to_string(),
            authored_at,
            item_count: items.len() as u64,
            bytes_total: items.iter().fold(0u64, |a, i| a.saturating_add(i.size)),
            bytes_to_free,
            checksum: String::new(),
        };
        header.checksum = checksum_of(&header, &section);
        Self { header, items }
    }

    /// The bytes of the chunk document.
    pub(crate) fn encode(&self) -> Vec<u8> {
        encode_parts(&self.header, &item_section(&self.items))
    }
}

/// Validate and decode a chunk document. `expected_generation` is the generation the chunk's
/// path names.
pub(crate) fn decode_chunk(bytes: &[u8], expected_generation: u64) -> ChunkRead {
    let Some(newline) = bytes.iter().position(|b| *b == b'\n') else {
        return ChunkRead::Torn(TornReason::MissingHeader);
    };
    let Ok(header) = serde_json::from_slice::<ChunkHeader>(&bytes[..newline]) else {
        return ChunkRead::Torn(TornReason::BadHeader);
    };
    if header.schema != SCHEMA {
        return ChunkRead::Torn(TornReason::UnknownSchema);
    }
    if header.generation != expected_generation {
        return ChunkRead::Torn(TornReason::GenerationMismatch);
    }
    let section = &bytes[newline + 1..];
    if checksum_of(&header, section) != header.checksum {
        return ChunkRead::Torn(TornReason::ChecksumMismatch);
    }
    let mut items = Vec::new();
    if !section.is_empty() {
        if section.last() != Some(&b'\n') {
            return ChunkRead::Torn(TornReason::BadItem);
        }
        for line in section[..section.len() - 1].split(|b| *b == b'\n') {
            match serde_json::from_slice::<WorkItem>(line) {
                Ok(item) => items.push(item),
                Err(_) => return ChunkRead::Torn(TornReason::BadItem),
            }
        }
    }
    if items.len() as u64 != header.item_count {
        return ChunkRead::Torn(TornReason::CountMismatch);
    }
    let total = items.iter().fold(0u64, |a, i| a.saturating_add(i.size));
    if total != header.bytes_total {
        return ChunkRead::Torn(TornReason::BytesMismatch);
    }
    ChunkRead::Chunk(Chunk { header, items })
}

// ---------------------------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------------------------

fn generation_dir(cache_dir: &Path, generation: u64) -> PathBuf {
    cache_dir
        .join("metadata")
        .join("_shard_state")
        .join(WORKLIST_DIR)
        .join(generation.to_string())
}

fn shard_file(cache_dir: &Path, generation: u64, shard: &Shard, suffix: &str) -> Option<PathBuf> {
    if !is_safe_path_component(&shard.bucket) || !is_safe_path_component(&shard.l1) {
        return None;
    }
    Some(
        generation_dir(cache_dir, generation)
            .join(&shard.bucket)
            .join(format!("{}{}", shard.l1, suffix)),
    )
}

/// `{generation}/{bucket}/{XX}.chunk`, or `None` if a component is not a safe path component.
pub(crate) fn chunk_path(cache_dir: &Path, generation: u64, shard: &Shard) -> Option<PathBuf> {
    shard_file(cache_dir, generation, shard, ".chunk")
}

/// `{generation}/{bucket}/{XX}.done`.
pub(crate) fn done_path(cache_dir: &Path, generation: u64, shard: &Shard) -> Option<PathBuf> {
    shard_file(cache_dir, generation, shard, ".done")
}

/// `{generation}/{bucket}/{XX}.progress.{instance}`. Task 47 writes it.
pub(crate) fn progress_path(
    cache_dir: &Path,
    generation: u64,
    shard: &Shard,
    instance: &str,
) -> Option<PathBuf> {
    if !is_safe_path_component(instance) {
        return None;
    }
    shard_file(
        cache_dir,
        generation,
        shard,
        &format!(".progress.{}", instance),
    )
}

// ---------------------------------------------------------------------------------------------
// Write-once I/O
// ---------------------------------------------------------------------------------------------

/// The link step of a publish: `link(temp, final)`. The production value is [`real_link`]; tests
/// pass closures that model an NFS retransmit (the link executes and the caller is told
/// `EEXIST` or `EREMOTEIO` anyway).
type LinkFn = dyn Fn(&Path, &Path) -> std::io::Result<()> + Send + Sync;

/// How many times a `link` that failed with an errno other than `EEXIST` is tried in all, when
/// the final does not exist afterwards (one attempt plus three retries).
const LINK_ATTEMPTS: u32 = 4;

/// Base of the linear backoff between link retries.
const LINK_BACKOFF: std::time::Duration = std::time::Duration::from_millis(20);

/// Red-side lever: skip the inode comparison, so an `EEXIST` always falls to the byte compare.
#[cfg(test)]
const PUBLISH_SKIPS_INODE_CHECK: bool = false;

/// Red-side lever: an Unknown stat inside `same_inode` alone reads as "different inode" again
/// (the old `_ => false`). Rule 3's and the read probe's handling are unchanged.
#[cfg(test)]
const SAME_INODE_STAT_ERROR_IS_VERDICT: bool = false;

fn real_link(tmp: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::hard_link(tmp, path)
}

fn fresh_tmp_path(path: &Path) -> PathBuf {
    let host = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
        .replace(['/', '\\'], "_");
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    path.with_file_name(crate::cache::bounded_instance_tmp_name(
        &name,
        &format!(".{}", TMP_COUNTER.fetch_add(1, Ordering::Relaxed)),
        &host,
        std::process::id(),
    ))
}

/// Device and inode of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    dev: u64,
    ino: u64,
}

type StatFn = dyn Fn(&Path) -> std::io::Result<FileId> + Send + Sync;
type ReadFn = dyn Fn(&Path) -> std::io::Result<Vec<u8>> + Send + Sync;
type ProbeFn<T> = dyn Fn(&Path) -> std::io::Result<T> + Send + Sync;

/// The filesystem calls a publish makes after staging. Tests inject each.
#[derive(Clone)]
struct PublishIo {
    link: std::sync::Arc<LinkFn>,
    stat: std::sync::Arc<StatFn>,
    read: std::sync::Arc<ReadFn>,
}

fn real_stat(path: &Path) -> std::io::Result<FileId> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).map(|m| FileId {
        dev: m.dev(),
        ino: m.ino(),
    })
}

fn real_read(path: &Path) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}

impl PublishIo {
    fn real(link: std::sync::Arc<LinkFn>) -> Self {
        Self {
            link,
            stat: std::sync::Arc::new(real_stat),
            read: std::sync::Arc::new(real_read),
        }
    }
}

/// Run `f(path)` and classify the answer: `Ok(Some)` Present, `Ok(None)` Absent (`NotFound`
/// only), `Err` Unknown after [`LINK_ATTEMPTS`] tries with [`LINK_BACKOFF`] between them.
///
/// `on_lever` is the answer the old code gave an Unknown error; only the red-side lever uses it.
async fn probe<T: Send + 'static>(
    f: &std::sync::Arc<ProbeFn<T>>,
    path: &Path,
    on_lever: Option<T>,
) -> std::io::Result<Option<T>> {
    let mut on_lever = on_lever;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let (f, p) = (f.clone(), path.to_path_buf());
        let answer = tokio::task::spawn_blocking(move || f(&p))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e)));
        match answer {
            Ok(v) => return Ok(Some(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                #[cfg(test)]
                if STAT_ERROR_IS_VERDICT {
                    return Ok(on_lever.take());
                }
                let _ = &mut on_lever;
                if attempt >= LINK_ATTEMPTS {
                    return Err(e);
                }
                tokio::time::sleep(LINK_BACKOFF * attempt).await;
            }
        }
    }
}

/// Whether `a` and `b` name the same inode on the same device. A missing temp or final is
/// "different"; an Unknown stat is `Err`.
async fn same_inode(io: &PublishIo, a: &Path, b: &Path) -> std::io::Result<bool> {
    #[cfg(test)]
    if PUBLISH_SKIPS_INODE_CHECK {
        return Ok(false);
    }
    let sa = probe(&io.stat, a, None).await;
    #[cfg(test)]
    let sa = if SAME_INODE_STAT_ERROR_IS_VERDICT {
        Ok(sa.unwrap_or(None))
    } else {
        sa
    };
    let sb = probe(&io.stat, b, None).await;
    #[cfg(test)]
    let sb = if SAME_INODE_STAT_ERROR_IS_VERDICT {
        Ok(sb.unwrap_or(None))
    } else {
        sb
    };
    match (sa?, sb?) {
        (Some(x), Some(y)) => Ok(x == y),
        _ => Ok(false),
    }
}

/// What [`resolve_existing`] found.
enum Resolved {
    Outcome(PublishOutcome),
    /// The final was gone by the time it was read: a peer removed it.
    Vanished,
}

/// The final exists and this call did not get a clean `Ok` from `link`. Decide who published.
///
/// An NFS client that retransmits a `link` RPC can have the first copy execute and the retransmit
/// answer `EEXIST` (or the reply be lost and surface as `EREMOTEIO`) to the SAME process. If the
/// temp and the final are one inode, this process published. Only otherwise is the final a peer's,
/// and then the bytes decide whether it is the same document. A stat or read that fails with
/// anything but `NotFound` is retried and then returned as `Err`, never read as a verdict.
async fn resolve_existing(
    io: &PublishIo,
    tmp: &Path,
    path: &Path,
    bytes: &[u8],
) -> std::io::Result<Resolved> {
    if same_inode(io, tmp, path).await? {
        return Ok(Resolved::Outcome(PublishOutcome::Published));
    }
    let Some(existing) = probe(&io.read, path, Some(Vec::new())).await? else {
        return Ok(Resolved::Vanished);
    };
    // `mut` is used only under the `cfg(test)` PUBLISH_IGNORES_BYTES lever; non-test builds
    // never mutate it.
    #[allow(unused_mut)]
    let mut identical = existing == bytes;
    #[cfg(test)]
    if PUBLISH_IGNORES_BYTES {
        identical = true;
    }
    Ok(Resolved::Outcome(PublishOutcome::AlreadyExists {
        identical,
    }))
}

/// Remove the temp. `ENOENT` is success (a replayed `unlink` or a vanished temp leaves the same
/// state); any other error leaves a stray temp name that no reader looks at.
async fn remove_tmp(tmp: &Path) {
    if let Err(e) = tokio::fs::remove_file(tmp).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::debug!("eviction work list: could not remove temp {:?}: {}", tmp, e);
        }
    }
}

/// Create and fill the temp, synced. One retry under a fresh name: an NFS `EREMOTEIO` on the
/// create was seen in 69 of 500 contended rounds on FSx for NetApp ONTAP.
async fn stage_tmp(path: &Path, bytes: &[u8]) -> std::io::Result<PathBuf> {
    use tokio::io::AsyncWriteExt;
    let mut last = None;
    for _ in 0..2 {
        let tmp = fresh_tmp_path(path);
        let staged = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)
                .await?;
            file.write_all(bytes).await?;
            // Before the link, never after: a published name must not point at unwritten blocks.
            file.sync_all().await
        }
        .await;
        match staged {
            Ok(()) => return Ok(tmp),
            Err(e) => {
                remove_tmp(&tmp).await;
                last = Some(e);
            }
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("temp create failed")))
}

/// Publish `bytes` at `path` if nothing is there. Never replaces an existing file.
///
/// `Err` means "could not establish", not "not published": the final may or may not be this
/// process's file, and a re-call reads `Published` or `AlreadyExists`; `identical: true` if this
/// process's earlier attempt landed, `identical: false` if a peer's did.
async fn publish_once(path: &Path, bytes: &[u8]) -> Result<PublishOutcome> {
    publish_once_with(path, bytes, std::sync::Arc::new(real_link)).await
}

/// [`publish_once`] with the link step injected.
async fn publish_once_with(
    path: &Path,
    bytes: &[u8],
    link: std::sync::Arc<LinkFn>,
) -> Result<PublishOutcome> {
    publish_once_io(path, bytes, PublishIo::real(link)).await
}

/// [`publish_once`] with every post-stage filesystem call injected.
///
/// | `link` returns | Then |
/// |---|---|
/// | `Ok` | `Published`. |
/// | `EEXIST` | Inode of temp and final equal: `Published`. Otherwise `AlreadyExists`, `identical` from the bytes. The final gone when read: back to the link loop. |
/// | any other error | Final present: as `EEXIST`. Final `NotFound`: retry up to [`LINK_ATTEMPTS`] in all, with backoff, then the error. Stat Unknown after its own retries: `Err`. |
///
/// `Err` means "could not establish", not "not published": the final may or may not be this
/// process's file, and a re-call reads `Published` or `AlreadyExists`; `identical: true` if this
/// process's earlier attempt landed, `identical: false` if a peer's did. The temp's `unlink` is best effort and `ENOENT` is success.
async fn publish_once_io(path: &Path, bytes: &[u8], io: PublishIo) -> Result<PublishOutcome> {
    #[cfg(test)]
    if PUBLISH_BY_RENAME_OVER {
        crate::disk_cache::write_atomic(path, bytes, crate::disk_cache::Shared::Yes).await?;
        return Ok(PublishOutcome::Published);
    }

    let cache_err = |what: &str, e: std::io::Error| {
        ProxyError::CacheError(format!("eviction work list: {} {:?}: {}", what, path, e))
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| cache_err("could not create the parent of", e))?;
    }
    let tmp = stage_tmp(path, bytes)
        .await
        .map_err(|e| cache_err("could not stage", e))?;

    let mut attempt = 0u32;
    let outcome = loop {
        attempt += 1;
        let (l, t, p) = (io.link.clone(), tmp.clone(), path.to_path_buf());
        let linked = tokio::task::spawn_blocking(move || l(&t, &p))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e)));
        let exists = match linked {
            Ok(()) => break Ok(PublishOutcome::Published),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(true),
            Err(e) => {
                // EREMOTEIO, EIO, ETIMEDOUT: the link may or may not have executed.
                match probe(&io.stat, path, None).await {
                    Ok(Some(_)) => Ok(true),
                    Ok(None) if attempt >= LINK_ATTEMPTS => {
                        break Err(cache_err("could not publish", e));
                    }
                    Ok(None) => Ok(false),
                    Err(stat_err) => Err(stat_err),
                }
            }
        };
        match exists {
            Err(e) => break Err(cache_err("could not establish the state of", e)),
            Ok(true) => match resolve_existing(&io, &tmp, path, bytes).await {
                Ok(Resolved::Outcome(o)) => break Ok(o),
                Ok(Resolved::Vanished) if attempt >= LINK_ATTEMPTS => {
                    break Err(ProxyError::CacheError(format!(
                        "eviction work list: {:?} kept vanishing after EEXIST",
                        path
                    )));
                }
                // A peer removed the final: nothing is published, so link again.
                Ok(Resolved::Vanished) => {}
                Err(e) => break Err(cache_err("could not establish the state of", e)),
            },
            Ok(false) => {}
        }
        tokio::time::sleep(LINK_BACKOFF * attempt).await;
    };
    remove_tmp(&tmp).await;
    outcome
}

/// Publish a chunk at `path`. A second publish to the same path returns
/// [`PublishOutcome::AlreadyExists`] and leaves the first file byte-for-byte.
///
/// `Err` means "could not establish", not "not published": the final may or may not be this
/// process's file, and a re-call reads `Published` or `AlreadyExists`; `identical: true` if this
/// process's earlier attempt landed, `identical: false` if a peer's did.
pub(crate) async fn publish_chunk(path: &Path, chunk: &Chunk) -> Result<PublishOutcome> {
    publish_once(path, &chunk.encode()).await
}

/// Read and validate the chunk at `path`, which belongs to `generation`.
pub(crate) async fn read_chunk(path: &Path, generation: u64) -> ChunkRead {
    match tokio::fs::read(path).await {
        Ok(bytes) => decode_chunk(&bytes, generation),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => ChunkRead::Absent,
        Err(_) => ChunkRead::Torn(TornReason::Unreadable),
    }
}

/// Publish a done-marker at `path`; the first finisher wins and later ones are
/// [`PublishOutcome::AlreadyExists`].
///
/// `Err` means "could not establish", not "not published": the final may or may not be this
/// process's file, and a re-call reads `Published` or `AlreadyExists`; `identical: true` if this
/// process's earlier attempt landed, `identical: false` if a peer's did.
pub(crate) async fn publish_done(path: &Path, marker: &DoneMarker) -> Result<PublishOutcome> {
    let mut body = serde_json::to_vec(marker)
        .map_err(|e| ProxyError::CacheError(format!("eviction work list: done marker: {}", e)))?;
    body.push(b'\n');
    publish_once(path, &body).await
}

/// Read the done-marker at `path`.
pub(crate) async fn read_done(path: &Path) -> DoneRead {
    match tokio::fs::read(path).await {
        Ok(bytes) => match serde_json::from_slice::<DoneMarker>(&bytes) {
            Ok(marker) if marker.schema == SCHEMA => DoneRead::Done(marker),
            _ => DoneRead::Torn,
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DoneRead::Absent,
        Err(_) => DoneRead::Unreadable,
    }
}

/// [`read_done`], re-read once after a short backoff when it reads `Torn` or `Unreadable`, so a
/// transient error is not taken for damage. Pair with [`DoneRead::is_finished`].
pub(crate) async fn read_done_settled(path: &Path) -> DoneRead {
    let first = read_done(path).await;
    if first != DoneRead::Torn && first != DoneRead::Unreadable {
        return first;
    }
    tokio::time::sleep(LINK_BACKOFF).await;
    read_done(path).await
}

/// [`read_chunk`], re-read once after a short backoff when it reads `Torn(Unreadable)`.
pub(crate) async fn read_chunk_settled(path: &Path, generation: u64) -> ChunkRead {
    let first = read_chunk(path, generation).await;
    if first != ChunkRead::Torn(TornReason::Unreadable) {
        return first;
    }
    tokio::time::sleep(LINK_BACKOFF).await;
    read_chunk(path, generation).await
}

// ---------------------------------------------------------------------------------------------
// Progress records (task 47)
// ---------------------------------------------------------------------------------------------

/// One line of `{XX}.progress.{instance}`: a batch of the chunk that is executed, accounted and
/// whose debit the accumulator flushed successfully. Written only after that flush, so a record
/// never claims a batch whose debit is still in memory.
///
/// The `_through` figures are cumulative over the executor's walk of the chunk, which visits
/// objects in the same order on every attempt (first occurrence in the item list), so a prefix
/// covers the same items whichever executor wrote it. `listed_bytes_through` is in the unit of
/// [`ChunkHeader::bytes_total`] (the items' `size`), which is what the author subtracts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProgressRecord {
    pub schema: u32,
    pub generation: u64,
    /// The checksum of the chunk the record belongs to.
    pub chunk_checksum: String,
    /// Batches completed by this executor in this attempt, this one included.
    pub batch: u64,
    /// Items completed so far: every item of every object in the completed batches, whether it
    /// was unlinked, already absent, identity-skipped or active.
    pub items_through: u64,
    /// Summed `size` of those items.
    pub listed_bytes_through: u64,
    /// `compressed_size` this batch actually unlinked and debited.
    pub unlinked_bytes: u64,
}

/// Append `record` to the progress file at `path` as one line, then `fsync`. The file has one
/// writer (the instance named in its path, holding the chunk's lease). A crash mid-append leaves
/// a torn last line, which [`chunk_progress_bytes`] skips.
pub(crate) async fn append_progress(path: &Path, record: &ProgressRecord) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut line = serde_json::to_vec(record)
        .map_err(|e| ProxyError::CacheError(format!("eviction progress: serialize: {}", e)))?;
    line.push(b'\n');
    let io_err =
        |e: std::io::Error| ProxyError::CacheError(format!("eviction progress: {:?}: {}", path, e));
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(io_err)?;
    file.write_all(&line).await.map_err(io_err)?;
    file.sync_data().await.map_err(io_err)
}

/// The largest `listed_bytes_through` any executor recorded for the chunk at `shard` of
/// `generation` with checksum `chunk_checksum`, capped at `bytes_total`. Lines for another chunk,
/// unparseable lines and unreadable files count as no progress, which over-counts what is in
/// flight: the safe direction, since under option B the figure only delays authoring.
pub(crate) async fn chunk_progress_bytes(
    cache_dir: &Path,
    generation: u64,
    shard: &Shard,
    chunk_checksum: &str,
    bytes_total: u64,
) -> u64 {
    let Some(bucket_dir) =
        chunk_path(cache_dir, generation, shard).and_then(|p| p.parent().map(Path::to_path_buf))
    else {
        return 0;
    };
    let prefix = format!("{}.progress.", shard.l1);
    let Ok(mut entries) = tokio::fs::read_dir(&bucket_dir).await else {
        return 0;
    };
    let mut best = 0u64;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if !entry.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        let Ok(bytes) = tokio::fs::read(entry.path()).await else {
            continue;
        };
        for line in bytes.split(|b| *b == b'\n') {
            if let Ok(r) = serde_json::from_slice::<ProgressRecord>(line) {
                if r.schema == SCHEMA
                    && r.generation == generation
                    && r.chunk_checksum == chunk_checksum
                {
                    best = best.max(r.listed_bytes_through);
                }
            }
        }
    }
    best.min(bytes_total)
}

// ---------------------------------------------------------------------------------------------
// The generation janitor (task 47)
// ---------------------------------------------------------------------------------------------

/// What [`reap_old_generations`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ReapedGenerations {
    /// The generation the document named when the janitor ran; never reaped.
    pub current: u64,
    /// Generation directories removed, ascending.
    pub reaped: Vec<u64>,
    /// Directories that could not be removed; the next run retries them.
    pub failed: u64,
}

/// Remove every generation directory except the two highest numbered and the current one.
///
/// Outside the execution path, on the pattern of the dead-instance temp reaper: nothing an
/// executor or the author does waits on it, and a failure leaves directories for the next run.
/// Scheduled by [`driver_janitor_tick`] after every abort tick, so the directory holds at most
/// three generations plus any whose removal failed.
///
/// **Never reaps a live generation.** Under supersession option B only the generation the document
/// names can hold an unfinished chunk, and it is excluded by number whatever its rank, so it is
/// kept even when crashed authors left two higher, never-published directories above it. A
/// document that reads torn twice is `Err` and nothing is reaped, because the janitor would not
/// know which generation is live. Every other directory is either drained (its generation was
/// superseded by a document that was only written once it drained) or was never published, so no
/// executor acts on it: [`generation_admits`] refuses any generation that is not the current one.
/// "Drained" covers an aborted generation too: [`unfinished_work`] counts the chunks of an aborted
/// generation whose executor lease is still live, and the author defers on them, so a generation
/// that was superseded after an abort had no executor left inside a batch.
pub(crate) async fn reap_old_generations(cache_dir: &Path) -> Result<ReapedGenerations> {
    let current = settled_current_generation(cache_dir).await?;
    let root = cache_dir
        .join("metadata")
        .join("_shard_state")
        .join(WORKLIST_DIR);
    let mut out = ReapedGenerations {
        current,
        ..Default::default()
    };
    let mut generations = Vec::new();
    match tokio::fs::read_dir(&root).await {
        Ok(mut entries) => loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    if let Ok(g) = entry.file_name().to_string_lossy().parse::<u64>() {
                        generations.push(g);
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(ProxyError::CacheError(format!(
                        "eviction work list janitor: could not list {:?}: {}",
                        root, e
                    )))
                }
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => {
            return Err(ProxyError::CacheError(format!(
                "eviction work list janitor: could not list {:?}: {}",
                root, e
            )))
        }
    }
    generations.sort_unstable();
    let keep_from = generations.len().saturating_sub(2);
    for &g in &generations[..keep_from] {
        if g == current {
            continue;
        }
        match tokio::fs::remove_dir_all(generation_dir(cache_dir, g)).await {
            Ok(()) => out.reaped.push(g),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => out.failed += 1,
        }
    }
    Ok(out)
}

/// The shards with a `.chunk` file under `generation`, sorted. A missing generation directory is
/// none; a directory that cannot be listed is `Err`.
pub(crate) async fn list_chunk_shards(cache_dir: &Path, generation: u64) -> Result<Vec<Shard>> {
    let mut out = Vec::new();
    if generation == 0 {
        return Ok(out);
    }
    let list_err = |dir: &Path, e: std::io::Error| {
        ProxyError::CacheError(format!(
            "eviction work list: could not list {:?}: {}",
            dir, e
        ))
    };
    let root = generation_dir(cache_dir, generation);
    let mut buckets = match tokio::fs::read_dir(&root).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(list_err(&root, e)),
    };
    while let Some(bucket) = buckets.next_entry().await.map_err(|e| list_err(&root, e))? {
        let bucket_name = bucket.file_name().to_string_lossy().to_string();
        let bucket_dir = bucket.path();
        let mut files = match tokio::fs::read_dir(&bucket_dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotADirectory => continue,
            Err(e) => return Err(list_err(&bucket_dir, e)),
        };
        while let Some(file) = files
            .next_entry()
            .await
            .map_err(|e| list_err(&bucket_dir, e))?
        {
            let name = file.file_name().to_string_lossy().to_string();
            if let Some(l1) = name.strip_suffix(".chunk") {
                out.push(Shard::new(bucket_name.clone(), l1));
            }
        }
    }
    out.sort();
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// The generation fence
// ---------------------------------------------------------------------------------------------

/// `eviction_generation.json` under `metadata/_shard_state/`.
const GENERATION_FILE: &str = "eviction_generation.json";

/// Schema of [`GenerationDoc`]. A document with another value reads as [`GenerationRead::Torn`].
pub(crate) const GENERATION_SCHEMA: u32 = 1;

/// Red-side lever: the latch takes whatever it last observed instead of the larger value.
#[cfg(test)]
const LATCH_ASSIGNS_OBSERVED: bool = false;

/// The generation document: the one number that says which work list is current.
///
/// Written only by the author, while it holds `global_eviction.lock`, and written **last**, after
/// every chunk of that generation is in place (task 45), so a generation an executor can see
/// is a generation whose chunks are all published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GenerationDoc {
    pub schema: u32,
    pub generation: u64,
}

/// What a read of `eviction_generation.json` found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenerationRead {
    /// No document: no work list has ever been authored on this volume.
    Absent,
    /// The document exists and could not be read, parsed, or carries an unknown schema.
    Torn,
    Generation(u64),
}

/// What [`advance_generation`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Advance {
    Advanced,
    /// `next` was not ahead of the newest generation this process has seen; nothing was written.
    NotAhead {
        current: u64,
    },
}

/// The per-cache-directory monotone latch: the newest generation this process has observed.
///
/// Keyed by directory, not process-global, for the reason `staged_authority::EPOCH_STATE` is:
/// lib tests are threads of one process, and a shared latch would let one test's generation
/// decide another's. Pattern of `staged_authority::epoch_latched_for`.
static GENERATION_LATCH: std::sync::RwLock<std::collections::BTreeMap<PathBuf, u64>> =
    std::sync::RwLock::new(std::collections::BTreeMap::new());

/// `{cache_dir}/metadata/_shard_state/eviction_generation.json`.
pub(crate) fn generation_path(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join("_shard_state")
        .join(GENERATION_FILE)
}

/// The newest generation this process has observed for `cache_dir`, from the latch alone, with
/// no I/O. `0` before any observation; no authored generation is ever `0`.
pub(crate) fn generation_latched_for(cache_dir: &Path) -> u64 {
    GENERATION_LATCH
        .read()
        .ok()
        .and_then(|m| m.get(cache_dir).copied())
        .unwrap_or(0)
}

/// Raise the latch for `cache_dir` to `observed` if it is higher, and return the latched value.
///
/// **A lower or equal `observed` changes nothing.** That is the whole fence: a reader that has
/// seen generation G never acts on G-1, so a stale directory listing, a cached NFS attribute or
/// a torn read cannot walk it backwards. The one legitimate way a volume's generation goes down
/// is a fresh cache volume, which is a fresh process-visible directory as well.
pub(crate) fn latch_generation(cache_dir: &Path, observed: u64) -> u64 {
    match GENERATION_LATCH.write() {
        Ok(mut map) => {
            let cell = map.entry(cache_dir.to_path_buf()).or_insert(0);
            #[cfg(test)]
            if LATCH_ASSIGNS_OBSERVED {
                *cell = observed;
                return *cell;
            }
            if observed > *cell {
                *cell = observed;
            }
            *cell
        }
        // A poisoned latch cannot be raised; the observation itself is still the best figure.
        Err(_) => observed,
    }
}

/// Read `eviction_generation.json`, ignoring the latch.
pub(crate) async fn read_generation(cache_dir: &Path) -> GenerationRead {
    match tokio::fs::read(generation_path(cache_dir)).await {
        Ok(bytes) => match serde_json::from_slice::<GenerationDoc>(&bytes) {
            Ok(doc) if doc.schema == GENERATION_SCHEMA => {
                GenerationRead::Generation(doc.generation)
            }
            _ => GenerationRead::Torn,
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => GenerationRead::Absent,
        Err(_) => GenerationRead::Torn,
    }
}

/// The current generation: read the document, raise the latch, return the latched value.
///
/// Never lower than an earlier return for the same directory: an absent or torn read, or a
/// document that reads lower than the latch, yields the latched value, not the read one.
pub(crate) async fn current_generation(cache_dir: &Path) -> u64 {
    match read_generation(cache_dir).await {
        GenerationRead::Generation(g) => latch_generation(cache_dir, g),
        GenerationRead::Absent | GenerationRead::Torn => generation_latched_for(cache_dir),
    }
}

/// Whether an executor may act on a chunk of `chunk_generation`: it must be the latched current
/// generation, re-read now. An older chunk is stale; a newer one is not yet published (the
/// author writes the generation document last) and is not acted on either.
pub(crate) async fn generation_admits(cache_dir: &Path, chunk_generation: u64) -> bool {
    chunk_generation != 0 && chunk_generation == current_generation(cache_dir).await
}

/// Write generation `next` as the current one.
///
/// **The caller holds `global_eviction.lock`**, and the `_lock` witness is how that is asserted:
/// only [`crate::cache::CacheManager::eviction_lock_held`] constructs one outside tests, and it
/// returns `None` unless this process holds the lock. The witness reads no lockfile, so the caller
/// also runs the R5.3 fence immediately before this call. The caller has also published every chunk
/// of `next`, which the type cannot see; the author's ordering test pins it. The document has one
/// writer at a time, which is why a plain atomic replace is correct for it, unlike a chunk.
/// `next` must be ahead of both the document and the latch; otherwise nothing is written. The
/// latch is raised only after the write lands, so a failed write leaves it where it was.
pub(crate) async fn advance_generation(
    _lock: &crate::cache::EvictionLockHeld<'_>,
    cache_dir: &Path,
    next: u64,
) -> Result<Advance> {
    let on_disk = match read_generation(cache_dir).await {
        GenerationRead::Generation(g) => g,
        GenerationRead::Absent | GenerationRead::Torn => 0,
    };
    let current = on_disk.max(generation_latched_for(cache_dir));
    if next <= current {
        latch_generation(cache_dir, current);
        return Ok(Advance::NotAhead { current });
    }
    let mut body = serde_json::to_vec(&GenerationDoc {
        schema: GENERATION_SCHEMA,
        generation: next,
    })
    .map_err(|e| ProxyError::CacheError(format!("eviction generation: serialize: {}", e)))?;
    body.push(b'\n');
    crate::disk_cache::write_atomic(
        &generation_path(cache_dir),
        &body,
        crate::disk_cache::Shared::Yes,
    )
    .await?;
    latch_generation(cache_dir, next);
    Ok(Advance::Advanced)
}

// ---------------------------------------------------------------------------------------------
// What the author reads
// ---------------------------------------------------------------------------------------------

/// The current generation for the author: like [`current_generation`], except that a document
/// that reads `Torn` twice is `Err`, not "the latch".
///
/// [`read_generation`] folds a read error that is not `NotFound` into `Torn`, so `Torn` is not
/// proof of damage. The executor may fall back to the latch because acting on nothing is safe for
/// it; the author may not, because a fresh process's latch is `0`, and authoring against `0` while
/// generation G holds unfinished chunks would list G's extents a second time.
pub(crate) async fn settled_current_generation(cache_dir: &Path) -> Result<u64> {
    for attempt in 1..=2u32 {
        match read_generation(cache_dir).await {
            GenerationRead::Generation(g) => return Ok(latch_generation(cache_dir, g)),
            GenerationRead::Absent => return Ok(generation_latched_for(cache_dir)),
            GenerationRead::Torn if attempt < 2 => tokio::time::sleep(LINK_BACKOFF).await,
            GenerationRead::Torn => {}
        }
    }
    Err(ProxyError::CacheError(format!(
        "eviction generation: {:?} read as torn twice; not authoring",
        generation_path(cache_dir)
    )))
}

/// The highest numbered directory under `eviction_worklist/`, `0` if there is none.
///
/// The listing is bounded: [`driver_janitor_tick`] reaps every directory but the two highest and
/// the current one on each consolidation cycle, so this reads a handful of entries, not every
/// generation the volume has had.
///
/// The author's next generation is one above the larger of this and the current generation, so a
/// directory an aborted or crashed author left behind (chunks published, generation never
/// written) is skipped rather than published into: its chunks are never executed, because no
/// generation document ever names them, and a new chunk can never collide with one of them.
pub(crate) async fn highest_generation_dir(cache_dir: &Path) -> Result<u64> {
    let root = cache_dir
        .join("metadata")
        .join("_shard_state")
        .join(WORKLIST_DIR);
    let mut entries = match tokio::fs::read_dir(&root).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(ProxyError::CacheError(format!(
                "eviction work list: could not list {:?}: {}",
                root, e
            )))
        }
    };
    let mut highest = 0u64;
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                if let Ok(g) = entry.file_name().to_string_lossy().parse::<u64>() {
                    highest = highest.max(g);
                }
            }
            Ok(None) => return Ok(highest),
            Err(e) => {
                return Err(ProxyError::CacheError(format!(
                    "eviction work list: could not list {:?}: {}",
                    root, e
                )))
            }
        }
    }
}

/// The chunks of one generation that have no done-marker: what is still in flight.
///
/// The author defers while `chunks` is non-zero (supersession option B), so it never selects
/// against an in-flight list and needs no exclusion set from it.
#[derive(Debug, Default)]
pub(crate) struct UnfinishedWork {
    /// Sum of the unfinished chunks' `bytes_total`, minus what their progress records show
    /// already executed and flushed ([`chunk_progress_bytes`]), reported by a deferring author.
    pub bytes: u64,
    /// The subtracted part: listed bytes of unfinished chunks that progress records cover.
    pub progressed_bytes: u64,
    /// How many chunks are unfinished.
    pub chunks: u64,
    /// The generation carries an abort marker ([`abort_path`]). Nothing of it is executed past
    /// an executor's next batch boundary, so every other figure is zero except `chunks`, which
    /// counts the chunks whose executor lease is still live (residual (a)): such an
    /// executor may be inside a batch it has unlinked and not yet flushed.
    pub aborted: bool,
    /// Done-marker paths of unfinished chunks whose marker read failed with an I/O error twice
    ///Each is logged at WARN; [`abort_if_unsafe`] aborts the generation
    /// when one stays unreadable past [`UNREADABLE_MARKER_ABORT_SECS`].
    pub unreadable_markers: Vec<PathBuf>,
}

/// Red-side lever: the guard reads an aborted generation as drained without
/// looking at executor leases.
#[cfg(test)]
const GUARD_IGNORES_LIVE_LEASES: bool = false;

/// The executor lease (`eviction_claims`) on `shard`: `Ok(None)` when there is none, `Err` when
/// it cannot be read or does not parse.
pub(crate) async fn read_executor_lease(
    cache_dir: &Path,
    shard: &Shard,
) -> std::io::Result<Option<crate::orphan_reclaim::ShardLease>> {
    let path =
        crate::orphan_reclaim::lease_path(cache_dir, crate::cache::EVICTION_CLAIMS_DIR, shard);
    match tokio::fs::read_to_string(&path).await {
        Ok(text) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Whether some executor may still hold `shard`'s chunk at `now`: a lease that has not lapsed,
/// or one that cannot be read.
///
/// A lease that reads but does not parse counts as live only until
/// it is older than `orphan_reclaim::UNREADABLE_LEASE_EXPIRE_SECS`, then it is removed and reads
/// as expired, as `orphan_reclaim::claim_shard_lease` treats it. No running executor holds such a
/// lease (its claim returned `Busy` when the write failed), so the bound only has to outlast a
/// peer's create caught mid-write. Without it a torn lease on an aborted generation deferred
/// every author forever. An I/O error reading the lease still counts as live.
async fn executor_lease_live(cache_dir: &Path, shard: &Shard, now: u64) -> bool {
    match read_executor_lease(cache_dir, shard).await {
        Ok(None) => false,
        Ok(Some(lease)) => lease.claimed_at.saturating_add(lease.lease_secs) > now,
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            let path = crate::orphan_reclaim::lease_path(
                cache_dir,
                crate::cache::EVICTION_CLAIMS_DIR,
                shard,
            );
            tokio::task::spawn_blocking(move || {
                if !crate::orphan_reclaim::unreadable_lease_expired(&path, now) {
                    return true;
                }
                let text = match std::fs::read(&path) {
                    Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
                    Err(_) => return false,
                };
                crate::orphan_reclaim::remove_expired_unreadable_lease(
                    &path,
                    &text,
                    now,
                    "drain-guard",
                );
                false
            })
            .await
            .unwrap_or(true)
        }
        Err(_) => true,
    }
}

/// The part of a chunk lease an executor keeps in reserve: it starts a batch only while at
/// least this much of its lease remains (residual (b)). Half the lease.
pub(crate) fn batch_lease_margin_secs(lease_secs: u64) -> u64 {
    lease_secs / 2
}

/// Whether `instance`, which claimed `shard`'s chunk at `claimed_at` for
/// `lease_secs`, may start another batch at `now`.
///
/// Only when the lease file still holds that exact claim and at least
/// [`batch_lease_margin_secs`] of it remains. A peer takes a chunk over only once its lease has
/// lapsed, so a batch started under this rule is flushed before any peer can claim the chunk,
/// and so before any done-marker but this executor's own, provided the batch (unlink,
/// accounting, flush) takes less than the margin less the clock skew between the two hosts. A
/// lease that is absent, unreadable, someone else's, or too close to lapsing stops the
/// executor at this boundary; stopping is always safe, because a re-executed chunk unlinks and
/// debits nothing twice.
pub(crate) async fn may_start_batch(
    cache_dir: &Path,
    shard: &Shard,
    instance: &str,
    claimed_at: u64,
    lease_secs: u64,
    now: u64,
) -> bool {
    match read_executor_lease(cache_dir, shard).await {
        Ok(Some(lease)) => {
            lease.instance_id == instance
                && lease.claimed_at == claimed_at
                && lease.lease_secs == lease_secs
                && claimed_at.saturating_add(lease_secs)
                    >= now.saturating_add(batch_lease_margin_secs(lease_secs))
        }
        Ok(None) | Err(_) => false,
    }
}

/// List `generation`'s chunks and collect the unfinished ones.
///
/// | Read | Treated as |
/// |---|---|
/// | chunk `Torn(Unreadable)` twice | `Err`: the chunk may be valid and executable, so whether the generation is drained cannot be established |
/// | chunk torn for a content reason | not in flight: every executor treats it as absent (R3.8), so it frees nothing |
/// | done-marker `Done` | finished |
/// | done-marker `Absent` | unfinished |
/// | done-marker `Torn` twice | finished ([`DoneRead::is_finished`]): the executor skips it too, and reading it as unfinished would defer the author forever |
/// | done-marker `Unreadable` twice | unfinished: an I/O error proves nothing was read, so the author defers |
///
/// A directory that cannot be listed is `Err`. A missing generation directory is no work.
pub(crate) async fn unfinished_work(cache_dir: &Path, generation: u64) -> Result<UnfinishedWork> {
    let mut out = UnfinishedWork::default();
    if generation == 0 {
        return Ok(out);
    }
    // Task 48: an aborted generation is over. Its executors stop at their next batch boundary,
    // and what they had not reached is never executed, so it is not in flight.
    //
    // A batch already past its boundary when the abort landed still unlinks,
    // accounts and flushes. Its executor releases the chunk's lease only after that flush (or
    // after the chunk ends some other way), so a live lease marks a chunk whose debit may be
    // missing from the projected size, and it is in flight. A lease that cannot be read is in
    // flight too. A crashed executor's lease defers the author until it lapses.
    match read_aborted(cache_dir, generation).await {
        Ok(true) => {
            out.aborted = true;
            #[cfg(test)]
            if GUARD_IGNORES_LIVE_LEASES {
                return Ok(out);
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            for shard in list_chunk_shards(cache_dir, generation).await? {
                if executor_lease_live(cache_dir, &shard, now).await {
                    out.chunks += 1;
                }
            }
            return Ok(out);
        }
        Ok(false) => {}
        Err(e) => {
            return Err(ProxyError::CacheError(format!(
                "eviction work list: abort marker of generation {} unreadable ({}); not authoring",
                generation, e
            )))
        }
    }
    let list_err = |dir: &Path, e: std::io::Error| {
        ProxyError::CacheError(format!(
            "eviction work list: could not list {:?}: {}",
            dir, e
        ))
    };
    let root = generation_dir(cache_dir, generation);
    let mut buckets = match tokio::fs::read_dir(&root).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(list_err(&root, e)),
    };
    while let Some(bucket) = buckets.next_entry().await.map_err(|e| list_err(&root, e))? {
        let bucket_name = bucket.file_name().to_string_lossy().to_string();
        let bucket_dir = bucket.path();
        let mut files = match tokio::fs::read_dir(&bucket_dir).await {
            Ok(e) => e,
            // A file where a bucket directory should be is not a work list (the abort marker is
            // one).
            Err(e) if e.kind() == std::io::ErrorKind::NotADirectory => continue,
            Err(e) => return Err(list_err(&bucket_dir, e)),
        };
        while let Some(file) = files
            .next_entry()
            .await
            .map_err(|e| list_err(&bucket_dir, e))?
        {
            let name = file.file_name().to_string_lossy().to_string();
            let Some(l1) = name.strip_suffix(".chunk") else {
                continue;
            };
            let shard = Shard::new(bucket_name.clone(), l1);
            let (Some(chunk_at), Some(done_at)) = (
                chunk_path(cache_dir, generation, &shard),
                done_path(cache_dir, generation, &shard),
            ) else {
                continue;
            };
            let mut read = read_chunk(&chunk_at, generation).await;
            if read == ChunkRead::Torn(TornReason::Unreadable) {
                tokio::time::sleep(LINK_BACKOFF).await;
                read = read_chunk(&chunk_at, generation).await;
            }
            let chunk = match read {
                ChunkRead::Chunk(c) => c,
                ChunkRead::Absent | ChunkRead::Torn(TornReason::GenerationMismatch) => continue,
                ChunkRead::Torn(TornReason::Unreadable) => {
                    return Err(ProxyError::CacheError(format!(
                        "eviction work list: chunk {:?} unreadable twice; not authoring",
                        chunk_at
                    )))
                }
                ChunkRead::Torn(_) => continue,
            };
            let done = read_done_settled(&done_at).await;
            if done.is_finished() {
                continue;
            }
            if done == DoneRead::Unreadable {
                warn!(
                    "[EVICTION_WORKLIST] done-marker {:?} of generation {} unreadable twice; the \
                     chunk counts as unfinished and the author defers",
                    done_at, generation
                );
                out.unreadable_markers.push(done_at.clone());
            }
            // Task 47: a half-executed chunk stops counting its executed half as pending.
            let progressed = chunk_progress_bytes(
                cache_dir,
                generation,
                &shard,
                &chunk.header.checksum,
                chunk.header.bytes_total,
            )
            .await;
            out.chunks += 1;
            out.progressed_bytes = out.progressed_bytes.saturating_add(progressed);
            out.bytes = out
                .bytes
                .saturating_add(chunk.header.bytes_total - progressed);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Task 48: aborting a live generation
// ---------------------------------------------------------------------------------------------

/// The abort marker's file name in a generation directory. The leading underscore keeps it out
/// of the bucket namespace (an S3 bucket name cannot contain `_`), so no bucket directory can
/// collide with it.
const ABORT_FILE: &str = "_aborted.json";

/// How long a done-marker may stay unreadable (an I/O error on every read) before the
/// generation it belongs to is aborted.
///
/// Under supersession option B an unfinished chunk defers every author, so a marker that never
/// reads again would stop eviction for the whole fleet. 30 minutes is twice the 900 s default
/// chunk lease task 49 sets: long enough that an NFS server failover or a remount, which clear in
/// seconds to minutes, is not taken for a permanent fault, and short enough that the stall is
/// bounded by one lease cycle past it. Aborting is the safe direction: it executes nothing, and
/// the next author selects afresh from the live size state with every item identity-checked at
/// execution, so the cost is one re-listing.
pub(crate) const UNREADABLE_MARKER_ABORT_SECS: u64 = 1800;

/// `{generation_dir}/_aborted.json`.
pub(crate) fn abort_path(cache_dir: &Path, generation: u64) -> PathBuf {
    generation_dir(cache_dir, generation).join(ABORT_FILE)
}

/// The abort marker: generation `generation` is over, and no executor runs another batch of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AbortMarker {
    pub schema: u32,
    pub generation: u64,
    /// Why: `not-capable: {instance}` or `done-marker unreadable: {path}`.
    pub reason: String,
    /// Unix seconds.
    pub aborted_at: u64,
    /// The generation's unfinished listed bytes when it was aborted (`UnfinishedWork::bytes`):
    /// the most the fleet can over-evict through it, since a legacy pass sizes itself from a
    /// projected size that does not yet carry them.
    pub remaining_bytes: u64,
}

/// Whether `generation` has an abort marker. The marker's existence is the decision, so a torn
/// one still reads `true`. `Err` is an I/O error other than `NotFound`; the executor treats it as
/// aborted (stop, the safe direction) and the author as unknown (defer).
pub(crate) async fn read_aborted(cache_dir: &Path, generation: u64) -> std::io::Result<bool> {
    match tokio::fs::metadata(abort_path(cache_dir, generation)).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// What [`abort_if_unsafe`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AbortTick {
    /// No live generation: none authored, or the current one is drained or already aborted.
    NoLiveGeneration,
    /// The current generation is live and stands.
    Live {
        generation: u64,
        capability: Capability,
    },
    /// The current generation was live and is now aborted (by this call, or by a peer that won
    /// the publish race).
    Aborted {
        generation: u64,
        reason: String,
        remaining_bytes: u64,
    },
}

/// First tick at which each done-marker path was seen unreadable, per process.
static UNREADABLE_SINCE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<PathBuf, u64>>,
> = std::sync::OnceLock::new();

/// Record `generation`'s unreadable markers at `now` and return the first one that has stayed
/// unreadable for [`UNREADABLE_MARKER_ABORT_SECS`]. A marker of this generation that has read
/// again since is forgotten, so a later fault starts its own clock.
fn stale_unreadable(
    cache_dir: &Path,
    generation: u64,
    unreadable: &[PathBuf],
    now: u64,
) -> Option<PathBuf> {
    let root = generation_dir(cache_dir, generation);
    let mut since = UNREADABLE_SINCE
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    since.retain(|path, _| !path.starts_with(&root) || unreadable.contains(path));
    let mut stale = None;
    for path in unreadable {
        let first = *since.entry(path.clone()).or_insert(now);
        if stale.is_none() && now.saturating_sub(first) >= UNREADABLE_MARKER_ABORT_SECS {
            stale = Some(path.clone());
        }
    }
    stale
}

/// Task 48 (b): re-evaluate the live generation and abort it when it is
/// no longer safe to finish.
///
/// Any proxy may call it on any tick; it takes no lock, because the abort marker is write-once
/// and every writer of it means the same thing. It aborts when:
///
/// - [`fleet_is_worklist_capable`] says `NotCapable`: an older proxy joined after the
///   generation was authored, and its legacy pass will evict from a projected size that does not
///   yet carry the generation's remaining bytes;
/// - a done-marker of the generation has read unreadable on every tick for
///   [`UNREADABLE_MARKER_ABORT_SECS`], which would otherwise defer every author indefinitely.
///
/// `Unknown` does not abort: it proves nothing about the roster, and the author already refuses
/// to author on it. Executors stop at their next batch boundary when they see the marker.
pub(crate) async fn abort_if_unsafe(cache_dir: &Path, now: u64) -> Result<AbortTick> {
    let generation = settled_current_generation(cache_dir).await?;
    if generation == 0 {
        return Ok(AbortTick::NoLiveGeneration);
    }
    // An aborted generation needs nothing more here. Checked before
    // `unfinished_work`, whose aborted arm reads one lease per chunk shard for the author's drain
    // guard; every proxy runs this tick every cycle. An I/O error falls through to
    // `unfinished_work`, which reports it.
    if matches!(read_aborted(cache_dir, generation).await, Ok(true)) {
        return Ok(AbortTick::NoLiveGeneration);
    }
    let work = unfinished_work(cache_dir, generation).await?;
    if work.aborted || work.chunks == 0 {
        return Ok(AbortTick::NoLiveGeneration);
    }
    let capability = fleet_is_worklist_capable(cache_dir, now);
    let reason = match &capability {
        Capability::NotCapable { instance } => Some(format!("not-capable: {}", instance)),
        _ => stale_unreadable(cache_dir, generation, &work.unreadable_markers, now)
            .map(|path| format!("done-marker unreadable: {}", path.display())),
    };
    let Some(reason) = reason else {
        return Ok(AbortTick::Live {
            generation,
            capability,
        });
    };
    let marker = AbortMarker {
        schema: SCHEMA,
        generation,
        reason: reason.clone(),
        aborted_at: now,
        remaining_bytes: work.bytes,
    };
    let mut body = serde_json::to_vec(&marker)
        .map_err(|e| ProxyError::CacheError(format!("eviction work list: abort marker: {}", e)))?;
    body.push(b'\n');
    let at = abort_path(cache_dir, generation);
    // `AlreadyExists` is a peer that aborted first; either marker ends the generation.
    publish_once(&at, &body).await?;
    warn!(
        "[EVICTION_WORKLIST] generation {} aborted ({}); {} listed bytes unexecuted, executors stop \
         at their next batch boundary",
        generation, reason, work.bytes
    );
    Ok(AbortTick::Aborted {
        generation,
        reason,
        remaining_bytes: work.bytes,
    })
}

/// Simulated fleet state for the task 48 tests in `cache.rs`: journals and `.ready` records.
#[cfg(test)]
pub(crate) mod test_fleet {
    use std::path::Path;

    /// A journal file named `{instance}.journal`, modified at `modified`.
    pub(crate) fn journal(cache: &Path, instance: &str, modified: u64) {
        let dir = cache.join("metadata").join("_journals");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.journal", instance));
        std::fs::write(&path, b"").unwrap();
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_unix_time(modified as i64, 0),
        )
        .unwrap();
    }

    /// A `.ready` record for `instance`.
    pub(crate) fn ready(cache: &Path, instance: &str, capable: bool, written_at: u64) {
        let record = crate::staged_authority::ReadyRecord {
            schema: crate::staged_authority::AUTHORITY_DOC_SCHEMA,
            instance_id: instance.to_string(),
            binary_version: "3.0.0".to_string(),
            fold_capable: true,
            rotation_complete: true,
            fold_incomplete_total: 0,
            repair_rotation_complete: true,
            repair_stalest_age_secs: None,
            evict_worklist_capable: capable,
            written_at,
        };
        let dir = crate::staged_authority::ready_dir(cache);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(format!("{}.ready", instance)),
            serde_json::to_string_pretty(&record).unwrap(),
        )
        .unwrap();
    }

    /// A capable fleet at `now`: `host-a:1` active and capable, and a capable record for every
    /// instance that already has a journal (a test process's own consolidator may write one).
    pub(crate) fn capable(cache: &Path, now: u64) {
        journal(cache, "host-a:1", now);
        let dir = cache.join("metadata").join("_journals");
        for entry in std::fs::read_dir(&dir).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            if let Some(instance) = super::journal_instance(&name) {
                ready(cache, instance, true, now);
            }
        }
    }

    /// Unix seconds now.
    pub(crate) fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

// ---------------------------------------------------------------------------------------------
// The fleet-capability gate
// ---------------------------------------------------------------------------------------------

/// How recent a journal write, and a `.ready` record, must be for an instance to count as
/// active and its record as current.
///
/// A proxy that has not touched its journal for this long is not a candidate to delete anything
/// behind the author's back, and a record older than this no longer describes a running binary.
/// Not configurable, on the same reasoning as `staged_authority::READY_RECORD_RETIRE_AGE_SECS`:
/// this is a gate, and a knob would let an operator widen it until the answer changed.
pub(crate) const CAPABILITY_WINDOW_SECS: u64 = 3600;

/// Red-side lever: build the roster from the `.ready` directory alone.
#[cfg(test)]
const ROSTER_FROM_READY_DIR: bool = false;

/// Whether every active instance can execute a work-list chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Capability {
    /// Every active instance has a current `.ready` record with `evict_worklist_capable` set.
    Capable,
    /// This instance is active and is not capable: no current record, or a record without the flag.
    NotCapable { instance: String },
    /// The answer could not be established: an unreadable directory or record, or no active
    /// instance at all. Treated as "do not author".
    Unknown,
}

/// The instance id a journal file name belongs to, or `None` for a file that is not a journal.
///
/// Names are `{instance}.journal` and, when the primary was locked, `{instance}:{nanos}.journal`
/// where `{instance}` is `hostname:pid`. A name with two colon-separated parts after the host
/// and a numeric last part is the second form. A hostname containing a colon would be misread,
/// which can only produce a false `NotCapable`, the safe direction.
fn journal_instance(file_name: &str) -> Option<&str> {
    let stem = file_name.strip_suffix(".journal")?;
    if stem.is_empty() {
        return None;
    }
    if let Some((head, tail)) = stem.rsplit_once(':') {
        if head.contains(':') && !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) {
            return Some(head);
        }
    }
    Some(stem)
}
/// Whether `file_name` is a fresh journal, `{instance}:{nanos}.journal`, written when the
/// primary's lock was busy.
///
/// Uses [`journal_instance`]'s parse, so journal cleanup and the readiness roster agree on which
/// files are primaries: cleanup empties a fully applied primary and keeps it, so its instance
/// stays in the roster, and deletes a fully applied fresh journal, which is never appended to
/// again.
pub(crate) fn is_fresh_journal_name(file_name: &str) -> bool {
    match (
        journal_instance(file_name),
        file_name.strip_suffix(".journal"),
    ) {
        (Some(instance), Some(stem)) => instance != stem,
        _ => false,
    }
}
/// Instance ids with a journal file modified within [`CAPABILITY_WINDOW_SECS`] of `now`.
///
/// `Err` when the directory cannot be listed or an entry cannot be examined. A missing
/// directory is an empty roster. A file that vanishes between listing and `stat` is skipped: it
/// was a journal being rotated away, not an unreadable one. A modification time in the future
/// (clock skew between hosts) counts as age 0, which keeps the instance in the roster.
fn active_journal_instances(
    cache_dir: &Path,
    now: u64,
) -> std::io::Result<std::collections::BTreeSet<String>> {
    let dir = cache_dir.join("metadata").join("_journals");
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(e) => return Err(e),
    };
    let mut out = std::collections::BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(instance) = journal_instance(&name) else {
            continue;
        };
        let modified = match entry.metadata().and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let modified_secs = modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now.saturating_sub(modified_secs) <= CAPABILITY_WINDOW_SECS {
            out.insert(instance.to_string());
        }
    }
    Ok(out)
}

/// Whether the fleet can execute a work list, judged at `now` (unix seconds).
///
/// # The roster is the journal directory, not the ready directory
///
/// A pre-C binary publishes no `.ready` record at all (`staged_authority::ReadyRecord::fold_capable`),
/// so a roster built from the ready directory never contains it and the gate would say `Capable`
/// with an incapable proxy on the volume. Every proxy that serves traffic appends to a journal,
/// so the journal directory is the one listing a proxy cannot be absent from.
///
/// Every instance with a journal file modified within [`CAPABILITY_WINDOW_SECS`] must have a
/// `.ready` record written within the same window with `evict_worklist_capable` set. A record
/// from an instance with no recent journal is ignored. An unreadable journal directory or
/// `.ready` record, or an empty roster (no evidence that anyone is running), is `Unknown`.
pub(crate) fn fleet_is_worklist_capable(cache_dir: &Path, now: u64) -> Capability {
    let records = match crate::staged_authority::read_ready_records(cache_dir) {
        Ok(r) => r,
        Err(_) => return Capability::Unknown,
    };
    #[cfg(test)]
    let roster = if ROSTER_FROM_READY_DIR {
        records.iter().map(|r| r.instance_id.clone()).collect()
    } else {
        match active_journal_instances(cache_dir, now) {
            Ok(r) => r,
            Err(_) => return Capability::Unknown,
        }
    };
    #[cfg(not(test))]
    let roster = match active_journal_instances(cache_dir, now) {
        Ok(r) => r,
        Err(_) => return Capability::Unknown,
    };
    if roster.is_empty() {
        return Capability::Unknown;
    }
    for instance in roster {
        let capable = records.iter().any(|r| {
            r.instance_id == instance
                && r.schema == crate::staged_authority::AUTHORITY_DOC_SCHEMA
                && r.evict_worklist_capable
                && now.saturating_sub(r.written_at) <= CAPABILITY_WINDOW_SECS
        });
        if !capable {
            return Capability::NotCapable { instance };
        }
    }
    Capability::Capable
}

// ---------------------------------------------------------------------------------------------
// Task 50: per-process counters
// ---------------------------------------------------------------------------------------------

/// This process's work-list counters for one cache directory since start, published as the
/// `/metrics` `eviction_worklist` section and the `eviction_worklist.*` OTLP instruments.
///
/// Keyed by cache directory, as [`latch_generation`] is. A proxy has one; a test's temporary
/// directory has its own, so a test reads only what its own fixture moved.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WorklistCounters {
    /// Chunks this process authored, counted once their generation document is written. A chunk
    /// whose generation was never written is never executed, so it is not counted.
    pub chunks_authored: u64,
    /// Chunk leases this executor claimed, fresh or taken over from a lapsed lease.
    pub chunks_claimed: u64,
    /// Claims refused: a live peer lease, or a lease file that could not be read or written.
    pub chunks_claim_busy: u64,
    /// Chunks this executor closed: the done-marker publish returned `Ok` (its own marker, or a
    /// peer's that landed first).
    pub chunks_done: u64,
    /// Chunks read torn or absent and treated as absent (R3.8). A torn chunk does not decode, so
    /// its items cannot be counted.
    pub chunks_unusable: u64,
    /// Items of objects this executor ran the identity-checked delete for.
    pub items_executed: u64,
    /// Items of objects whose `.meta` no longer matched the recorded identity at execution.
    pub items_refused_identity_changed: u64,
    /// Items of a chunk left unexecuted because a newer generation was published.
    pub items_refused_stale_generation: u64,
    /// Claims that replaced a lapsed lease (read before the claim).
    pub lease_takeovers: u64,
    /// Author ticks that ran the legacy pass because the capability gate read `NotCapable`.
    pub author_fallback_not_capable: u64,
    /// Author ticks that ran the legacy pass because the capability gate read `Unknown`.
    pub author_fallback_unknown: u64,
    /// Listed bytes not yet executed as this process's last author call read them: the previous
    /// generation's unfinished bytes, or the bytes it has just listed.
    pub in_flight_bytes: u64,
}

static COUNTERS: std::sync::Mutex<std::collections::BTreeMap<PathBuf, WorklistCounters>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Apply `update` to `cache_dir`'s counters.
pub(crate) fn record(cache_dir: &Path, update: impl FnOnce(&mut WorklistCounters)) {
    let mut map = COUNTERS.lock().unwrap_or_else(|p| p.into_inner());
    update(map.entry(cache_dir.to_path_buf()).or_default());
}

/// `cache_dir`'s counters as of now; zeroes before the first event.
pub(crate) fn counters(cache_dir: &Path) -> WorklistCounters {
    COUNTERS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(cache_dir)
        .copied()
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------------------------
// The driver (task 49): what the consolidation cycle schedules on every proxy
// ---------------------------------------------------------------------------------------------

/// Chunks one scheduled executor pass may execute before it returns: no count bound. A pass
/// keeps claiming and executing chunks of the current generation while any is claimable, and
/// stops at [`EXECUTOR_PASS_BUDGET`]. One chunk per pass bounded the drain by the consolidation
/// cadence (two proxies at the default 5 s interval finished at most 0.4 chunks/s), not by what
/// the volume can delete.
pub(crate) const EXECUTOR_MAX_CHUNKS_PER_PASS: u64 = u64::MAX;

/// Wall-clock budget of one scheduled executor pass. Once it has elapsed the pass claims no
/// further chunk and returns, and the next consolidation cycle schedules a new pass. Checked
/// between chunks only, once the pass has claimed one, so every pass makes progress and no chunk
/// is cut short by it: each chunk keeps its own lease and its per-batch fences.
///
/// 60 s because a pass reads the current generation, settles held leases and lists the
/// generation's chunks once, at its start: returning once a minute bounds how long a chunk it
/// skipped as busy waits to be looked at again, while the gap until the next cycle schedules a
/// pass (at most one `consolidation_interval`, 5 s by default) costs under a tenth of the pass.
/// It is under a tenth of the shortest chunk lease (`config::EVICTION_WORKLIST_LEASE_MIN_SECS`), so no
/// timing argument about leases rests on it.
pub(crate) const EXECUTOR_PASS_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);

/// Objects per executor batch: the generation fence, the abort check, the accounting and the
/// accumulator flush run once per this many objects.
pub(crate) const EXECUTOR_BATCH_OBJECTS: usize = 64;

static ABORT_TICK_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
static EXECUTOR_PASS_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Red-side lever for task 49 amendment 1: run the abort only on the eviction-lock holder.
#[cfg(test)]
const ABORT_ONLY_UNDER_LOCK: bool = false;

/// A per-process claim on one of the driver's two passes. Clears its in-progress flag on drop,
/// however the pass ends, on the pattern of `orphan_reclaim::PassClaim`.
#[derive(Debug)]
pub(crate) struct DriverClaim(&'static AtomicBool);

impl Drop for DriverClaim {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

fn claim(flag: &'static AtomicBool) -> Option<DriverClaim> {
    flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .ok()
        .map(|_| DriverClaim(flag))
}

/// Claim this process's abort tick: `Some` unless one is already running.
pub(crate) fn claim_abort_tick() -> Option<DriverClaim> {
    claim(&ABORT_TICK_IN_PROGRESS)
}

/// Claim this process's executor pass: `Some` unless one is already running. Its own claim, so
/// a long chunk never holds up the abort tick.
pub(crate) fn claim_executor_pass() -> Option<DriverClaim> {
    claim(&EXECUTOR_PASS_IN_PROGRESS)
}

/// The executor budget for a scheduled pass, with the configured chunk lease, raised to
/// `config::eviction_worklist_lease_floor_secs` for the configured lock wait budget
/// so one batch fits in half of it.
pub(crate) fn executor_budget(
    shared_storage: &crate::config::SharedStorageConfig,
) -> crate::cache::ExecutorBudget {
    let floor = crate::config::eviction_worklist_lease_floor_secs(
        shared_storage.lock_acquire_wait_budget.as_secs(),
    );
    crate::cache::ExecutorBudget {
        max_chunks: EXECUTOR_MAX_CHUNKS_PER_PASS,
        batch_objects: EXECUTOR_BATCH_OBJECTS,
        lease_secs: shared_storage.eviction_worklist_lease.as_secs().max(floor),
    }
}

/// Task 49 amendment 1: the abort tick every proxy runs on every consolidation cycle, holding
/// no lock and regardless of the size trigger.
///
/// [`abort_if_unsafe`] takes no lock, and this caller must not add one. An older proxy's legacy
/// pass can hold `global_eviction.lock` indefinitely, and if only the lock holder re-evaluated the
/// gate nothing would abort a live generation while that proxy is in the fleet: executors would
/// keep executing it, or pause and resume it after the legacy pass had already met target. Here
/// every capable proxy writes the abort marker on `NotCapable`, and every executor reads it at the
/// start of its pass and at each batch boundary, so it stops rather than pauses.
pub(crate) async fn driver_abort_tick(
    manager: &crate::cache::CacheManager,
    now: u64,
) -> Result<AbortTick> {
    #[cfg(test)]
    if ABORT_ONLY_UNDER_LOCK && manager.eviction_lock_held().is_none() {
        return Ok(AbortTick::NoLiveGeneration);
    }
    abort_if_unsafe(manager.get_cache_dir(), now).await
}

/// Red-side lever: the scheduled janitor does nothing, as before it was wired.
#[cfg(test)]
const JANITOR_UNSCHEDULED: bool = false;

/// The generation janitor's scheduled run: [`reap_old_generations`] on every consolidation cycle,
/// after the abort tick and under its claim. Holds no lock, as the janitor's documentation
/// requires. A failure is logged and the next cycle retries.
pub(crate) async fn driver_janitor_tick(cache_dir: &Path) {
    #[cfg(test)]
    if JANITOR_UNSCHEDULED {
        return;
    }
    match reap_old_generations(cache_dir).await {
        Ok(r) if r.failed > 0 => warn!(
            "[EVICTION_WORKLIST] janitor: {} generation directories could not be removed \
             (current {}, reaped {:?}); the next cycle retries",
            r.failed, r.current, r.reaped
        ),
        Ok(r) if !r.reaped.is_empty() => tracing::debug!(
            "[EVICTION_WORKLIST] janitor: reaped generations {:?} (current {})",
            r.reaped,
            r.current
        ),
        Ok(_) => {}
        Err(e) => warn!("[EVICTION_WORKLIST] janitor skipped: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::NewCacheMetadata;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};
    use tempfile::TempDir;

    /// An identity taken from a real metadata document, with a sub-second `created_at` so a
    /// round trip that drops the fraction is visible.
    fn metadata() -> NewCacheMetadata {
        let mut m = NewCacheMetadata {
            cache_key: "bkt/obj".to_string(),
            ..Default::default()
        };
        m.object_metadata.etag = "\"abc123\"".to_string();
        m.object_metadata.last_modified = "Tue, 06 Oct 2026 15:00:00 GMT".to_string();
        m.object_metadata.is_write_cached = true;
        m.created_at = UNIX_EPOCH + Duration::new(1_790_000_000, 123_456_789);
        m
    }

    fn item(n: u64) -> WorkItem {
        WorkItem {
            cache_key: format!("bkt/obj-{}", n),
            start: n * 1024,
            end: n * 1024 + 1023,
            identity: ExpectedObjectIdentity::of(&metadata()),
            staged: n.is_multiple_of(2),
            is_write_cached: true,
            compressed_size: 700 + n,
            size: 1024,
        }
    }

    fn chunk(generation: u64, author: &str, count: u64) -> Chunk {
        Chunk::new(
            generation,
            author,
            1_790_000_100,
            (0..count).map(item).collect(),
        )
    }

    #[test]
    fn an_item_round_trips_and_its_identity_compares_equal_under_of() {
        let m = metadata();
        let original = item(3);
        let line = serde_json::to_vec(&original).unwrap();
        let back: WorkItem = serde_json::from_slice(&line).unwrap();
        assert_eq!(back, original);
        assert_eq!(
            back.identity,
            ExpectedObjectIdentity::of(&m),
            "the recheck compares SystemTime for equality, so the nanoseconds must survive"
        );
        assert_eq!(
            back.identity.created_at,
            UNIX_EPOCH + Duration::new(1_790_000_000, 123_456_789)
        );
    }

    #[test]
    fn created_at_round_trips_at_the_edges_of_the_representable_range() {
        for t in [
            UNIX_EPOCH,
            UNIX_EPOCH + Duration::new(0, 1),
            UNIX_EPOCH + Duration::new(1, 999_999_999),
            UNIX_EPOCH - Duration::new(5, 0),
            UNIX_EPOCH - Duration::new(5, 250_000_000),
            UNIX_EPOCH - Duration::new(0, 1),
        ] {
            let mut id = ExpectedObjectIdentity::of(&metadata());
            id.created_at = t;
            let back: ExpectedObjectIdentity =
                serde_json::from_slice(&serde_json::to_vec(&id).unwrap()).unwrap();
            assert_eq!(back, id, "{:?}", t);
        }
    }

    #[test]
    fn a_whole_chunk_round_trips() {
        let c = chunk(7, "proxy-a", 5);
        assert_eq!(c.header.item_count, 5);
        assert_eq!(c.header.bytes_total, 5 * 1024);
        assert_eq!(decode_chunk(&c.encode(), 7), ChunkRead::Chunk(c));
    }

    #[test]
    fn an_empty_chunk_is_valid() {
        let c = chunk(1, "proxy-a", 0);
        assert_eq!(decode_chunk(&c.encode(), 1), ChunkRead::Chunk(c));
    }

    #[test]
    fn every_truncation_reads_as_torn() {
        let bytes = chunk(7, "proxy-a", 4).encode();
        for len in 0..bytes.len() {
            assert!(
                matches!(decode_chunk(&bytes[..len], 7), ChunkRead::Torn(_)),
                "a {}-byte prefix of a {}-byte chunk was accepted",
                len,
                bytes.len()
            );
        }
    }

    #[test]
    fn every_single_bit_flip_reads_as_torn() {
        let bytes = chunk(7, "proxy-a", 3).encode();
        for pos in 0..bytes.len() {
            for bit in 0..8 {
                let mut flipped = bytes.clone();
                flipped[pos] ^= 1 << bit;
                assert!(
                    matches!(decode_chunk(&flipped, 7), ChunkRead::Torn(_)),
                    "flipping bit {} of byte {} was accepted",
                    bit,
                    pos
                );
            }
        }
    }

    /// A chunk whose header lies about `item_count` but carries a checksum that matches, so the
    /// count check is the only thing that can reject it.
    #[test]
    fn a_count_mismatch_with_a_valid_checksum_reads_as_torn() {
        let c = chunk(7, "proxy-a", 3);
        let section = item_section(&c.items);
        let mut header = c.header.clone();
        header.item_count += 1;
        header.checksum = checksum_of(&header, &section);
        assert_eq!(
            decode_chunk(&encode_parts(&header, &section), 7),
            ChunkRead::Torn(TornReason::CountMismatch)
        );
    }

    #[test]
    fn a_byte_total_mismatch_with_a_valid_checksum_reads_as_torn() {
        let c = chunk(7, "proxy-a", 3);
        let section = item_section(&c.items);
        let mut header = c.header.clone();
        header.bytes_total += 1;
        header.checksum = checksum_of(&header, &section);
        assert_eq!(
            decode_chunk(&encode_parts(&header, &section), 7),
            ChunkRead::Torn(TornReason::BytesMismatch)
        );
    }

    #[test]
    fn a_chunk_read_under_the_wrong_generation_or_schema_is_torn() {
        let c = chunk(7, "proxy-a", 2);
        assert_eq!(
            decode_chunk(&c.encode(), 8),
            ChunkRead::Torn(TornReason::GenerationMismatch)
        );
        let section = item_section(&c.items);
        let mut header = c.header.clone();
        header.schema = SCHEMA + 1;
        header.checksum = checksum_of(&header, &section);
        assert_eq!(
            decode_chunk(&encode_parts(&header, &section), 7),
            ChunkRead::Torn(TornReason::UnknownSchema)
        );
    }

    #[tokio::test]
    async fn a_second_publish_to_an_existing_chunk_path_leaves_the_first_byte_for_byte() {
        let temp = TempDir::new().unwrap();
        let shard = Shard::new("bkt", "0a");
        let path = chunk_path(temp.path(), 7, &shard).unwrap();
        let first = chunk(7, "proxy-a", 3);
        let second = chunk(7, "proxy-b", 9);

        assert_eq!(
            publish_chunk(&path, &first).await.unwrap(),
            PublishOutcome::Published
        );
        let before = std::fs::read(&path).unwrap();
        assert_eq!(before, first.encode());

        assert_eq!(
            publish_chunk(&path, &second).await.unwrap(),
            PublishOutcome::AlreadyExists { identical: false }
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "the first chunk was replaced"
        );
        // A retry of the same document is recognisable as such.
        assert_eq!(
            publish_chunk(&path, &first).await.unwrap(),
            PublishOutcome::AlreadyExists { identical: true }
        );
        assert_eq!(read_chunk(&path, 7).await, ChunkRead::Chunk(first));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn of_many_concurrent_authors_exactly_one_wins_and_no_temp_file_is_left() {
        let temp = TempDir::new().unwrap();
        let shard = Shard::new("bkt", "0b");
        let path = Arc::new(chunk_path(temp.path(), 2, &shard).unwrap());
        let mut tasks = Vec::new();
        for n in 0..16u64 {
            let path = Arc::clone(&path);
            tasks.push(tokio::spawn(async move {
                let c = chunk(2, &format!("proxy-{}", n), n + 1);
                (n, publish_chunk(&path, &c).await.unwrap())
            }));
        }
        let mut winners = Vec::new();
        for task in tasks {
            let (n, outcome) = task.await.unwrap();
            if outcome == PublishOutcome::Published {
                winners.push(n);
            }
        }
        assert_eq!(winners.len(), 1, "winners: {:?}", winners);
        let ChunkRead::Chunk(stored) = read_chunk(&path, 2).await else {
            panic!("the published chunk does not read back");
        };
        assert_eq!(stored.header.author, format!("proxy-{}", winners[0]));
        let names: Vec<String> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(
            names,
            vec!["0b.chunk".to_string()],
            "left behind: {:?}",
            names
        );
    }

    // -----------------------------------------------------------------------------------------
    // The publish guard against NFS retransmits
    // -----------------------------------------------------------------------------------------

    const EREMOTEIO: i32 = 121;

    fn seam<F>(f: F) -> Arc<LinkFn>
    where
        F: Fn(&Path, &Path) -> std::io::Result<()> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    fn no_strays(path: &Path) {
        let names: Vec<String> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(names.len(), 1, "left behind: {:?}", names);
    }

    /// The retransmit shape: the first copy of the `link` RPC executes, the retransmit answers
    /// `EEXIST` to the same process.
    #[tokio::test]
    async fn a_link_that_executed_and_then_reports_eexist_is_published() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "aa")).unwrap();
        let c = chunk(1, "proxy-a", 3);
        let link = seam(|tmp, path| {
            std::fs::hard_link(tmp, path)?;
            Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
        });
        let outcome = publish_once_with(&path, &c.encode(), link).await.unwrap();
        assert_eq!(outcome, PublishOutcome::Published);
        assert_eq!(read_chunk(&path, 1).await, ChunkRead::Chunk(c));
        no_strays(&path);
    }

    /// The link executed and its reply was lost: the caller sees `EREMOTEIO`.
    #[tokio::test]
    async fn a_link_that_executed_and_then_reports_eremoteio_is_published() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "ab")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let link = seam(|tmp, path| {
            std::fs::hard_link(tmp, path)?;
            Err(std::io::Error::from_raw_os_error(EREMOTEIO))
        });
        let outcome = publish_once_with(&path, &c.encode(), link).await.unwrap();
        assert_eq!(outcome, PublishOutcome::Published);
        no_strays(&path);
    }

    /// `EREMOTEIO` that never reached the server: the final is absent, so the link is retried.
    #[tokio::test]
    async fn a_link_that_failed_without_executing_is_retried_and_published() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "ac")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let calls = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&calls);
        let link = seam(move |tmp, path| {
            if seen.fetch_add(1, Ordering::SeqCst) < 2 {
                return Err(std::io::Error::from_raw_os_error(EREMOTEIO));
            }
            std::fs::hard_link(tmp, path)
        });
        let outcome = publish_once_with(&path, &c.encode(), link).await.unwrap();
        assert_eq!(outcome, PublishOutcome::Published);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        no_strays(&path);
    }

    #[tokio::test]
    async fn a_link_that_never_succeeds_gives_up_after_the_attempt_bound_with_no_stray_temp() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "ad")).unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&calls);
        let link = seam(move |_, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::from_raw_os_error(EREMOTEIO))
        });
        let outcome = publish_once_with(&path, &chunk(1, "a", 1).encode(), link).await;
        assert!(outcome.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), u64::from(LINK_ATTEMPTS));
        assert!(!path.exists());
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            0
        );
    }

    /// A peer won: `EEXIST` with a different inode and different bytes is a loss, not a publish.
    #[tokio::test]
    async fn eexist_because_a_peer_won_is_already_exists_not_identical() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "ae")).unwrap();
        let peer = chunk(1, "proxy-peer", 5);
        let peer_bytes = peer.encode();
        let link = seam(move |_, path| {
            std::fs::write(path, &peer_bytes)?;
            Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
        });
        let mine = chunk(1, "proxy-a", 2);
        let outcome = publish_once_with(&path, &mine.encode(), link)
            .await
            .unwrap();
        assert_eq!(outcome, PublishOutcome::AlreadyExists { identical: false });
        assert_eq!(read_chunk(&path, 1).await, ChunkRead::Chunk(peer));
        no_strays(&path);
    }

    /// A peer that published the same bytes under its own inode is `identical`, not `Published`.
    #[tokio::test]
    async fn eexist_from_a_peer_with_the_same_bytes_is_identical_but_not_published() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "af")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let bytes = c.encode();
        let peer_bytes = bytes.clone();
        let link = seam(move |_, path| {
            std::fs::write(path, &peer_bytes)?;
            Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
        });
        let outcome = publish_once_with(&path, &bytes, link).await.unwrap();
        assert_eq!(outcome, PublishOutcome::AlreadyExists { identical: true });
    }

    /// A replayed `unlink` of the temp returns `ENOENT`; the publish still succeeded. Here the
    /// temp is already gone when the publisher removes it. This pins a non-regression: it also
    /// passes on the code before the change, so it detects no defect by itself.
    #[tokio::test]
    async fn a_temp_that_is_already_gone_after_the_link_is_still_published() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "ag")).unwrap();
        let c = chunk(1, "proxy-a", 1);
        let link = seam(|tmp, path| {
            std::fs::hard_link(tmp, path)?;
            std::fs::remove_file(tmp)
        });
        let outcome = publish_once_with(&path, &c.encode(), link).await.unwrap();
        assert_eq!(outcome, PublishOutcome::Published);
        no_strays(&path);
    }

    // -----------------------------------------------------------------------------------------
    // Stat and read errors are never a verdict
    // -----------------------------------------------------------------------------------------

    const EIO: i32 = 5;

    /// A stat seam that fails with `EIO` on calls numbered `fail_from..fail_to` (1-based, counted
    /// across all paths after staging) and is the real stat otherwise.
    fn flaky_stat(fail_from: u64, fail_to: u64) -> (Arc<StatFn>, Arc<AtomicU64>) {
        let calls = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&calls);
        let f: Arc<StatFn> = Arc::new(move |p| {
            let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
            if n >= fail_from && n < fail_to {
                return Err(std::io::Error::from_raw_os_error(EIO));
            }
            real_stat(p)
        });
        (f, calls)
    }

    fn io_with(link: Arc<LinkFn>, stat: Arc<StatFn>, read: Arc<ReadFn>) -> PublishIo {
        PublishIo { link, stat, read }
    }

    fn real_read_seam() -> Arc<ReadFn> {
        Arc::new(real_read)
    }

    fn executing_eremoteio() -> Arc<LinkFn> {
        seam(|tmp, path| {
            std::fs::hard_link(tmp, path)?;
            Err(std::io::Error::from_raw_os_error(EREMOTEIO))
        })
    }

    /// (a) The link executed, the reply was lost, and the first stats fail: still `Published`.
    #[tokio::test]
    async fn a_stat_error_after_an_executed_link_is_retried_and_published() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "ba")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let (stat, _) = flaky_stat(1, 3);
        let io = io_with(executing_eremoteio(), stat, real_read_seam());
        let outcome = publish_once_io(&path, &c.encode(), io).await.unwrap();
        assert_eq!(outcome, PublishOutcome::Published);
        no_strays(&path);
    }

    /// (b) The link executed and every stat fails: `Err`, not a claim about a peer.
    #[tokio::test]
    async fn a_stat_that_always_fails_after_an_executed_link_is_err_not_already_exists() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "bb")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let (stat, _) = flaky_stat(1, u64::MAX);
        let io = io_with(executing_eremoteio(), stat, real_read_seam());
        let outcome = publish_once_io(&path, &c.encode(), io).await;
        assert!(outcome.is_err(), "got {:?}", outcome);
        // The final is this call's file and the re-call reads it as identical.
        assert_eq!(read_chunk(&path, 1).await, ChunkRead::Chunk(c.clone()));
        let again = publish_once_with(&path, &c.encode(), seam(|t, p| std::fs::hard_link(t, p)))
            .await
            .unwrap();
        assert_eq!(again, PublishOutcome::AlreadyExists { identical: true });
    }

    /// (c) The link did not execute; one rule-3 stat fails with `EIO`, then `NotFound`: relink.
    #[tokio::test]
    async fn a_stat_error_then_not_found_after_a_failed_link_relinks_and_publishes() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "bc")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let calls = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&calls);
        let link = seam(move |tmp, path| {
            if seen.fetch_add(1, Ordering::SeqCst) < 1 {
                return Err(std::io::Error::from_raw_os_error(EREMOTEIO));
            }
            std::fs::hard_link(tmp, path)
        });
        let (stat, _) = flaky_stat(1, 2);
        let io = io_with(link, stat, real_read_seam());
        let outcome = publish_once_io(&path, &c.encode(), io).await.unwrap();
        assert_eq!(outcome, PublishOutcome::Published);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        no_strays(&path);
    }

    /// (d) A peer won; reading its file fails with `EIO` every time: `Err`, not `identical: false`.
    #[tokio::test]
    async fn a_read_that_always_fails_after_eexist_is_err_not_a_different_document() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "bd")).unwrap();
        let peer_bytes = chunk(1, "proxy-peer", 5).encode();
        let link = seam(move |_, path| {
            std::fs::write(path, &peer_bytes)?;
            Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
        });
        let read: Arc<ReadFn> = Arc::new(|_| Err(std::io::Error::from_raw_os_error(EIO)));
        let io = io_with(link, Arc::new(real_stat), read);
        let outcome = publish_once_io(&path, &chunk(1, "proxy-a", 2).encode(), io).await;
        assert!(outcome.is_err(), "got {:?}", outcome);
        no_strays(&path);
    }

    /// `same_inode` alone: the link executed and reports `EEXIST` (rule 3 is skipped), and every
    /// stat fails. The read would succeed and match, so only `same_inode`'s own handling of the
    /// Unknown stat separates `Err` from `AlreadyExists { identical: true }`.
    #[tokio::test]
    async fn a_stat_that_always_fails_inside_same_inode_is_err() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "bf")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let link = seam(|tmp, path| {
            std::fs::hard_link(tmp, path)?;
            Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
        });
        let (stat, _) = flaky_stat(1, u64::MAX);
        let io = io_with(link, stat, real_read_seam());
        let outcome = publish_once_io(&path, &c.encode(), io).await;
        assert!(outcome.is_err(), "got {:?}", outcome);
        // The seam linked the final: it is this process's file.
        assert_eq!(read_chunk(&path, 1).await, ChunkRead::Chunk(c));
        no_strays(&path);
    }

    /// The final vanishes after every `EEXIST`: the loop ends in `Err` after `LINK_ATTEMPTS`.
    #[tokio::test]
    async fn a_final_that_vanishes_every_time_is_err_after_the_attempt_bound() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "bg")).unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&calls);
        let peer_bytes = chunk(1, "proxy-peer", 5).encode();
        let link = seam(move |_, path| {
            seen.fetch_add(1, Ordering::SeqCst);
            std::fs::write(path, &peer_bytes)?;
            Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
        });
        let read: Arc<ReadFn> = Arc::new(|p| {
            std::fs::remove_file(p)?;
            Err(std::io::Error::from(std::io::ErrorKind::NotFound))
        });
        let io = io_with(link, Arc::new(real_stat), read);
        let outcome = publish_once_io(&path, &chunk(1, "proxy-a", 2).encode(), io).await;
        let err = outcome.expect_err("four vanishes must be Err");
        assert!(err.to_string().contains("kept vanishing"), "got {}", err);
        assert_eq!(calls.load(Ordering::SeqCst), u64::from(LINK_ATTEMPTS));
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            0
        );
    }

    /// (e) `EEXIST`, then the final is gone when read (a peer removed it): the link is retried
    /// and this call publishes.
    #[tokio::test]
    async fn a_final_that_vanishes_after_eexist_is_linked_again() {
        let temp = TempDir::new().unwrap();
        let path = chunk_path(temp.path(), 1, &Shard::new("bkt", "be")).unwrap();
        let c = chunk(1, "proxy-a", 2);
        let calls = Arc::new(AtomicU64::new(0));
        let seen = Arc::clone(&calls);
        let peer_bytes = chunk(1, "proxy-peer", 5).encode();
        let link = seam(move |tmp, path| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                std::fs::write(path, &peer_bytes)?;
                return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
            }
            std::fs::hard_link(tmp, path)
        });
        let read: Arc<ReadFn> = Arc::new(|p| {
            // The peer removes its file between the inode check and the read.
            std::fs::remove_file(p)?;
            Err(std::io::Error::from(std::io::ErrorKind::NotFound))
        });
        let io = io_with(link, Arc::new(real_stat), read);
        let outcome = publish_once_io(&path, &c.encode(), io).await.unwrap();
        assert_eq!(outcome, PublishOutcome::Published);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(read_chunk(&path, 1).await, ChunkRead::Chunk(c));
        no_strays(&path);
    }

    #[tokio::test]
    async fn reading_a_missing_chunk_is_absent_and_a_damaged_one_is_torn() {
        let temp = TempDir::new().unwrap();
        let shard = Shard::new("bkt", "0c");
        let path = chunk_path(temp.path(), 3, &shard).unwrap();
        assert_eq!(read_chunk(&path, 3).await, ChunkRead::Absent);
        let c = chunk(3, "proxy-a", 4);
        publish_chunk(&path, &c).await.unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 5]).unwrap();
        assert!(matches!(read_chunk(&path, 3).await, ChunkRead::Torn(_)));
    }

    #[tokio::test]
    async fn the_done_marker_is_write_once_and_names_its_chunk() {
        let temp = TempDir::new().unwrap();
        let shard = Shard::new("bkt", "0d");
        let path = done_path(temp.path(), 4, &shard).unwrap();
        assert_eq!(read_done(&path).await, DoneRead::Absent);
        let c = chunk(4, "proxy-a", 2);
        let marker = |executor: &str| DoneMarker {
            schema: SCHEMA,
            generation: 4,
            executor: executor.to_string(),
            completed_at: 1_790_000_200,
            chunk_checksum: c.header.checksum.clone(),
        };
        assert_eq!(
            publish_done(&path, &marker("proxy-b")).await.unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(
            publish_done(&path, &marker("proxy-c")).await.unwrap(),
            PublishOutcome::AlreadyExists { identical: false }
        );
        assert_eq!(read_done(&path).await, DoneRead::Done(marker("proxy-b")));
        std::fs::write(&path, b"{\"schema\":1,").unwrap();
        assert_eq!(read_done(&path).await, DoneRead::Torn);
    }

    #[test]
    fn paths_follow_the_layout_and_refuse_unsafe_components() {
        let root = Path::new("/c");
        let shard = Shard::new("bkt", "0a");
        assert_eq!(
            chunk_path(root, 9, &shard).unwrap(),
            Path::new("/c/metadata/_shard_state/eviction_worklist/9/bkt/0a.chunk")
        );
        assert_eq!(
            done_path(root, 9, &shard).unwrap(),
            Path::new("/c/metadata/_shard_state/eviction_worklist/9/bkt/0a.done")
        );
        assert_eq!(
            progress_path(root, 9, &shard, "proxy-a").unwrap(),
            Path::new("/c/metadata/_shard_state/eviction_worklist/9/bkt/0a.progress.proxy-a")
        );
        assert!(chunk_path(root, 9, &Shard::new("../x", "0a")).is_none());
        assert!(chunk_path(root, 9, &Shard::new("bkt", "a/b")).is_none());
        assert!(chunk_path(root, 9, &Shard::new("", "0a")).is_none());
        assert!(progress_path(root, 9, &shard, "a/b").is_none());
    }

    // -----------------------------------------------------------------------------------------
    // Task 44: the generation fence
    // -----------------------------------------------------------------------------------------

    #[test]
    fn the_latch_only_moves_forward_and_directories_are_independent() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        assert_eq!(generation_latched_for(a.path()), 0);
        assert_eq!(latch_generation(a.path(), 5), 5);
        assert_eq!(
            latch_generation(a.path(), 3),
            5,
            "a lower observation lowered the latch"
        );
        assert_eq!(latch_generation(a.path(), 5), 5);
        assert_eq!(latch_generation(a.path(), 0), 5, "zero lowered the latch");
        assert_eq!(latch_generation(a.path(), 9), 9);
        assert_eq!(generation_latched_for(a.path()), 9);
        assert_eq!(
            generation_latched_for(b.path()),
            0,
            "the latch leaked across directories"
        );
    }

    #[tokio::test]
    async fn a_torn_absent_or_lower_document_cannot_resurrect_an_older_generation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        assert_eq!(current_generation(dir).await, 0);
        assert_eq!(
            advance_generation(&crate::cache::EvictionLockHeld::for_tests(), dir, 4)
                .await
                .unwrap(),
            Advance::Advanced
        );
        assert_eq!(current_generation(dir).await, 4);

        // A stale reader: a peer's lagging view shows generation 2.
        std::fs::write(generation_path(dir), br#"{"schema":1,"generation":2}"#).unwrap();
        assert_eq!(read_generation(dir).await, GenerationRead::Generation(2));
        assert_eq!(
            current_generation(dir).await,
            4,
            "a lower document lowered the fence"
        );
        assert!(
            !generation_admits(dir, 2).await,
            "a stale chunk was admitted"
        );
        assert!(generation_admits(dir, 4).await);

        // Torn: truncated, wrong schema, garbage.
        for torn in [
            &b"{\"schema\":1,\"gener"[..],
            br#"{"schema":9,"generation":99}"#,
            b"\xff\x00",
        ] {
            std::fs::write(generation_path(dir), torn).unwrap();
            assert_eq!(read_generation(dir).await, GenerationRead::Torn);
            assert_eq!(current_generation(dir).await, 4);
        }

        // Absent.
        std::fs::remove_file(generation_path(dir)).unwrap();
        assert_eq!(read_generation(dir).await, GenerationRead::Absent);
        assert_eq!(current_generation(dir).await, 4);

        // A higher document moves it, and the chunk one ahead of it is not yet admitted.
        std::fs::write(generation_path(dir), br#"{"schema":1,"generation":7}"#).unwrap();
        assert_eq!(current_generation(dir).await, 7);
        assert!(generation_admits(dir, 7).await);
        assert!(!generation_admits(dir, 6).await);
        assert!(
            !generation_admits(dir, 8).await,
            "a chunk ahead of the document was admitted"
        );
        assert!(!generation_admits(dir, 0).await);
    }

    #[tokio::test]
    async fn advance_writes_only_a_strictly_newer_generation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        assert_eq!(
            advance_generation(&crate::cache::EvictionLockHeld::for_tests(), dir, 1)
                .await
                .unwrap(),
            Advance::Advanced
        );
        let first = std::fs::read(generation_path(dir)).unwrap();
        assert_eq!(
            advance_generation(&crate::cache::EvictionLockHeld::for_tests(), dir, 1)
                .await
                .unwrap(),
            Advance::NotAhead { current: 1 }
        );
        assert_eq!(std::fs::read(generation_path(dir)).unwrap(), first);
        assert_eq!(
            advance_generation(&crate::cache::EvictionLockHeld::for_tests(), dir, 2)
                .await
                .unwrap(),
            Advance::Advanced
        );
        assert_eq!(
            serde_json::from_slice::<GenerationDoc>(&std::fs::read(generation_path(dir)).unwrap())
                .unwrap(),
            GenerationDoc {
                schema: GENERATION_SCHEMA,
                generation: 2
            }
        );

        // A peer author is ahead of this process's latch: the document wins over the empty latch.
        let fresh = TempDir::new().unwrap();
        std::fs::create_dir_all(generation_path(fresh.path()).parent().unwrap()).unwrap();
        std::fs::write(
            generation_path(fresh.path()),
            br#"{"schema":1,"generation":10}"#,
        )
        .unwrap();
        assert_eq!(generation_latched_for(fresh.path()), 0);
        assert_eq!(
            advance_generation(
                &crate::cache::EvictionLockHeld::for_tests(),
                fresh.path(),
                5
            )
            .await
            .unwrap(),
            Advance::NotAhead { current: 10 }
        );
        assert_eq!(generation_latched_for(fresh.path()), 10);
    }

    // -----------------------------------------------------------------------------------------
    // Task 44: the fleet-capability gate
    // -----------------------------------------------------------------------------------------

    const NOW: u64 = 1_790_000_000;

    fn journal(cache: &Path, file_name: &str, modified: u64) {
        let dir = cache.join("metadata").join("_journals");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(file_name);
        std::fs::write(&path, b"").unwrap();
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_unix_time(modified as i64, 0),
        )
        .unwrap();
    }

    fn ready(cache: &Path, instance: &str, capable: bool, written_at: u64) {
        let record = crate::staged_authority::ReadyRecord {
            schema: crate::staged_authority::AUTHORITY_DOC_SCHEMA,
            instance_id: instance.to_string(),
            binary_version: "3.0.0".to_string(),
            fold_capable: true,
            rotation_complete: true,
            fold_incomplete_total: 0,
            repair_rotation_complete: true,
            repair_stalest_age_secs: None,
            evict_worklist_capable: capable,
            written_at,
        };
        let dir = crate::staged_authority::ready_dir(cache);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(format!("{}.ready", instance)),
            serde_json::to_string_pretty(&record).unwrap(),
        )
        .unwrap();
    }

    /// Three active, capable instances: the baseline every other case perturbs.
    fn capable_fleet(cache: &Path) {
        for id in ["host-a:1", "host-b:2", "host-c:3"] {
            journal(cache, &format!("{}.journal", id), NOW - 5);
            ready(cache, id, true, NOW - 5);
        }
    }

    #[test]
    fn three_fresh_capable_records_are_capable() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::Capable
        );
    }

    #[test]
    fn one_record_without_the_flag_makes_the_fleet_not_capable() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        ready(tmp.path(), "host-b:2", false, NOW - 5);
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::NotCapable {
                instance: "host-b:2".to_string()
            }
        );
    }

    #[test]
    fn a_record_from_an_older_binary_without_the_field_is_not_capable() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        // Byte-for-byte what the pre-task-44 code wrote for host-c:3.
        let old = serde_json::json!({
            "schema": 1, "instance_id": "host-c:3", "binary_version": "2.9.13",
            "fold_capable": true, "rotation_complete": true, "fold_incomplete_total": 0,
            "repair_rotation_complete": true, "repair_stalest_age_secs": 5, "written_at": NOW - 5,
        });
        std::fs::write(
            crate::staged_authority::ready_dir(tmp.path()).join("host-c:3.ready"),
            old.to_string(),
        )
        .unwrap();
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::NotCapable {
                instance: "host-c:3".to_string()
            }
        );
    }

    #[test]
    fn a_journal_bearing_instance_with_no_record_is_not_capable() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        // A pre-C proxy publishes no .ready record; its journal is all that shows it is there.
        journal(tmp.path(), "host-old:9.journal", NOW - 30);
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::NotCapable {
                instance: "host-old:9".to_string()
            }
        );
    }

    #[test]
    fn a_rotated_journal_name_counts_for_its_instance() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        journal(
            tmp.path(),
            "host-old:9:1790000000123456789.journal",
            NOW - 30,
        );
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::NotCapable {
                instance: "host-old:9".to_string()
            }
        );
        // A rotated journal of a capable instance maps to that instance's record.
        let ok = TempDir::new().unwrap();
        journal(ok.path(), "host-a:1:1790000000123456789.journal", NOW - 30);
        ready(ok.path(), "host-a:1", true, NOW - 5);
        assert_eq!(
            fleet_is_worklist_capable(ok.path(), NOW),
            Capability::Capable
        );
    }

    #[test]
    fn a_stale_record_from_an_instance_with_no_recent_journal_is_ignored() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        // A departed, incapable proxy: old record, old journal.
        ready(
            tmp.path(),
            "host-gone:4",
            false,
            NOW - 10 * CAPABILITY_WINDOW_SECS,
        );
        journal(
            tmp.path(),
            "host-gone:4.journal",
            NOW - 10 * CAPABILITY_WINDOW_SECS,
        );
        // And a record with no journal at all.
        ready(tmp.path(), "host-nojournal:5", false, NOW - 5);
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::Capable
        );
    }

    #[test]
    fn an_active_instance_whose_record_is_older_than_the_window_is_not_capable() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        ready(
            tmp.path(),
            "host-a:1",
            true,
            NOW - CAPABILITY_WINDOW_SECS - 1,
        );
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::NotCapable {
                instance: "host-a:1".to_string()
            }
        );
    }

    #[test]
    fn an_unreadable_record_or_directory_is_unknown() {
        let tmp = TempDir::new().unwrap();
        capable_fleet(tmp.path());
        std::fs::write(
            crate::staged_authority::ready_dir(tmp.path()).join("host-b:2.ready"),
            b"{\"schema\":1,\"inst",
        )
        .unwrap();
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::Unknown
        );

        // The ready "directory" is a file: the listing fails.
        let bad_ready = TempDir::new().unwrap();
        journal(bad_ready.path(), "host-a:1.journal", NOW - 5);
        let ready_path = crate::staged_authority::ready_dir(bad_ready.path());
        std::fs::create_dir_all(ready_path.parent().unwrap()).unwrap();
        std::fs::write(&ready_path, b"x").unwrap();
        assert_eq!(
            fleet_is_worklist_capable(bad_ready.path(), NOW),
            Capability::Unknown
        );

        // The journal "directory" is a file: the roster cannot be built.
        let bad_journals = TempDir::new().unwrap();
        ready(bad_journals.path(), "host-a:1", true, NOW - 5);
        let journals = bad_journals.path().join("metadata").join("_journals");
        std::fs::create_dir_all(journals.parent().unwrap()).unwrap();
        std::fs::write(&journals, b"x").unwrap();
        assert_eq!(
            fleet_is_worklist_capable(bad_journals.path(), NOW),
            Capability::Unknown
        );
    }

    #[test]
    fn no_active_instance_is_unknown_not_capable() {
        let empty = TempDir::new().unwrap();
        assert_eq!(
            fleet_is_worklist_capable(empty.path(), NOW),
            Capability::Unknown
        );
        // Journals exist but every one is outside the window, and lock files are not journals.
        let tmp = TempDir::new().unwrap();
        journal(
            tmp.path(),
            "host-a:1.journal",
            NOW - CAPABILITY_WINDOW_SECS - 1,
        );
        journal(tmp.path(), "host-b:2.journal.lock", NOW - 5);
        ready(tmp.path(), "host-a:1", true, NOW - 5);
        assert_eq!(
            fleet_is_worklist_capable(tmp.path(), NOW),
            Capability::Unknown
        );
    }

    #[test]
    fn journal_names_map_to_instances() {
        assert_eq!(journal_instance("h:1.journal"), Some("h:1"));
        assert_eq!(journal_instance("h:1:1790000000123.journal"), Some("h:1"));
        assert_eq!(journal_instance("h:1.journal.lock"), None);
        assert_eq!(journal_instance(".journal"), None);
        assert_eq!(journal_instance("notes.txt"), None);
    }

    fn write_generation_doc(cache_dir: &Path, body: &[u8]) {
        let path = generation_path(cache_dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    /// Task 47's janitor. Generations 1 to 5 exist; the document names 3, which still holds an
    /// undone chunk (it is live), and 4 and 5 are directories crashed authors left unpublished.
    /// The janitor keeps the two highest and the live one whatever its rank, reaps the rest, and
    /// reaps nothing when the document cannot be read.
    #[tokio::test]
    async fn the_janitor_keeps_the_latest_two_and_never_reaps_the_live_generation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        for g in 1..=5u64 {
            let bucket = generation_dir(dir, g).join("bkt");
            std::fs::create_dir_all(&bucket).unwrap();
            std::fs::write(bucket.join("00.chunk"), b"x").unwrap();
        }
        write_generation_doc(dir, br#"{"schema":1,"generation":3}"#);
        let reaped = reap_old_generations(dir).await.unwrap();
        assert_eq!(reaped.current, 3);
        assert_eq!(reaped.reaped, vec![1, 2], "{reaped:?}");
        assert!(
            generation_dir(dir, 3).join("bkt/00.chunk").exists(),
            "the janitor reaped the live generation"
        );
        assert!(generation_dir(dir, 4).exists() && generation_dir(dir, 5).exists());

        write_generation_doc(dir, br#"{"schema":1,"generation":5}"#);
        let reaped = reap_old_generations(dir).await.unwrap();
        assert_eq!(reaped.reaped, vec![3], "{reaped:?}");
        assert!(generation_dir(dir, 5).join("bkt/00.chunk").exists());
        assert!(generation_dir(dir, 4).exists());

        let older = generation_dir(dir, 2).join("bkt");
        std::fs::create_dir_all(&older).unwrap();
        write_generation_doc(dir, b"{\"schema\":1,\"gene");
        assert!(
            reap_old_generations(dir).await.is_err(),
            "a torn generation document reaps nothing"
        );
        assert!(older.exists());
    }
}

/// Task 49: the cycle wiring and the lock-free abort.
#[cfg(test)]
mod driver_tests {
    use super::*;
    use crate::cache::{CacheEvictionAlgorithm, CacheManager};
    use crate::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
    use crate::compression::CompressionAlgorithm;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    /// The driver's claims are process-global; these tests take them through real cycles.
    static DRIVER_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    const OBJECTS: u64 = 10;
    const OBJECT_BYTES: u64 = 100_000;
    const MAX: u64 = OBJECTS * OBJECT_BYTES;

    async fn manager(
        cache_dir: &Path,
        shared: crate::config::SharedStorageConfig,
        max: u64,
    ) -> Arc<CacheManager> {
        for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
            std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
        }
        let m = Arc::new(CacheManager::new_with_shared_storage(
            cache_dir.to_path_buf(),
            false,
            0,
            max,
            CacheEvictionAlgorithm::TinyLFU,
            1024,
            true,
            Duration::from_secs(315_360_000),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            false,
            shared,
            10.0,
            false,
            Duration::from_secs(86_400),
            crate::config::MetadataCacheConfig::default(),
            95,
            80,
            true,
            Duration::from_secs(60),
            1_048_576,
            false,
            Duration::from_secs(10),
            64,
            Duration::from_secs(5),
        ));
        let _ = m.create_configured_disk_cache_manager();
        m.set_cache_manager_in_consolidator().await;
        m
    }

    fn plant(manager: &CacheManager, cache_dir: &Path, i: u64, at: SystemTime) {
        let cache_key = format!("test-bucket/scn1-object-{i:02}");
        let ranges_base = cache_dir.join("ranges");
        let bin = crate::disk_cache::get_sharded_path(
            &ranges_base,
            &cache_key,
            &format!("_0-{}.bin", OBJECT_BYTES - 1),
        )
        .unwrap();
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, vec![0u8; OBJECT_BYTES as usize]).unwrap();
        let rel = bin
            .strip_prefix(&ranges_base)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let mut range = RangeSpec::new(
            0,
            OBJECT_BYTES - 1,
            rel,
            CompressionAlgorithm::None,
            OBJECT_BYTES,
            OBJECT_BYTES,
        );
        range.created_at = at;
        range.last_accessed = at;
        range.access_count = 1;
        range.staged = Some(false);
        let metadata = NewCacheMetadata {
            cache_key: cache_key.clone(),
            object_metadata: ObjectMetadata {
                content_length: OBJECT_BYTES,
                etag: format!("\"scn1-{i}\""),
                last_modified: "Tue, 06 Oct 2026 15:00:00 GMT".to_string(),
                ..Default::default()
            },
            ranges: vec![range],
            created_at: at,
            expires_at: SystemTime::now() + Duration::from_secs(3600),
            ..Default::default()
        };
        let path = manager.get_new_metadata_file_path(&cache_key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_string(&metadata).unwrap()).unwrap();
    }

    fn bins(cache_dir: &Path) -> usize {
        walkdir::WalkDir::new(cache_dir.join("ranges"))
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".bin"))
            .count()
    }

    fn worklist_dirs(cache_dir: &Path) -> Vec<PathBuf> {
        let state = cache_dir.join("metadata").join("_shard_state");
        [WORKLIST_DIR, crate::cache::EVICTION_CLAIMS_DIR]
            .iter()
            .map(|d| state.join(d))
            .filter(|p| p.exists())
            .collect()
    }

    /// The cycle-level pin: with a default `SharedStorageConfig`, a
    /// consolidation cycle on a volume over the eviction trigger, in a fleet every member of
    /// which advertises the capability, authors a work list rather than running the legacy pass.
    ///
    /// The cycle spawns its eviction detached, so the test waits for a positive sign that a pass
    /// ran: a range file left the disk (legacy) or a work-list directory appeared (authored).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_a_default_config_a_tripped_cycle_in_a_capable_fleet_authors_a_worklist() {
        let _serial = DRIVER_SERIAL.lock().await;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir, crate::config::SharedStorageConfig::default(), MAX).await;
        let consolidator = m
            .get_journal_consolidator()
            .await
            .expect("fixture: the consolidator must be wired");
        let now = SystemTime::now();
        for i in 0..OBJECTS {
            plant(&m, dir, i, now - Duration::from_secs(36_000 - i * 600));
        }
        consolidator
            .persist_size_state(&crate::journal_consolidator::SizeState {
                total_size: MAX,
                ..Default::default()
            })
            .await
            .unwrap();
        test_fleet::capable(dir, test_fleet::now());
        assert_eq!(
            fleet_is_worklist_capable(dir, test_fleet::now()),
            Capability::Capable,
            "fixture: a capable fleet, so the lock holder authors"
        );
        assert_eq!(bins(dir), OBJECTS as usize);

        consolidator.run_consolidation_cycle().await.unwrap();
        consolidator.await_maintenance_passes().await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while bins(dir) == OBJECTS as usize && worklist_dirs(dir).is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "APPARATUS: no eviction pass was observed within 30 s; the verdict is unknown"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !worklist_dirs(dir).is_empty(),
            "a default config in a capable fleet must author a work list; {} of {} range files \
             remain, so the legacy pass ran instead",
            bins(dir),
            OBJECTS
        );
    }

    /// Per-generation pickup: the generation the lock holder authors starts executing on the
    /// authoring process at once. No consolidation cycle runs anywhere in this test, so the only
    /// executor pass that can run is the one the author starts; a cycle between authoring and
    /// the check would schedule its own pass and make the test pass without the pickup.
    ///
    /// Red side: `AUTHOR_SKIPS_EXECUTOR_PICKUP`; every chunk of generation 1 stays without a
    /// done-marker, waiting for a cycle that never comes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_authored_generation_starts_executing_on_its_author_without_a_cycle() {
        let _serial = DRIVER_SERIAL.lock().await;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir, crate::config::SharedStorageConfig::default(), MAX).await;
        let consolidator = m
            .get_journal_consolidator()
            .await
            .expect("fixture: the consolidator must be wired");
        let now = SystemTime::now();
        for i in 0..OBJECTS {
            plant(&m, dir, i, now - Duration::from_secs(36_000 - i * 600));
        }
        consolidator
            .persist_size_state(&crate::journal_consolidator::SizeState {
                total_size: MAX,
                ..Default::default()
            })
            .await
            .unwrap();
        test_fleet::capable(dir, test_fleet::now());
        assert_eq!(
            fleet_is_worklist_capable(dir, test_fleet::now()),
            Capability::Capable,
            "fixture: a capable fleet, so the lock holder authors"
        );
        assert_eq!(
            current_generation(dir).await,
            0,
            "fixture: no generation yet"
        );

        // The author's own call, as the cycle's spawned eviction task makes it. It returns once
        // the generation is written; it does not wait for any chunk.
        let freed = tokio::time::timeout(
            Duration::from_secs(60),
            m.enforce_disk_cache_limits_skip_consolidation(),
        )
        .await
        .expect("the author must return")
        .unwrap();
        assert_eq!(freed, 0, "the author frees nothing itself");
        assert_eq!(
            current_generation(dir).await,
            1,
            "fixture: the call must have authored generation 1"
        );
        let shards = list_chunk_shards(dir, 1).await.unwrap();
        assert!(!shards.is_empty(), "fixture: generation 1 lists chunks");

        // No consolidation cycle. The pickup is tracked with the maintenance passes.
        tokio::time::timeout(
            Duration::from_secs(60),
            consolidator.await_maintenance_passes(),
        )
        .await
        .expect("the pickup pass must finish");
        let undone: Vec<String> = shards
            .iter()
            .filter(|s| !done_path(dir, 1, s).unwrap().exists())
            .map(|s| format!("{s:?}"))
            .collect();
        assert!(
            undone.is_empty(),
            "the author's process must execute the generation it authored without waiting for a \
             consolidation cycle: {} of {} chunk(s) have no done-marker ({undone:?}), {} of {} \
             range files remain",
            undone.len(),
            shards.len(),
            bins(dir),
            OBJECTS
        );
        assert!(
            bins(dir) < OBJECTS as usize,
            "executing the chunks must have deleted range files"
        );
    }

    /// The roster-refresh schedule. Proxy P (`host-p:2`) has been quiet for two hours, so its
    /// journal is gone and its record is two hours old, and the fleet reads `Capable` with a live
    /// generation. P serves one cache hit: the hit buffer's flush appends to P's journal, which
    /// puts P back in the roster at once. A peer's consolidation cycle runs before P's own: its
    /// abort tick reads the gate. P's own cycle would be idle once the peer has applied the entry
    /// and deleted the journal, so nothing else refreshes P's record.
    ///
    /// Red side: `staged_authority::JOURNAL_APPEND_SKIPS_READY_REFRESH`; the peer's abort tick
    /// reads `NotCapable { host-p:2 }` and aborts generation 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_quiet_proxys_first_append_does_not_let_a_peer_abort_the_live_generation() {
        let _serial = DRIVER_SERIAL.lock().await;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // A capacity far above anything planted, so no size trigger is involved.
        let peer = manager(dir, crate::config::SharedStorageConfig::default(), 1 << 40).await;
        const P: &str = "host-p:2";

        // A live generation 1 with one chunk under a peer lease (the `an_old_proxy...` pattern),
        // so the cycle's executor leaves it alone and the abort tick is the only actor on it.
        let shard = Shard::new("test-bucket", "00");
        let item = WorkItem {
            cache_key: "test-bucket/scn2-live".to_string(),
            start: 0,
            end: 4095,
            identity: ExpectedObjectIdentity::of(&NewCacheMetadata::default()),
            staged: false,
            is_write_cached: false,
            compressed_size: 4096,
            size: 4096,
        };
        let chunk = Chunk::new_for_pass(1, "host-a:1", test_fleet::now(), 4096, vec![item]);
        let path = chunk_path(dir, 1, &shard).unwrap();
        assert_eq!(
            publish_chunk(&path, &chunk).await.unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(
            advance_generation(&crate::cache::EvictionLockHeld::for_tests(), dir, 1)
                .await
                .unwrap(),
            Advance::Advanced
        );
        assert_eq!(
            crate::orphan_reclaim::claim_shard_lease(
                dir,
                crate::cache::EVICTION_CLAIMS_DIR,
                3600,
                &shard,
                "host-peer:7",
                test_fleet::now(),
            ),
            crate::orphan_reclaim::ClaimState::Claimed,
            "fixture: a peer holds the chunk's lease"
        );

        // P: quiet for two hours. No journal, and a capable record two hours old.
        let now = test_fleet::now();
        test_fleet::capable(dir, now);
        test_fleet::ready(dir, P, true, now - 2 * CAPABILITY_WINDOW_SECS);
        let journal = dir
            .join("metadata")
            .join("_journals")
            .join(format!("{P}.journal"));
        assert!(!journal.exists(), "fixture: P has no journal");
        assert_eq!(
            fleet_is_worklist_capable(dir, now),
            Capability::Capable,
            "fixture: with P out of the roster the fleet is capable"
        );
        assert!(!read_aborted(dir, 1).await.unwrap(), "fixture: G is live");

        // P serves one cache hit; the hit buffer flushes it to P's journal.
        let hits = crate::cache_hit_update_buffer::CacheHitUpdateBuffer::new(
            dir.to_path_buf(),
            P.to_string(),
        );
        hits.record_access_update("test-bucket/scn2-hit", 0, 1023, 1)
            .await
            .unwrap();
        hits.flush().await.unwrap();
        assert!(
            std::fs::metadata(&journal).is_ok_and(|m| m.len() > 0),
            "fixture: the flush appended to P's journal"
        );
        // The refresh, if any, is a detached write: wait for it (bounded), and go on either way so
        // the verdict comes from the peer's cycle below.
        let record_at = |dir: &Path| {
            crate::staged_authority::read_ready_records(dir)
                .unwrap()
                .into_iter()
                .find(|r| r.instance_id == P)
                .map(|r| r.written_at)
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while record_at(dir) < Some(now - CAPABILITY_WINDOW_SECS)
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The peer's cycle, before P's own: its abort tick reads the gate with P in the roster.
        let consolidator = peer.get_journal_consolidator().await.unwrap();
        consolidator.run_consolidation_cycle().await.unwrap();
        consolidator.await_maintenance_passes().await;

        let aborted = std::fs::read(abort_path(dir, 1))
            .ok()
            .map(|b| String::from_utf8_lossy(&b).to_string());
        assert_eq!(
            aborted,
            None,
            "a quiet proxy's first journal append must not leave it in the roster with a stale \
             record: the peer's cycle aborted generation 1 (gate now {:?}, P's record written_at \
             {:?}, now {})",
            fleet_is_worklist_capable(dir, test_fleet::now()),
            record_at(dir),
            now
        );
        assert!(
            !read_aborted(dir, 1).await.unwrap(),
            "generation 1 stays live"
        );
    }

    /// Amendment 1: an older proxy holds `global_eviction.lock` and is in the roster without the
    /// capability. A capable proxy's ordinary consolidation cycle still writes the abort marker
    /// for the live generation, and its executor then reads the generation as aborted rather
    /// than paused.
    ///
    /// Red side: `ABORT_ONLY_UNDER_LOCK`; the capable proxy does not hold the lock, so nothing
    /// aborts the generation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_old_proxy_holding_the_eviction_lock_still_gets_the_live_generation_aborted() {
        let _serial = DRIVER_SERIAL.lock().await;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // A capacity far above anything planted, so no size trigger is involved.
        let new_proxy = manager(dir, crate::config::SharedStorageConfig::default(), 1 << 40).await;
        let old_proxy = manager(dir, crate::config::SharedStorageConfig::default(), 1 << 40).await;

        // A live generation 1 with one unexecuted chunk, authored while the fleet was capable.
        let shard = Shard::new("test-bucket", "00");
        let item = WorkItem {
            cache_key: "test-bucket/scn1-live".to_string(),
            start: 0,
            end: 4095,
            identity: ExpectedObjectIdentity::of(&NewCacheMetadata::default()),
            staged: false,
            is_write_cached: false,
            compressed_size: 4096,
            size: 4096,
        };
        let chunk = Chunk::new_for_pass(1, "host-a:1", test_fleet::now(), 4096, vec![item]);
        let path = chunk_path(dir, 1, &shard).unwrap();
        assert_eq!(
            publish_chunk(&path, &chunk).await.unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(
            advance_generation(&crate::cache::EvictionLockHeld::for_tests(), dir, 1)
                .await
                .unwrap(),
            Advance::Advanced
        );

        // The older proxy joins: a fresh journal and no `.ready` record. It wins the lock.
        let now = test_fleet::now();
        test_fleet::capable(dir, now);
        test_fleet::journal(dir, "host-old:9", now);
        assert!(old_proxy.try_acquire_global_eviction_lock().await.unwrap());
        assert!(
            !new_proxy.try_acquire_global_eviction_lock().await.unwrap(),
            "fixture: the older proxy must hold the lock, so the capable proxy cannot"
        );
        assert!(new_proxy.eviction_lock_held().is_none());
        assert!(!read_aborted(dir, 1).await.unwrap(), "fixture: G is live");
        // A live peer lease on the chunk: the cycle also spawns this proxy's
        // executor, which reads `Busy` and leaves the chunk alone, so the abort tick is the only
        // actor on it. Without the lease the executor could finish the chunk first, and an
        // abort tick that then finds no live generation writes nothing.
        assert_eq!(
            crate::orphan_reclaim::claim_shard_lease(
                dir,
                crate::cache::EVICTION_CLAIMS_DIR,
                3600,
                &shard,
                "host-peer:7",
                test_fleet::now(),
            ),
            crate::orphan_reclaim::ClaimState::Claimed,
            "fixture: a peer holds the chunk's lease"
        );

        let consolidator = new_proxy.get_journal_consolidator().await.unwrap();
        consolidator.run_consolidation_cycle().await.unwrap();
        consolidator.await_maintenance_passes().await;

        assert!(
            read_aborted(dir, 1).await.unwrap(),
            "the capable proxy's cycle must abort generation 1 while an older proxy holds the \
             eviction lock"
        );
        let marker: AbortMarker =
            serde_json::from_slice(&std::fs::read(abort_path(dir, 1)).unwrap()).unwrap();
        assert!(marker.reason.contains("host-old:9"), "{marker:?}");
        assert!(
            !done_path(dir, 1, &shard).unwrap().exists(),
            "fixture: the peer's lease kept this proxy's executor off the chunk"
        );
        let pass = new_proxy
            .run_eviction_executor_pass(&executor_budget(
                &crate::config::SharedStorageConfig::default(),
            ))
            .await
            .unwrap();
        assert!(
            pass.aborted,
            "executors see the abort, not a pause: {pass:?}"
        );
        assert!(pass.chunks_executed.is_empty());
        old_proxy.release_global_eviction_lock().await.unwrap();
    }

    /// The consolidation cycle schedules the generation janitor.
    ///
    /// Seven generation directories over time: 1 to 4 published and drained (every chunk
    /// done-marked), 5 published and live (its chunk has no done-marker and a peer holds its
    /// lease), 6 and 7 left by crashed authors that never wrote the generation document. One
    /// ordinary cycle must leave exactly {5, 6, 7}: the two highest, plus the live generation
    /// although it ranks third, with its chunk intact.
    ///
    /// Red side: `JANITOR_UNSCHEDULED`; all seven directories remain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cycle_reaps_superseded_generations_and_keeps_the_live_one() {
        let _serial = DRIVER_SERIAL.lock().await;
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        // A capacity far above anything planted, so no size trigger and no author is involved.
        let m = manager(dir, crate::config::SharedStorageConfig::default(), 1 << 40).await;
        let shard = Shard::new("test-bucket", "00");
        let item = WorkItem {
            cache_key: "test-bucket/scn3-janitor".to_string(),
            start: 0,
            end: 4095,
            identity: ExpectedObjectIdentity::of(&NewCacheMetadata::default()),
            staged: false,
            is_write_cached: false,
            compressed_size: 4096,
            size: 4096,
        };
        let lock = crate::cache::EvictionLockHeld::for_tests();
        for g in 1..=7u64 {
            let chunk =
                Chunk::new_for_pass(g, "host-a:1", test_fleet::now(), 4096, vec![item.clone()]);
            let path = chunk_path(dir, g, &shard).unwrap();
            assert_eq!(
                publish_chunk(&path, &chunk).await.unwrap(),
                PublishOutcome::Published
            );
            if g <= 4 {
                let done = DoneMarker {
                    schema: SCHEMA,
                    generation: g,
                    executor: "host-a:1".to_string(),
                    completed_at: test_fleet::now(),
                    chunk_checksum: chunk.header.checksum.clone(),
                };
                let mut body = serde_json::to_vec(&done).unwrap();
                body.push(b'\n');
                std::fs::write(done_path(dir, g, &shard).unwrap(), body).unwrap();
            }
            if g <= 5 {
                assert_eq!(
                    advance_generation(&lock, dir, g).await.unwrap(),
                    Advance::Advanced
                );
            }
        }
        assert_eq!(read_generation(dir).await, GenerationRead::Generation(5));
        // The live chunk's lease belongs to a peer, so this proxy's executor leaves it alone.
        assert_eq!(
            crate::orphan_reclaim::claim_shard_lease(
                dir,
                crate::cache::EVICTION_CLAIMS_DIR,
                3600,
                &shard,
                "host-peer:7",
                test_fleet::now(),
            ),
            crate::orphan_reclaim::ClaimState::Claimed,
            "fixture: a peer holds the live chunk's lease"
        );
        let present = |dir: &Path| -> Vec<u64> {
            (1..=7u64)
                .filter(|g| generation_dir(dir, *g).exists())
                .collect()
        };
        assert_eq!(present(dir), vec![1, 2, 3, 4, 5, 6, 7], "fixture");

        let consolidator = m.get_journal_consolidator().await.unwrap();
        tokio::time::timeout(Duration::from_secs(120), async {
            consolidator.run_consolidation_cycle().await.unwrap();
            consolidator.await_maintenance_passes().await;
        })
        .await
        .expect("APPARATUS: the cycle did not finish within 120 s; the verdict is unknown");

        assert_eq!(
            present(dir),
            vec![5, 6, 7],
            "one cycle must reap every generation but the two highest and the live one"
        );
        assert!(
            chunk_path(dir, 5, &shard).unwrap().exists(),
            "the live generation's chunk must survive the janitor"
        );
        assert!(
            !done_path(dir, 5, &shard).unwrap().exists(),
            "fixture: the live chunk is still unexecuted"
        );
        assert_eq!(
            highest_generation_dir(dir).await.unwrap(),
            7,
            "the author's next generation is still one above every directory left"
        );
    }
}
