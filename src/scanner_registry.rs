//! Scanner documents (cache-eviction-at-scale § 16.6.13 K1, K5).
//!
//! Each instance publishes one small document per `(instance_id, ScanKind)` under
//! `metadata/_shard_state/scanners/` when a pass starts and ends it when the pass finishes. The
//! scan limiter reads the directory to learn how many OTHER scanners are live (`n`) and how many
//! distinct instances scanned recently (`k_max`). One readdir plus one small read per document,
//! so O(I) in the number of instances.
//!
//! Three properties, each with a red-side test below:
//!
//! - **A torn, unparseable or unreadable document counts as ABSENT (K5).** This is the opposite
//!   of the shard-claim convention, deliberately: here a phantom scanner would make the limiter
//!   defer, and deferral is the direction that suppresses scanning. An absent reading only
//!   lets one more pass run, which is the status quo.
//! - **An ended document is not live.** The lease ([`SCANNER_LEASE_SECS`]) is only the backstop for
//!   an instance that died mid-pass; a pass that finishes calls [`end`].
//! - **The caller's own `(instance_id, kind)` is never counted.** The same instance's OTHER kind
//!   is counted, because fold and repair run as separate tasks.
//!
//! Nothing here is load-bearing for correctness (R19.2): a lost, stale or garbled document can
//! only change how many passes run concurrently, never what a pass computes.

use crate::path_safety::is_safe_path_component;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Schema of a scanner document. Any other value reads as absent.
pub const SCANNER_DOC_SCHEMA: u32 = 1;

/// Lease on a scanner document: two staged-fold intervals, so a live instance that publishes at
/// the start of every pass never lapses, and a dead one stops counting within two passes.
pub const SCANNER_LEASE_SECS: u64 = 2 * crate::staged_fold::STAGED_FOLD_INTERVAL_SECS;
const _: () = assert!(SCANNER_LEASE_SECS == 600);

const SCANNER_DOC_SUFFIX: &str = ".scan";

/// The kind of maintenance pass a document describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScanKind {
    Fold,
    Repair,
    Validation,
}

impl ScanKind {
    /// The kind's name as it appears in the file name and the document.
    pub fn as_str(self) -> &'static str {
        match self {
            ScanKind::Fold => "fold",
            ScanKind::Repair => "repair",
            ScanKind::Validation => "validation",
        }
    }
}

/// One scanner's published state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScannerDocument {
    pub schema: u32,
    pub instance_id: String,
    pub kind: ScanKind,
    pub published_at: u64,
    pub ended_at: Option<u64>,
    pub lease_secs: u64,
}

impl ScannerDocument {
    fn lease_covers(&self, now: u64) -> bool {
        self.published_at.saturating_add(self.lease_secs) > now
    }
}

/// `{cache_dir}/metadata/_shard_state/scanners`.
pub fn scanners_dir(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join("_shard_state")
        .join("scanners")
}

fn doc_path(cache_dir: &Path, instance_id: &str, kind: ScanKind) -> std::io::Result<PathBuf> {
    if !is_safe_path_component(instance_id) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unsafe scanner instance id {instance_id:?}"),
        ));
    }
    Ok(scanners_dir(cache_dir).join(format!(
        "{instance_id}.{}{SCANNER_DOC_SUFFIX}",
        kind.as_str()
    )))
}

/// Write `body` to `path` through an instance-specific temp file `{name}.tmp.{host}.{pid}`,
/// synced, then rename it into place, so a reader sees the old document or the new one whole.
///
/// The same scheme as `staged_authority`'s private `install_atomic`; that helper is not
/// visible outside its module, so it is restated here rather than widened.
fn install_atomic(path: &Path, body: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let host = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
        .replace(['/', '\\'], "_");
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "doc".to_string());
    let tmp = path.with_file_name(format!("{name}.tmp.{host}.{}", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

fn write_doc(cache_dir: &Path, doc: &ScannerDocument) -> std::io::Result<()> {
    let path = doc_path(cache_dir, &doc.instance_id, doc.kind)?;
    let body = serde_json::to_string(doc).map_err(std::io::Error::other)?;
    install_atomic(&path, &body)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Publish (or refresh) this instance's document for `kind`, live from now for one lease.
pub fn publish(cache_dir: &Path, instance_id: &str, kind: ScanKind) -> std::io::Result<()> {
    publish_at(cache_dir, instance_id, kind, now_secs())
}

fn publish_at(
    cache_dir: &Path,
    instance_id: &str,
    kind: ScanKind,
    now: u64,
) -> std::io::Result<()> {
    write_doc(
        cache_dir,
        &ScannerDocument {
            schema: SCANNER_DOC_SCHEMA,
            instance_id: instance_id.to_string(),
            kind,
            published_at: now,
            ended_at: None,
            lease_secs: SCANNER_LEASE_SECS,
        },
    )
}

/// End this instance's document for `kind`. Keeps the published time, so the instance still
/// counts toward [`k_max`] for the rest of its lease. A missing or unreadable document is
/// replaced by one published and ended now.
pub fn end(cache_dir: &Path, instance_id: &str, kind: ScanKind) -> std::io::Result<()> {
    end_at(cache_dir, instance_id, kind, now_secs())
}

fn end_at(cache_dir: &Path, instance_id: &str, kind: ScanKind, now: u64) -> std::io::Result<()> {
    let path = doc_path(cache_dir, instance_id, kind)?;
    let mut doc = read_doc(&path)
        .filter(|d| d.instance_id == instance_id && d.kind == kind)
        .unwrap_or(ScannerDocument {
            schema: SCANNER_DOC_SCHEMA,
            instance_id: instance_id.to_string(),
            kind,
            published_at: now,
            ended_at: None,
            lease_secs: SCANNER_LEASE_SECS,
        });
    doc.ended_at = Some(now);
    write_doc(cache_dir, &doc)
}

/// Parse one document. `None` for unreadable, torn, unparseable or unknown-schema content.
fn read_doc(path: &Path) -> Option<ScannerDocument> {
    let body = std::fs::read(path).ok()?;
    let doc: ScannerDocument = serde_json::from_slice(&body).ok()?;
    (doc.schema == SCANNER_DOC_SCHEMA).then_some(doc)
}

/// Every readable document in the directory. Temp files and other names are skipped; an
/// unreadable directory yields nothing.
fn read_all(cache_dir: &Path) -> Vec<ScannerDocument> {
    let Ok(entries) = std::fs::read_dir(scanners_dir(cache_dir)) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .ends_with(SCANNER_DOC_SUFFIX)
        })
        .filter_map(|e| read_doc(&e.path()))
        .collect()
}

/// Live scanners other than the caller's own `(instance_id, kind)`: not ended and inside their
/// lease at `now`. Absent, torn and unreadable documents do not count.
pub fn count_live_others(cache_dir: &Path, instance_id: &str, kind: ScanKind, now: u64) -> u32 {
    read_all(cache_dir)
        .iter()
        .filter(|d| !(d.instance_id == instance_id && d.kind == kind))
        .filter(|d| d.ended_at.is_none())
        .filter(|d| d.lease_covers(now))
        .count() as u32
}

/// Distinct instances with any document published inside one lease of `now`, ended or not.
/// Never below 1, since the caller is itself a scanner.
pub fn k_max(cache_dir: &Path, now: u64) -> u32 {
    k_max_including(cache_dir, None, now)
}

/// [`k_max`], counting `instance_id` whether or not its own document is inside the lease.
///
/// The caller is about to scan, so it is one of the instances the ceiling must admit. Without
/// this, an instance whose document has lapsed sees one live peer as `k_max = 1` and defers,
/// which serialises a fleet that today scans concurrently.
pub fn k_max_including(cache_dir: &Path, instance_id: Option<&str>, now: u64) -> u32 {
    let mut ids: std::collections::HashSet<String> = read_all(cache_dir)
        .into_iter()
        .filter(|d| d.lease_covers(now))
        .map(|d| d.instance_id)
        .collect();
    if let Some(id) = instance_id {
        ids.insert(id.to_string());
    }
    (ids.len() as u32).max(1)
}

/// Remove the documents of the ids `is_retired` names whose lease has lapsed at `now`, given when
/// each lapsed. Returns how many were removed. Blocking, for the dead-instance reaper.
///
/// A lapsed document counts in neither [`count_live_others`] nor [`k_max`], so removing one
/// changes no limiter decision. Unreadable documents and temps are left alone.
pub(crate) fn reap_retired(
    cache_dir: &Path,
    now: u64,
    is_retired: &dyn Fn(&str, std::time::SystemTime) -> bool,
) -> u64 {
    let Ok(entries) = std::fs::read_dir(scanners_dir(cache_dir)) else {
        return 0;
    };
    let mut removed = 0u64;
    for entry in entries.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .ends_with(SCANNER_DOC_SUFFIX)
        {
            continue;
        }
        let path = entry.path();
        let Some(doc) = read_doc(&path) else {
            continue;
        };
        if doc.lease_covers(now) {
            continue;
        }
        // The document must sit at its own path, so a mislabelled body cannot remove another
        // id's file.
        if doc_path(cache_dir, &doc.instance_id, doc.kind)
            .ok()
            .as_deref()
            != Some(path.as_path())
        {
            continue;
        }
        let lapsed_at = std::time::UNIX_EPOCH
            + std::time::Duration::from_secs(doc.published_at.saturating_add(doc.lease_secs));
        if is_retired(&doc.instance_id, lapsed_at) && std::fs::remove_file(&path).is_ok() {
            removed += 1;
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const T0: u64 = 1_000_000;
    const A: &str = "ip-10-0-0-1.internal:1234";
    const B: &str = "ip-10-0-0-2.internal:5678";

    #[test]
    fn torn_document_counts_as_absent() {
        let dir = TempDir::new().unwrap();
        let cache = dir.path();
        publish_at(cache, B, ScanKind::Fold, T0).unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 1), 1);
        let path = doc_path(cache, B, ScanKind::Fold).unwrap();
        let body = std::fs::read(&path).unwrap();
        std::fs::write(&path, &body[..body.len() / 2]).unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 1), 0);
        // Unknown schema and empty files read as absent too.
        std::fs::write(&path, b"").unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 1), 0);
        let mut doc: ScannerDocument = serde_json::from_slice(&body).unwrap();
        doc.schema = 99;
        std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 1), 0);
    }

    #[test]
    fn expired_lease_is_not_live() {
        let dir = TempDir::new().unwrap();
        let cache = dir.path();
        publish_at(cache, B, ScanKind::Repair, T0).unwrap();
        let last_live = T0 + SCANNER_LEASE_SECS - 1;
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, last_live), 1);
        assert_eq!(
            count_live_others(cache, A, ScanKind::Fold, T0 + SCANNER_LEASE_SECS),
            0
        );
        assert_eq!(k_max(cache, T0 + SCANNER_LEASE_SECS), 1);
    }

    #[test]
    fn own_document_is_not_counted_but_own_other_kind_is() {
        let dir = TempDir::new().unwrap();
        let cache = dir.path();
        publish_at(cache, A, ScanKind::Fold, T0).unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 1), 0);
        publish_at(cache, A, ScanKind::Repair, T0).unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 1), 1);
        assert_eq!(count_live_others(cache, B, ScanKind::Fold, T0 + 1), 2);
    }

    #[test]
    fn ended_document_is_not_live_but_counts_toward_k_max() {
        let dir = TempDir::new().unwrap();
        let cache = dir.path();
        publish_at(cache, B, ScanKind::Fold, T0).unwrap();
        publish_at(cache, A, ScanKind::Fold, T0).unwrap();
        end_at(cache, B, ScanKind::Fold, T0 + 10).unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 11), 0);
        let doc = read_doc(&doc_path(cache, B, ScanKind::Fold).unwrap()).unwrap();
        assert_eq!(doc.published_at, T0);
        assert_eq!(doc.ended_at, Some(T0 + 10));
        assert_eq!(k_max(cache, T0 + 11), 2);
    }

    /// A caller whose own document has lapsed still counts toward the ceiling, so one live peer
    /// gives it `k_max = 2` and it does not defer behind that peer.
    #[test]
    fn k_max_including_counts_a_caller_with_no_live_document() {
        let dir = TempDir::new().unwrap();
        let cache = dir.path();
        publish_at(cache, B, ScanKind::Fold, T0).unwrap();
        assert_eq!(k_max(cache, T0 + 1), 1);
        assert_eq!(k_max_including(cache, Some(A), T0 + 1), 2);
        assert_eq!(k_max_including(cache, Some(B), T0 + 1), 1);
    }

    #[test]
    fn k_max_floor_and_distinct_instances() {
        let dir = TempDir::new().unwrap();
        let cache = dir.path();
        assert_eq!(k_max(cache, T0), 1);
        publish_at(cache, A, ScanKind::Fold, T0).unwrap();
        publish_at(cache, A, ScanKind::Repair, T0).unwrap();
        publish_at(cache, B, ScanKind::Validation, T0).unwrap();
        assert_eq!(k_max(cache, T0 + 1), 2);
    }

    #[test]
    fn unreadable_directory_and_temp_files_count_nothing() {
        let dir = TempDir::new().unwrap();
        let cache = dir.path();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0), 0);
        // A whole document under a temp name is not a published document.
        publish_at(cache, B, ScanKind::Fold, T0).unwrap();
        let path = doc_path(cache, B, ScanKind::Fold).unwrap();
        std::fs::rename(&path, path.with_file_name("x.fold.scan.tmp.host.1")).unwrap();
        assert_eq!(count_live_others(cache, A, ScanKind::Fold, T0 + 1), 0);
        assert!(std::fs::read_dir(scanners_dir(cache)).unwrap().all(|e| !e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".scan")));
    }

    #[test]
    fn unsafe_instance_id_is_rejected() {
        let dir = TempDir::new().unwrap();
        for bad in ["", "../x", "a/b", "a\\b", "nul\0"] {
            assert!(publish(dir.path(), bad, ScanKind::Fold).is_err(), "{bad:?}");
            assert!(end(dir.path(), bad, ScanKind::Fold).is_err(), "{bad:?}");
        }
        publish(dir.path(), A, ScanKind::Fold).unwrap();
        end(dir.path(), A, ScanKind::Fold).unwrap();
    }
}
