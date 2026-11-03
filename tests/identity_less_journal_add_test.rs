//! R12.1: a journal `Add` with no `file_identity` must never make another version's
//! file servable.
//!
//! Every 2.x writer journals its `Add`s with no identity, and so does a 3.0.x writer whose
//! `identity_after_close` failed. When another version's fill has since been renamed over the
//! extent's path and its own `Add` is not in the batch, folding the identity-less `Add` lists
//! the extent under the old version's ETag over the new version's bytes, and a listing with no
//! identity is read unverified.
//!
//! The extent is the whole object, `(0, N-1)` with `content_length = N`, which the serve path
//! does not drop as a short `0..` prefix.
//!
//! Control: the same `Add` recording the identity of the file it wrote is refused when another
//! file is at the path.
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, UploadState};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const LEN: u64 = 300_000;
const V1: u8 = 0x11;
const V2: u8 = 0x22;
const ETAG_V1: &str = "\"11111111111111111111111111111111\"";
const ETAG_V2: &str = "\"22222222222222222222222222222222\"";
const KEY: &str = "test-bucket/r12/identity-less.bin";

fn version_bytes(v: u8) -> Vec<u8> {
    (0..LEN as usize).map(|i| ((i % 251) as u8) ^ v).collect()
}

/// Which version filled `bytes`, or `None` for a mixture.
fn version_of(bytes: &[u8]) -> Option<u8> {
    let first = *bytes.first()?;
    bytes
        .iter()
        .enumerate()
        .all(|(i, b)| *b == ((i % 251) as u8) ^ first)
        .then_some(first)
}

fn setup(cache_dir: &Path) -> Arc<CacheManager> {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    Arc::new(CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false,
        0,
        CacheEvictionAlgorithm::TinyLFU,
    ))
}

fn object_metadata(etag: &str) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag.to_string(),
        content_length: LEN,
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        upload_state: UploadState::Complete,
        cumulative_size: LEN,
        ..Default::default()
    }
}

/// Fill the whole object from version `v`; returns the `.bin`'s path. The fill journals one
/// `Add` and has no fill witness.
async fn fill(manager: &CacheManager, cache_dir: &Path, v: u8, etag: &str) -> PathBuf {
    let mut dcm = manager.create_configured_disk_cache_manager();
    dcm.store_range(
        KEY,
        0,
        LEN - 1,
        &version_bytes(v),
        object_metadata(etag),
        Duration::from_secs(3600),
        true,
    )
    .await
    .expect("range fill");
    let spec = dcm
        .find_pending_journal_ranges(KEY, 0, LEN - 1)
        .await
        .expect("pending lookup")
        .into_iter()
        .next()
        .expect("the fill is journal-pending");
    cache_dir.join("ranges").join(&spec.file_path)
}

fn journal_files(cache_dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(cache_dir.join("metadata/_journals"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("journal"))
        .collect()
}

/// Drop `file_identity` from the key's journal `Add`, as every 2.x writer leaves it.
fn strip_journal_identity(cache_dir: &Path) {
    let mut stripped = 0;
    for path in journal_files(cache_dir) {
        let text = std::fs::read_to_string(&path).unwrap();
        let mut out = String::new();
        for line in text.lines() {
            let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
            if v["cache_key"] == KEY && v["operation"] == "Add" {
                v["range_spec"]
                    .as_object_mut()
                    .unwrap()
                    .remove("file_identity");
                stripped += 1;
            }
            out.push_str(&v.to_string());
            out.push('\n');
        }
        std::fs::write(&path, out).unwrap();
    }
    assert_eq!(stripped, 1, "fixture: the fill's one Add lost its identity");
}

/// Another version's file renamed over `path`, published at a fixed time years from this
/// test's fills, so it never shares an identity with the file it replaces.
async fn rename_v2_over(path: &Path) {
    let other = TempDir::new().unwrap();
    let manager = setup(other.path());
    let foreign = fill(&manager, other.path(), V2, ETAG_V2).await;
    let bytes = std::fs::read(&foreign).unwrap();
    let tmp = path.with_extension("r12tmp");
    std::fs::write(&tmp, bytes).unwrap();
    filetime::set_file_mtime(&tmp, filetime::FileTime::from_unix_time(1_000_000_000, 7)).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

fn read_meta(manager: &CacheManager) -> Option<NewCacheMetadata> {
    let text = std::fs::read_to_string(manager.get_new_metadata_file_path(KEY)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Fill V1, optionally strip its `Add`'s identity, rename V2's file over the path, fold, then
/// read every listing the fold wrote. Returns the fold's result and the `.meta`.
async fn fold_over_replaced_file(
    strip_identity: bool,
) -> (
    TempDir,
    s3_proxy::journal_consolidator::ConsolidationResult,
    Option<NewCacheMetadata>,
) {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    if strip_identity {
        strip_journal_identity(dir.path());
    }
    rename_v2_over(&bin).await;
    let result = manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .consolidate_object(KEY)
        .await
        .expect("consolidation");
    let meta = read_meta(&manager);
    if let Some(meta) = &meta {
        let dcm = manager.create_configured_disk_cache_manager();
        for spec in &meta.ranges {
            if let Ok(bytes) = dcm.load_range_data(spec).await {
                assert_eq!(
                    version_of(&bytes),
                    Some(V1),
                    "WRONG BYTES SERVED: the .meta (etag {}) lists {}-{} (identity {:?}) and \
                     load_range_data returned {} bytes of v{:?}",
                    meta.object_metadata.etag,
                    spec.start,
                    spec.end,
                    spec.file_identity,
                    bytes.len(),
                    version_of(&bytes)
                );
            }
        }
    }
    (dir, result, meta)
}

/// The identity-less `Add` neither creates a `.meta` nor serves V2's bytes under
/// V1's ETag, and is returned for cleanup like an applied entry.
#[tokio::test]
async fn an_identity_less_add_never_lists_another_versions_file() {
    let (_dir, result, meta) = fold_over_replaced_file(true).await;
    assert!(
        meta.is_none(),
        "an identity-less Add created a .meta: {:?}",
        meta.map(|m| (m.object_metadata.etag, m.ranges.len()))
    );
    assert_eq!(
        result.consolidated_entries.len(),
        1,
        "the retired Add is removed from the journal like an applied entry"
    );
}

/// Control: the `Add` records the identity of the file it wrote, so the replacement is
/// refused today and after R12.
#[tokio::test]
async fn control_an_identified_add_over_another_file_is_refused() {
    let _ = fold_over_replaced_file(false).await;
}

/// Set `bin`'s modification time an hour before now: older than the unlink's 600 s margin, so
/// only the guard under test can keep the file.
fn backdate(bin: &Path) {
    filetime::set_file_mtime(
        bin,
        filetime::FileTime::from_system_time(
            std::time::SystemTime::now() - Duration::from_secs(3600),
        ),
    )
    .unwrap();
}
/// Age the fill as a real one is aged: its journal `Add` stamped an hour ago and its file a
/// moment older than that `Add`, since a writer publishes the file before it stamps the `Add`.
fn age_fill(cache_dir: &Path, bin: &Path) {
    let add_time = std::time::SystemTime::now() - Duration::from_secs(3600);
    let since = add_time.duration_since(std::time::UNIX_EPOCH).unwrap();
    let mut aged = 0;
    for path in journal_files(cache_dir) {
        let text = std::fs::read_to_string(&path).unwrap();
        let mut out = String::new();
        for line in text.lines() {
            let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
            if v["cache_key"] == KEY && v["operation"] == "Add" {
                v["timestamp"] = serde_json::json!({
                    "secs_since_epoch": since.as_secs(),
                    "nanos_since_epoch": since.subsec_nanos(),
                });
                aged += 1;
            }
            out.push_str(&v.to_string());
            out.push('\n');
        }
        std::fs::write(&path, out).unwrap();
    }
    assert_eq!(
        aged, 1,
        "fixture: the fill's one Add was stamped an hour ago"
    );
    filetime::set_file_mtime(
        bin,
        filetime::FileTime::from_system_time(add_time - Duration::from_secs(1)),
    )
    .unwrap();
}

/// Held by every test in this file that consolidates, so the counter test's before/after window
/// holds no other increment from this file. Other files of the `group_readpath` binary do not
/// take it.
static COUNTER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Run one consolidation of `KEY`.
async fn consolidate_key(manager: &CacheManager) {
    manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .consolidate_object(KEY)
        .await
        .expect("consolidation");
}

/// Retiring an identity-less `Add` unlinks the unlisted `.bin` it wrote, so
/// a retired 2.x backlog does not wait a day or more for the orphan rotation. The `Add` and the
/// file are aged together, the file a moment older than its `Add`, as a real fill writes them.
/// Red sides: the retire leaving every `.bin` in place; the `Add`-relative comparison of an earlier fix.
#[tokio::test]
async fn retiring_an_identity_less_add_unlinks_its_own_unlisted_file() {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    strip_journal_identity(dir.path());
    age_fill(dir.path(), &bin);
    let before = s3_proxy::journal_consolidator::retired_fill_files_unlinked();
    assert!(bin.exists(), "fixture: the fill's file is published");
    consolidate_key(&manager).await;
    assert!(
        read_meta(&manager).is_none(),
        "the retired Add must not create a .meta"
    );
    assert!(
        !bin.exists(),
        "THE RETIRED IDENTITY-LESS ADD'S OWN UNLISTED .bin WAS LEFT IN PLACE: {:?}",
        bin
    );
    assert_eq!(
        s3_proxy::journal_consolidator::retired_fill_files_unlinked(),
        before + 1,
        "atomic_metadata.retired_fill_files_unlinked did not rise by exactly one from {}",
        before
    );
}

/// A fill whose file was modified in the last 600 s is kept, so the unlink
/// waits for the orphan rotation. Red side: the age check removed.
#[tokio::test]
async fn retiring_an_identity_less_add_keeps_a_file_younger_than_the_margin() {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    strip_journal_identity(dir.path());
    consolidate_key(&manager).await;
    assert!(
        bin.exists(),
        "a retired fill's file younger than the margin was unlinked: {:?}",
        bin
    );
}
/// Control: a file renamed over the path after the `Add` was journalled, as by a later fill
/// whose `Add` the cycle did not read, is kept. Red side: the time guard removed.
#[tokio::test]
async fn retiring_an_identity_less_add_keeps_a_file_renamed_over_it_later() {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    strip_journal_identity(dir.path());
    let tmp = bin.with_extension("later");
    std::fs::write(&tmp, version_bytes(V2)).unwrap();
    filetime::set_file_mtime(
        &tmp,
        filetime::FileTime::from_system_time(
            std::time::SystemTime::now() + Duration::from_secs(3600),
        ),
    )
    .unwrap();
    std::fs::rename(&tmp, &bin).unwrap();
    consolidate_key(&manager).await;
    assert!(
        bin.exists(),
        "a file renamed over the path after the retired Add was journalled was unlinked: {:?}",
        bin
    );
}

/// A retired identity-less `Add` whose path the key's `.meta` lists keeps
/// its file, which a listing serves. Red side: the `.meta` guard removed.
#[tokio::test]
async fn retiring_an_identity_less_add_keeps_a_file_the_meta_lists() {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    let journal = journal_files(dir.path()).remove(0);
    let saved = std::fs::read_to_string(&journal).unwrap();
    consolidate_key(&manager).await;
    let listed = read_meta(&manager).map(|m| m.ranges.len()).unwrap_or(0);
    assert_eq!(listed, 1, "fixture: the first cycle lists the fill's file");
    backdate(&bin);
    std::fs::write(&journal, saved).unwrap();
    strip_journal_identity(dir.path());
    consolidate_key(&manager).await;
    assert!(
        bin.exists(),
        "a retired Add's file that the .meta lists was unlinked: {:?}",
        bin
    );
}
/// Another entry of the batch that is not a retired `Add` names the path
/// (here the same fill's `Add` with its identity), so the file is kept. Red side: the batch guard
/// removed.
#[tokio::test]
async fn retiring_an_identity_less_add_keeps_a_file_another_entry_names() {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    let journal = journal_files(dir.path()).remove(0);
    let text = std::fs::read_to_string(&journal).unwrap();
    let mut out = String::new();
    let mut doubled = 0;
    for line in text.lines() {
        let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
        if v["cache_key"] == KEY && v["operation"] == "Add" {
            out.push_str(line);
            out.push('\n');
            v["range_spec"]
                .as_object_mut()
                .unwrap()
                .remove("file_identity");
            out.push_str(&v.to_string());
            out.push('\n');
            doubled += 1;
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    assert_eq!(doubled, 1, "fixture: the fill's Add is journalled twice");
    std::fs::write(&journal, out).unwrap();
    backdate(&bin);
    consolidate_key(&manager).await;
    assert!(
        bin.exists(),
        "a retired Add's file that another entry of the batch names was unlinked: {:?}",
        bin
    );
}
/// A file renamed over the path with a time 300 s before now (a peer fill
/// renamed in before this `Add` was journalled, or a clock skew) is inside the 600 s age margin
/// and kept. Red side: the age check removed.
#[tokio::test]
async fn retiring_an_identity_less_add_keeps_a_file_renamed_over_it_within_the_margin() {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    strip_journal_identity(dir.path());
    let tmp = bin.with_extension("skewed");
    std::fs::write(&tmp, version_bytes(V2)).unwrap();
    filetime::set_file_mtime(
        &tmp,
        filetime::FileTime::from_system_time(
            std::time::SystemTime::now() - Duration::from_secs(300),
        ),
    )
    .unwrap();
    std::fs::rename(&tmp, &bin).unwrap();
    consolidate_key(&manager).await;
    assert!(
        bin.exists(),
        "a file renamed over the path within the margin was unlinked: {:?}",
        bin
    );
}
/// The key's lines in this cache's journal files.
fn journal_lines_of_key(cache_dir: &Path) -> usize {
    journal_files(cache_dir)
        .iter()
        .map(|p| {
            std::fs::read_to_string(p)
                .unwrap()
                .lines()
                .filter(|l| {
                    serde_json::from_str::<serde_json::Value>(l)
                        .is_ok_and(|v| v["cache_key"] == KEY)
                })
                .count()
        })
        .sum()
}

/// One consolidation cycle, discovery to cleanup, removes a retired
/// identity-less `Add`'s line from the journal file. Red side: the retire not handing the entry
/// to cleanup, so it stays as if pending.
#[tokio::test]
async fn a_cycle_removes_a_retired_identity_less_add_from_the_journal() {
    let dir = TempDir::new().unwrap();
    let manager = setup(dir.path());
    let _counter = COUNTER_LOCK.lock().await;
    let _bin = fill(&manager, dir.path(), V1, ETAG_V1).await;
    strip_journal_identity(dir.path());
    assert_eq!(
        journal_lines_of_key(dir.path()),
        1,
        "fixture: the fill's one Add is journalled"
    );
    let result = manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    let left = journal_lines_of_key(dir.path());
    println!(
        "keys_processed={}, keys_skipped={}, entries_consolidated={}, the key's \
         journal lines after the cycle {}",
        result.keys_processed, result.keys_skipped, result.entries_consolidated, left
    );
    assert!(
        result.keys_processed >= 1,
        "fixture: the cycle must process the key"
    );
    assert!(
        read_meta(&manager).is_none(),
        "the retired Add must not create a .meta"
    );
    assert_eq!(
        left, 0,
        "THE RETIRED IDENTITY-LESS ADD IS STILL IN THE JOURNAL AFTER A CYCLE ({} \
         line(s))",
        left
    );
}
