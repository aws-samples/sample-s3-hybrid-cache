//! A cached range is served only if the file its read opened is the file its listing
//! describes, by `fstat` of the read's own descriptor against the listing's recorded
//! `file_identity` (length and modification time).
//!
//! Each test builds a state in which the `.bin` at a listed path is another version's file,
//! through the production writer (`DiskCacheManager::store_range`), the production consolidator
//! and the production read primitives, and checks the served bytes by their fill value:
//! version `v` stores `((i % 251) as u8) ^ v` at offset `i`, so both versions compress to the
//! same length and only the modification time can tell the files apart. Every replacement
//! file gets an explicit modification time (`PEER_MTIME`), so no case rests on two fills
//! landing in different clock ticks.
//!
//! - (a) A `.bin` replaced by another version's file under a listing for the old one:
//!   buffered (`load_range_data`), streamed (`stream_range_data`) and slice
//!   (`stream_range_data_from` at an offset, the chained and slice serves' primitive).
//! - (b) The cross-batch inversion: B's fill is journaled, A's file is renamed over B's before
//!   the consolidator takes the slot lock, and A's journal line is not in the batch, so the
//!   `.meta` lists B's identity over A's file.
//! - (c) A rename between spec resolution and open.
//! - (f) A shared `.bin` with two pending `Add`s of two versions under one (empty) ETag: the
//!   other version's file renamed over the path after the reader resolved its `Add`.
//! - (g) Two pending `Add`s whose prepare order inverts their rename order: the consolidator
//!   lists the fill whose identity is the file at the path, not the later `created_at`.
//! - (d) A pending `Add` of another version is never the spec for a head naming this version.
//!
//! Controls: a matching file is served byte-exact, and a listing with no identity is served
//! unverified, as before.
use futures::StreamExt;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{
    NewCacheMetadata, ObjectMetadata, RangeFileIdentity, RangeLookupPurpose, UploadState,
};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::RangeHandler;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const LEN: u64 = 300_000;
const V1: u8 = 0x11;
const V2: u8 = 0x22;
const ETAG_V1: &str = "\"11111111111111111111111111111111\"";
const ETAG_V2: &str = "\"22222222222222222222222222222222\"";
const KEY: &str = "test-bucket/scn1/verify.bin";
const SLICE_AT: u64 = 100_000;
const SLICE_LEN: usize = 1024;

fn version_bytes(v: u8) -> Vec<u8> {
    (0..LEN as usize).map(|i| ((i % 251) as u8) ^ v).collect()
}

/// Which version filled `bytes`, read from extent offset `offset`, or `None` for a mixture.
fn version_at(bytes: &[u8], offset: u64) -> Option<u8> {
    let first = *bytes.first()?;
    let v = first ^ ((offset as usize % 251) as u8);
    bytes
        .iter()
        .enumerate()
        .all(|(i, b)| *b == (((offset as usize + i) % 251) as u8) ^ v)
        .then_some(v)
}

fn version_of(bytes: &[u8]) -> Option<u8> {
    version_at(bytes, 0)
}

fn label(v: Option<u8>) -> Option<String> {
    v.map(|v| format!("v{:#x}", v))
}

fn setup(cache_dir: &Path) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let manager = Arc::new(CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false,
        0,
        CacheEvictionAlgorithm::TinyLFU,
    ));
    let dcm: Arc<tokio::sync::RwLock<DiskCacheManager>> = Arc::new(tokio::sync::RwLock::new(
        manager.create_configured_disk_cache_manager(),
    ));
    let rh = Arc::new(RangeHandler::new(manager.clone(), dcm));
    (manager, rh)
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

async fn fill(manager: &CacheManager, v: u8, etag: &str) {
    manager
        .create_configured_disk_cache_manager()
        .store_range(
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
}

async fn consolidate(manager: &CacheManager) {
    manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .consolidate_object(KEY)
        .await
        .expect("consolidation");
}

fn read_meta(manager: &CacheManager) -> Option<NewCacheMetadata> {
    let text = std::fs::read_to_string(manager.get_new_metadata_file_path(KEY)).ok()?;
    serde_json::from_str(&text).ok()
}

fn bin_path(cache_dir: &Path, file_path: &str) -> PathBuf {
    cache_dir.join("ranges").join(file_path)
}

fn identity_at(path: &Path) -> Option<RangeFileIdentity> {
    std::fs::metadata(path)
        .ok()
        .and_then(|m| RangeFileIdentity::of(&m))
}

fn listed_identity(spec: &s3_proxy::cache_types::RangeSpec) -> Option<RangeFileIdentity> {
    spec.file_identity
}

/// The modification time a peer's publish carries here: fixed, and years from any fill this
/// test makes, so a replacement differs from every listing by more than the match window
/// whatever the clock tick.
const PEER_MTIME: (i64, u32) = (1_000_000_000, 123_456_789);

fn peer_mtime() -> filetime::FileTime {
    filetime::FileTime::from_unix_time(PEER_MTIME.0, PEER_MTIME.1)
}

/// Give a journal-pending fill in `cache_dir` the peer's modification time, as if its writer
/// had published it then: the `.bin`'s mtime and the identity its `Add` records move together.
/// Two fills milliseconds apart can share a clock tick, and with it an identity.
fn restamp_pending_fill(cache_dir: &Path, spec: &mut s3_proxy::cache_types::RangeSpec) {
    let bin = bin_path(cache_dir, &spec.file_path);
    filetime::set_file_mtime(&bin, peer_mtime()).unwrap();
    let stamped = identity_at(&bin).expect("the restamped file's identity");
    let mut patched = 0;
    for entry in std::fs::read_dir(cache_dir.join("metadata/_journals")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("journal") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut out = String::new();
        for line in text.lines() {
            let mut v: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => {
                    out.push_str(line);
                    out.push('\n');
                    continue;
                }
            };
            let rs = &mut v["range_spec"];
            if rs["file_path"] == serde_json::Value::String(spec.file_path.clone())
                && rs["file_identity"].is_object()
            {
                rs["file_identity"] = serde_json::to_value(stamped).unwrap();
                patched += 1;
            }
            out.push_str(&v.to_string());
            out.push('\n');
        }
        std::fs::write(&path, out).unwrap();
    }
    assert_eq!(
        patched, 1,
        "fixture: the fill's one journal Add was restamped"
    );
    spec.file_identity = Some(stamped);
}

/// The `.bin` another proxy would write for version `v` of `KEY`, built in its own cache dir.
async fn foreign_bin(v: u8, etag: &str) -> (TempDir, Vec<u8>) {
    let other = TempDir::new().unwrap();
    let (manager, _) = setup(other.path());
    fill(&manager, v, etag).await;
    let spec = manager
        .create_configured_disk_cache_manager()
        .find_pending_journal_ranges(KEY, 0, LEN - 1)
        .await
        .expect("pending lookup")
        .into_iter()
        .next()
        .expect("the foreign fill is journal-pending");
    let bytes = std::fs::read(bin_path(other.path(), &spec.file_path)).expect("foreign .bin");
    (other, bytes)
}

/// Rename `bytes` over `path`, as a peer's publish does: a new file, published at
/// [`PEER_MTIME`].
fn rename_over(path: &Path, bytes: &[u8]) {
    let tmp = path.with_extension("scn1tmp");
    std::fs::write(&tmp, bytes).unwrap();
    filetime::set_file_mtime(&tmp, peer_mtime()).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

/// A buffered read never returns bytes other than the version its listing describes.
async fn assert_buffered_never_wrong(
    dcm: &DiskCacheManager,
    spec: &s3_proxy::cache_types::RangeSpec,
    listed: u8,
    what: &str,
) {
    if let Ok(bytes) = dcm.load_range_data(spec).await {
        assert_eq!(
            version_of(&bytes),
            Some(listed),
            "WRONG BYTES SERVED ({} buffered): listing describes v{:#x} (identity {:?}), served \
             {} bytes of {:?}",
            what,
            listed,
            listed_identity(spec),
            bytes.len(),
            label(version_of(&bytes))
        );
    }
}

/// Collect a stream to its end or its first error. Returns the bytes and whether it errored.
async fn drain(
    stream: impl futures::Stream<Item = s3_proxy::Result<bytes::Bytes>>,
    limit: usize,
) -> (Vec<u8>, bool) {
    let mut stream = std::pin::pin!(stream);
    let mut body = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => {
                body.extend_from_slice(&chunk);
                if body.len() >= limit {
                    break;
                }
            }
            Err(_) => return (body, true),
        }
    }
    (body, false)
}

/// A streamed read never completes with bytes other than the listed version.
async fn assert_streamed_never_completes_wrong(
    dcm: &DiskCacheManager,
    spec: &s3_proxy::cache_types::RangeSpec,
    listed: u8,
    what: &str,
) {
    let Ok(stream) = dcm.stream_range_data(spec, 65_536).await else {
        return; // a clean miss before the first byte
    };
    let (body, errored) = drain(stream, usize::MAX).await;
    if errored {
        assert!(
            (body.len() as u64) < LEN,
            "{} streamed: the error came after the whole body",
            what
        );
        return;
    }
    assert_eq!(
        version_of(&body),
        Some(listed),
        "WRONG BYTES SERVED ({} streamed): listing describes v{:#x} (identity {:?}), the stream \
         completed {} bytes of {:?} with no error",
        what,
        listed,
        listed_identity(spec),
        body.len(),
        label(version_of(&body))
    );
}

/// A slice (a sub-range of the extent, the chained and slice serves' read) never completes
/// with bytes other than the listed version.
async fn assert_slice_never_wrong(
    dcm: &DiskCacheManager,
    spec: &s3_proxy::cache_types::RangeSpec,
    listed: u8,
    what: &str,
) {
    let Ok((stream, skipped)) = dcm.stream_range_data_from(spec, 65_536, SLICE_AT).await else {
        return;
    };
    let need = (SLICE_AT - skipped) as usize + SLICE_LEN;
    let (body, errored) = drain(stream, need).await;
    if errored || body.len() < need {
        return; // short of the slice: the client's body ends early
    }
    let slice = &body[(SLICE_AT - skipped) as usize..need];
    assert_eq!(
        version_at(slice, SLICE_AT),
        Some(listed),
        "WRONG BYTES SERVED ({} slice {}+{}): listing describes v{:#x} (identity {:?}), served \
         {:?}",
        what,
        SLICE_AT,
        SLICE_LEN,
        listed,
        listed_identity(spec),
        label(version_at(slice, SLICE_AT))
    );
}

/// (a) state: v1 listed with its identity, then v2's file renamed over the path.
async fn replaced_bin_state(
    cache_dir: &Path,
) -> (Arc<CacheManager>, s3_proxy::cache_types::RangeSpec, TempDir) {
    let (manager, _) = setup(cache_dir);
    fill(&manager, V1, ETAG_V1).await;
    consolidate(&manager).await;
    let meta = read_meta(&manager).expect("v1 .meta");
    let spec = meta.ranges[0].clone();
    let path = bin_path(cache_dir, &spec.file_path);
    assert_eq!(
        listed_identity(&spec),
        identity_at(&path),
        "fixture: the listing records the identity of the file its writer published"
    );
    let (other, v2_bin) = foreign_bin(V2, ETAG_V2).await;
    assert_eq!(
        v2_bin.len() as u64,
        spec.compressed_size,
        "fixture: both versions are the same length, so only the modification time tells \
         them apart"
    );
    rename_over(&path, &v2_bin);
    (manager, spec, other)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_bin_is_never_served_buffered() {
    let temp = TempDir::new().unwrap();
    let (manager, spec, _other) = replaced_bin_state(temp.path()).await;
    let dcm = manager.create_configured_disk_cache_manager();
    assert_buffered_never_wrong(&dcm, &spec, V1, "(a)").await;
    let meta = read_meta(&manager);
    assert!(
        meta.is_none_or(|m| m.ranges.is_empty()),
        "(a) buffered: the refused range must be removed from its listing"
    );
    assert!(
        !bin_path(temp.path(), &spec.file_path).exists(),
        "(a) buffered: the refused .bin must be removed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_bin_is_never_served_streamed() {
    let temp = TempDir::new().unwrap();
    let (manager, spec, _other) = replaced_bin_state(temp.path()).await;
    let dcm = manager.create_configured_disk_cache_manager();
    assert_streamed_never_completes_wrong(&dcm, &spec, V1, "(a)").await;
    let meta = read_meta(&manager);
    assert!(
        meta.is_none_or(|m| m.ranges.is_empty()),
        "(a) streamed: the refused range must be removed from its listing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_bin_is_never_served_as_a_slice() {
    let temp = TempDir::new().unwrap();
    let (manager, spec, _other) = replaced_bin_state(temp.path()).await;
    let dcm = manager.create_configured_disk_cache_manager();
    assert_slice_never_wrong(&dcm, &spec, V1, "(a)").await;
}

/// (b) The schedule: B (v2) prepares and journals; A (v1) renames its
/// file over B's before the consolidator takes the slot lock; A's journal line is not in the
/// batch (A journals into its own cache dir), so the pass lists B's identity over A's file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_cross_batch_inversion_is_never_served() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, _) = setup(cache_dir);
    fill(&manager, V2, ETAG_V2).await; // B
    let pending = manager
        .create_configured_disk_cache_manager()
        .find_pending_journal_ranges(KEY, 0, LEN - 1)
        .await
        .unwrap();
    let path = bin_path(cache_dir, &pending[0].file_path);
    let (_other, v1_bin) = foreign_bin(V1, ETAG_V1).await; // A
    rename_over(&path, &v1_bin);
    consolidate(&manager).await;
    let meta = read_meta(&manager).expect("the pass publishes a .meta");
    assert_eq!(meta.object_metadata.etag, ETAG_V2, "fixture: B's version");
    let spec = meta.ranges[0].clone();
    assert_ne!(
        listed_identity(&spec),
        identity_at(&path),
        "fixture: B's identity listed over A's file"
    );
    let dcm = manager.create_configured_disk_cache_manager();
    assert_slice_never_wrong(&dcm, &spec, V2, "(b)").await;
    assert_streamed_never_completes_wrong(&dcm, &spec, V2, "(b)").await;
    assert_buffered_never_wrong(&dcm, &spec, V2, "(b)").await;
}

/// (c) The spec is resolved, then a peer renames another version over the path, then the
/// read opens it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_rename_between_resolution_and_open_is_never_served() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, _) = setup(cache_dir);
    fill(&manager, V1, ETAG_V1).await;
    consolidate(&manager).await;
    let dcm = manager.create_configured_disk_cache_manager();
    let lookup = dcm
        .find_cached_ranges(KEY, 0, LEN - 1, None, RangeLookupPurpose::FreshServe)
        .await
        .expect("lookup");
    let spec = lookup.ranges[0].clone();
    let (_other, v2_bin) = foreign_bin(V2, ETAG_V2).await;
    rename_over(&bin_path(cache_dir, &spec.file_path), &v2_bin); // the seam
    assert_buffered_never_wrong(&dcm, &spec, V1, "(c)").await;
    rename_over(&bin_path(cache_dir, &spec.file_path), &v2_bin);
    assert_streamed_never_completes_wrong(&dcm, &spec, V1, "(c)").await;
}

/// (f) Two pending `Add`s of two versions under one (empty) ETag, so the conflict rule
/// does not drop the extent; the file at the path is the other version's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_shared_bin_whose_pending_add_names_another_file_is_never_served() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, _) = setup(cache_dir);
    fill(&manager, V1, "").await;
    fill(&manager, V2, "").await;
    let dcm = manager.create_configured_disk_cache_manager();
    let pending = dcm
        .find_pending_journal_ranges(KEY, 0, LEN - 1)
        .await
        .expect("pending lookup");
    assert_eq!(pending.len(), 1, "fixture: one extent selected");
    let spec = pending[0].clone();
    let path = bin_path(cache_dir, &spec.file_path);
    let on_disk = version_of(&dcm.load_range_data(&spec).await.unwrap_or_default());
    let described = if listed_identity(&spec) == identity_at(&path) {
        on_disk.expect("fixture: the file decodes")
    } else if on_disk == Some(V1) {
        V2
    } else {
        V1
    };
    let other = if described == V1 { V2 } else { V1 };
    let (_other, other_bin) = foreign_bin(other, "").await;
    rename_over(&path, &other_bin);
    assert_buffered_never_wrong(&dcm, &spec, described, "(f)").await;
    assert_streamed_never_completes_wrong(&dcm, &spec, described, "(f)").await;
}

/// (g) Two pending `Add`s of two versions of one extent (ETag-less origin) whose prepare order
/// inverts their rename order. A (v1) prepares first on a peer; B (v2) prepares and publishes
/// here, later by `created_at`; A's rename lands last. Both journal lines are in the batch (A's
/// journal file is copied into this cache's `_journals`, as a peer's would sit there). The pass
/// must list the fill whose identity is the file at the path, and the read serve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_consolidation_lists_the_fill_whose_file_is_at_the_path() {
    // Either rename order: whichever fill the `created_at` rules would pick, one order puts
    // the other fill's file at the path.
    g_case(true).await;
    g_case(false).await;
}

async fn g_case(a_renames_last: bool) {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, _) = setup(cache_dir);
    // A, on a peer, first.
    let peer = TempDir::new_in(cache_dir.parent().unwrap()).unwrap();
    let (peer_manager, _) = setup(peer.path());
    fill(&peer_manager, V1, "").await;
    let mut a_spec = peer_manager
        .create_configured_disk_cache_manager()
        .find_pending_journal_ranges(KEY, 0, LEN - 1)
        .await
        .unwrap()
        .remove(0);
    // A's identity differs from B's by its modification time whatever the clock tick.
    restamp_pending_fill(peer.path(), &mut a_spec);
    std::thread::sleep(Duration::from_millis(20));
    // B, here, later by `created_at`.
    fill(&manager, V2, "").await;
    let dcm = manager.create_configured_disk_cache_manager();
    let path = bin_path(cache_dir, &a_spec.file_path);
    // A's journal line reaches the shared journals; A's rename lands last.
    for entry in std::fs::read_dir(peer.path().join("metadata/_journals")).unwrap() {
        let entry = entry.unwrap();
        let name = format!("peer-a-{}", entry.file_name().to_string_lossy());
        std::fs::copy(
            entry.path(),
            cache_dir.join("metadata/_journals").join(name),
        )
        .unwrap();
    }
    let (on_disk, last) = if a_renames_last {
        std::fs::rename(bin_path(peer.path(), &a_spec.file_path), &path).unwrap();
        assert_eq!(
            identity_at(&path),
            listed_identity(&a_spec),
            "fixture: A's rename lands last"
        );
        (V1, "A's, v1")
    } else {
        (V2, "B's, v2")
    };
    consolidate(&manager).await;
    let meta = read_meta(&manager).expect("the pass publishes a .meta");
    assert_eq!(meta.ranges.len(), 1, "the extent stays listed");
    let spec = meta.ranges[0].clone();
    assert_eq!(
        listed_identity(&spec),
        identity_at(&path),
        "CONSOLIDATED LISTING NAMES ANOTHER FILE ((g)): the listing records {:?}, the file at \
         the path is {:?} ({})",
        listed_identity(&spec),
        identity_at(&path),
        last
    );
    let bytes = dcm.load_range_data(&spec).await.expect("(g) a hit");
    assert_eq!(
        version_of(&bytes),
        Some(on_disk),
        "(g) the listed fill's bytes"
    );
}

/// (d) The streaming serves' `NotListed` arm takes its spec from
/// `find_pending_journal_range_for_etag` with the ETag the response head carries. A pending
/// `Add` of another version is never returned for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_pending_add_of_another_version_is_never_the_spec_for_this_label() {
    let temp = TempDir::new().unwrap();
    let (manager, _) = setup(temp.path());
    fill(&manager, V2, ETAG_V2).await;
    let dcm = manager.create_configured_disk_cache_manager();
    let for_v1 = dcm
        .find_pending_journal_range_for_etag(KEY, 0, LEN - 1, ETAG_V1)
        .await;
    assert!(
        for_v1.is_none(),
        "WRONG LABEL ((d)): a pending Add of v2 was returned for a head naming {}: {:?}",
        ETAG_V1,
        for_v1.map(|s| s.file_path)
    );
    assert!(
        dcm.find_pending_journal_range_for_etag(
            KEY,
            0,
            LEN - 1,
            "22222222222222222222222222222222"
        )
        .await
        .is_some(),
        "(d) control: the same version's Add is found (quote-insensitive)"
    );
}

/// Control: a matching file is served byte-exact on every path, and stays listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_a_matching_file_is_served_byte_exact() {
    let temp = TempDir::new().unwrap();
    let (manager, _) = setup(temp.path());
    fill(&manager, V1, ETAG_V1).await;
    consolidate(&manager).await;
    let spec = read_meta(&manager).expect(".meta").ranges[0].clone();
    assert!(spec.file_identity.is_some(), "fixture: identity recorded");
    let dcm = manager.create_configured_disk_cache_manager();
    let bytes = dcm.load_range_data(&spec).await.expect("buffered hit");
    assert!(
        bytes == version_bytes(V1),
        "buffered control not byte-exact"
    );
    let stream = dcm.stream_range_data(&spec, 65_536).await.expect("stream");
    let (body, errored) = drain(stream, usize::MAX).await;
    assert!(!errored && body == version_bytes(V1), "streamed control");
    let (stream, skipped) = dcm
        .stream_range_data_from(&spec, 65_536, SLICE_AT)
        .await
        .expect("slice");
    let need = (SLICE_AT - skipped) as usize + SLICE_LEN;
    let (body, _) = drain(stream, need).await;
    assert!(
        body[(SLICE_AT - skipped) as usize..need]
            == version_bytes(V1)[SLICE_AT as usize..SLICE_AT as usize + SLICE_LEN],
        "slice control not byte-exact"
    );
    assert_eq!(read_meta(&manager).unwrap().ranges.len(), 1, "stays listed");
}

/// Control: a listing with no recorded identity is served unverified, as before, even when
/// its file is another version's of the same length.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_a_listing_with_no_identity_is_served_unverified() {
    let temp = TempDir::new().unwrap();
    let (manager, spec, _other) = replaced_bin_state(temp.path()).await;
    let mut legacy = spec.clone();
    legacy.file_identity = None;
    let dcm = manager.create_configured_disk_cache_manager();
    let bytes = dcm.load_range_data(&legacy).await.expect("unverified read");
    assert_eq!(version_of(&bytes), Some(V2), "served as before");
}

/// A listing or part record written by an earlier 3.0 development build records its identity as
/// inode number and length. This build parses it, ignores the inode number, and records no
/// identity, so the range is unverified (served as `control_a_listing_with_no_identity_...`
/// shows). A malformed identity reads the same way: the document is never dropped for it.
#[test]
fn a_listing_with_an_inode_identity_parses_as_unverified() {
    let spec_json = |identity: &str| {
        format!(
            r#"{{"start":0,"end":9,"file_path":"b/00/000/k_0-9.bin","compression_algorithm":"Lz4","compressed_size":30,"uncompressed_size":10,"created_at":{{"secs_since_epoch":1,"nanos_since_epoch":0}},"last_accessed":{{"secs_since_epoch":1,"nanos_since_epoch":0}},"access_count":1,"file_identity":{}}}"#,
            identity
        )
    };
    for identity in [
        r#"{"ino":123456,"size":30}"#,
        r#"{"size":30}"#,
        r#""garbage""#,
        r#"{"size":"thirty","mtime_ns":5}"#,
        "null",
    ] {
        let spec: s3_proxy::cache_types::RangeSpec = serde_json::from_str(&spec_json(identity))
            .unwrap_or_else(|e| panic!("listing with identity {} must parse: {}", identity, e));
        assert!(
            spec.file_identity.is_none(),
            "identity {} must read as unverified, got {:?}",
            identity,
            spec.file_identity
        );
    }
    let part: s3_proxy::cache_types::CachedPartInfo = serde_json::from_str(
        r#"{"part_number":1,"size":10,"etag":"e","file_identity":{"ino":7,"size":30}}"#,
    )
    .expect("a part record with an inode identity parses");
    assert!(
        part.file_identity.is_none(),
        "the part's range is unverified"
    );
    // A current identity round-trips.
    let current: s3_proxy::cache_types::RangeSpec =
        serde_json::from_str(&spec_json(r#"{"size":30,"mtime_ns":1700000000123456789}"#)).unwrap();
    assert_eq!(
        current.file_identity,
        Some(RangeFileIdentity {
            size: 30,
            mtime_ns: 1_700_000_000_123_456_789
        })
    );
    let again: s3_proxy::cache_types::RangeSpec =
        serde_json::from_str(&serde_json::to_string(&current).unwrap()).unwrap();
    assert_eq!(again.file_identity, current.file_identity);
}

/// A listing written by an unreleased 3.0 development build carries `content_hash`; this build
/// parses it and ignores the field.
#[test]
fn a_listing_with_a_content_hash_field_parses_and_ignores_it() {
    let json = r#"{"start":0,"end":9,"file_path":"b/00/000/k_0-9.bin","compression_algorithm":"Lz4","compressed_size":30,"uncompressed_size":10,"created_at":{"secs_since_epoch":1,"nanos_since_epoch":0},"last_accessed":{"secs_since_epoch":1,"nanos_since_epoch":0},"access_count":1,"content_hash":"xxh64:0123456789abcdef"}"#;
    let spec: s3_proxy::cache_types::RangeSpec =
        serde_json::from_str(json).expect("a content_hash field is ignored");
    assert!(spec.file_identity.is_none());
    let out = serde_json::to_string(&spec).unwrap();
    assert!(!out.contains("file_identity") && !out.contains("content_hash"));
}
