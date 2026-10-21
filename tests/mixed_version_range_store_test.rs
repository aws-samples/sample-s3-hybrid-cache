//! A cached object must never be served with bytes from two versions.
//!
//! Reproduces Q4 (i) and (iii) at the unit level, through the production writer
//! (`DiskCacheManager::store_range`, the GET range-miss fill), the production
//! `JournalConsolidator`, and the production read path (`find_cached_ranges` +
//! `load_range_data_with_cache`).
//!
//! (i) An object is cached as v1 in two extents. One extent's `.bin` goes missing. The
//! origin changes to v2 (same length). The missing-`.bin` recovery in
//! `get_cached_range_data` refetches that extent from S3 and spawns
//! `DiskCacheManager::store_range` with S3's v2 response metadata; this test performs that same
//! call directly. After consolidation the whole object is served and every byte must come
//! from one version, with the `.meta` ETag naming that version.
//!
//! (iii) Two fills of different versions land with no `.meta` yet; consolidation must not
//! publish both under one ETag.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeLookupPurpose, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const EXTENT: u64 = 4096;
const OBJECT_LEN: u64 = 2 * EXTENT;
const ETAG_V1: &str = "\"11111111111111111111111111111111\"";
const ETAG_V2: &str = "\"22222222222222222222222222222222\"";
const BYTE_V1: u8 = 0x11;
const BYTE_V2: u8 = 0x22;

fn setup(cache_dir: &Path) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let manager = Arc::new(CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false, // RAM off: the disk document is the only store
        0,
        CacheEvictionAlgorithm::TinyLFU,
    ));
    let dcm: Arc<tokio::sync::RwLock<DiskCacheManager>> = Arc::new(tokio::sync::RwLock::new(
        manager.create_configured_disk_cache_manager(),
    ));
    let rh = Arc::new(RangeHandler::new(manager.clone(), dcm));
    (manager, rh)
}

/// What the recovery spawn builds from S3's 206 headers: total length, the response ETag.
fn version_metadata(etag: &str) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag.to_string(),
        content_length: OBJECT_LEN,
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        upload_state: UploadState::Complete,
        cumulative_size: EXTENT,
        ..Default::default()
    }
}

async fn fill(rh: &RangeHandler, start: u64, byte: u8, etag: &str) {
    rh.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            start,
            start + EXTENT - 1,
            &vec![byte; EXTENT as usize],
            version_metadata(etag),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("range fill");
}

const KEY: &str = "test-bucket/scn1/mixed.bin";

async fn consolidate(manager: &CacheManager) {
    manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .consolidate_object(KEY)
        .await
        .expect("consolidation");
}

fn meta_path(cache_dir: &Path) -> std::path::PathBuf {
    s3_proxy::disk_cache::get_sharded_path(&cache_dir.join("metadata"), KEY, ".meta")
        .expect("sharded .meta path")
}

fn read_meta(cache_dir: &Path) -> Option<NewCacheMetadata> {
    let text = std::fs::read_to_string(meta_path(cache_dir)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Serve the whole object from cache the way a mainline cache hit does: lookup with the
/// cached ETag as the only known version, then load each listed extent. Returns the
/// `.meta` ETag and, per extent, `Some(byte)` for a loaded uniform extent or `None` for a
/// miss (a missing `.bin` is a miss, which is allowed; a wrong version is not).
async fn serve_whole_object(
    cache_dir: &Path,
    manager: &CacheManager,
    rh: &RangeHandler,
) -> (String, Vec<(u64, Option<u8>)>) {
    let meta = read_meta(cache_dir).expect("a .meta must be published");
    let overlap = rh
        .find_cached_ranges(
            KEY,
            &RangeSpec {
                start: 0,
                end: OBJECT_LEN - 1,
            },
            None,
            None,
            RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("lookup");
    let mut extents = Vec::new();
    for range in &overlap.cached_ranges {
        let loaded = manager
            .load_range_data_with_cache(KEY, range, rh)
            .await
            .ok()
            .map(|(bytes, _)| {
                let first = bytes[0];
                assert!(
                    bytes.iter().all(|b| *b == first),
                    "extent {}-{} is not uniform: fixture broken",
                    range.start,
                    range.end
                );
                first
            });
        extents.push((range.start, loaded));
    }
    (meta.object_metadata.etag, extents)
}

fn assert_one_version(etag: &str, extents: &[(u64, Option<u8>)]) {
    let expected = if etag == ETAG_V1 {
        BYTE_V1
    } else if etag == ETAG_V2 {
        BYTE_V2
    } else {
        panic!("unexpected .meta etag {}", etag)
    };
    for (start, loaded) in extents {
        if let Some(byte) = loaded {
            assert_eq!(
                *byte, expected,
                "MIXED VERSION: .meta etag {} but extent at {} holds byte {:#x} (v1={:#x}, \
                 v2={:#x}); extents={:?}",
                etag, start, byte, BYTE_V1, BYTE_V2, extents
            );
        }
    }
}

/// Q4 (i): the missing-`.bin` recovery refetch stores a v2 extent under a v1 `.meta`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_extent_of_a_new_version_is_not_served_under_the_old_etag() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, rh) = setup(cache_dir);

    fill(&rh, 0, BYTE_V1, ETAG_V1).await;
    fill(&rh, EXTENT, BYTE_V1, ETAG_V1).await;
    consolidate(&manager).await;

    let meta = read_meta(cache_dir).expect("v1 .meta");
    assert_eq!(meta.object_metadata.etag, ETAG_V1, "fixture: v1 published");
    assert_eq!(meta.ranges.len(), 2, "fixture: two v1 extents listed");

    // One extent's `.bin` goes missing.
    let second = meta
        .ranges
        .iter()
        .find(|r| r.start == EXTENT)
        .expect("second extent");
    let bin = cache_dir.join("ranges").join(&second.file_path);
    std::fs::remove_file(&bin).expect("delete second extent .bin");

    // The recovery refetch: S3 now holds v2 and answers the Range GET with v2 bytes and
    // headers, which `get_cached_range_data` hands to `DiskCacheManager::store_range`.
    fill(&rh, EXTENT, BYTE_V2, ETAG_V2).await;
    // A journal entry's timestamp must be newer than the listed spec's `created_at` for
    // `resolve_conflicts` to take it; the fill above already is.
    consolidate(&manager).await;

    let (etag, extents) = serve_whole_object(cache_dir, &manager, &rh).await;
    assert_one_version(&etag, &extents);
}

/// Seed v1 as two consolidated extents and return a manager whose second extent's `.bin` is
/// still present.
async fn seed_v1(cache_dir: &Path) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    let (manager, rh) = setup(cache_dir);
    fill(&rh, 0, BYTE_V1, ETAG_V1).await;
    fill(&rh, EXTENT, BYTE_V1, ETAG_V1).await;
    consolidate(&manager).await;
    let meta = read_meta(cache_dir).expect("v1 .meta");
    assert_eq!(meta.object_metadata.etag, ETAG_V1, "fixture: v1 published");
    assert_eq!(meta.ranges.len(), 2, "fixture: two v1 extents listed");
    (manager, rh)
}

/// The hybrid `store_range` arm: a v2 fill of an extent the v1 `.meta` lists, `.bin`
/// present, must not overwrite it. Served before any consolidation, so only the arm's own
/// check can keep the v1 bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hybrid_fill_of_a_new_version_does_not_overwrite_a_listed_extent() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, rh) = seed_v1(cache_dir).await;

    fill(&rh, EXTENT, BYTE_V2, ETAG_V2).await;

    let (etag, extents) = serve_whole_object(cache_dir, &manager, &rh).await;
    assert_one_version(&etag, &extents);
}

/// The streamed arm (`commit_incremental_range`, the GET-response tee): same contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_fill_of_a_new_version_does_not_overwrite_a_listed_extent() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, rh) = seed_v1(cache_dir).await;

    let dcm = manager.create_configured_disk_cache_manager();
    let mut writer = dcm
        .begin_incremental_range_write(KEY, EXTENT, OBJECT_LEN - 1, false)
        .await
        .expect("begin")
        .expect("admitted");
    DiskCacheManager::write_range_chunk(&mut writer, &vec![BYTE_V2; EXTENT as usize])
        .expect("chunk");
    dcm.commit_incremental_range(writer, version_metadata(ETAG_V2), Duration::from_secs(3600))
        .await
        .expect("commit");

    let (etag, extents) = serve_whole_object(cache_dir, &manager, &rh).await;
    assert_one_version(&etag, &extents);
}

/// Q4 (iii): two fills of different versions with no `.meta` yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fills_of_two_versions_before_any_meta_are_not_published_together() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, rh) = setup(cache_dir);

    fill(&rh, 0, BYTE_V1, ETAG_V1).await;
    fill(&rh, EXTENT, BYTE_V2, ETAG_V2).await;
    assert!(
        read_meta(cache_dir).is_none(),
        "fixture: no .meta before consolidation"
    );
    consolidate(&manager).await;

    let (etag, extents) = serve_whole_object(cache_dir, &manager, &rh).await;
    assert_one_version(&etag, &extents);
}

/// Prepare order can invert rename order.
///
/// A range fill's `created_at` is stamped when the `.tmp` is finished, before the slot lock
/// the rename takes. A finishes page 0 as v1 (t1), B finishes page 0 as v2 (t2), B renames
/// (t3), A renames (t4): the `.bin` holds v1. B then fills page 1 as v2. With no `.meta`
/// yet, consolidation builds one from the newest entry (v2), and a decision by `created_at`
/// keeps B's page-0 listing over A's v1 bytes. Every listed extent must hold the `.meta`'s
/// version; an extent two fills in one pass disagree about must be a miss.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_order_inverting_rename_order_never_lists_an_extent_over_the_other_versions_bytes()
{
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let (manager, rh) = setup(cache_dir);
    // A: a streamed fill of page 0 as v1, finished (created_at t1) but not yet renamed.
    let dcm_a = manager.create_configured_disk_cache_manager();
    let mut writer = dcm_a
        .begin_incremental_range_write(KEY, 0, EXTENT - 1, false)
        .await
        .expect("begin")
        .expect("admitted");
    DiskCacheManager::write_range_chunk(&mut writer, &vec![BYTE_V1; EXTENT as usize])
        .expect("chunk");
    let a = dcm_a
        .prepare_incremental_commit(writer, version_metadata(ETAG_V1), Duration::from_secs(3600))
        .expect("prepare A");
    tokio::time::sleep(Duration::from_millis(20)).await;
    // B: a buffered fill of page 0 as v2, prepared (t2 > t1) and renamed first (t3).
    let mut dcm_b = manager.create_configured_disk_cache_manager();
    let b = dcm_b
        .prepare_range_store(
            KEY,
            0,
            EXTENT - 1,
            &vec![BYTE_V2; EXTENT as usize],
            version_metadata(ETAG_V2),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("prepare B")
        .expect("B admitted");
    b.finish().await.expect("B publishes");
    // A renames last (t4): the page-0 `.bin` now holds v1.
    a.finish().await.expect("A publishes");
    // B fills page 1 as v2 (t5): the newest entry, so the new `.meta` is v2.
    tokio::time::sleep(Duration::from_millis(20)).await;
    fill(&rh, EXTENT, BYTE_V2, ETAG_V2).await;
    assert!(
        read_meta(cache_dir).is_none(),
        "PRECONDITION MISSING: no .meta before consolidation"
    );
    let page0 = manager
        .create_configured_disk_cache_manager()
        .get_new_range_file_path(KEY, 0, EXTENT - 1);
    assert!(
        page0.exists(),
        "PRECONDITION MISSING: page 0 .bin published"
    );
    consolidate(&manager).await;
    let (etag, extents) = serve_whole_object(cache_dir, &manager, &rh).await;
    assert_eq!(etag, ETAG_V2, "fixture: the newest entry names the .meta");
    assert_one_version(&etag, &extents);
    assert!(
        !extents.iter().any(|(s, b)| *s == 0 && b.is_some()),
        "page 0 must read as a miss after a disagreeing pass: {:?}",
        extents
    );
    assert!(
        extents
            .iter()
            .any(|(s, b)| *s == EXTENT && *b == Some(BYTE_V2)),
        "control: page 1 (one fill, v2) must stay a hit: {:?}",
        extents
    );
    assert!(
        !page0.exists(),
        "the disagreeing page-0 .bin must be unlinked, not left unlisted"
    );
}
