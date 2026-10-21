//! MULTIPART-ETAG — a cached entry whose ETag differs from the request's validator
//! ONLY in surrounding quotes is the same object and must not be invalidated.
//!
//! # The defect
//!
//! CompleteMultipartUpload entries were stored with their ETag quotes stripped (`abc-2`),
//! while S3 and every other cache writer use the quoted form (`"abc-2"`). The comparison
//! sites below compared byte-wise, so the first HEAD, the first conditional GET and every
//! TTL revalidation of a multipart entry read as a version change and discarded a correct
//! entry. The writer now stores the quoted form, but an upgraded fleet still holds entries
//! written the old way, so each comparison site tolerates one balanced pair of quotes
//! through `revalidation::etags_equal_ignoring_quotes`.
//!
//! # The predicate, per test
//!
//! Every positive test asserts the DECISION the comparison drives, never the bytes: the
//! defect returns correct bytes too, because it refetches.
//!
//! - HEAD refresh (`CacheManager::update_metadata_head_fields`): `etag_changed` clears
//!   `ranges` and sets `expires_at = now`. Asserted on the published `.meta`'s `ranges`.
//! - Range lookup (`RangeHandler::find_cached_ranges`, the `ETag mismatch` arm): mismatch
//!   returns `RangeOverlap::all_missing` and invalidates. Asserted on
//!   `has_complete_coverage()` and on the `.bin` still being on disk.
//! - Shared-document re-check (`confirm_absence_against_shared_storage`, R1.2): mismatch
//!   returns `None`, keeping the caller's miss. Asserted on `has_complete_coverage()`.
//!
//! Each fixture's two ETags differ ONLY in quoting: both quoted or both bare passes with
//! the defect present. Each positive has a content-differs negative (`"abc-2"` vs
//! `abd-2`), the guard that quote tolerance never becomes a permissive comparison.
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CacheMetadata, NewCacheMetadata, ObjectMetadata, RangeLookupPurpose};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::sync::RwLock;

const OBJECT_LEN: u64 = 8192;
const HALF_END: u64 = 4095;
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";
const TTL: Duration = Duration::from_secs(3600);

/// An old on-disk multipart entry: stored unquoted.
const STORED_UNQUOTED: &str = "3858f62230ac3c915f300c664312c11f-2";
/// The same ETag as S3 sends it on HEAD/GET.
const ORIGIN_QUOTED: &str = "\"3858f62230ac3c915f300c664312c11f-2\"";
/// A different object whose ETag ALSO differs in quoting: must still read as changed.
const ORIGIN_DIFFERENT_QUOTED: &str = "\"3858f62230ac3c915f300c664312c11e-2\"";

fn object_metadata(etag: &str) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        content_length: OBJECT_LEN,
        content_type: Some("application/octet-stream".to_string()),
        ..Default::default()
    }
}

async fn disk_instance(cache_dir: &Path) -> DiskCacheManager {
    let manager = DiskCacheManager::new(cache_dir.to_path_buf(), false, 1024, false, 1_048_576);
    manager
        .initialize()
        .await
        .expect("disk cache manager must initialize");
    manager
}

struct Fixture {
    _dir: TempDir,
    cache_dir: PathBuf,
    manager: Arc<CacheManager>,
    /// The writer of the stored entry: a separate manager over the same directory, so the
    /// `.meta` it publishes is exactly what an old on-disk entry looks like to the reader.
    writer: DiskCacheManager,
    range_handler: RangeHandler,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let cache_dir = dir.path().to_path_buf();
        let manager = Arc::new(CacheManager::new_with_defaults(cache_dir.clone(), false, 0));
        let reader_disk = Arc::new(RwLock::new(disk_instance(&cache_dir).await));
        let range_handler = RangeHandler::new(manager.clone(), reader_disk);
        let writer = disk_instance(&cache_dir).await;
        Fixture {
            _dir: dir,
            cache_dir,
            manager,
            writer,
            range_handler,
        }
    }

    async fn store(&mut self, key: &str, start: u64, end: u64, etag: &str) {
        let body = vec![0x5Au8; (end - start + 1) as usize];
        self.writer
            .store_range(key, start, end, &body, object_metadata(etag), TTL, false)
            .await
            .expect("fixture: the range store must succeed");
    }

    fn read_meta(&self, key: &str) -> NewCacheMetadata {
        let path = self.manager.get_new_metadata_file_path(key);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("the .meta must be readable at {path:?}: {e}"));
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("the .meta must parse: {e}"))
    }

    fn bin_count(&self) -> usize {
        fn walk(dir: &Path, acc: &mut usize) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for e in entries.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        walk(&p, acc);
                    } else if p.extension().and_then(|s| s.to_str()) == Some("bin") {
                        *acc += 1;
                    }
                }
            }
        }
        let mut n = 0;
        walk(&self.cache_dir.join("ranges"), &mut n);
        n
    }

    async fn whole_object_lookup(
        &self,
        key: &str,
        current_etag: &str,
        preloaded: Option<&NewCacheMetadata>,
    ) -> bool {
        self.range_handler
            .find_cached_ranges(
                key,
                &RangeSpec {
                    start: 0,
                    end: OBJECT_LEN - 1,
                },
                Some(current_etag),
                preloaded,
                RangeLookupPurpose::FreshServe,
            )
            .await
            .expect("find_cached_ranges must not error")
            .has_complete_coverage()
    }
}

fn head_response(etag: &str) -> CacheMetadata {
    CacheMetadata {
        etag: etag.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        content_length: OBJECT_LEN,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    }
}

// ───────────────────────── HEAD refresh: `etag_changed` ─────────────────────────

async fn head_refresh_ranges_after(head_etag: &str) -> usize {
    let mut f = Fixture::new().await;
    let key = "scn1-bucket/head-refresh.bin";
    f.store(key, 0, OBJECT_LEN - 1, STORED_UNQUOTED).await;
    let before = f.read_meta(key);
    assert_eq!(
        before.object_metadata.etag, STORED_UNQUOTED,
        "FIXTURE: the stored entry must carry the UNQUOTED ETag, or it is not the old shape"
    );
    assert_eq!(before.ranges.len(), 1, "FIXTURE: one range must be cached");

    f.manager
        .store_head_cache_entry_unified(key, HashMap::new(), head_response(head_etag))
        .await
        .expect("the HEAD store must succeed");

    f.read_meta(key).ranges.len()
}

#[tokio::test]
async fn head_refresh_does_not_invalidate_an_unquoted_multipart_entry() {
    let ranges_after = head_refresh_ranges_after(ORIGIN_QUOTED).await;
    assert_eq!(
        ranges_after, 1,
        "HEAD refresh treated {ORIGIN_QUOTED} vs stored {STORED_UNQUOTED} as an object \
         change and cleared the cached ranges. They differ only in quoting, so this discards \
         a correct multipart entry on its first HEAD after upgrade"
    );
}

#[tokio::test]
async fn head_refresh_still_invalidates_when_content_differs() {
    let ranges_after = head_refresh_ranges_after(ORIGIN_DIFFERENT_QUOTED).await;
    assert_eq!(
        ranges_after, 0,
        "HEAD refresh kept ranges for {ORIGIN_DIFFERENT_QUOTED} vs stored {STORED_UNQUOTED}; \
         the opaque tags differ, so this serves a changed object from cache"
    );
}

// ───────────────────── Range lookup: the `ETag mismatch` arm ─────────────────────

async fn range_lookup_after(current_etag: &str) -> (bool, usize) {
    let mut f = Fixture::new().await;
    let key = "scn1-bucket/range-lookup.bin";
    f.store(key, 0, OBJECT_LEN - 1, STORED_UNQUOTED).await;
    assert_eq!(f.bin_count(), 1, "FIXTURE: one .bin must be on disk");
    // No preloaded metadata, so the lookup reads the `.meta` and the R1.2 re-check (which
    // requires a preloaded document) cannot run and confound this arm.
    let covered = f.whole_object_lookup(key, current_etag, None).await;
    (covered, f.bin_count())
}

#[tokio::test]
async fn range_lookup_does_not_invalidate_an_unquoted_multipart_entry() {
    let (covered, bins) = range_lookup_after(ORIGIN_QUOTED).await;
    assert!(
        covered,
        "find_cached_ranges treated request validator {ORIGIN_QUOTED} vs stored \
         {STORED_UNQUOTED} as an ETag mismatch and reported no coverage. They differ only \
         in quoting"
    );
    assert_eq!(
        bins, 1,
        "the ETag-mismatch arm invalidated the entry's range file"
    );
}

#[tokio::test]
async fn range_lookup_still_invalidates_when_content_differs() {
    let (covered, _bins) = range_lookup_after(ORIGIN_DIFFERENT_QUOTED).await;
    assert!(
        !covered,
        "find_cached_ranges reported coverage for {ORIGIN_DIFFERENT_QUOTED} against stored \
         {STORED_UNQUOTED}; the opaque tags differ"
    );
}

// ─────────────── R1.2 shared-document re-check (`confirm_absence…`) ───────────────

/// The preloaded (RAM) document carries the request's own quoted ETag and half the object,
/// so the first ETag check passes and the first coverage verdict is a miss. The shared
/// `.meta` covers the whole object under the UNQUOTED ETag. Only the R1.2 re-check stands
/// between that miss and a hit, so this isolates its ETag comparison.
async fn shared_recheck_covered(shared_etag: &str) -> bool {
    let mut f = Fixture::new().await;
    assert!(
        f.manager.get_metadata_cache().is_enabled(),
        "APPARATUS: the metadata cache must be enabled or the R1.2 re-check never runs"
    );
    let key = "scn1-bucket/shared-recheck.bin";

    f.store(key, 0, HALF_END, shared_etag).await;
    let mut preloaded = f.read_meta(key);
    assert_eq!(
        preloaded.ranges.len(),
        1,
        "FIXTURE: one range after the first store"
    );
    preloaded.object_metadata.etag = ORIGIN_QUOTED.to_string();

    f.store(key, HALF_END + 1, OBJECT_LEN - 1, shared_etag)
        .await;
    let shared = f.read_meta(key);
    assert_eq!(
        shared.ranges.len(),
        2,
        "FIXTURE: the shared .meta must cover the whole object, or the re-check has \
         nothing to find"
    );
    assert_eq!(shared.object_metadata.etag, shared_etag);

    f.whole_object_lookup(key, ORIGIN_QUOTED, Some(&preloaded))
        .await
}

#[tokio::test]
async fn shared_recheck_accepts_an_unquoted_shared_multipart_entry() {
    assert!(
        shared_recheck_covered(STORED_UNQUOTED).await,
        "the R1.2 shared-document re-check rejected shared ETag {STORED_UNQUOTED} against \
         request validator {ORIGIN_QUOTED} and kept the miss. They differ only in quoting, so \
         this refetches from S3 bytes that are cached on this volume"
    );
}

#[tokio::test]
async fn shared_recheck_still_rejects_when_content_differs() {
    assert!(
        !shared_recheck_covered("3858f62230ac3c915f300c664312c11e-2").await,
        "the R1.2 re-check accepted a shared document whose opaque tag differs from the \
         request validator {ORIGIN_QUOTED}; that serves coverage from a superseded version"
    );
}
