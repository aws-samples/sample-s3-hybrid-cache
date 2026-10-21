//! `ram-tier-effectiveness:R1.2` — a Metadata_Cache entry is not authoritative for the
//! **absence** of cached ranges. Written for `tasks.md` task 8.
//!
//! # The standing property, and how it differs from task 6/7's
//!
//! Task 6 removed the *cause* of the live violation: no empty-`ranges` document reaches the
//! RAM metadata tier on any path any more, and
//! `tests/ram_metadata_masking_regression_test.rs` guards that. This file covers what is
//! left, which task 6 cannot touch — a resident entry with a **non-empty** `ranges` that
//! does not cover *this* request. Nothing about admission prevents that state; time creates
//! it. Inside one `metadata_cache.refresh_interval` another instance's consolidation can
//! extend the shared `.meta` with ranges whose `.bin` files are already on this volume, and
//! a verdict taken from the resident snapshot reports a miss that disk would refuse.
//!
//! Cost defect only. The bytes served are correct either way; what is wrong is that the
//! proxy refetches from S3 data it already holds. An assertion on response correctness
//! would pass against the defect, which is why the predicate below is the coverage verdict.
//!
//! # THE PREDICATE, named as a branch
//!
//! `RangeHandler::find_cached_ranges`' `!overlap.can_serve_from_cache` guard
//! (`src/range_handler.rs`), where `can_serve_from_cache` is
//! `missing_ranges.is_empty()` computed in `overlap_from_lookup`. That is the value the
//! production callers branch on — `Ok(overlap) if overlap.can_serve_from_cache` on the
//! full-object GET path, `is_serveable_unvalidated()` on the serve paths — and it is what
//! `assert!(covered)` below reads, through `RangeOverlap::has_complete_coverage()`.
//!
//! Deliberately NOT asserted, each for a reason:
//!
//! - **The shared `.meta`'s contents.** Read only as a fixture precondition. The defect
//!   leaves it perfectly correct, so asserting on it cannot fail.
//! - **A hit/miss counter.** `MetadataCache`'s counters move on every call and are
//!   per-instance; a rate is a poor predicate for a coverage question
//!   (`cache-coherency-invariants.md` § "Metrics are per-instance too").
//! - **Bytes served.** Correct in both the defective and the fixed case.
//!
//! # Why the divergence has to be CROSS-INSTANCE
//!
//! The cache coherency invariants: a fixture
//! where both instances hold the same view passes whatever the code does. The peer's
//! publishes here go through a **second `DiskCacheManager`** over the same cache directory
//! — the construction `tests/ram_metadata_masking_regression_test.rs` and
//! `tests/range_invalidation_divergent_instance_test.rs` established — never through the
//! reader's `CacheManager`. That manager holds no `MetadataCache` and no handle on the
//! reader's, so its `.meta` rename is the only thing the reader can observe, which is
//! exactly what a peer's consolidation looks like from here. `peer_publish` asserts the
//! reader's RAM tier was **not** touched across it, so a future edit that reached for the
//! local publish path fails loudly instead of quietly under-verifying.
//!
//! # Landing on the UNCOVERED branch, asserted rather than assumed
//!
//! A fixture built from the ordinary case is covered on the first check and never reaches
//! the confirmation at all, in which case both sides of a red/green pair are green. So the
//! subject's resident document holds range `0..=4095` of an 8192-byte object and the
//! request is the whole object: the first coverage computation is missing `4096..=8191` by
//! construction. The BEFORE reading asserts that verdict, and `resident_ranges` asserts the
//! resident document is the one-range snapshot at the moment the assertion is taken — not a
//! re-read of the extended disk document, which is the way this test could pass for a
//! timing reason. `MetadataCacheEntry::touch` updates `last_accessed` only, so reading the
//! entry does not extend its `loaded_at`-anchored freshness window.
//!
//! # The covered branch is guarded too, and that guard is not a discriminator
//!
//! `a_covered_request_reads_no_shared_metadata` deletes the shared `.meta` outright and
//! asserts a covered request still reports coverage. Any `.meta` read on that branch — the
//! thing the v1.9.4 fix rejected and this change must not reintroduce — would find the
//! file absent and the verdict would change. It passes before and after this change, by
//! design: it is a pin on the covered branch's read count, labelled SANITY for the same
//! reason a fleet test is, and it must not be read as evidence that the confirmation works.
//!
//! # Red side
//!
//! The investigation record carries the marker, the `grep -c`
//! proving the defect was in the tree the compiler read, the `pgrep -fl cargo` checks at
//! apply and at read, and the verbatim failure output.
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeLookupPurpose};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::RwLock;

/// The subject. The peer publishes its first half, the reader caches that view in RAM, then
/// the peer publishes the second half.
const SUBJECT_KEY: &str = "rt8-absence-bucket/half-then-whole.bin";
/// The covered-branch control, fully covered from the start.
const COVERED_KEY: &str = "rt8-absence-bucket/fully-covered.bin";

const OBJECT_LEN: u64 = 8192;
const FIRST_HALF_END: u64 = 4095;

/// Deliberately not the empty object's MD5 — `has_cached_ranges` refuses an entry claiming
/// that ETag alongside a positive length, which would make the readings below vacuous.
const ETAG: &str = "\"rt8-etag-v1\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

const TTL: Duration = Duration::from_secs(3600);

struct Fixture {
    _dir: TempDir,
    /// Instance A: the reader. Owns the RAM metadata tier under test.
    reader: Arc<CacheManager>,
    /// Instance B: the peer. A separate manager over the same cache directory, with no
    /// `MetadataCache` of its own and no handle on the reader's.
    peer: DiskCacheManager,
    range_handler: RangeHandler,
}

async fn disk_instance(cache_dir: &Path) -> DiskCacheManager {
    // Compression off so `.bin` size arithmetic never depends on a ratio.
    let manager = DiskCacheManager::new(cache_dir.to_path_buf(), false, 1024, false, 1_048_576);
    manager
        .initialize()
        .await
        .expect("disk cache manager must initialize over the shared cache directory");
    manager
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let cache_dir: PathBuf = dir.path().to_path_buf();

        let reader = Arc::new(CacheManager::new_with_defaults(cache_dir.clone(), false, 0));
        let reader_disk = Arc::new(RwLock::new(disk_instance(&cache_dir).await));
        let range_handler = RangeHandler::new(reader.clone(), reader_disk);
        let peer = disk_instance(&cache_dir).await;

        Fixture {
            _dir: dir,
            reader,
            peer,
            range_handler,
        }
    }

    /// The reader's own view of the RAM metadata tier: `Some` iff an entry is resident and
    /// inside `refresh_interval`.
    async fn resident(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        self.reader.get_metadata_cache().get(cache_key).await
    }

    /// The resident document's range count, or `None` when nothing is resident.
    async fn resident_ranges(&self, cache_key: &str) -> Option<usize> {
        self.resident(cache_key).await.map(|m| m.ranges.len())
    }

    /// The production sequence a full-object GET takes: the metadata lookup that may answer
    /// from RAM, then the coverage verdict computed from what it returned.
    ///
    /// Returns `(complete_coverage, missing_range_count)`.
    async fn whole_object_lookup(&self, cache_key: &str) -> (bool, usize) {
        let preloaded = self
            .reader
            .get_metadata_cached(cache_key)
            .await
            .expect("metadata lookup must not error");

        let requested = RangeSpec {
            start: 0,
            end: OBJECT_LEN - 1,
        };
        let overlap = self
            .range_handler
            .find_cached_ranges(
                cache_key,
                &requested,
                None,
                preloaded.as_ref(),
                // FreshServe: stored expiry is an hour out, so a `RevalidationCandidate`
                // would let a stored-expired entry through and weaken the reading.
                RangeLookupPurpose::FreshServe,
            )
            .await
            .expect("find_cached_ranges must not error");

        (
            overlap.has_complete_coverage(),
            overlap.missing_ranges.len(),
        )
    }

    /// Instance B's publish, as a peer's consolidation appears: the shared `.meta` gains a
    /// range and the `.bin` appears, written by a manager the reader shares nothing with
    /// except the directory.
    async fn peer_publish(&mut self, cache_key: &str, start: u64, end: u64) {
        let resident_before = self.resident_ranges(cache_key).await;

        let body = vec![0xB8u8; (end - start + 1) as usize];
        self.peer
            .store_range(cache_key, start, end, &body, object_metadata(), TTL, false)
            .await
            .expect("the peer's range publish must succeed");

        // Not decoration. If a future edit routed this through the reader's own
        // `CacheManager`, the local publish would refresh or invalidate the resident copy,
        // the divergence window would never open, and this fixture would pass whatever the
        // lookup code does.
        let resident_after = self.resident_ranges(cache_key).await;
        assert_eq!(
            resident_before, resident_after,
            "DIVERGENCE: the peer's publish changed the reader's RAM metadata tier for {} \
             (before={:?} after={:?}). The publish is supposed to be cross-instance — a \
             local publish path keeps the reader coherent with disk by construction and \
             this fixture would then prove nothing",
            cache_key, resident_before, resident_after
        );
    }
}

fn object_metadata() -> ObjectMetadata {
    ObjectMetadata {
        etag: ETAG.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        content_length: OBJECT_LEN,
        content_type: Some("application/octet-stream".to_string()),
        ..Default::default()
    }
}

fn read_meta(path: &Path) -> NewCacheMetadata {
    let raw = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("the shared .meta must be readable at {:?}: {}", path, e));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("the shared .meta at {:?} must parse: {}", path, e))
}

/// Everything that has to be true before any reading below means anything. Both TTLs
/// non-zero, because `MetadataTtls::forbids_admission` refuses admission outright when
/// neither consumer may use the entry — and under that condition a document that fails to
/// become resident is not this fixture's divergence, it is the admission rule.
async fn assert_apparatus(f: &Fixture) {
    for key in [SUBJECT_KEY, COVERED_KEY] {
        assert!(
            !f.reader.get_effective_get_ttl(key).await.is_zero(),
            "APPARATUS: resolved get_ttl for {} is zero, so admission would be refused for \
             a reason unrelated to this test",
            key
        );
        assert!(
            !f.reader.get_effective_head_ttl(key).await.is_zero(),
            "APPARATUS: resolved head_ttl for {} is zero, so admission would be refused \
             for a reason unrelated to this test",
            key
        );
    }
}

/// `ram-tier-effectiveness:R1.2`. THE guard, and the only discriminator in this file.
///
/// A resident entry covering half the object must not be authoritative for the absence of
/// the other half once a peer has published it to the shared `.meta`.
#[tokio::test]
async fn range_lookup_confirms_absence_against_shared_storage_before_reporting_a_miss() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;

    // The peer publishes the first half only.
    f.peer_publish(SUBJECT_KEY, 0, FIRST_HALF_END).await;
    let meta_path = f.reader.get_new_metadata_file_path(SUBJECT_KEY);
    assert_eq!(
        read_meta(&meta_path).ranges.len(),
        1,
        "FIXTURE: the peer's first publish must leave exactly one range on the shared .meta"
    );

    // The BEFORE reading. This call is also what makes the one-range document resident:
    // `get_metadata_cached` misses RAM, reads disk, and admits it (the ranges are
    // non-empty, so `ram_admission_permitted` allows it).
    let (before_covered, before_missing) = f.whole_object_lookup(SUBJECT_KEY).await;
    assert!(
        !before_covered,
        "BEFORE: a document holding only 0..={} must not report complete coverage of a \
         whole-object request for {} bytes — without this the reading after the peer's \
         second publish is not evidence that anything changed, and the fixture would never \
         reach the uncovered branch the requirement is about",
        FIRST_HALF_END, OBJECT_LEN
    );
    assert_eq!(
        before_missing,
        1,
        "BEFORE: exactly one gap ({}..={}) must be reported",
        FIRST_HALF_END + 1,
        OBJECT_LEN - 1
    );
    assert_eq!(
        f.resident_ranges(SUBJECT_KEY).await,
        Some(1),
        "APPARATUS: the one-range document was NOT admitted to the RAM metadata tier, so \
         the verdict below would be taken from disk and this fixture cannot distinguish the \
         confirmation from an ordinary disk read"
    );

    // The peer's consolidation appears on the shared `.meta`. Nothing tells this instance.
    f.peer_publish(SUBJECT_KEY, FIRST_HALF_END + 1, OBJECT_LEN - 1)
        .await;
    let on_disk = read_meta(&meta_path);
    assert_eq!(
        on_disk.ranges.len(),
        2,
        "FIXTURE: the peer's second publish must extend the shared .meta to two ranges, or \
         there is nothing for the confirmation to find"
    );

    // The divergence window must still be open, or the assertion below could pass because
    // the resident entry went stale and was re-read from disk — a timing pass, not a
    // verdict. Asserted immediately before the reading, and reading the entry does not
    // extend its freshness (`touch` moves `last_accessed`, `is_stale` reads `loaded_at`).
    assert_eq!(
        f.resident_ranges(SUBJECT_KEY).await,
        Some(1),
        "APPARATUS: the resident entry is no longer the one-range snapshot — either \
         metadata_cache.refresh_interval elapsed mid-test or something invalidated it, so \
         the reading below cannot distinguish the confirmation from a disk re-read. Re-run"
    );

    // THE ASSERTION.
    let (covered, missing) = f.whole_object_lookup(SUBJECT_KEY).await;
    assert!(
        covered,
        "R1.2 VIOLATION: the shared .meta carries {} range(s) covering the whole object, \
         and the range lookup still reports {} missing range(s). The verdict was taken from \
         a resident RAM metadata document holding only 0..={}, which is not authoritative \
         for the ABSENCE of ranges — so this GET refetches from S3 bytes whose .bin files \
         are already on this volume. RangeHandler::find_cached_ranges must confirm a \
         not-covered verdict against Shared_Storage before classifying the request a miss",
        on_disk.ranges.len(),
        missing,
        FIRST_HALF_END
    );

    // Convergence. The resident copy is behind the shared `.meta` and is the only reason
    // the request was about to be classified a miss, so the confirmation drops it — without
    // that, every request inside the remaining refresh_interval pays the confirmation again.
    assert_eq!(
        f.resident_ranges(SUBJECT_KEY).await,
        None,
        "R1.2: the stale resident entry survived a confirmation that contradicted it, so \
         every subsequent lookup inside refresh_interval re-reads the same superseded \
         document and repeats the confirmation"
    );
}

/// SANITY, not a discriminator: it passes with or without the R1.2 confirmation, and says
/// so here so that a green result is not read as evidence the confirmation works.
///
/// What it does pin is the covered branch's shared-storage read count. The v1.9.4 fix
/// rejected re-reading the `.meta` on every GET, so the confirmation must sit strictly
/// inside the not-covered branch. With the shared `.meta` deleted, any read on the covered
/// branch finds it absent and the verdict changes — so this fails if a future edit hoists
/// the confirmation, or adds any other `.meta` read, ahead of the coverage computation.
#[tokio::test]
async fn a_covered_request_reads_no_shared_metadata() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;

    f.peer_publish(COVERED_KEY, 0, OBJECT_LEN - 1).await;

    // Make the covering document resident.
    let (warm_covered, _) = f.whole_object_lookup(COVERED_KEY).await;
    assert!(
        warm_covered,
        "APPARATUS: a document covering the whole object must report complete coverage"
    );
    assert_eq!(
        f.resident_ranges(COVERED_KEY).await,
        Some(1),
        "APPARATUS: the covering document was not admitted to the RAM metadata tier, so \
         the reading below would come from disk and pin nothing"
    );

    // Remove the only shared-storage copy. A serve would now fail; the lookup verdict is
    // what is under test, and it must be reached without consulting this file.
    let meta_path = f.reader.get_new_metadata_file_path(COVERED_KEY);
    std::fs::remove_file(&meta_path).expect("the shared .meta must be removable");
    assert!(
        !meta_path.exists(),
        "FIXTURE: the shared .meta is still present, so its absence cannot be the signal"
    );

    let (covered, missing) = f.whole_object_lookup(COVERED_KEY).await;
    assert!(
        covered,
        "COVERED-BRANCH READ COUNT: a request fully covered by the resident metadata \
         document reported {} missing range(s) after the shared .meta was deleted, which \
         means the covered branch consulted Shared_Storage. v1.9.4 rejected \
         exactly that, and ram-tier-effectiveness:R1.2's confirmation is required to sit \
         inside the not-covered branch only",
        missing
    );
    assert_eq!(
        f.resident_ranges(COVERED_KEY).await,
        Some(1),
        "COVERED-BRANCH: the resident entry was invalidated by a covered lookup. Only a \
         confirmation that contradicts a resident document may drop it"
    );
}
