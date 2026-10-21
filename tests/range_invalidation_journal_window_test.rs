//! Invalidation reaches a key's Journal_Window ranges, and does not over-report.
//!
//! `range-invalidation-completeness:R1`, the red/green pair for `RANGE-INVAL-R1`.
//!
//! # The defect
//!
//! `DiskCacheManager::remove_all_ranges` enumerated the `.bin` files to delete from
//! `self.get_metadata(cache_key)` and nothing else. `get_metadata` reads exactly one file —
//! `metadata/{bucket}/{XX}/{YYY}/{key}.meta` — and never consults `metadata/_journals/`. A
//! range whose journal `Add` entry no consolidation cycle has folded yet was therefore absent
//! from the enumeration, its `.bin` survived the invalidation, and the returned count did not
//! include it. Every downstream consumer — the read path, the size accounting, the operator
//! reading `ranges_removed=N` — treats that count as fact.
//!
//! # The PARTIAL shape, not the total one
//!
//! The fixture below has a `.meta` that **exists and is incomplete**. The 2026-08-22
//! observation of this finding was a *total* no-op measured with no consolidated `.meta` at
//! all, and that case is not reachable from any of the three production callers: the
//! ETag-mismatch arm is lexically inside `if let Some(metadata) = metadata_opt`, and both
//! proxy-injected-412 sites need a partial cache hit whose cached ranges carried an ETag,
//! which only a `.meta` supplies. A test built on the total shape would specify a case
//! customers cannot hit.
//!
//! # What makes each assertion non-vacuous
//!
//! - **The fixture is SHARDED and asserts that it is** (`:R6.2`). On a flat layout the
//!   `.meta`-derived and journal-derived enumerations resolve against the same paths and the
//!   test can pass with the defect fully present. Worse, for a full-object key that does not
//!   parse into bucket and object key the sharded constructor *falls back* to the legacy flat
//!   path, so the two genuinely coincide — the vacuity `EVICT-METAPATH-FIX` had to add a
//!   precondition assertion to prevent.
//! - **The Journal_Window is established from an OBSERVABLE SIGNAL** (`:R6.4`), not from an
//!   assumption that no consolidator is running: the `.meta` on disk is parsed and asserted
//!   *not* to list range B, while the journal file on disk is parsed and asserted to name it.
//!   No consolidator background task is started here, but the assertion does not rely on that.
//! - **The `Add` for range A sits in the journal as well as in the `.meta`**, so the two
//!   enumerations name one file and the deduplication requirement (`:R1.4`) is exercised
//!   rather than described. **Which assertion covers it is not the obvious one, and this is
//!   measured rather than argued** (`RANGE-INVAL-OWED-RED-SIDES`, red side 5): with the
//!   `candidates.retain(...)` dedup removed, `removed_count` is still **2**, not 3 — the
//!   `.meta` candidate is pushed first and unlinks the file, so the duplicate journal
//!   candidate takes the absent-file branch, which deliberately does *not* count a
//!   journal-derived absence. The count assertion therefore cannot see this defect. The
//!   assertion that goes red is `a_removal_marker_is_written_for_each_removed_range`, which
//!   observes `[(0, 1023), (0, 1023), (4096, 5119)]` against the two distinct extents owed.
//! - **The journal file belongs to a DIFFERENT instance** than the one invalidating
//!   (`:R1.3`, `:R4.1`), so the enumeration has to read a file it did not write. A fixture
//!   where one instance holds both sides passes whatever the code does.
//!
//! # Red side
//!
//! Taken by deleting the `candidates.extend(self.enumerate_journal_add_ranges(...))` call in
//! `remove_all_ranges_detailed`, confirmed present in the tree with `grep -c` before the
//! result was read, with `pgrep -fl cargo` empty at apply and at read. Pre-fix:
//! `journal_window_bin_is_removed_and_the_count_does_not_double_count` fails on
//! `removed_count` (1, not 2) and on range B's `.bin` still existing. See the unit's return.
//!
//! Three further red sides were taken by `RANGE-INVAL-OWED-RED-SIDES`, same protocol:
//!
//! - **The flattened fixture.** `KEY` changed to a key with no `/`, so `parse_cache_key` fails
//!   and both path constructors fall back to their flat form. **All three sharding clauses in
//!   `assert_preconditions` fire**, one per run with the earlier clauses neutralised in turn:
//!   the component count (1, not 4), the bucket-first component
//!   (`Some("partial-meta-object.bin.meta")`, not `Some("journal-window-bucket")`), and the
//!   `.bin` layout (`"partial-meta-object.bin_0-1023.bin"`).
//! - **The path-safety guard removed** from `enumerate_journal_add_ranges`. The
//!   `outside.exists()` assertion in `an_unsafe_journal_file_path_is_not_resolved` is the only
//!   failure, so it is evidence for that defect — see that test's own doc comment.
//! - **The dedup removed** from `remove_all_ranges_detailed`. See the bullet above for which
//!   assertion moves, and which does not.
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Parses into bucket `journal-window-bucket` and object key, which is what makes
/// `get_new_metadata_file_path` produce a sharded path rather than falling back to flat.
const KEY: &str = "journal-window-bucket/partial-meta-object.bin";

/// Range A: folded into the `.meta`, and still named by an unfolded journal `Add` as well —
/// journal cleanup lags consolidation, so this is the ordinary steady state, and it is the
/// duplicate the dedup requirement is about.
const A_START: u64 = 0;
const A_END: u64 = 1023;
/// Range B: the Journal_Window range. Named by the journal only.
const B_START: u64 = 4096;
const B_END: u64 = 5119;

const A_LEN: u64 = A_END - A_START + 1;
const B_LEN: u64 = B_END - B_START + 1;

/// The instance that performs the invalidation, and therefore the instance whose journal file
/// the `Remove` markers land in.
const INVALIDATING_INSTANCE: &str = "rir1-invalidating-instance";
/// The instance whose journal holds both `Add` entries. Deliberately not the one above.
const PEER_INSTANCE: &str = "rir1-peer-instance";

struct Fixture {
    _dir: TempDir,
    cache_dir: PathBuf,
    manager: DiskCacheManager,
    metadata_path: PathBuf,
    a_bin: PathBuf,
    b_bin: PathBuf,
}

fn relative_range_path(cache_dir: &Path, bin: &Path) -> String {
    bin.strip_prefix(cache_dir.join("ranges"))
        .expect("range file must live under cache_dir/ranges")
        .to_string_lossy()
        .to_string()
}

fn journal_add_entry(
    cache_key: &str,
    start: u64,
    end: u64,
    len: u64,
    relative_path: String,
) -> JournalEntry {
    JournalEntry {
        timestamp: SystemTime::now(),
        instance_id: PEER_INSTANCE.to_string(),
        cache_key: cache_key.to_string(),
        range_spec: RangeSpec::new(
            start,
            end,
            relative_path,
            CompressionAlgorithm::None,
            len,
            len,
        ),
        operation: JournalOperation::Add,
        // Empty, exactly as `HybridMetadataWriter::write_journal_only` writes it, with the
        // comment "Will be filled by consolidation". The enumeration under test must resolve
        // the `.bin` from `range_spec.file_path` instead; resolving from this field would
        // produce `{cache_dir}/ranges` and the path-safety guard would reject it. Leaving it
        // empty here is what makes that a real assertion rather than a coincidence.
        range_file_path: String::new(),
        metadata_version: 0,
        new_ttl_secs: None,
        object_ttl_secs: None,
        access_increment: None,
        object_metadata: None,
        fill_witness: None,
    }
}

/// Seed the partial shape: a `.meta` listing range A only, `.bin` files for A and B, and a
/// peer instance's journal holding `Add` entries for **both**.
///
/// Everything is written **directly onto the shared volume**, the way a peer's publish and a
/// peer's journal append appear. This instance's store path is never driven.
async fn seed() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let cache_dir = dir.path().to_path_buf();

    // Compression off, so `compressed_size` equals the body length and the size arithmetic
    // below is exact rather than approximate.
    let manager = DiskCacheManager::new(cache_dir.clone(), false, 1024, false, 1_048_576);
    manager.initialize().await.expect("disk cache init");

    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        INVALIDATING_INSTANCE.to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_millis(300),
        0,
    ));
    // Constructed but never started: `JournalConsolidator::new` spawns nothing, so no
    // background cycle can fold the journal entry mid-test. The R6.4 assertions below do not
    // depend on that — they read the two files — but a folded entry would make the test
    // measure something else, so not starting it keeps the fixture deterministic.
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.clone(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    ));
    let mut manager = manager;
    manager.set_journal_consolidator(consolidator);

    let a_bin = manager.get_new_range_file_path(KEY, A_START, A_END);
    let b_bin = manager.get_new_range_file_path(KEY, B_START, B_END);
    let a_relative = relative_range_path(&cache_dir, &a_bin);
    let b_relative = relative_range_path(&cache_dir, &b_bin);

    for (bin, len, fill) in [(&a_bin, A_LEN, 0xAAu8), (&b_bin, B_LEN, 0xBBu8)] {
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(bin, vec![fill; len as usize]).unwrap();
    }

    // The `.meta`: range A only. This is the partial shape — the document exists, and the
    // Journal_Window range is missing from it.
    let now = SystemTime::now();
    let metadata = NewCacheMetadata {
        cache_key: KEY.to_string(),
        object_metadata: ObjectMetadata::new(
            "\"peer-published-etag\"".to_string(),
            "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
            A_LEN + B_LEN,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![RangeSpec::new(
            A_START,
            A_END,
            a_relative.clone(),
            CompressionAlgorithm::None,
            A_LEN,
            A_LEN,
        )],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: Some(now + Duration::from_secs(60)),
        ..Default::default()
    };
    let metadata_path = manager.get_new_metadata_file_path(KEY);
    std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
    std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

    // The peer's journal: `Add` for A (the duplicate) and `Add` for B (the Journal_Window).
    let journals_dir = cache_dir.join("metadata").join("_journals");
    std::fs::create_dir_all(&journals_dir).unwrap();
    let lines = [
        journal_add_entry(KEY, A_START, A_END, A_LEN, a_relative),
        journal_add_entry(KEY, B_START, B_END, B_LEN, b_relative),
    ]
    .iter()
    .map(|e| serde_json::to_string(e).unwrap())
    .collect::<Vec<_>>()
    .join("\n");
    std::fs::write(
        journals_dir.join(format!("{}.journal", PEER_INSTANCE)),
        lines,
    )
    .unwrap();

    Fixture {
        _dir: dir,
        cache_dir,
        manager,
        metadata_path,
        a_bin,
        b_bin,
    }
}

/// Read the extents a journal file names for `KEY` under one operation.
fn journal_extents(path: &Path, operation: JournalOperation) -> Vec<(u64, u64)> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<(u64, u64)> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<JournalEntry>(l).ok())
        .filter(|e| e.cache_key == KEY && e.operation == operation)
        .map(|e| (e.range_spec.start, e.range_spec.end))
        .collect();
    out.sort_unstable();
    out
}

/// The preconditions, asserted rather than assumed. Every one of these can fail, and each
/// failure mode is a different fixture defect with a different fix.
fn assert_preconditions(f: &Fixture) {
    // SHARDED `.meta` — `{bucket}/{XX}/{YYY}/{file}.meta` is four components under
    // `metadata/`. A flat fixture is also removed by the legacy flat path, so an assertion
    // about the sharded enumeration would be vacuous.
    let relative_metadata = f
        .metadata_path
        .strip_prefix(f.cache_dir.join("metadata"))
        .expect("metadata path must live under cache_dir/metadata");
    assert_eq!(
        relative_metadata.components().count(),
        4,
        "PRECONDITION: fixture must be SHARDED ({{bucket}}/{{XX}}/{{YYY}}/{{file}}.meta), \
         got {:?}",
        relative_metadata
    );
    // And bucket-first sharding must actually have happened rather than the constructor
    // falling back to its flat form, which it does for a key it cannot parse into bucket and
    // object key. The first component being the bucket is the signal; the fallback form puts
    // the sanitized whole key directly under `metadata/`.
    assert_eq!(
        relative_metadata
            .components()
            .next()
            .map(|c| c.as_os_str().to_string_lossy().to_string()),
        Some("journal-window-bucket".to_string()),
        "PRECONDITION: the sharded constructor must have parsed the bucket out of the key. \
         A fallback to the flat form makes the .meta-derived and journal-derived enumerations \
         resolve against the same paths and this test cannot fail: {:?}",
        relative_metadata
    );
    // SHARDED `.bin` paths too: more than one component under `ranges/`.
    for bin in [&f.a_bin, &f.b_bin] {
        let relative = bin
            .strip_prefix(f.cache_dir.join("ranges"))
            .expect("range file must live under cache_dir/ranges");
        assert!(
            relative.components().count() > 1,
            "PRECONDITION: range file must be SHARDED, got {:?}",
            relative
        );
        assert!(bin.exists(), "PRECONDITION: .bin must exist: {:?}", bin);
    }

    // R6.4's observable signal: the Journal_Window is read off the two files on disk, not
    // inferred from "no consolidator is running".
    let on_disk: NewCacheMetadata =
        serde_json::from_str(&std::fs::read_to_string(&f.metadata_path).unwrap())
            .expect("fixture .meta must parse");
    let meta_extents: Vec<(u64, u64)> = on_disk.ranges.iter().map(|r| (r.start, r.end)).collect();
    assert_eq!(
        meta_extents,
        vec![(A_START, A_END)],
        "PRECONDITION: the .meta must list range A and NOT the Journal_Window range B — \
         this is the partial shape the production callers reach"
    );

    let peer_journal = f
        .cache_dir
        .join("metadata")
        .join("_journals")
        .join(format!("{}.journal", PEER_INSTANCE));
    assert_eq!(
        journal_extents(&peer_journal, JournalOperation::Add),
        vec![(A_START, A_END), (B_START, B_END)],
        "PRECONDITION: the peer's journal must hold Add entries for BOTH A (the duplicate) \
         and B (the Journal_Window range)"
    );
    // The journal belongs to a different instance than the one invalidating, so the
    // enumeration has to read a file it did not write.
    assert_ne!(
        PEER_INSTANCE, INVALIDATING_INSTANCE,
        "PRECONDITION: the Add entries must belong to a PEER instance"
    );
}

/// The headline. `remove_all_ranges` must delete the Journal_Window `.bin` as well as the
/// `.meta`-derived one, and must report **2** — the number of distinct resolved paths — not 1
/// (the defect) and not 3 (a naive union, which counts range A twice because the file-absent
/// branch counts an absent file as removed).
#[tokio::test]
async fn journal_window_bin_is_removed_and_the_count_does_not_double_count() {
    let f = seed().await;
    assert_preconditions(&f);

    let (removed_count, size_freed) = f
        .manager
        .remove_all_ranges(KEY)
        .await
        .expect("remove_all_ranges");

    // Captured before anything is asserted, so a failure prints the whole state: a panic
    // naming only the count cannot distinguish "reported 2 and deleted one" from
    // "reported 1 and deleted one".
    let a_exists = f.a_bin.exists();
    let b_exists = f.b_bin.exists();

    assert!(
        !b_exists,
        "DEFECT: the Journal_Window range's .bin survived invalidation — the journal \
         enumeration did not reach it. count={} size_freed={} a_exists={} b_exists={} b={:?}",
        removed_count, size_freed, a_exists, b_exists, f.b_bin
    );
    assert!(
        !a_exists,
        "the .meta-derived range's .bin survived: count={} size_freed={} a={:?}",
        removed_count, size_freed, f.a_bin
    );
    assert_eq!(
        removed_count, 2,
        "count must equal the number of DISTINCT resolved paths (2). 1 means the journal \
         enumeration is missing; 3 means range A was counted by both enumerations, which \
         makes the figure MORE wrong than before the union. size_freed={}",
        size_freed
    );
    assert_eq!(
        size_freed,
        A_LEN + B_LEN,
        "size_freed must count each distinct file once: expected {} (A {} + B {})",
        A_LEN + B_LEN,
        A_LEN,
        B_LEN
    );
}

/// A `JournalOperation::Remove` marker is written per removed range, **after** the removal,
/// into the invalidating instance's own journal file.
///
/// Two distinct extents, not four: the marker set is deduplicated by the same candidate list
/// the removal walks, so range A gets one marker even though two enumerations named it.
#[tokio::test]
async fn a_removal_marker_is_written_for_each_removed_range() {
    let f = seed().await;
    assert_preconditions(&f);

    let own_journal = f
        .cache_dir
        .join("metadata")
        .join("_journals")
        .join(format!("{}.journal", INVALIDATING_INSTANCE));
    assert!(
        journal_extents(&own_journal, JournalOperation::Remove).is_empty(),
        "PRECONDITION: no Remove marker may exist before the invalidation"
    );

    f.manager
        .remove_all_ranges(KEY)
        .await
        .expect("remove_all_ranges");

    assert_eq!(
        journal_extents(&own_journal, JournalOperation::Remove),
        vec![(A_START, A_END), (B_START, B_END)],
        "a Remove marker is owed for each removed range, once per distinct range, in this \
         instance's own journal file: {:?}",
        own_journal
    );
    // Ordering (`:R3.3`): the removal happens first, so by the time the marker exists the
    // file it names is already gone. The tolerable crash-between state is a deleted `.bin`
    // with no marker (wasted work); the reverse would be a marker for a present file.
    assert!(
        !f.a_bin.exists() && !f.b_bin.exists(),
        "both .bin files must be gone by the time the markers exist"
    );
}

/// End-to-end through the locking entry point: `invalidate_all_ranges` reaches the
/// Journal_Window range and then removes the `.meta`.
///
/// Distinct from the first test rather than redundant with it. `invalidate_all_ranges` holds
/// the key's metadata slot lock across the whole read-then-delete, so this is the arm that
/// would catch a journal read placed where it deadlocks against that acquisition or runs
/// after the `.meta` is already gone.
#[tokio::test]
async fn invalidate_all_ranges_reaches_the_journal_window_range() {
    let f = seed().await;
    assert_preconditions(&f);

    f.manager
        .invalidate_all_ranges(KEY)
        .await
        .expect("invalidate_all_ranges");

    let a_exists = f.a_bin.exists();
    let b_exists = f.b_bin.exists();
    let meta_exists = f.metadata_path.exists();

    assert!(
        !b_exists,
        "DEFECT: the Journal_Window range's .bin survived invalidate_all_ranges: \
         a_exists={} meta_exists={} b={:?}",
        a_exists, meta_exists, f.b_bin
    );
    assert!(!a_exists, "the .meta-derived .bin survived: {:?}", f.a_bin);
    assert!(
        !meta_exists,
        "the sharded .meta survived: {:?}",
        f.metadata_path
    );
}

/// The safety guard is applied to the journal side too, and an unsafe `file_path` is skipped
/// rather than resolved.
///
/// The traversal target is created inside the cache directory, so if the guard were missing
/// the escape would be *observable* — the file would be deleted — rather than merely
/// attempted. Without this arm, a missing guard would be invisible: the resolved path would
/// point outside the fixture and the removal would simply fail.
///
/// **`outside.exists()` IS evidence — but only against its own defect, and it is a control
/// against the other one.** Read the two separately:
///
/// - Against the **journal-enumeration-removed** build it passed, which it must: with no journal
///   enumeration there is nothing to traverse. There it is a control, and the `removed_count`
///   assertion beside it is what moved.
/// - Against the **guard-removed** build (`is_safe_relative_file_path` neutralised in
///   `enumerate_journal_add_ranges`) it is the discriminator, and it has now been shown failing —
///   `RANGE-INVAL-OWED-RED-SIDES`, red side 2, `3 passed; 1 failed`, this assertion the only
///   failure, panicking with its own DEFECT message. The traversal target lives *inside*
///   `cache_dir`, reachable as `ranges/../escape-target.bin`, which is what makes the escape
///   observable as a deleted file rather than a failed unlink.
///
/// `removed_count == 2` is never reached under the guard-removed build, because this assertion
/// panics first. It would also fail there (3, not 2), so the two are not independent evidence.
#[tokio::test]
async fn an_unsafe_journal_file_path_is_not_resolved() {
    let f = seed().await;
    // Without this, a flattened fixture passes this test while the other three fail
    // (observed in all three `RANGE-INVAL-OWED-RED-SIDES` red side 1 cycles).
    assert_preconditions(&f);

    let outside = f.cache_dir.join("escape-target.bin");
    std::fs::write(&outside, b"must survive").unwrap();

    let evil = journal_add_entry(KEY, 8192, 9215, 1024, "../escape-target.bin".to_string());
    let journals_dir = f.cache_dir.join("metadata").join("_journals");
    std::fs::write(
        journals_dir.join("rir1-evil-instance.journal"),
        serde_json::to_string(&evil).unwrap(),
    )
    .unwrap();

    let (removed_count, _) = f
        .manager
        .remove_all_ranges(KEY)
        .await
        .expect("remove_all_ranges");

    assert!(
        outside.exists(),
        "DEFECT: a journal entry with a traversing file_path deleted a file outside \
         cache_dir/ranges — the path-safety guard is not applied to the journal enumeration"
    );
    assert_eq!(
        removed_count, 2,
        "the rejected entry must not be counted as removed"
    );
}
