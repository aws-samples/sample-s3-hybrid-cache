//! One instance's journal holds the `Add`; a DIFFERENT instance invalidates.
//!
//! `range-invalidation-completeness:R4.4` and `:R6.6`, the divergent-instance fixture for
//! `RANGE-INVAL-T7`. It covers **both halves** of the invalidation contract across the
//! instance boundary: the `.bin` is gone (`:R1`, the cross-instance enumeration in
//! `enumerate_journal_add_ranges`) **and** the lookup reports no coverage (`:R2`, the
//! `Removal_Marker` suppression in `find_pending_journal_ranges`).
//!
//! # Why a divergent fixture, and why a same-view one is worse than none
//!
//! The cache coherency invariants is explicit: a
//! fixture where both instances share one view **passes whatever the code does**. That is why
//! this defect class survived — the shared volume gives a local decision fleet-wide reach, and
//! a single-instance fixture cannot express it. `:R6.6` additionally forbids counting the fleet
//! group as satisfying this: the three verification proxies do share one volume, but a group
//! that drives one request cannot create the divergence, so a passing fleet group is silent on
//! this property.
//!
//! # How the divergence is constructed
//!
//! Two `DiskCacheManager`s over **one** cache directory, each with its own
//! `HybridMetadataWriter` and `JournalConsolidator` built on its own `JournalManager`:
//!
//! | Manager | Instance id | Journal file | Role |
//! |---|---|---|---|
//! | `peer` | `rir7-peer-instance` | `metadata/_journals/rir7-peer-instance.journal` | writes the `Add` |
//! | `invalidator` | `rir7-invalidating-instance` | `metadata/_journals/rir7-invalidating-instance.journal` | performs the invalidation, and its `Remove` markers land here |
//!
//! `JournalManager::new` derives the filename from the instance id, so distinct ids give
//! distinct files and each instance must `read_dir` a file it did not write. The `Add` is
//! appended through **production code** — `HybridMetadataWriter::write_range_metadata` with
//! `WriteMode::JournalOnly` — not with `std::fs::write`, so the peer's filename is produced by
//! the same construction production uses rather than by this file's `format!`.
//!
//! **No `src/` test seam was needed.** Every symbol this fixture reaches is already public:
//! `set_hybrid_metadata_writer`, `set_journal_consolidator`, `write_range_metadata`,
//! `invalidate_all_ranges`, `find_pending_journal_ranges`, `is_shared_storage_mode`.
//!
//! # The PARTIAL shape, for the same reason task 5's fixture uses it
//!
//! The `.meta` exists and is incomplete — it lists range A and not the Journal_Window range B.
//! The *total* shape (no `.meta` at all) is not reachable from any production caller of
//! `invalidate_all_ranges`: the ETag-mismatch arm is lexically inside
//! `if let Some(metadata) = metadata_opt`, and both proxy-injected-412 sites need a partial
//! cache hit whose cached ranges carried an ETag, which only a `.meta` supplies.
//!
//! # The two vacuity traps this file has to survive, and the guards for each
//!
//! **Trap 1 — the mode gate.** `DiskCacheManager::new` leaves `hybrid_metadata_writer` as
//! `None`, `is_shared_storage_mode()` is `self.hybrid_metadata_writer.is_some()`, and
//! `find_pending_journal_ranges` returns `Ok(Vec::new())` immediately when it is false. So
//! **every "the lookup reports no coverage" assertion passes vacuously on a manager built by
//! `new` alone**, on correct code and on broken code alike. `seed()` therefore asserts
//! `is_shared_storage_mode()` on **both** managers, each with its own message naming which one.
//!
//! Note the asymmetry, because it decides where one guard is not enough:
//! `enumerate_journal_add_ranges` does **not** gate on `is_shared_storage_mode()` — its only
//! gate is whether the journals directory exists — while `find_pending_journal_ranges` does. So
//! the `.bin`-is-gone half can pass with the mode off while the no-coverage half is vacuous;
//! one guard cannot stand for both halves.
//!
//! On top of the mode assertions there are two positive apparatus signals, and they are
//! different in kind:
//!
//! - **The BEFORE reading** in the two no-coverage tests. The lookup MUST report the range
//!   before the invalidation. This is the observable signal `:R6.4` asks for, applied to the
//!   lookup half: an empty result afterwards is only a verdict about marker handling if the
//!   same call returned the range beforehand.
//! - **`CONTROL_KEY`, the apparatus control.** A journal `Add` under a second key, never
//!   invalidated, whose lookup MUST report coverage *after* the subject key's invalidation. It
//!   shares the managers, the journals directory, the journal **files** and the key shape with
//!   the subject, so it separates "the marker suppressed this range" from "the reader stopped
//!   working". It is asserted **before** the subject assertion in each test, so an apparatus
//!   failure is reported as an apparatus failure rather than as a product defect, and its
//!   assertion text says in so many words that it is not evidence for the property under test.
//!
//! **Trap 2 — the divergence itself.** A same-view fixture reports a pass, so the divergence is
//! **asserted, not assumed**: `assert_preconditions` requires the two instance ids to differ,
//! the two journal paths to differ, the `Add` for the Journal_Window range to be in the peer's
//! file, and that same file set to hold **no** `Add` for it in the invalidator's file. Built
//! with one id for both managers, both `Add` and marker land in one file and those clauses fail
//! — which is the first of this unit's two red sides, and it attacks the fixture rather than the
//! code.
//!
//! # Red sides
//!
//! Two, different in kind, both taken after `cargo fmt` against the exact bytes that ship:
//!
//! 1. **Against the FIXTURE** — `PEER_INSTANCE` set to the invalidating instance's id, so both
//!    managers share one journal file. `assert_preconditions` must fail. If it passes, the
//!    divergence is decorative and nothing in this file is evidence.
//! 2. **Against the CODE** — `enumerate_journal_add_ranges` narrowed to this instance's own
//!    journal file only, using the same `hostname:pid` construction
//!    `journal_consolidator::get_instance_id` uses. The Journal_Window `.bin` must survive and
//!    the lookup must keep reporting coverage.
//!
//! See the unit's return for the verbatim panic output, the `grep -c` confirming each defect was
//! present in the tree before the result was read, the `pgrep -fl cargo` checks at apply and at
//! read, and the post-revert `diff -q` against a named pre-apply byte copy.
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter, WriteMode};
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::sync::Mutex;

/// The bucket both keys parse into, which is what makes `get_new_metadata_file_path` produce a
/// sharded path rather than falling back to its flat form.
const BUCKET: &str = "rir7-divergence-bucket";

/// The subject key: invalidated by the `invalidator` manager.
const KEY: &str = "rir7-divergence-bucket/journal-window-object.bin";
/// The apparatus control's key. Nothing ever invalidates it, so its journal `Add` must keep
/// being reported after the subject key's invalidation.
const CONTROL_KEY: &str = "rir7-divergence-bucket/never-invalidated-object.bin";

/// Range A: folded into the `.meta`. This is what makes the fixture the **partial** shape.
const A_START: u64 = 0;
const A_END: u64 = 1023;
/// Range B: the Journal_Window range. Named by the **peer's** journal only.
const B_START: u64 = 4096;
const B_END: u64 = 5119;
/// Range C: the control key's range. Named by the peer's journal only, never invalidated.
const C_START: u64 = 0;
const C_END: u64 = 1023;

const A_LEN: u64 = A_END - A_START + 1;
const B_LEN: u64 = B_END - B_START + 1;
const C_LEN: u64 = C_END - C_START + 1;

/// The instance whose journal holds the `Add` entries.
const PEER_INSTANCE: &str = "rir7-peer-instance";
/// The instance that performs the invalidation, and therefore the one whose journal file the
/// `Remove` markers land in. Deliberately never the one above.
const INVALIDATING_INSTANCE: &str = "rir7-invalidating-instance";

struct Fixture {
    _dir: TempDir,
    cache_dir: PathBuf,
    /// Instance A. Its journal holds the `Add`s; it never invalidates anything.
    peer: DiskCacheManager,
    /// Instance B. It performs the invalidation and has never written a journal `Add`.
    invalidator: DiskCacheManager,
    peer_journal: PathBuf,
    invalidator_journal: PathBuf,
    metadata_path: PathBuf,
    a_bin: PathBuf,
    b_bin: PathBuf,
    c_bin: PathBuf,
    b_relative: String,
    c_relative: String,
}

fn relative_range_path(cache_dir: &Path, bin: &Path) -> String {
    bin.strip_prefix(cache_dir.join("ranges"))
        .expect("range file must live under cache_dir/ranges")
        .to_string_lossy()
        .to_string()
}

/// One fully configured instance over the shared cache directory.
///
/// Returns the manager and its `HybridMetadataWriter`, because the peer has to drive the writer
/// directly to append its `Add` through production code.
///
/// The `JournalConsolidator` is **constructed and never started** — `JournalConsolidator::new`
/// spawns nothing, so no background cycle can fold a journal entry mid-test. The preconditions
/// below do not rely on that (they read the files), but a folded entry would make this fixture
/// measure something else.
async fn instance(
    cache_dir: &Path,
    instance_id: &str,
) -> (DiskCacheManager, Arc<Mutex<HybridMetadataWriter>>) {
    // Compression off, so `compressed_size` equals the body length and nothing here depends on
    // a compression ratio.
    let mut manager = DiskCacheManager::new(cache_dir.to_path_buf(), false, 1024, false, 1_048_576);
    manager.initialize().await.expect("disk cache init");

    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.to_path_buf(),
        instance_id.to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.to_path_buf(),
        Duration::from_millis(300),
        0,
    ));

    let writer = Arc::new(Mutex::new(HybridMetadataWriter::new(
        cache_dir.to_path_buf(),
        Arc::clone(&lock_manager),
        Arc::clone(&journal_manager),
        Arc::new(ConsolidationTrigger::new(10 * 1024 * 1024, 1000)),
    )));
    manager.set_hybrid_metadata_writer(Arc::clone(&writer));

    manager.set_journal_consolidator(Arc::new(JournalConsolidator::new(
        cache_dir.to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig::default(),
    )));

    (manager, writer)
}

/// Seed the divergence: a `.meta` listing range A, three `.bin` files, and the **peer**
/// instance's journal holding production-written `Add` entries for B and C.
///
/// The `.bin` files are written directly onto the shared volume, which is how a peer's publish
/// appears: `store_range`'s hybrid arm renames the `.bin` into place **before** appending its
/// journal entry, deliberately, so a peer consolidating the entry finds the file already there.
async fn seed() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let cache_dir = dir.path().to_path_buf();

    let (peer, peer_writer) = instance(&cache_dir, PEER_INSTANCE).await;
    let (invalidator, _invalidator_writer) = instance(&cache_dir, INVALIDATING_INSTANCE).await;

    // TRAP 1's guard, one assertion per manager with its own message. Without these, every
    // "the lookup reports no coverage" assertion in this file passes on any code at all,
    // because `find_pending_journal_ranges` short-circuits to an empty vector when the mode is
    // off and `DiskCacheManager::new` leaves it off.
    assert!(
        peer.is_shared_storage_mode(),
        "fixture precondition: the PEER manager ({}) must be in shared storage mode. \
         find_pending_journal_ranges returns Ok(vec![]) immediately when it is not, so the \
         peer's no-coverage assertions would pass vacuously on correct and broken code alike",
        PEER_INSTANCE
    );
    assert!(
        invalidator.is_shared_storage_mode(),
        "fixture precondition: the INVALIDATING manager ({}) must be in shared storage mode. \
         Its own no-coverage assertion would otherwise pass vacuously — note that the .bin \
         removal it also performs does NOT gate on this, so one guard cannot stand for both \
         halves",
        INVALIDATING_INSTANCE
    );

    let a_bin = peer.get_new_range_file_path(KEY, A_START, A_END);
    let b_bin = peer.get_new_range_file_path(KEY, B_START, B_END);
    let c_bin = peer.get_new_range_file_path(CONTROL_KEY, C_START, C_END);
    let a_relative = relative_range_path(&cache_dir, &a_bin);
    let b_relative = relative_range_path(&cache_dir, &b_bin);
    let c_relative = relative_range_path(&cache_dir, &c_bin);

    for (bin, len, fill) in [
        (&a_bin, A_LEN, 0xAAu8),
        (&b_bin, B_LEN, 0xBBu8),
        (&c_bin, C_LEN, 0xCCu8),
    ] {
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(bin, vec![fill; len as usize]).unwrap();
    }

    // The `.meta` for the subject key: range A only. Written directly, the way a peer's
    // consolidated document appears on the shared volume.
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
            a_relative,
            CompressionAlgorithm::None,
            A_LEN,
            A_LEN,
        )
        // As a 3.0.0 writer records it (`RangeSpec::is_unverified_prefix`).
        .with_file_identity(Some(s3_proxy::cache_types::RangeFileIdentity {
            size: A_LEN,
            mtime_ns: 1,
        }))],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: Some(now + Duration::from_secs(60)),
        ..Default::default()
    };
    let metadata_path = peer.get_new_metadata_file_path(KEY);
    std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
    std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

    // `created_at` strictly in the past, because the suppression discriminator is
    // `range.created_at <= marker.timestamp` and the marker is timestamped when the
    // invalidation runs. Five seconds is far outside any clock granularity this could turn on,
    // so the republish guard cannot retain these ranges for a timing reason.
    let published_at = now - Duration::from_secs(5);

    // The `Add`s, through PRODUCTION code rather than `std::fs::write`: the journal filename is
    // then derived by `JournalManager` from the peer's instance id, which is the property this
    // fixture is about.
    for (key, start, end, len, relative) in [
        (KEY, B_START, B_END, B_LEN, b_relative.clone()),
        (CONTROL_KEY, C_START, C_END, C_LEN, c_relative.clone()),
    ] {
        let mut spec = RangeSpec::new(start, end, relative, CompressionAlgorithm::None, len, len)
            // As a 3.0.0 writer records it (`RangeSpec::is_unverified_prefix`).
            .with_file_identity(Some(s3_proxy::cache_types::RangeFileIdentity {
                size: len,
                mtime_ns: 1,
            }));
        spec.created_at = published_at;
        peer_writer
            .lock()
            .await
            .write_range_metadata(
                key,
                spec,
                WriteMode::JournalOnly,
                None,
                Duration::from_secs(3600),
            )
            .await
            .expect("the peer instance must be able to append its journal Add");
    }

    let journals_dir = cache_dir.join("metadata").join("_journals");
    let peer_journal = journals_dir.join(format!("{}.journal", PEER_INSTANCE));
    let invalidator_journal = journals_dir.join(format!("{}.journal", INVALIDATING_INSTANCE));

    Fixture {
        _dir: dir,
        cache_dir,
        peer,
        invalidator,
        peer_journal,
        invalidator_journal,
        metadata_path,
        a_bin,
        b_bin,
        c_bin,
        b_relative,
        c_relative,
    }
}

/// The extents a journal file names for one key under one operation.
fn journal_extents(path: &Path, cache_key: &str, operation: JournalOperation) -> Vec<(u64, u64)> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<(u64, u64)> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<JournalEntry>(l).ok())
        .filter(|e| e.cache_key == cache_key && e.operation == operation)
        .map(|e| (e.range_spec.start, e.range_spec.end))
        .collect();
    out.sort_unstable();
    out
}

/// The predicate the no-coverage half reads: the set of relative range-file paths
/// `find_pending_journal_ranges` reports for a key, through a NAMED manager (`:R6.3`).
///
/// Which manager asks is the whole point here, so it is a parameter rather than a field.
async fn reported_paths(
    manager: &DiskCacheManager,
    cache_key: &str,
    start: u64,
    end: u64,
) -> Vec<String> {
    let mut paths: Vec<String> = manager
        .find_pending_journal_ranges(cache_key, start, end)
        .await
        .expect("journal lookup must not error")
        .into_iter()
        .map(|spec| spec.file_path)
        .collect();
    paths.sort();
    paths
}

/// The preconditions, asserted rather than assumed. Every clause can fail, and each failure is
/// a different fixture defect.
///
/// **The divergence clauses are TRAP 2's guard.** A fixture whose two managers share one view
/// reports a pass whatever the code does, so "the `Add` is in the peer's file and not in the
/// invalidator's" is asserted here. Built with one instance id for both managers, both entries
/// land in one file and these clauses fail — which is this unit's first red side.
fn assert_preconditions(f: &Fixture) {
    // The divergence, named at its cause first so a same-id fixture fails with the clearest
    // possible message.
    assert_ne!(
        PEER_INSTANCE, INVALIDATING_INSTANCE,
        "PRECONDITION: the two instance ids must DIFFER. With one id both managers write and \
         read the same journal file, there is no cross-instance read, and this whole file \
         passes whatever the code does — a same-view fixture is worse than no fixture"
    );
    assert_ne!(
        f.peer_journal, f.invalidator_journal,
        "PRECONDITION: the two instance ids must produce DIFFERENT journal files, because that \
         is what forces each instance to read a file it did not write"
    );
    assert_eq!(
        journal_extents(&f.peer_journal, KEY, JournalOperation::Add),
        vec![(B_START, B_END)],
        "PRECONDITION: the PEER's journal file ({:?}) must hold the Add for the Journal_Window \
         range",
        f.peer_journal
    );
    assert!(
        journal_extents(&f.invalidator_journal, KEY, JournalOperation::Add).is_empty(),
        "PRECONDITION: the INVALIDATING instance's journal file ({:?}) must hold NO Add for \
         this key — it has published nothing. If it does, the two managers are sharing one \
         journal file and the divergence is decorative: got {:?}",
        f.invalidator_journal,
        journal_extents(&f.invalidator_journal, KEY, JournalOperation::Add)
    );
    assert!(
        journal_extents(&f.invalidator_journal, KEY, JournalOperation::Remove).is_empty(),
        "PRECONDITION: no Remove marker may exist before the invalidation"
    );
    // The apparatus control's Add is in the same peer file, so the control shares the reader's
    // whole input path with the subject.
    assert_eq!(
        journal_extents(&f.peer_journal, CONTROL_KEY, JournalOperation::Add),
        vec![(C_START, C_END)],
        "PRECONDITION: the apparatus control's Add must be in the PEER's journal file too"
    );

    // SHARDED `.meta` — `{bucket}/{XX}/{YYY}/{file}.meta` is four components under `metadata/`.
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
    assert_eq!(
        relative_metadata
            .components()
            .next()
            .map(|c| c.as_os_str().to_string_lossy().to_string()),
        Some(BUCKET.to_string()),
        "PRECONDITION: the sharded constructor must have parsed the bucket out of the key. A \
         fallback to the flat form makes the .meta-derived and journal-derived enumerations \
         resolve against the same paths: {:?}",
        relative_metadata
    );

    // The PARTIAL shape, read off the file rather than assumed: the `.meta` lists A and not the
    // Journal_Window range B. Together with the peer-journal clause above this is `:R6.4`'s
    // observable signal for the Journal_Window.
    let on_disk: NewCacheMetadata =
        serde_json::from_str(&std::fs::read_to_string(&f.metadata_path).unwrap())
            .expect("fixture .meta must parse");
    assert_eq!(
        on_disk
            .ranges
            .iter()
            .map(|r| (r.start, r.end))
            .collect::<Vec<_>>(),
        vec![(A_START, A_END)],
        "PRECONDITION: the .meta must list range A and NOT the Journal_Window range B — this \
         is the partial shape the production callers reach"
    );

    for bin in [&f.a_bin, &f.b_bin, &f.c_bin] {
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
}

/// HALF 1 (`:R1`, task 3's property): a peer's Journal_Window `.bin` is deleted by an
/// invalidation performed on a **different** instance.
///
/// The invalidating manager has written no journal entry of its own, so the only way it can
/// reach this `.bin` is by reading the peer's journal file — which is what
/// `enumerate_journal_add_ranges` does, and what a narrowing to "this instance's own journal
/// file only" would remove.
#[tokio::test]
async fn a_divergent_instance_removes_the_peers_journal_window_bin() {
    let f = seed().await;
    assert_preconditions(&f);

    f.invalidator
        .invalidate_all_ranges(KEY)
        .await
        .expect("invalidate_all_ranges");

    // Captured before anything is asserted, so a failure prints the whole state rather than one
    // bit of it.
    let a_exists = f.a_bin.exists();
    let b_exists = f.b_bin.exists();
    let c_exists = f.c_bin.exists();
    let meta_exists = f.metadata_path.exists();

    assert!(
        !b_exists,
        "DEFECT: the PEER's Journal_Window .bin survived an invalidation performed by a \
         DIFFERENT instance ({}). The invalidating instance has published nothing, so the only \
         way to reach this file is to read {:?} — a journal file it did not write. \
         a_exists={} c_exists={} meta_exists={} b={:?}",
        INVALIDATING_INSTANCE, f.peer_journal, a_exists, c_exists, meta_exists, f.b_bin
    );
    assert!(
        !a_exists,
        "the .meta-derived .bin survived: b_exists={} meta_exists={} a={:?}",
        b_exists, meta_exists, f.a_bin
    );
    assert!(
        !meta_exists,
        "the sharded .meta survived: {:?}",
        f.metadata_path
    );
    // WIDTH GUARD, not evidence for the property above: invalidation is scoped to one key, so
    // the control key's .bin must be untouched. An implementation that deleted every .bin any
    // journal names would pass the three assertions above and fail here.
    assert!(
        c_exists,
        "WIDTH GUARD: invalidating {} deleted the control key's .bin as well ({:?}). The \
         enumeration must be scoped to the key.",
        KEY, f.c_bin
    );
}

/// HALF 2 (`:R2`, task 4's property), through the PEER's own lookup — the discriminating
/// direction.
///
/// The marker is written into the **invalidator's** journal file, so for the peer to stop
/// reporting coverage it has to read a file it did not write. This is the sharper of the two
/// no-coverage assertions: a cross-instance marker read, taken from the instance that published
/// the range.
#[tokio::test]
async fn the_peers_own_lookup_stops_reporting_coverage_after_a_divergent_invalidation() {
    let f = seed().await;
    assert_preconditions(&f);

    // The BEFORE reading. An empty result after the invalidation is only a verdict about marker
    // handling if this call returned the range beforehand — otherwise it is indistinguishable
    // from a fixture that never reaches the function.
    let before = reported_paths(&f.peer, KEY, B_START, B_END).await;
    assert_eq!(
        before,
        vec![f.b_relative.clone()],
        "APPARATUS FAILURE, not a product verdict: the peer's own lookup did not report its \
         own journal Add before any invalidation, so this fixture does not reach \
         find_pending_journal_ranges and nothing below it can be read as evidence"
    );

    f.invalidator
        .invalidate_all_ranges(KEY)
        .await
        .expect("invalidate_all_ranges");

    // Where the marker landed, asserted rather than assumed — it is the invalidating instance's
    // own journal file, which is what makes the peer's read below a cross-instance one.
    assert_eq!(
        journal_extents(&f.invalidator_journal, KEY, JournalOperation::Remove),
        vec![(A_START, A_END), (B_START, B_END)],
        "the Remove markers must be in the INVALIDATING instance's own journal file: {:?}",
        f.invalidator_journal
    );
    assert!(
        journal_extents(&f.peer_journal, KEY, JournalOperation::Remove).is_empty(),
        "no marker may have been written into the peer's journal file — the peer did not \
         invalidate anything: {:?}",
        f.peer_journal
    );

    let control = reported_paths(&f.peer, CONTROL_KEY, C_START, C_END).await;
    let after = reported_paths(&f.peer, KEY, B_START, B_END).await;

    // APPARATUS CONTROL, asserted BEFORE the subject so an apparatus failure is reported as
    // one. THIS LINE IS NOT EVIDENCE FOR THE NO-COVERAGE PROPERTY UNDER TEST — it asserts the
    // opposite outcome through the same manager, the same journals directory and the same
    // journal file, purely so that the empty `after` below is attributable to the marker rather
    // than to the reader having stopped working.
    assert_eq!(
        control,
        vec![f.c_relative.clone()],
        "APPARATUS FAILURE, not a product verdict: a key with no invalidation against it \
         stopped being reported after an unrelated key was invalidated. Read nothing into the \
         subject assertion below until this passes. NOT evidence that markers suppress \
         coverage."
    );
    assert!(
        after.is_empty(),
        "DEFECT: after an invalidation performed by a DIFFERENT instance ({}), the PEER's own \
         lookup still reports coverage for a range whose .bin is gone. The marker is in {:?} — \
         a journal file this manager did not write — so either the marker scan is scoped to \
         this instance's own file or it was never written. reported={:?} b_bin_exists={}",
        INVALIDATING_INSTANCE,
        f.invalidator_journal,
        after,
        f.b_bin.exists()
    );
}

/// HALF 2 again, through the INVALIDATING instance's own lookup.
///
/// Weaker than the test above — the marker is in this manager's own journal file — but its
/// BEFORE reading is a cross-instance read in the other direction: this instance reports a range
/// the **peer** published, which is the same enumeration property half 1 depends on, observed
/// through the lookup rather than through a deletion.
#[tokio::test]
async fn the_invalidating_instance_reports_no_coverage_for_the_range_it_removed() {
    let f = seed().await;
    assert_preconditions(&f);

    let before = reported_paths(&f.invalidator, KEY, B_START, B_END).await;
    assert_eq!(
        before,
        vec![f.b_relative.clone()],
        "APPARATUS FAILURE, not a product verdict: the invalidating instance did not report a \
         range the PEER published, so its lookup is not reading {:?} and nothing below can be \
         read as evidence",
        f.peer_journal
    );

    f.invalidator
        .invalidate_all_ranges(KEY)
        .await
        .expect("invalidate_all_ranges");

    let control = reported_paths(&f.invalidator, CONTROL_KEY, C_START, C_END).await;
    let after = reported_paths(&f.invalidator, KEY, B_START, B_END).await;

    // APPARATUS CONTROL, same role and same caveat as in the test above. NOT evidence for the
    // no-coverage property; it exists so that an empty `after` is attributable to the marker.
    assert_eq!(
        control,
        vec![f.c_relative.clone()],
        "APPARATUS FAILURE, not a product verdict: a key with no invalidation against it \
         stopped being reported. NOT evidence that markers suppress coverage."
    );
    assert!(
        after.is_empty(),
        "DEFECT: the invalidating instance still reports coverage for a range it removed from \
         a PEER's journal. reported={:?} b_bin_exists={}",
        after,
        f.b_bin.exists()
    );
}
