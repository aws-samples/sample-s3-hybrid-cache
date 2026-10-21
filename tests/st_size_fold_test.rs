//! The `st_size` fold installs the four folded fields onto per-shard checkpoints.
//!
//! # Every fixture here has more than one shard, and that is load-bearing
//!
//! The per-shard install, the mixed-age sum and the never-overlap property are all
//! unreachable on a single-shard fixture: one shard means one install, so "installed per
//! shard as the fold proceeds" and "installed once at the end" are indistinguishable, and a
//! budget abort has nothing to stop short of. A one-leaf, one-file tree passes every
//! assertion below while exercising none of them.
//!
//! # These assert field VALUES, never that the document parses
//!
//! A checkpoint with every one of this fold's fields **absent** still parses — that is exactly
//! the property `StagedShardCheckpoint::other`'s `#[serde(flatten)]` exists to guarantee — so
//! "it parsed" discriminates nothing at all. Each assertion below reads a value out of `other`
//! and compares it against a figure the fixture computed independently.

use std::collections::BTreeMap;
use std::path::Path;

use s3_proxy::st_size_fold::{
    fold_legacy_flat_root, fold_shard, install_fold_fields, run_st_size_fold, ShardFigure,
    HISTOGRAM_CLASSES,
};
use s3_proxy::staged_fold::{read_shard_checkpoints, Shard, StagedShardCheckpoint};
use tempfile::TempDir;

/// Seed one range file into `ranges/{bucket}/{l1}/{l2}/{name}` and return its size.
fn seed_range(cache_dir: &Path, bucket: &str, l1: &str, l2: &str, name: &str, size: usize) -> u64 {
    let dir = cache_dir.join("ranges").join(bucket).join(l1).join(l2);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), vec![0u8; size]).unwrap();
    size as u64
}

/// Seed one metadata file into `metadata/{bucket}/{l1}/{l2}/{name}`.
fn seed_meta(cache_dir: &Path, bucket: &str, l1: &str, l2: &str, name: &str) {
    let dir = cache_dir.join("metadata").join(bucket).join(l1).join(l2);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), b"{}").unwrap();
}

fn field(c: &StagedShardCheckpoint, key: &str) -> Option<serde_json::Value> {
    c.other.get(key).cloned()
}

fn u64_field(c: &StagedShardCheckpoint, key: &str) -> u64 {
    field(c, key)
        .unwrap_or_else(|| panic!("field `{key}` is absent from the installed checkpoint"))
        .as_u64()
        .unwrap_or_else(|| panic!("field `{key}` is not an unsigned integer"))
}

/// A two-shard, four-leaf fixture with independently computed byte totals.
///
/// Shard `aa` holds two leaves; shard `bb` holds two. The sizes are all distinct and none is a
/// multiple of another, so a figure that summed the wrong shard's files, or double-counted a
/// leaf, cannot land on the right total by coincidence.
struct Fixture {
    _tmp: TempDir,
    cache_dir: std::path::PathBuf,
    expected_bytes: BTreeMap<Shard, u64>,
    expected_ranges: BTreeMap<Shard, u64>,
    expected_entries: BTreeMap<Shard, u64>,
}

impl Fixture {
    fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let mut bytes = BTreeMap::new();
        let mut ranges = BTreeMap::new();
        let mut entries = BTreeMap::new();

        // Shard bucket/aa — two leaves, three range files, two objects.
        let a = Shard::new("bucket", "aa");
        let mut a_bytes = 0;
        a_bytes += seed_range(&dir, "bucket", "aa", "111", "one_0-1099.bin", 1_100);
        a_bytes += seed_range(&dir, "bucket", "aa", "111", "one_1100-4399.bin", 3_300);
        a_bytes += seed_range(&dir, "bucket", "aa", "222", "two_0-6999.bin", 7_000);
        seed_meta(&dir, "bucket", "aa", "111", "one.meta");
        seed_meta(&dir, "bucket", "aa", "222", "two.meta");
        bytes.insert(a.clone(), a_bytes);
        ranges.insert(a.clone(), 3);
        entries.insert(a, 2);

        // Shard bucket/bb — two leaves, two range files, one object. The second leaf has a
        // range file but no `.meta`, which is what an orphan looks like to a readdir: it must
        // be in `byte_sum` and must NOT be in `entry_count`.
        let b = Shard::new("bucket", "bb");
        let mut b_bytes = 0;
        b_bytes += seed_range(&dir, "bucket", "bb", "333", "three_0-511.bin", 512);
        b_bytes += seed_range(&dir, "bucket", "bb", "444", "orphan_0-99.bin", 100);
        seed_meta(&dir, "bucket", "bb", "333", "three.meta");
        bytes.insert(b.clone(), b_bytes);
        ranges.insert(b.clone(), 2);
        entries.insert(b, 1);

        Self {
            _tmp: tmp,
            cache_dir: dir,
            expected_bytes: bytes,
            expected_ranges: ranges,
            expected_entries: entries,
        }
    }

    fn dir(&self) -> &Path {
        &self.cache_dir
    }
}

/// The fold installs `entry_count`, `range_count`, `byte_sum` and `histogram` on every shard,
/// with the values the fixture computed.
///
/// # THIS IS THE RED SIDE
///
/// No checkpoint on disk carries any of these fields before this fold exists, so the pre-fix
/// tree cannot pass this: `u64_field` panics naming the absent field rather than reporting a
/// wrong number. That is deliberately a *value* assertion on each field and not a check that
/// the document parses — a document with all four absent parses perfectly.
///
/// A failure implicates `st_size_fold::install_fold_fields`'s six `other.insert` calls and
/// `fold_shard_blocking`'s two accumulation loops.
#[tokio::test]
async fn the_fold_installs_the_four_folded_fields_with_the_right_values() {
    let f = Fixture::new();
    let outcome = run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;

    assert_eq!(
        outcome.installed.len(),
        2,
        "both shards must be installed, one document each: {:?}",
        outcome.installed
    );
    assert!(outcome.failed.is_empty(), "failed: {:?}", outcome.failed);
    assert!(!outcome.enumeration_failed);

    let set = read_shard_checkpoints(f.dir());
    assert!(
        set.is_complete(),
        "the installed documents must all be readable"
    );

    for (shard, expected_bytes) in f.expected_bytes.iter() {
        let c = set
            .get(shard)
            .unwrap_or_else(|| panic!("no checkpoint installed for shard {shard}"));
        assert_eq!(
            u64_field(c, "byte_sum"),
            *expected_bytes,
            "byte_sum for shard {shard}"
        );
        assert_eq!(
            u64_field(c, "range_count"),
            f.expected_ranges[shard],
            "range_count for shard {shard}"
        );
        assert_eq!(
            u64_field(c, "entry_count"),
            f.expected_entries[shard],
            "entry_count for shard {shard} — `.meta` names, not `.bin` names"
        );
        let hist = field(c, "histogram")
            .expect("histogram absent")
            .as_array()
            .expect("histogram is not an array")
            .len();
        assert_eq!(hist, HISTOGRAM_CLASSES, "histogram width for shard {shard}");
    }
}

/// `scanned_by` is the folding instance and `scanned_at` is a real timestamp.
///
/// Separated from the value test above because it is a different claim: those four fields are
/// the figure, these two are its provenance, and the design requires this age be published
/// separately from the staged fold's own.
///
/// A failure implicates `install_fold_fields`'s `scanned_at` / `scanned_by` inserts.
#[tokio::test]
async fn the_fold_stamps_its_own_age_and_instance() {
    let f = Fixture::new();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    run_st_size_fold(f.dir(), "host:7777", std::time::Duration::ZERO).await;

    let set = read_shard_checkpoints(f.dir());
    let c = set.get(&Shard::new("bucket", "aa")).unwrap();
    assert_eq!(
        field(c, "scanned_by"),
        Some(serde_json::json!("host:7777")),
        "scanned_by must name the folding instance"
    );
    let at = u64_field(c, "scanned_at");
    assert!(
        at >= before,
        "scanned_at must be this pass's own nanosecond timestamp, got {at} against {before}"
    );

    // And the staged fold's own age is NOT stamped by this fold — the two ages are separate.
    assert_eq!(
        c.staged_scanned_at, 0,
        "no staged fold has visited this shard, so its age must stay 0 (= maximally stale) \
         rather than being given this pass's timestamp"
    );
    assert_eq!(c.staged_byte_sum, 0);
    assert_eq!(c.staged_range_count, 0);
    assert!(c.staged_fold_cut.is_empty());
}

/// The three fields this fold does NOT compute are absent, not zero.
///
/// `tail_floor` and `tail_entries` describe a partition this pass does not write;
/// `orphan_bytes_seen` is defined as a readdir diffed against the parse, which a `stat`-only
/// fold cannot produce. An absent key and a key holding `0` are different claims — the first
/// says "not computed", the second says "computed, and it is zero" — and this asserts the
/// fold makes the first claim rather than silently making the second.
///
/// A failure implicates the `other.insert` list in `install_fold_fields`: it must insert
/// exactly its six fields (`entry_count`, `range_count`, `byte_sum`, `histogram`,
/// `scanned_at`, `scanned_by`) and nothing else.
#[tokio::test]
async fn the_fields_this_fold_cannot_compute_are_absent_rather_than_zero() {
    let f = Fixture::new();
    run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;

    let set = read_shard_checkpoints(f.dir());
    let c = set.get(&Shard::new("bucket", "aa")).unwrap();
    for absent in ["tail_floor", "tail_entries", "orphan_bytes_seen"] {
        assert_eq!(
            field(c, absent),
            None,
            "`{absent}` must be ABSENT, not present as a folded zero — an installed zero is a \
             claim this fold is not entitled to make"
        );
    }
    // And the distinction is observable: a folded field this pass DID compute is present, and
    // `bb`'s smallest histogram bin is a genuine zero sitting beside those absences.
    assert!(field(c, "byte_sum").is_some());
}

/// A `*.tmp` file is excluded by name, and `mpus_in_progress/` is not traversed at all.
///
/// Both are scope guards rather than optimisations: a partially written file is not resident,
/// and an in-progress multipart part is unevictable, so counting either would put bytes into
/// an authority that cannot be acted on.
///
/// The construction is a differential: fold, plant, fold again, and require the figure not to
/// move. A single fold with the files already present would pass if the fold counted them and
/// the expectation had been written to match.
///
/// A failure implicates `is_folded_file` and `enumerate_shards`' two-root traversal.
#[tokio::test]
async fn temp_files_and_in_progress_parts_are_outside_the_folds_scope() {
    let f = Fixture::new();
    let shard = Shard::new("bucket", "aa");

    let baseline = fold_shard(f.dir(), &shard).await.unwrap();
    assert_eq!(baseline.byte_sum, f.expected_bytes[&shard]);

    // Both temp shapes, in a leaf the fold definitely reads.
    seed_range(
        f.dir(),
        "bucket",
        "aa",
        "111",
        "one_5000-9999.bin.tmp",
        5_000,
    );
    seed_range(
        f.dir(),
        "bucket",
        "aa",
        "111",
        "one_5000-9999.bin.tmp.host.4242",
        5_000,
    );
    // A `.tmp` metadata file too, which must not move `entry_count`.
    seed_meta(f.dir(), "bucket", "aa", "111", "one.meta.tmp");
    // And an in-progress multipart part, laid out as the upload path lays it out.
    let mpu = f.dir().join("mpus_in_progress").join("upload-1");
    std::fs::create_dir_all(&mpu).unwrap();
    std::fs::write(mpu.join("part1.bin"), vec![0u8; 8_000_000]).unwrap();

    let after = fold_shard(f.dir(), &shard).await.unwrap();
    assert_eq!(
        after, baseline,
        "no scope-guarded file may move any of the four figures"
    );

    // The in-progress part must also be invisible to a whole-pass run, which is the level the
    // `mpus_in_progress/` guard actually applies at.
    let outcome = run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;
    let set = read_shard_checkpoints(f.dir());
    let total: u64 = set
        .shards()
        .into_iter()
        .map(|s| u64_field(set.get(&s).unwrap(), "byte_sum"))
        .sum();
    let expected: u64 = f.expected_bytes.values().sum();
    assert_eq!(
        total, expected,
        "the 8 MB in-progress part must be in no shard's byte_sum"
    );
    assert_eq!(outcome.installed.len(), 2);
}

/// A `.bin` at the `ranges/` root is folded as a pass-scoped figure, never into a shard.
///
/// A flat file's path carries neither a bucket nor an `L1`, so no `(bucket, L1)` checkpoint can
/// honestly claim it. The assertion is two-sided: the figure must appear at pass scope AND must
/// not appear in any shard's `byte_sum`.
///
/// A failure implicates `fold_legacy_flat_root` and the absence of any flat-root term in
/// `fold_shard_blocking`.
#[tokio::test]
async fn the_legacy_flat_root_is_folded_at_pass_scope_and_not_into_a_shard() {
    let f = Fixture::new();
    let ranges_root = f.dir().join("ranges");
    std::fs::write(ranges_root.join("legacy_0-2047.bin"), vec![0u8; 2_048]).unwrap();
    // A temp file at the root is excluded here too.
    std::fs::write(ranges_root.join("legacy_0-2047.bin.tmp"), vec![0u8; 999]).unwrap();

    let flat = fold_legacy_flat_root(f.dir());
    assert!(!flat.unreadable);
    assert_eq!(flat.range_count, 1, "one flat range file, not the temp one");
    assert_eq!(flat.byte_sum, 2_048);

    let outcome = run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;
    assert_eq!(outcome.legacy_flat.byte_sum, 2_048);

    let set = read_shard_checkpoints(f.dir());
    let total: u64 = set
        .shards()
        .into_iter()
        .map(|s| u64_field(set.get(&s).unwrap(), "byte_sum"))
        .sum();
    assert_eq!(
        total,
        f.expected_bytes.values().sum::<u64>(),
        "no shard's byte_sum may claim the flat file — a flat path names no shard"
    );
}

/// The install preserves the staged fold's four fields and its `staged_fold_cut`.
///
/// The mirror of `a_staged_install_preserves_the_st_size_folds_fields`, from this producer's
/// side. That test pins one direction of the two-producer contract; this pins the other, and
/// both are needed because each producer reads and rewrites the whole document.
///
/// A failure implicates `install_fold_fields`' read-modify-write: it must load the current
/// document and mutate `other` rather than constructing a fresh one.
#[tokio::test]
async fn the_folds_install_preserves_the_staged_folds_own_fields() {
    let f = Fixture::new();
    let shard = Shard::new("bucket", "aa");

    // A document as the staged fold would leave it.
    let mut seeded = StagedShardCheckpoint::unfolded_for(&shard);
    seeded.staged_byte_sum = 987_654_321;
    seeded.staged_range_count = 4_242;
    seeded.staged_scanned_at = 1_756_400_000_123_456_789;
    seeded.staged_scanned_by = "host:9999".to_string();
    seeded
        .staged_fold_cut
        .insert("host:9999".to_string(), 55_555);
    // And a field belonging to neither producer, to prove nothing is dropped by name.
    seeded
        .other
        .insert("tail_entries".to_string(), serde_json::json!(227));
    seeded.install(f.dir()).unwrap();

    run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;

    let after = read_shard_checkpoints(f.dir()).get(&shard).unwrap().clone();
    assert_eq!(after.staged_byte_sum, 987_654_321);
    assert_eq!(after.staged_range_count, 4_242);
    assert_eq!(
        after.staged_scanned_at, 1_756_400_000_123_456_789,
        "the staged fold's own age must not be stamped by this fold"
    );
    assert_eq!(after.staged_scanned_by, "host:9999");
    assert_eq!(after.staged_fold_cut.get("host:9999"), Some(&55_555));
    assert_eq!(
        field(&after, "tail_entries"),
        Some(serde_json::json!(227)),
        "a third producer's field must survive too"
    );
    // And this fold's own figure did land.
    assert_eq!(u64_field(&after, "byte_sum"), f.expected_bytes[&shard]);
}

/// A shard folded more recently than this pass started keeps the newer figure.
///
/// Newest-`scanned_at` wins. The comparison is against the pass's START, which is what makes
/// it impossible for a pass to decline its own write: a pass visits each shard once, so the
/// timestamp it compares against is never one it wrote.
///
/// A failure implicates the `scanned_at` comparison in `install_fold_fields`.
#[tokio::test]
async fn a_newer_peer_scan_is_not_overwritten_by_an_older_pass() {
    let f = Fixture::new();
    let shard = Shard::new("bucket", "aa");

    // A peer's install, stamped in the future relative to the pass we are about to run.
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        + 60_000_000_000;
    let mut peer = StagedShardCheckpoint::unfolded_for(&shard);
    peer.other
        .insert("scanned_at".to_string(), serde_json::json!(future));
    peer.other
        .insert("byte_sum".to_string(), serde_json::json!(1_234_567));
    peer.other
        .insert("scanned_by".to_string(), serde_json::json!("host:peer"));
    peer.install(f.dir()).unwrap();

    // A pass whose start predates the peer's stamp.
    let figure = ShardFigure::default();
    let verdict = install_fold_fields(f.dir(), &shard, &figure, "host:1", 1).unwrap();
    assert_eq!(
        verdict,
        s3_proxy::st_size_fold::FoldFieldInstall::DeclinedNewerScan
    );

    let after = read_shard_checkpoints(f.dir()).get(&shard).unwrap().clone();
    assert_eq!(
        u64_field(&after, "byte_sum"),
        1_234_567,
        "the peer's newer figure must survive the older pass"
    );
    assert_eq!(
        field(&after, "scanned_by"),
        Some(serde_json::json!("host:peer"))
    );

    // And the same pass, started AFTER the peer's stamp, does install — so the decline above
    // is attributable to the ordering and not to the install being broken.
    let verdict = install_fold_fields(f.dir(), &shard, &figure, "host:1", u64::MAX).unwrap();
    assert_eq!(verdict, s3_proxy::st_size_fold::FoldFieldInstall::Installed);
    let after = read_shard_checkpoints(f.dir()).get(&shard).unwrap().clone();
    assert_eq!(u64_field(&after, "byte_sum"), 0);
    assert_eq!(
        field(&after, "scanned_by"),
        Some(serde_json::json!("host:1"))
    );
}

/// A pass that runs out of budget installs whole shards and leaves the rest untouched.
///
/// The shard is the unit of abortability: the budget is checked before a shard is visited, so
/// there is no such state as a half-installed shard. What a mid-fold abort leaves is a
/// mixed-age sum — some shards fresh, some carrying their previous figure — which is
/// well-defined and visible in the per-shard ages.
///
/// The assertion is that the shards it DID reach carry complete figures, not that a particular
/// number of shards was reached: how many fit in a 0-length budget is a timing fact, and
/// asserting on it would make this a flake.
///
/// A failure implicates the budget check at the top of `run_st_size_fold`'s shard loop.
#[tokio::test]
async fn a_budget_abort_leaves_whole_shards_and_a_mixed_age_sum() {
    let f = Fixture::new();

    // A one-nanosecond budget: the first check may pass, but the pass cannot reach both shards.
    let outcome = run_st_size_fold(f.dir(), "host:1", std::time::Duration::from_nanos(1)).await;
    assert!(
        outcome.installed.len() < 2,
        "a 1ns budget must not complete a two-shard volume: {:?}",
        outcome.installed
    );

    // Whatever it reached is complete — no half figures.
    let set = read_shard_checkpoints(f.dir());
    for shard in set.shards() {
        let c = set.get(&shard).unwrap();
        assert_eq!(
            u64_field(c, "byte_sum"),
            f.expected_bytes[&shard],
            "shard {shard} was installed, so its figure must be the whole shard's"
        );
        assert_eq!(u64_field(c, "range_count"), f.expected_ranges[&shard]);
    }
    // And a shard it did not reach has no document at all — its figure is absent rather than
    // a partial one, which is the whole point of installing per shard.
    assert!(
        set.shards().len() <= 2,
        "no document may exist for a shard that does not exist"
    );

    // A second, unbounded pass completes the volume — so the abort is slower convergence and
    // not a terminal state.
    let outcome = run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;
    assert!(!outcome.budget_exhausted);
    let set = read_shard_checkpoints(f.dir());
    assert_eq!(set.shards().len(), 2);
    for shard in set.shards() {
        assert_eq!(
            u64_field(set.get(&shard).unwrap(), "byte_sum"),
            f.expected_bytes[&shard]
        );
    }
}

/// Shard enumeration is the union of both trees, so a shard with only `.meta` files is folded.
///
/// A shard whose range files have all been evicted still has objects to count. Enumerating from
/// `ranges/` alone would leave such a shard carrying its previous figure forever — the one kind
/// of staleness the per-shard ages cannot show, because an unvisited shard is
/// indistinguishable from one visited and found unchanged.
///
/// A failure implicates `enumerate_shards`' two-root loop.
#[tokio::test]
async fn a_shard_with_metadata_but_no_range_files_is_still_folded() {
    let f = Fixture::new();
    seed_meta(f.dir(), "bucket", "cc", "555", "drained.meta");

    run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;
    let set = read_shard_checkpoints(f.dir());
    let c = set
        .get(&Shard::new("bucket", "cc"))
        .expect("a metadata-only shard must be enumerated and folded");
    assert_eq!(u64_field(c, "entry_count"), 1);
    assert_eq!(u64_field(c, "range_count"), 0);
    assert_eq!(
        u64_field(c, "byte_sum"),
        0,
        "a FOLDED zero — present, because this pass did visit the shard and found no ranges"
    );
    assert!(
        c.other.contains_key("byte_sum"),
        "and it is present as a key, which is what distinguishes it from a field this fold \
         cannot compute"
    );
}

/// `metadata/`'s non-bucket siblings are not mistaken for buckets.
///
/// `_shard_state`, `_journals` and `_write_ledger` live beside the bucket directories and hold
/// no cache entries. A fold that treated them as buckets would install checkpoints for shards
/// no cache key can map to, and those documents would then be summed into the global figure.
///
/// A failure implicates the `starts_with('_')` guard in `enumerate_shards`.
#[tokio::test]
async fn the_shard_state_siblings_are_not_folded_as_buckets() {
    let f = Fixture::new();
    // The `_shard_state` tree the checkpoints themselves live in, plus a decoy shaped exactly
    // like a shard directory.
    let decoy = f
        .dir()
        .join("metadata")
        .join("_write_ledger")
        .join("ab")
        .join("cde");
    std::fs::create_dir_all(&decoy).unwrap();
    std::fs::write(decoy.join("host.meta"), b"{}").unwrap();

    run_st_size_fold(f.dir(), "host:1", std::time::Duration::ZERO).await;
    let set = read_shard_checkpoints(f.dir());
    assert_eq!(
        set.shards().len(),
        2,
        "only the two real shards: {:?}",
        set.shards()
    );
    assert!(set.get(&Shard::new("_write_ledger", "ab")).is_none());
}
