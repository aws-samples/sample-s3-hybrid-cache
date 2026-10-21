//! The per-`(shard, instance)` write-ledger layout, its migration, and the sequence floor.
//!
//! # Why these live in `tests/` rather than beside the module
//!
//! Every assertion here reads the **installed** shard checkpoint — `staged_byte_sum`,
//! `staged_range_count`, `staged_fold_cut`, back off disk — or the **record set**
//! `read_for_authority` returns. `staged_fold.rs`'s
//! `the_fold_is_the_only_staged_authority_and_it_has_one_decision_surface` greps `src/*.rs` for
//! those gauge symbols and permits exactly two files to name them, and that containment is worth
//! more than the convenience of reaching a `pub(crate)` path helper. So the layout is expressed
//! here the way an external reader must express it: from the public `shard_of` plus the
//! documented directory shape.
//!
//! # What is deliberately NOT asserted: a file count
//!
//! "N shard ledgers exist" passes on a tree where the fold reads none of them, reads only the
//! first, or reads them twice. The predicate that matters is what `run_staged_fold` installs.
//!
//! # Fixture shape
//!
//! Two instance identities across three shards, one shard fed by BOTH. Two instances is the
//! minimum that makes the cut's per-instance keying observable; a shard fed by both is the
//! minimum that makes a per-`(shard, instance)` cut distinguishable from a per-shard scalar;
//! three shards so a pass can visit a strict subset. A single-instance fixture exercises the
//! file naming and none of the accounting.
//!
//! Spec: cache-eviction-at-scale § 16.6.12.
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::staged_fold::{
    read_shard_checkpoints, run_staged_fold, shard_of, FoldRetainReason, Shard,
};
use s3_proxy::write_ledger::{WriteLedger, WriteLedgerEntry};
use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// `metadata/_write_ledger/{bucket}/{l1}/{instance}.ledger`.
///
/// Derived from the public `shard_of` rather than from a `pub(crate)` helper, deliberately: this
/// is the on-disk contract under test, so an external statement of it is the assertion.
fn shard_ledger_path(cache_dir: &Path, cache_key: &str, instance_id: &str) -> PathBuf {
    let (bucket, l1) = shard_of(cache_key).expect("the fixture keys are shardable");
    cache_dir
        .join("metadata")
        .join("_write_ledger")
        .join(bucket)
        .join(l1)
        .join(format!("{instance_id}.ledger"))
}

/// `metadata/_write_ledger/{instance}.ledger` — the pre-split flat path.
fn legacy_ledger_path(cache_dir: &Path, instance_id: &str) -> PathBuf {
    cache_dir
        .join("metadata")
        .join("_write_ledger")
        .join(format!("{instance_id}.ledger"))
}

/// `metadata/_write_ledger/{instance}.ledger.mark` — the persistent sequence floor.
fn seq_mark_path(cache_dir: &Path, instance_id: &str) -> PathBuf {
    cache_dir
        .join("metadata")
        .join("_write_ledger")
        .join(format!("{instance_id}.ledger.mark"))
}

fn shard_of_key(cache_key: &str) -> Shard {
    let (bucket, l1) = shard_of(cache_key).unwrap();
    Shard::new(bucket, l1)
}

/// Write a staged `.meta` holding `ranges` as `(start, end, compressed_size)`.
///
/// Ranges are back-dated 5 s so a ledger entry stamped now is not judged superseded by the copy
/// it describes — which would make every figure below zero, and every comparison of two figures a
/// comparison of two zeros.
fn seed_staged_ranges(cache_dir: &Path, cache_key: &str, ranges: &[(u64, u64, u64)]) {
    let meta_path = get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta").unwrap();
    std::fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
    let at = SystemTime::now() - Duration::from_secs(5);
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            is_write_cached: true,
            content_length: ranges.iter().map(|(_, _, b)| *b).sum(),
            ..Default::default()
        },
        ranges: ranges
            .iter()
            .map(|(start, end, bytes)| RangeSpec {
                start: *start,
                end: *end,
                file_path: format!("r_{}_{start}-{end}.bin", cache_key.replace('/', "%2F")),
                compression_algorithm: CompressionAlgorithm::Lz4,
                compressed_size: *bytes,
                uncompressed_size: *bytes,
                created_at: at,
                last_accessed: at,
                access_count: 0,
                staged: Some(true),
                unservable: false,
                file_identity: None,
            })
            .collect(),
        created_at: at,
        ..Default::default()
    };
    std::fs::write(&meta_path, serde_json::to_string(&metadata).unwrap()).unwrap();
}

fn seed_staged(cache_dir: &Path, cache_key: &str, bytes: u64) {
    seed_staged_ranges(cache_dir, cache_key, &[(0, bytes.saturating_sub(1), bytes)]);
}

/// Seed the `.meta` and append the ledger entry, as a credit site does.
async fn credit(cache_dir: &Path, ledger: &WriteLedger, cache_key: &str, bytes: u64) {
    seed_staged(cache_dir, cache_key, bytes);
    ledger
        .append_staged_range(cache_key, 0, bytes.saturating_sub(1), bytes)
        .await
        .unwrap();
}

/// Fold repeatedly until every visible shard has been installed.
///
/// One pass installs `shards_per_pass(observed)` shards — 1 for a handful — so a multi-shard
/// fixture needs several passes. Bounded, so a fold that installs nothing fails an assertion
/// rather than hanging.
async fn fold_until_settled(cache_dir: &Path, ledger: &WriteLedger, instance: &str) {
    for _ in 0..12 {
        run_staged_fold(cache_dir, ledger, instance).await;
    }
}

/// The record set an authority read returns, as
/// `(cache_key, range_start, range_end, seq, instance)`.
async fn record_set(ledger: &WriteLedger) -> BTreeSet<(String, u64, u64, Option<u64>, String)> {
    ledger
        .read_for_authority(0)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|e| {
            (
                e.cache_key,
                e.range_start,
                e.range_end,
                e.seq,
                e.instance_id,
            )
        })
        .collect()
}

/// Plant a legacy flat ledger file holding `entries`, with a schema-2 header.
fn plant_legacy_ledger(
    cache_dir: &Path,
    instance_id: &str,
    entries: &[WriteLedgerEntry],
    max_seq_ever: u64,
) {
    let path = legacy_ledger_path(cache_dir, instance_id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut body = format!("{{\"schema\":2,\"max_seq_ever\":{max_seq_ever}}}\n");
    for e in entries {
        body.push_str(&serde_json::to_string(e).unwrap());
        body.push('\n');
    }
    std::fs::write(&path, body).unwrap();
}

/// One legacy entry with an explicit seq, as a pre-split binary would have written it.
fn legacy_entry(cache_key: &str, bytes: u64, seq: u64, instance_id: &str) -> WriteLedgerEntry {
    WriteLedgerEntry {
        // Newer than the `.meta`'s back-dated range, or the entry reads as superseded and
        // contributes nothing.
        timestamp: SystemTime::now(),
        cache_key: cache_key.to_string(),
        range_start: 0,
        range_end: bytes - 1,
        compressed_size: bytes,
        instance_id: instance_id.to_string(),
        seq: Some(seq),
    }
}

/// Two instance identities across three shards, one shard fed by BOTH, and the fold's installed
/// figures and record set are correct for every one of them.
#[tokio::test]
async fn the_fold_installs_correct_totals_across_two_instances_and_three_shards() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let a = WriteLedger::new(dir.to_path_buf(), "hostA:1".to_string());
    let b = WriteLedger::new(dir.to_path_buf(), "hostB:2".to_string());

    let shared = shard_of_key("bucket/shared.bin");
    let only_a = shard_of_key("bucket/only-a.bin");
    let only_b = shard_of_key("bucket/only-b.bin");
    assert!(
        shared != only_a && shared != only_b && only_a != only_b,
        "fixture precondition: the three keys must land in three DISTINCT shards, or the \
         confinement these tests are about is untestable — got {shared} {only_a} {only_b}"
    );

    // One object with TWO staged ranges, one credited by each instance — a multipart upload split
    // across two proxies. The `.meta` carries both, so the shard's figure is 8192 over 2 ranges.
    seed_staged_ranges(
        dir,
        "bucket/shared.bin",
        &[(0, 4095, 4096), (4096, 8191, 4096)],
    );
    a.append_staged_range("bucket/shared.bin", 0, 4095, 4096)
        .await
        .unwrap();
    b.append_staged_range("bucket/shared.bin", 4096, 8191, 4096)
        .await
        .unwrap();
    credit(dir, &b, "bucket/only-b.bin", 8192).await;
    credit(dir, &a, "bucket/only-a.bin", 16384).await;

    // Both instances' ledger files live in the shared shard's directory, which is the layout.
    assert!(shard_ledger_path(dir, "bucket/shared.bin", "hostA:1").is_file());
    assert!(shard_ledger_path(dir, "bucket/shared.bin", "hostB:2").is_file());

    let records = record_set(&a).await;
    assert_eq!(records.len(), 4, "four credits, four records: {records:?}");
    assert!(records
        .iter()
        .any(|r| r.0 == "bucket/shared.bin" && r.4 == "hostA:1" && r.1 == 0 && r.3 == Some(1)));
    // Seq 1 for BOTH, because the sequence is per instance: two identities' numbers are
    // independent and incomparable, which is why the cut is keyed by instance rather than being a
    // scalar.
    assert!(records
        .iter()
        .any(|r| r.0 == "bucket/shared.bin" && r.4 == "hostB:2" && r.1 == 4096 && r.3 == Some(1)));

    let read = a.read_for_authority(0).await.unwrap();
    assert_eq!(
        read.max_seq_per_instance.len(),
        2,
        "two instance identities must produce two positions: {:?}",
        read.max_seq_per_instance
    );

    fold_until_settled(dir, &a, "hostA:1").await;
    let installed = read_shard_checkpoints(dir);

    let shared_doc = installed.get(&shared).expect("the shared shard installed");
    assert_eq!(shared_doc.staged_byte_sum, 8192);
    assert_eq!(shared_doc.staged_range_count, 2);
    assert_eq!(
        installed.get(&only_a).map(|d| d.staged_byte_sum),
        Some(16384)
    );
    assert_eq!(
        installed.get(&only_b).map(|d| d.staged_byte_sum),
        Some(8192)
    );

    // The cut is keyed at `(shard, instance)` and its SHAPE is unchanged: one entry per instance
    // whose position the read observed.
    assert_eq!(
        shared_doc.staged_fold_cut.len(),
        2,
        "the shared shard's cut names both instances: {:?}",
        shared_doc.staged_fold_cut
    );
}

/// A drained shard ledger file survives as a header-only file, contributes zero entries and zero
/// bytes, and still puts its `max_seq_ever` into the cut.
///
/// Two-sided deliberately. The file-exists-but-contributes-nothing half is what stops a file-count
/// assertion from meaning anything; the mark-still-observed half is the one that matters, because
/// dropping it lowers the cut and resurrects an already-folded credit into the pending term — a
/// double count.
///
/// A failure in the mark assertion implicates the `parsed.max_seq_ever()` observation in
/// `read_for_authority`, or `retire_identities`' pre-drop `max_seq_ever` computation.
#[tokio::test]
async fn a_header_only_shard_file_contributes_nothing_but_still_carries_its_mark() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

    // ORDER IS THE EXPERIMENT. The shard that gets DRAINED must hold the instance's HIGHEST
    // seq, or its mark is redundant with the survivor's and dropping it changes nothing — the
    // assertion would pass against a tree that ignores a header-only file entirely.
    credit(dir, &ledger, "bucket/survivor.bin", 512).await;
    credit(dir, &ledger, "bucket/drain-me.bin", 4096).await;
    let drained_shard = shard_of_key("bucket/drain-me.bin");
    let drained_path = shard_ledger_path(dir, "bucket/drain-me.bin", "host:1");
    assert_ne!(
        drained_shard,
        shard_of_key("bucket/survivor.bin"),
        "fixture precondition: two distinct shards"
    );

    let before = ledger.read_for_authority(0).await.unwrap();
    let top = *before.max_seq_per_instance.get("host:1").unwrap();

    let retire: HashSet<_> = before
        .entries
        .iter()
        .filter(|e| e.cache_key == "bucket/drain-me.bin")
        .map(|e| e.identity())
        .collect();
    ledger.retire_identities(&retire).await.unwrap();
    assert!(
        drained_path.is_file(),
        "the drained file must SURVIVE, header-only, or its mark is gone: {drained_path:?}"
    );

    let after = ledger.read_for_authority(0).await.unwrap();
    assert!(
        !after
            .entries
            .iter()
            .any(|e| e.cache_key == "bucket/drain-me.bin"),
        "the drained file contributes no entries"
    );
    assert!(
        after.is_complete(),
        "and a header-only file is not a malformed line"
    );
    assert_eq!(
        after.max_seq_per_instance.get("host:1"),
        Some(&top),
        "and its mark is still observed into the cut — dropping it lowers the cut and resurrects \
         an already-folded credit into the pending term, i.e. counts it twice"
    );

    // The other half: keeping the file is not keeping the bytes.
    fold_until_settled(dir, &ledger, "host:1").await;
    assert_eq!(
        read_shard_checkpoints(dir)
            .get(&drained_shard)
            .map(|d| d.staged_byte_sum),
        Some(0),
        "a drained shard publishes zero, not its last non-zero figure"
    );
}

/// L2 confinement: losing one shard's ledger file leaves every other shard's figure unchanged.
///
/// This is the property the per-`(shard, instance)` file set *is*, and it is what later permits a
/// lost file to be detected in `O(shards)`. It does not implement that detection.
///
/// Under the flat layout an unreadable ledger file was one instance's whole contribution, so its
/// loss was volume-wide.
#[tokio::test]
async fn deleting_one_shard_ledger_file_leaves_every_other_shards_figure_unchanged() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());

    credit(dir, &ledger, "bucket/victim.bin", 4096).await;
    credit(dir, &ledger, "bucket/bystander.bin", 65536).await;
    let victim = shard_of_key("bucket/victim.bin");
    let bystander = shard_of_key("bucket/bystander.bin");
    assert_ne!(
        victim, bystander,
        "fixture precondition: two distinct shards"
    );

    fold_until_settled(dir, &ledger, "host:1").await;
    let before = read_shard_checkpoints(dir);
    assert_eq!(before.get(&victim).map(|d| d.staged_byte_sum), Some(4096));
    assert_eq!(
        before.get(&bystander).map(|d| d.staged_byte_sum),
        Some(65536)
    );

    std::fs::remove_file(shard_ledger_path(dir, "bucket/victim.bin", "host:1")).unwrap();
    fold_until_settled(dir, &ledger, "host:1").await;

    assert_eq!(
        read_shard_checkpoints(dir)
            .get(&bystander)
            .map(|d| d.staged_byte_sum),
        Some(65536),
        "the surviving shard's figure must be untouched by the other shard's loss — that \
         confinement is what the per-shard file set buys"
    );
}

/// Converting a legacy ledger twice — the crash-before-removal case — installs exactly the figure
/// one clean conversion installs.
///
/// # Why re-running is safe rather than merely tolerable
///
/// **The ledger is an index, not a figure.** `assess_staged_candidate` reads `staged_bytes` from
/// the `.meta`'s `staged_compressed_size()` and `staged_range_count` from its own `ranges`, never
/// from an entry's `compressed_size`; and `group_by_key` collapses every entry naming one key into
/// ONE candidate, hence one `.meta` read. So a duplicated entry cannot double a byte sum or a range
/// count. This test is the executable statement of that, which matters because the conversion's
/// whole posture depends on it.
#[tokio::test]
async fn converting_a_legacy_ledger_twice_installs_the_same_figure_as_one_clean_conversion() {
    // ---- Arm A: one clean conversion.
    let clean_tmp = TempDir::new().unwrap();
    let clean_dir = clean_tmp.path();
    let clean = WriteLedger::new(clean_dir.to_path_buf(), "host:9".to_string());
    seed_staged(clean_dir, "bucket/legacy-one.bin", 4096);
    seed_staged(clean_dir, "bucket/legacy-two.bin", 8192);
    let legacy = [
        legacy_entry("bucket/legacy-one.bin", 4096, 40, "host:9"),
        legacy_entry("bucket/legacy-two.bin", 8192, 41, "host:9"),
    ];
    plant_legacy_ledger(clean_dir, "host:9", &legacy, 41);
    clean.migrate_legacy_ledgers().await;
    fold_until_settled(clean_dir, &clean, "host:9").await;
    let clean_installed = read_shard_checkpoints(clean_dir);

    // Non-vacuous first: two zeros compare equal, so the clean arm must have installed a real
    // figure before the comparison below means anything.
    assert_eq!(
        clean_installed
            .get(&shard_of_key("bucket/legacy-one.bin"))
            .map(|d| d.staged_byte_sum),
        Some(4096),
        "fixture precondition: one clean conversion must install a NON-ZERO figure"
    );

    // ---- Arm B: the same conversion interrupted before the legacy file was removed, then re-run.
    // Modelled by planting the legacy file again after a first conversion, which is exactly the
    // state a crash between step 3 and step 4 leaves — and also what a pre-upgrade peer recreates.
    let crash_tmp = TempDir::new().unwrap();
    let crash_dir = crash_tmp.path();
    let crashed = WriteLedger::new(crash_dir.to_path_buf(), "host:9".to_string());
    seed_staged(crash_dir, "bucket/legacy-one.bin", 4096);
    seed_staged(crash_dir, "bucket/legacy-two.bin", 8192);
    plant_legacy_ledger(crash_dir, "host:9", &legacy, 41);
    crashed.migrate_legacy_ledgers().await;
    plant_legacy_ledger(crash_dir, "host:9", &legacy, 41);
    crashed.migrate_legacy_ledgers().await;
    fold_until_settled(crash_dir, &crashed, "host:9").await;
    let crash_installed = read_shard_checkpoints(crash_dir);

    for key in ["bucket/legacy-one.bin", "bucket/legacy-two.bin"] {
        let shard = shard_of_key(key);
        assert_eq!(
            crash_installed.get(&shard).map(|d| d.staged_byte_sum),
            clean_installed.get(&shard).map(|d| d.staged_byte_sum),
            "a twice-converted ledger must install the SAME byte sum as a once-converted one \
             for {key}"
        );
        assert_eq!(
            crash_installed.get(&shard).map(|d| d.staged_range_count),
            clean_installed.get(&shard).map(|d| d.staged_range_count),
            "and the same range count for {key}"
        );
    }

    for dir in [clean_dir, crash_dir] {
        assert!(
            !legacy_ledger_path(dir, "host:9").exists(),
            "the legacy file is removed LAST, and it must be removed"
        );
        assert!(
            seq_mark_path(dir, "host:9").is_file(),
            "and the sequence mark must carry the legacy file's high-water mark forward"
        );
    }

    // No entry was renumbered. Renumbering would invalidate every installed cut, because a cut is
    // a threshold on exactly these numbers and they exist nowhere else.
    let records = record_set(&crashed).await;
    let seqs: BTreeSet<Option<u64>> = records.iter().map(|r| r.3).collect();
    assert_eq!(
        seqs,
        [Some(40), Some(41)].into_iter().collect(),
        "the converted entries must keep their ORIGINAL seqs: {records:?}"
    );
}

/// The fold retains rather than installing while an unconverted legacy flat ledger exists.
///
/// A legacy file's entries name no shard, so a figure installed while one exists is short by
/// whatever it holds — and under-statement makes the staging eviction trigger fire late, which is
/// the direction ruled out by name. Retaining over-states instead, which is safe.
#[tokio::test]
async fn the_fold_retains_while_a_legacy_flat_ledger_is_present() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let ledger = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
    credit(dir, &ledger, "bucket/converted.bin", 4096).await;
    fold_until_settled(dir, &ledger, "host:1").await;
    let shard = shard_of_key("bucket/converted.bin");
    assert_eq!(
        read_shard_checkpoints(dir)
            .get(&shard)
            .map(|d| d.staged_byte_sum),
        Some(4096)
    );

    // A pre-upgrade PEER appears, with its own flat file.
    seed_staged(dir, "bucket/from-old-peer.bin", 65536);
    plant_legacy_ledger(
        dir,
        "host:7",
        &[legacy_entry("bucket/from-old-peer.bin", 65536, 5, "host:7")],
        5,
    );

    let outcome = run_staged_fold(dir, &ledger, "host:1").await;
    assert_eq!(
        outcome.retain_reason(),
        Some(FoldRetainReason::LegacyLedgerPresent),
        "the pass must retain under its own name: {outcome:?}"
    );

    // Converting the peer's file clears it, and the peer's bytes then land in their own shard.
    ledger.migrate_legacy_ledgers().await;
    fold_until_settled(dir, &ledger, "host:1").await;
    assert_eq!(
        read_shard_checkpoints(dir)
            .get(&shard_of_key("bucket/from-old-peer.bin"))
            .map(|d| d.staged_byte_sum),
        Some(65536),
        "and once converted, the peer's bytes are counted in their own shard"
    );
}

/// The sequence floor keeps a REUSED instance identity above every cut it has already justified,
/// including for a shard whose ledger file does not exist.
///
/// # The case, and why a per-file header cannot cover it
///
/// An instance id is `{hostname}:{pid}` and pids are recycled, so a later process can inherit an
/// identity whose cuts are already installed. Under the flat layout that process re-read its own
/// single file and continued the sequence. Under the per-shard layout its numbers are spread over
/// up to `B x 256` files and it reads none of them, so the mark is the whole record — and a shard
/// with an installed cut and NO file of its own has no header to derive one from.
///
/// The assertion is on the fold's own arithmetic as well as on the number: a credit into a fresh
/// shard must reach that shard's installed figure. Without the floor its seq lands at or below the
/// installed cut, the pending record is filtered as already-folded, and the bytes are counted by
/// NEITHER term.
#[tokio::test]
async fn the_sequence_floor_keeps_a_reused_identity_above_every_installed_cut() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();

    // A first process under this identity credits several shards and has its cuts installed.
    let first = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
    for i in 0..4 {
        credit(dir, &first, &format!("bucket/early-{i}.bin"), 1024).await;
    }
    fold_until_settled(dir, &first, "host:1").await;
    let checkpoints = read_shard_checkpoints(dir);
    let highest_cut = checkpoints
        .shards()
        .iter()
        .filter_map(|s| {
            checkpoints
                .get(s)
                .and_then(|d| d.staged_fold_cut.get("host:1").copied())
        })
        .max()
        .unwrap_or(0);
    assert!(
        highest_cut > 0,
        "fixture precondition: a cut must actually be installed for this identity, or the floor \
         has nothing to be above"
    );

    // A LATER process reuses the identity — a recycled pid — and credits a shard with no file.
    let reused = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
    let fresh_key = "bucket/after-restart.bin";
    assert!(
        !shard_ledger_path(dir, fresh_key, "host:1").exists(),
        "fixture precondition: the target shard must have NO ledger file, which is the case a \
         per-file header cannot cover"
    );
    credit(dir, &reused, fresh_key, 4096).await;

    let issued = record_set(&reused)
        .await
        .into_iter()
        .find(|r| r.0 == fresh_key)
        .and_then(|r| r.3)
        .expect("the fresh credit was appended");
    assert!(
        issued > highest_cut,
        "a reused identity's next seq ({issued}) must be strictly above every cut it has already \
         justified ({highest_cut}) — otherwise the credit is filtered as already-folded while no \
         fold has read its .meta, and its bytes are in neither term"
    );

    fold_until_settled(dir, &reused, "host:1").await;
    assert_eq!(
        read_shard_checkpoints(dir)
            .get(&shard_of_key(fresh_key))
            .map(|d| d.staged_byte_sum),
        Some(4096),
        "and the fold agrees: the credit reaches its shard's installed figure"
    );
}
