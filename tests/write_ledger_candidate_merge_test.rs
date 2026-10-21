//! Staging eviction's candidate merge, across the per-`(shard, instance)` ledger layout.
//!
//! # What this file is for, and what it deliberately does not do
//!
//! `CacheManager::evict_staging_tier_locked` reads the write ledger through
//! `WriteLedger::read_for_authority(STAGING_EVICTION_CANDIDATE_CAP)` — the **whole-volume**
//! authority reader, capped, on a trigger rather than per pass. Of the three whole-volume readers
//! that survived the per-shard layout it is the one that **stays** whole-volume, and this file is
//! the confirmation of that rather than a change to it. Nothing in `cache.rs` was touched.
//!
//! Its scope cannot narrow the way the fold's and the completeness repair's did, and the reason is
//! not cost: the pass **ranks candidates against each other** in fleet-wide insertion order
//! (R2.3/R2.4, `order_staging_candidates`) and then frees bytes until a shortfall is met. A
//! shard-at-a-time candidate list would evict the oldest object *in one shard* while an older one
//! sat in another, and a per-shard cap would admit a prefix of each shard rather than a prefix of
//! the fleet. The ordering is the product requirement, so the read that feeds it has to see
//! everything.
//!
//! # Four claims, and the third is the one that is easy to get wrong
//!
//! 1. the merge spans **shard directories** — the two-level listing reaches every `{bucket}/{l1}/`;
//! 2. it spans **instances** within a shard, so a shard fed by two proxies contributes both files;
//! 3. the oldest-first order is **global**, so the capped prefix is the fleet's oldest entries and
//!    not the first shard's — this is the claim a per-shard reader would silently break while
//!    still returning a plausible, correctly-ordered, wrong list;
//! 4. `truncated` is **reported** rather than swallowed, which is what lets that call site tolerate
//!    the cap deliberately while refusing every other incompleteness class.
//!
//! The fixture keeps the 2-instance × 3-shard shape the layout work established, with one shard
//! fed by both instances. A single-instance fixture cannot distinguish a per-`(shard, instance)`
//! file set from a per-shard one, and a single-shard fixture exercises no merge at all.
//!
//! Spec: cache-eviction-at-scale § 16.6.12 point 2; write-cache-accounting-and-eviction R2.3, R2.6.
use s3_proxy::staged_fold::shard_of;
use s3_proxy::write_ledger::{WriteLedger, WriteLedgerEntry};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// The production cap is `STAGING_EVICTION_CANDIDATE_CAP` in `cache.rs`, which is private to that
/// module. The claims here are about the reader's behaviour at *a* cap, so a small one is used
/// where truncation must actually be reached — 10,000 entries would make the fixture the test.
const SMALL_CAP: usize = 2;

/// `metadata/_write_ledger/{bucket}/{l1}/{instance}.ledger`, stated from the public `shard_of` and
/// the documented directory shape rather than a `pub(crate)` helper.
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

/// An entry with an EXPLICIT timestamp, because three of the four claims are about order.
///
/// `append_staged_range` stamps `SystemTime::now()`, which on a fast machine gives several entries
/// the same instant and makes any ordering assertion a coin toss.
fn entry_at(cache_key: &str, secs_ago: u64, instance_id: &str) -> WriteLedgerEntry {
    WriteLedgerEntry {
        timestamp: SystemTime::now() - Duration::from_secs(secs_ago),
        cache_key: cache_key.to_string(),
        range_start: 0,
        range_end: 1023,
        compressed_size: 1024,
        instance_id: instance_id.to_string(),
        seq: None,
    }
}

/// Three shards, two instances, one shard fed by both — and every entry's age is distinct so the
/// merge order is checkable rather than incidental.
///
/// Returns the two ledgers and the keys in the order the merge must produce them, oldest first.
async fn seed_three_shards_two_instances(dir: &Path) -> (WriteLedger, WriteLedger, Vec<String>) {
    let a = WriteLedger::new(dir.to_path_buf(), "host:1".to_string());
    let b = WriteLedger::new(dir.to_path_buf(), "host:2".to_string());

    // Deliberately interleaved: the oldest entry belongs to instance B, and the shared shard holds
    // both the second-oldest and the newest. A reader that concatenated per shard, or per
    // instance, would produce a different order while still looking sorted within each group.
    a.append(entry_at("bucket/shared.bin", 20, "host:1"))
        .await
        .unwrap();
    b.append(entry_at("bucket/shared.bin", 5, "host:2"))
        .await
        .unwrap();
    a.append(entry_at("bucket/only-a.bin", 10, "host:1"))
        .await
        .unwrap();
    b.append(entry_at("bucket/only-b.bin", 30, "host:2"))
        .await
        .unwrap();

    let expected_order = vec![
        "bucket/only-b.bin".to_string(), // 30 s
        "bucket/shared.bin".to_string(), // 20 s, instance A
        "bucket/only-a.bin".to_string(), // 10 s
        "bucket/shared.bin".to_string(), // 5 s, instance B
    ];
    (a, b, expected_order)
}

/// Claims 1 and 2: the merge reaches every shard directory and every instance file within one.
///
/// The precondition assertions matter as much as the verdict. Without them a fixture whose keys
/// happened to collide into one shard, or whose second instance never wrote a file, would pass
/// this test while exercising no merge — the vacuity trap the shard-layout work kept hitting.
#[tokio::test]
async fn the_candidate_merge_spans_every_shard_directory_and_every_instance() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (a, _b, _order) = seed_three_shards_two_instances(dir).await;

    let shards: BTreeSet<_> = [
        "bucket/shared.bin",
        "bucket/only-a.bin",
        "bucket/only-b.bin",
    ]
    .iter()
    .map(|k| shard_of(k).unwrap())
    .collect();
    assert_eq!(
        shards.len(),
        3,
        "fixture precondition: three DISTINCT shards, or the two-level listing is never exercised"
    );
    for instance in ["host:1", "host:2"] {
        assert!(
            shard_ledger_path(dir, "bucket/shared.bin", instance).is_file(),
            "fixture precondition: the shared shard must carry {instance}'s own ledger file, or a \
             reader that took only the first file in a shard directory would still pass"
        );
    }

    let read = a.read_for_authority(0).await.unwrap();

    assert_eq!(
        read.files_expected, 4,
        "four shard ledger files exist — one per (shard, instance) pair that has credited"
    );
    assert_eq!(
        read.files_read, read.files_expected,
        "and every one of them must be opened: this is the field the eviction pass checks \
         individually, and a short read there means a candidate list missing staged objects"
    );
    assert!(
        read.is_complete(),
        "an undamaged fixture must read complete, or the claims below hold for a refused read"
    );

    let keys: BTreeSet<_> = read.entries.iter().map(|e| e.cache_key.clone()).collect();
    assert_eq!(
        keys,
        [
            "bucket/only-a.bin",
            "bucket/only-b.bin",
            "bucket/shared.bin"
        ]
        .iter()
        .map(|s| s.to_string())
        .collect::<BTreeSet<_>>(),
        "THE VERDICT for claims 1 and 2: every shard's entries are present. A reader that listed \
         one level, or one shard directory, returns a strict subset here"
    );
    assert_eq!(
        read.entries.len(),
        4,
        "and the shared shard contributes BOTH instances' entries, not one of them"
    );
    assert_eq!(
        read.max_seq_per_instance.keys().collect::<Vec<_>>(),
        vec!["host:1", "host:2"],
        "the per-instance high-water marks are keyed by instance across shards, which is what a \
         reader collapsing a shard's files into one would lose"
    );
}

/// Claim 3: the oldest-first order is GLOBAL across shards, so a capped read is a prefix of the
/// fleet's oldest entries.
///
/// # Why this is the claim worth the most here
///
/// The eviction pass tolerates `truncated` deliberately — the cap is its own bound — and that
/// tolerance is only safe because the prefix is the *right* prefix. A per-shard reader would hand
/// it the oldest entries of whichever shard it read first: correctly sorted, plausible, and the
/// wrong candidates. Nothing downstream could detect it, because a candidate list is a list of
/// hints and every one of them verifies against its own `.meta` before anything is deleted.
#[tokio::test]
async fn the_merge_order_is_global_oldest_first_so_a_capped_prefix_is_the_fleets_oldest() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (a, _b, expected_order) = seed_three_shards_two_instances(dir).await;

    let full = a.read_for_authority(0).await.unwrap();
    assert_eq!(
        full.entries
            .iter()
            .map(|e| e.cache_key.clone())
            .collect::<Vec<_>>(),
        expected_order,
        "THE VERDICT for claim 3: entries must be oldest-first across the whole volume. The \
         fixture interleaves ages between shards and instances precisely so a per-shard or \
         per-instance concatenation produces a different sequence here"
    );

    let capped = a.read_for_authority(SMALL_CAP).await.unwrap();
    assert_eq!(
        capped
            .entries
            .iter()
            .map(|e| e.cache_key.clone())
            .collect::<Vec<_>>(),
        expected_order[..SMALL_CAP].to_vec(),
        "and the capped read is the prefix of THAT order — the two globally oldest entries, one \
         from each of two different shards, not the first shard's two"
    );
}

/// Claim 4: the cap is reported, and it is the ONLY incompleteness the eviction pass tolerates.
///
/// A truncation that did not set the flag would be indistinguishable from a complete read, which
/// is the state the `LedgerReadOutcome` type exists to make impossible. Asserted from the other
/// side too: the same fixture read uncapped is complete, so `truncated` is tracking the cap rather
/// than a property of the fixture.
#[tokio::test]
async fn hitting_the_cap_is_reported_and_is_the_only_tolerated_incompleteness() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (a, _b, _order) = seed_three_shards_two_instances(dir).await;

    let capped = a.read_for_authority(SMALL_CAP).await.unwrap();
    assert!(
        capped.truncated,
        "hitting the cap must be reported, or a prefix is indistinguishable from the whole ledger"
    );
    assert!(
        !capped.is_complete(),
        "so `is_complete()` is false — which is exactly why the eviction pass checks the fields \
         INDIVIDUALLY rather than calling it: refusing on its own cap would disable staging \
         eviction on any ledger longer than the cap, i.e. when the tier is largest"
    );
    // The three classes that call site DOES refuse on are all clean here, so its tolerance of
    // `truncated` is the only thing standing between this read and a refusal.
    assert_eq!(capped.files_unreadable, 0);
    assert_eq!(capped.malformed_lines, 0);
    assert_eq!(capped.files_read, capped.files_expected);

    let uncapped = a.read_for_authority(0).await.unwrap();
    assert!(
        !uncapped.truncated && uncapped.is_complete(),
        "and the same fixture read uncapped is complete, so the flag tracks the cap and not the \
         fixture"
    );
}

/// A legacy flat ledger file's entries are NOT in the candidate list, and the reader says so on
/// `legacy_files_present`.
///
/// # This is a FINDING recorded as a test, not a behaviour being introduced
///
/// `read_for_authority` lists `shard_ledger_files()` and reports legacy flat files separately, so
/// during a rolling deploy an unconverted participant's staged objects are invisible to staging
/// eviction's candidate list. That call site checks `files_unreadable`, `malformed_lines` and the
/// file count individually and does **not** check `legacy_files_present`, so it proceeds on a list
/// short by whatever the legacy file holds.
///
/// **Left as it is, deliberately.** The direction is under-reclaim — the pass frees less than it
/// could, never the wrong object, because every candidate is still verified against its own
/// `.meta` before anything is deleted. Refusing instead would *disable* staging eviction for the
/// duration of a rolling deploy, which is the direction that lets the tier overflow, and the
/// window is one consolidation cycle: `migrate_legacy_ledgers` is ungated and runs before the fold
/// on every cycle.
///
/// Pinned here so that trade is a decision on the record rather than an accident, and so a future
/// change to either half fails a test that explains itself.
#[tokio::test]
async fn a_legacy_flat_ledgers_entries_are_absent_from_the_merge_and_reported_separately() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (a, _b, _order) = seed_three_shards_two_instances(dir).await;

    let legacy = legacy_ledger_path(dir, "host:9");
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    let orphan = entry_at("bucket/pre-upgrade.bin", 1, "host:9");
    std::fs::write(
        &legacy,
        format!(
            "{{\"schema\":2,\"max_seq_ever\":7}}\n{}\n",
            serde_json::to_string(&orphan).unwrap()
        ),
    )
    .unwrap();

    let read = a.read_for_authority(0).await.unwrap();

    assert_eq!(
        read.legacy_files_present, 1,
        "the reader must REPORT the unconverted participant — a caller that wants to refuse on it \
         needs a field to refuse on"
    );
    assert!(
        !read
            .entries
            .iter()
            .any(|e| e.cache_key == "bucket/pre-upgrade.bin"),
        "and its entries are absent from the candidate list, because they name no shard. This is \
         the finding: staging eviction does not check legacy_files_present, so it runs on a list \
         short by this much. Direction is under-reclaim, never a wrong deletion"
    );
    assert!(
        read.is_complete(),
        "legacy presence is deliberately NOT part of is_complete(): every other field there is \
         'this read could not see the file', and this one is 'the file is readable and names no \
         shard'. The two have different remedies — retry versus finish the deploy"
    );
    assert_eq!(
        read.entries.len(),
        4,
        "the shard-attributed entries are unaffected, so the shortfall is exactly the legacy \
         file's contents and not a corrupted read"
    );
}
