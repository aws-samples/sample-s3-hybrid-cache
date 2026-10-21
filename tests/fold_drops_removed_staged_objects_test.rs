//! The staged fold's figure must fall when staged bytes leave the write tier, through the
//! production removal paths, within the number of fold passes the design promises.
//!
//! Unit `FOLD-DROPS-DELETED-AND-GRADUATED-OBJECTS`. The fold is the write-cache
//! authority on the fleet since 2026-09-25 (epoch 1), and two fleet readings raised the
//! question of whether its figure falls at all: saw it unchanged across "the next fold"
//! after a DELETE, and saw it rise across a suite whose cleanup deletes everything it
//! wrote. Neither reading established that the deleted object's SHARD had been revisited.
//!
//! # What removes a contribution, traced
//!
//! The fold never subtracts anything. Each visit to a shard recomputes that shard's
//! `staged_byte_sum` from scratch: every ledger candidate in the shard is read against its
//! `.meta` (`write_ledger::assess_staged_candidate`), and only an `Evictable` verdict
//! contributes. So a removal leaves the figure only when the fold next VISITS the shard, and
//! the verdict then carries it out:
//!
//! | Removal | What the next visit sees |
//! |---|---|
//! | DELETE (single or multipart) | no `.meta` → `MetadataAbsent` → 0 |
//! | graduation (first GET) | `is_write_cached == false` → `Graduated` → 0 |
//! | staging eviction | no `.meta` → `MetadataAbsent` → 0 |
//! | re-PUT | the new `.meta`'s own staged bytes, once |
//!
//! A post-cut pending credit is counted by the READER regardless of the `.meta`, so a credit
//! whose object was removed before its shard's first fold also stays in the figure until that
//! shard's visit moves the cut past it. Either way the bound is one visit of the object's
//! shard, and the design promises one visit per shard per rotation: `shards_per_pass` is
//! `ceil(observed / passes_per_window)`, so a fixture of at most [`SWEEP_PASSES`] shards gets a
//! budget of one and is swept in at most that many passes. Each test below asserts within that
//! many passes, and asserts the figure fell by EXACTLY the object's staged compressed bytes,
//! read from its `.meta` before the removal.
//!
//! # The case that is not bounded by that
//!
//! Staging eviction is the one removal whose NEXT DECISION reads the figure. At epoch 1 the
//! byte arm of `evict_staging_tier` compares the fold's figure against the trigger, and an
//! eviction pass does not move that figure — so a second pass before the evicted objects'
//! shards are revisited sees the same over-trigger figure and evicts again, down past the
//! target. `a_second_staging_pass_before_the_next_fold_does_not_evict_again` pins that.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::staged_fold::{
    collect_staged_fold_gauges, dirty_shard_count, read_gauge_inputs, read_shard_checkpoints,
    run_staged_fold, shard_of, Shard, StagedShardCheckpoint, MAX_AUTHORITY_AGE_SECS,
    STAGED_FOLD_INTERVAL_SECS,
};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

/// The passes one rotation takes: every shard is visited at least once inside it.
const ROTATION_PASSES: u64 = MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS;

/// Passes that sweep every shard of a fixture in this file. Every fixture holds at most
/// `_SHARDS + 1` shards, under `ROTATION_PASSES`, so its budget is one visit a pass and a sweep
/// is one pass per shard. Running a whole 288-pass rotation instead proves nothing more and
/// costs a full fold per pass.
const SWEEP_PASSES: u64 = 64;
const _: () = assert!((SHARDS as u64) < SWEEP_PASSES && SWEEP_PASSES <= ROTATION_PASSES);

/// A manager with a real `max_cache_size`, fully initialised so the journal consolidator,
/// the write ledger and the write-cache manager (which staging eviction needs) are all wired.
async fn manager(cache_dir: &Path, max_cache_size: u64) -> CacheManager {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let m = CacheManager::new_with_shared_storage(
        cache_dir.to_path_buf(),
        false,
        0,
        max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(315_360_000), // get_ttl
        Duration::from_secs(3600),        // head_ttl
        Duration::from_secs(3600),        // put_ttl
        false,
        SharedStorageConfig::default(),
        10.0, // write_cache_percent: the Staging_Bound is a tenth of max_cache_size
        true, // write_cache_enabled
        Duration::from_secs(86_400),
        MetadataCacheConfig::default(),
        95,
        80,
        true,
        Duration::from_secs(60),
        1_048_576,
        false,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    );
    // Installs the journal components, as production's startup does before `initialize`.
    let _ = m.create_configured_disk_cache_manager();
    m.initialize().await.expect("cache manager initialises");
    m
}

/// Incompressible bytes, so the staged compressed size is close to the body length and the
/// eviction fixture's arithmetic below has margin. The assertions never assume this: every
/// expected figure is read from the `.meta`.
fn body(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    (0..len)
        .map(|_| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (x >> 33) as u8
        })
        .collect()
}

/// The production single-part write-through PUT: pre-store invalidation, then the store,
/// which credits the ledger and the pending store through `credit_staged_range`.
async fn put(m: &CacheManager, cache_key: &str, data: &[u8]) {
    m.invalidate_cache_unified_for_operation(cache_key, "PUT")
        .await
        .unwrap();
    m.store_put_as_write_cached_range_with_ttl(
        cache_key,
        data,
        "\"scn2-etag\"".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        Some("application/octet-stream".to_string()),
        HashMap::new(),
        Duration::from_secs(3600),
    )
    .await
    .expect("write-cache store succeeds");
}

fn meta_path(cache_dir: &Path, cache_key: &str) -> std::path::PathBuf {
    get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta").unwrap()
}

/// The object's staged compressed bytes, from its `.meta` — the figure the fold counts.
fn staged_bytes_on_disk(cache_dir: &Path, cache_key: &str) -> u64 {
    let raw = std::fs::read_to_string(meta_path(cache_dir, cache_key)).unwrap();
    let meta: NewCacheMetadata = serde_json::from_str(&raw).unwrap();
    meta.staged_compressed_size()
}

/// `write_cache.staged_fold_bytes` as `/metrics` publishes it: `F + post-cut pending + P`.
async fn staged_fold_bytes(cache_dir: &Path) -> u64 {
    let (pending, set) = read_gauge_inputs(cache_dir).await;
    collect_staged_fold_gauges(
        cache_dir,
        0,
        &pending,
        &set,
        s3_proxy::signed_put_handler::in_progress_part_bytes(cache_dir),
    )
    .await
    .staged_fold_bytes
    .expect("the fold figure must be readable in this fixture")
}

/// Enough fold passes by this instance to visit every shard of a fixture ([`SWEEP_PASSES`]).
async fn fold_rotation(m: &CacheManager) {
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let instance = ledger.instance_id().to_string();
    for _ in 0..SWEEP_PASSES {
        let _ = run_staged_fold(m.get_cache_dir(), &ledger, &instance).await;
    }
}

/// The figure fell by exactly `expected_drop` between the two readings.
fn assert_falls_by(cache_dir: &Path, before: u64, after: u64, expected_drop: u64, case: &str) {
    println!("SCN2 {case}: before={before} after={after} expected_drop={expected_drop}");
    assert!(expected_drop > 0, "{case}: fixture staged nothing");
    assert_eq!(
        before.saturating_sub(after),
        expected_drop,
        "{case}: staged_fold_bytes must fall by exactly the object's staged compressed \
         bytes within one sweep ({SWEEP_PASSES} passes). before={before} after={after} \
         expected_drop={expected_drop} checkpoints={:?}",
        read_shard_checkpoints(cache_dir).staged_byte_sum()
    );
}

/// Case 1: DELETE of a never-read single-part staged object.
#[tokio::test(flavor = "multi_thread")]
async fn delete_of_a_single_part_staged_object_leaves_the_fold_within_one_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    let key = "test-bucket/scn2/delete-single.bin";
    put(&m, "test-bucket/scn2/keep.bin", &body(512 * 1024, 7)).await;
    put(&m, key, &body(2 * 1024 * 1024, 1)).await;
    let staged = staged_bytes_on_disk(dir, key);
    fold_rotation(&m).await;
    let before = staged_fold_bytes(dir).await;

    m.invalidate_cache_unified_for_operation(key, "DELETE")
        .await
        .unwrap();
    assert!(
        !meta_path(dir, key).exists(),
        "fixture: DELETE removes the .meta"
    );
    fold_rotation(&m).await;
    let after = staged_fold_bytes(dir).await;
    assert_falls_by(dir, before, after, staged, "single-part DELETE");
}

/// Case 1, pre-fold arm: DELETE before the object's shard was ever folded, so its bytes are
/// in the post-cut pending term rather than in `F`. The reader counts pending regardless of
/// the `.meta`, so the figure must still fall once the shard's visit moves the cut past it.
#[tokio::test(flavor = "multi_thread")]
async fn delete_before_the_first_fold_drops_the_pending_credit_within_one_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    put(&m, "test-bucket/scn2/anchor.bin", &body(256 * 1024, 11)).await;
    fold_rotation(&m).await;

    let key = "test-bucket/scn2/delete-unfolded.bin";
    put(&m, key, &body(2 * 1024 * 1024, 12)).await;
    let staged = staged_bytes_on_disk(dir, key);
    let before = staged_fold_bytes(dir).await;
    let (pending_read, set) = read_gauge_inputs(dir).await;
    let pending = collect_staged_fold_gauges(
        dir,
        0,
        &pending_read,
        &set,
        s3_proxy::signed_put_handler::in_progress_part_bytes(dir),
    )
    .await
    .staged_fold_pending_bytes
    .unwrap();
    assert!(
        pending >= staged,
        "fixture: the unfolded object must be in the pending term (pending={pending}, \
         staged={staged})"
    );

    m.invalidate_cache_unified_for_operation(key, "DELETE")
        .await
        .unwrap();
    assert_eq!(
        staged_fold_bytes(dir).await,
        before,
        "before any fold, the post-cut pending credit is still counted — this is the \
         bounded over-statement, recorded so the next line is a real transition"
    );
    fold_rotation(&m).await;
    let after = staged_fold_bytes(dir).await;
    assert_falls_by(dir, before, after, staged, "DELETE before first fold");
}

/// Case 2: DELETE of a multipart staged object — one `.meta` with one staged range per part,
/// and one ledger entry plus pending credit per part, the shape `CompleteMultipartUpload`
/// leaves (the `.meta` written directly, each range credited through `record_staged_range`).
#[tokio::test(flavor = "multi_thread")]
async fn delete_of_a_multipart_staged_object_leaves_the_fold_within_one_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    put(&m, "test-bucket/scn2/keep-mpu.bin", &body(512 * 1024, 21)).await;

    let key = "test-bucket/scn2/delete-multipart.bin";
    const PART: u64 = 1024 * 1024;
    let ranges_dir = dir.join("ranges");
    let mut ranges = Vec::new();
    let mut part_ranges = HashMap::new();
    for part in 0..2u64 {
        let (start, end) = (part * PART, part * PART + PART - 1);
        let bin = get_sharded_path(&ranges_dir, key, &format!("_{}-{}.bin", start, end)).unwrap();
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, body(PART as usize, 30 + part)).unwrap();
        let rel = bin
            .strip_prefix(&ranges_dir)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let mut r = RangeSpec::new(start, end, rel, CompressionAlgorithm::Lz4, PART, PART);
        r.staged = Some(true);
        ranges.push(r);
        part_ranges.insert(part as u32 + 1, (start, end));
    }
    let mut om = ObjectMetadata::new(
        "\"scn2-mpu-2\"".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        2 * PART,
        Some("application/octet-stream".to_string()),
    );
    om.is_write_cached = true;
    om.parts_count = Some(2);
    om.part_ranges = part_ranges;
    let meta = NewCacheMetadata {
        cache_key: key.to_string(),
        object_metadata: om,
        ranges: ranges.clone(),
        expires_at: std::time::SystemTime::now() + Duration::from_secs(3600),
        ..Default::default()
    };
    let mp = meta_path(dir, key);
    std::fs::create_dir_all(mp.parent().unwrap()).unwrap();
    std::fs::write(&mp, serde_json::to_string(&meta).unwrap()).unwrap();
    let consolidator = m.get_journal_consolidator().await.unwrap();
    for r in &ranges {
        assert!(
            consolidator
                .record_staged_range(key, r.start, r.end, r.compressed_size)
                .await,
            "fixture: ledger append must land"
        );
    }
    let staged = staged_bytes_on_disk(dir, key);
    assert_eq!(staged, 2 * PART, "fixture: both parts staged");
    fold_rotation(&m).await;
    let before = staged_fold_bytes(dir).await;

    m.invalidate_cache_unified_for_operation(key, "DELETE")
        .await
        .unwrap();
    assert!(!mp.exists(), "fixture: DELETE removes the multipart .meta");
    fold_rotation(&m).await;
    let after = staged_fold_bytes(dir).await;
    assert_falls_by(dir, before, after, staged, "multipart DELETE");
}

/// Case 3: graduation on first GET. At epoch 1 the `Graduation` journal entry is a
/// cleanup-only no-op; what removes the bytes is the `.meta` flag the GET clears
/// synchronously, read by the next visit as `Graduated`.
#[tokio::test(flavor = "multi_thread")]
async fn graduation_leaves_the_fold_within_one_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    put(&m, "test-bucket/scn2/keep-grad.bin", &body(512 * 1024, 41)).await;
    let key = "test-bucket/scn2/graduate.bin";
    put(&m, key, &body(2 * 1024 * 1024, 42)).await;
    let staged = staged_bytes_on_disk(dir, key);
    fold_rotation(&m).await;
    let before = staged_fold_bytes(dir).await;

    let graduated = m.refresh_write_cache_ttl(key).await.unwrap();
    assert!(graduated, "fixture: the first GET graduates the object");
    assert_eq!(
        staged_bytes_on_disk(dir, key),
        0,
        "fixture: .meta no longer staged"
    );
    fold_rotation(&m).await;
    let after = staged_fold_bytes(dir).await;
    assert_falls_by(dir, before, after, staged, "graduation");
}

/// Case 5: a re-PUT of the same key. The figure must end at the NEW copy's staged bytes,
/// not at old + new: the old copy leaves and the new one is counted once.
#[tokio::test(flavor = "multi_thread")]
async fn a_re_put_is_counted_once_at_the_new_copys_size_within_one_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    put(&m, "test-bucket/scn2/keep-reput.bin", &body(512 * 1024, 51)).await;
    let key = "test-bucket/scn2/reput.bin";
    put(&m, key, &body(3 * 1024 * 1024, 52)).await;
    let old = staged_bytes_on_disk(dir, key);
    fold_rotation(&m).await;
    let before = staged_fold_bytes(dir).await;

    put(&m, key, &body(1024 * 1024, 53)).await;
    let new = staged_bytes_on_disk(dir, key);
    assert!(new > 0 && new < old, "fixture: the re-PUT is smaller");
    fold_rotation(&m).await;
    let after = staged_fold_bytes(dir).await;
    println!("SCN2 re-PUT: before={before} after={after} old={old} new={new}");
    assert_eq!(
        after,
        before - old + new,
        "re-PUT: the old copy must leave and the new copy be counted once. before={before} \
         after={after} old={old} new={new}"
    );
}

/// Write the activation epoch document, as `activate_if_ready` would.
fn activate_epoch_1(cache_dir: &Path) {
    let dir = cache_dir.join("metadata").join("_shard_state");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("staged_authority_epoch.json"),
        r#"{"schema":1,"epoch":1,"activated_at":0,"activated_by":"scn2-test","binary_version":"test"}"#,
    )
    .unwrap();
}

const EVICT_OBJECTS: usize = 4;
const EVICT_SIZE: usize = 6 * 1024 * 1024;
/// 200 MiB, so the Staging_Bound is 20 MiB, the trigger 95% of it and the target 80%.
const EVICT_MAX_CACHE: u64 = 200 * 1024 * 1024;

async fn staged_eviction_fixture(dir: &Path) -> (CacheManager, Vec<String>) {
    let m = manager(dir, EVICT_MAX_CACHE).await;
    activate_epoch_1(dir);
    let mut keys = Vec::new();
    for i in 0..EVICT_OBJECTS {
        let key = format!("test-bucket/scn2/evict-{i}.bin");
        put(&m, &key, &body(EVICT_SIZE, 60 + i as u64)).await;
        keys.push(key);
    }
    fold_rotation(&m).await;
    (m, keys)
}

fn surviving(dir: &Path, keys: &[String]) -> Vec<String> {
    keys.iter()
        .filter(|k| meta_path(dir, k).exists())
        .cloned()
        .collect()
}

/// Case 4: staging eviction removes objects, and the next rotation's figure falls by
/// exactly the evicted objects' staged bytes.
#[tokio::test(flavor = "multi_thread")]
async fn staging_eviction_leaves_the_fold_within_one_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, keys) = staged_eviction_fixture(dir).await;
    let per_key: HashMap<String, u64> = keys
        .iter()
        .map(|k| (k.clone(), staged_bytes_on_disk(dir, k)))
        .collect();
    let before = staged_fold_bytes(dir).await;
    let (trigger, _) = m.get_staging_eviction_thresholds();
    assert!(
        before > trigger,
        "fixture: the fold figure must be over the trigger (before={before}, trigger={trigger})"
    );
    assert_eq!(
        m.get_staging_resident_bytes().await,
        Some(before),
        "fixture: at epoch 1 the staging figure IS the fold's"
    );

    let freed = m.evict_staging_tier().await;
    assert!(freed > 0, "fixture: the pass evicts");
    let evicted: u64 = keys
        .iter()
        .filter(|k| !meta_path(dir, k).exists())
        .map(|k| per_key[k])
        .sum();
    fold_rotation(&m).await;
    let after = staged_fold_bytes(dir).await;
    assert_falls_by(dir, before, after, evicted, "staging eviction");
}

/// Case 4, the decision arm: a SECOND staging pass before the evicted objects' shards are
/// revisited must not evict again. The first pass reduced the tier below its target; if the
/// figure the byte arm reads has not moved, the second pass sees the pre-eviction figure,
/// still over the trigger, and evicts objects the bound never asked it to.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_staging_pass_before_the_next_fold_does_not_evict_again() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, keys) = staged_eviction_fixture(dir).await;
    let (trigger, target) = m.get_staging_eviction_thresholds();
    let before = staged_fold_bytes(dir).await;
    assert!(before > trigger, "fixture: over the trigger");

    let first = m.evict_staging_tier().await;
    let after_first = surviving(dir, &keys);
    let resident_on_disk: u64 = after_first
        .iter()
        .map(|k| staged_bytes_on_disk(dir, k))
        .sum();
    assert!(first > 0, "fixture: the first pass evicts");
    assert!(
        resident_on_disk <= trigger,
        "fixture: the first pass must leave the tier under its trigger, or a second pass \
         would be legitimate (on_disk={resident_on_disk}, trigger={trigger}, target={target})"
    );

    let second = m.evict_staging_tier().await;
    let after_second = surviving(dir, &keys);
    assert_eq!(
        (second, after_second.len()),
        (0, after_first.len()),
        "a second staging pass evicted again with the tier already under its trigger: \
         staged on disk {resident_on_disk} <= trigger {trigger}, but the byte arm read \
         {:?}. freed_first={first} freed_second={second} surviving_after_first={} \
         surviving_after_second={}",
        m.get_staging_resident_bytes().await,
        after_first.len(),
        after_second.len()
    );
}

// ───────────────────────────────────────────────────────────────────────────────────────────
// (`STAGED-FIGURE-LAGS-DELETES-AND-GRADUATIONS-BY-A-ROTATION`): the shard a removal touched
// is visited at the acting instance's NEXT pass, not when the rotation next reaches it.
//
// Every test above asserts within a full rotation, which is the bound the fold had. measured
// what that costs on the fleet: three removed objects stayed in the figure for ~20 minutes and
// left on the 11th-13th pass. The tests below build a population larger than one pass visits,
// drive the removed object's shard to the FRESHEST position so stalest-first would not reach it
// for many passes, remove the object, and run exactly ONE pass. Design § 16.6.9, "Dirty shards
// first".
//
// The last test is the other half of the trade: a dirty visit is a visit the stalest-first
// order does not get, so under a sustained load of dirty shards the stalest shard must still be
// visited inside `MAX_AUTHORITY_AGE_SECS`.
// ───────────────────────────────────────────────────────────────────────────────────────────

/// Shards in the population. `shards_per_pass(40) = ceil(40 / 288) = 1`, so one pass visits
/// 1 of 40, and a shard a pass has just visited sits behind 39 others in stalest-first order.
const SHARDS: usize = 40;

/// The per-pass budget the fold derives for the population, restated from
/// `shards_per_pass`: `ceil(observed / (MAX_AUTHORITY_AGE_SECS / STAGED_FOLD_INTERVAL_SECS))`.
fn budget_a() -> usize {
    SHARDS.div_ceil(ROTATION_PASSES as usize)
}

fn shard_of_key(key: &str) -> Shard {
    let (bucket, l1) = shard_of(key).expect("fixture keys are shardable");
    Shard::new(bucket, l1)
}

/// One key per distinct `(bucket, L1)` shard, `_SHARDS` of them, all in `test-bucket`.
fn keys_in_distinct_shards(prefix: &str) -> Vec<(Shard, String)> {
    let mut seen: BTreeMap<Shard, String> = BTreeMap::new();
    let mut i = 0u64;
    while seen.len() < SHARDS {
        let key = format!("test-bucket/{prefix}/filler-{i}.bin");
        seen.entry(shard_of_key(&key)).or_insert(key);
        i += 1;
    }
    seen.into_iter().collect()
}

/// A second key in `shard`, distinct from that shard's filler.
fn another_key_in(shard: &Shard, prefix: &str) -> String {
    (0u64..)
        .map(|i| format!("test-bucket/{prefix}/target-{i}.bin"))
        .find(|k| &shard_of_key(k) == shard)
        .expect("some key lands in every shard")
}

/// One periodic fold pass by this instance. Returns the shards it installed.
async fn one_pass(m: &CacheManager) -> Vec<Shard> {
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let instance = ledger.instance_id().to_string();
    let outcome = run_staged_fold(m.get_cache_dir(), &ledger, &instance).await;
    outcome
        .summary()
        .installed
        .iter()
        .map(|c| c.shard())
        .collect()
}

/// Where `shard` sits in the fold's stalest-first order: how many shards a pass would visit
/// before it. The fold's own ordering key is `(staged_scanned_at, shard)`.
fn stalest_first_rank(cache_dir: &Path, shard: &Shard) -> usize {
    let set = read_shard_checkpoints(cache_dir);
    let own = (
        set.get(shard)
            .expect("the shard has a checkpoint")
            .staged_scanned_at,
        shard.clone(),
    );
    set.iter()
        .filter(|(s, c)| (c.staged_scanned_at, (*s).clone()) < own)
        .count()
}

/// The population: one small staged filler in each of `_SHARDS` shards, plus a 2 MiB
/// staged target in the first shard, all folded; then passes run until one installs the
/// target's shard, which leaves that shard the freshest. Returns the target's shard and key.
///
/// The precondition the red side rests on is asserted rather than assumed: the target's shard
/// must sit at or behind rank `budget` in stalest-first order, so a stalest-first pass does not
/// visit it next.
async fn fixture_a(m: &CacheManager, prefix: &str) -> (Shard, String) {
    let dir = m.get_cache_dir().to_path_buf();
    let fillers = keys_in_distinct_shards(prefix);
    for (i, (_, key)) in fillers.iter().enumerate() {
        put(m, key, &body(4 * 1024, 200 + i as u64)).await;
    }
    let target_shard = fillers[0].0.clone();
    let target = another_key_in(&target_shard, prefix);
    put(m, &target, &body(2 * 1024 * 1024, 199)).await;
    fold_rotation(m).await;

    let mut reached = false;
    for _ in 0..(2 * SWEEP_PASSES) {
        if one_pass(m).await.contains(&target_shard) {
            reached = true;
            break;
        }
    }
    assert!(reached, "fixture: no pass installed the target's shard");
    assert_eq!(
        read_shard_checkpoints(&dir).shards().len(),
        SHARDS,
        "fixture: the observed shard population the budget is derived from"
    );
    let rank = stalest_first_rank(&dir, &target_shard);
    assert!(
        rank >= budget_a(),
        "fixture: the target's shard must not be among the {} a stalest-first pass visits next \
         (rank {rank})",
        budget_a()
    );
    (target_shard, target)
}

fn assert_left_in_one_pass(
    before: u64,
    after: u64,
    expected_drop: u64,
    visited: &[Shard],
    target_shard: &Shard,
    case: &str,
) {
    println!(
        "SCN1 {case}: before={before} after={after} expected_drop={expected_drop} \
         budget={} target_shard_visited={}",
        budget_a(),
        visited.contains(target_shard)
    );
    assert!(expected_drop > 0, "{case}: fixture staged nothing");
    assert_eq!(
        before.saturating_sub(after),
        expected_drop,
        "{case}: staged_fold_bytes must fall by exactly the object's staged compressed bytes at \
         the NEXT pass of the instance that removed it, though stalest-first would not reach its \
         shard for many passes. before={before} after={after} expected_drop={expected_drop} \
         visited={visited:?} target_shard={target_shard:?}"
    );
}

/// DELETE of a staged object whose shard was just folded: gone from the figure after one pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_leaves_the_fold_at_the_next_pass_though_its_shard_is_not_next_in_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    let (target_shard, target) = fixture_a(&m, "scn1-del").await;
    let staged = staged_bytes_on_disk(dir, &target);
    let before = staged_fold_bytes(dir).await;

    m.invalidate_cache_unified_for_operation(&target, "DELETE")
        .await
        .unwrap();
    assert!(
        !meta_path(dir, &target).exists(),
        "fixture: DELETE removes the .meta"
    );
    let visited = one_pass(&m).await;
    let after = staged_fold_bytes(dir).await;
    assert_left_in_one_pass(before, after, staged, &visited, &target_shard, "DELETE");
}

/// Graduation on first GET: gone from the figure after one pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_graduation_leaves_the_fold_at_the_next_pass_though_its_shard_is_not_next_in_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    let (target_shard, target) = fixture_a(&m, "scn1-grad").await;
    let staged = staged_bytes_on_disk(dir, &target);
    let before = staged_fold_bytes(dir).await;

    assert!(
        m.refresh_write_cache_ttl(&target).await.unwrap(),
        "fixture: the first GET graduates the object"
    );
    assert_eq!(
        staged_bytes_on_disk(dir, &target),
        0,
        "fixture: .meta no longer staged"
    );
    let visited = one_pass(&m).await;
    let after = staged_fold_bytes(dir).await;
    assert_left_in_one_pass(before, after, staged, &visited, &target_shard, "graduation");
}

/// Re-PUT: after one pass the figure holds the new copy once, not old + new.
#[tokio::test(flavor = "multi_thread")]
async fn a_re_put_is_counted_once_after_the_next_pass_though_its_shard_is_not_next_in_rotation() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    let (target_shard, target) = fixture_a(&m, "scn1-reput").await;
    let old = staged_bytes_on_disk(dir, &target);
    let before = staged_fold_bytes(dir).await;

    put(&m, &target, &body(1024 * 1024, 198)).await;
    let new = staged_bytes_on_disk(dir, &target);
    assert!(new > 0 && new < old, "fixture: the re-PUT is smaller");
    let visited = one_pass(&m).await;
    let after = staged_fold_bytes(dir).await;
    println!(
        "SCN1 re-PUT: before={before} after={after} old={old} new={new} target_shard_visited={}",
        visited.contains(&target_shard)
    );
    assert_eq!(
        after,
        before - old + new,
        "re-PUT: after the next pass the old copy must have left and the new copy be counted \
         once. before={before} after={after} old={old} new={new} visited={visited:?}"
    );
}

/// Age every installed checkpoint by `secs`, as if that much time passed between two passes.
///
/// The fold judges ages in real seconds and a test's passes are milliseconds apart, so the
/// sustained-load test below advances a simulated clock instead of waiting. It ages by one second
/// LESS than the fold interval, so the milliseconds each real pass takes cannot push a shard over
/// the bound by accumulation: the simulated clock runs slightly slow, never fast.
fn age_every_checkpoint(cache_dir: &Path, secs: u64) {
    let set = read_shard_checkpoints(cache_dir);
    for (shard, _) in set.iter() {
        let mut c = StagedShardCheckpoint::load(cache_dir, shard)
            .unwrap()
            .expect("listed checkpoint loads");
        c.staged_scanned_at = c.staged_scanned_at.saturating_sub(secs * 1_000_000_000);
        c.install(cache_dir).unwrap();
    }
}

/// Under a sustained, adversarial dirty load — every pass, the shards the PREVIOUS pass just
/// visited are dirtied again by a re-PUT, so a dirty visit is always a wasted visit — the stalest
/// shard must still be visited inside `MAX_AUTHORITY_AGE_SECS`.
///
/// This is the test the dirty share can fail. A pass that spent its whole budget on dirty shards
/// would never reach the rest: each simulated pass ages every checkpoint by 299 s, so the shards
/// not dirtied would pass the 86,400 s bound at simulated pass 290. The test runs
/// `ROTATION_PASSES + 8` passes, just past that point.
#[tokio::test(flavor = "multi_thread")]
async fn the_stalest_shard_stays_inside_the_age_bound_under_a_sustained_dirty_load() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    let fillers = keys_in_distinct_shards("scn1-load");
    let by_shard: BTreeMap<Shard, String> = fillers.iter().cloned().collect();
    for (i, (_, key)) in fillers.iter().enumerate() {
        put(&m, key, &body(4 * 1024, 300 + i as u64)).await;
    }
    fold_rotation(&m).await;
    let mut last_visited = one_pass(&m).await;

    let sim_passes = ROTATION_PASSES + 8;
    assert!(
        sim_passes * (STAGED_FOLD_INTERVAL_SECS - 1) > MAX_AUTHORITY_AGE_SECS,
        "fixture: the simulated time must exceed one window, or a starved shard cannot breach"
    );
    let mut max_stalest = 0u64;
    for pass in 0..sim_passes {
        age_every_checkpoint(dir, STAGED_FOLD_INTERVAL_SECS - 1);
        for shard in &last_visited {
            put(&m, &by_shard[shard], &body(4 * 1024, 1_000 + pass)).await;
        }
        let stalest = read_shard_checkpoints(dir)
            .stalest_age_secs()
            .expect("every shard has a folded checkpoint");
        max_stalest = max_stalest.max(stalest);
        assert!(
            stalest <= MAX_AUTHORITY_AGE_SECS,
            "simulated pass {pass}: the stalest shard is {stalest}s old against a \
             {MAX_AUTHORITY_AGE_SECS}s bound, under a dirty load of {} re-PUT shard(s) per pass \
             and a budget of {} per pass",
            last_visited.len(),
            budget_a()
        );
        last_visited = one_pass(&m).await;
        assert!(
            !last_visited.is_empty(),
            "simulated pass {pass} installed nothing"
        );
    }
    println!(
        "SCN1 sustained dirty load: {sim_passes} simulated passes, budget {}, max stalest age \
         {max_stalest}s (bound {MAX_AUTHORITY_AGE_SECS}s)",
        budget_a()
    );
}

// ───────────────────────────────────────────────────────────────────────────────────────────
// (`GRADUATION-RECORDED-NO-DIRTY-MARK`): the backfill graduation path.
//
// S3's PutObject response carries no `Last-Modified`, so a write-through entry stores none, and
// `refresh_write_cache_ttl` defers graduation while `effective_last_modified()` is `None`. The
// first GET then revalidates, and on the `304` `backfill_write_cache_last_modified` records the
// header AND graduates. That second producer recorded no dirty mark, and at epoch 1 it still
// appended a `Graduation` journal entry whose failure turned a successful graduation into `Err`.
// Saw it on the fleet: two DELETEs and one graduating GET left the dirty gauge at 2.
// ───────────────────────────────────────────────────────────────────────────────────────────

/// A write-through PUT as it lands against real S3: the stored entry has no `Last-Modified`.
async fn put_without_last_modified(m: &CacheManager, cache_key: &str, data: &[u8]) {
    m.invalidate_cache_unified_for_operation(cache_key, "PUT")
        .await
        .unwrap();
    m.store_put_as_write_cached_range_with_ttl(
        cache_key,
        data,
        "\"scn2-etag\"".to_string(),
        String::new(),
        Some("application/octet-stream".to_string()),
        HashMap::new(),
        Duration::from_secs(3600),
    )
    .await
    .expect("write-cache store succeeds");
}

/// `Graduation` journal lines naming `cache_key`, over every instance's journal file.
fn graduation_journal_entries_for(cache_dir: &Path, cache_key: &str) -> usize {
    let dir = cache_dir.join("metadata").join("_journals");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return 0;
    };
    let quoted_key = serde_json::to_string(cache_key).unwrap();
    entries
        .flatten()
        .filter(|e| e.path().to_string_lossy().ends_with(".journal"))
        .map(|e| {
            std::fs::read_to_string(e.path())
                .unwrap_or_default()
                .lines()
                .filter(|l| l.contains("\"Graduation\"") && l.contains(&quoted_key))
                .count()
        })
        .sum()
}

/// At epoch 1, a staged entry with no `Last-Modified` graduated through the backfill path marks
/// its shard, writes no `Graduation` journal entry, and leaves the figure at the next pass though
/// its shard is not next in rotation.
///
/// # RED SIDE
///
/// Without the mark in `backfill_write_cache_last_modified` the target's shard is not visited and
/// the figure does not move; without the epoch gate one `Graduation` entry is appended.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_graduation_at_epoch_1_marks_its_shard_and_writes_no_journal_entry() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let m = manager(dir, 10 * 1024 * 1024 * 1024).await;
    activate_epoch_1(dir);
    assert!(
        s3_proxy::staged_authority::fold_authority_active(dir),
        "fixture: epoch 1"
    );
    let (target_shard, target) = fixture_a(&m, "scn3-backfill").await;

    // Re-stage the target as a real-S3 write-through lands, then let one pass consume the
    // re-PUT's own mark so the graduation's mark is the only one left.
    put_without_last_modified(&m, &target, &body(2 * 1024 * 1024, 197)).await;
    one_pass(&m).await;
    assert_eq!(
        dirty_shard_count(dir),
        0,
        "fixture: the re-PUT's mark is consumed"
    );
    let rank = stalest_first_rank(dir, &target_shard);
    assert!(
        rank >= budget_a(),
        "fixture: the target's shard must not be next in rotation (rank {rank})"
    );
    let raw = std::fs::read_to_string(meta_path(dir, &target)).unwrap();
    let meta: NewCacheMetadata = serde_json::from_str(&raw).unwrap();
    assert!(
        meta.object_metadata.is_write_cached
            && meta.object_metadata.effective_last_modified().is_none(),
        "fixture: a staged entry with no effective Last-Modified"
    );
    assert!(
        !m.refresh_write_cache_ttl(&target).await.unwrap(),
        "fixture: the mainline graduation defers without a Last-Modified"
    );

    let staged = staged_bytes_on_disk(dir, &target);
    let before = staged_fold_bytes(dir).await;
    let journal_before = graduation_journal_entries_for(dir, &target);
    assert!(
        m.backfill_write_cache_last_modified(
            &target,
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            Some("\"scn2-etag\""),
        )
        .await
        .expect("backfill succeeds"),
        "fixture: the 304 backfill persists and graduates"
    );
    assert_eq!(staged_bytes_on_disk(dir, &target), 0, "fixture: graduated");
    let dirty = dirty_shard_count(dir);
    let journal_added = graduation_journal_entries_for(dir, &target) - journal_before;
    let visited = one_pass(&m).await;
    let after = staged_fold_bytes(dir).await;
    println!(
        "SCN3 backfill graduation: dirty_after_backfill={dirty} graduation_journal_entries_added=\
         {journal_added}"
    );
    assert_left_in_one_pass(
        before,
        after,
        staged,
        &visited,
        &target_shard,
        "backfill graduation",
    );
    assert_eq!(
        dirty, 1,
        "the backfill graduation must mark its shard dirty"
    );
    assert_eq!(
        journal_added, 0,
        "at epoch 1 the fold is the accounting: no Graduation journal entry may be written"
    );
}
