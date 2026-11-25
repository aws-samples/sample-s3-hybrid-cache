//! The staging pass as a shard-scoped, concurrent single holder: per-shard quotas from the sizing
//! derivation's weights, chunked execution with objects in flight, batch-sized debit publication
//! by the replace rule, and the debit taken from the `.meta` read under the slot lock.
use super::staging_pass_pending_lock_timeout_test::{body, put, staged_on_disk};
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager, StagingBatchPoint};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::staged_fold::{
    age_authority_snapshot_for_tests, claim_shard, run_staged_fold_over_shards,
    run_staged_fold_sampled, shard_of, ClaimOutcome, Shard, ShardWeight, ShardWeights,
    StagedRemoval, AUTHORITY_SNAPSHOT_TTL_SECS,
};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

const BOUND: Duration = Duration::from_secs(180);
const MIB: usize = 1024 * 1024;
const PEER: &str = "peer-host:9";

/// 200 MiB, so the Staging_Bound is 20 MiB, its trigger 19 MiB and its target 16 MiB.
async fn manager_with_budget(cache_dir: &Path, budget: Duration) -> CacheManager {
    manager_with(cache_dir, budget, 200 * MIB as u64).await
}

/// A manager whose `max_cache_size` is `max_cache_size`; the Staging_Bound is a tenth of it.
async fn manager_with(cache_dir: &Path, budget: Duration, max_cache_size: u64) -> CacheManager {
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
        Duration::from_secs(315_360_000),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig {
            eviction_lock_timeout: budget,
            ..Default::default()
        },
        10.0,
        true,
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
    let _ = m.create_configured_disk_cache_manager();
    m.initialize().await.expect("cache manager initialises");
    m
}

async fn manager(cache_dir: &Path) -> CacheManager {
    manager_with_budget(cache_dir, Duration::from_secs(60)).await
}

fn activate_epoch_1(cache_dir: &Path) {
    let dir = cache_dir.join("metadata").join("_shard_state");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("staged_authority_epoch.json"),
        r#"{"schema":1,"epoch":1,"activated_at":0,"activated_by":"shard-scoped-test","binary_version":"test"}"#,
    )
    .unwrap();
}

fn shard(key: &str) -> Shard {
    let (bucket, l1) = shard_of(key).expect("fixture keys name a shard");
    Shard::new(bucket, l1)
}

/// `n` keys under `prefix` whose shard `l1` satisfies `want`, in creation order.
fn keys_where(prefix: &str, n: usize, want: impl Fn(&str) -> bool) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0u64;
    while out.len() < n {
        let key = format!("test-bucket/{prefix}/object-{i}.bin");
        if want(&shard(&key).l1) {
            out.push(key);
        }
        i += 1;
        assert!(
            i < 1_000_000,
            "fixture: could not find keys in the wanted shards"
        );
    }
    out
}

/// `n` keys all in one shard.
fn keys_in_one_shard(prefix: &str, n: usize) -> Vec<String> {
    let first = shard(&format!("test-bucket/{prefix}/object-0.bin")).l1;
    keys_where(prefix, n, |l1| l1 == first)
}

/// Stage `keys` with the given sizes (incompressible), activate epoch 1, and fold every shard.
async fn stage_keys(m: &CacheManager, keys: &[String], sizes: &[usize]) {
    let dir = m.get_cache_dir().to_path_buf();
    activate_epoch_1(&dir);
    for (i, key) in keys.iter().enumerate() {
        put(m, key, &body(sizes[i % sizes.len()], 900 + i as u64)).await;
        // Distinct ledger timestamps, so the oldest-first order is the creation order.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    sweep(m).await;
}

async fn sweep(m: &CacheManager) {
    let dir = m.get_cache_dir().to_path_buf();
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let instance = ledger.instance_id().to_string();
    for _ in 0..16 {
        let _ = run_staged_fold_sampled(&dir, &ledger, &instance, None).await;
    }
}

fn meta_of(dir: &Path, key: &str) -> Option<NewCacheMetadata> {
    let path = get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

/// The staged bytes of the `.meta`s still on disk: what a fold of the volume counts.
fn census(dir: &Path, keys: &[String]) -> u64 {
    keys.iter()
        .filter_map(|k| meta_of(dir, k))
        .map(|m| m.staged_compressed_size())
        .sum()
}

async fn derived(m: &CacheManager) -> u64 {
    age_authority_snapshot_for_tests(m.get_cache_dir(), AUTHORITY_SNAPSHOT_TTL_SECS);
    m.get_staging_resident_bytes()
        .await
        .expect("a staged figure")
}

async fn instance_of(m: &CacheManager) -> String {
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    consolidator.write_ledger().instance_id().to_string()
}

async fn claim_all(dir: &Path, keys: &[String]) {
    let shards: BTreeSet<Shard> = keys.iter().map(|k| shard(k)).collect();
    for s in &shards {
        assert_ne!(
            claim_shard(dir, s, PEER).await,
            ClaimOutcome::Busy,
            "fixture: the peer must take the claim on {s}"
        );
    }
}

fn release_claims(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir.join("metadata").join("_shard_state").join("claims"));
}

fn debit_entries(dir: &Path, instance: &str) -> Vec<(Shard, u64, u64)> {
    s3_proxy::staged_fold::removal_debit_entries_for_tests(dir, instance)
        .into_iter()
        .map(|e| {
            (
                Shard::new(e.bucket.clone(), e.l1.clone()),
                e.bytes,
                e.ranges,
            )
        })
        .collect()
}

fn lock_is_free(lock_path: &Path) -> bool {
    let f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .unwrap();
    let free = fs4::FileExt::try_lock(&f).is_ok();
    if free {
        let _ = fs4::FileExt::unlock(&f);
    }
    free
}

/// Run a pass on its own task, so a hook may block its worker.
async fn spawned_pass(m: &Arc<CacheManager>) -> u64 {
    let m = Arc::clone(m);
    tokio::spawn(async move { m.evict_staging_tier().await })
        .await
        .expect("the pass must not panic")
}

/// (a) A heal strips the object's staged range between the pass's unlocked assessment and its
/// locked delete, publishing its own debit. The pass debits the `.meta` it read under the lock,
/// so the figure before any re-fold equals the census of the surviving `.meta`s. Debiting the
/// assessment's figure debits the stripped range a second time and reads low by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_heal_strip_after_the_assessment_is_debited_once() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let keys = keys_in_one_shard("strip", 8);
        stage_keys(&m, &keys, &[3 * MIB]).await;
        assert_eq!(
            derived(&m).await,
            census(&dir, &keys),
            "fixture: folded figure is the census"
        );
        let instance = instance_of(&m).await;
        let stripped_key = keys[0].clone();
        let stripped_bytes = meta_of(&dir, &stripped_key)
            .unwrap()
            .staged_compressed_size();
        CacheManager::set_staging_after_assess_hook_for_tests(
            &dir,
            &stripped_key,
            Box::new({
                let dir = dir.clone();
                let key = stripped_key.clone();
                let instance = instance.clone();
                move || {
                    // The heal strip's effect: the range leaves the `.meta` and its file goes,
                    // and the strip publishes its own debit for it.
                    let path = get_sharded_path(&dir.join("metadata"), &key, ".meta").unwrap();
                    let mut meta: NewCacheMetadata =
                        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
                    for r in &meta.ranges {
                        let _ = std::fs::remove_file(dir.join("ranges").join(&r.file_path));
                    }
                    meta.ranges.clear();
                    std::fs::write(&path, serde_json::to_vec(&meta).unwrap()).unwrap();
                    s3_proxy::staged_fold::mark_shard_dirty(
                        &dir,
                        &instance,
                        &key,
                        StagedRemoval {
                            bytes: stripped_bytes,
                            ranges: 1,
                        },
                    );
                }
            }),
        );
        claim_all(&dir, &keys).await;
        spawned_pass(&m).await;
        assert!(
            meta_of(&dir, &stripped_key).is_none(),
            "fixture: the stripped object was evicted"
        );
        let after = derived(&m).await;
        let truth = census(&dir, &keys);
        release_claims(&dir);
        assert_eq!(
            after, truth,
            "the figure before any re-fold must equal the census of the surviving .metas \
             (figure {after}, census {truth}, stripped range {stripped_bytes} bytes): a figure \
             low by the stripped range debited it twice"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// (b) Evictions run concurrently: two objects are held inside their evictions (after the locked
/// `.meta` read) at the same time. With one object in flight the second is never reached while
/// the first is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pass_has_more_than_one_eviction_in_flight() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let keys = keys_in_one_shard("concurrent", 8);
        stage_keys(&m, &keys, &[3 * MIB]).await;
        let mut reached = Vec::new();
        let mut resumes = Vec::new();
        for key in &keys[..2] {
            let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
            let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
            s3_proxy::write_cache_manager::PAUSE_EVICTION_AFTER_META_READ
                .lock()
                .unwrap()
                .push((key.clone(), (reached_tx, resume_rx)));
            reached.push(reached_rx);
            resumes.push(resume_tx);
        }
        let pass = tokio::spawn({
            let m = Arc::clone(&m);
            async move { m.evict_staging_tier().await }
        });
        let both = tokio::time::timeout(Duration::from_secs(10), async {
            for r in reached.iter_mut() {
                r.await.unwrap();
            }
        })
        .await;
        for r in resumes {
            let _ = r.send(());
        }
        pass.await.unwrap();
        assert!(
            both.is_ok(),
            "two evictions must be held in flight at once; only one was reached in 10 s, so the \
             pass evicts one object at a time"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// (c) On mixed sizes with objects in flight, the pass frees its shortfall and overshoots it by
/// less than one object, and no batch removes more than its byte cap plus one object. Dispatching
/// without counting the estimate in flight sends every candidate at once and frees them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overshoot_is_under_one_object_and_a_batch_stays_under_its_byte_cap() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let keys = keys_in_one_shard("overshoot", 24);
        let sizes = [MIB, 2 * MIB, 3 * MIB];
        stage_keys(&m, &keys, &sizes).await;
        const CAP: u64 = 4 * MIB as u64;
        CacheManager::set_staging_limits_for_tests(&dir, 32, 64, CAP);
        let (_, target) = m.get_staging_eviction_thresholds();
        let before = derived(&m).await;
        let to_free = before - target;
        let largest = keys
            .iter()
            .map(|k| meta_of(&dir, k).unwrap().staged_compressed_size())
            .max()
            .unwrap();
        let batches: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let last_census = Arc::new(Mutex::new(before));
        CacheManager::set_staging_batch_hook_for_tests(
            &dir,
            Arc::new({
                let dir = dir.clone();
                let keys = keys.clone();
                let batches = Arc::clone(&batches);
                let last = Arc::clone(&last_census);
                move |point| {
                    if let StagingBatchPoint::BeforePublish { .. } = point {
                        let now = census(&dir, &keys);
                        let mut last = last.lock().unwrap();
                        batches.lock().unwrap().push(*last - now);
                        *last = now;
                    }
                }
            }),
        );
        spawned_pass(&m).await;
        let freed = before - census(&dir, &keys);
        let batches = batches.lock().unwrap().clone();
        assert!(
            freed >= to_free && freed - to_free < largest,
            "freed {freed} against a shortfall of {to_free}: the overshoot must be under one \
             object ({largest} bytes); batches removed {batches:?}"
        );
        assert!(
            batches.iter().all(|&b| b < CAP + largest),
            "a batch removed more than its {CAP}-byte cap plus one object: {batches:?}"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// Stage a multipart object at `key`: one `.meta` naming `parts` staged ranges of `part` bytes,
/// and one ledger entry and pending credit per part, in part order, the shape
/// `CompleteMultipartUpload` leaves.
async fn stage_multipart(m: &CacheManager, key: &str, parts: u64, part: u64, seed: u64) {
    let dir = m.get_cache_dir().to_path_buf();
    let ranges_dir = dir.join("ranges");
    let mut ranges = Vec::new();
    let mut part_ranges = HashMap::new();
    for p in 0..parts {
        let (start, end) = (p * part, p * part + part - 1);
        let bin = get_sharded_path(&ranges_dir, key, &format!("_{start}-{end}.bin")).unwrap();
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, body(part as usize, seed + p)).unwrap();
        let rel = bin
            .strip_prefix(&ranges_dir)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let mut r = RangeSpec::new(start, end, rel, CompressionAlgorithm::Lz4, part, part);
        r.staged = Some(true);
        ranges.push(r);
        part_ranges.insert(p as u32 + 1, (start, end));
    }
    let mut om = ObjectMetadata::new(
        format!("\"mpu-{seed}-{parts}\""),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        parts * part,
        Some("application/octet-stream".to_string()),
    );
    om.is_write_cached = true;
    om.parts_count = Some(parts as u32);
    om.part_ranges = part_ranges;
    let meta = NewCacheMetadata {
        cache_key: key.to_string(),
        object_metadata: om,
        ranges: ranges.clone(),
        expires_at: std::time::SystemTime::now() + Duration::from_secs(3600),
        ..Default::default()
    };
    let mp = get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
    std::fs::create_dir_all(mp.parent().unwrap()).unwrap();
    std::fs::write(&mp, serde_json::to_string(&meta).unwrap()).unwrap();
    let consolidator = m.get_journal_consolidator().await.unwrap();
    for r in &ranges {
        assert!(
            consolidator
                .record_staged_range(key, r.start, r.end, r.compressed_size)
                .await,
            "fixture: the ledger append must land"
        );
    }
}

/// (c) on multipart objects at a prefix boundary. Every shard of a chunk holds a multipart
/// object whose parts run past the shard's share, so each shard's prefix read stops part-way
/// through the object's entries and its ledger estimate counts only the parts read, while its
/// eviction removes the whole object. The pass still overshoots its shortfall by less than one
/// object, and no batch removes more than its byte cap plus one object, because each delete is
/// admitted by the assessed `.meta`'s staged figure. Admitting by the ledger estimate dispatches a
/// chunk's objects on one or two parts each and removes several whole objects past the allowance.
async fn multipart_prefix_boundary_pass(cap: u64) {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        // 1280 MiB, so the Staging_Bound is 128 MiB, its trigger 121.6 MiB and its target
        // 102.4 MiB, against 64 objects of 2 MiB.
        let m = Arc::new(manager_with(&dir, Duration::from_secs(60), 1280 * MIB as u64).await);
        activate_epoch_1(&dir);
        // One object in each of 64 contiguous shards, so the plan is 8 chunks of 8 shards and a
        // shard's share (about 0.4 MiB) is under one object (2 MiB in 8 parts of 256 KiB).
        const PART: u64 = 256 * 1024;
        const PARTS: u64 = 8;
        let mut keys = Vec::new();
        for l1 in 0..64u32 {
            let want = format!("{l1:02x}");
            keys.extend(keys_where(&format!("mpu-{want}"), 1, |s| s == want));
        }
        for (i, key) in keys.iter().enumerate() {
            stage_multipart(&m, key, PARTS, PART, 5_000 + 100 * i as u64).await;
        }
        let consolidator = m.get_journal_consolidator().await.expect("consolidator");
        let ledger = consolidator.write_ledger().clone();
        let instance = ledger.instance_id().to_string();
        let shards: BTreeSet<Shard> = keys.iter().map(|k| shard(k)).collect();
        let _ = run_staged_fold_over_shards(&dir, &ledger, &instance, &shards).await;
        CacheManager::set_staging_limits_for_tests(&dir, 32, 64, cap);
        let (trigger, target) = m.get_staging_eviction_thresholds();
        let before = derived(&m).await;
        assert!(
            before > trigger,
            "fixture: the tier must be over its trigger ({before} against {trigger})"
        );
        let to_free = before - target;
        let largest = PARTS * PART;
        assert!(
            to_free / 64 < largest / 2,
            "fixture: a shard's share ({} bytes) must end inside its object's parts",
            to_free / 64
        );
        let batches: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let last_census = Arc::new(Mutex::new(census(&dir, &keys)));
        CacheManager::set_staging_batch_hook_for_tests(
            &dir,
            Arc::new({
                let dir = dir.clone();
                let keys = keys.clone();
                let batches = Arc::clone(&batches);
                let last = Arc::clone(&last_census);
                move |point| {
                    if let StagingBatchPoint::BeforePublish { .. } = point {
                        let now = census(&dir, &keys);
                        let mut last = last.lock().unwrap();
                        batches.lock().unwrap().push(*last - now);
                        *last = now;
                    }
                }
            }),
        );
        let census_before = census(&dir, &keys);
        spawned_pass(&m).await;
        let freed = census_before - census(&dir, &keys);
        let batches = batches.lock().unwrap().clone();
        assert!(
            freed >= to_free && freed - to_free < largest,
            "freed {freed} against a shortfall of {to_free}: the overshoot must be under one \
             object ({largest} bytes); batches removed {batches:?}"
        );
        assert!(
            batches.iter().all(|&b| b < cap + largest),
            "a batch removed more than its {cap}-byte cap plus one object ({largest} bytes): \
             {batches:?}"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// The multipart fixture at the production batch cap, 64 MiB, which never binds here. Admitting by
/// the ledger estimate dispatches most of a chunk's objects on two parts each, and the pass frees
/// more than one object past its shortfall.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_objects_at_a_prefix_boundary_overshoot_by_under_one_object() {
    multipart_prefix_boundary_pass(64 * 1024 * 1024).await;
}

/// The multipart fixture at a 2 MiB batch cap. A batch filled by ledger estimate (two parts,
/// 512 KiB, a candidate) takes four whole objects, 8 MiB, against the cap plus one object, 4 MiB.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_objects_at_a_prefix_boundary_keep_each_batch_under_its_byte_cap() {
    multipart_prefix_boundary_pass(2 * 1024 * 1024).await;
}

/// A count pass whose shard's share is used up by graduated residue at the shard's head reads
/// only that residue, reclaims nothing and has not seen the evictable object behind it, so it
/// must not latch the count trigger. The residue retires once the pass releases the lock, and the
/// next pass reaches the object and evicts it. A pass that latches holds the count verdict at
/// `NotNeeded` until the next fold install, so the next pass never takes the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_pass_that_read_only_head_residue_does_not_latch_and_the_next_pass_evicts() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        activate_epoch_1(&dir);
        let keys = keys_in_one_shard("residue", 4);
        let (residue, object) = (&keys[..3], &keys[3]);
        for (i, key) in keys.iter().enumerate() {
            put(&m, key, &body(64 * 1024, 300 + i as u64)).await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // Graduate the residue, and age its ledger entries past the retirement grace, so a pass
        // that reads them retires them.
        for key in residue {
            let path = get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
            let mut meta: NewCacheMetadata =
                serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            meta.object_metadata.is_write_cached = false;
            std::fs::write(&path, serde_json::to_vec(&meta).unwrap()).unwrap();
        }
        let s = shard(object);
        let shard_dir = dir
            .join("metadata")
            .join("_write_ledger")
            .join(&s.bucket)
            .join(&s.l1);
        let aged = std::time::SystemTime::now() - Duration::from_secs(600);
        let mut aged_entries = 0;
        for file in std::fs::read_dir(&shard_dir).unwrap() {
            let path = file.unwrap().path();
            if path.extension().is_none_or(|x| x != "ledger") {
                continue;
            }
            let mut out = String::new();
            for line in std::fs::read_to_string(&path).unwrap().lines() {
                match serde_json::from_str::<s3_proxy::write_ledger::WriteLedgerEntry>(line) {
                    Ok(mut e) if residue.contains(&e.cache_key) => {
                        e.timestamp = aged;
                        aged_entries += 1;
                        out.push_str(&serde_json::to_string(&e).unwrap());
                    }
                    _ => out.push_str(line),
                }
                out.push('\n');
            }
            std::fs::write(&path, out).unwrap();
        }
        assert_eq!(
            aged_entries, 3,
            "fixture: one ledger entry per residue object"
        );
        // The count arm over its trigger, with this shard's share of it two or three ranges: a
        // peer shard in a bucket sorting after it carries nearly all the weight and no ledger.
        s3_proxy::staged_fold::seed_shard_figure_for_tests(&dir, &s.bucket, &s.l1, 0, 10);
        s3_proxy::staged_fold::seed_shard_figure_for_tests(
            &dir,
            "zzz-bucket",
            "00",
            0,
            s3_proxy::staged_fold::STAGED_RANGE_BUDGET,
        );
        s3_proxy::staged_fold::invalidate_budget_snapshot_for_tests(&dir);
        assert!(
            s3_proxy::staged_fold::count_eviction_verdict(&dir, 95, 80)
                .await
                .is_needed(),
            "fixture: the count trigger must be over"
        );
        let meta_path = get_sharded_path(&dir.join("metadata"), object, ".meta").unwrap();
        spawned_pass(&m).await;
        assert!(
            meta_path.exists(),
            "fixture: the first pass's share must end inside the residue, before the object"
        );
        assert!(
            s3_proxy::staged_fold::count_eviction_verdict(&dir, 95, 80)
                .await
                .is_needed(),
            "a pass that read only the residue at its shard's head latched the count trigger \
             off, though an evictable object sat behind it"
        );
        spawned_pass(&m).await;
        assert!(
            !meta_path.exists(),
            "the next pass must reach the object behind the retired residue and evict it"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// (d) The pass opens only the ledger files of the shards that carry a quota. A shard whose
/// checkpoint nets to zero gets none, so its ledger file is never read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pass_reads_only_the_ledger_files_of_quota_shards() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        // Eight quota shards (`l1` 00 to 07), so the weights carry the shortfall on their own.
        let quota_keys: Vec<String> = (0u16..8)
            .flat_map(|v| {
                let want = format!("{v:02x}");
                keys_where(&format!("quota-{v}"), 2, move |l1| l1 == want)
            })
            .collect();
        let other_keys = keys_where("other", 4, |l1| {
            u16::from_str_radix(l1, 16).is_ok_and(|v| v >= 8)
        });
        let all: Vec<String> = quota_keys
            .iter()
            .chain(other_keys.iter())
            .cloned()
            .collect();
        stage_keys(&m, &all, &[3 * MIB]).await;
        // The other shards' checkpoints carry nothing and cut past every credit, so the figure
        // and the weights give them nothing.
        let instance = instance_of(&m).await;
        for k in &other_keys {
            let s = shard(k);
            s3_proxy::staged_fold::seed_shard_cuts_for_tests(
                &dir,
                &s.bucket,
                &s.l1,
                &[(instance.as_str(), u64::MAX)],
            );
        }
        let ledger_root = dir.join("metadata").join("_write_ledger");
        let before =
            s3_proxy::write_ledger::authority_files_read_under_for_tests(&ledger_root).len();
        spawned_pass(&m).await;
        let read = s3_proxy::write_ledger::authority_files_read_under_for_tests(&ledger_root);
        let quota_dirs: Vec<_> = quota_keys
            .iter()
            .map(|k| ledger_root.join("test-bucket").join(shard(k).l1))
            .collect();
        let outside: Vec<_> = read[before..]
            .iter()
            .filter(|p| !quota_dirs.iter().any(|d| p.starts_with(d)))
            .collect();
        assert!(
            staged_on_disk(&dir, &quota_keys).len() < quota_keys.len(),
            "fixture: the pass must evict from the quota shard"
        );
        assert!(
            outside.is_empty(),
            "the pass opened ledger files of shards with no quota: {outside:?}"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// (e) Quotas sum exactly to the shortfall on both arms; a shortfall under 2 GiB over enough
/// weighted shards spans at least eight chunks; no chunk spans a bucket or a shard without
/// weight; and the uniform fallback shares the shortfall evenly.
#[test]
fn quotas_sum_exactly_and_a_small_shortfall_spans_eight_chunks() {
    let mut weights = ShardWeights::new();
    for (bucket, l1s) in [("a", 0u16..40), ("b", 100u16..130)] {
        for l1 in l1s {
            if l1 % 7 == 3 {
                continue; // a gap: a shard with no weight
            }
            weights.insert(
                Shard::new(bucket, format!("{l1:02x}")),
                ShardWeight {
                    bytes: 1_000_003 + l1 as u64 * 7_919,
                    ranges: 11 + l1 as u64 % 5,
                },
            );
        }
    }
    let (bytes, ranges) = (1_073_741_827u64, 9_973u64);
    let plan = s3_proxy::cache::staging_chunk_plan(bytes, ranges, &weights, &[], 0);
    let sum_b: u64 = plan.iter().map(|c| c.quota_bytes).sum();
    let sum_r: u64 = plan.iter().map(|c| c.quota_ranges).sum();
    assert_eq!(
        (sum_b, sum_r),
        (bytes, ranges),
        "quotas must sum exactly to the shortfall"
    );
    assert!(
        plan.len() >= 8,
        "a sub-2 GiB shortfall must span at least 8 chunks, got {}",
        plan.len()
    );
    for c in &plan {
        assert!(
            c.shards.iter().all(|s| s.bucket == c.shards[0].bucket),
            "a chunk spans buckets"
        );
        assert!(
            c.shards.iter().all(|s| weights.contains_key(s)),
            "a chunk holds an unweighted shard"
        );
        for w in c.shards.windows(2) {
            let (a, b) = (
                u16::from_str_radix(&w[0].l1, 16).unwrap(),
                u16::from_str_radix(&w[1].l1, 16).unwrap(),
            );
            assert_eq!(
                a + 1,
                b,
                "a chunk's shards must be contiguous: {:?}",
                c.shards
            );
        }
    }
    // Contiguous weights, so the runs alone do not reach eight: the floor does.
    let contiguous: ShardWeights = (0u16..40)
        .map(|l1| {
            (
                Shard::new("c", format!("{l1:02x}")),
                ShardWeight {
                    bytes: 1_000_000,
                    ranges: 10,
                },
            )
        })
        .collect();
    let plan = s3_proxy::cache::staging_chunk_plan(300 * 1024 * 1024, 0, &contiguous, &[], 0);
    assert!(
        plan.len() >= 8,
        "a 300 MiB shortfall over 40 contiguous shards must span at least 8 chunks, got {}",
        plan.len()
    );
    let uniform: Vec<Shard> = (0..20)
        .map(|i| Shard::new("u", format!("{i:02x}")))
        .collect();
    let plan = s3_proxy::cache::staging_chunk_plan(1_000, 0, &ShardWeights::new(), &uniform, 0);
    let shares: Vec<u64> = plan.iter().map(|c| c.quota_bytes).collect();
    assert_eq!(
        shares.iter().sum::<u64>(),
        1_000,
        "uniform shares must sum: {shares:?}"
    );
    let per_shard: Vec<f64> = plan
        .iter()
        .map(|c| c.quota_bytes as f64 / c.shards.len() as f64)
        .collect();
    assert!(
        per_shard.iter().all(|&s| (s - 50.0).abs() <= 1.0),
        "uniform weights must share evenly per shard: {per_shard:?}"
    );
}

/// (e) end to end. On a volume where no shard has a folded figure yet (each shard's checkpoint
/// nets to zero and every staged byte is a credit after its cut), each shard carries weight by its
/// post-cut credits, and the pass frees the shortfall. Weights from the checkpoints alone are empty
/// here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_volume_with_no_folded_figure_is_evicted_by_its_post_cut_credits() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        activate_epoch_1(&dir);
        let keys = keys_where("uniform", 8, |_| true);
        for key in &keys {
            let s = shard(key);
            s3_proxy::staged_fold::seed_shard_figure_for_tests(&dir, &s.bucket, &s.l1, 0, 0);
        }
        for (i, key) in keys.iter().enumerate() {
            put(&m, key, &body(3 * MIB, 1_700 + i as u64)).await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let (trigger, target) = m.get_staging_eviction_thresholds();
        let before = derived(&m).await;
        assert!(
            before > trigger,
            "fixture: the tier must be over its trigger ({before} against {trigger})"
        );
        let to_free = before - target;
        let census_before = census(&dir, &keys);
        spawned_pass(&m).await;
        let freed = census_before - census(&dir, &keys);
        assert!(
            freed >= to_free,
            "a pass on a volume where no shard has a folded figure freed {freed} of a \
             {to_free}-byte shortfall ({} of {} objects left): its shards carry weight by their \
             post-cut credits",
            staged_on_disk(&dir, &keys).len(),
            keys.len()
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// (f) Across a multi-chunk pass with several batches per chunk, the debit document never holds
/// more than one entry per shard, nor more entries than the executing and previous chunks'
/// shards, and no cap merge runs. Appending per batch gives a shard one entry per batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_debit_document_holds_one_entry_per_shard_of_the_last_two_chunks() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        // Sixteen contiguous shards, so the shortfall spans eight chunks of two shards.
        let keys = keys_where("multichunk", 192, |l1| {
            u16::from_str_radix(l1, 16).is_ok_and(|v| v < 16)
        });
        stage_keys(&m, &keys, &[128 * 1024]).await;
        CacheManager::set_staging_limits_for_tests(&dir, 32, 3, 64 * MIB as u64);
        let instance = instance_of(&m).await;
        let violations: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let samples = Arc::new(Mutex::new(0u64));
        // (executing chunk, its shard count, the previous chunk's shard count)
        let chunks = Arc::new(Mutex::new((0u64, 0usize, 0usize)));
        CacheManager::set_staging_batch_hook_for_tests(
            &dir,
            Arc::new({
                let dir = dir.clone();
                let violations = Arc::clone(&violations);
                let samples = Arc::clone(&samples);
                let chunks = Arc::clone(&chunks);
                move |point| {
                    if let StagingBatchPoint::AfterPublish { chunk, chunk_shards, batch } = point {
                        let entries = debit_entries(&dir, &instance);
                        let shards: BTreeSet<Shard> = entries.iter().map(|e| e.0.clone()).collect();
                        let mut c = chunks.lock().unwrap();
                        if c.0 != chunk {
                            *c = (chunk, chunk_shards, c.1);
                        }
                        let bound = c.1 + c.2;
                        *samples.lock().unwrap() += 1;
                        if entries.len() != shards.len() || entries.len() > bound {
                            violations.lock().unwrap().push(format!(
                                "chunk {chunk} batch {batch}: {} entries over {} shards, bound {bound}",
                                entries.len(),
                                shards.len()
                            ));
                        }
                    }
                }
            }),
        );
        spawned_pass(&m).await;
        let samples = *samples.lock().unwrap();
        let violations = violations.lock().unwrap().clone();
        assert!(samples >= 8, "fixture: the pass must publish several batches, got {samples}");
        assert!(
            violations.is_empty(),
            "the debit document must hold one entry per shard of the executing and previous \
             chunks: {violations:?}"
        );
        assert_eq!(
            s3_proxy::staged_fold::removal_debit_merges_for_tests(&dir),
            0,
            "the debit document reached its cap and merged during a staging pass"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// (g) A pass that dies after batch k's evictions and before its publish leaves the debits of
/// batches 1 to k-1 on the volume. Publishing once per pass leaves none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pass_killed_in_batch_three_leaves_the_first_two_batches_debited() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let keys = keys_in_one_shard("killed", 12);
        stage_keys(&m, &keys, &[3 * MIB]).await;
        let before = census(&dir, &keys);
        CacheManager::set_staging_limits_for_tests(&dir, 32, 2, 64 * MIB as u64);
        let instance = instance_of(&m).await;
        let after_two = Arc::new(Mutex::new(None::<u64>));
        CacheManager::set_staging_batch_hook_for_tests(
            &dir,
            Arc::new({
                let dir = dir.clone();
                let keys = keys.clone();
                let after_two = Arc::clone(&after_two);
                move |point| match point {
                    StagingBatchPoint::AfterPublish { batch: 2, .. } => {
                        *after_two.lock().unwrap() = Some(before - census(&dir, &keys));
                    }
                    StagingBatchPoint::BeforePublish { batch: 3 } => {
                        panic!("injected: the process dies in batch 3, before its publish");
                    }
                    _ => {}
                }
            }),
        );
        let pass = tokio::spawn({
            let m = Arc::clone(&m);
            async move { m.evict_staging_tier().await }
        })
        .await;
        assert!(pass.is_err(), "fixture: the pass must die in batch 3");
        let expected = after_two
            .lock()
            .unwrap()
            .expect("fixture: batch 2 was published");
        let published: u64 = debit_entries(&dir, &instance).iter().map(|e| e.1).sum();
        assert!(expected > 0, "fixture: batches 1 and 2 removed objects");
        assert_eq!(
            published, expected,
            "the volume must hold the debits of batches 1 and 2 ({expected} bytes) after a death \
             in batch 3; it holds {published}"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// (h) An own install of the chunk's shard after batch 2 prunes the shard's entry. Batch 3 then
/// publishes only its own removals as the one entry for the shard, in both orders (h1: the
/// consolidation cycle's flush writes first; h2: batch 3's own write is the first), and batch 4
/// replaces it with batches 3 and 4. The figure before the final re-fold equals the census.
/// One batch's reading: (batch, the debit document's entries, the census on disk).
type Reading = (u64, Vec<(Shard, u64, u64)>, u64);

async fn replace_after_an_own_install(flush_first: bool) {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let keys = keys_in_one_shard(if flush_first { "h1" } else { "h2" }, 12);
        stage_keys(&m, &keys, &[3 * MIB]).await;
        CacheManager::set_staging_limits_for_tests(&dir, 32, 2, 64 * MIB as u64);
        let consolidator = m.get_journal_consolidator().await.expect("consolidator");
        let ledger = consolidator.write_ledger().clone();
        let instance = ledger.instance_id().to_string();
        let s = shard(&keys[0]);
        let at_install = Arc::new(Mutex::new(0u64));
        let at_batch3 = Arc::new(Mutex::new(0u64));
        let readings: Arc<Mutex<Vec<Reading>>> = Arc::new(Mutex::new(Vec::new()));
        CacheManager::set_staging_batch_hook_for_tests(
            &dir,
            Arc::new({
                let dir = dir.clone();
                let keys = keys.clone();
                let ledger = Arc::clone(&ledger);
                let instance = instance.clone();
                let s = s.clone();
                let at_install = Arc::clone(&at_install);
                let at_batch3 = Arc::clone(&at_batch3);
                let readings = Arc::clone(&readings);
                move |point| {
                    let StagingBatchPoint::AfterPublish { batch, .. } = point else {
                        return;
                    };
                    let on_disk = census(&dir, &keys);
                    if batch == 2 {
                        let shards: BTreeSet<Shard> = [s.clone()].into_iter().collect();
                        tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current().block_on(run_staged_fold_over_shards(
                                &dir, &ledger, &instance, &shards,
                            ))
                        });
                        if flush_first {
                            s3_proxy::staged_fold::flush_removal_debits(&dir, &instance);
                        }
                        *at_install.lock().unwrap() = on_disk;
                    }
                    if batch == 3 {
                        *at_batch3.lock().unwrap() = on_disk;
                    }
                    readings
                        .lock()
                        .unwrap()
                        .push((batch, debit_entries(&dir, &instance), on_disk));
                }
            }),
        );
        // The final re-fold must not install, so the figure is read with the debits applied.
        CacheManager::set_before_staging_refold_hook_for_tests(
            &dir,
            Box::new({
                let dir = dir.clone();
                let s = s.clone();
                move || {
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                            assert_ne!(claim_shard(&dir, &s, PEER).await, ClaimOutcome::Busy);
                        })
                    });
                }
            }),
        );
        spawned_pass(&m).await;
        let readings = readings.lock().unwrap().clone();
        let at_install = *at_install.lock().unwrap();
        let at_batch3 = *at_batch3.lock().unwrap();
        let entry_at = |batch: u64| -> Vec<(Shard, u64, u64)> {
            readings
                .iter()
                .find(|r| r.0 == batch)
                .map(|r| r.1.clone())
                .unwrap_or_else(|| panic!("fixture: no batch {batch}; readings {readings:?}"))
        };
        let batch3_removed = at_install - at_batch3;
        let after3 = entry_at(3);
        assert_eq!(
            after3
                .iter()
                .map(|e| (e.0.clone(), e.1))
                .collect::<Vec<_>>(),
            vec![(s.clone(), batch3_removed)],
            "after batch 3 the document must hold one entry for the shard with batch 3's \
             removals alone ({batch3_removed} bytes); readings {readings:?}"
        );
        if readings.iter().any(|r| r.0 == 4) {
            let at_batch4 = readings.iter().find(|r| r.0 == 4).unwrap().2;
            let after4 = entry_at(4);
            assert_eq!(
                after4
                    .iter()
                    .map(|e| (e.0.clone(), e.1))
                    .collect::<Vec<_>>(),
                vec![(s.clone(), at_install - at_batch4)],
                "batch 4 must replace the entry with batches 3 and 4; readings {readings:?}"
            );
        }
        let figure = derived(&m).await;
        let truth = census(&dir, &keys);
        release_claims(&dir);
        assert_eq!(
            figure, truth,
            "the figure before the final re-fold must equal the census (figure {figure}, census \
             {truth}); readings {readings:?}"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_after_an_own_install_with_the_flush_first() {
    replace_after_an_own_install(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_after_an_own_install_with_no_flush() {
    replace_after_an_own_install(false).await;
}

/// A pass the deadline stops while evictions are in flight lets them resolve, publishes their
/// batch's debits, dispatches nothing more and releases the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadline_stopped_pass_publishes_the_debits_of_the_evictions_in_flight() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager_with_budget(&dir, Duration::from_secs(1)).await);
        let keys = keys_in_one_shard("deadline", 8);
        stage_keys(&m, &keys, &[3 * MIB]).await;
        CacheManager::set_staging_limits_for_tests(&dir, 2, 64, 64 * MIB as u64);
        let instance = instance_of(&m).await;
        let held: u64 = keys[..2]
            .iter()
            .map(|k| meta_of(&dir, k).unwrap().staged_compressed_size())
            .sum();
        let mut reached = Vec::new();
        let mut resumes = Vec::new();
        for key in &keys[..2] {
            let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
            let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
            s3_proxy::write_cache_manager::PAUSE_EVICTION_AFTER_META_READ
                .lock()
                .unwrap()
                .push((key.clone(), (reached_tx, resume_rx)));
            reached.push(reached_rx);
            resumes.push(resume_tx);
        }
        let pass = tokio::spawn({
            let m = Arc::clone(&m);
            async move { m.evict_staging_tier().await }
        });
        for r in reached {
            tokio::time::timeout(Duration::from_secs(10), r)
                .await
                .expect("fixture: both evictions reached their pause")
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let mut resumes = resumes.into_iter();
        let _ = resumes.next().unwrap().send(());
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = resumes.next().unwrap().send(());
        pass.await.unwrap();
        let gone: Vec<bool> = keys.iter().map(|k| meta_of(&dir, k).is_none()).collect();
        let published: u64 = debit_entries(&dir, &instance).iter().map(|e| e.1).sum();
        assert_eq!(
            (gone[0], gone[1], published),
            (true, true, held),
            "both evictions in flight at the deadline must resolve and their debits ({held} \
             bytes) be published (gone {gone:?}, published {published})"
        );
        assert!(
            gone[2..].iter().all(|g| !g),
            "nothing may be dispatched after the deadline: gone {gone:?}"
        );
        assert!(
            lock_is_free(&m.get_global_eviction_lock_path()),
            "the pass must release global_eviction.lock"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// Seed `n` single-shard buckets that sort after `test-bucket`, `ranges` each, and return their
/// weights.
fn seed_other_buckets(dir: &Path, n: usize, ranges: u64) -> ShardWeights {
    let mut weights = ShardWeights::new();
    for i in 0..n {
        let bucket = format!("w{i:03}-bucket");
        s3_proxy::staged_fold::seed_shard_figure_for_tests(dir, &bucket, "00", 0, ranges);
        weights.insert(Shard::new(bucket, "00"), ShardWeight { bytes: 0, ranges });
    }
    weights
}

/// The count arm's shortfall in ranges, asserting the count trigger is over.
async fn count_shortfall(dir: &Path) -> u64 {
    s3_proxy::staged_fold::invalidate_budget_snapshot_for_tests(dir);
    match s3_proxy::staged_fold::count_eviction_verdict(dir, 95, 80).await {
        s3_proxy::staged_fold::CountEvictionVerdict::Needed { ranges_to_free, .. } => {
            ranges_to_free
        }
        v => panic!("fixture: the count trigger must be over, got {v:?}"),
    }
}

/// A count-only pass in the nothing-reclaimable state, with a weighted shard whose range share is
/// zero: `test-bucket`'s shard of one graduated object, weighted by bytes and no ranges, and
/// `others` single-shard buckets after it carrying the count at `ranges` each. With `neighbour`,
/// the next `l1` of `test-bucket` carries ranges too, so the zero-share shard shares a chunk with
/// a quota; without, it is the plan's first chunk, alone and with no quota. The pass asked that
/// shard for nothing, so it reclaims nothing having seen every candidate it asked for, latches the
/// count trigger, and the next cycle runs no pass under `global_eviction.lock`.
async fn zero_share_pass_latches(neighbour: bool, others: usize, ranges: u64) {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        activate_epoch_1(&dir);
        let key = keys_in_one_shard("zero-share", 1).remove(0);
        put(&m, &key, &body(64 * 1024, 4_100)).await;
        let path = get_sharded_path(&dir.join("metadata"), &key, ".meta").unwrap();
        let mut meta: NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        meta.object_metadata.is_write_cached = false;
        std::fs::write(&path, serde_json::to_vec(&meta).unwrap()).unwrap();
        let zero = shard(&key);
        let mut weights = seed_other_buckets(&dir, others, ranges);
        // The checkpoint cuts past the PUT's credit, so the pass weighs the shard by bytes and
        // no ranges, as the weights below do; with the credit after the cut it would carry a
        // range and get a share.
        let instance = instance_of(&m).await;
        s3_proxy::staged_fold::seed_shard_figure_and_cuts_for_tests(
            &dir,
            &zero.bucket,
            &zero.l1,
            1_000,
            0,
            &[(instance.as_str(), u64::MAX)],
        );
        weights.insert(
            zero.clone(),
            ShardWeight {
                bytes: 1_000,
                ranges: 0,
            },
        );
        if neighbour {
            let l1 = u16::from_str_radix(&zero.l1, 16).unwrap();
            let next = format!("{:02x}", if l1 == 0xff { l1 - 1 } else { l1 + 1 });
            s3_proxy::staged_fold::seed_shard_figure_for_tests(
                &dir,
                &zero.bucket,
                &next,
                0,
                ranges,
            );
            weights.insert(
                Shard::new(zero.bucket.clone(), next),
                ShardWeight { bytes: 0, ranges },
            );
        }
        let to_free = count_shortfall(&dir).await;
        // The plan the pass makes from these weights.
        let plan = s3_proxy::cache::staging_chunk_plan(0, to_free, &weights, &[], 0);
        let at = plan
            .iter()
            .position(|c| c.shards.contains(&zero))
            .expect("fixture: the zero-share shard is planned");
        let chunk = &plan[at];
        let own = chunk.shard_quotas[chunk.shards.iter().position(|s| s == &zero).unwrap()];
        assert_eq!(own, (0, 0), "fixture: the shard's share must be zero");
        if neighbour {
            assert!(
                chunk.shards.len() == 2 && chunk.quota_ranges > 0,
                "fixture: the shard must share a chunk that has a quota: {chunk:?}"
            );
        } else {
            assert!(
                at == 0 && chunk.shards.len() == 1 && chunk.quota_ranges == 0,
                "fixture: the shard must be the first chunk, alone, with no quota: {chunk:?}"
            );
        }
        assert_eq!(
            plan.iter().map(|c| c.quota_ranges).sum::<u64>(),
            to_free,
            "fixture: the plan must drop no runs ({} chunks)",
            plan.len()
        );
        spawned_pass(&m).await;
        // The premise, read from the plan the pass made rather than the one built above.
        let real = s3_proxy::cache::last_staging_plan_for_tests(&dir).expect("the pass planned");
        let at = real
            .iter()
            .position(|c| c.shards.contains(&zero))
            .expect("fixture: the pass planned the zero-share shard");
        let chunk = &real[at];
        let own = chunk.shard_quotas[chunk.shards.iter().position(|s| s == &zero).unwrap()];
        assert_eq!(
            own,
            (0, 0),
            "fixture: the pass gave the shard a zero share: {chunk:?}"
        );
        if neighbour {
            assert!(
                chunk.shards.len() == 2 && chunk.quota_ranges > 0,
                "fixture: in the pass's plan the shard shares a chunk with a quota: {chunk:?}"
            );
        } else {
            assert!(
                at == 0 && chunk.shards.len() == 1 && chunk.quota_ranges == 0,
                "fixture: in the pass's plan the shard is the first chunk, alone, with no \
                 quota: {chunk:?}"
            );
        }
        let first = s3_proxy::eviction_result::WriteCacheCountPassResult::load(&dir)
            .expect("fixture: the count pass published its result");
        assert_eq!(first.ranges_freed, 0, "fixture: nothing is reclaimable");
        assert!(
            !s3_proxy::staged_fold::count_eviction_verdict(&dir, 95, 80)
                .await
                .is_needed(),
            "a count pass that reclaimed nothing, with a zero-share shard {} in its plan, left the \
             count trigger unlatched, so every consolidation cycle runs another pass under \
             global_eviction.lock",
            if neighbour {
                "inside a chunk"
            } else {
                "as its first chunk"
            }
        );
        spawned_pass(&m).await;
        let second = s3_proxy::eviction_result::WriteCacheCountPassResult::load(&dir).unwrap();
        assert_eq!(
            second.passes_total, first.passes_total,
            "the next cycle ran a count pass under global_eviction.lock"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// A zero-share shard inside a chunk. 255 other buckets make 256 single-shard runs with the pair,
/// so the plan splits no run and the pair stays one chunk. Reading the shard with a zero want
/// reports its entry as left behind, and the pass as partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_pass_with_a_zero_share_shard_in_a_chunk_latches_and_the_next_cycle_runs_no_pass() {
    zero_share_pass_latches(true, 255, 130_000).await;
}

/// A first chunk with no quota, skipped as covered before any debit. 8 other buckets make 9
/// single-shard chunks, and the zero-share shard's sorts first. Counting the skip as partial
/// keeps the latch clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_pass_whose_first_chunk_has_no_quota_latches_and_the_next_cycle_runs_no_pass() {
    zero_share_pass_latches(false, 8, 4_000_000).await;
}

/// A candidate assessed evictable while the deletes already admitted cover the allowance is held
/// for the next batch: an admitted delete can still decline. Here the covering object is replaced
/// between its assessment and its delete, so the delete declines and debits nothing, and the next
/// batch evicts the held object. Dropping it leaves the object on disk while the pass evicts a
/// younger one in its place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_candidate_held_back_by_a_covering_delete_that_declines_is_evicted_in_the_same_pass() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let keys = keys_in_one_shard("covered", 5);
        // 9 MiB then four of 3 MiB: 21 MiB against the 19 MiB trigger, so the shortfall to the
        // 16 MiB target is about 5 MiB. The first object alone covers it; the second does not.
        stage_keys(&m, &keys, &[9 * MIB, 3 * MIB, 3 * MIB, 3 * MIB, 3 * MIB]).await;
        let (covering, held) = (keys[0].clone(), keys[1].clone());
        let (_, target) = m.get_staging_eviction_thresholds();
        let to_free = derived(&m).await - target;
        let covering_bytes = meta_of(&dir, &covering).unwrap().staged_compressed_size();
        let held_bytes = meta_of(&dir, &held).unwrap().staged_compressed_size();
        assert!(
            covering_bytes >= to_free && held_bytes < to_free,
            "fixture: the first object ({covering_bytes}) must cover the shortfall ({to_free}) \
             and the second ({held_bytes}) must not"
        );
        // The covering object's ledger estimate is one byte, so the executor dispatches its
        // assessment beside the held object's.
        let s = shard(&covering);
        let shard_dir = dir
            .join("metadata")
            .join("_write_ledger")
            .join(&s.bucket)
            .join(&s.l1);
        let mut rewritten = 0;
        for file in std::fs::read_dir(&shard_dir).unwrap() {
            let path = file.unwrap().path();
            if path.extension().is_none_or(|x| x != "ledger") {
                continue;
            }
            let mut out = String::new();
            for line in std::fs::read_to_string(&path).unwrap().lines() {
                match serde_json::from_str::<s3_proxy::write_ledger::WriteLedgerEntry>(line) {
                    Ok(mut e) if e.cache_key == covering => {
                        e.compressed_size = 1;
                        rewritten += 1;
                        out.push_str(&serde_json::to_string(&e).unwrap());
                    }
                    _ => out.push_str(line),
                }
                out.push('\n');
            }
            std::fs::write(&path, out).unwrap();
        }
        assert_eq!(
            rewritten, 1,
            "fixture: one ledger entry for the covering object"
        );
        CacheManager::set_staging_limits_for_tests(&dir, 2, 64, 64 * MIB as u64);
        // A different object at the covering key after its assessment: its delete declines.
        CacheManager::set_staging_after_assess_hook_for_tests(
            &dir,
            &covering,
            Box::new({
                let dir = dir.clone();
                let key = covering.clone();
                move || {
                    let path = get_sharded_path(&dir.join("metadata"), &key, ".meta").unwrap();
                    let mut meta: NewCacheMetadata =
                        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
                    meta.object_metadata.etag = "\"replaced-after-assessment\"".to_string();
                    std::fs::write(&path, serde_json::to_vec(&meta).unwrap()).unwrap();
                }
            }),
        );
        // Hold the covering delete after its locked read and the held object's assessment before
        // it returns, so that assessment comes back while the delete is admitted and unresolved.
        let (delete_reached_tx, delete_reached) = tokio::sync::oneshot::channel();
        let (delete_resume, delete_resume_rx) = tokio::sync::oneshot::channel();
        s3_proxy::write_cache_manager::PAUSE_EVICTION_AFTER_META_READ
            .lock()
            .unwrap()
            .push((covering.clone(), (delete_reached_tx, delete_resume_rx)));
        let (assess_reached, assess_resume) =
            CacheManager::pause_staging_assessment_for_tests(&dir, &held);
        let pass = tokio::spawn({
            let m = Arc::clone(&m);
            async move { m.evict_staging_tier().await }
        });
        let both = tokio::time::timeout(Duration::from_secs(30), async {
            delete_reached.await.unwrap();
            assess_reached.await.unwrap();
        })
        .await;
        let _ = assess_resume.send(());
        // The executor takes the assessment woken first; the delay is margin.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = delete_resume.send(());
        pass.await.unwrap();
        assert!(
            both.is_ok(),
            "fixture: the covering delete and the held assessment must both be in flight"
        );
        assert_eq!(
            s3_proxy::cache::staging_covered_holds_for_tests(&dir),
            1,
            "fixture: the held object must be held back exactly once as covered by the admitted \
             delete; otherwise this test does not reach the branch it is for"
        );
        assert!(
            meta_of(&dir, &covering).is_some(),
            "fixture: the replaced object must survive its declined delete"
        );
        let others_evicted = keys[2..]
            .iter()
            .filter(|k| meta_of(&dir, k).is_none())
            .count();
        assert!(
            meta_of(&dir, &held).is_none(),
            "the object held back while the covering delete was in flight is still on disk: the \
             delete declined, and the pass evicted {others_evicted} younger object(s) instead"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// A plan with more runs than a pass executes leaves the smallest, and their quota, to the next
/// pass. A count pass that reclaims nothing under such a plan has not looked at every candidate,
/// so it must not latch the count trigger. 257 single-shard buckets of equal weight make 257
/// runs, each with a share; the plan keeps 256.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_pass_whose_plan_dropped_a_run_with_a_quota_does_not_latch() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        activate_epoch_1(&dir);
        let weights = seed_other_buckets(&dir, 257, 128_000);
        let to_free = count_shortfall(&dir).await;
        let plan = s3_proxy::cache::staging_chunk_plan(0, to_free, &weights, &[], 0);
        let planned: u64 = plan.iter().map(|c| c.quota_ranges).sum();
        assert!(
            plan.len() == 256 && planned < to_free,
            "fixture: the plan must drop a run that carries a quota ({} chunks, {planned} of \
             {to_free} ranges planned)",
            plan.len()
        );
        spawned_pass(&m).await;
        let result = s3_proxy::eviction_result::WriteCacheCountPassResult::load(&dir)
            .expect("fixture: the count pass published its result");
        assert_eq!(result.ranges_freed, 0, "fixture: nothing is reclaimable");
        assert!(
            s3_proxy::staged_fold::count_eviction_verdict(&dir, 95, 80)
                .await
                .is_needed(),
            "a count pass that left runs of its plan unread ({planned} of {to_free} ranges \
             planned) latched the count trigger off"
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// One key in each of `test-bucket`'s 256 shards, in `l1` order.
fn one_key_per_l1(prefix: &str) -> Vec<String> {
    let mut by_l1: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    let mut i = 0u64;
    while by_l1.len() < 256 {
        let key = format!("test-bucket/{prefix}/object-{i}.bin");
        by_l1.entry(shard(&key).l1).or_insert(key);
        i += 1;
        assert!(i < 1_000_000, "fixture: could not cover every shard");
    }
    by_l1.into_values().collect()
}

/// Three shards' checkpoints carry most of the figure, and every shard of the bucket holds a credit
/// after its cut (bytes written since its last fold visit, or with no visit yet). The weights the
/// sizing derivation returns are each shard's terms of the figure, so all 256 shards carry weight
/// and the plan has the chunk count the shortfall asks for. Weights from the checkpoints alone
/// weigh three shards, and the plan is lowered to three chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_cut_credits_over_256_shards_plan_the_chunk_count_the_shortfall_asks_for() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = manager(&dir).await;
        activate_epoch_1(&dir);
        let keys = one_key_per_l1("spread");
        for (i, key) in keys.iter().enumerate() {
            put(&m, key, &body(4 * 1024, 5_000 + i as u64)).await;
        }
        // No cut in these checkpoints, so every credit, theirs included, stays after the cut.
        const GIB: u64 = 1024 * 1024 * 1024;
        for key in &keys[..3] {
            let s = shard(key);
            s3_proxy::staged_fold::seed_shard_figure_for_tests(
                &dir,
                &s.bucket,
                &s.l1,
                2 * GIB,
                1_000,
            );
        }
        let (_, weights) =
            s3_proxy::staged_fold::staged_authority_figure_fresh_with_weights(&dir).await;
        let weights = weights.expect("fixture: the checkpoint read is complete");
        // Twenty chunks' worth: inside [8, 256], so the plan's count is exactly that.
        let bytes_to_free = 20 * 256 * MIB as u64;
        assert!(
            !s3_proxy::cache::staging_plan_needs_uniform(bytes_to_free, 0, &weights),
            "fixture: the weights must carry the shortfall"
        );
        let plan = s3_proxy::cache::staging_chunk_plan(bytes_to_free, 0, &weights, &[], 0);
        assert_eq!(
            plan.len(),
            20,
            "a 5 GiB shortfall must plan 20 chunks; {} shards carry weight ({} of them with a \
             checkpoint), so the weights leave out the credits after each shard's cut",
            weights.len(),
            3
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// A pass under concurrent fill. Three shards at the end of the bucket are folded; every byte
/// written after them is a credit after its shard's cut, in 64 shards at the start, and PUTs keep
/// landing in other shards while the pass runs. The pass plans at least `STAGING_MIN_CHUNKS` (8)
/// chunks and evicts from more than 8 shards. Weights from the checkpoints alone plan one chunk
/// per folded shard and evict from those three only, whatever the shortfall.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pass_under_concurrent_fill_plans_eight_chunks_and_evicts_across_more_than_eight_shards()
{
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let spread = one_key_per_l1("fill");
        // Folded: three objects of 1 MiB in each of the last three shards, 9 MiB.
        let folded: Vec<String> = spread[253..]
            .iter()
            .enumerate()
            .flat_map(|(j, k)| {
                let want = shard(k).l1;
                keys_where(&format!("folded-{j}"), 3, move |l1| l1 == want)
            })
            .collect();
        stage_keys(&m, &folded, &[MIB]).await;
        // After the cut: 192 KiB in each of the first 64 shards, 12 MiB, so the figure is 21 MiB
        // against the 19 MiB trigger and the shortfall to the 16 MiB target about 5 MiB, which
        // the folded shards alone also carry.
        let fresh: Vec<String> = spread[..64].to_vec();
        for (i, key) in fresh.iter().enumerate() {
            put(&m, key, &body(192 * 1024, 6_000 + i as u64)).await;
        }
        let (trigger, target) = m.get_staging_eviction_thresholds();
        let before = derived(&m).await;
        assert!(
            before > trigger && before - target <= 9 * MIB as u64,
            "fixture: the figure ({before}) must be over the trigger ({trigger}) by a shortfall \
             the folded shards carry"
        );
        // Fill: 64 KiB every 20 ms into the shards between, until the pass returns.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let filler = tokio::spawn({
            let m = Arc::clone(&m);
            let stop = Arc::clone(&stop);
            let keys = spread[64..253].to_vec();
            async move {
                let mut written = 0usize;
                for (i, key) in keys.iter().enumerate() {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    put(&m, key, &body(64 * 1024, 7_000 + i as u64)).await;
                    written += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                written
            }
        });
        spawned_pass(&m).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let written = filler.await.unwrap();
        let plan = s3_proxy::cache::last_staging_plan_for_tests(&dir).expect("the pass planned");
        let evicted: BTreeSet<Shard> = folded
            .iter()
            .chain(fresh.iter())
            .filter(|k| meta_of(&dir, k).is_none())
            .map(|k| shard(k))
            .collect();
        assert!(written > 0, "fixture: PUTs must land while the pass runs");
        assert!(
            plan.len() >= 8 && evicted.len() > 8,
            "a pass under fill planned {} chunks over {} shards and evicted from {} shards \
             ({evicted:?}); it must plan at least 8 chunks and evict from more than 8 shards",
            plan.len(),
            plan.iter().map(|c| c.shards.len()).sum::<usize>(),
            evicted.len()
        );
    })
    .await
    .expect("the test must finish within its bound");
}

/// Weights that carry less than the shortfall leave the rest to the Write_Ledger's shard
/// directories: each weighted shard takes its whole weight, every ledger shard an equal part of
/// the rest, and each arm still sums exactly. A plan from the weights alone gives a ledger shard
/// with no weight nothing.
#[test]
fn a_shortfall_the_weights_do_not_carry_is_shared_over_the_ledger_shards() {
    let weights: ShardWeights = (0u16..3)
        .map(|v| {
            (
                Shard::new("a", format!("{v:02x}")),
                ShardWeight {
                    bytes: 100,
                    ranges: 1,
                },
            )
        })
        .collect();
    let ledger: Vec<Shard> = (0u16..16)
        .map(|v| Shard::new("a", format!("{v:02x}")))
        .collect();
    assert!(
        s3_proxy::cache::staging_plan_needs_uniform(1_000, 50, &weights),
        "fixture: the weights carry 300 bytes and 3 ranges"
    );
    let plan = s3_proxy::cache::staging_chunk_plan(1_000, 50, &weights, &ledger, 0);
    let quotas: HashMap<Shard, (u64, u64)> = plan
        .iter()
        .flat_map(|c| c.shards.iter().cloned().zip(c.shard_quotas.iter().copied()))
        .collect();
    let sums = quotas
        .values()
        .fold((0u64, 0u64), |acc, q| (acc.0 + q.0, acc.1 + q.1));
    assert_eq!(sums, (1_000, 50), "each arm must sum exactly: {quotas:?}");
    for s in &ledger {
        let q = quotas.get(s).copied().unwrap_or((0, 0));
        assert!(
            q.0 > 0 && q.1 > 0,
            "ledger shard {s} got {q:?}: the 700 bytes and 47 ranges the weights do not carry \
             must be shared over every ledger shard ({quotas:?})"
        );
        if let Some(w) = weights.get(s) {
            assert!(
                q.0 >= w.bytes && q.1 >= w.ranges,
                "weighted shard {s} got {q:?}, under its weight {w:?}"
            );
        }
    }
}

/// Twenty shards of equal weight and a shortfall of five units on each arm: every share rounds to
/// zero with an equal remainder, so the five units go by the tie order. Over twenty successive
/// plans with unchanged weights, as twenty passes make them, every shard gets a unit at least
/// once. Ties broken by shard order give the same five shards the units in every plan.
#[test]
fn equal_weight_shards_each_get_a_share_over_successive_plans() {
    let weights: ShardWeights = (0u16..20)
        .map(|v| {
            (
                Shard::new("t", format!("{v:02x}")),
                ShardWeight {
                    bytes: 7,
                    ranges: 7,
                },
            )
        })
        .collect();
    let mut reached: BTreeSet<Shard> = BTreeSet::new();
    for rotation in 0..weights.len() {
        let plan = s3_proxy::cache::staging_chunk_plan(5, 5, &weights, &[], rotation);
        let (bytes, ranges) = plan.iter().fold((0, 0), |acc, c| {
            (acc.0 + c.quota_bytes, acc.1 + c.quota_ranges)
        });
        assert_eq!(
            (bytes, ranges),
            (5, 5),
            "plan {rotation}: quotas must sum exactly"
        );
        for c in &plan {
            for (s, q) in c.shards.iter().zip(&c.shard_quotas) {
                if q.0 > 0 && q.1 > 0 {
                    reached.insert(s.clone());
                }
            }
        }
    }
    let starved: Vec<&Shard> = weights.keys().filter(|s| !reached.contains(*s)).collect();
    assert!(
        starved.is_empty(),
        "over {} plans with unchanged weights, {} shards never got a share: {starved:?}",
        weights.len(),
        starved.len()
    );
}

/// The production caller advances the tie-break rotation once per pass, so equal-weight shards
/// that rounding leaves without a share in one plan get it in a later one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_staging_pass_plans_with_the_next_rotation() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let m = Arc::new(manager(&dir).await);
        let (trigger, _) = m.get_staging_eviction_thresholds();
        let first = keys_in_one_shard("rotation-first", 21);
        stage_keys(&m, &first, &[MIB]).await;
        assert!(
            derived(&m).await > trigger,
            "fixture precondition: the first pass is over its trigger"
        );
        spawned_pass(&m).await;
        let r1 = s3_proxy::cache::last_staging_plan_rotation_for_tests(&dir)
            .expect("the first pass planned");
        let second = keys_in_one_shard("rotation-second", 6);
        stage_keys(&m, &second, &[MIB]).await;
        assert!(
            derived(&m).await > trigger,
            "fixture precondition: the second pass is over its trigger"
        );
        spawned_pass(&m).await;
        let r2 = s3_proxy::cache::last_staging_plan_rotation_for_tests(&dir)
            .expect("the second pass planned");
        assert!(
            r2 > r1,
            "each staging pass must plan with the next rotation: first {r1}, second {r2}"
        );
    })
    .await
    .expect("the test must finish within its bound");
}
