//! A staging pass at staged-authority epoch 1 must not size itself from
//! `size_state.write_cache_size` because one `.pending` lock was not acquired in time.
//!
//! At epoch 1 graduation writes no debit, so between validation scans that counter holds every
//! byte staged since the last scan, read or not. The fold's figure is the authority, and it is
//! unknown whenever one `{instance}.pending.lock` is not acquired within its 100 ms bound, which
//! under write load happens many times an hour; the authority memo then holds that `None` for
//! its 5 s TTL. A pass in that window used to read the counter, and with the counter far over
//! the trigger it evicted unread uploads down to its target.
//!
//! Each test below drives the production paths: write-through PUTs, fold passes, a pass that
//! reads the figure with nothing held, then a pass with a peer's pending lock held. The counter
//! is set the way it stands between validation scans, far above the fold's figure.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::staged_fold::{run_staged_fold, PendingStore};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

/// 200 MiB, so the Staging_Bound is 20 MiB, its trigger 19 MiB and its target 16 MiB.
const MAX_CACHE: u64 = 200 * 1024 * 1024;
/// 3 MiB, so the target is not a whole number of objects and each assertion below has about
/// 2 MiB of margin either side.
const OBJECT_SIZE: usize = 3 * 1024 * 1024;

/// Longer than the authority memo's 5 s TTL, so the pass's first read re-derives the figure.
const PAST_THE_MEMO_TTL: Duration = Duration::from_secs(6);

/// Fold passes that visit every shard of these fixtures.
const SWEEP_PASSES: usize = 16;

async fn manager(cache_dir: &Path) -> CacheManager {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let m = CacheManager::new_with_shared_storage(
        cache_dir.to_path_buf(),
        false,
        0,
        MAX_CACHE,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(315_360_000), // get_ttl
        Duration::from_secs(3600),        // head_ttl
        Duration::from_secs(3600),        // put_ttl
        false,
        SharedStorageConfig::default(),
        10.0, // write_cache_percent
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
    let _ = m.create_configured_disk_cache_manager();
    m.initialize().await.expect("cache manager initialises");
    m
}

fn activate_epoch_1(cache_dir: &Path) {
    let dir = cache_dir.join("metadata").join("_shard_state");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("staged_authority_epoch.json"),
        r#"{"schema":1,"epoch":1,"activated_at":0,"activated_by":"lock-timeout-test","binary_version":"test"}"#,
    )
    .unwrap();
}

/// Incompressible, so each object's staged compressed size is close to its length.
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

async fn put(m: &CacheManager, cache_key: &str, data: &[u8]) {
    m.invalidate_cache_unified_for_operation(cache_key, "PUT")
        .await
        .unwrap();
    m.store_put_as_write_cached_range_with_ttl(
        cache_key,
        data,
        "\"lock-timeout-etag\"".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        Some("application/octet-stream".to_string()),
        HashMap::new(),
        Duration::from_secs(3600),
    )
    .await
    .expect("write-cache store succeeds");
}

fn staged_on_disk(cache_dir: &Path, keys: &[String]) -> Vec<String> {
    keys.iter()
        .filter(|k| {
            get_sharded_path(&cache_dir.join("metadata"), k, ".meta")
                .unwrap()
                .exists()
        })
        .cloned()
        .collect()
}

/// `objects` unread uploads, folded, with the counter at three times the Staging_Bound.
async fn fixture(dir: &Path, objects: usize) -> (CacheManager, Vec<String>) {
    let m = manager(dir).await;
    activate_epoch_1(dir);
    let mut keys = Vec::new();
    for i in 0..objects {
        let key = format!("test-bucket/lock-timeout/object-{i}.bin");
        put(&m, &key, &body(OBJECT_SIZE, 70 + i as u64)).await;
        keys.push(key);
    }
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let instance = ledger.instance_id().to_string();
    for _ in 0..SWEEP_PASSES {
        let _ = run_staged_fold(m.get_cache_dir(), &ledger, &instance).await;
    }
    let mut state = consolidator.load_size_state().await.unwrap();
    state.write_cache_size = 3 * MAX_CACHE / 10;
    consolidator.persist_size_state(&state).await.unwrap();
    (m, keys)
}

/// A peer's `.pending` file and its lock, held for the duration of `pass`, as a peer holds it
/// while it appends.
async fn with_a_peer_pending_lock_held<F, T>(dir: &Path, pass: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let pending_dir = PendingStore::new(dir.to_path_buf()).dir();
    std::fs::create_dir_all(&pending_dir).unwrap();
    std::fs::write(pending_dir.join("peer-host:4242.pending"), b"").unwrap();
    let holder = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(pending_dir.join("peer-host:4242.pending.lock"))
        .unwrap();
    fs4::FileExt::try_lock(&holder).expect("the test must own the peer's pending lock");
    let out = pass.await;
    fs4::FileExt::unlock(&holder).unwrap();
    out
}

/// Under the trigger: a pass during a lock timeout evicts nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_pending_lock_timeout_under_the_trigger_evicts_nothing() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, keys) = fixture(dir, 3).await;
    let (trigger, target) = m.get_staging_eviction_thresholds();
    let staged = m
        .get_staging_resident_bytes()
        .await
        .expect("fixture: a staged figure");
    assert!(
        staged < trigger,
        "fixture: the staged uploads must be under the trigger (staged={staged}, \
         trigger={trigger})"
    );
    assert_eq!(
        m.evict_staging_tier().await,
        0,
        "fixture: with nothing held, the pass reads the fold and evicts nothing"
    );

    tokio::time::sleep(PAST_THE_MEMO_TTL).await;
    let freed = with_a_peer_pending_lock_held(dir, m.evict_staging_tier()).await;
    let survivors = staged_on_disk(dir, &keys);
    println!(
        "LOCK-TIMEOUT under trigger: staged={staged} trigger={trigger} target={target} \
         freed={freed} survivors={}/{}",
        survivors.len(),
        keys.len()
    );
    assert_eq!(
        (freed, survivors.len()),
        (0, keys.len()),
        "a staging pass during a pending-lock timeout evicted unread uploads with the staged \
         tier under its trigger (staged={staged}, trigger={trigger}): it sized itself from \
         size_state.write_cache_size instead of the fold's figure"
    );
}

/// Over the trigger: a pass during a lock timeout frees down to the target, not past it.
#[tokio::test(flavor = "multi_thread")]
async fn a_pending_lock_timeout_over_the_trigger_frees_down_to_the_target_only() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, keys) = fixture(dir, 7).await;
    let (trigger, target) = m.get_staging_eviction_thresholds();
    let staged = m
        .get_staging_resident_bytes()
        .await
        .expect("fixture: a staged figure");
    assert!(
        staged > trigger,
        "fixture: the staged uploads must be over the trigger (staged={staged}, \
         trigger={trigger})"
    );

    tokio::time::sleep(PAST_THE_MEMO_TTL).await;
    let freed = with_a_peer_pending_lock_held(dir, m.evict_staging_tier()).await;
    let survivors = staged_on_disk(dir, &keys);
    let left = staged.saturating_sub(freed);
    println!(
        "LOCK-TIMEOUT over trigger: staged={staged} trigger={trigger} target={target} \
         freed={freed} left={left} survivors={}/{}",
        survivors.len(),
        keys.len()
    );
    assert!(
        left <= trigger,
        "the pass must still evict when the staged tier is over its trigger (staged={staged}, \
         freed={freed}, trigger={trigger})"
    );
    assert!(
        left + OBJECT_SIZE as u64 > target,
        "a staging pass during a pending-lock timeout freed past its target by more than one \
         object (staged={staged}, freed={freed}, left={left}, target={target}, survivors={}/{}): \
         it sized itself from size_state.write_cache_size instead of the fold's figure",
        survivors.len(),
        keys.len()
    );
}
