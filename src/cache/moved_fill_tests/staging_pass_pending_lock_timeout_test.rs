//! A staging pass at staged-authority epoch 1 must not size itself from a figure it cannot
//! trust because one `.pending` lock was not acquired in time.
//!
//! At epoch 1 graduation writes no debit, so between validation scans `size_state.write_cache_size`
//! holds every byte staged since the last scan, read or not. The fold's figure is the authority,
//! and it is unknown whenever one `{instance}.pending.lock` is not acquired within its 100 ms
//! bound, which under write load happens many times an hour; the authority memo then holds that
//! `None` for its 5 s TTL. A pass in that window used to read the counter, and with the counter far
//! over the trigger it evicted unread uploads down to its target (GitHub PR #27).
//!
//! A reused figure is no better for sizing: the last complete figure, or a memo another instance's
//! eviction has not expired, predates evictions it does not account for, and a pass sized from it
//! evicts the same band again. So the pass's post-lock read derives the figure fresh and frees
//! nothing on bytes when it cannot. The published figures keep the last complete one, of any
//! age.
//!
//! Each test drives the production paths: write-through PUTs, fold passes, then staging passes
//! with a peer's pending lock held. The counter is set the way it stands between validation scans,
//! far above the fold's figure. Memo ageing replaces real sleeps, so no 64-directory memo clear by
//! a neighbouring test can fall inside a wait.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::staged_fold::{
    age_authority_snapshot_for_tests, pending_paths_for_tests, run_staged_fold_sampled,
    AUTHORITY_SNAPSHOT_TTL_SECS,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// 200 MiB, so the Staging_Bound is 20 MiB, its trigger 19 MiB and its target 16 MiB.
pub(super) const MAX_CACHE: u64 = 200 * 1024 * 1024;
/// 3 MiB, so the target is not a whole number of objects and each assertion below has about
/// 2 MiB of margin either side.
pub(super) const OBJECT_SIZE: usize = 3 * 1024 * 1024;
/// The counter as it stands between validation scans at epoch 1: three times the Staging_Bound.
pub(super) const COUNTER_BYTES: u64 = 3 * MAX_CACHE / 10;

/// Fold passes that visit every shard of these fixtures.
const SWEEP_PASSES: usize = 16;

pub(super) async fn manager(cache_dir: &Path) -> CacheManager {
    manager_with_shared(cache_dir, SharedStorageConfig::default()).await
}

async fn manager_with_shared(cache_dir: &Path, shared: SharedStorageConfig) -> CacheManager {
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
        shared,
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

pub(super) fn staged_on_disk(cache_dir: &Path, keys: &[String]) -> Vec<String> {
    keys.iter()
        .filter(|k| {
            get_sharded_path(&cache_dir.join("metadata"), k, ".meta")
                .unwrap()
                .exists()
        })
        .cloned()
        .collect()
}

/// `objects` unread uploads through `m`, folded, with the counter at [`COUNTER_BYTES`].
async fn stage(m: &CacheManager, objects: usize) -> Vec<String> {
    let dir = m.get_cache_dir().to_path_buf();
    activate_epoch_1(&dir);
    let mut keys = Vec::new();
    for i in 0..objects {
        let key = format!("test-bucket/lock-timeout/object-{i}.bin");
        put(m, &key, &body(OBJECT_SIZE, 70 + i as u64)).await;
        keys.push(key);
    }
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let instance = ledger.instance_id().to_string();
    for _ in 0..SWEEP_PASSES {
        let _ = run_staged_fold_sampled(&dir, &ledger, &instance, None).await;
    }
    let mut state = consolidator.load_size_state().await.unwrap();
    state.write_cache_size = COUNTER_BYTES;
    consolidator
        .seed_size_state_for_tests(&state)
        .await
        .unwrap();
    keys
}

pub(super) async fn fixture(dir: &Path, objects: usize) -> (CacheManager, Vec<String>) {
    let m = manager(dir).await;
    let keys = stage(&m, objects).await;
    (m, keys)
}

/// A peer's `.pending` file and its lock, held until the guard is dropped, as a peer holds it
/// while it appends.
pub(super) struct PeerPendingLock(std::fs::File);

impl PeerPendingLock {
    pub(super) fn hold(dir: &Path) -> Self {
        let (pending, lock) = pending_paths_for_tests(dir, "peer-host:4242");
        std::fs::create_dir_all(pending.parent().unwrap()).unwrap();
        std::fs::write(&pending, b"").unwrap();
        let holder = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock)
            .unwrap();
        fs4::FileExt::try_lock(&holder).expect("the test must own the peer's pending lock");
        Self(holder)
    }
}

impl Drop for PeerPendingLock {
    fn drop(&mut self) {
        let _ = fs4::FileExt::unlock(&self.0);
    }
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

    // One TTL on, so the pass's first read re-derives the figure.
    age_authority_snapshot_for_tests(dir, AUTHORITY_SNAPSHOT_TTL_SECS);
    let lock = PeerPendingLock::hold(dir);
    let freed = m.evict_staging_tier().await;
    drop(lock);
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

/// Over the trigger: a pass during a lock timeout evicts nothing on bytes. It cannot read the
/// fold, and the only other figures it has are the counter and figures taken before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_pending_lock_timeout_over_the_trigger_does_not_size_from_the_counter() {
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

    age_authority_snapshot_for_tests(dir, AUTHORITY_SNAPSHOT_TTL_SECS);
    let lock = PeerPendingLock::hold(dir);
    let freed = m.evict_staging_tier().await;
    drop(lock);
    let survivors = staged_on_disk(dir, &keys);
    let left = staged.saturating_sub(freed);
    println!(
        "LOCK-TIMEOUT over trigger: staged={staged} trigger={trigger} target={target} \
         freed={freed} left={left} survivors={}/{}",
        survivors.len(),
        keys.len()
    );
    assert!(
        left + OBJECT_SIZE as u64 > target,
        "a staging pass during a pending-lock timeout freed past its target by more than one \
         object (staged={staged}, freed={freed}, left={left}, target={target}, survivors={}/{}): \
         it sized itself from size_state.write_cache_size instead of the fold's figure",
        survivors.len(),
        keys.len()
    );

    // Once the lock is free, the next pass derives the figure and evicts to the target.
    let freed_after = m.evict_staging_tier().await;
    let left_after = left.saturating_sub(freed_after);
    assert!(
        left_after <= trigger && left_after + OBJECT_SIZE as u64 > target,
        "with the lock free the pass must evict down to the target and no further \
         (staged={staged}, freed={freed}, freed_after={freed_after}, left_after={left_after}, \
         trigger={trigger}, target={target})"
    );
}

/// An evicting pass, then a second pass at once with a peer's
/// pending lock held. The last complete figure the second pass could be served was taken before
/// the first pass evicted, so sizing from it evicts the same band again.
#[tokio::test(flavor = "multi_thread")]
async fn a_pass_after_an_eviction_with_a_pending_lock_held_does_not_evict_again() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, keys) = fixture(dir, 7).await;
    let (trigger, target) = m.get_staging_eviction_thresholds();
    let before = m
        .get_staging_resident_bytes()
        .await
        .expect("fixture: a staged figure");
    assert!(before > trigger, "fixture: over the trigger");

    let first = m.evict_staging_tier().await;
    let after_first = staged_on_disk(dir, &keys);
    assert!(first > 0, "fixture: the first pass evicts");
    assert!(
        before.saturating_sub(first) <= trigger,
        "fixture: the first pass must leave the tier under its trigger, or a second pass would \
         be legitimate (before={before}, first={first}, trigger={trigger}, target={target})"
    );

    let lock = PeerPendingLock::hold(dir);
    let second = m.evict_staging_tier().await;
    drop(lock);
    let after_second = staged_on_disk(dir, &keys);
    assert_eq!(
        (second, after_second.len()),
        (0, after_first.len()),
        "a pass with a pending lock held evicted again after an evicting pass: it sized itself \
         from a figure taken before that eviction (before={before}, freed_first={first}, \
         freed_second={second}, trigger={trigger}, target={target})"
    );
}

/// Two proxies on one volume. B reads the figure, A evicts and re-folds, and then B runs a pass.
/// B's memo predates A's eviction, and nothing in B's process learns of A's install, so a pass
/// sized from B's memo or last complete figure evicts the band A already freed.
///
/// B reaches the volume through a symlink, so its memo entries are its own, as they are in a
/// second process. `held` holds a peer's pending lock across B's pass.
async fn a_peer_eviction_is_not_repeated(held: bool) {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("volume");
    std::fs::create_dir_all(&dir).unwrap();
    let link = tmp.path().join("volume-via-b");
    std::os::unix::fs::symlink(&dir, &link).unwrap();
    let a = manager(&dir).await;
    let keys = stage(&a, 7).await;
    let b = manager(&link).await;
    let (trigger, target) = b.get_staging_eviction_thresholds();
    let seen_by_b = b
        .get_staging_resident_bytes()
        .await
        .expect("fixture: B reads a staged figure");
    assert!(
        seen_by_b > trigger,
        "fixture: over the trigger as B sees it"
    );

    let freed_by_a = a.evict_staging_tier().await;
    let after_a = staged_on_disk(&dir, &keys);
    assert!(freed_by_a > 0, "fixture: A evicts");
    assert!(
        seen_by_b.saturating_sub(freed_by_a) <= trigger,
        "fixture: A's pass must leave the tier under its trigger (seen_by_b={seen_by_b}, \
         freed_by_a={freed_by_a}, trigger={trigger}, target={target})"
    );

    let lock = held.then(|| PeerPendingLock::hold(&dir));
    let freed_by_b = b.evict_staging_tier().await;
    drop(lock);
    let after_b = staged_on_disk(&dir, &keys);
    assert_eq!(
        (freed_by_b, after_b.len()),
        (0, after_a.len()),
        "B evicted again after A's eviction (held={held}): it sized itself from a figure taken \
         before A evicted (seen_by_b={seen_by_b}, freed_by_a={freed_by_a}, \
         freed_by_b={freed_by_b}, trigger={trigger}, target={target})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_eviction_is_not_repeated_with_a_pending_lock_held() {
    a_peer_eviction_is_not_repeated(true).await;
}

/// The same with nothing held: B's memo is inside its 5 s TTL, so a pass that sized from it
/// repeated A's eviction before this fix whether or not any lock was held.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_eviction_is_not_repeated_inside_the_memo_ttl() {
    a_peer_eviction_is_not_repeated(false).await;
}

/// `/metrics` keeps publishing the last complete fold figure through a held pending lock for as
/// long as the fresh figure stays unknown, of any age, and the unknown-figure gauge says how old
/// it is. Before 3.0.3's design the published figure fell to the counter after 60 s.
#[tokio::test(flavor = "multi_thread")]
async fn metrics_publish_the_last_complete_figure_for_as_long_as_the_fresh_one_is_unknown() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, _keys) = fixture(dir, 3).await;
    let m = Arc::new(m);
    let mut metrics_manager = s3_proxy::metrics::MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&m));
    let published = |metrics: &s3_proxy::metrics::SystemMetrics| {
        (
            metrics.write_cache.resident_bytes,
            metrics
                .cache
                .as_ref()
                .expect("cache metrics with a cache manager wired")
                .write_cache_size,
        )
    };

    let fold = m
        .get_staging_resident_bytes()
        .await
        .expect("fixture: the fold answers with nothing held");
    assert_ne!(fold, COUNTER_BYTES, "fixture: the two figures must differ");

    // A deciding read one TTL on re-derives, cannot read the pending file, and memoises `None`.
    assert!(
        age_authority_snapshot_for_tests(dir, AUTHORITY_SNAPSHOT_TTL_SECS),
        "fixture: the memo entry exists"
    );
    let lock = PeerPendingLock::hold(dir);
    let _ = m.get_staging_resident_bytes().await;
    let within = published(&metrics_manager.collect_metrics().await);

    // Still held, and the last complete figure is now 61 s older.
    assert!(
        age_authority_snapshot_for_tests(dir, 61),
        "fixture: the memo entry still exists"
    );
    let collected = metrics_manager.collect_metrics().await;
    let beyond = published(&collected);
    let unknown = collected.write_cache.staging_figure_unknown_seconds;
    drop(lock);

    assert_eq!(
        within,
        (fold, fold),
        "during a held pending lock /metrics must publish the last complete fold figure \
         ({fold}) as write_cache.resident_bytes and cache.write_cache_size, not the counter \
         ({COUNTER_BYTES})"
    );
    assert_eq!(
        beyond,
        (fold, fold),
        "61 s on, with the fresh figure still unknown, /metrics must keep publishing the last \
         complete fold figure ({fold}), never the counter ({COUNTER_BYTES})"
    );
    assert!(
        unknown >= 61,
        "the unknown-figure gauge must say the published figure is at least 61 s old \
         (unknown={unknown})"
    );
}

/// A figure the byte arm cannot read is visible on `/metrics`: the pass counts, the gauge
/// rises while the lock stays held, and one pass that reads the figure resets the gauge.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_staged_figure_is_published_and_clears_on_a_successful_pass() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, _keys) = fixture(dir, 7).await;
    let m = Arc::new(m);
    let mut metrics_manager = s3_proxy::metrics::MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&m));
    let published = |metrics: &s3_proxy::metrics::SystemMetrics| {
        (
            metrics.write_cache.staging_figure_unknown_seconds,
            metrics.write_cache.staging_figure_unknown_passes_total,
        )
    };
    assert_eq!(
        published(&metrics_manager.collect_metrics().await),
        (0, 0),
        "fixture: nothing unknown before the first pass"
    );

    let lock = PeerPendingLock::hold(dir);
    for pass in 0..3 {
        age_authority_snapshot_for_tests(dir, AUTHORITY_SNAPSHOT_TTL_SECS);
        assert_eq!(
            m.evict_staging_tier().await,
            0,
            "pass {pass} with the lock held frees nothing"
        );
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (seconds, passes) = published(&metrics_manager.collect_metrics().await);
    assert_eq!(
        passes, 3,
        "each pass that could not read the figure is counted"
    );
    assert!(
        seconds >= 1,
        "the gauge must rise while the figure stays unknown (seconds={seconds})"
    );

    drop(lock);
    age_authority_snapshot_for_tests(dir, AUTHORITY_SNAPSHOT_TTL_SECS);
    assert!(
        m.evict_staging_tier().await > 0,
        "with the lock free the pass reads the figure and evicts"
    );
    assert_eq!(
        published(&metrics_manager.collect_metrics().await),
        (0, 3),
        "a pass that reads the figure resets the gauge and leaves the counter"
    );
}

/// T1b: the unknown-figure gauge rises on a proxy that runs no staging pass. The figure is under
/// the trigger, so nothing nudges a pass; a peer's pending lock is held across two metrics
/// collections, each of which attempts a derivation. The gauge reads at least the time the memo
/// was aged by, with no pass counted and none run, and reads 0 once a collection derives the
/// figure again.
///
/// Red side: the earlier pass-event gauge, which only a failed locked pass could start, so
/// it read 0 throughout.
#[tokio::test(flavor = "multi_thread")]
async fn the_unknown_gauge_rises_without_any_staging_pass() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, _keys) = fixture(dir, 3).await;
    let m = Arc::new(m);
    let mut metrics_manager = s3_proxy::metrics::MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&m));
    let read = |metrics: &s3_proxy::metrics::SystemMetrics| {
        (
            metrics.write_cache.staging_figure_unknown_seconds,
            metrics.write_cache.staging_figure_unknown_passes_total,
            metrics.write_cache.staging_evictions_total,
        )
    };
    let (trigger, _) = m.get_staging_eviction_thresholds();
    let fold = m
        .get_staging_resident_bytes()
        .await
        .expect("fixture: the fold answers with nothing held");
    assert!(
        fold < trigger,
        "fixture: under the trigger, so nothing nudges a pass (fold={fold}, trigger={trigger})"
    );

    let lock = PeerPendingLock::hold(dir);
    assert!(
        age_authority_snapshot_for_tests(dir, AUTHORITY_SNAPSHOT_TTL_SECS),
        "fixture: the memo entry exists"
    );
    let first = read(&metrics_manager.collect_metrics().await);
    assert!(
        age_authority_snapshot_for_tests(dir, 10),
        "fixture: the memo entry still exists (a 64-directory clear would reset the gauge)"
    );
    let second = read(&metrics_manager.collect_metrics().await);
    drop(lock);
    assert!(
        age_authority_snapshot_for_tests(dir, AUTHORITY_SNAPSHOT_TTL_SECS),
        "fixture: the memo entry still exists"
    );
    let after = read(&metrics_manager.collect_metrics().await);
    println!("T1b: first={first:?} second={second:?} after={after:?}");

    assert!(
        second.0 >= AUTHORITY_SNAPSHOT_TTL_SECS + 10,
        "with a peer's pending lock held across two collections and no staging pass, the \
         unknown-figure gauge must read at least the {} s the memo was aged by \
         (first={first:?}, second={second:?})",
        AUTHORITY_SNAPSHOT_TTL_SECS + 10
    );
    assert_eq!(
        (second.1, second.2),
        (0, 0),
        "no staging pass ran: staging_figure_unknown_passes_total and staging_evictions_total \
         must both be 0 (second={second:?})"
    );
    assert_eq!(
        after.0, 0,
        "once a collection derives the figure the gauge reads 0 (after={after:?})"
    );
}

/// T2c: at epoch 1 the pre-lock read never falls to the counter. A peer's pending lock is held
/// and the last complete figure is 61 s old; the counter is over the trigger and the fold's
/// figure under it. The pass serves the last complete figure, needs no lock, and counts no
/// failed locked pass.
///
/// Red side: the earlier pre-lock read, which served the counter once the last complete figure
/// was older than 60 s, took the lock, and failed its fresh derivation (`passes_total` 1).
#[tokio::test(flavor = "multi_thread")]
async fn past_sixty_seconds_the_pre_lock_read_never_takes_the_lock_on_the_counter() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (m, keys) = fixture(dir, 3).await;
    let (trigger, _) = m.get_staging_eviction_thresholds();
    let fold = m
        .get_staging_resident_bytes()
        .await
        .expect("fixture: the fold answers with nothing held");
    assert!(
        fold < trigger && COUNTER_BYTES > trigger,
        "fixture: fold under, counter over (fold={fold}, counter={COUNTER_BYTES}, \
         trigger={trigger})"
    );

    let lock = PeerPendingLock::hold(dir);
    assert!(
        age_authority_snapshot_for_tests(dir, 61),
        "fixture: the memo entry exists"
    );
    let freed = m.evict_staging_tier().await;
    let (_, passes) = m.get_staging_figure_unknown().await;
    drop(lock);
    let survivors = staged_on_disk(dir, &keys);
    assert_eq!(
        (freed, passes, survivors.len()),
        (0, 0, keys.len()),
        "the pre-lock read must serve the last complete fold figure ({fold}) of any age, not the \
         counter ({COUNTER_BYTES}): a pass that took the lock on the counter and then failed its \
         fresh derivation counts passes_total (freed={freed}, passes_total={passes}, \
         survivors={}/{})",
        survivors.len(),
        keys.len()
    );
}
