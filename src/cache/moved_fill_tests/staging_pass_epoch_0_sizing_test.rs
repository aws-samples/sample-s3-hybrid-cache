//! At staged-authority epoch 0 a staging pass must not size itself from the counter alone.
//!
//! Every volume starts at epoch 0 and stays there until the fold completes a rotation and the
//! activation preconditions pass. There `size_state.write_cache_size` is the published figure, and
//! it over-counts: a graduation's debit is a journal entry applied at consolidation, and it is lost
//! whenever the object's `.meta` is removed before that entry is consolidated, or the process dies
//! between the `.meta` transition and the append. Nothing but a validation scan corrects it. A pass
//! sized from it removes unread uploads down to its target by the counter's error.
//!
//! The pass now sizes itself from the fold's figure derived after it takes the eviction lock,
//! bounded above by the counter, and frees nothing by size when the fold has no figure. Each test
//! drives the production paths (write-through PUTs, fold passes, staging passes) and compares what
//! the pass freed with a census of the `.meta`s on disk.

use super::staging_pass_pending_lock_timeout_test::{
    body, fixture, manager, put, staged_on_disk, PeerPendingLock, OBJECT_SIZE,
};
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::NewCacheMetadata;
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::staged_fold::{
    age_authority_snapshot_for_tests, run_staged_fold_sampled, AUTHORITY_SNAPSHOT_TTL_SECS,
};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

/// Every test's bound. Seven 3 MiB objects stage, fold and evict in seconds.
const BOUND: Duration = Duration::from_secs(120);

/// The counter's over-count in these fixtures: four times the staged bytes on disk, the order of
/// the over-statement measured on a fleet volume between validation scans.
const FACTOR: u64 = 4;

/// Fold passes that visit every shard of these fixtures.
const SWEEP_PASSES: usize = 16;

fn meta_of(dir: &Path, key: &str) -> Option<NewCacheMetadata> {
    let path = get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

/// Staged compressed bytes over the `.meta`s on disk, and the largest one object holds.
fn census(dir: &Path, keys: &[String]) -> (u64, u64) {
    let sizes: Vec<u64> = keys
        .iter()
        .filter_map(|k| meta_of(dir, k))
        .map(|m| m.staged_compressed_size())
        .collect();
    (sizes.iter().sum(), sizes.iter().copied().max().unwrap_or(0))
}

fn at_epoch_0(dir: &Path) -> bool {
    !crate::staged_authority::fold_authority_active(dir)
}

/// `objects` unread uploads through `m` at epoch 0, not folded.
async fn stage_at_epoch_0(m: &CacheManager, objects: usize) -> Vec<String> {
    let mut keys = Vec::new();
    for i in 0..objects {
        let key = format!("test-bucket/epoch-0-sizing/object-{i}.bin");
        put(m, &key, &body(OBJECT_SIZE, 300 + i as u64)).await;
        keys.push(key);
    }
    keys
}

/// Fold passes of `m`'s own instance over every fixture shard.
async fn sweep(m: &CacheManager) {
    let dir = m.get_cache_dir().to_path_buf();
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let instance = ledger.instance_id().to_string();
    for _ in 0..SWEEP_PASSES {
        let _ = run_staged_fold_sampled(&dir, &ledger, &instance, None).await;
    }
    age_authority_snapshot_for_tests(&dir, AUTHORITY_SNAPSHOT_TTL_SECS);
}

/// `size_state.write_cache_size` set to `bytes`, as it stands between validation scans.
async fn seed_counter(m: &CacheManager, bytes: u64) {
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let mut state = consolidator.load_size_state().await.unwrap();
    state.write_cache_size = bytes;
    consolidator
        .seed_size_state_for_tests(&state)
        .await
        .unwrap();
}

/// At epoch 0, with the counter at [`FACTOR`] times the staged bytes and the fold holding a
/// complete figure, a pass frees the true excess over its target and no more than one object past
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn an_epoch_0_pass_frees_no_more_than_the_true_excess_over_the_target() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage_at_epoch_0(&m, 7).await;
        sweep(&m).await;
        let (truth, object) = census(dir, &keys);
        let counter = FACTOR * truth;
        seed_counter(&m, counter).await;
        let (trigger, target) = m.get_staging_eviction_thresholds();
        assert!(at_epoch_0(dir), "fixture: the volume is at epoch 0");
        assert!(
            truth > trigger && object > 0,
            "fixture: the staged bytes are over the trigger (truth={truth}, trigger={trigger})"
        );
        let excess = truth - target;

        let freed = m.evict_staging_tier().await;
        let (left, _) = census(dir, &keys);
        println!(
            "EPOCH-0 over-count: counter={counter} truth={truth} trigger={trigger} \
             target={target} excess={excess} object={object} freed={freed} left={left}"
        );
        assert!(at_epoch_0(dir), "fixture: the volume stayed at epoch 0");
        assert!(
            freed < excess + object,
            "an epoch-0 pass sized itself from the counter: it freed {freed} bytes where the true \
             excess over the target is {excess} (counter={counter}, truth={truth}, \
             target={target}, object={object}, left={left})"
        );
        assert!(
            freed >= excess && left <= target,
            "an epoch-0 pass over its trigger freed less than the true excess (freed={freed}, \
             excess={excess}, left={left}, target={target})"
        );
    })
    .await
    .expect("epoch-0 over-count test timed out");
}

/// An in-progress multipart part of `bytes` compressed bytes, outside any staged object.
fn seed_in_progress_part(dir: &Path, bytes: u64) {
    let upload = dir.join("mpus_in_progress").join("epoch-0-live-upload");
    std::fs::create_dir_all(&upload).unwrap();
    let info = s3_proxy::cache_types::CachedPartInfo::new(
        1,
        bytes,
        "\"etag-live\"".to_string(),
        s3_proxy::compression::CompressionAlgorithm::Lz4,
    )
    .with_compressed_size(bytes);
    std::fs::write(
        upload.join("part1.json"),
        serde_json::to_string(&info).unwrap(),
    )
    .unwrap();
    assert_eq!(
        s3_proxy::signed_put_handler::in_progress_part_bytes(dir),
        Some(bytes),
        "fixture: the in-progress part must be readable"
    );
}
/// At epoch 0, with the counter over-counting and an in-progress multipart part on the volume, a
/// pass frees the resident staged excess only: the counter never counted in-progress parts, so
/// the part is not sized as staged bytes the pass can remove.
///
/// # RED SIDE
///
/// With the counter bounding the fold's figure including `P`, the pass frees the part's bytes on
/// top of the excess.
#[tokio::test(flavor = "multi_thread")]
async fn an_epoch_0_pass_does_not_size_in_progress_parts_as_staged_bytes() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage_at_epoch_0(&m, 7).await;
        let (_, object) = census(dir, &keys);
        let part_bytes = 2 * object;
        seed_in_progress_part(dir, part_bytes);
        sweep(&m).await;
        let (truth, object) = census(dir, &keys);
        let counter = FACTOR * truth;
        seed_counter(&m, counter).await;
        // The part was written directly, so no proxy's published in-progress figure counts it:
        // age the kept walk so the next derivation walks `mpus_in_progress/` afresh.
        s3_proxy::signed_put_handler::age_last_in_progress_walk_for_tests(
            dir,
            s3_proxy::signed_put_handler::PUBLISHED_WALK_BASE_MAX_AGE + Duration::from_secs(1),
        );
        let fold = crate::staged_fold::staged_authority_figure_fresh_with_weights(dir)
            .await
            .0
            .expect("fixture: the fold has a figure");
        assert_eq!(
            fold.in_progress_part_bytes, part_bytes,
            "fixture: the fold's figure counts the in-progress part, or this test is vacuous"
        );
        let (trigger, target) = m.get_staging_eviction_thresholds();
        assert!(at_epoch_0(dir), "fixture: the volume is at epoch 0");
        assert!(
            truth > trigger && object > 0,
            "fixture: the staged bytes are over the trigger (truth={truth}, trigger={trigger})"
        );
        let excess = truth - target;
        let freed = m.evict_staging_tier().await;
        let (left, _) = census(dir, &keys);
        println!(
            "EPOCH-0 in-progress part: counter={counter} truth={truth} part={part_bytes} \
             target={target} excess={excess} object={object} freed={freed} left={left}"
        );
        assert!(
            freed < excess + object,
            "AN EPOCH-0 PASS SIZED IN-PROGRESS PART BYTES AS STAGED BYTES: it freed {freed} where \
             the resident excess is {excess} (part={part_bytes}, counter={counter}, \
             truth={truth}, object={object})"
        );
        assert!(
            freed >= excess && left <= target,
            "an epoch-0 pass over its trigger freed less than the true excess (freed={freed}, \
             excess={excess}, left={left}, target={target})"
        );
    })
    .await
    .expect("epoch-0 in-progress part test timed out");
}
/// A fresh volume's first rotation at epoch 0: before the fold has installed any shard there is
/// no figure, so a pass frees nothing by size even with an accurate counter over the trigger.
/// After the first install it frees the true excess.
#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_epoch_0_volume_frees_nothing_by_size_until_the_fold_has_a_figure() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage_at_epoch_0(&m, 7).await;
        let (truth, object) = census(dir, &keys);
        seed_counter(&m, truth).await;
        let (trigger, target) = m.get_staging_eviction_thresholds();
        assert!(at_epoch_0(dir), "fixture: the volume is at epoch 0");
        assert!(
            truth > trigger,
            "fixture: the staged bytes are over the trigger (truth={truth}, trigger={trigger})"
        );
        let checkpoints = dir.join("metadata/_shard_state/checkpoints");
        assert!(
            !checkpoints.exists()
                || std::fs::read_dir(&checkpoints)
                    .map(|mut d| d.next().is_none())
                    .unwrap_or(true),
            "fixture: no fold has installed a shard yet"
        );

        let before_fold = m.evict_staging_tier().await;
        let survivors = staged_on_disk(dir, &keys).len();
        println!(
            "EPOCH-0 fresh volume, before the first install: counter={truth} trigger={trigger} \
             target={target} freed={before_fold} survivors={survivors}/{}",
            keys.len()
        );
        assert_eq!(
            (before_fold, survivors),
            (0, keys.len()),
            "an epoch-0 pass with no fold figure sized itself from the counter (counter={truth}, \
             target={target})"
        );

        sweep(&m).await;
        let after_fold = m.evict_staging_tier().await;
        let (left, _) = census(dir, &keys);
        println!(
            "EPOCH-0 fresh volume, after the first install: freed={after_fold} left={left} \
             excess={} object={object}",
            truth - target
        );
        assert!(
            after_fold >= truth - target && after_fold < truth - target + object,
            "after the first install an epoch-0 pass frees the true excess over the target \
             (freed={after_fold}, excess={}, object={object})",
            truth - target
        );
    })
    .await
    .expect("fresh-volume test timed out");
}

/// At epoch 0, a pass whose fold derivation fails (a peer holds its `.pending` lock past the bound)
/// frees nothing by size, as at epoch 1, rather than falling back to the counter.
#[tokio::test(flavor = "multi_thread")]
async fn an_epoch_0_pass_with_no_fold_figure_frees_nothing_by_size() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage_at_epoch_0(&m, 7).await;
        sweep(&m).await;
        let (truth, _) = census(dir, &keys);
        let counter = FACTOR * truth;
        seed_counter(&m, counter).await;
        assert!(at_epoch_0(dir), "fixture: the volume is at epoch 0");

        let lock = PeerPendingLock::hold(dir);
        let freed = m.evict_staging_tier().await;
        drop(lock);
        let survivors = staged_on_disk(dir, &keys).len();
        println!(
            "EPOCH-0 lock timeout: counter={counter} truth={truth} freed={freed} \
             survivors={survivors}/{}",
            keys.len()
        );
        assert_eq!(
            (freed, survivors),
            (0, keys.len()),
            "an epoch-0 pass that could not derive the fold's figure sized itself from the counter \
             (counter={counter}, truth={truth}, freed={freed})"
        );
    })
    .await
    .expect("epoch-0 lock-timeout test timed out");
}

/// At epoch 0 the counter still bounds the figure from above: with the counter under the fold's
/// figure, the pass frees no more than the counter's excess over the target.
#[tokio::test(flavor = "multi_thread")]
async fn an_epoch_0_pass_frees_no_more_than_the_counter_allows() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage_at_epoch_0(&m, 8).await;
        sweep(&m).await;
        let (truth, object) = census(dir, &keys);
        let (trigger, target) = m.get_staging_eviction_thresholds();
        let counter = trigger + object / 2;
        assert!(
            counter < truth && counter - target + object < truth - target,
            "fixture: the counter is under the fold's figure by more than one object \
             (counter={counter}, truth={truth}, object={object})"
        );
        seed_counter(&m, counter).await;
        assert!(at_epoch_0(dir), "fixture: the volume is at epoch 0");

        let freed = m.evict_staging_tier().await;
        println!(
            "EPOCH-0 counter under the fold: counter={counter} truth={truth} target={target} \
             object={object} freed={freed}"
        );
        assert!(
            freed >= counter - target && freed < counter - target + object,
            "an epoch-0 pass with the counter under the fold's figure did not size itself from \
             the counter (freed={freed}, counter={counter}, truth={truth}, target={target}, \
             object={object})"
        );
    })
    .await
    .expect("counter-bound test timed out");
}

/// Control: at epoch 1 the pass sizes from the fold's figure, with the counter far over it, as
/// before.
#[tokio::test(flavor = "multi_thread")]
async fn an_epoch_1_pass_still_sizes_from_the_fold() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let (m, keys) = fixture(dir, 7).await;
        assert!(!at_epoch_0(dir), "fixture: the volume is at epoch 1");
        let (truth, object) = census(dir, &keys);
        let (_, target) = m.get_staging_eviction_thresholds();
        let freed = m.evict_staging_tier().await;
        println!("EPOCH-1 control: truth={truth} target={target} object={object} freed={freed}");
        assert!(
            freed >= truth - target && freed < truth - target + object,
            "an epoch-1 pass did not free the true excess (freed={freed}, truth={truth}, \
             target={target}, object={object})"
        );
    })
    .await
    .expect("epoch-1 control timed out");
}
