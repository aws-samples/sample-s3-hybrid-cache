//! A staging pass must not size itself from bytes already removed.
//!
//! The staged fold never subtracts: a removal leaves its shard's figure until a visit re-reads that
//! shard. So when a staging pass's re-fold does not install a shard (a peer holds its claim, or the
//! fold retains it), and when an object graduates, is deleted, overwritten, healed or expires, the
//! figure every later pass sizes from still holds those bytes, and the next pass evicts unread
//! uploads that the first pass, or the graduation, had already accounted for.
//!
//! The removal debit closes that: each removal is published per shard with the time it happened,
//! and a reader subtracts it while the shard's checkpoint began its visit before the removal. Each
//! test drives the production paths (write-through PUTs, fold passes, staging passes, graduation)
//! and compares the figure with a census of the `.meta`s left on disk.

use super::staging_pass_pending_lock_timeout_test::{
    manager, stage, staged_on_disk, PeerPendingLock,
};
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::NewCacheMetadata;
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::staged_fold::{
    age_authority_snapshot_for_tests, claim_shard, record_shard_debit_for_tests,
    run_staged_fold_over_shards, run_staged_fold_sampled, set_visit_hook_for_tests, shard_of,
    ClaimOutcome, Shard, VisitHookPhase, AUTHORITY_SNAPSHOT_TTL_SECS,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

/// Every test's bound. A fixture of seven 3 MiB objects completes in seconds.
const BOUND: Duration = Duration::from_secs(120);

/// The instance a peer's claim is held under.
const PEER: &str = "peer-host:9";

fn meta_of(dir: &Path, key: &str) -> Option<NewCacheMetadata> {
    let path = get_sharded_path(&dir.join("metadata"), key, ".meta").unwrap();
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

/// Staged compressed bytes over the `.meta`s on disk: what the figure should be.
fn census(dir: &Path, keys: &[String]) -> u64 {
    keys.iter()
        .filter_map(|k| meta_of(dir, k))
        .map(|m| m.staged_compressed_size())
        .sum()
}

/// Staged ranges of one `.meta`.
fn staged_ranges(dir: &Path, key: &str) -> u64 {
    let meta = meta_of(dir, key).expect("fixture: the object's .meta");
    meta.ranges
        .iter()
        .filter(|r| {
            s3_proxy::cache_types::is_staged_range_spec(r, meta.object_metadata.is_write_cached)
        })
        .count() as u64
}

fn shard(key: &str) -> Shard {
    let (bucket, l1) = shard_of(key).expect("fixture keys name a shard");
    Shard::new(bucket, l1)
}

/// The staged figure as a reader that has just derived it sees it: the memo is aged past its TTL,
/// so the read derives.
async fn derived(m: &CacheManager) -> u64 {
    age_authority_snapshot_for_tests(m.get_cache_dir(), AUTHORITY_SNAPSHOT_TTL_SECS);
    m.get_staging_resident_bytes()
        .await
        .expect("a staged figure")
}

/// A peer holds the claim on every fixture shard, so a re-fold of them reports busy.
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

/// The peer's claims released, as its visit ends.
fn release_claims(dir: &Path) {
    let claims = dir.join("metadata").join("_shard_state").join("claims");
    let _ = std::fs::remove_dir_all(claims);
}

/// `n` fold passes of `m`'s own instance, enough to visit every fixture shard.
async fn sweep(m: &CacheManager) {
    let dir = m.get_cache_dir().to_path_buf();
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let instance = ledger.instance_id().to_string();
    for _ in 0..16 {
        let _ = run_staged_fold_sampled(&dir, &ledger, &instance, None).await;
    }
}

async fn instance_of(m: &CacheManager) -> String {
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    consolidator.write_ledger().instance_id().to_string()
}

/// Two managers on one volume: `b` reaches it through a symlink, so its memo entries and its
/// in-process state are its own, as they are in a second process.
async fn two_on_one_volume(
    tmp: &TempDir,
    link_name: &str,
) -> (std::path::PathBuf, CacheManager, Vec<String>, CacheManager) {
    let dir = tmp.path().join("volume");
    std::fs::create_dir_all(&dir).unwrap();
    let link = tmp.path().join(link_name);
    std::os::unix::fs::symlink(&dir, &link).unwrap();
    let a = manager(&dir).await;
    let keys = stage(&a, 7).await;
    let b = manager(&link).await;
    (dir, a, keys, b)
}

/// T4a. A pass whose re-fold found every evicted shard busy: the next pass must free nothing,
/// and the figure must equal the census of the surviving `.meta`s.
#[tokio::test(flavor = "multi_thread")]
async fn a_pass_whose_re_fold_found_the_shard_busy_is_not_repeated() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage(&m, 7).await;
        let (trigger, target) = m.get_staging_eviction_thresholds();
        let before = derived(&m).await;
        assert_eq!(
            before,
            census(dir, &keys),
            "fixture: the folded figure is the census"
        );
        assert!(
            before > trigger,
            "fixture: over the trigger (before={before}, trigger={trigger})"
        );

        claim_all(dir, &keys).await;
        let first = m.evict_staging_tier().await;
        release_claims(dir);
        let truth = census(dir, &keys);
        assert!(
            first > 0 && truth <= trigger,
            "fixture: the first pass evicts under the trigger (first={first}, truth={truth})"
        );
        let survivors = staged_on_disk(dir, &keys).len();
        let figure = derived(&m).await;
        let second = m.evict_staging_tier().await;
        println!(
            "T4a busy re-fold: before={before} first={first} truth={truth} figure={figure} \
             second={second} trigger={trigger} target={target}"
        );
        assert_eq!(
            (second, staged_on_disk(dir, &keys).len(), figure),
            (0, survivors, truth),
            "after a pass whose re-fold found its shards busy, the next pass sized itself from the \
             pre-eviction figure (before={before}, first={first}, truth={truth}, figure={figure}, \
             second={second})"
        );
    })
    .await
    .expect("T4a timed out");
}

/// T4b. A evicts with its re-fold busy; peer B's derivation equals B's census, and B's pass frees
/// nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_sizes_from_the_evictors_debit() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let (dir, a, keys, b) = two_on_one_volume(&tmp, "volume-via-b").await;
        let (trigger, _) = b.get_staging_eviction_thresholds();
        let seen_by_b = derived(&b).await;
        assert!(
            seen_by_b > trigger,
            "fixture: over the trigger as B sees it"
        );

        claim_all(&dir, &keys).await;
        let freed_by_a = a.evict_staging_tier().await;
        release_claims(&dir);
        assert!(freed_by_a > 0, "fixture: A evicts");
        let truth = census(&dir, &keys);
        let figure_b = derived(&b).await;
        let freed_by_b = b.evict_staging_tier().await;
        println!(
            "T4b peer: seen_by_b={seen_by_b} freed_by_a={freed_by_a} truth={truth} \
             figure_b={figure_b} freed_by_b={freed_by_b} trigger={trigger}"
        );
        assert_eq!(
            (figure_b, freed_by_b),
            (truth, 0),
            "B sized itself from the figure before A's eviction, whose re-fold found the shards \
             busy (seen_by_b={seen_by_b}, freed_by_a={freed_by_a}, truth={truth})"
        );
    })
    .await
    .expect("T4b timed out");
}

/// T4c. Once the evictor's periodic fold re-installs the shards, the debit is inert: the figure is
/// the installed figure exactly, not the installed figure less the debit.
#[tokio::test(flavor = "multi_thread")]
async fn a_debit_is_inert_once_its_shard_re_installs() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage(&m, 7).await;
        claim_all(dir, &keys).await;
        let freed = m.evict_staging_tier().await;
        release_claims(dir);
        assert!(freed > 0, "fixture: the pass evicts");
        sweep(&m).await;
        let truth = census(dir, &keys);
        let figure = derived(&m).await;
        println!("T4c inert: freed={freed} truth={truth} figure={figure}");
        assert_eq!(
            figure, truth,
            "after the shards re-installed, the figure must be the installed figure: a debit \
             applied to a checkpoint whose visit began after the removal subtracts it twice"
        );
    })
    .await
    .expect("T4c timed out");
}

/// A second staged upload in `key`'s shard, folded, so that shard's figure stays above the removed
/// object's bytes and the reader's per-shard clamp is not what decides the figure.
async fn companion_in_shard_of(m: &CacheManager, key: &str) -> String {
    let target = shard(key);
    let companion = (0..20_000)
        .map(|i| format!("test-bucket/t4d/companion-{i}.bin"))
        .find(|k| shard(k) == target)
        .expect("a key in the same shard");
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let data: Vec<u8> = (0..3 * 1024 * 1024)
        .map(|_| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (x >> 33) as u8
        })
        .collect();
    m.invalidate_cache_unified_for_operation(&companion, "PUT")
        .await
        .unwrap();
    m.store_put_as_write_cached_range_with_ttl(
        &companion,
        &data,
        "\"t4d-etag\"".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        Some("application/octet-stream".to_string()),
        std::collections::HashMap::new(),
        Duration::from_secs(3600),
    )
    .await
    .expect("write-cache store succeeds");
    sweep(m).await;
    companion
}

/// T4d, three ways. A removal made inside a fold visit of its shard, at the visit's start (the
/// visit's reads see it) or after its reads (they do not), and then with the remover's clock an
/// hour behind the folder's. The shard holds a second upload, so its figure stays above the debit.
async fn removal_inside_a_visit(
    phase: VisitHookPhase,
    remover_clock_behind: Option<Duration>,
) -> (u64, u64, u64) {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let m = manager(&dir).await;
    let mut keys = stage(&m, 3).await;
    let instance = instance_of(&m).await;
    let key = keys[0].clone();
    keys.push(companion_in_shard_of(&m, &key).await);
    let s = shard(&key);
    let bytes = census(&dir, std::slice::from_ref(&key));
    let ranges = staged_ranges(&dir, &key);
    let meta = get_sharded_path(&dir.join("metadata"), &key, ".meta").unwrap();
    let (hook_dir, hook_instance, hook_shard) = (dir.clone(), instance.clone(), s.clone());
    set_visit_hook_for_tests(
        &dir,
        phase,
        Box::new(move || {
            std::fs::remove_file(&meta).expect("the hook removes the object's .meta");
            let removed_at = remover_clock_behind.map(|behind| {
                (std::time::SystemTime::now() - behind)
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
            record_shard_debit_for_tests(
                &hook_dir,
                &hook_instance,
                &hook_shard.bucket,
                &hook_shard.l1,
                bytes,
                ranges,
                removed_at,
            );
        }),
    );
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let ledger = consolidator.write_ledger().clone();
    let only: BTreeSet<Shard> = [s].into_iter().collect();
    let outcome = run_staged_fold_over_shards(&dir, &ledger, &instance, &only).await;
    assert_eq!(
        outcome.summary().installed.len(),
        1,
        "fixture: the visit installs"
    );
    let truth = census(&dir, &keys);
    let figure = derived(&m).await;
    (truth, figure, bytes)
}

/// T4d: the visit began before the removal and its reads saw it. The rule applies the debit, so
/// the figure under-states by the removed bytes until the shard's next install: the late-trigger
/// direction, which the rule chooses over re-eviction.
#[tokio::test(flavor = "multi_thread")]
async fn a_visit_that_began_before_the_removal_applies_the_debit() {
    let (truth, figure, bytes) =
        tokio::time::timeout(BOUND, removal_inside_a_visit(VisitHookPhase::Started, None))
            .await
            .expect("T4d timed out");
    println!("T4d straddle (reads after removal): truth={truth} figure={figure} bytes={bytes}");
    assert_eq!(
        figure,
        truth.saturating_sub(bytes),
        "a visit that began before the removal must apply its debit (truth={truth}, bytes={bytes})"
    );
}

/// T4d: the visit's reads preceded the removal, so its install counts the removed bytes. The rule
/// applies the debit and the figure is exact; keying on the install time would leave it inert and
/// over-count, the re-eviction direction.
#[tokio::test(flavor = "multi_thread")]
async fn a_removal_after_the_visits_reads_is_debited_exactly() {
    let (truth, figure, bytes) = tokio::time::timeout(
        BOUND,
        removal_inside_a_visit(VisitHookPhase::BeforeInstall, None),
    )
    .await
    .expect("T4d timed out");
    println!("T4d straddle (reads before removal): truth={truth} figure={figure} bytes={bytes}");
    assert_eq!(
        figure, truth,
        "a removal after the visit's reads must be debited from that visit's install \
         (truth={truth}, bytes={bytes}, figure={figure})"
    );
}

/// T4d, skewed: the remover's clock is behind the folder's by more than the visit's length, so a
/// removal after the visit's reads is stamped before the visit began. The entry is inert and the
/// figure over-counts by the removed bytes until the shard's next install. This is the clock-skew
/// residual, in the direction it has.
#[tokio::test(flavor = "multi_thread")]
async fn a_remover_clock_behind_the_folder_leaves_the_debit_inert() {
    let (truth, figure, bytes) = tokio::time::timeout(
        BOUND,
        removal_inside_a_visit(
            VisitHookPhase::BeforeInstall,
            Some(Duration::from_secs(3600)),
        ),
    )
    .await
    .expect("T4d skewed timed out");
    println!("T4d skewed: truth={truth} figure={figure} bytes={bytes}");
    assert_eq!(
        figure,
        truth + bytes,
        "with the remover's clock behind the folder's, the debit is inert and the figure \
         over-counts by the removed bytes (truth={truth}, bytes={bytes}, figure={figure})"
    );
}

/// T4e. A graduation is debited at the next consolidation cycle, without a fold.
#[tokio::test(flavor = "multi_thread")]
async fn a_graduation_is_debited_at_the_next_cycle_without_a_fold() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage(&m, 3).await;
        let before = derived(&m).await;
        let bytes = census(dir, std::slice::from_ref(&keys[0]));
        let graduations = m.get_write_cache_graduations_total().await;
        let graduated = m.refresh_write_cache_ttl(&keys[0]).await;
        let consolidator = m.get_journal_consolidator().await.expect("consolidator");
        let _ = consolidator.run_consolidation_cycle().await;
        let figure = derived(&m).await;
        let graduations_after = m.get_write_cache_graduations_total().await;
        println!(
            "T4e graduation: before={before} bytes={bytes} figure={figure} graduated={graduated:?} \
             graduations={graduations}->{graduations_after} truth={}",
            census(dir, &keys)
        );
        assert!(
            graduations_after > graduations,
            "fixture: the read graduated the object"
        );
        assert_eq!(
            figure,
            before - bytes,
            "after one consolidation cycle the figure must exclude the graduated object's bytes \
             (before={before}, bytes={bytes})"
        );
    })
    .await
    .expect("T4e timed out");
}

/// Wait, bounded, for a staging pass the consolidation cycle started to finish.
async fn settle(m: &CacheManager) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while m.staging_eviction_in_progress() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a staging pass the cycle started did not finish within 30 s");
}

/// T4f. The debit survives the evictor's restart: a new process on the volume, with none of the
/// evictor's in-process state, derives the debited figure.
#[tokio::test(flavor = "multi_thread")]
async fn a_debit_survives_the_evictors_restart() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let (dir, a, keys, _b) = two_on_one_volume(&tmp, "volume-via-b").await;
        claim_all(&dir, &keys).await;
        let freed = a.evict_staging_tier().await;
        release_claims(&dir);
        assert!(freed > 0, "fixture: A evicts");
        drop(a);
        let link = tmp.path().join("volume-after-restart");
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        let restarted = manager(&link).await;
        let truth = census(&dir, &keys);
        let figure = derived(&restarted).await;
        println!("T4f restart: freed={freed} truth={truth} figure={figure}");
        assert_eq!(
            figure, truth,
            "a process started after the eviction derived the pre-eviction figure: the debit did \
             not reach the shared volume (freed={freed}, truth={truth})"
        );
    })
    .await
    .expect("T4f timed out");
}

/// How the evicting pass's re-fold is made to retain.
#[derive(Debug, Clone, Copy)]
enum Retain {
    /// A peer's pending lock held through the re-fold: `PendingUnreadable`.
    PendingLock,
    /// A legacy flat ledger file present: `LegacyLedgerPresent`.
    LegacyLedger,
}

/// A evicts and its re-fold retains; then B passes and A passes again. Neither may free anything:
/// the first pass already brought the tier under its trigger.
async fn a_retained_re_fold_is_not_re_evicted(retain: Retain) {
    let tmp = TempDir::new().unwrap();
    let (dir, a, keys, b) = two_on_one_volume(&tmp, "volume-via-b").await;
    let (trigger, target) = a.get_staging_eviction_thresholds();
    let before = derived(&a).await;
    assert!(before > trigger, "fixture: over the trigger");
    let held: Arc<Mutex<Option<PeerPendingLock>>> = Arc::new(Mutex::new(None));
    let legacy = s3_proxy::write_ledger::WriteLedger::legacy_path_for(&dir, "peer-host:7");
    {
        let (held, dir, legacy) = (held.clone(), dir.clone(), legacy.clone());
        CacheManager::set_before_staging_refold_hook_for_tests(
            &dir.clone(),
            Box::new(move || match retain {
                Retain::PendingLock => {
                    *held.lock().unwrap() = Some(PeerPendingLock::hold(&dir));
                }
                Retain::LegacyLedger => {
                    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
                    std::fs::write(&legacy, b"").unwrap();
                }
            }),
        );
    }
    let first = a.evict_staging_tier().await;
    drop(held.lock().unwrap().take());
    let _ = std::fs::remove_file(&legacy);
    let truth = census(&dir, &keys);
    assert!(
        first > 0 && truth <= trigger,
        "fixture: A's pass evicts under the trigger (first={first}, truth={truth})"
    );
    let by_b = b.evict_staging_tier().await;
    let again = a.evict_staging_tier().await;
    println!(
        "RE-EVICTION {retain:?}: before={before} first={first} truth={truth} by_b={by_b} \
         again={again} trigger={trigger} target={target}"
    );
    assert_eq!(
        (by_b, again),
        (0, 0),
        "after a pass whose re-fold retained ({retain:?}), a later pass evicted again from the \
         pre-eviction figure (before={before}, first={first}, truth={truth})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_re_fold_retained_on_a_held_pending_lock_is_not_re_evicted() {
    tokio::time::timeout(
        BOUND,
        a_retained_re_fold_is_not_re_evicted(Retain::PendingLock),
    )
    .await
    .expect("timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_re_fold_retained_on_a_legacy_ledger_is_not_re_evicted() {
    tokio::time::timeout(
        BOUND,
        a_retained_re_fold_is_not_re_evicted(Retain::LegacyLedger),
    )
    .await
    .expect("timed out");
}

/// Graduations since the last fold of their shards: the pass must size without them. Seven uploads
/// are over the trigger; two of them are read, which takes the staged tier under it.
///
/// The consolidation cycle's in-loop check reads the memoised figure, which still holds the
/// graduated bytes, and starts a pass of its own; the test waits for it and counts what it evicted
/// in the survivors, so both passes are judged.
#[tokio::test(flavor = "multi_thread")]
async fn a_pass_after_graduations_sizes_without_the_graduated_bytes() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage(&m, 7).await;
        let (trigger, target) = m.get_staging_eviction_thresholds();
        let before = derived(&m).await;
        assert!(before > trigger, "fixture: over the trigger");
        for key in &keys[..2] {
            let _ = m.refresh_write_cache_ttl(key).await;
        }
        let consolidator = m.get_journal_consolidator().await.expect("consolidator");
        let _ = consolidator.run_consolidation_cycle().await;
        settle(&m).await;
        let truth = census(dir, &[keys[0].clone(), keys[1].clone()]);
        assert_eq!(truth, 0, "fixture: both reads graduated");
        let truth = census(dir, &keys);
        let freed = m.evict_staging_tier().await;
        println!(
            "GRADUATION pass: before={before} truth={truth} freed={freed} trigger={trigger} \
             target={target} graduated_bytes={}",
            before - truth
        );
        assert_eq!(
            (freed, staged_on_disk(dir, &keys).len()),
            (0, keys.len()),
            "the pass sized itself with graduated bytes still in the figure (before={before}, \
             truth={truth}, freed={freed})"
        );
    })
    .await
    .expect("graduation pass timed out");
}

/// A pass whose debits could not be published (the debit directory is unusable for that one pass)
/// must not lose them: the consolidation cycle publishes them once the directory works, and a
/// later pass frees nothing beyond what the first pass already freed.
#[tokio::test(flavor = "multi_thread")]
async fn a_debit_that_could_not_be_published_is_published_by_the_next_cycle() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        // Eight objects, so that the figure still holding the first pass's bytes (one object
        // fewer than `before`, as the red side showed) is over the trigger and a repeat pass
        // would evict again.
        let keys = stage(&m, 8).await;
        let (trigger, _) = m.get_staging_eviction_thresholds();
        let before = derived(&m).await;
        assert!(before > trigger, "fixture: over the trigger");
        assert!(
            before - census(dir, &keys[..1]) > trigger,
            "fixture: the figure stays over the trigger when one object's bytes leave it \
             (before={before}, trigger={trigger})"
        );

        // A regular file where the debit directory goes: creating the directory fails.
        let state = dir.join("metadata").join("_shard_state");
        let blocker = state.join("staged_debits");
        std::fs::create_dir_all(&state).unwrap();
        std::fs::write(&blocker, b"in the way").unwrap();
        claim_all(dir, &keys).await;
        let first = m.evict_staging_tier().await;
        release_claims(dir);
        std::fs::remove_file(&blocker).unwrap();
        assert!(first > 0, "fixture: the first pass evicts (first={first})");

        let consolidator = m.get_journal_consolidator().await.expect("consolidator");
        let _ = consolidator.run_consolidation_cycle().await;
        settle(&m).await;
        let second = m.evict_staging_tier().await;
        let truth = census(dir, &keys);
        let figure = derived(&m).await;
        let document = state
            .join("staged_debits")
            .join(format!("{}.json", instance_of(&m).await));
        println!(
            "T4h unpublished debit: before={before} first={first} second={second} truth={truth} \
             figure={figure} document_exists={}",
            document.exists()
        );
        assert!(
            document.exists(),
            "the debit the first pass could not write was not published by the next cycle \
             (first={first}, second={second})"
        );
        assert_eq!(
            (second, figure),
            (0, truth),
            "after the debit directory worked again, a later pass freed more or the figure still \
             holds the first pass's bytes (before={before}, first={first}, truth={truth})"
        );
    })
    .await
    .expect("T4h timed out");
}

/// This process's own graduation is in its own reads at once, not one consolidation interval
/// later: no cycle runs between the graduation and the read.
#[tokio::test(flavor = "multi_thread")]
async fn a_graduation_is_in_the_graduating_proxys_own_figure_before_its_cycle_flushes() {
    tokio::time::timeout(BOUND, async {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let m = manager(dir).await;
        let keys = stage(&m, 3).await;
        let before = derived(&m).await;
        let bytes = census(dir, std::slice::from_ref(&keys[0]));
        let graduated = m.refresh_write_cache_ttl(&keys[0]).await;
        let figure = derived(&m).await;
        println!(
            "T4i own unflushed debit: before={before} bytes={bytes} figure={figure} \
             graduated={graduated:?}"
        );
        assert!(bytes > 0, "fixture: the object holds staged bytes");
        assert_eq!(
            figure,
            before - bytes,
            "the graduating proxy sized from a figure that still held the graduated bytes before \
             its cycle flushed them (before={before}, bytes={bytes})"
        );
    })
    .await
    .expect("T4i timed out");
}
