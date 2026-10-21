//! An idle consolidation cycle must still collect a size delta another instance left behind.
//!
//! # The fleet fact this pins (2026-09-25)
//!
//! One proxy flushed a `+1,048,595` size delta to `size_tracking/delta_*.json` and was then
//! stopped. No cycle collected that file for 5 min 12 s, until another proxy's first
//! post-restart write made its own cycle non-idle. For that whole window the shared
//! `size_state.json` `total_size`, which capacity and eviction decisions read, was low by the
//! delta.
//!
//! # Why it happened, traced
//!
//! `JournalConsolidator::run_consolidation_cycle` returns early, before taking the global
//! consolidation lock, when `SizeAccumulator::has_pending_delta` (this instance's two in-memory
//! atomics only) and `has_pending_journal_files` (a listing of `metadata/_journals/` for any
//! instance's non-empty `.journal`) are both false. Neither looks at `size_tracking/`, and
//! delta files are written there OUTSIDE a cycle by three sites: `shutdown` step 0, read-tier
//! eviction (`cache.rs`) and write-tier eviction (`write_cache_manager.rs`). So a delta flushed
//! by an instance that then stops is invisible to every running proxy's idle check.
//!
//! `shutdown` is the sharpest case: it flushes its accumulator and THEN runs a final cycle,
//! which finds its own accumulator at zero and returns idle, leaving the file it has just
//! written uncollected.
//!
//! # What these tests assert
//!
//! - an idle peer collects a stopped instance's delta within ONE cycle;
//! - `shutdown` collects the delta its own step 0 flushed;
//! - the fix does not cost idleness: a directory holding only the non-delta `size_tracking/`
//!   files, a `.json.tmp`, or nothing pending leaves the cycle idle, observed through the idle
//!   return's `current_size: 0` (every lock-taking return reads the real size instead).
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use tempfile::TempDir;

/// The figure observed, so a failure message reads in the fleet's units.
const PEER_DELTA: u64 = 1_048_595;

fn consolidator(cache_dir: &Path, journal_instance: &str) -> JournalConsolidator {
    JournalConsolidator::new(
        cache_dir.to_path_buf(),
        Arc::new(JournalManager::new(
            cache_dir.to_path_buf(),
            journal_instance.to_string(),
        )),
        Arc::new(MetadataLockManager::new(
            cache_dir.to_path_buf(),
            Duration::from_secs(30),
            3,
        )),
        ConsolidationConfig::default(),
    )
}

/// Every `delta_*.json` in `size_tracking/`, excluding `.json.tmp`, sorted.
fn delta_files(cache_dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(cache_dir.join("size_tracking"))
        .map(|d| {
            d.flatten()
                .filter_map(|e| e.file_name().to_str().map(str::to_string))
                .filter(|n| n.starts_with("delta_") && n.ends_with(".json"))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

async fn total_size(c: &JournalConsolidator) -> u64 {
    c.get_size_state().await.map(|s| s.total_size).unwrap_or(0)
}

/// Instance B flushes a delta and stops without its own final cycle (killed, or its final
/// cycle lost the global lock to a peer). Instance A has no traffic at all and must collect
/// B's delta within one cycle.
#[tokio::test]
async fn an_idle_peer_collects_a_stopped_instances_delta_file_within_one_cycle() {
    let tmp = TempDir::new().unwrap();

    let a = consolidator(tmp.path(), "instance-a");
    a.initialize().await.unwrap();
    assert_eq!(total_size(&a).await, 0, "FIXTURE: the volume starts empty");

    {
        let b = consolidator(tmp.path(), "instance-b");
        b.size_accumulator().add(PEER_DELTA);
        // `shutdown` step 0, and the eviction sites, flush exactly like this: outside a cycle.
        b.size_accumulator().flush().await.unwrap();
        // B stops here. Dropped with no final cycle of its own.
    }
    let pending = delta_files(tmp.path());
    assert_eq!(
        pending.len(),
        1,
        "FIXTURE: B's flush must have left exactly one delta file, found {:?}",
        pending
    );
    assert!(
        !a.size_accumulator().has_pending_delta(),
        "FIXTURE: A must have nothing of its own pending, or its cycle is non-idle for a reason \
         unrelated to B's file and this asserts nothing"
    );

    // One cycle, no traffic on A.
    let result = a.run_consolidation_cycle().await.unwrap();

    assert_eq!(
        total_size(&a).await,
        PEER_DELTA,
        "an idle instance must collect a stopped peer's delta file within one cycle; \
         size_state.total_size is still low by {} bytes and {:?} is still on disk. The idle \
         check at the top of run_consolidation_cycle looks only at this instance's accumulator \
         and at journal files, never at size_tracking/delta_*.json",
        PEER_DELTA,
        delta_files(tmp.path())
    );
    assert!(
        delta_files(tmp.path()).is_empty(),
        "the collected delta file must be removed, found {:?}",
        delta_files(tmp.path())
    );
    assert_eq!(
        result.current_size, PEER_DELTA,
        "the cycle that collected must report the real size, which only a lock-taking return does"
    );

    // And having collected, the next cycle is idle again: the check does not stick.
    let again = a.run_consolidation_cycle().await.unwrap();
    assert_eq!(
        again.current_size, 0,
        "with nothing pending the cycle must take the idle return (current_size 0) rather than \
         the lock-taking path, which reads the real size ({})",
        PEER_DELTA
    );
    assert_eq!(
        total_size(&a).await,
        PEER_DELTA,
        "and must not collect twice"
    );
}

/// `shutdown` flushes its accumulator and then runs a final cycle. That cycle must collect the
/// delta the flush has just written, rather than finding its own accumulator at zero and
/// returning idle.
#[tokio::test]
async fn shutdown_collects_the_delta_its_own_flush_wrote() {
    let tmp = TempDir::new().unwrap();

    let b = consolidator(tmp.path(), "instance-b");
    b.initialize().await.unwrap();
    b.size_accumulator().add(PEER_DELTA);
    b.shutdown().await.unwrap();

    assert_eq!(
        total_size(&b).await,
        PEER_DELTA,
        "shutdown's final cycle must collect the delta shutdown step 0 flushed; it is still on \
         disk as {:?}, and no running peer's idle check would see it either",
        delta_files(tmp.path())
    );
    assert!(delta_files(tmp.path()).is_empty());
}

/// The fix must not make an idle volume non-idle. `size_tracking/` always holds
/// `size_state.json` and its lock, and can hold a stale `.json.tmp` from an interrupted write;
/// none of those is a pending delta, and none must make a cycle take the global lock.
#[tokio::test]
async fn non_delta_size_tracking_files_leave_the_cycle_idle() {
    let tmp = TempDir::new().unwrap();

    // Seed a non-zero total through a real collection, so an idle return (current_size 0) and a
    // lock-taking return (the real size) are distinguishable.
    let a = consolidator(tmp.path(), "instance-a");
    a.initialize().await.unwrap();
    a.size_accumulator().add(PEER_DELTA);
    let seeded = a.run_consolidation_cycle().await.unwrap();
    assert_eq!(
        seeded.current_size, PEER_DELTA,
        "FIXTURE: the seeding cycle must collect"
    );

    let st = tmp.path().join("size_tracking");
    assert!(
        st.join("size_state.json").exists(),
        "FIXTURE: size_state.json present"
    );
    std::fs::write(st.join("validation.json"), "{}").unwrap();
    std::fs::write(st.join("delta_instance-x_7.json.tmp"), "{\"delta\": 5}").unwrap();
    std::fs::write(st.join("unrelated.json"), "{}").unwrap();

    let idle = a.run_consolidation_cycle().await.unwrap();
    assert_eq!(
        idle.current_size, 0,
        "size_state.json, validation.json, a delta .json.tmp and an unrelated file are not a \
         pending delta; the cycle must take the idle return (current_size 0), not the \
         lock-taking path (which reports {})",
        PEER_DELTA
    );
    assert_eq!(total_size(&a).await, PEER_DELTA);
}
