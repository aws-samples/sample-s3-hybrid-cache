//! The first consolidation cycle after start consolidates journal work without waiting for the
//! maintenance passes (`CONSOLIDATION-MAINTENANCE-PASSES-RUN-INLINE-IN-THE-CYCLE`).
//!
//! `run_consolidation_cycle` used to await the staged fold, the `st_size` fold and the ledger
//! repair before its idle check. Each gates on a last-run timestamp that is 0 at process start,
//! so all three ran in the first cycle, and the `st_size` fold is O(resident range files) with a
//! 30-minute budget. On the fleet the first cycle took 104 s, 78 s of it the `st_size`
//! fold, and no journal entry became a `.meta` on that instance until it finished.
//!
//! One test in this binary, deliberately: the `st_size` fold's cadence is process-global state
//! (`LAST_FOLD_COMPLETED_SECS` starts at 0 and is set by the first completed fold), and the
//! pending-first-fold state this test needs exists only until some fold in the process
//! completes. A second test here that ran a cycle first would leave nothing pending.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use tempfile::TempDir;

const INSTANCE: &str = "off-cycle-test-instance";

/// `shards` × `per_shard` one-byte range files, one L2 leaf per shard. This is what the
/// `st_size` fold `stat`s, one file at a time, which is what makes it slow.
fn seed_range_files(cache_dir: &Path, shards: usize, per_shard: usize) {
    for s in 0..shards {
        let l1 = format!("{:02x}", s % 256);
        let dir = cache_dir
            .join("ranges")
            .join("rangebucket")
            .join(&l1)
            .join("000");
        std::fs::create_dir_all(&dir).unwrap();
        for f in 0..per_shard {
            std::fs::write(dir.join(format!("obj{f}_0-0.bin")), b"x").unwrap();
        }
        std::fs::create_dir_all(
            cache_dir
                .join("metadata")
                .join("rangebucket")
                .join(&l1)
                .join("000"),
        )
        .unwrap();
    }
}

/// One journal `Add` for a fresh key with its range file present, so the cycle is non-idle and
/// has exactly one entry to turn into a `.meta`.
async fn append_one_entry(cache_dir: &Path, journal: &JournalManager, cache_key: &str) {
    let range_path =
        s3_proxy::disk_cache::get_sharded_path(&cache_dir.join("ranges"), cache_key, "_0-99.bin")
            .unwrap();
    std::fs::create_dir_all(range_path.parent().unwrap()).unwrap();
    std::fs::write(&range_path, vec![0u8; 100]).unwrap();
    let now = SystemTime::now();
    let entry = JournalEntry {
        timestamp: now,
        instance_id: INSTANCE.to_string(),
        cache_key: cache_key.to_string(),
        range_spec: RangeSpec {
            start: 0,
            end: 99,
            file_path: range_path.to_string_lossy().to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 100,
            uncompressed_size: 100,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        },
        operation: JournalOperation::Add,
        range_file_path: range_path.to_string_lossy().to_string(),
        metadata_version: 1,
        new_ttl_secs: None,
        object_ttl_secs: None,
        access_increment: None,
        // A range fill carries the response's metadata; only that may create the `.meta`.
        object_metadata: Some(s3_proxy::cache_types::ObjectMetadata::default()),
        fill_witness: None,
    };
    journal.append_range_entry(cache_key, entry).await.unwrap();
}

/// The first cycle, with the `st_size` fold pending and slow, turns its journal entry into a
/// `.meta` and returns while the fold is still running.
///
/// # What is asserted, and why each half is needed
///
/// - **At the instant the cycle returns, the entry's `.meta` exists AND the fold is still in
///   flight** (`st_size_fold::fold_in_progress()`). The `.meta` alone would pass with the
///   passes inline too — the cycle writes it eventually either way. What distinguishes the fix
///   is the ordering: the entry is servable before the pass reports completion.
/// - **The fold then completes and was slower than the cycle.** Without this the first half
///   could pass because the fold never started, or finished instantly; the fold's own measured
///   duration against the cycle's is what shows the fixture actually made it slow.
///
/// # RED SIDE
///
/// Replace `self.spawn_maintenance_passes();` in `run_consolidation_cycle` with the three passes
/// awaited inline and the first assertion fails: the cycle returns only after the fold has
/// completed, so `fold_in_progress()` is `false` when the `.meta` is first observable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_cycle_consolidates_without_waiting_for_the_st_size_fold() {
    // 64 shards x 1,000 files. measured the fold at ~0.037 ms per file locally, so ~2.4 s,
    // against a one-entry cycle of ~15-20 ms.
    const SHARDS: usize = 64;
    const PER_SHARD: usize = 1_000;
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    seed_range_files(dir, SHARDS, PER_SHARD);

    assert!(
        !s3_proxy::st_size_fold::fold_in_progress(),
        "fixture precondition: no fold may be running before the cycle"
    );
    assert_eq!(
        s3_proxy::st_size_fold::last_fold_duration_secs(),
        0,
        "fixture precondition: no fold has completed in this process, so the first cycle's \
         gate is open exactly as it is after a restart"
    );

    let journal_manager = Arc::new(JournalManager::new(dir.to_path_buf(), INSTANCE.to_string()));
    let lock_manager = Arc::new(MetadataLockManager::new(
        dir.to_path_buf(),
        Duration::from_secs(5),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        dir.to_path_buf(),
        journal_manager.clone(),
        lock_manager,
        ConsolidationConfig {
            interval: Duration::from_secs(5),
            // 0 disables eviction, so no detached eviction pass competes with the timing.
            max_cache_size: 0,
            consolidation_cycle_timeout: Duration::from_secs(30),
            max_keys_per_cycle: 5000,
            ..ConsolidationConfig::default()
        },
    ));
    consolidator.initialize().await.unwrap();

    let cache_key = "fixture-bucket/first-cycle-object";
    append_one_entry(dir, &journal_manager, cache_key).await;
    let meta_path =
        s3_proxy::disk_cache::get_sharded_path(&dir.join("metadata"), cache_key, ".meta").unwrap();
    assert!(
        !meta_path.exists(),
        "fixture precondition: the entry must not already have a .meta"
    );

    let started = Instant::now();
    let result = consolidator.run_consolidation_cycle().await.unwrap();
    let cycle_elapsed = started.elapsed();
    let fold_running_at_return = s3_proxy::st_size_fold::fold_in_progress();
    let meta_at_return = meta_path.exists();

    assert!(
        result.entries_consolidated >= 1 && meta_at_return,
        "the first cycle must consolidate its entry: entries_consolidated={}, .meta present={}",
        result.entries_consolidated,
        meta_at_return
    );
    assert!(
        fold_running_at_return,
        "the first cycle returned after {cycle_elapsed:?} with the `st_size` fold already \
         complete: the cycle waited for the pass, so the entry was not servable until the pass \
         finished"
    );

    let fold_started = Instant::now();
    consolidator.await_maintenance_passes().await;
    let fold_tail = fold_started.elapsed();
    assert!(
        !s3_proxy::st_size_fold::fold_in_progress(),
        "the fold must complete once awaited"
    );
    println!(
        "first cycle {:?} (entries={}); st_size fold still running at return, finished {:?} \
         later",
        cycle_elapsed, result.entries_consolidated, fold_tail
    );
}
