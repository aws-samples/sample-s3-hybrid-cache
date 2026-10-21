//! How a consolidation cycle's cost scales with the volume it runs on.
//!
//! # A MEASUREMENT, not an assertion — both tests are `#[ignore]`
//!
//! Recorded, on the fleet's EFS volume, that a consolidation cycle carrying one journal
//! entry logs `duration=` 4,955-6,219 ms, and that the first cycle after a restart logs
//! 98,060 ms. The source trace in the investigation record
//! attributes the steady-state cost to `JournalConsolidator::maintain_staged_authority`, which
//! reads every fold checkpoint, walks the shard population twice and reads every repair
//! document twice on every non-idle cycle, and attributes the first-cycle cost to the staged
//! fold, the `st_size` fold and the ledger repair all running inline in that first cycle.
//!
//! These tests produce the evidence a local directory can give for that trace: the **shape**
//! of the cost as a function of shard count and of range-file count. A local APFS or tmpfs
//! directory answers an open or a `stat` in microseconds where EFS takes milliseconds, so the
//! absolute figures here say nothing about the fleet. They assert nothing, because a timing
//! threshold on a developer laptop is a flake by construction. Run them with:
//!
//! ```text
//! cargo test --test consolidation_cycle_cost_scaling_test -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `--test-threads=1` because both tests drive process-global pass state (`st_size_fold`'s
//! cadence statics) and time wall clock, and two in parallel would measure each other.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::ledger_repair::{
    observed_shards_on_disk, read_repair_checkpoints, repair_rotation_complete,
    repair_stalest_age_secs, RepairShardCheckpoint,
};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use s3_proxy::staged_fold::{
    read_shard_checkpoints, rotation_complete, Shard, StagedShardCheckpoint,
};
use tempfile::TempDir;

const INSTANCE: &str = "scaling-test-instance";
const FIXTURE_BUCKET: &str = "fixture-bucket";

fn l1_name(i: usize) -> String {
    format!("{:02x}", i % 256)
}

/// `n` shards spread over `ceil(n / 256)` buckets, each with what a populated fleet shard
/// carries for the per-cycle reads: an L1 directory under `metadata/` (what
/// `observed_shards_on_disk` counts), a fold checkpoint, and a completed repair document.
fn seed_shards(cache_dir: &Path, n: usize) {
    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    for i in 0..n {
        let bucket = format!("bucket{:03}", i / 256);
        let l1 = l1_name(i);
        std::fs::create_dir_all(
            cache_dir
                .join("metadata")
                .join(&bucket)
                .join(&l1)
                .join("000"),
        )
        .unwrap();
        let shard = Shard::new(bucket.clone(), l1.clone());
        StagedShardCheckpoint::unfolded_for(&shard)
            .install(cache_dir)
            .unwrap();
        RepairShardCheckpoint {
            schema: 1,
            bucket,
            l1,
            swept_at: now_nanos,
            swept_by: INSTANCE.to_string(),
            resume_l2: None,
            ranges_reappended: 0,
            metas_examined: 0,
            dirs_read: 1,
            other: serde_json::Map::new(),
        }
        .install(cache_dir)
        .unwrap();
    }
}

/// `per_shard` one-byte range files in each of `shards` shards of one bucket, one L2 leaf each.
/// This is what the `st_size` fold `stat`s.
fn seed_range_files(cache_dir: &Path, shards: usize, per_shard: usize) {
    for s in 0..shards {
        let dir = cache_dir
            .join("ranges")
            .join("rangebucket")
            .join(l1_name(s))
            .join("000");
        std::fs::create_dir_all(&dir).unwrap();
        for f in 0..per_shard {
            std::fs::write(dir.join(format!("obj{f}_0-0.bin")), b"x").unwrap();
        }
        std::fs::create_dir_all(
            cache_dir
                .join("metadata")
                .join("rangebucket")
                .join(l1_name(s))
                .join("000"),
        )
        .unwrap();
    }
}

/// One journal `Add` entry for a fresh key, with its range file present so the cycle can
/// consolidate it — the fleet's `keys=1, entries=1` shape.
async fn append_one_entry(cache_dir: &Path, journal: &JournalManager, seq: usize) {
    let cache_key = format!("{FIXTURE_BUCKET}/object-{seq}");
    let range_path =
        s3_proxy::disk_cache::get_sharded_path(&cache_dir.join("ranges"), &cache_key, "_0-99.bin")
            .unwrap();
    std::fs::create_dir_all(range_path.parent().unwrap()).unwrap();
    std::fs::write(&range_path, vec![0u8; 100]).unwrap();
    let now = SystemTime::now();
    let entry = JournalEntry {
        timestamp: now,
        instance_id: INSTANCE.to_string(),
        cache_key: cache_key.clone(),
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
        object_metadata: None,
        fill_witness: None,
    };
    journal.append_range_entry(&cache_key, entry).await.unwrap();
}

async fn consolidator_for(cache_dir: &Path) -> (Arc<JournalConsolidator>, Arc<JournalManager>) {
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.to_path_buf(),
        INSTANCE.to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.to_path_buf(),
        Duration::from_secs(5),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.to_path_buf(),
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
    (consolidator, journal_manager)
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// The last fold attempt the freshness document records, so a steady-state cycle that re-ran
/// the staged fold is visible rather than silently inflating the figure.
fn fold_last_attempt(cache_dir: &Path) -> Option<u64> {
    let p = cache_dir
        .join("metadata")
        .join("_shard_state")
        .join("staged_fold_freshness.json");
    let v: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(p).ok()?).ok()?;
    v.get("last_attempt_at").and_then(|x| x.as_u64())
}

/// Steady-state cycle cost against shard count, with the per-cycle reads timed separately.
///
/// The columns to read are `steady_ms` against `authority_reads_ms`: if the trace is right,
/// both grow linearly with `shards` and the second accounts for most of the first. The
/// `fold_reran` column must read `0`, or a steady-state cycle was re-running the staged fold and
/// the figure is not the steady state the fleet logs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement: prints a scaling table and asserts nothing; run with --ignored --nocapture"]
async fn steady_state_cycle_cost_scales_with_shard_count() {
    println!(
        "\n{:>7} | {:>9} | {:>9} | {:>7} | {:>11} | {:>8} | {:>9} | {:>9} | {:>9} | {:>10}",
        "shards",
        "first_ms",
        "steady_ms",
        "entries",
        "fold_reran",
        "authz_ms",
        "fold_ck_ms",
        "shardwalk",
        "repair_ck",
        "authz/stdy"
    );
    for &n in &[0usize, 256, 1024, 2048, 4096, 8192] {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        seed_shards(dir, n);
        let (consolidator, journal) = consolidator_for(dir).await;

        append_one_entry(dir, &journal, 0).await;
        let first = consolidator.run_consolidation_cycle().await.unwrap();

        let mut steady = Vec::new();
        let mut entries = Vec::new();
        let mut fold_reran = 0;
        for seq in 1..=5 {
            append_one_entry(dir, &journal, seq).await;
            let before = fold_last_attempt(dir);
            let r = consolidator.run_consolidation_cycle().await.unwrap();
            if fold_last_attempt(dir) != before {
                fold_reran += 1;
            }
            steady.push(r.cycle_duration);
            entries.push(r.entries_consolidated);
        }

        // The three calls `maintain_staged_authority` makes that read the volume, in the order it
        // makes them, and the three underlying reads individually.
        let mut authz = Vec::new();
        let mut fold_ck = Vec::new();
        let mut walk = Vec::new();
        let mut repair_ck = Vec::new();
        for _ in 0..3 {
            let t = Instant::now();
            let _ = rotation_complete(dir);
            let _ = repair_rotation_complete(dir);
            let _ = repair_stalest_age_secs(dir);
            authz.push(t.elapsed());

            let t = Instant::now();
            let _ = read_shard_checkpoints(dir);
            fold_ck.push(t.elapsed());
            let t = Instant::now();
            let _ = observed_shards_on_disk(dir);
            walk.push(t.elapsed());
            let t = Instant::now();
            let _ = read_repair_checkpoints(dir);
            repair_ck.push(t.elapsed());
        }
        let steady_m = median(steady);
        let authz_m = median(authz);
        println!(
            "{:>7} | {:>9.1} | {:>9.2} | {:>7} | {:>11} | {:>8.2} | {:>9.2} | {:>9.2} | {:>9.2} | {:>10.2}",
            n,
            ms(first.cycle_duration),
            ms(steady_m),
            format!("{:?}", entries.iter().max().unwrap()),
            fold_reran,
            ms(authz_m),
            ms(median(fold_ck)),
            ms(median(walk)),
            ms(median(repair_ck)),
            if steady_m.is_zero() {
                0.0
            } else {
                authz_m.as_secs_f64() / steady_m.as_secs_f64()
            }
        );
    }
}

/// The `st_size` fold's cost against range-file count, at a fixed shard count, beside the
/// steady-state cycle on the same volume.
///
/// The first cycle after a restart runs this pass inline, because its "last completed"
/// timestamp starts at 0. That timestamp is a process-global static with no reset reachable
/// from an integration test, so only the first cycle in this whole binary would run it; the
/// pass is therefore timed by calling `run_st_size_fold` directly, with the same unbounded
/// budget shape the first cycle gives it (30 min by default, never reached here). If the trace
/// is right, `st_size_ms` grows with `files` while `steady_ms` does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement: prints a scaling table and asserts nothing; run with --ignored --nocapture"]
async fn first_cycle_st_size_fold_cost_scales_with_range_file_count() {
    const SHARDS: usize = 16;
    println!(
        "\n{:>8} | {:>10} | {:>9}",
        "files", "st_size_ms", "steady_ms"
    );
    for &per_shard in &[0usize, 250, 1000, 4000] {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        seed_range_files(dir, SHARDS, per_shard);
        let (consolidator, journal) = consolidator_for(dir).await;

        let mut fold = Vec::new();
        for _ in 0..3 {
            let t = Instant::now();
            let outcome =
                s3_proxy::st_size_fold::run_st_size_fold(dir, INSTANCE, Duration::from_secs(1800))
                    .await;
            fold.push(t.elapsed());
            assert!(
                !outcome.enumeration_failed,
                "the fixture must be enumerable or the figure times nothing"
            );
        }

        append_one_entry(dir, &journal, 0).await;
        consolidator.run_consolidation_cycle().await.unwrap();
        let mut steady = Vec::new();
        for seq in 1..=3 {
            append_one_entry(dir, &journal, seq).await;
            steady.push(
                consolidator
                    .run_consolidation_cycle()
                    .await
                    .unwrap()
                    .cycle_duration,
            );
        }
        println!(
            "{:>8} | {:>10.2} | {:>9.2}",
            SHARDS * per_shard,
            ms(median(fold)),
            ms(median(steady))
        );
    }
}
