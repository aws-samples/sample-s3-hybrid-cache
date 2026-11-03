//! Pending-journal lookup throughput: `TASKS` concurrent tasks each make `LOOKUPS`
//! `find_pending_journal_ranges` calls over a journal of 1 MiB (about what production holds
//! with the consolidator keeping up) and of 32 MiB (one that has fallen behind), on a 4-worker
//! runtime. Prints lookups per second for each.
//!
//! Ignored by default because it measures time; run it alone with
//! `cargo test --release --test pending_journal_lookup_throughput_test -- --ignored --nocapture`.

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use tempfile::TempDir;
use tokio::sync::Mutex;

const TASKS: usize = 64;
const LOOKUPS: usize = 20;
const ROUNDS: usize = 5;

const INSTANCE: &str = "pending-lookup-cost-instance";
const KEYS: usize = 256;

/// A pending range-fill `Add`, as a 3.0.0 writer records it (with a file identity, so it is
/// not an unverified prefix).
fn pending_add(cache_key: &str, start: u64) -> JournalEntry {
    let len = 100;
    let file_path = format!("lookup-bucket/{}_{start}.bin", cache_key.replace('/', "_"));
    let spec = RangeSpec::new(
        start,
        start + len - 1,
        file_path,
        CompressionAlgorithm::None,
        len,
        len,
    )
    .with_file_identity(Some(s3_proxy::cache_types::RangeFileIdentity {
        size: len,
        mtime_ns: 1,
    }));
    JournalEntry {
        timestamp: SystemTime::now(),
        instance_id: INSTANCE.to_string(),
        cache_key: cache_key.to_string(),
        range_spec: spec,
        operation: JournalOperation::Add,
        range_file_path: String::new(),
        metadata_version: 0,
        new_ttl_secs: None,
        object_ttl_secs: None,
        access_increment: None,
        object_metadata: None,
        fill_witness: None,
    }
}

/// A shared-storage `DiskCacheManager` (the journal lookup returns nothing outside that mode)
/// over a journal of `KEYS` keys' entries, interleaved, of at least `journal_target` bytes. Returns the
/// manager, the journal size, and the cache directory.
async fn manager_over_journal(journal_target: usize) -> (DiskCacheManager, usize, TempDir) {
    let dir = TempDir::new().unwrap();
    let cache_dir = dir.path().to_path_buf();
    let mut manager = DiskCacheManager::new(cache_dir.clone(), false, 1024, false, 1_048_576);
    manager.initialize().await.unwrap();
    manager.set_hybrid_metadata_writer(Arc::new(Mutex::new(HybridMetadataWriter::new(
        cache_dir.clone(),
        Arc::new(MetadataLockManager::new(
            cache_dir.clone(),
            Duration::from_millis(300),
            0,
        )),
        Arc::new(JournalManager::new(cache_dir.clone(), INSTANCE.to_string())),
        Arc::new(ConsolidationTrigger::new(10 * 1024 * 1024, 1000)),
    ))));
    assert!(manager.is_shared_storage_mode(), "fixture precondition");

    let journals = cache_dir.join("metadata").join("_journals");
    std::fs::create_dir_all(&journals).unwrap();
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(journals.join(format!("{INSTANCE}.journal"))).unwrap(),
    );
    let mut written = 0usize;
    let mut round = 0u64;
    while written < journal_target {
        for k in 0..KEYS {
            let key = format!("lookup-bucket/object-{k:04}");
            let line = serde_json::to_string(&pending_add(&key, round * 1000)).unwrap();
            writeln!(out, "{line}").unwrap();
            written += line.len() + 1;
        }
        round += 1;
    }
    out.flush().unwrap();
    (manager, written, dir)
}

async fn lookups_per_second(manager: &Arc<DiskCacheManager>) -> f64 {
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(TASKS);
    for task in 0..TASKS {
        let manager = Arc::clone(manager);
        tasks.push(tokio::spawn(async move {
            let key = format!("lookup-bucket/object-{:04}", task % KEYS);
            for _ in 0..LOOKUPS {
                let found = manager
                    .find_pending_journal_ranges(&key, 0, 99)
                    .await
                    .unwrap();
                assert!(!found.is_empty());
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    (TASKS * LOOKUPS) as f64 / started.elapsed().as_secs_f64()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn pending_journal_lookups_per_second() {
    for journal_target in [1024 * 1024, 32 * 1024 * 1024] {
        let (manager, journal_bytes, _dir) = manager_over_journal(journal_target).await;
        let manager = Arc::new(manager);
        lookups_per_second(&manager).await; // untimed warm-up
        let mut runs = Vec::new();
        for _ in 0..ROUNDS {
            runs.push(lookups_per_second(&manager).await);
        }
        println!(
            "LOOKUPS journal={}KiB median={:.0}/s runs={runs:.0?}",
            journal_bytes >> 10,
            median(runs.clone())
        );
    }
}
