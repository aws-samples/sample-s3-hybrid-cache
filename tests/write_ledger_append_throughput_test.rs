//! Local throughput check for the Write_Ledger appender: N concurrent `append` callers over a
//! temp directory on local disk.
//!
//! Report only, not an acceptance: the acceptance measurement runs on shared storage under fleet
//! load. Ignored by default because it measures rather than asserts; run it with
//! `cargo test --release --test write_ledger_append_throughput_test -- --ignored --nocapture`.
use s3_proxy::write_ledger::{WriteLedger, WriteLedgerEntry};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tempfile::TempDir;

const CALLERS: usize = 64;
const APPENDS_PER_CALLER: usize = 32;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn concurrent_appends_per_second_on_local_disk() {
    let tmp = TempDir::new().unwrap();
    let ledger = Arc::new(WriteLedger::new(
        tmp.path().to_path_buf(),
        "bench:1".to_string(),
    ));
    // One warm-up append so seeding and the first mark write are outside the timed window.
    ledger
        .append(entry("bench/warmup.bin"))
        .await
        .expect("warm-up append");

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(CALLERS);
    for caller in 0..CALLERS {
        let ledger = Arc::clone(&ledger);
        tasks.push(tokio::spawn(async move {
            for i in 0..APPENDS_PER_CALLER {
                ledger
                    .append(entry(&format!("bench/obj-{caller}-{i}.bin")))
                    .await
                    .expect("append");
            }
        }));
    }
    for task in tasks {
        task.await.expect("caller task");
    }
    let elapsed = started.elapsed();
    let total = CALLERS * APPENDS_PER_CALLER;
    println!(
        "THROUGHPUT callers={} appends={} elapsed_ms={} appends_per_s={:.0}",
        CALLERS,
        total,
        elapsed.as_millis(),
        total as f64 / elapsed.as_secs_f64()
    );
}

fn entry(key: &str) -> WriteLedgerEntry {
    WriteLedgerEntry {
        timestamp: SystemTime::now(),
        cache_key: key.to_string(),
        range_start: 0,
        range_end: 1023,
        compressed_size: 1024,
        instance_id: "bench:1".to_string(),
        seq: None,
    }
}
