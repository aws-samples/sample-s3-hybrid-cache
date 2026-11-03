//! Revalidated cache-hit throughput: `TASKS` concurrent clients each make `HITS` range GETs that
//! revalidate with S3 (`get_ttl: 0`, the stub answers `304`), against keys of their own.
//!
//! Two scenarios: the hits alone, and the hits while a background task holds the disk cache
//! manager's read lock for `SCAN_HOLD` out of every `SCAN_PERIOD`, as a pending-journal lookup
//! holds it while it reads the journals. Each prints hits per second.
//!
//! Ignored by default because it measures time; run it alone with
//! `cargo test --release --test revalidated_hit_throughput_test -- --ignored --nocapture`.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::expired_fixture::{body_of, not_modified, test_config, zero_ttl, Fixture, SeedSpec};
use common::StubS3Client;
use s3_proxy::metadata_lock_manager::{
    metadata_lock_acquisitions_failed, metadata_lock_acquisitions_successful,
};

const OBJECT_SIZE: u64 = 4096;
const TASKS: usize = 64;
const HITS: usize = 50;
const ROUNDS: usize = 5;
const SCAN_HOLD: Duration = Duration::from_millis(20);
const SCAN_PERIOD: Duration = Duration::from_millis(25);
const ETAG: &str = "\"throughput-v1\"";

fn key(task: usize) -> String {
    format!("bucket/revalidated-throughput-{task:03}.bin")
}

/// Hits per second of one round of `TASKS` × `HITS` revalidated hits.
async fn round(fixture: &Arc<Fixture>, stub: &StubS3Client) -> f64 {
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(TASKS);
    for task in 0..TASKS {
        let fixture = Arc::clone(fixture);
        let stub = stub.clone();
        tasks.push(tokio::spawn(async move {
            let key = key(task);
            for _ in 0..HITS {
                fixture.invalidate_metadata_cache(&key).await;
                let response = fixture
                    .range_get(
                        &key,
                        "bytes=0-1023",
                        HashMap::new(),
                        &zero_ttl(),
                        fixture.production_current_etag(&key),
                        stub.clone().into_trait_object(),
                    )
                    .await;
                assert_eq!(body_of(response).await.len(), 1024);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    (TASKS * HITS) as f64 / started.elapsed().as_secs_f64()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn revalidated_hits_per_second() {
    let fixture = Arc::new(Fixture::new(test_config(OBJECT_SIZE)).await);
    for task in 0..TASKS {
        fixture.seed(
            &key(task),
            &SeedSpec::expired(vec![(0, 1023)], OBJECT_SIZE, ETAG),
        );
    }
    let stub = StubS3Client::new().with_response_for_etag(ETAG, not_modified(ETAG));
    round(&fixture, &stub).await; // untimed warm-up
    let (ok_before, failed_before) = (
        metadata_lock_acquisitions_successful(),
        metadata_lock_acquisitions_failed(),
    );

    let alone: Vec<f64> = {
        let mut v = Vec::new();
        for _ in 0..ROUNDS {
            v.push(round(&fixture, &stub).await);
        }
        v
    };

    let stop = Arc::new(AtomicBool::new(false));
    let scanner = {
        let lock = Arc::clone(&fixture.disk_cache_manager);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let guard = lock.read().await;
                tokio::time::sleep(SCAN_HOLD).await;
                drop(guard);
                tokio::time::sleep(SCAN_PERIOD - SCAN_HOLD).await;
            }
        })
    };
    let scanned: Vec<f64> = {
        let mut v = Vec::new();
        for _ in 0..ROUNDS {
            v.push(round(&fixture, &stub).await);
        }
        v
    };
    stop.store(true, Ordering::Relaxed);
    scanner.await.unwrap();

    // Every hit's `.meta` write takes its key's slot lock once; a refused attempt skips the
    // write, so a rate bought by skipped writes shows here.
    println!(
        "SLOT_LOCKS hits={} acquired={} refused={}",
        2 * ROUNDS * TASKS * HITS,
        metadata_lock_acquisitions_successful() - ok_before,
        metadata_lock_acquisitions_failed() - failed_before
    );
    println!(
        "REVALIDATED tasks={TASKS} hits={HITS} alone median={:.0}/s runs={alone:.0?} \
         with_reader median={:.0}/s runs={scanned:.0?}",
        median(alone.clone()),
        median(scanned.clone())
    );
}
