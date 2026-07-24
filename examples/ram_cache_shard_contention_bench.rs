//! `ShardedRamCache` shard-count contention microbenchmark.
//!
//! Purpose: characterize how RAM-cache throughput scales with `ram_cache_shard_count`
//! under high concurrent access, to inform the `page-aligned-range-cache` decision to
//! keep the default `max_ram_cache_size` at 256 MiB (which, with the unconditional
//! 64 MiB admission-ceiling shard clamp, yields 4 effective shards instead of 8).
//!
//! This is the CORRECT instrument for that question — the per-shard `RwLock` is the only
//! thing that differs across shard counts, and it is exercised only under high concurrent
//! request rate across many distinct keys. The single-client S3Proxy-Bench rig cannot
//! surface it (one stream, one/few keys), and the network fleet buries it (the lock is
//! held for a hashmap lookup + `Arc<Bytes>` clone, nanoseconds, dwarfed by S3 latency).
//! So we measure it in isolation, in-process, with zero network noise.
//!
//! Consistent with the repo's no-new-dependency posture (`nonpublic/LICENSE_COMPLIANCE.md`),
//! this uses ONLY `std::time::Instant` + `tokio` (already a dependency) — no `criterion`.
//!
//! Workload: pre-populate `KEYS` distinct entries (all subsequent reads are HITS, so we
//! measure the get() lock path, not eviction or S3), then run `CONCURRENCY` tasks each
//! issuing `OPS_PER_TASK` `get()`s over pseudo-random keys, for each shard count in
//! `SHARDS`. Total capacity is held constant and large enough that nothing is evicted.
//!
//! Run in RELEASE (microbenchmarks are meaningless in debug):
//!
//! ```text
//! cargo run --release --example ram_cache_shard_contention_bench
//! # tunables (env): SHARDS=1,4,8,16,32 KEYS=20000 OPS_PER_TASK=500000 CONCURRENCY=<cpus>
//! ```
//!
//! Reading the result: if ops/sec is ~flat from 4→8→16 shards, shard contention is
//! negligible and keeping the 256 MiB / 4-shard default is safe. If throughput climbs
//! materially with more shards, the clamp's 8→4 reduction is a real regression and the
//! default should be bumped to 512 MiB to preserve 8 shards.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use s3_proxy::cache::{CacheEvictionAlgorithm, RamCacheEntry};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::ram_cache::ShardedRamCache;

const TOTAL_CAPACITY: usize = 512 * 1024 * 1024; // constant across shard counts; no eviction
const DATA_SIZE: usize = 256;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn key_for(idx: usize) -> String {
    format!("bench-bucket/obj/{idx:08}")
}

fn make_entry(key: &str) -> RamCacheEntry {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    RamCacheEntry {
        cache_key: key.to_string(),
        data: Arc::new(Bytes::from(vec![0u8; DATA_SIZE])),
        metadata: CacheMetadata {
            etag: "bench-etag".to_string(),
            last_modified: "bench-modified".to_string(),
            content_length: DATA_SIZE as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: SystemTime::now(),
        },
        created_at: SystemTime::now(),
        last_accessed: AtomicU64::new(now_ms),
        access_count: AtomicU64::new(0),
        compressed: false,
        compression_algorithm: CompressionAlgorithm::Lz4,
    }
}

/// One shard-count run: returns aggregate ops/sec and mean ns/op.
async fn run_config(
    shard_count: usize,
    keys: usize,
    concurrency: usize,
    ops_per_task: usize,
) -> (f64, f64) {
    let cache = Arc::new(ShardedRamCache::new(
        TOTAL_CAPACITY,
        shard_count,
        CacheEvictionAlgorithm::TinyLFU,
    ));

    // Pre-populate: every key is a HIT thereafter.
    for i in 0..keys {
        cache.put(make_entry(&key_for(i))).await.expect("put");
    }

    // Warm-up (not timed): touch each shard, let the runtime settle.
    for i in 0..keys.min(4096) {
        let _ = cache.get(&key_for(i)).await;
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for t in 0..concurrency {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            // Deterministic per-task LCG (no rng dependency) over [0, keys).
            let mut state = 0x9E3779B97F4A7C15u64 ^ (t as u64).wrapping_mul(0x1000_0001B3);
            let mut sink: u64 = 0;
            for _ in 0..ops_per_task {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let idx = (state >> 33) as usize % keys;
                if let Some(read) = cache.get(&key_for(idx)).await {
                    sink = sink.wrapping_add(read.data.len() as u64);
                }
            }
            sink
        }));
    }

    let mut sink_total: u64 = 0;
    for h in handles {
        sink_total = sink_total.wrapping_add(h.await.expect("task"));
    }
    let elapsed = start.elapsed();

    // Guard against dead-code elimination of the get() results.
    std::hint::black_box(sink_total);

    let total_ops = (concurrency * ops_per_task) as f64;
    let ops_per_sec = total_ops / elapsed.as_secs_f64();
    let ns_per_op = elapsed.as_nanos() as f64 / total_ops;
    (ops_per_sec, ns_per_op)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let shards: Vec<usize> = std::env::var("SHARDS")
        .unwrap_or_else(|_| "1,4,8,16,32".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let keys = env_usize("KEYS", 20_000);
    let ops_per_task = env_usize("OPS_PER_TASK", 500_000);
    let concurrency = env_usize(
        "CONCURRENCY",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8),
    );

    eprintln!(
        "ram_cache shard-contention bench: keys={keys} concurrency={concurrency} \
         ops_per_task={ops_per_task} total_capacity={}MiB data={DATA_SIZE}B algo=TinyLFU",
        TOTAL_CAPACITY / (1024 * 1024)
    );
    eprintln!("(all reads are cache HITS; measuring per-shard RwLock contention only)\n");

    println!(
        "{:>7} | {:>7} | {:>14} | {:>10} | {:>10}",
        "shards", "threads", "ops/sec", "ns/op", "vs 8-shard"
    );
    println!("{}", "-".repeat(62));

    let mut baseline_8: Option<f64> = None;
    let mut results: Vec<(usize, f64, f64)> = Vec::new();
    for &sc in &shards {
        let (ops_per_sec, ns_per_op) = run_config(sc, keys, concurrency, ops_per_task).await;
        if sc == 8 {
            baseline_8 = Some(ops_per_sec);
        }
        results.push((sc, ops_per_sec, ns_per_op));
    }

    for (sc, ops_per_sec, ns_per_op) in &results {
        let rel = match baseline_8 {
            Some(b) if b > 0.0 => format!("{:+.1}%", (ops_per_sec / b - 1.0) * 100.0),
            _ => "-".to_string(),
        };
        println!(
            "{:>7} | {:>7} | {:>14.0} | {:>10.1} | {:>10}",
            sc, concurrency, ops_per_sec, ns_per_op, rel
        );
    }

    // Headline: the exact 8->4 delta the clamp introduces at the 256 MiB default.
    let ops4 = results.iter().find(|(s, _, _)| *s == 4).map(|(_, o, _)| *o);
    if let (Some(o4), Some(o8)) = (ops4, baseline_8) {
        if o8 > 0.0 {
            let delta = (o4 / o8 - 1.0) * 100.0;
            eprintln!(
                "\n8->4 shard delta (the clamp's effect at 256 MiB default): {delta:+.1}% throughput"
            );
            if delta.abs() < 5.0 {
                eprintln!(
                    "Interpretation: within run-to-run noise => keeping 256 MiB / 4 shards is safe."
                );
            } else {
                eprintln!(
                    "Interpretation: MATERIAL ({delta:+.1}%) => the 8->4 clamp is a real RAM-cache \
                     throughput regression under high contention. Consider bumping the default \
                     max_ram_cache_size to 512 MiB to preserve 8 shards. NOTE: this is an isolated \
                     worst case (pure RAM hits, max concurrency); real end-to-end impact is smaller, \
                     diluted by network/disk/S3 time per request."
                );
            }
        }
    }
}
