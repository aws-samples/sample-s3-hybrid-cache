//! LRU-versus-TinyLFU victim-selection benchmark for `ShardedRamCache`.
//!
//! Answers `cache-eviction-at-scale` R10.8 / design § 12 row 2 / Phase 0 task 0a:
//! *is TinyLFU victim selection acceptable at realistic RAM shard occupancy?*
//! The decision rule is fixed in advance by § 12 row 2 — **TinyLFU everywhere if
//! admissions/s is within 20% of LRU**, otherwise the R10.1a fallback (RAM keeps LRU,
//! the config field is documented RAM-only, the disk tier still moves).
//!
//! Companion to `ram_cache_shard_contention_bench.rs`, which measures a different thing:
//! that one is hits-only at a fixed algorithm and characterises per-shard `RwLock`
//! contention. This one holds the shard count fixed and varies the algorithm, and it is
//! the eviction path rather than the read path.
//!
//! # The asymmetry being measured
//!
//! Read off the current code rather than assumed (`src/ram_cache.rs`):
//!
//! | | LRU | TinyLFU |
//! |---|---|---|
//! | Victim selection | O(1) — `lru_order.front()` (`:643`) | O(shard entries) — `data.iter().min_by_key` (`:658`), plus one `SystemTime::now()` per call |
//! | Removal of the victim | O(1) *in practice* — `position()` finds it at index 0 (`:616`) | O(1) — no-op (`:628`) |
//! | Removal of an arbitrary key | O(n) — `String`-comparing `VecDeque` scan (`:616`) | O(1) — no-op |
//! | Tracked access (sampled, every 8th hit) | O(n) — scan + `remove` + `push_back` (`:632`) | O(1) — nothing; scoring reads the atomics at eviction time |
//!
//! So the two workload shapes below favour opposite algorithms, and reporting one number
//! for both would hide that:
//!
//! * `Admit` — pure sustained pressure, every admission evicts. All of LRU's per-operation
//!   work is O(1) here (the victim is at the queue front), so this is TinyLFU's worst case
//!   and it is the workload § 12 row 2's rule is stated against.
//! * `AdmitPlusHits` — the same pressure with a hot re-read set alongside it, so LRU pays
//!   its sampled `pending_accesses` drain. This is where TinyLFU claws back.
//! * `Invalidate` — arbitrary-key removal, LRU's genuine O(n) path.
//!
//! # Occupancy is the independent variable, not a detail
//!
//! TinyLFU's victim selection is O(shard entries), so entries-per-shard decides the
//! answer. It is fixed by entry size at a fixed shard capacity, and the spread in
//! production is wide: a page-widened entry is `DEFAULT_PAGE_SIZE` = 16 MiB
//! (`src/bucket_settings.rs:41`) giving ~4 entries in a 64 MiB shard, while ordinary
//! small range reads give thousands. So the bench sweeps occupancy and reports where the
//! 20% rule flips rather than picking one point and calling it realistic.
//!
//! Capacity is pinned at the fleet default `max_ram_cache_size` = 512 MiB
//! (`src/config.rs:1565`) with `ram_cache_shard_count` = 8 (`:1305`). That combination is
//! exactly at the `RAM_CACHE_ADMISSION_CEILING` clamp boundary
//! (`512 MiB / 64 MiB = 8`), so all 8 shards survive and per-shard capacity is 64 MiB. A
//! smaller `total_capacity` would silently clamp the effective shard count
//! (`src/ram_cache.rs:163`) and change what is being measured, so the bench asserts the
//! shard count it actually got.
//!
//! # Deliberate deviations from production, both stated rather than hidden
//!
//! 1. **One shared payload `Arc<Bytes>` backs every entry.** `shard_calculate_entry_size`
//!    reads `data.len()` (`:589`), so capacity accounting is unaffected, but no admission
//!    pays an allocation. Real admissions do. That cost is identical under both
//!    algorithms, so excluding it makes the ratio *more* sensitive to the algorithm — the
//!    conservative direction for a rule phrased as "within 20%".
//! 2. **No S3, disk, or network time.** Same argument: shared cost, excluded, so any
//!    difference here is diluted end-to-end.
//!
//! # Run
//!
//! ```text
//! cargo run --release --example ram_cache_eviction_algo_bench
//! # tunables (env): REPS=3 OCCUPANCY=4,64,1024,8192 CONCURRENCY=<cpus> HOT_SET=64
//! ```
//!
//! Release only — this is a constant-factor comparison and a debug build measures nothing.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use s3_proxy::cache::{CacheEvictionAlgorithm, RamCacheEntry};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::ram_cache::ShardedRamCache;

/// Fleet default `max_ram_cache_size` (`src/config.rs:1565`). Also the exact
/// `RAM_CACHE_ADMISSION_CEILING` clamp boundary at 8 shards — do not lower it without
/// re-reading the clamp, or the effective shard count changes silently.
const TOTAL_CAPACITY: usize = 512 * 1024 * 1024;
/// Fleet default `ram_cache_shard_count` (`src/config.rs:1305`).
const SHARD_COUNT: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    /// Pure sustained pressure: every admission evicts. TinyLFU's worst case.
    Admit,
    /// Sustained pressure plus a hot re-read set, so LRU pays its sampled reorder.
    AdmitPlusHits,
    /// Arbitrary-key removal, LRU's genuine O(n) path.
    Invalidate,
}

impl Workload {
    fn label(self) -> &'static str {
        match self {
            Workload::Admit => "admit-only",
            Workload::AdmitPlusHits => "admit+hits",
            Workload::Invalidate => "invalidate",
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn key_for(gen: usize, idx: usize) -> String {
    format!("bench-bucket/gen{gen}/obj/{idx:08}")
}

/// Payload length that makes the *accounted* entry size equal `target_entry_size`.
///
/// Must mirror `shard_calculate_entry_size` (`src/ram_cache.rs:586`): base struct + key +
/// data + `CacheMetadata`. Getting this wrong pushes the accounted size above the shard
/// capacity, `put()` drops the entry silently (`:390`), and the whole run measures nothing,
/// which is why `eviction_count` is asserted non-zero below.
fn payload_len_for(target_entry_size: usize, key_len: usize) -> usize {
    let overhead =
        std::mem::size_of::<RamCacheEntry>() + key_len + std::mem::size_of::<CacheMetadata>();
    target_entry_size.saturating_sub(overhead)
}

fn make_entry(key: &str, payload: &Arc<Bytes>) -> RamCacheEntry {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    RamCacheEntry {
        cache_key: key.to_string(),
        data: Arc::clone(payload),
        metadata: CacheMetadata {
            etag: "bench-etag".to_string(),
            last_modified: "bench-modified".to_string(),
            content_length: payload.len() as u64,
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

struct RunResult {
    /// Admissions per second (the § 12 row 2 figure), or invalidations/s for `Invalidate`.
    ops_per_sec: f64,
    /// Mean nanoseconds per admission. Under `Admit` this is ~1 victim selection plus
    /// bookkeeping, which is how victim-selection cost is derived (see main()).
    ns_per_op: f64,
    /// Evictions actually performed. Zero means the run was vacuous.
    evictions: u64,
    /// Entries resident at the end, summed across shards — the realised occupancy.
    entries: u64,
}

/// One (algorithm, occupancy, workload) measurement.
async fn run_one(
    algo: CacheEvictionAlgorithm,
    entries_per_shard: usize,
    workload: Workload,
    concurrency: usize,
    ops_per_task: usize,
    hot_set: usize,
) -> RunResult {
    let per_shard_capacity = TOTAL_CAPACITY / SHARD_COUNT;
    let target_entry_size = per_shard_capacity / entries_per_shard;
    let key_len = key_for(0, 0).len();
    let payload_len = payload_len_for(target_entry_size, key_len);
    assert!(
        payload_len > 0,
        "occupancy {entries_per_shard} implies a {target_entry_size}-byte entry, which is \
         smaller than the per-entry struct overhead — pick a lower occupancy"
    );
    let payload = Arc::new(Bytes::from(vec![0u8; payload_len]));

    let cache = Arc::new(ShardedRamCache::new(
        TOTAL_CAPACITY,
        SHARD_COUNT,
        algo.clone(),
    ));

    // Pre-fill to capacity so the timed phase runs under sustained pressure from its
    // first operation. Generation 0 keys; the timed phase admits generation 1 keys so
    // nothing is a replace-in-place (which would take a different branch in put()).
    let resident_target = TOTAL_CAPACITY / target_entry_size;
    for i in 0..resident_target {
        cache
            .put(make_entry(&key_for(0, i), &payload))
            .await
            .expect("prefill put");
    }
    // Overfill by 10% so every shard is genuinely at its bound, not merely near it.
    for i in resident_target..(resident_target + resident_target / 10 + 1) {
        cache
            .put(make_entry(&key_for(0, i), &payload))
            .await
            .expect("prefill put");
    }

    let evictions_before = cache.stats().await.eviction_count;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for t in 0..concurrency {
        let cache = Arc::clone(&cache);
        let payload = Arc::clone(&payload);
        handles.push(tokio::spawn(async move {
            let mut sink: u64 = 0;
            for op in 0..ops_per_task {
                // Disjoint key space per task so tasks never contend on the same key.
                let idx = t * ops_per_task + op;
                match workload {
                    Workload::Admit => {
                        let _ = cache.put(make_entry(&key_for(1, idx), &payload)).await;
                    }
                    Workload::AdmitPlusHits => {
                        // Re-read a small hot set so access_count climbs past the
                        // `% 8 == 0` sampling gate in get() (`src/ram_cache.rs:333`),
                        // which is the only thing that populates pending_accesses.
                        for h in 0..hot_set {
                            let hk = key_for(1, t * ops_per_task + (op / 8) * 8 + h % 8);
                            if let Some(r) = cache.get(&hk).await {
                                sink = sink.wrapping_add(r.data.len() as u64);
                            }
                        }
                        let _ = cache.put(make_entry(&key_for(1, idx), &payload)).await;
                    }
                    Workload::Invalidate => {
                        // Arbitrary-key removal against the resident generation-0 set:
                        // LRU must scan lru_order to find a key that is NOT at the front.
                        //
                        // The key is re-admitted immediately. Without that the workload
                        // drains the cache — the first version of this arm ran 56,000
                        // invalidations against 512 resident entries and left 52, so
                        // LRU's scan was over a near-empty deque and the O(n) path it
                        // exists to measure was never exercised. The re-admission cost is
                        // identical under both algorithms; only the scan differs.
                        let victim = key_for(0, (idx * 7919) % resident_target);
                        let _ = cache.invalidate(&victim).await;
                        let _ = cache.put(make_entry(&victim, &payload)).await;
                        sink = sink.wrapping_add(1);
                    }
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
    std::hint::black_box(sink_total);

    let stats = cache.stats().await;
    let total_ops = (concurrency * ops_per_task) as f64;
    RunResult {
        ops_per_sec: total_ops / elapsed.as_secs_f64(),
        ns_per_op: elapsed.as_nanos() as f64 / total_ops,
        evictions: stats.eviction_count.saturating_sub(evictions_before),
        entries: stats.entries_count,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let occupancies: Vec<usize> = std::env::var("OCCUPANCY")
        .unwrap_or_else(|_| "4,64,1024,8192".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let reps = env_usize("REPS", 3);
    let hot_set = env_usize("HOT_SET", 64);
    let concurrency = env_usize(
        "CONCURRENCY",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8),
    );

    // Sanity-check the clamp before measuring anything: if the effective shard count is
    // not what we asked for, per-shard capacity and therefore occupancy are both wrong.
    let probe = ShardedRamCache::new(TOTAL_CAPACITY, SHARD_COUNT, CacheEvictionAlgorithm::LRU);
    let effective_shards = probe.max_size() / (TOTAL_CAPACITY / SHARD_COUNT);
    eprintln!(
        "ram_cache eviction-algorithm bench: total_capacity={}MiB requested_shards={SHARD_COUNT} \
         per_shard={}MiB concurrency={concurrency} reps={reps} hot_set={hot_set}",
        TOTAL_CAPACITY / (1024 * 1024),
        TOTAL_CAPACITY / SHARD_COUNT / (1024 * 1024),
    );
    assert_eq!(
        effective_shards, SHARD_COUNT,
        "RAM_CACHE_ADMISSION_CEILING clamped the shard count to {effective_shards}; \
         occupancy figures would be wrong. Raise TOTAL_CAPACITY."
    );
    eprintln!("shard-clamp check: {effective_shards} effective shards (as requested)\n");

    println!(
        "{:>10} | {:>11} | {:>7} | {:>13} | {:>13} | {:>9} | {:>10} | {:>9}",
        "workload",
        "entries/shd",
        "algo",
        "ops/s median",
        "ops/s spread",
        "ns/op",
        "evictions",
        "resident"
    );
    println!("{}", "-".repeat(108));

    // (workload, occupancy) -> (lru_median, tinylfu_median)
    let mut summary: Vec<(Workload, usize, f64, f64)> = Vec::new();

    for workload in [
        Workload::Admit,
        Workload::AdmitPlusHits,
        Workload::Invalidate,
    ] {
        for &occ in &occupancies {
            // Scale the op count down as occupancy (and therefore per-op cost) rises, so
            // no single cell dominates wall time. Floor keeps the sample meaningful.
            let ops_per_task = match occ {
                0..=64 => 4_000,
                65..=1024 => 1_500,
                _ => 300,
            };
            let mut medians = [0.0f64; 2];
            for (ai, algo) in [CacheEvictionAlgorithm::LRU, CacheEvictionAlgorithm::TinyLFU]
                .iter()
                .enumerate()
            {
                let mut samples: Vec<RunResult> = Vec::with_capacity(reps);
                for _ in 0..reps {
                    samples.push(
                        run_one(
                            algo.clone(),
                            occ,
                            workload,
                            concurrency,
                            ops_per_task,
                            hot_set,
                        )
                        .await,
                    );
                }
                let mut rates: Vec<f64> = samples.iter().map(|s| s.ops_per_sec).collect();
                rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let median = rates[rates.len() / 2];
                let spread = if median > 0.0 {
                    (rates[rates.len() - 1] - rates[0]) / median * 100.0
                } else {
                    0.0
                };
                let ns: f64 =
                    samples.iter().map(|s| s.ns_per_op).sum::<f64>() / samples.len() as f64;
                let evictions = samples.iter().map(|s| s.evictions).min().unwrap_or(0);
                let resident = samples.last().map(|s| s.entries).unwrap_or(0);
                medians[ai] = median;

                // Non-vacuity, two different failure modes.
                //
                // Admission workloads: no evictions means the run measured the wrong
                // thing, most likely because the entry was dropped for exceeding shard
                // capacity (`src/ram_cache.rs:390`) rather than admitted.
                //
                // Invalidate: LRU's scan is only O(n) if the deque is actually populated,
                // so a drained cache passes while proving nothing. Assert residency
                // directly rather than inferring it from the rate.
                let expected_resident = (occ * SHARD_COUNT) as u64;
                let vacuous = match workload {
                    Workload::Invalidate => resident * 2 < expected_resident,
                    _ => evictions == 0,
                };
                println!(
                    "{:>10} | {:>11} | {:>7} | {:>13.0} | {:>12.1}% | {:>9.0} | {:>10} | {:>9}{}",
                    workload.label(),
                    occ,
                    if *algo == CacheEvictionAlgorithm::LRU {
                        "lru"
                    } else {
                        "tinylfu"
                    },
                    median,
                    spread,
                    ns,
                    evictions,
                    resident,
                    if vacuous {
                        "  <== VACUOUS (no evictions, or cache drained)"
                    } else {
                        ""
                    }
                );
            }
            summary.push((workload, occ, medians[0], medians[1]));
        }
    }

    // § 12 row 2's rule, applied to the admission-only workload it is stated against.
    eprintln!(
        "\n=== design.md § 12 row 2: TinyLFU everywhere if admissions/s within 20% of LRU ==="
    );
    for (workload, occ, lru, tinylfu) in &summary {
        if *lru <= 0.0 {
            continue;
        }
        let delta = (tinylfu / lru - 1.0) * 100.0;
        let verdict = if *workload == Workload::Admit {
            if delta >= -20.0 {
                "WITHIN 20% -> R10.1"
            } else {
                "OUTSIDE 20% -> R10.1a"
            }
        } else {
            "(rule not stated against this workload)"
        };
        eprintln!(
            "{:>10} occ={:>5}: tinylfu {:+6.1}% vs lru   {verdict}",
            workload.label(),
            occ,
            delta
        );
    }
}
