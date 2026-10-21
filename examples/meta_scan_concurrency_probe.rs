//! Measure the achievable `.meta` read-and-parse rate as a function of outstanding requests,
//! with concurrency decoupled from the core count.
//!
//! # Why concurrency, not cores
//!
//! On NFS, per-request round-trip latency dominates the wall clock, so the gap between one
//! reader and N readers measures NFS concurrency rather than per-object CPU cost. Rayon couples
//! concurrency to the thread count, so on an 8-vCPU host it can keep only about 8 reads
//! outstanding. These are small reads against a network file system, where the limiting
//! resource is outstanding requests rather than CPU. `buffer_unordered(N)` decouples the two:
//! 256 reads in flight on 8 cores is achievable, because each one waits on a round trip rather
//! than occupying a core.
//!
//! # The measurement guards against three confounds
//!
//! 1. Page-cache warming across levels. Each concurrency level reads a disjoint set of files.
//!    Re-reading one set at rising concurrency would measure the local page cache at every
//!    level after the first and produce a rising curve with no NFS in it. Files are assigned to
//!    levels by stride (`i % levels`) rather than by block, so every level draws from the same
//!    spread of directories and no level gets a denser or shallower part of the tree.
//! 2. Doing less work than the real scan. The per-file body is
//!    `serde_json::from_slice::<NewCacheMetadata>` plus the shared staged predicate, the same
//!    typed parse `scan_metadata_file` performs, not a `serde_json::Value` or a byte count, so
//!    the figure is comparable with the proxy's own scan.
//! 3. Unbounded enumeration. There is no `find` and no `WalkDir`. Enumeration is bounded by
//!    `--max-l1-shards` and `--max-l2-per-shard` and walks constructed paths only, because a
//!    recursive walk of a live multi-million-file cache over NFS blocks for tens of minutes and
//!    returns nothing.
//!
//! # It is read-only under `--cache-dir`
//!
//! Nothing here writes, renames or deletes anything under `--cache-dir`. The optional
//! `--latency-probe-dir` measures the fixed cost per visit by performing the shape of the claim
//! protocol (read, temporary write, `fsync`, rename), and it must be given a path outside the
//! cache tree. The probe refuses a path inside one.
//!
//! # Usage
//!
//! ```text
//! cargo run --release --example meta_scan_concurrency_probe -- \
//!     --cache-dir /mnt/efs/cache-bench \
//!     --concurrency 1,4,8,16,32,64,128,256 \
//!     --files-per-level 2000 \
//!     --rayon-control \
//!     --latency-probe-dir /mnt/efs/probe-scratch
//! ```

use clap::Parser;
use futures::stream::{self, StreamExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "meta_scan_concurrency_probe",
    about = "Measure .meta read+parse rate as a function of outstanding requests"
)]
struct Args {
    /// Cache directory. MUST be the one the running proxy is configured with — confirm it
    /// from the deployed config.yaml, because a stale sibling tree reads plausibly.
    #[arg(long)]
    cache_dir: PathBuf,

    /// Comma-separated outstanding-request counts to sweep.
    #[arg(long, default_value = "1,4,8,16,32,64,128,256")]
    concurrency: String,

    /// Files each level reads. Every level gets its own disjoint set, so the sample needs
    /// `levels * this` files (plus one more set when `--rayon-control` is given).
    #[arg(long, default_value_t = 2000)]
    files_per_level: usize,

    /// Cap on L1 shard directories enumerated. Bounds the readdir cost.
    #[arg(long, default_value_t = 64)]
    max_l1_shards: usize,

    /// Cap on L2 directories enumerated per L1 shard.
    #[arg(long, default_value_t = 512)]
    max_l2_per_shard: usize,

    /// Also run a rayon `par_iter` pass on its own disjoint set, at rayon's default thread
    /// count, as a like-for-like control against the production scan's shape.
    #[arg(long, default_value_t = false)]
    rayon_control: bool,

    /// Directory OUTSIDE the cache tree in which to measure the per-visit fixed cost
    /// `c_r`. Omit to skip that measurement.
    #[arg(long)]
    latency_probe_dir: Option<PathBuf>,

    /// Iterations for the `c_r` claim-protocol measurement.
    #[arg(long, default_value_t = 50)]
    latency_probe_iterations: usize,

    /// Skip this many L1 shard directories before collecting. Lets a second run target a
    /// part of the tree an earlier run did not touch, so its files are page-cache cold.
    #[arg(long, default_value_t = 0)]
    skip_l1_shards: usize,

    /// Unix seconds at which to begin the sweep, as a cross-instance barrier.
    ///
    /// Enumeration takes ~90 s and the sweep can take under a second, so two instances
    /// started together do not overlap by default — and without overlap a
    /// per-client-versus-fleet-wide question cannot be answered. Enumeration completes,
    /// then the probe waits for this instant, so every participant's sweep runs in the
    /// same window.
    #[arg(long)]
    start_sweep_at: Option<u64>,
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// One concurrency level's result.
struct LevelResult {
    concurrency: usize,
    files: usize,
    elapsed: Duration,
    objects_per_s: f64,
    parse_errors: u64,
    bytes_read: u64,
    staged_objects: u64,
    /// Per-file wall-clock read+parse latencies, for the round-trip figures.
    latencies: Vec<Duration>,
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn mean(ds: &[Duration]) -> Duration {
    if ds.is_empty() {
        return Duration::ZERO;
    }
    let total: Duration = ds.iter().sum();
    total / ds.len() as u32
}

/// Read and parse one `.meta`, doing the same typed work the production scan does.
///
/// Returns `(bytes, parse_ok, staged)`. A parse failure is counted, never acted on — this
/// probe never removes a file, unlike `scan_metadata_file`'s self-heal.
fn read_and_parse(path: &Path) -> (u64, bool, bool) {
    match std::fs::read(path) {
        Ok(content) => {
            let len = content.len() as u64;
            match serde_json::from_slice::<s3_proxy::cache_types::NewCacheMetadata>(&content) {
                Ok(m) => {
                    // The same per-range staged predicate the fold, the repair sweep and
                    // eviction all share, so this probe's notion of work matches theirs.
                    let staged = m.object_metadata.is_write_cached
                        && m.ranges.iter().any(|r| {
                            s3_proxy::cache_types::is_staged_range_spec(
                                r,
                                m.object_metadata.is_write_cached,
                            )
                        });
                    (len, true, staged)
                }
                Err(_) => (len, false, false),
            }
        }
        Err(_) => (0, false, false),
    }
}

/// Enumerate a bounded sample of `.meta` paths, timing every readdir so `d` is measured
/// rather than estimated.
///
/// Returns `(paths, l1_readdir_latencies, l2_readdir_latencies, l2_dirs_seen)`.
#[allow(clippy::type_complexity)]
fn enumerate_bounded(
    metadata_dir: &Path,
    max_l1: usize,
    max_l2_per_shard: usize,
    want_files: usize,
    skip_l1: usize,
) -> std::io::Result<(Vec<PathBuf>, Vec<Duration>, Vec<Duration>, usize)> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut l1_lat: Vec<Duration> = Vec::new();
    let mut l2_lat: Vec<Duration> = Vec::new();
    let mut l2_seen = 0usize;

    // Buckets. Bounded by however many buckets the cache holds, which is small.
    let mut buckets: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(metadata_dir)?.flatten() {
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        // `_journals`, `_write_ledger`, `_shard_state` are internal, same skip the
        // production scan applies.
        if p.file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with('_'))
        {
            continue;
        }
        buckets.push(p);
    }
    buckets.sort();

    // L1 shards, round-robin across buckets so a single large bucket does not consume the
    // whole L1 budget.
    let mut l1_dirs: Vec<PathBuf> = Vec::new();
    for bucket in &buckets {
        let start = Instant::now();
        let entries = std::fs::read_dir(bucket);
        l1_lat.push(start.elapsed());
        if let Ok(entries) = entries {
            let mut here: Vec<PathBuf> = entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.is_dir())
                .collect();
            here.sort();
            l1_dirs.extend(here);
        }
    }
    if skip_l1 < l1_dirs.len() {
        l1_dirs.drain(..skip_l1);
    } else {
        l1_dirs.clear();
    }
    l1_dirs.truncate(max_l1);

    'outer: for l1 in &l1_dirs {
        let start = Instant::now();
        let entries = std::fs::read_dir(l1);
        l1_lat.push(start.elapsed());
        let mut l2_dirs: Vec<PathBuf> = match entries {
            Ok(e) => e
                .flatten()
                .map(|x| x.path())
                .filter(|p| p.is_dir())
                .collect(),
            Err(_) => continue,
        };
        l2_dirs.sort();
        l2_dirs.truncate(max_l2_per_shard);

        for l2 in &l2_dirs {
            let start = Instant::now();
            let entries = std::fs::read_dir(l2);
            let d = start.elapsed();
            l2_lat.push(d);
            l2_seen += 1;
            if let Ok(entries) = entries {
                for e in entries.flatten() {
                    let p = e.path();
                    if p.file_name()
                        .is_some_and(|n| n.to_string_lossy().ends_with(".meta"))
                    {
                        paths.push(p);
                        if paths.len() >= want_files {
                            break 'outer;
                        }
                    }
                }
            }
        }
    }

    Ok((paths, l1_lat, l2_lat, l2_seen))
}

/// Run one concurrency level over its own disjoint slice.
async fn run_level(concurrency: usize, paths: Vec<PathBuf>) -> LevelResult {
    let files = paths.len();
    let start = Instant::now();
    let results: Vec<(Duration, u64, bool, bool)> = stream::iter(paths)
        .map(|path| async move {
            let t = Instant::now();
            // `spawn_blocking` per file is what gives concurrency independent of core
            // count: each task parks on the NFS round trip rather than occupying a core.
            let out = tokio::task::spawn_blocking(move || read_and_parse(&path))
                .await
                .unwrap_or((0, false, false));
            (t.elapsed(), out.0, out.1, out.2)
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let elapsed = start.elapsed();

    let mut latencies: Vec<Duration> = Vec::with_capacity(results.len());
    let mut bytes_read = 0u64;
    let mut parse_errors = 0u64;
    let mut staged_objects = 0u64;
    for (lat, bytes, ok, staged) in results {
        latencies.push(lat);
        bytes_read += bytes;
        if !ok {
            parse_errors += 1;
        }
        if staged {
            staged_objects += 1;
        }
    }
    latencies.sort();

    LevelResult {
        concurrency,
        files,
        elapsed,
        objects_per_s: files as f64 / elapsed.as_secs_f64().max(1e-9),
        parse_errors,
        bytes_read,
        staged_objects,
        latencies,
    }
}

/// The rayon control: the production scan's shape, at rayon's default thread count.
fn run_rayon_control(paths: &[PathBuf]) -> (Duration, f64, u64, u64) {
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    let errors = AtomicU64::new(0);
    let staged = AtomicU64::new(0);
    let start = Instant::now();
    paths.par_iter().for_each(|p| {
        let (_, ok, st) = read_and_parse(p);
        if !ok {
            errors.fetch_add(1, Ordering::Relaxed);
        }
        if st {
            staged.fetch_add(1, Ordering::Relaxed);
        }
    });
    let elapsed = start.elapsed();
    (
        elapsed,
        paths.len() as f64 / elapsed.as_secs_f64().max(1e-9),
        errors.load(Ordering::Relaxed),
        staged.load(Ordering::Relaxed),
    )
}

/// Measure the per-visit fixed cost `c_r`: the claim protocol's shape, outside the cache.
///
/// `claim_repair_shard` does read → `create_dir_all` → temp create → write → `sync_all` →
/// rename → re-read. This performs the same sequence so § 16.6.4's 10 ms estimate can be
/// replaced with a figure.
fn measure_visit_fixed_cost(dir: &Path, iterations: usize) -> std::io::Result<Vec<Duration>> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let body = br#"{"schema":1,"bucket":"probe","l1":"ab","claimed_at":0,"lease_secs":1800}"#;
    let mut out = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let target = dir.join(format!("probe-claim-{i}.json"));
        let tmp = dir.join(format!("probe-claim-{i}.json.tmp"));
        let start = Instant::now();
        // Read the existing document (absent on the first pass, present after).
        let _ = std::fs::read_to_string(&target);
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &target)?;
        let _ = std::fs::read_to_string(&target);
        out.push(start.elapsed());
    }
    // Tidy up after ourselves. This is the probe's own scratch directory, never the cache.
    for i in 0..iterations {
        let _ = std::fs::remove_file(dir.join(format!("probe-claim-{i}.json")));
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let levels: Vec<usize> = args
        .concurrency
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .collect();
    if levels.is_empty() {
        return Err("no valid concurrency levels given".into());
    }

    let metadata_dir = args.cache_dir.join("metadata");
    if !metadata_dir.is_dir() {
        return Err(format!(
            "{:?} is not a directory — is --cache-dir the tree the running proxy uses?",
            metadata_dir
        )
        .into());
    }

    // Refuse a scratch directory inside the cache tree. The probe's whole licence to run
    // against a live shared cache is that it does not write to it.
    if let Some(scratch) = &args.latency_probe_dir {
        let cache_abs = args
            .cache_dir
            .canonicalize()
            .unwrap_or(args.cache_dir.clone());
        let scratch_abs = scratch
            .canonicalize()
            .unwrap_or_else(|_| scratch.to_path_buf());
        if scratch_abs.starts_with(&cache_abs) {
            return Err(format!(
                "--latency-probe-dir {:?} is inside --cache-dir {:?}. This probe must not write \
                 under the cache tree.",
                scratch_abs, cache_abs
            )
            .into());
        }
    }

    let sets = levels.len() + usize::from(args.rayon_control);
    let want = args.files_per_level * sets;

    println!("=== meta_scan_concurrency_probe ===");
    println!("cache_dir        : {:?}", args.cache_dir);
    println!("concurrency sweep: {:?}", levels);
    println!("files per level  : {}", args.files_per_level);
    println!(
        "disjoint sets    : {} (levels{})",
        sets,
        if args.rayon_control {
            " + rayon control"
        } else {
            ""
        }
    );
    println!("target sample    : {} files", want);
    println!("rayon threads    : {}", rayon::current_num_threads());
    println!(
        "available_parallelism: {}",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );
    println!();

    // ---- Enumeration, bounded, with readdir timed so `d` is measured ----
    let enum_start = Instant::now();
    let (paths, l1_lat, mut l2_lat, l2_seen) = enumerate_bounded(
        &metadata_dir,
        args.max_l1_shards,
        args.max_l2_per_shard,
        want,
        args.skip_l1_shards,
    )?;
    let enum_elapsed = enum_start.elapsed();
    l2_lat.sort();

    println!("--- enumeration (bounded, no recursive walk) ---");
    println!("elapsed            : {:.3} s", enum_elapsed.as_secs_f64());
    println!("l1 readdirs        : {}", l1_lat.len());
    println!(
        "l2 readdirs        : {} (dirs seen {})",
        l2_lat.len(),
        l2_seen
    );
    println!("paths collected    : {}", paths.len());
    if !l2_lat.is_empty() {
        println!(
            "L2 readdir latency : mean {:.3} ms  p50 {:.3} ms  p95 {:.3} ms  p99 {:.3} ms  max {:.3} ms",
            mean(&l2_lat).as_secs_f64() * 1000.0,
            percentile(&l2_lat, 0.50).as_secs_f64() * 1000.0,
            percentile(&l2_lat, 0.95).as_secs_f64() * 1000.0,
            percentile(&l2_lat, 0.99).as_secs_f64() * 1000.0,
            l2_lat.last().copied().unwrap_or_default().as_secs_f64() * 1000.0,
        );
        println!(
            "  ^ this is `d` in § 16.6.4's model, which estimates it at 2.000 ms (unmeasured)"
        );
    }
    println!();

    if paths.len() < sets {
        return Err(format!(
            "collected only {} paths, need at least {} (one per set). Raise --max-l1-shards \
             or --max-l2-per-shard.",
            paths.len(),
            sets
        )
        .into());
    }

    // ---- Partition by STRIDE into disjoint sets ----
    //
    // Stride, not block: every set then draws from the same spread of L2 directories, so a
    // level cannot be advantaged by landing on a denser or shallower part of the tree. And
    // the sets are disjoint, so no level reads a file a previous level has already pulled
    // into the local page cache.
    let mut assigned: Vec<Vec<PathBuf>> = vec![Vec::new(); sets];
    for (i, p) in paths.iter().enumerate() {
        assigned[i % sets].push(p.clone());
    }
    let per_set = assigned.iter().map(|v| v.len()).min().unwrap_or(0);
    for set in assigned.iter_mut() {
        set.truncate(per_set);
    }
    println!("--- disjoint sets: {} files each ---", per_set);
    if per_set < args.files_per_level {
        println!(
            "NOTE: {} files per set, short of the requested {} — the sample was smaller than \
             asked for. The rate figures are still valid; the sample is just smaller.",
            per_set, args.files_per_level
        );
    }
    println!();

    // ---- Cross-instance barrier ----
    //
    // Without this, two instances launched together do not overlap: enumeration is ~90 s
    // and the sweep can be under a second, so the two sweeps miss each other entirely and
    // the run answers nothing about shared-versus-per-client capacity.
    if let Some(at) = args.start_sweep_at {
        let now = unix_now();
        if at > now {
            println!(
                "--- barrier: enumeration done at {}, waiting {} s until {} ---",
                now,
                at - now,
                at
            );
            tokio::time::sleep(Duration::from_secs(at - now)).await;
        } else {
            println!(
                "--- barrier MISSED: enumeration finished at {}, barrier was {} ({} s late). \
                 A cross-instance comparison from this run is NOT valid. ---",
                now,
                at,
                now - at
            );
        }
        println!("sweep starts at {}", unix_now());
    }

    // ---- The sweep ----
    println!("--- read+parse rate vs outstanding requests ---");
    println!(
        "{:>6}  {:>7}  {:>9}  {:>11}  {:>10}  {:>10}  {:>10}  {:>7}",
        "conc", "files", "secs", "obj/s", "mean ms", "p50 ms", "p99 ms", "errs"
    );
    let mut results: Vec<LevelResult> = Vec::new();
    for (idx, &conc) in levels.iter().enumerate() {
        let r = run_level(conc, assigned[idx].clone()).await;
        println!(
            "{:>6}  {:>7}  {:>9.3}  {:>11.1}  {:>10.3}  {:>10.3}  {:>10.3}  {:>7}",
            r.concurrency,
            r.files,
            r.elapsed.as_secs_f64(),
            r.objects_per_s,
            mean(&r.latencies).as_secs_f64() * 1000.0,
            percentile(&r.latencies, 0.50).as_secs_f64() * 1000.0,
            percentile(&r.latencies, 0.99).as_secs_f64() * 1000.0,
            r.parse_errors,
        );
        results.push(r);
    }
    println!();

    // ---- The rayon control ----
    if args.rayon_control {
        let set = &assigned[levels.len()];
        let (elapsed, rate, errors, staged) = run_rayon_control(set);
        println!("--- rayon control (production scan's shape) ---");
        println!(
            "threads {}  files {}  {:.3} s  {:.1} obj/s  errs {}  staged {}",
            rayon::current_num_threads(),
            set.len(),
            elapsed.as_secs_f64(),
            rate,
            errors,
            staged
        );
        if let Some(best) = results.iter().max_by(|a, b| {
            a.objects_per_s
                .partial_cmp(&b.objects_per_s)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            println!(
                "async best (conc {}) is {:.2}x the rayon control",
                best.concurrency,
                best.objects_per_s / rate.max(1e-9)
            );
        }
        println!();
    }

    // ---- Where does it stop improving ----
    println!("--- knee analysis ---");
    let mut prev: Option<&LevelResult> = None;
    for r in &results {
        if let Some(p) = prev {
            let ratio = r.objects_per_s / p.objects_per_s.max(1e-9);
            let conc_ratio = r.concurrency as f64 / p.concurrency as f64;
            println!(
                "{:>4} -> {:>4}: concurrency x{:.1}, rate x{:.2} ({:.0}% of linear)",
                p.concurrency,
                r.concurrency,
                conc_ratio,
                ratio,
                100.0 * (ratio - 1.0) / (conc_ratio - 1.0).max(1e-9)
            );
        }
        prev = Some(r);
    }
    println!();

    // ---- Aggregate facts a reader needs to judge the figures ----
    let total_bytes: u64 = results.iter().map(|r| r.bytes_read).sum();
    let total_files: usize = results.iter().map(|r| r.files).sum();
    let total_staged: u64 = results.iter().map(|r| r.staged_objects).sum();
    println!("--- sample properties ---");
    println!("files read (sweep) : {}", total_files);
    println!(
        "mean .meta size    : {} bytes",
        if total_files > 0 {
            total_bytes / total_files as u64
        } else {
            0
        }
    );
    println!(
        "staged objects     : {} of {} ({:.3}%)",
        total_staged,
        total_files,
        100.0 * total_staged as f64 / (total_files as f64).max(1.0)
    );
    println!();

    // ---- `c_r`, the per-visit fixed cost ----
    if let Some(scratch) = &args.latency_probe_dir {
        match measure_visit_fixed_cost(scratch, args.latency_probe_iterations) {
            Ok(mut lats) => {
                lats.sort();
                println!(
                    "--- per-visit fixed cost `c_r` (claim protocol shape, OUTSIDE the cache) ---"
                );
                println!("scratch dir  : {:?}", scratch);
                println!("iterations   : {}", lats.len());
                println!(
                    "read+write+fsync+rename+read: mean {:.3} ms  p50 {:.3} ms  p99 {:.3} ms  max {:.3} ms",
                    mean(&lats).as_secs_f64() * 1000.0,
                    percentile(&lats, 0.50).as_secs_f64() * 1000.0,
                    percentile(&lats, 0.99).as_secs_f64() * 1000.0,
                    lats.last().copied().unwrap_or_default().as_secs_f64() * 1000.0,
                );
                println!(
                    "  ^ this is `c_r` in § 16.6.4's model, which estimates it at 10.000 ms \
                     (unmeasured)"
                );
                println!();
            }
            Err(e) => println!("c_r probe failed: {e}\n"),
        }
    }

    println!("=== done. READ ONLY under the cache tree: no writes, renames or deletes. ===");
    Ok(())
}
