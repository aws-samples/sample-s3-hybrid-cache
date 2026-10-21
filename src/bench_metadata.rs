//! `s3-proxy bench-metadata`: measure the shared cache volume's `.meta` read rate.
//!
//! Operators size the RAM metadata tier against `A(I)`, the rate at which the shared volume
//! serves `.meta` reads (see `docs/AWS_DEPLOYMENT.md`, "Sizing the metadata rate"). That rate
//! depends on the backend, its tier and the number of proxies sharing it, so it has to be
//! measured on the operator's own volume. This command measures it.
//!
//! # Read only
//!
//! The command never writes, renames, deletes or locks anything. Every filesystem call it makes
//! goes through the private `list_dir` and `read_meta`, which only list directories and read files, and
//! the unit test `module_source_has_no_mutating_filesystem_calls` fails the build's test run if
//! this file ever names a mutating filesystem API. It does not need a running proxy and does not
//! read or change any proxy state: it skips every `_`-prefixed directory under `metadata/`
//! (journals, write ledger, shard state), exactly as the production scan does.
//!
//! # What it measures, and the confounds it is built against
//!
//! For each concurrency level it reads and parses a set of `.meta` files with that many reads
//! outstanding, and reports objects per second.
//!
//! 1. **Page-cache warming.** Each level reads a disjoint set of files, assigned by stride
//!    (`i % levels`) so every level draws from the same spread of directories. Re-reading one
//!    set would measure the client's page cache after the first level.
//! 2. **Less work than the real scan.** Each file is parsed as the typed
//!    [`NewCacheMetadata`](crate::cache_types::NewCacheMetadata) and run through the staged
//!    predicate, the same work the production scan does per object.
//! 3. **Unbounded enumeration.** There is no recursive walk. Enumeration is capped by
//!    `--max-l1-shards` and `--max-l2-per-shard`, because a recursive walk of a
//!    multi-million-file cache over NFS can block for tens of minutes.
//!
//! Concurrency is decoupled from core count: each read runs on the blocking pool and parks on
//! its round trip, so 256 reads can be outstanding on an 8-vCPU host.

use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The subcommand's name. A compatibility commitment once released.
pub const SUBCOMMAND: &str = "bench-metadata";

/// Default concurrency sweep, the levels the measurement programme used.
pub const DEFAULT_CONCURRENCY: &str = "1,4,8,16,32,64,128,256";
/// Default files read per level.
pub const DEFAULT_FILES_PER_LEVEL: usize = 2000;
/// Default cap on L1 shard directories enumerated.
pub const DEFAULT_MAX_L1_SHARDS: usize = 64;
/// Default cap on L2 directories enumerated per L1 shard.
pub const DEFAULT_MAX_L2_PER_SHARD: usize = 512;

/// A parsed `bench-metadata` invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchOptions {
    /// Cache directory; `.meta` files are read from its `metadata/` subtree.
    pub cache_dir: PathBuf,
    /// Outstanding-read counts to sweep, in order.
    pub levels: Vec<usize>,
    /// Files each level reads, from its own disjoint set.
    pub files_per_level: usize,
    /// Per-level time limit. A level stops issuing reads once it is reached.
    pub max_level_duration: Option<Duration>,
    /// Cap on L1 shard directories enumerated.
    pub max_l1_shards: usize,
    /// Cap on L2 directories enumerated per L1 shard.
    pub max_l2_per_shard: usize,
    /// L1 shard directories skipped before collecting, so a repeat run reads cold files.
    pub skip_l1_shards: usize,
    /// Unix seconds at which to start the sweep, so several hosts measure the same window.
    pub start_sweep_at: Option<u64>,
    /// Print JSON instead of text.
    pub json: bool,
}

/// Latency summary in milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LatencyMs {
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

/// Bounded enumeration figures.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnumerationReport {
    pub elapsed_secs: f64,
    pub l1_readdirs: usize,
    pub l2_readdirs: usize,
    pub paths_collected: usize,
    pub l2_readdir_latency_ms: LatencyMs,
}

/// One concurrency level's result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LevelReport {
    pub concurrency: usize,
    /// Files read at this level (fewer than the set if `--max-level-secs` cut it short).
    pub files_read: usize,
    pub elapsed_secs: f64,
    pub objects_per_s: f64,
    pub latency_ms: LatencyMs,
    /// Files that could not be read or did not parse as cache metadata.
    pub errors: u64,
    pub bytes_read: u64,
    pub staged_objects: u64,
}

/// The whole run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchReport {
    pub cache_dir: String,
    pub files_per_level_requested: usize,
    /// Files in each level's disjoint set.
    pub files_per_level: usize,
    pub available_parallelism: usize,
    pub enumeration: EnumerationReport,
    /// Set when `--start-sweep-at` was given: whether the sweep started on time.
    pub barrier_missed_by_secs: Option<u64>,
    pub levels: Vec<LevelReport>,
}

/// The `clap` definition. Standalone, so the proxy's own startup parse is untouched and this
/// command cannot fall through into starting a proxy.
pub fn cli() -> clap::Command {
    use clap::{Arg, ArgAction, ArgGroup};
    clap::Command::new("s3-proxy")
        .subcommand_required(true)
        .subcommand(
            clap::Command::new(SUBCOMMAND)
                .about("Measure the shared cache volume's .meta read rate at each concurrency level")
                .long_about(
                    "Measure the shared cache volume's .meta read rate, in objects per second, at \
                     each concurrency level. This is the A(I) figure the metadata sizing rule needs.\n\n\
                     READ ONLY: it lists directories and reads .meta files under <cache_dir>/metadata, \
                     and never writes, renames, deletes or locks anything. It does not need a running \
                     proxy and does not touch proxy state. Run it on every proxy host at once with \
                     --start-sweep-at to measure a fleet's aggregate rate.",
                )
                .arg(
                    Arg::new("config")
                        .short('c')
                        .long("config")
                        .value_name("FILE")
                        .help("The proxy's configuration file; only cache.cache_dir is read"),
                )
                .arg(
                    Arg::new("cache-dir")
                        .long("cache-dir")
                        .value_name("DIR")
                        .help("The cache directory to measure, instead of --config"),
                )
                .group(
                    ArgGroup::new("source")
                        .args(["config", "cache-dir"])
                        .required(true),
                )
                .arg(
                    Arg::new("concurrency")
                        .long("concurrency")
                        .value_name("LIST")
                        .default_value(DEFAULT_CONCURRENCY)
                        .help("Comma-separated outstanding-read counts to sweep"),
                )
                .arg(
                    Arg::new("files-per-level")
                        .long("files-per-level")
                        .value_name("N")
                        .value_parser(clap::value_parser!(usize))
                        .default_value("2000")
                        .help("Files each level reads; each level reads its own disjoint set"),
                )
                .arg(
                    Arg::new("max-level-secs")
                        .long("max-level-secs")
                        .value_name("SECS")
                        .value_parser(clap::value_parser!(u64).range(1..))
                        .help("Stop each level after this many seconds (default: no limit)"),
                )
                .arg(
                    Arg::new("max-l1-shards")
                        .long("max-l1-shards")
                        .value_name("N")
                        .value_parser(clap::value_parser!(usize))
                        .default_value("64")
                        .help("Cap on L1 shard directories enumerated"),
                )
                .arg(
                    Arg::new("max-l2-per-shard")
                        .long("max-l2-per-shard")
                        .value_name("N")
                        .value_parser(clap::value_parser!(usize))
                        .default_value("512")
                        .help("Cap on L2 directories enumerated per L1 shard"),
                )
                .arg(
                    Arg::new("skip-l1-shards")
                        .long("skip-l1-shards")
                        .value_name("N")
                        .value_parser(clap::value_parser!(usize))
                        .default_value("0")
                        .help("Skip this many L1 shards first, so a repeat run reads cold files"),
                )
                .arg(
                    Arg::new("start-sweep-at")
                        .long("start-sweep-at")
                        .value_name("UNIX_SECS")
                        .value_parser(clap::value_parser!(u64))
                        .help("Wait after enumeration until this Unix time, to align several hosts"),
                )
                .arg(
                    Arg::new("json")
                        .long("json")
                        .action(ArgAction::SetTrue)
                        .help("Print the report as JSON instead of text"),
                ),
        )
}

/// True when the command line asks for this subcommand: it must be the first argument.
pub fn is_requested(args: &[OsString]) -> bool {
    args.get(1).is_some_and(|a| a == SUBCOMMAND)
}

/// Parse the command line into options. `--config` is resolved to its `cache.cache_dir`, with
/// the `CACHE_DIR` override applied as the proxy applies it at startup.
pub fn parse(args: &[OsString]) -> Result<BenchOptions, String> {
    let matches = cli()
        .try_get_matches_from(args)
        .map_err(|e| e.to_string())?;
    let m = matches
        .subcommand_matches(SUBCOMMAND)
        .ok_or_else(|| format!("expected the {SUBCOMMAND} subcommand"))?;

    let cache_dir = if let Some(dir) = m.get_one::<String>("cache-dir") {
        PathBuf::from(dir)
    } else {
        let path = m.get_one::<String>("config").cloned().unwrap_or_default();
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("could not read the configuration file {path}: {e}"))?;
        let mut config: crate::config::Config = serde_yaml_ng::from_str(&text)
            .map_err(|e| format!("could not parse the configuration file {path}: {e}"))?;
        config.apply_env_overrides();
        config.cache.cache_dir
    };

    let list = m
        .get_one::<String>("concurrency")
        .cloned()
        .unwrap_or_default();
    let mut levels = Vec::new();
    for part in list.split(',') {
        let n: usize = part
            .trim()
            .parse()
            .map_err(|_| format!("--concurrency: {part:?} is not a positive integer"))?;
        if n == 0 {
            return Err("--concurrency: levels must be at least 1".to_string());
        }
        levels.push(n);
    }

    let files_per_level = *m.get_one::<usize>("files-per-level").unwrap_or(&0);
    if files_per_level == 0 {
        return Err("--files-per-level must be at least 1".to_string());
    }

    Ok(BenchOptions {
        cache_dir,
        levels,
        files_per_level,
        max_level_duration: m
            .get_one::<u64>("max-level-secs")
            .map(|s| Duration::from_secs(*s)),
        max_l1_shards: *m.get_one::<usize>("max-l1-shards").unwrap_or(&0),
        max_l2_per_shard: *m.get_one::<usize>("max-l2-per-shard").unwrap_or(&0),
        skip_l1_shards: *m.get_one::<usize>("skip-l1-shards").unwrap_or(&0),
        start_sweep_at: m.get_one::<u64>("start-sweep-at").copied(),
        json: m.get_flag("json"),
    })
}

/// Run the subcommand end to end and return the process exit code: 0 measured, 1 the run
/// failed, 2 usage error.
pub fn main_entry(args: &[OsString]) -> i32 {
    // clap's own outcomes first, so `--help` prints to stdout and exits 0.
    if let Err(e) = cli().try_get_matches_from(args) {
        let _ = e.print();
        return e.exit_code();
    }
    let opts = match parse(args) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let max_level = opts.levels.iter().copied().max().unwrap_or(1);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Every outstanding read occupies a blocking-pool thread while it waits.
        .max_blocking_threads(max_level.max(1) + 16)
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("could not start the runtime: {e}");
            return 1;
        }
    };
    match runtime.block_on(run(&opts)) {
        Ok(report) => {
            if opts.json {
                match serde_json::to_string_pretty(&report) {
                    Ok(s) => println!("{s}"),
                    Err(e) => {
                        eprintln!("could not serialise the report: {e}");
                        return 1;
                    }
                }
            } else {
                print!("{}", render_text(&report));
            }
            0
        }
        Err(e) => {
            eprintln!("bench-metadata: {e}");
            1
        }
    }
}

// ---- The only two filesystem calls this module makes. Both are read-only. ----

/// List a directory's entries as `(path, is_dir)`. Read only.
fn list_dir(dir: &Path) -> std::io::Result<Vec<(PathBuf, bool)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)?.flatten() {
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        out.push((entry.path(), is_dir));
    }
    Ok(out)
}

/// Read one file's bytes. Read only.
fn read_meta(path: &Path) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}

// ---- Measurement ----

fn latency_ms(sorted: &[Duration]) -> LatencyMs {
    let pick = |p: f64| -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
        sorted[idx.min(sorted.len() - 1)].as_secs_f64() * 1000.0
    };
    let mean = if sorted.is_empty() {
        0.0
    } else {
        sorted.iter().map(|d| d.as_secs_f64()).sum::<f64>() / sorted.len() as f64 * 1000.0
    };
    LatencyMs {
        mean,
        p50: pick(0.50),
        p95: pick(0.95),
        p99: pick(0.99),
        max: pick(1.0),
    }
}

/// Read and parse one `.meta`, doing the production scan's typed work.
/// Returns `(bytes, ok, staged)`; a failure is counted, never acted on.
fn read_and_parse(path: &Path) -> (u64, bool, bool) {
    let Ok(content) = read_meta(path) else {
        return (0, false, false);
    };
    let len = content.len() as u64;
    match serde_json::from_slice::<crate::cache_types::NewCacheMetadata>(&content) {
        Ok(m) => {
            let wc = m.object_metadata.is_write_cached;
            let staged = wc
                && m.ranges
                    .iter()
                    .any(|r| crate::cache_types::is_staged_range_spec(r, wc));
            (len, true, staged)
        }
        Err(_) => (len, false, false),
    }
}

struct Enumerated {
    paths: Vec<PathBuf>,
    l1_readdirs: usize,
    l2_latencies: Vec<Duration>,
}

/// Collect a bounded sample of `.meta` paths, timing each L2 readdir.
fn enumerate_bounded(
    metadata_dir: &Path,
    opts: &BenchOptions,
    want: usize,
) -> std::io::Result<Enumerated> {
    let mut l1_readdirs = 0usize;
    let mut l2_latencies = Vec::new();
    let mut paths = Vec::new();

    // Buckets, skipping the internal `_journals`, `_write_ledger`, `_shard_state` trees.
    let mut buckets: Vec<PathBuf> = list_dir(metadata_dir)?
        .into_iter()
        .filter(|(p, is_dir)| {
            *is_dir
                && !p
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with('_'))
        })
        .map(|(p, _)| p)
        .collect();
    buckets.sort();

    let mut l1_dirs: Vec<PathBuf> = Vec::new();
    for bucket in &buckets {
        l1_readdirs += 1;
        if let Ok(entries) = list_dir(bucket) {
            let mut here: Vec<PathBuf> = entries
                .into_iter()
                .filter(|(_, d)| *d)
                .map(|(p, _)| p)
                .collect();
            here.sort();
            l1_dirs.extend(here);
        }
    }
    let skip = opts.skip_l1_shards.min(l1_dirs.len());
    l1_dirs.drain(..skip);
    l1_dirs.truncate(opts.max_l1_shards);

    'outer: for l1 in &l1_dirs {
        l1_readdirs += 1;
        let mut l2_dirs: Vec<PathBuf> = match list_dir(l1) {
            Ok(e) => e.into_iter().filter(|(_, d)| *d).map(|(p, _)| p).collect(),
            Err(_) => continue,
        };
        l2_dirs.sort();
        l2_dirs.truncate(opts.max_l2_per_shard);
        for l2 in &l2_dirs {
            let start = Instant::now();
            let entries = list_dir(l2);
            l2_latencies.push(start.elapsed());
            let Ok(entries) = entries else { continue };
            let mut metas: Vec<PathBuf> = entries
                .into_iter()
                .filter(|(p, d)| {
                    !*d && p
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().ends_with(".meta"))
                })
                .map(|(p, _)| p)
                .collect();
            metas.sort();
            for p in metas {
                paths.push(p);
                if paths.len() >= want {
                    break 'outer;
                }
            }
        }
    }
    Ok(Enumerated {
        paths,
        l1_readdirs,
        l2_latencies,
    })
}

async fn run_level(
    concurrency: usize,
    paths: Vec<PathBuf>,
    limit: Option<Duration>,
) -> LevelReport {
    let start = Instant::now();
    let deadline = limit.map(|d| start + d);
    let results: Vec<(Duration, u64, bool, bool)> = stream::iter(paths)
        .take_while(|_| futures::future::ready(deadline.is_none_or(|d| Instant::now() < d)))
        .map(|path| async move {
            let t = Instant::now();
            let out = tokio::task::spawn_blocking(move || read_and_parse(&path))
                .await
                .unwrap_or((0, false, false));
            (t.elapsed(), out.0, out.1, out.2)
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let elapsed = start.elapsed();

    let mut latencies = Vec::with_capacity(results.len());
    let (mut bytes_read, mut errors, mut staged_objects) = (0u64, 0u64, 0u64);
    for (lat, bytes, ok, staged) in &results {
        latencies.push(*lat);
        bytes_read += bytes;
        errors += u64::from(!ok);
        staged_objects += u64::from(*staged);
    }
    latencies.sort();
    LevelReport {
        concurrency,
        files_read: results.len(),
        elapsed_secs: elapsed.as_secs_f64(),
        objects_per_s: results.len() as f64 / elapsed.as_secs_f64().max(1e-9),
        latency_ms: latency_ms(&latencies),
        errors,
        bytes_read,
        staged_objects,
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Enumerate, partition into disjoint per-level sets, and sweep.
pub async fn run(opts: &BenchOptions) -> Result<BenchReport, String> {
    if opts.levels.is_empty() {
        return Err("no concurrency levels given".to_string());
    }
    let metadata_dir = opts.cache_dir.join("metadata");
    if !metadata_dir.is_dir() {
        return Err(format!(
            "{} is not a directory; is this the cache_dir the proxies are configured with?",
            metadata_dir.display()
        ));
    }
    let sets = opts.levels.len();
    let want = opts.files_per_level.saturating_mul(sets);

    let enum_start = Instant::now();
    let dir = metadata_dir.clone();
    let o = opts.clone();
    let mut en = tokio::task::spawn_blocking(move || enumerate_bounded(&dir, &o, want))
        .await
        .map_err(|e| format!("enumeration task failed: {e}"))?
        .map_err(|e| format!("could not list {}: {e}", metadata_dir.display()))?;
    let enum_elapsed = enum_start.elapsed();
    en.l2_latencies.sort();

    if en.paths.len() < sets {
        return Err(format!(
            "found only {} .meta files, need at least {} (one per level). Raise --max-l1-shards \
             or --max-l2-per-shard, or sweep fewer levels.",
            en.paths.len(),
            sets
        ));
    }

    let mut assigned: Vec<Vec<PathBuf>> = vec![Vec::new(); sets];
    for (i, p) in en.paths.iter().enumerate() {
        assigned[i % sets].push(p.clone());
    }
    let per_set = assigned.iter().map(Vec::len).min().unwrap_or(0);
    for set in assigned.iter_mut() {
        set.truncate(per_set);
    }

    let mut barrier_missed_by_secs = None;
    if let Some(at) = opts.start_sweep_at {
        let now = unix_now();
        if at > now {
            tokio::time::sleep(Duration::from_secs(at - now)).await;
            barrier_missed_by_secs = Some(0);
        } else {
            barrier_missed_by_secs = Some(now - at);
        }
    }

    let mut levels = Vec::with_capacity(sets);
    for (idx, &conc) in opts.levels.iter().enumerate() {
        let set = std::mem::take(&mut assigned[idx]);
        levels.push(run_level(conc, set, opts.max_level_duration).await);
    }

    Ok(BenchReport {
        cache_dir: opts.cache_dir.display().to_string(),
        files_per_level_requested: opts.files_per_level,
        files_per_level: per_set,
        available_parallelism: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        enumeration: EnumerationReport {
            elapsed_secs: enum_elapsed.as_secs_f64(),
            l1_readdirs: en.l1_readdirs,
            l2_readdirs: en.l2_latencies.len(),
            paths_collected: en.paths.len(),
            l2_readdir_latency_ms: latency_ms(&en.l2_latencies),
        },
        barrier_missed_by_secs,
        levels,
    })
}

/// The human-readable report.
pub fn render_text(r: &BenchReport) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = writeln!(
        s,
        "bench-metadata (read only: nothing under the cache directory is written)"
    );
    let _ = writeln!(s, "cache_dir             : {}", r.cache_dir);
    let _ = writeln!(s, "available_parallelism : {}", r.available_parallelism);
    let e = &r.enumeration;
    let _ = writeln!(
        s,
        "enumeration           : {:.3} s, {} L1 readdirs, {} L2 readdirs, {} paths collected",
        e.elapsed_secs, e.l1_readdirs, e.l2_readdirs, e.paths_collected
    );
    let l = &e.l2_readdir_latency_ms;
    let _ = writeln!(
        s,
        "L2 readdir latency    : mean {:.3} ms, p50 {:.3}, p95 {:.3}, p99 {:.3}, max {:.3}",
        l.mean, l.p50, l.p95, l.p99, l.max
    );
    let _ = writeln!(
        s,
        "files per level       : {} (requested {})",
        r.files_per_level, r.files_per_level_requested
    );
    match r.barrier_missed_by_secs {
        Some(0) => {
            let _ = writeln!(s, "sweep barrier         : met");
        }
        Some(late) => {
            let _ = writeln!(
                s,
                "sweep barrier         : MISSED by {late} s; a cross-host comparison from this run is not valid"
            );
        }
        None => {}
    }
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "{:>6}  {:>7}  {:>9}  {:>11}  {:>9}  {:>9}  {:>9}  {:>6}",
        "conc", "files", "secs", "obj/s", "mean ms", "p50 ms", "p99 ms", "errs"
    );
    for v in &r.levels {
        let _ = writeln!(
            s,
            "{:>6}  {:>7}  {:>9.3}  {:>11.1}  {:>9.3}  {:>9.3}  {:>9.3}  {:>6}",
            v.concurrency,
            v.files_read,
            v.elapsed_secs,
            v.objects_per_s,
            v.latency_ms.mean,
            v.latency_ms.p50,
            v.latency_ms.p99,
            v.errors
        );
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    /// The read-only guarantee, enforced: this file must not name any mutating filesystem API
    /// outside this test. The needles are split so this test's own text does not match them.
    #[test]
    fn module_source_has_no_mutating_filesystem_calls() {
        let source = include_str!("bench_metadata.rs");
        let body = source
            .split(concat!("#[cfg(test)]", "\nmod tests"))
            .next()
            .unwrap();
        let needles = [
            concat!("fs::", "write"),
            concat!("File::", "create"),
            concat!("Open", "Options"),
            concat!("remove_", "file"),
            concat!("remove_", "dir"),
            concat!("fs::", "rename"),
            concat!("create_", "dir"),
            concat!("set_", "permissions"),
            concat!("hard_", "link"),
            concat!("sym", "link"),
            concat!("fs::", "copy"),
            concat!("sync_", "all"),
            concat!("set_", "len"),
            concat!("fl", "ock"),
            concat!("try_", "lock"),
            concat!("metadata_lock_", "manager"),
        ];
        for n in needles {
            assert!(
                !body.contains(n),
                "bench_metadata.rs must stay read-only, but names `{n}`"
            );
        }
    }

    #[test]
    fn parse_defaults_and_requested() {
        assert!(is_requested(&os(&[
            "s3-proxy",
            SUBCOMMAND,
            "--cache-dir",
            "/x"
        ])));
        assert!(!is_requested(&os(&["s3-proxy", "-c", SUBCOMMAND])));
        let o = parse(&os(&["s3-proxy", SUBCOMMAND, "--cache-dir", "/x"])).unwrap();
        assert_eq!(o.levels, vec![1, 4, 8, 16, 32, 64, 128, 256]);
        assert_eq!(o.files_per_level, DEFAULT_FILES_PER_LEVEL);
        assert_eq!(o.max_l1_shards, DEFAULT_MAX_L1_SHARDS);
        assert_eq!(o.max_l2_per_shard, DEFAULT_MAX_L2_PER_SHARD);
        assert_eq!(o.max_level_duration, None);
        assert!(!o.json);
        assert!(parse(&os(&["s3-proxy", SUBCOMMAND])).is_err());
        assert!(parse(&os(&[
            "s3-proxy",
            SUBCOMMAND,
            "--cache-dir",
            "/x",
            "--concurrency",
            "4,0"
        ]))
        .is_err());
    }
}
