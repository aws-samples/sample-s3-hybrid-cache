//! Drive sustained eviction against sustained admission on the same fixture and hardware, in
//! bytes/s and entries/s, and derive the smallest mean evicted object size at which eviction
//! keeps up with admission.
//!
//! # What this measures
//!
//! The shipped eviction path against the shipped admission path,
//! `DiskCacheManager::store_range`, the method every cache-miss GET and write-cache PUT calls.
//! Nothing in `src/` exists for this measurement. `--eviction-mode` chooses which deleting
//! process runs (next section): the eviction work list's executors (`worklist`), or the
//! single-holder pass, `CacheManager::enforce_disk_cache_limits` →
//! `perform_eviction_with_lock` → `batch_evict_ranges` (`legacy`).
//!
//! # `--eviction-mode`: work list or single-holder pass
//!
//! Each consolidation cycle this binary drives (`run_consolidation_cycle`) schedules the work
//! list's executor pass (`CacheManager::run_eviction_executor_pass`, which executes chunks until
//! none is claimable or its pass budget has elapsed) and its abort tick, on every process, as
//! the proxy does. The eviction-lock winner's `enforce_disk_cache_limits` writes a work list
//! when the capability gate (`fleet_is_worklist_capable`) reads `Capable`, and otherwise runs
//! the single-holder pass. The gate's roster is `metadata/_journals/`: every proxy with a
//! journal modified in the last hour must have a current `.ready` record with
//! `evict_worklist_capable` set.
//!
//! - `worklist` (default): the shipped behavior, from the state where every process has
//!   published its `.ready` record. Each process installs the startup record a proxy writes
//!   before its first write-ledger append, before it writes a journal, because a process with
//!   no ledger append otherwise publishes only on cycles where it wins the consolidation lock,
//!   and until then the gate reads `NotCapable`. A control journal left on the volume by an
//!   earlier `legacy` run is removed at startup.
//! - `legacy`: the control. Before `initialize()`, and on every consolidation tick after it,
//!   the binary keeps an empty journal with a fresh modification time for a phantom instance
//!   (`LEGACY_CONTROL_INSTANCE`). The phantom publishes no `.ready` record, so the gate reads
//!   `NotCapable`, as it does while a proxy without the work list shares the volume, and every
//!   trigger runs the single-holder pass. The journal is left in place at exit.
//!
//! The work-list figures are read from the volume, sampled every second, because the
//! generation janitor removes all but the newest generations: chunks claimed (a progress record
//! or a done-marker naming the instance), chunks done (done-markers by `executor`), and
//! takeovers (chunks an instance finished after another instance had recorded a batch of
//! them). A claim that ended before its first batch leaves no record and is not counted.
//! `eviction_last_result.json` is sampled every 250 ms. The strict margin is drain over active
//! pass time, divided by fill, counting only single-holder passes, or fully executed
//! generations, that finished inside this process's window. Each process prints an
//! `EVICTION_MODE_SUMMARY` JSON line; a multi-process margin divides by the sum of the
//! processes' fill.
//!
//! # The read-fill bound, as the proxy registers it
//!
//! After `initialize()` and `set_cache_manager_in_consolidator()`, the binary calls
//! `CacheManager::start_put_usage_refresher()`, the call `HttpProxy::start` makes before its
//! accept loop. That call publishes the `max_cache_size` bound that read fills consult, so an
//! admission that would take the cache past `max_cache_size` on the 5 s usage snapshot is
//! declined, as a GET-miss fill is on a proxy. Admission goes through
//! `DiskCacheManager::prepare_range_store` and `PreparedRangeFill::finish`, the two halves of
//! `store_range`; a `None` from the first is a declined fill. The binary counts attempted and
//! declined fills, by bytes and by count, and prints them in the results and in the
//! `EVICTION_MODE_SUMMARY` line. `fill_bytes_per_sec` is the bytes actually admitted. A decline
//! here includes any reason `prepare_range_store` declines for (the danger-band guard as well
//! as the `max_cache_size` bound); on a volume with free space only the `max_cache_size` bound
//! can fire. A declined fill returns at once, so a worker waits the smoothed time of its
//! admitted fills before the next attempt; the attempted rate is then the rate the workers
//! would deliver if every fill were stored, and the declined share is a share of that.
//!
//! `--fixture-start-percent <n>` sets `max_cache_size` so the restored fixture occupies `n`
//! percent of it. With the default trigger of 95 percent, `n` = 85 starts the run ten points
//! below the trigger, and eviction begins only after admission has grown the cache to the
//! trigger. Like the 90 percent default, it is sized from the `ranges/` bytes the process sees
//! at its own startup, so a multi-process run passes every process the same
//! `--max-cache-size-gib`, computed from the fixture, instead.
//!
//! `RUST_LOG` sets the log capture's filter (default `info`).
//!
//! # Why one process with two loops, not two processes
//!
//! The question is sustained eviction against sustained admission on the same fixture and
//! hardware, not isolated single-direction rates. Two tokio tasks against one
//! `Arc<CacheManager>` reproduce the real contention: eviction's global lock, journal
//! consolidation, and the shared `metadata/` and `ranges/` trees all see both directions
//! competing, which two separate benchmark runs would not.
//!
//! # Making admission visible to eviction: manual consolidation cycles
//!
//! `store_range` writes the `.bin` immediately, defers the `.meta` write to the journal, and
//! bumps an in-memory `SizeAccumulator` delta, none of which
//! `enforce_disk_cache_limits_internal` reads. That function reads
//! `consolidator.get_current_size()`, which is `size_state.json` on disk, updated only when
//! `run_consolidation_cycle()` runs. Outside the proxy's main loop nothing schedules that cycle,
//! so this binary drives it from a third task on a fixed interval, the same shape the proxy
//! uses, with the interval as a setting.
//!
//! # Usage
//!
//! ```text
//! cargo run --release --example eviction_vs_admission_throughput -- \
//!     --cache-dir /mnt/cache-fixture/tree \
//!     --max-cache-size-gib 400 \
//!     --duration-secs 300 \
//!     --admission-threads 8 \
//!     --consolidation-interval-secs 5 \
//!     --eviction-mode worklist
//! ```
//!
//! A multi-process run must pass the same `--max-cache-size-gib` to every process: the default
//! is 90% of the `ranges/` bytes each process sees at its own startup, so a process started
//! after another's first eviction pass gets a lower ceiling.
//!
//! Reports admitted and evicted bytes/s and entries/s over the run, and the minimum mean
//! evicted object size derived from them (see the final printed section).

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// `ranges_evicted=` is not returned by `enforce_disk_cache_limits` — only bytes
/// freed is (`perform_eviction_with_lock`'s doc comment: "Return total_bytes_freed
/// (not total_ranges_evicted) for accurate size tracking"). Rather than change that
/// function's signature for a measurement binary, capture its own
/// `[DISK_CACHE_EVICTION] Eviction completed: keys_evicted=N, ranges_evicted=M, ...`
/// info-level log line, which already carries the entries-side figure R2.1 needs.
/// A tracing subscriber writing to a plain file is cheaper and less risky than
/// touching production code's return type to serve one binary.
fn init_log_capture(log_path: &std::path::Path) -> std::fs::File {
    let file = std::fs::File::create(log_path).expect("failed to create log capture file");
    let writer = file.try_clone().expect("failed to clone log file handle");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(move || writer.try_clone().expect("clone for tracing writer"))
        .with_ansi(false)
        .with_target(false)
        .init();
    file
}

/// Marker written to the log immediately before the measurement window's clock
/// starts (`run_start`), so the two parsers below can exclude everything that
/// happened during `initialize()` — including any eviction `initialize()` itself
/// triggers, which does NOT belong to the measured window and corrupted an early
/// run's reported eviction rate by attributing minutes of pre-window eviction work
/// to the post-window duration.
const MEASUREMENT_WINDOW_MARKER: &str = "=== MEASUREMENT WINDOW STARTS HERE ===";

/// Return only the log content AFTER the measurement-window marker. If the marker
/// is absent (defensive — should not happen given main's ordering), returns the
/// whole file rather than silently reporting zero.
fn content_after_window_marker(log_path: &std::path::Path) -> String {
    let content = std::fs::read_to_string(log_path).unwrap_or_default();
    match content.find(MEASUREMENT_WINDOW_MARKER) {
        Some(pos) => content[pos..].to_string(),
        None => content,
    }
}

/// Sum `ranges_evicted=<n>` across every `[DISK_CACHE_EVICTION] Eviction completed`
/// line in the captured log — the entries-side figure for R2.1's eviction rate.
/// Scoped to the measurement window only; see `MEASUREMENT_WINDOW_MARKER`.
fn sum_ranges_evicted_from_log(log_path: &std::path::Path) -> u64 {
    let content = content_after_window_marker(log_path);
    let mut total = 0u64;
    for line in content.lines() {
        if let Some(pos) = line.find("ranges_evicted=") {
            let rest = &line[pos + "ranges_evicted=".len()..];
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse::<u64>() {
                total += n;
            }
        }
    }
    total
}

/// Sum the aggregate bytes-freed figure from the `Eviction completed: bytes_freed=<N>
/// (size will be updated via journal consolidation)` line at `src/cache.rs`'s
/// post-eviction-pass log point (the plain-integer companion to the human-readable
/// `[DISK_CACHE_EVICTION] Eviction completed: ... freed=<human>` line).
///
/// Deliberately NOT matching a bare `bytes_freed=` substring: `[BATCH_EVICTION]`
/// emits one such line PER OBJECT inside the same pass, and matching that too would
/// double-count every object's contribution on top of the pass-level aggregate this
/// function wants. The trailing `(size will be updated...)` clause is unique to the
/// aggregate line and absent from `[BATCH_EVICTION]`'s.
fn sum_bytes_freed_from_log(log_path: &std::path::Path) -> u64 {
    let content = content_after_window_marker(log_path);
    let mut total = 0u64;
    for line in content.lines() {
        if !line.contains("(size will be updated via journal consolidation)") {
            continue;
        }
        if let Some(pos) = line.find("bytes_freed=") {
            let rest = &line[pos + "bytes_freed=".len()..];
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse::<u64>() {
                total += n;
            }
        }
    }
    total
}

fn usage() -> ! {
    eprintln!(
        "eviction_vs_admission_throughput — sustained eviction vs sustained admission\n\
         \n\
         Required:\n\
         \x20 --cache-dir <dir>                    cache root; MUST already contain a\n\
         \x20                                       populated fixture (metadata/ + ranges/)\n\
         \n\
         Optional:\n\
         \x20 --max-cache-size-gib <n>              eviction ceiling, GiB (default: 90% of\n\
         \x20                                       the fixture's on-disk size, computed at\n\
         \x20                                       startup, so eviction has something to do\n\
         \x20                                       from the first cycle)\n\
         \x20 --fixture-start-percent <n>           set the eviction ceiling so the fixture\n\
         \x20                                       starts at n percent of it (for example 85\n\
         \x20                                       to start ten points below a 95 trigger);\n\
         \x20                                       overrides the 90 percent default and is\n\
         \x20                                       refused together with --max-cache-size-gib\n\
         \x20 --duration-secs <n>                   measurement window (default 300)\n\
         \x20 --admission-threads <n>               concurrent admission workers (default 8)\n\
         \x20 --admission-object-size-bytes <n>     fixed size per admitted range (default\n\
         \x20                                       1048576 = 1 MiB; use --admission-sizes for\n\
         \x20                                       a graded mix instead)\n\
         \x20 --admission-sizes <a,b,c,...>         comma-separated byte sizes, cycled\n\
         \x20                                       round-robin across admissions (overrides\n\
         \x20                                       --admission-object-size-bytes)\n\
         \x20 --consolidation-interval-secs <n>     how often admission is flushed to\n\
         \x20                                       size_state.json (default 5, matches the\n\
         \x20                                       production LEDGER_COMPACTION_INTERVAL_SECS\n\
         \x20                                       pattern)\n\
         \x20 --eviction-trigger-percent <n>        default 80 (lower than production's 95, so\n\
         \x20                                       eviction fires promptly against a fixed\n\
         \x20                                       ceiling rather than needing the fixture to\n\
         \x20                                       grow into a 95% band)\n\
         \x20 --eviction-target-percent <n>         default 70\n\
         \x20 --eviction-mode legacy|worklist        default worklist (the shipped behaviour);\n\
         \x20                                       legacy keeps a phantom pre-3.0.0 journal\n\
         \x20                                       on the volume so every trigger runs the\n\
         \x20                                       legacy single-holder pass\n"
    );
    std::process::exit(2);
}

struct Args {
    cache_dir: PathBuf,
    max_cache_size_gib: Option<f64>,
    fixture_start_percent: Option<f64>,
    duration_secs: u64,
    admission_threads: usize,
    admission_sizes: Vec<u64>,
    consolidation_interval_secs: u64,
    eviction_trigger_percent: u8,
    eviction_target_percent: u8,
    eviction_mode: EvictionMode,
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut cache_dir: Option<PathBuf> = None;
    let mut max_cache_size_gib: Option<f64> = None;
    let mut fixture_start_percent: Option<f64> = None;
    let mut duration_secs = 300u64;
    let mut admission_threads = 8usize;
    let mut admission_sizes: Vec<u64> = vec![1024 * 1024];
    let mut consolidation_interval_secs = 5u64;
    let mut eviction_trigger_percent = 80u8;
    let mut eviction_target_percent = 70u8;
    let mut eviction_mode = EvictionMode::Worklist;

    let mut i = 0;
    while i < raw.len() {
        let need = |i: usize| -> String {
            raw.get(i + 1).cloned().unwrap_or_else(|| {
                eprintln!("missing value for {}", raw[i]);
                usage()
            })
        };
        match raw[i].as_str() {
            "--cache-dir" => {
                cache_dir = Some(PathBuf::from(need(i)));
                i += 2;
            }
            "--max-cache-size-gib" => {
                max_cache_size_gib = Some(need(i).parse().unwrap_or_else(|_| usage()));
                i += 2;
            }
            "--fixture-start-percent" => {
                let v: f64 = need(i).parse().unwrap_or_else(|_| usage());
                if !(v > 0.0 && v <= 100.0) {
                    eprintln!("--fixture-start-percent must be in (0, 100]");
                    usage()
                }
                fixture_start_percent = Some(v);
                i += 2;
            }
            "--duration-secs" => {
                duration_secs = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--admission-threads" => {
                admission_threads = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--admission-object-size-bytes" => {
                admission_sizes = vec![need(i).parse().unwrap_or_else(|_| usage())];
                i += 2;
            }
            "--admission-sizes" => {
                admission_sizes = need(i)
                    .split(',')
                    .map(|v| v.trim().parse().unwrap_or_else(|_| usage()))
                    .collect();
                i += 2;
            }
            "--consolidation-interval-secs" => {
                consolidation_interval_secs = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--eviction-trigger-percent" => {
                eviction_trigger_percent = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--eviction-target-percent" => {
                eviction_target_percent = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--eviction-mode" => {
                eviction_mode = match need(i).as_str() {
                    "legacy" => EvictionMode::Legacy,
                    "worklist" => EvictionMode::Worklist,
                    other => {
                        eprintln!("--eviction-mode must be legacy or worklist, not {other}");
                        usage()
                    }
                };
                i += 2;
            }
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown argument: {other}");
                usage();
            }
        }
    }

    if max_cache_size_gib.is_some() && fixture_start_percent.is_some() {
        eprintln!("--max-cache-size-gib and --fixture-start-percent cannot be combined");
        usage()
    }

    Args {
        cache_dir: cache_dir.unwrap_or_else(|| {
            eprintln!("--cache-dir is required");
            usage()
        }),
        max_cache_size_gib,
        fixture_start_percent,
        duration_secs,
        admission_threads,
        admission_sizes,
        consolidation_interval_secs,
        eviction_trigger_percent,
        eviction_target_percent,
        eviction_mode,
    }
}

/// Sum of file lengths under `ranges/`, i.e. the real on-disk `.bin` footprint —
/// the same quantity task 29 established `st_size` gives exactly, used here only to
/// size the eviction ceiling relative to what actually exists.
fn sum_ranges_bytes(ranges_root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![ranges_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// Which deleting process a run measures. See the module doc's `--eviction-mode` section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionMode {
    /// The shipped behaviour: the lock winner authors a work list when the capability gate reads
    /// `Capable`, and every process's consolidation cycle executes chunks of it.
    Worklist,
    /// The control: a phantom pre-3.0.0 instance on the volume holds the gate at `NotCapable`,
    /// so every trigger runs the legacy single-holder pass.
    Legacy,
}

/// The phantom instance the `legacy` control keeps on the volume. Its host part is not this
/// host's name, so the journal reaper treats it as a foreign host's journal and leaves an empty
/// one alone until it has been idle for a day.
const LEGACY_CONTROL_INSTANCE: &str = "eviction-harness-legacy-control:0";

fn legacy_control_journal(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join("_journals")
        .join(format!("{LEGACY_CONTROL_INSTANCE}.journal"))
}

/// Create the phantom's journal if absent and set its modification time to now. An empty
/// journal holds nothing to consolidate; the gate reads only its age, against a one-hour window,
/// which is why every consolidation tick refreshes it.
fn touch_legacy_control(cache_dir: &Path) -> std::io::Result<()> {
    let path = legacy_control_journal(cache_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    file.set_modified(SystemTime::now())
}

/// Install, if this instance has none, the `.ready` record a proxy publishes before its first
/// write-ledger append (`staged_authority::publish_startup_ready_record`): work-list capable,
/// both rotation flags false, which block nobody. Staged in a temporary file and hard-linked,
/// so a concurrent reader never sees it half-written. The product's own publications overwrite
/// it. `Ok(false)` when a record was already there.
fn install_startup_ready_record(cache_dir: &Path, instance: &str) -> std::io::Result<bool> {
    let record = s3_proxy::staged_authority::ReadyRecord {
        schema: 1, // `staged_authority::AUTHORITY_DOC_SCHEMA`, which is crate-private
        instance_id: instance.to_string(),
        binary_version: env!("CARGO_PKG_VERSION").to_string(),
        fold_capable: true,
        rotation_complete: false,
        fold_incomplete_total: 0,
        repair_rotation_complete: false,
        repair_stalest_age_secs: None,
        evict_worklist_capable: true,
        written_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    };
    let body = serde_json::to_string_pretty(&record).map_err(std::io::Error::other)?;
    let dir = cache_dir
        .join("metadata")
        .join("_shard_state")
        .join("ready");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{instance}.ready"));
    let tmp = dir.join(format!(".{instance}.ready.harness-tmp"));
    std::fs::write(&tmp, body.as_bytes())?;
    let linked = std::fs::hard_link(&tmp, &path);
    let _ = std::fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

fn unix_now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn worklist_root(cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("metadata")
        .join("_shard_state")
        .join("eviction_worklist")
}

/// What one instance's progress file for one chunk said, the last time its length changed.
#[derive(Debug, Default, Clone)]
struct ProgressObs {
    file_len: u64,
    /// Lines for this chunk's checksum: batches recorded, over every attempt.
    batches: u64,
    /// Summed `unlinked_bytes` of those lines.
    unlinked: u64,
}

/// One work-list chunk, as sampled from `{generation}/{bucket}/{XX}.*`.
#[derive(Debug, Default, Clone)]
struct ChunkObs {
    authored_at: Option<u64>,
    checksum: Option<String>,
    /// `(executor, completed_at)` from the done-marker.
    done: Option<(String, u64)>,
    /// Per instance named in a `.progress.{instance}` file.
    progress: BTreeMap<String, ProgressObs>,
}

#[derive(Debug, Default)]
struct WorklistObs {
    /// Generation directories present before `initialize()`.
    preexisting: BTreeSet<u64>,
    /// Chunks whose done-marker was already there before `initialize()`; excluded from every
    /// figure. A chunk of a live generation finished after this process started counts.
    done_before_start: BTreeSet<(u64, String)>,
    generations: BTreeSet<u64>,
    aborted: BTreeSet<u64>,
    /// Keyed by `(generation, "bucket/XX")`.
    chunks: BTreeMap<(u64, String), ChunkObs>,
}

/// One distinct `eviction_last_result.json` document, keyed by instance, completion time and
/// bytes freed.
#[derive(Debug, Clone)]
struct PassDoc {
    instance_id: String,
    completed_at: u64,
    bytes_freed: u64,
    ranges_evicted: u64,
    duration_ms: Option<u64>,
    /// `mode: "authored"`: the author's document, which lists work and frees nothing.
    authored: bool,
}

#[derive(Debug, Default)]
struct VolumeObs {
    worklist: WorklistObs,
    passes: Vec<PassDoc>,
    last_result_bytes: Vec<u8>,
}

fn list_generations(cache_dir: &Path) -> BTreeSet<u64> {
    std::fs::read_dir(worklist_root(cache_dir))
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.file_name().to_string_lossy().parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn read_json(path: &Path) -> Option<serde_json::Value> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

/// A chunk's header is its first line.
fn read_chunk_header(path: &Path) -> Option<serde_json::Value> {
    let mut line = String::new();
    std::io::BufReader::new(std::fs::File::open(path).ok()?)
        .read_line(&mut line)
        .ok()?;
    serde_json::from_str(&line).ok()
}

fn scan_bucket(dir: &Path, generation: u64, bucket: &str, obs: &mut WorklistObs) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut names: Vec<String> = entries
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    // `XX.chunk` sorts before `XX.done` and `XX.progress.*`, so a chunk's header (and with it
    // the checksum progress lines are matched against) is read first.
    names.sort();
    for name in names {
        let path = dir.join(&name);
        if let Some(l1) = name.strip_suffix(".chunk") {
            let c = obs
                .chunks
                .entry((generation, format!("{bucket}/{l1}")))
                .or_default();
            if c.checksum.is_none() {
                if let Some(h) = read_chunk_header(&path) {
                    c.authored_at = h["authored_at"].as_u64();
                    c.checksum = h["checksum"].as_str().map(str::to_string);
                }
            }
        } else if let Some(l1) = name.strip_suffix(".done") {
            let c = obs
                .chunks
                .entry((generation, format!("{bucket}/{l1}")))
                .or_default();
            if c.done.is_none() {
                if let Some(d) = read_json(&path) {
                    if let (Some(executor), Some(at)) =
                        (d["executor"].as_str(), d["completed_at"].as_u64())
                    {
                        c.done = Some((executor.to_string(), at));
                    }
                }
            }
        } else if let Some((l1, instance)) = name.split_once(".progress.") {
            let c = obs
                .chunks
                .entry((generation, format!("{bucket}/{l1}")))
                .or_default();
            let Some(checksum) = c.checksum.clone() else {
                continue;
            };
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let seen = c.progress.entry(instance.to_string()).or_default();
            if seen.file_len == len {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            let mut fresh = ProgressObs {
                file_len: len,
                ..Default::default()
            };
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue; // a torn last line
                };
                if v["chunk_checksum"].as_str() == Some(checksum.as_str()) {
                    fresh.batches += 1;
                    fresh.unlinked += v["unlinked_bytes"].as_u64().unwrap_or(0);
                }
            }
            *seen = fresh;
        }
    }
}

fn scan_worklist(cache_dir: &Path, obs: &mut WorklistObs) {
    let Ok(generations) = std::fs::read_dir(worklist_root(cache_dir)) else {
        return;
    };
    for g in generations.flatten() {
        let Ok(generation) = g.file_name().to_string_lossy().parse::<u64>() else {
            continue;
        };
        obs.generations.insert(generation);
        let Ok(entries) = std::fs::read_dir(g.path()) else {
            continue;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                scan_bucket(&path, generation, &e.file_name().to_string_lossy(), obs);
            } else if e.file_name() == "_aborted.json" {
                obs.aborted.insert(generation);
            }
        }
    }
}

fn poll_last_result(cache_dir: &Path, obs: &mut VolumeObs) {
    let path = cache_dir
        .join("metadata")
        .join("_shard_state")
        .join("eviction_last_result.json");
    let Ok(bytes) = std::fs::read(&path) else {
        return;
    };
    if bytes.is_empty() || bytes == obs.last_result_bytes {
        return;
    }
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return;
    };
    obs.last_result_bytes = bytes;
    let doc = PassDoc {
        instance_id: v["instance_id"].as_str().unwrap_or_default().to_string(),
        completed_at: v["completed_at"].as_u64().unwrap_or(0),
        bytes_freed: v["bytes_freed"].as_u64().unwrap_or(0),
        ranges_evicted: v["ranges_evicted"].as_u64().unwrap_or(0),
        duration_ms: v["duration_ms"].as_u64(),
        authored: v["mode"].as_str() == Some("authored"),
    };
    let seen = obs.passes.iter().any(|p| {
        p.instance_id == doc.instance_id
            && p.completed_at == doc.completed_at
            && p.bytes_freed == doc.bytes_freed
    });
    if !seen {
        obs.passes.push(doc);
    }
}

/// Sample the volume until `stop`: the pass document every 250 ms, the work list every second,
/// and both once more after `stop` is seen.
fn spawn_volume_sampler(
    cache_dir: PathBuf,
    obs: Arc<Mutex<VolumeObs>>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut tick: u64 = 0;
        loop {
            let stopping = stop.load(Ordering::Relaxed);
            {
                let mut o = obs.lock().unwrap_or_else(|p| p.into_inner());
                poll_last_result(&cache_dir, &mut o);
                if tick.is_multiple_of(4) || stopping {
                    scan_worklist(&cache_dir, &mut o.worklist);
                }
            }
            if stopping {
                break;
            }
            tick += 1;
            std::thread::sleep(Duration::from_millis(250));
        }
    })
}

#[derive(Debug, Default)]
struct InstanceTally {
    claimed: u64,
    done: u64,
    takeovers: u64,
    unlinked_bytes: u64,
}

/// Per instance, from the sampled chunks not already done when this process started. Also
/// returns the number of chunks more than one instance recorded work on.
fn tally_instances(obs: &WorklistObs) -> (BTreeMap<String, InstanceTally>, u64) {
    let mut out: BTreeMap<String, InstanceTally> = BTreeMap::new();
    let mut shared = 0u64;
    for (key, c) in &obs.chunks {
        if obs.done_before_start.contains(key) {
            continue;
        }
        let mut names: BTreeSet<&str> = c
            .progress
            .iter()
            .filter(|(_, p)| p.batches > 0)
            .map(|(i, _)| i.as_str())
            .collect();
        if let Some((executor, _)) = &c.done {
            names.insert(executor.as_str());
        }
        if names.len() > 1 {
            shared += 1;
        }
        for name in &names {
            out.entry(name.to_string()).or_default().claimed += 1;
        }
        for (instance, p) in &c.progress {
            out.entry(instance.clone()).or_default().unlinked_bytes += p.unlinked;
        }
        if let Some((executor, _)) = &c.done {
            let started_by_another = c
                .progress
                .iter()
                .any(|(i, p)| i != executor && p.batches > 0);
            let t = out.entry(executor.clone()).or_default();
            t.done += 1;
            if started_by_another {
                t.takeovers += 1;
            }
        }
    }
    (out, shared)
}

/// A generation whose every sampled chunk has a done-marker, treated as one pass: from its
/// earliest `authored_at` to its last `completed_at`. An aborted generation, and one whose
/// chunks were all done before this process started, are not passes.
#[derive(Debug)]
struct GenerationPass {
    generation: u64,
    chunks: u64,
    authored_at: u64,
    last_done_at: u64,
    unlinked_bytes: u64,
}

fn finished_generations(obs: &WorklistObs) -> Vec<GenerationPass> {
    let mut by_generation: BTreeMap<u64, Vec<(bool, &ChunkObs)>> = BTreeMap::new();
    for (key, c) in &obs.chunks {
        by_generation
            .entry(key.0)
            .or_default()
            .push((obs.done_before_start.contains(key), c));
    }
    by_generation
        .into_iter()
        .filter_map(|(generation, chunks)| {
            if obs.aborted.contains(&generation) || chunks.iter().all(|(before, _)| *before) {
                return None;
            }
            let mut authored_at = u64::MAX;
            let mut last_done_at = 0u64;
            let mut unlinked_bytes = 0u64;
            for (_, c) in &chunks {
                authored_at = authored_at.min(c.authored_at?);
                last_done_at = last_done_at.max(c.done.as_ref()?.1);
                unlinked_bytes += c.progress.values().map(|p| p.unlinked).sum::<u64>();
            }
            Some(GenerationPass {
                generation,
                chunks: chunks.len() as u64,
                authored_at,
                last_done_at,
                unlinked_bytes,
            })
        })
        .collect()
}

struct ModeSummaryInput<'a> {
    mode: EvictionMode,
    cache_dir: &'a Path,
    me: &'a str,
    obs: &'a VolumeObs,
    /// `run_id`: unix seconds at the window's start.
    window_start: u64,
    window_end: f64,
    /// Bytes admitted per second (declined fills excluded).
    fill_bytes_per_sec: f64,
    attempted_bytes: u64,
    attempted_entries: u64,
    declined_bytes: u64,
    declined_entries: u64,
    max_cache_size: u64,
    measured_secs: f64,
}

/// The work-list section and the strict margin, then one `EVICTION_MODE_SUMMARY` JSON line.
fn print_mode_summary(input: &ModeSummaryInput<'_>) {
    const MIB: f64 = 1024.0 * 1024.0;
    let obs = input.obs;
    let wl = &obs.worklist;
    let in_window = |at: u64| at > input.window_start && (at as f64) <= input.window_end;
    let authored: Vec<u64> = wl
        .generations
        .difference(&wl.preexisting)
        .copied()
        .collect();
    let (tally, shared) = tally_instances(wl);
    let control_present = legacy_control_journal(input.cache_dir).exists();

    println!("\n=== EVICTION MODE: {:?} ===", input.mode);
    println!(
        "legacy control journal at exit : {}",
        if control_present { "present" } else { "absent" }
    );
    println!(
        "generations authored during the run: {:?} (present before initialize: {:?}; aborted: {:?}; \
         chunks already done before initialize, excluded: {})",
        authored,
        wl.preexisting,
        wl.aborted,
        wl.done_before_start.len()
    );
    println!("per instance, read from progress records and done-markers on the volume:");
    if tally.is_empty() {
        println!("  (no instance recorded work-list work)");
    }
    for (instance, t) in &tally {
        println!(
            "  {instance}{}: chunks_claimed={} chunks_done={} lease_takeovers={} unlinked={} B",
            if instance == input.me {
                " (this process)"
            } else {
                ""
            },
            t.claimed,
            t.done,
            t.takeovers,
            t.unlinked_bytes
        );
    }
    println!("chunks with work recorded by more than one instance: {shared}");
    println!(
        "NOTE: the RESULTS eviction lines above and the R2.2 section below parse legacy-pass log \
         lines only; work-list drain is reported here."
    );
    match input.mode {
        EvictionMode::Worklist if tally.values().all(|t| t.done == 0) => println!(
            "WARNING: no instance finished a work-list chunk during the run. Either eviction \
             never triggered or the capability gate did not read Capable, so any pass was the \
             legacy pass."
        ),
        EvictionMode::Legacy if !authored.is_empty() => println!(
            "WARNING: generations {authored:?} were authored during a legacy run; the control \
             was not in force."
        ),
        _ => {}
    }

    // The strict margin: drain = bytes freed / active pass time, over passes that finished
    // inside the window (the strict count). Legacy passes come from `eviction_last_result.json`;
    // the author's own documents free nothing and are excluded.
    let legacy: Vec<&PassDoc> = obs
        .passes
        .iter()
        .filter(|p| !p.authored && p.duration_ms.unwrap_or(0) > 0 && in_window(p.completed_at))
        .collect();
    let legacy_bytes: u64 = legacy.iter().map(|p| p.bytes_freed).sum();
    let legacy_ranges: u64 = legacy.iter().map(|p| p.ranges_evicted).sum();
    let legacy_secs: f64 = legacy
        .iter()
        .map(|p| p.duration_ms.unwrap_or(0) as f64 / 1000.0)
        .fold(0.0, |a, b| a + b);
    let legacy_drain = if legacy_secs > 0.0 {
        legacy_bytes as f64 / legacy_secs
    } else {
        0.0
    };
    // A fully executed generation is the work list's pass. Times are whole seconds, so a
    // generation that drained inside one second counts one.
    let generations: Vec<GenerationPass> = finished_generations(wl)
        .into_iter()
        .filter(|g| in_window(g.last_done_at))
        .collect();
    let generation_bytes: u64 = generations.iter().map(|g| g.unlinked_bytes).sum();
    let generation_secs: f64 = generations
        .iter()
        .map(|g| g.last_done_at.saturating_sub(g.authored_at).max(1) as f64)
        .fold(0.0, |a, b| a + b);
    let generation_drain = if generation_secs > 0.0 {
        generation_bytes as f64 / generation_secs
    } else {
        0.0
    };
    // Over the window as a whole: bytes of chunks whose done-marker falls inside it.
    let window_secs = (input.window_end - input.window_start as f64).max(1.0);
    let window_chunk_bytes: u64 = wl
        .chunks
        .iter()
        .filter(|(k, c)| {
            !wl.done_before_start.contains(*k)
                && c.done.as_ref().is_some_and(|(_, at)| in_window(*at))
        })
        .map(|(_, c)| c.progress.values().map(|p| p.unlinked).sum::<u64>())
        .sum();
    let window_drain = window_chunk_bytes as f64 / window_secs;
    let margin = |drain: f64| {
        if input.fill_bytes_per_sec > 0.0 {
            drain / input.fill_bytes_per_sec
        } else {
            0.0
        }
    };
    println!(
        "\n=== STRICT MARGIN (finished inside this process's window, {}..{:.0}) ===",
        input.window_start, input.window_end
    );
    println!(
        "fill, this process              : {:.2} MiB/s",
        input.fill_bytes_per_sec / MIB
    );
    println!(
        "legacy passes, strict           : {} pass(es), {} B, {} ranges, {:.1} s active, drain {:.2} MiB/s",
        legacy.len(),
        legacy_bytes,
        legacy_ranges,
        legacy_secs,
        legacy_drain / MIB
    );
    println!(
        "work-list generations, strict   : {} generation(s) {:?}, {} B, {:.0} s authored-to-last-done, drain {:.2} MiB/s",
        generations.len(),
        generations
            .iter()
            .map(|g| (g.generation, g.chunks))
            .collect::<Vec<_>>(),
        generation_bytes,
        generation_secs,
        generation_drain / MIB
    );
    println!(
        "work-list chunks done in window : {} B over {:.1} s, drain {:.2} MiB/s",
        window_chunk_bytes,
        window_secs,
        window_drain / MIB
    );
    println!(
        "margin_strict vs this process's fill: legacy {:.2}x, work-list generations {:.2}x; \
         work-list window {:.2}x. Sum fill across processes for a multi-process margin.",
        margin(legacy_drain),
        margin(generation_drain),
        margin(window_drain)
    );

    let summary = serde_json::json!({
        "instance": input.me,
        "mode": format!("{:?}", input.mode).to_lowercase(),
        "window_start": input.window_start,
        "window_end": input.window_end,
        "fill_bytes_per_sec": input.fill_bytes_per_sec,
        "max_cache_size": input.max_cache_size,
        "measured_secs": input.measured_secs,
        "attempted_bytes": input.attempted_bytes,
        "attempted_entries": input.attempted_entries,
        "declined_bytes": input.declined_bytes,
        "declined_entries": input.declined_entries,
        "legacy_control_present_at_exit": control_present,
        "generations_authored": authored,
        "generations_aborted": wl.aborted,
        "instances": tally.iter().map(|(i, t)| serde_json::json!({
            "instance": i,
            "chunks_claimed": t.claimed,
            "chunks_done": t.done,
            "lease_takeovers": t.takeovers,
            "unlinked_bytes": t.unlinked_bytes,
        })).collect::<Vec<_>>(),
        "legacy_passes": obs.passes.iter().filter(|p| !p.authored).map(|p| serde_json::json!({
            "instance_id": p.instance_id,
            "completed_at": p.completed_at,
            "bytes_freed": p.bytes_freed,
            "ranges_evicted": p.ranges_evicted,
            "duration_ms": p.duration_ms,
        })).collect::<Vec<_>>(),
        "generation_passes": finished_generations(wl).iter().map(|g| serde_json::json!({
            "generation": g.generation,
            "chunks": g.chunks,
            "authored_at": g.authored_at,
            "last_done_at": g.last_done_at,
            "unlinked_bytes": g.unlinked_bytes,
        })).collect::<Vec<_>>(),
        "window_chunk_bytes": window_chunk_bytes,
    });
    println!("EVICTION_MODE_SUMMARY {summary}");
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    let log_path =
        std::env::temp_dir().join(format!("task13-eviction-log-{}.txt", std::process::id()));
    let _log_file = init_log_capture(&log_path);
    println!("eviction log capture   : {}", log_path.display());

    if !args.cache_dir.join("metadata").is_dir() || !args.cache_dir.join("ranges").is_dir() {
        eprintln!(
            "FATAL: {} does not contain metadata/ and ranges/ — populate a fixture first",
            args.cache_dir.display()
        );
        std::process::exit(1);
    }

    println!("cache dir              : {}", args.cache_dir.display());
    let existing_bytes = sum_ranges_bytes(&args.cache_dir.join("ranges"));
    println!(
        "existing ranges/ bytes  : {} ({:.2} GiB)",
        existing_bytes,
        existing_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    let max_cache_size: u64 = match (args.max_cache_size_gib, args.fixture_start_percent) {
        (Some(gib), _) => (gib * 1024.0 * 1024.0 * 1024.0) as u64,
        (None, Some(percent)) => ((existing_bytes as f64) * 100.0 / percent) as u64,
        (None, None) => ((existing_bytes as f64) * 0.9) as u64,
    };
    println!(
        "max_cache_size          : {} ({:.2} GiB)",
        max_cache_size,
        max_cache_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "eviction trigger/target : {}% / {}%",
        args.eviction_trigger_percent, args.eviction_target_percent
    );
    println!(
        "admission threads       : {}  sizes: {:?}",
        args.admission_threads, args.admission_sizes
    );
    println!(
        "consolidation interval  : {}s",
        args.consolidation_interval_secs
    );
    println!("duration                : {}s", args.duration_secs);
    println!("eviction mode           : {:?}", args.eviction_mode);

    // Construction per the traced pattern in examples/eviction_candidate_probe.rs +
    // tests/eviction_buffer_test.rs's create_test_cache_manager, generalised to
    // new_with_shared_storage so max_cache_size/eviction_trigger_percent/
    // eviction_target_percent are configurable (the higher-level constructors
    // hardcode 10 GiB / 95 / 80 and expose none of the three).
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        args.cache_dir.clone(),
        false, // ram_cache_enabled — irrelevant to disk eviction/admission cost
        0,     // max_ram_cache_size
        max_cache_size,
        // TinyLFU, the only algorithm (R10.1 deleted LRU). Every eviction and admission
        // figure recorded from this harness before that deletion was taken under LRU, so
        // a run now is NOT comparable with that series: it is the first point of a new one
        // measured under the shipped configuration.
        CacheEvictionAlgorithm::TinyLFU,
        1024 * 1024 * 1024, // compression_threshold: effectively never compresses inside
        // this run — compression cost is orthogonal to what row 9
        // asks for and would confound the eviction/admission
        // comparison with a third variable
        false,                            // compression_enabled
        Duration::from_secs(315_360_000), // get_ttl ~10yr: nothing should expire mid-run
        Duration::from_secs(315_360_000), // head_ttl
        Duration::from_secs(315_360_000), // put_ttl
        false,                            // actively_remove_cached_data
        SharedStorageConfig::default(),
        10.0,  // write_cache_percent — unused; nothing here is write-cached
        false, // write_cache_enabled
        Duration::from_secs(86400),
        MetadataCacheConfig::default(),
        args.eviction_trigger_percent,
        args.eviction_target_percent,
        true, // read_cache_enabled
        Duration::from_secs(60),
        1_048_576,
        false,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));

    // Populates journal_consolidator / hybrid_metadata_writer / cache_hit_update_buffer
    // via the OnceLock in build_journal_components(). The returned DiskCacheManager is
    // NOT what admission uses below — a fresh one is created per store_range call, the
    // same pattern every real call site in src/ uses (see src/cache.rs:7901).
    let _ = cache_manager.create_configured_disk_cache_manager();

    // Before initialize(), so a pass initialize() itself triggers is already under the mode.
    match args.eviction_mode {
        EvictionMode::Legacy => {
            if let Err(e) = touch_legacy_control(&args.cache_dir) {
                eprintln!(
                    "FATAL: could not write the legacy control journal {}: {e}",
                    legacy_control_journal(&args.cache_dir).display()
                );
                std::process::exit(1);
            }
            println!(
                "legacy control journal  : {} (refreshed every consolidation tick)",
                legacy_control_journal(&args.cache_dir).display()
            );
        }
        EvictionMode::Worklist => {
            let path = legacy_control_journal(&args.cache_dir);
            if path.exists() {
                match std::fs::remove_file(&path) {
                    Ok(()) => println!(
                        "removed a legacy control journal an earlier legacy run left: {}",
                        path.display()
                    ),
                    Err(e) => {
                        eprintln!(
                            "FATAL: a legacy control journal is on the volume and could not be \
                             removed ({}: {e}); the gate would read NotCapable",
                            path.display()
                        );
                        std::process::exit(1);
                    }
                }
            }
        }
    }
    let me = format!(
        "{}:{}",
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
        std::process::id()
    );
    println!("instance                : {me}");
    // A process with no write-ledger append publishes its `.ready` record only on a
    // consolidation cycle where it wins the consolidation lock, which a second process can keep
    // losing for minutes. Until it publishes, its journal makes the gate read `NotCapable`: the
    // live generation is aborted when it joins and every trigger runs the legacy pass. So in
    // `worklist` mode each process installs the startup record before it writes a journal, and
    // the run starts in the state where every process has published.
    if args.eviction_mode == EvictionMode::Worklist {
        match install_startup_ready_record(&args.cache_dir, &me) {
            Ok(installed) => println!(
                "startup .ready record   : {}",
                if installed {
                    "installed"
                } else {
                    "already present"
                }
            ),
            Err(e) => {
                eprintln!("FATAL: could not install the startup .ready record: {e}");
                std::process::exit(1);
            }
        }
    }
    let volume_obs = Arc::new(Mutex::new(VolumeObs::default()));
    {
        let mut o = volume_obs.lock().unwrap();
        o.worklist.preexisting = list_generations(&args.cache_dir);
        scan_worklist(&args.cache_dir, &mut o.worklist);
        o.worklist.done_before_start = o
            .worklist
            .chunks
            .iter()
            .filter(|(_, c)| c.done.is_some())
            .map(|(k, _)| k.clone())
            .collect();
    }
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler = spawn_volume_sampler(
        args.cache_dir.clone(),
        volume_obs.clone(),
        sampler_stop.clone(),
    );

    println!("\ninitializing (scans the fixture to seed size_state.json if absent)...");
    let init_start = Instant::now();
    cache_manager
        .initialize()
        .await
        .expect("CacheManager::initialize failed");
    println!("initialize took {:.1}s", init_start.elapsed().as_secs_f64());

    // REQUIRED for maybe_trigger_eviction to ever fire. The consolidator holds only a
    // Weak<CacheManager>, set here — production wires it in http_proxy.rs right after
    // Arc-wrapping the CacheManager (`set_cache_manager_in_consolidator`). Omitting
    // this call is not a startup-ordering nicety: `maybe_trigger_eviction`'s
    // `weak.upgrade()` silently returns None and the function returns `false` on
    // every single cycle, with only a debug!-level log ("Cache manager not
    // available, skipping eviction") — invisible at this binary's info-level
    // subscriber. An earlier run of this driver spent five separate FSxZ
    // measurement windows with total_cache_size climbing steadily past the trigger
    // threshold and eviction never firing once, because of exactly this. The ONLY
    // eviction that fired in those runs came from `initialize()`'s own direct
    // `enforce_disk_cache_limits()` call, which does not go through this path at
    // all — which is what made the omission look like intermittent, threshold-
    // adjacent flakiness rather than a wiring bug.
    cache_manager.set_cache_manager_in_consolidator().await;

    // The call `HttpProxy::start` makes after the two above and before its accept loop: it takes the
    // first usage snapshot, starts the 5 s refresher and publishes the read-fill bound. Without it
    // `read_fill_refusal` finds no bound for this directory and admits every fill.
    cache_manager.start_put_usage_refresher().await;
    // Read back from the registry `read_fill_refusal` looks the bound up in, keyed on this
    // directory, rather than printing "registered" because the call above returned. A key that
    // did not match, or a registration that was skipped, leaves the soft stop inert, and the
    // summariser's flag is parsed from this line.
    match s3_proxy::cache::registered_read_fill_bound(&args.cache_dir) {
        Some(registered) => println!(
            "read-fill bound         : registered (max_cache_size {registered}, usage snapshot every {:?})",
            s3_proxy::cache::PUT_USAGE_REFRESH_INTERVAL
        ),
        None => println!(
            "read-fill bound         : NOT REGISTERED for {} (max_cache_size {max_cache_size}): every fill will be admitted and this run cannot measure the soft stop",
            args.cache_dir.display()
        ),
    }

    let consolidator = cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator not wired — SharedStorageConfig path required");

    let current_size = consolidator.get_current_size().await.unwrap_or(0);
    println!(
        "size_state.json after init: {} ({:.2} GiB) — this is what enforce_disk_cache_limits reads",
        current_size,
        current_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    if current_size <= max_cache_size {
        println!(
            "NOTE: current_size <= max_cache_size at startup. Eviction will not fire until \
             admission (or the trigger check inside enforce_disk_cache_limits, which re-reads \
             current_size) pushes it over the {}% trigger threshold.",
            args.eviction_trigger_percent
        );
    }

    // Counters. Admission and eviction each get bytes+entries; the consolidation task
    // reports nothing itself (it is plumbing, not a measured direction).
    let admitted_bytes = Arc::new(AtomicU64::new(0));
    let admitted_entries = Arc::new(AtomicU64::new(0));
    // Every attempt, and the attempts `prepare_range_store` declined (it returned `None`).
    let attempted_bytes = Arc::new(AtomicU64::new(0));
    let attempted_entries = Arc::new(AtomicU64::new(0));
    let declined_bytes = Arc::new(AtomicU64::new(0));
    let declined_entries = Arc::new(AtomicU64::new(0));
    // Eviction bytes/s and entries/s are NOT tracked via atomic counters — eviction is
    // spawned detached from inside `run_consolidation_cycle` (production's own
    // maybe_trigger_eviction), so the triggering call site never sees its result.
    // Both figures are parsed from the [DISK_CACHE_EVICTION] log lines after the run
    // (see sum_bytes_freed_from_log, sum_ranges_evicted_from_log).
    let eviction_passes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Mark the measurement window's start in the captured log, so eviction that
    // `initialize()` itself may have triggered above (a startup catch-up pass on a
    // fixture already over its ceiling) is excluded from the eviction-rate figures
    // below. An early run of this binary attributed 19.61 GiB of pre-window,
    // initialize()-triggered eviction to a 300s post-window duration, which produced
    // an eviction rate figure that was really "how fast did the fixture drain during
    // a 6-minute initialize() call", not a steady-state rate. This marker plus the
    // window-scoped log parsers are the fix.
    tracing::info!("{MEASUREMENT_WINDOW_MARKER}");
    let run_start = Instant::now();

    // --- Admission workers: one DiskCacheManager per task (store_range takes &mut
    // self), all writing under distinct cache_key prefixes so they cannot collide on
    // the same .meta and serialise on its per-object lock. ---
    // Per-run key namespace. `testing.md`'s "run-unique key" rule applies here exactly:
    // reusing a fixed prefix across invocations against the SAME fixture directory makes
    // every subsequent run overwrite the previous run's objects rather than genuinely
    // admitting new bytes. `store_range` credits only the net size delta on an overwrite
    // (near-zero when the new write is the same size as the old), so a re-run's
    // admitted_bytes counter — which counts every attempted write regardless of whether
    // it was new or an overwrite — read far higher than what the consolidator's
    // accumulator actually accumulated. Measured directly: one run counted 29.25 GiB
    // admitted by store_range call count, but the accumulator only accumulated 10.34 GiB,
    // a ~19 GiB gap matching a PRIOR run's overwritten key space almost exactly.
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("run_id (key namespace): {run_id}");
    let mut admission_handles = Vec::new();
    for worker_id in 0..args.admission_threads {
        let cache_manager = cache_manager.clone();
        let admitted_bytes = admitted_bytes.clone();
        let admitted_entries = admitted_entries.clone();
        let attempted_bytes = attempted_bytes.clone();
        let attempted_entries = attempted_entries.clone();
        let declined_bytes = declined_bytes.clone();
        let declined_entries = declined_entries.clone();
        let stop = stop.clone();
        let sizes = args.admission_sizes.clone();
        admission_handles.push(tokio::spawn(async move {
            let mut disk_cache = cache_manager.create_configured_disk_cache_manager();
            let mut counter: u64 = 0;
            let mut size_idx = 0usize;
            // Seconds a worker spends on one admitted fill, smoothed. A declined fill returns without
            // waiting on anything, so a worker that did not pace it would spin through declines and
            // starve the runtime's other tasks. Pacing a decline at the smoothed admitted-fill time
            // keeps the offered load at what the worker would deliver if every fill were stored, the way a
            // client's request rate does not depend on whether the proxy kept a copy.
            let mut fill_secs_ewma: f64 = 0.0;
            while !stop.load(Ordering::Relaxed) {
                let size = sizes[size_idx % sizes.len()];
                size_idx = size_idx.wrapping_add(1);
                let data = vec![0xABu8; size as usize]; // incompressible-ish content is
                                                        // irrelevant since compression_enabled=false above; content is inert.
                                                        // run_id makes this key genuinely new admission rather than an overwrite
                                                        // of a previous invocation's object at the same key (see the run_id
                                                        // comment above main's admission loop for the incident this fixes).
                let cache_key = format!("task13-admission/r{run_id}/w{worker_id}/obj{counter:012}");
                let object_metadata = ObjectMetadata::new(
                    format!("etag-{worker_id}-{counter}"),
                    "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                    size,
                    Some("application/octet-stream".to_string()),
                );
                attempted_bytes.fetch_add(size, Ordering::Relaxed);
                attempted_entries.fetch_add(1, Ordering::Relaxed);
                let fill_started = Instant::now();
                // `store_range` is these two calls; `None` is a declined fill.
                let prepared = disk_cache
                    .prepare_range_store(
                        &cache_key,
                        0,
                        size - 1,
                        &data,
                        object_metadata,
                        Duration::from_secs(315_360_000),
                        false,
                    )
                    .await;
                let stored = match prepared {
                    Ok(Some(fill)) => fill.finish().await.map(|()| true),
                    Ok(None) => Ok(false),
                    Err(e) => Err(e),
                };
                match stored {
                    Ok(true) => {
                        admitted_bytes.fetch_add(size, Ordering::Relaxed);
                        admitted_entries.fetch_add(1, Ordering::Relaxed);
                        let took = fill_started.elapsed().as_secs_f64();
                        fill_secs_ewma = if fill_secs_ewma == 0.0 {
                            took
                        } else {
                            0.8 * fill_secs_ewma + 0.2 * took
                        };
                    }
                    Ok(false) => {
                        declined_bytes.fetch_add(size, Ordering::Relaxed);
                        declined_entries.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_secs_f64(fill_secs_ewma.max(0.001)))
                            .await;
                    }
                    Err(e) => {
                        eprintln!("[admission w{worker_id}] range store failed: {e}");
                    }
                }
                counter += 1;
            }
        }));
    }

    // --- Consolidation task: THE SAME MECHANISM PRODUCTION USES, not a separate
    // eviction timer. `run_consolidation_cycle` flushes admitted deltas into
    // size_state.json and THEN calls `maybe_trigger_eviction` internally
    // (src/journal_consolidator.rs, after `atomic_update_size_delta`), which spawns
    // eviction as a detached task guarded by `eviction_in_progress` so a second cycle
    // arriving mid-eviction cannot double-trigger. An earlier version of this driver
    // ran eviction from its own independent timer alongside this one, which raced
    // eviction against a stale on-disk size and produced impossible bytes_freed
    // sequences in a local smoke test (two full-target evictions back to back,
    // because the second fired before the first's freed bytes had been consolidated).
    // Driving ONLY this one mechanism, exactly as `main.rs` does, is what makes this
    // measurement's coupling match production rather than an artifact of the harness.
    {
        let consolidator = consolidator.clone();
        let eviction_passes = eviction_passes.clone();
        let stop = stop.clone();
        let interval_secs = args.consolidation_interval_secs;
        let legacy_control =
            (args.eviction_mode == EvictionMode::Legacy).then(|| args.cache_dir.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
            while !stop.load(Ordering::Relaxed) {
                tick.tick().await;
                if let Some(dir) = &legacy_control {
                    if let Err(e) = touch_legacy_control(dir) {
                        eprintln!("[legacy control] journal refresh failed: {e}");
                    }
                }
                match consolidator.run_consolidation_cycle().await {
                    Ok(result) => {
                        if result.eviction_triggered {
                            eviction_passes.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        eprintln!("[consolidation] cycle failed: {e}");
                    }
                }
            }
        });
    }

    // Eviction's freed-bytes figure is not returned by `run_consolidation_cycle`
    // (`eviction_triggered` is a bool, not a byte count — the spawned eviction task
    // is detached, by design, so the triggering cycle does not wait for it). Both
    // eviction bytes/s and entries/s are therefore read from the
    // `[DISK_CACHE_EVICTION] Eviction completed: ... freed=..., ranges_evicted=...`
    // log line after the run, via sum_bytes_freed_from_log / sum_ranges_evicted_from_log.

    // --- Progress reporting, every 10s, so a long run is observable mid-flight.
    // Eviction bytes are read from the log tail-side (not an atomic), so progress
    // reports admission plus eviction PASS count only; the byte figure is a final
    // summary, not a running one, to avoid re-parsing a growing log every 10s. ---
    let progress_handle = {
        let admitted_bytes = admitted_bytes.clone();
        let eviction_passes = eviction_passes.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            let start = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                tick.tick().await;
                let elapsed = start.elapsed().as_secs_f64();
                println!(
                    "  t={elapsed:6.1}s  admitted={:>14}B  eviction_passes={}",
                    admitted_bytes.load(Ordering::Relaxed),
                    eviction_passes.load(Ordering::Relaxed)
                );
            }
        })
    };

    tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;
    let measured_secs = run_start.elapsed().as_secs_f64();
    let window_end_unix = unix_now_f64();
    stop.store(true, Ordering::Relaxed);

    // Let in-flight work settle rather than hard-aborting mid-write.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for h in admission_handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
    }
    progress_handle.abort();

    // Give any eviction task that was still mid-flight when the window ended a
    // chance to finish and write its log line before we parse it and before the
    // process exits. Eviction is spawned DETACHED (a tokio task, not a separate OS
    // process), so if this binary exits while a pass is running, that task is
    // killed mid-write along with it — an incomplete pass leaves the fixture in
    // an unknown partial state (some journal entries written, `.meta`/`.bin` files
    // possibly mid-unlink) rather than merely under-measuring. A first version of
    // this driver waited a fixed 3s and exited while an 8.86 GiB pass (started
    // near the window's end) was still writing thousands of journal entries; the
    // pass never got a chance to log completion and its outcome is unknown.
    //
    // Poll rather than sleep a fixed amount: `eviction_passes` (incremented when
    // `run_consolidation_cycle`'s result reports `eviction_triggered`) tells us
    // whether ANY pass was started in this run, and the log's own completion line
    // tells us whether the LAST one finished. Measured passes on this backend take
    // several minutes for tens of thousands of journal entries, so the cap here is
    // generous rather than a guess.
    if eviction_passes.load(Ordering::Relaxed) > 0 {
        println!("waiting for the last triggered eviction pass to complete before exiting...");
        let settle_deadline = Instant::now() + Duration::from_secs(600);
        loop {
            let content = std::fs::read_to_string(&log_path).unwrap_or_default();
            let started = content
                .matches("[DISK_CACHE_EVICTION] Starting eviction")
                .count();
            let completed = content
                .matches("[DISK_CACHE_EVICTION] Eviction completed")
                .count();
            if completed >= started {
                println!(
                    "all {started} triggered eviction pass(es) completed after {:.0}s of waiting",
                    600.0
                        - settle_deadline
                            .saturating_duration_since(Instant::now())
                            .as_secs_f64()
                );
                break;
            }
            // A pass that selects nothing logs a start and no completion, so the counts can
            // disagree for good. A running pass holds the eviction lock, so winning it means no
            // pass is in flight on the volume.
            if matches!(
                cache_manager.try_acquire_global_eviction_lock().await,
                Ok(true)
            ) {
                let _ = cache_manager.release_global_eviction_lock().await;
                println!(
                    "{completed}/{started} triggered eviction pass(es) logged completion; the \
                     eviction lock is free, so none is in flight (a pass that selects nothing \
                     logs no completion)"
                );
                break;
            }
            if Instant::now() >= settle_deadline {
                eprintln!(
                    "WARNING: {completed}/{started} eviction passes completed after a 600s wait. \
                     Exiting anyway — the fixture may be left in a partial state from the \
                     incomplete pass, and this run's eviction figures undercount it."
                );
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    // One final consolidation cycle so admitted-but-not-yet-consolidated bytes are
    // counted in the closing size_state.json read, though not in the throughput
    // figures above (which are measured directly from store_range/enforce_disk_cache_limits
    // return values, not from size_state.json deltas).
    // The cycle spawns maintenance passes, the work-list executor among them; a process that
    // exits mid-chunk stops between an unlink and its `.meta` update. Wait for them first.
    consolidator.await_maintenance_passes().await;
    let _ = consolidator.run_consolidation_cycle().await;
    consolidator.await_maintenance_passes().await;
    let final_size = consolidator.get_current_size().await.unwrap_or(0);
    sampler_stop.store(true, Ordering::Relaxed);
    let _ = sampler.join();

    // Flush the tracing writer before reading its file back.
    drop(_log_file);
    std::io::stdout().flush().ok();

    let ab = admitted_bytes.load(Ordering::Relaxed);
    let ae = admitted_entries.load(Ordering::Relaxed);
    let attempted_b = attempted_bytes.load(Ordering::Relaxed);
    let attempted_e = attempted_entries.load(Ordering::Relaxed);
    let declined_b = declined_bytes.load(Ordering::Relaxed);
    let declined_e = declined_entries.load(Ordering::Relaxed);
    let passes = eviction_passes.load(Ordering::Relaxed);
    let ee = sum_ranges_evicted_from_log(&log_path);
    let eb = sum_bytes_freed_from_log(&log_path);

    println!("\n=== RESULTS (measured window: {measured_secs:.1}s) ===");
    println!(
        "admission : {ab} bytes ({:.2} GiB), {ae} entries",
        ab as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  admission bytes/s   : {:.1} ({:.2} MiB/s)",
        ab as f64 / measured_secs,
        ab as f64 / measured_secs / (1024.0 * 1024.0)
    );
    println!("  admission entries/s : {:.1}", ae as f64 / measured_secs);
    println!(
        "attempted : {attempted_b} bytes, {attempted_e} entries; declined : {declined_b} bytes, {declined_e} entries \
         (declined share {:.2}% by bytes, {:.2}% by count)",
        100.0 * declined_b as f64 / attempted_b.max(1) as f64,
        100.0 * declined_e as f64 / attempted_e.max(1) as f64
    );
    println!(
        "eviction  : {eb} bytes ({:.2} GiB) freed, {ee} ranges evicted, in {passes} passes",
        eb as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  eviction bytes/s    : {:.1} ({:.2} MiB/s)",
        eb as f64 / measured_secs,
        eb as f64 / measured_secs / (1024.0 * 1024.0)
    );
    println!("  eviction entries/s  : {:.1}", ee as f64 / measured_secs);
    println!(
        "final size_state.json cache size: {final_size} ({:.2} GiB)",
        final_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    {
        let obs = volume_obs.lock().unwrap_or_else(|p| p.into_inner());
        print_mode_summary(&ModeSummaryInput {
            mode: args.eviction_mode,
            cache_dir: &args.cache_dir,
            me: &me,
            obs: &obs,
            window_start: run_id,
            window_end: window_end_unix,
            fill_bytes_per_sec: ab as f64 / measured_secs,
            attempted_bytes: attempted_b,
            attempted_entries: attempted_e,
            declined_bytes: declined_b,
            declined_entries: declined_e,
            max_cache_size,
            measured_secs,
        });
    }

    let mean_evicted_object_bytes = if ee > 0 { eb as f64 / ee as f64 } else { 0.0 };
    println!(
        "mean evicted range size: {:.0} bytes ({:.2} MiB)",
        mean_evicted_object_bytes,
        mean_evicted_object_bytes / (1024.0 * 1024.0)
    );

    println!("\n=== R2.2 operating limit ===");
    println!(
        "R2.2 asks for the minimum mean evicted object size at which the cache can hold at\n\
         capacity for a given ingest rate. From this run:\n\
         \x20 ingest rate (measured)     : {:.2} MiB/s ({:.1} entries/s)\n\
         \x20 eviction rate (measured)   : {:.2} MiB/s ({:.1} entries/s)\n\
         \x20 mean evicted range size    : {:.2} MiB\n\
         \x20 => at THIS admission rate, eviction {} keep pace.\n\
         \x20 Minimum mean evicted object size to match this admission rate in bytes/s:\n\
         \x20   (admission MiB/s) / (eviction entries/s) = {:.4} MiB, IF eviction entries/s\n\
         \x20   is held constant as object size varies — see the caveat below.",
        ab as f64 / measured_secs / (1024.0 * 1024.0),
        ae as f64 / measured_secs,
        eb as f64 / measured_secs / (1024.0 * 1024.0),
        ee as f64 / measured_secs,
        mean_evicted_object_bytes / (1024.0 * 1024.0),
        if eb >= ab { "DID" } else { "DID NOT" },
        if ee > 0 {
            (ab as f64 / measured_secs / (1024.0 * 1024.0)) / (ee as f64 / measured_secs)
        } else {
            f64::NAN
        }
    );
    println!(
        "\nCAVEATS:\n\
         1. Eviction ENTRIES/s is a fixed-cost figure per range regardless of that range's\n\
         \x20  size (flock + .meta rewrite/unlink dominate, per requirements.md's per-object\n\
         \x20  cost table), so the minimum-mean-object-size derivation above holds only near\n\
         \x20  this run's admission size mix. Re-run with --admission-sizes matching the\n\
         \x20  deployment's real distribution before publishing a limit for a different mix.\n\
         2. This binary drives the current eviction code (no directory affinity, no\n\
         \x20  whole-object preference), so it does not measure either of those.\n\
         3. Single admission-key-space run. entries/s for BOTH directions came from this\n\
         \x20  process's own counters (admission: exact; eviction: parsed from the\n\
         \x20  [DISK_CACHE_EVICTION] log line at {}).",
        log_path.display()
    );
}
