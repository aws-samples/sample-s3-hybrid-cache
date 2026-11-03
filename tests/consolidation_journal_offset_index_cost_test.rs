//! A consolidation cycle reads about one journal's worth of bytes per pass over it, not one
//! journal per key.
//!
//! PR #23 made each key's read stream its journal files a line at a time, but each key still
//! scanned every file it appeared in, so a cycle over one large journal read keys x journal
//! bytes. With the offset index each key reads only its own recorded lines, so the cycle reads
//! the journal once in discovery and once more, in pieces, across the keys.
//! A second arm applies half the journal's lines, so its figure includes cleanup's two
//! passes over the journal.
//!
//! The observable is `rchar` in `/proc/self/io`: bytes the process's `read`/`pread` calls
//! returned, deterministic and independent of the page cache. It is process-wide, as is the
//! counting allocator that records the cycle's peak heap (the index included), so this binary
//! holds one test. Linux only.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use s3_proxy::cache_types::{ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Counting allocator (pattern from `tests/buffered_serve_memory_bound_test.rs`)
// ---------------------------------------------------------------------------

struct CountingAlloc;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn note_alloc(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every method delegates to `System` with the caller's layout and only
// adjusts two atomics on success.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            note_alloc(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            note_alloc(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            note_alloc(new_size);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const INSTANCE: &str = "journal-offset-index-cost-test-instance";
/// More keys than `KEY_CONCURRENCY_LIMIT` (32), interleaved line by line, so every key's lines
/// are spread across the whole journal.
const KEYS: usize = 256;
/// Journal size the fixture writes up to.
const JOURNAL_BYTES: usize = 16 * 1024 * 1024;
/// The busy arms' key count, `KEY_CONCURRENCY_LIMIT` (32): every key's read can be in flight at
/// once. Each key has a line in every 32, so in the dense arm a key's lines are about 19 KB
/// apart, inside `SPAN_BATCH_GAP` (64 KiB), and its read batches them up to `SPAN_BATCH_MAX`.
const BUSY_KEYS: usize = 32;
/// The busy arms' entry bytes: 8 MiB, so each busy key's lines span 8 MiB of journal.
const BUSY_JOURNAL_BYTES: usize = 8 * 1024 * 1024;
/// A line that is not a journal entry, written after each round of the sparse busy arm so that
/// no two of a key's lines are within `SPAN_BATCH_GAP` and every positioned read is one line.
const PAD_BYTES: usize = 64 * 1024;
/// `journal_manager::SPAN_BATCH_MAX` (private there): the longest one batched read.
const SPAN_BATCH_MAX: usize = 1024 * 1024;

/// A range-fill `Add` of `cache_key` at `start`. With a fresh timestamp and no range file it is
/// neither valid nor stale, so the cycle reads it and keeps it pending; with its range file
/// present (`create_range_file`) the cycle applies it and cleanup removes its line.
fn fixture_add(cache_key: &str, start: u64) -> JournalEntry {
    let now = SystemTime::now();
    let file_path = format!("/nonexistent/{cache_key}_{start}.bin");
    JournalEntry {
        timestamp: now,
        instance_id: INSTANCE.to_string(),
        cache_key: cache_key.to_string(),
        range_spec: RangeSpec {
            start,
            end: start + 99,
            file_path: file_path.clone(),
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
        range_file_path: file_path,
        metadata_version: 1,
        new_ttl_secs: None,
        object_ttl_secs: Some(3600),
        access_increment: None,
        object_metadata: Some(ObjectMetadata::default()),
        fill_witness: None,
    }
}

/// The range file the consolidator checks for `cache_key`'s range at `start`
/// (`get_range_file_path`): its existence is what makes the entry valid. Returns its identity,
/// which the entry must record to be folded (R12.1).
fn create_range_file(
    cache_dir: &Path,
    cache_key: &str,
    start: u64,
) -> Option<s3_proxy::cache_types::RangeFileIdentity> {
    let path = s3_proxy::disk_cache::get_sharded_path(
        &cache_dir.join("ranges"),
        cache_key,
        &format!("_{}-{}.bin", start, start + 99),
    )
    .unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, [0u8; 100]).unwrap();
    s3_proxy::cache_types::RangeFileIdentity::of(&std::fs::metadata(&path).unwrap())
}

fn journal_path(cache_dir: &Path) -> std::path::PathBuf {
    cache_dir
        .join("metadata")
        .join("_journals")
        .join(format!("{INSTANCE}.journal"))
}

/// Write `KEYS` keys' entries, interleaved, into this instance's journal until it holds at
/// least `JOURNAL_BYTES`, creating the range file of every entry of an even-numbered key when
/// `apply_even_keys`. Returns the journal's size in bytes and its line count.
fn seed_journal(cache_dir: &Path, apply_even_keys: bool) -> (usize, usize) {
    let path = journal_path(cache_dir);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut out = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
    let (mut written, mut lines) = (0usize, 0usize);
    let mut round = 0u64;
    while written < JOURNAL_BYTES {
        for k in 0..KEYS {
            let key = format!("fixture-bucket/object-{k:04}");
            let start = round * 100;
            let mut entry = fixture_add(&key, start);
            if apply_even_keys && k % 2 == 0 {
                entry.range_spec.file_identity = create_range_file(cache_dir, &key, start);
            }
            let line = serde_json::to_string(&entry).unwrap();
            writeln!(out, "{line}").unwrap();
            written += line.len() + 1;
            lines += 1;
        }
        round += 1;
    }
    out.flush().unwrap();
    (written, lines)
}

/// Write `BUSY_KEYS` keys' pending entries, interleaved, until they hold `BUSY_JOURNAL_BYTES`;
/// with `pad`, a `PAD_BYTES` line that is no entry follows each round. Both layouts hold the same
/// entries, so the same index and the same entries in flight: only the batch buffers differ.
/// Returns the journal's size in bytes and its line count.
fn seed_busy_journal(cache_dir: &Path, pad: bool) -> (usize, usize) {
    let path = journal_path(cache_dir);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut out = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
    let (mut entry_bytes, mut written, mut lines) = (0usize, 0usize, 0usize);
    let pad_line = format!("{{\"pad\":\"{}\"}}", "x".repeat(PAD_BYTES - 11));
    let mut round = 0u64;
    while entry_bytes < BUSY_JOURNAL_BYTES {
        for k in 0..BUSY_KEYS {
            let key = format!("busy-bucket/object-{k:04}");
            let line = serde_json::to_string(&fixture_add(&key, round * 100)).unwrap();
            writeln!(out, "{line}").unwrap();
            entry_bytes += line.len() + 1;
            written += line.len() + 1;
            lines += 1;
        }
        if pad {
            writeln!(out, "{pad_line}").unwrap();
            written += pad_line.len() + 1;
            lines += 1;
        }
        round += 1;
    }
    out.flush().unwrap();
    (written, lines)
}

/// `rchar` from `/proc/self/io`: bytes returned by this process's read system calls.
fn bytes_read_by_process() -> u64 {
    let io = std::fs::read_to_string("/proc/self/io").expect("/proc/self/io (Linux only)");
    io.lines()
        .find_map(|l| l.strip_prefix("rchar: "))
        .and_then(|v| v.trim().parse().ok())
        .expect("rchar line in /proc/self/io")
}

/// What one arm's cycle measured.
struct Arm {
    journal_bytes: usize,
    journal_lines: usize,
    journal_after: u64,
    read: u64,
    keys_run: usize,
    entries_consolidated: usize,
    /// The cycle's peak heap above the heap live when it started, in bytes.
    peak_growth: usize,
    /// The most span reads that ran at once during the cycle.
    peak_span_reads: u64,
}

/// Seed a fresh cache directory with `seed`, then run and measure one consolidation cycle over
/// it. `keys` is the key count `seed` writes.
async fn run_arm(name: &str, seed: impl FnOnce(&Path) -> (usize, usize), keys: usize) -> Arm {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let (journal_bytes, journal_lines) = seed(dir);

    let journal_manager = Arc::new(JournalManager::new(dir.to_path_buf(), INSTANCE.to_string()));
    let lock_manager = Arc::new(MetadataLockManager::new(
        dir.to_path_buf(),
        Duration::from_secs(5),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        dir.to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig {
            interval: Duration::from_secs(5),
            // 0 disables eviction, so no detached eviction pass reads during the cycle.
            max_cache_size: 0,
            consolidation_cycle_timeout: Duration::from_secs(120),
            max_keys_per_cycle: 5000,
            ..ConsolidationConfig::default()
        },
    ));
    consolidator.initialize().await.unwrap();
    consolidator.await_maintenance_passes().await;

    let read_before = bytes_read_by_process();
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    s3_proxy::journal_manager::reset_span_reads_peak();
    let started = Instant::now();
    let result = consolidator.run_consolidation_cycle().await.unwrap();
    let elapsed = started.elapsed();
    let growth = PEAK.load(Ordering::Relaxed).saturating_sub(base);
    let peak_span_reads = s3_proxy::journal_manager::span_reads_peak();
    let read = bytes_read_by_process() - read_before;
    consolidator.await_maintenance_passes().await;
    let journal_after = std::fs::metadata(journal_path(dir)).map_or(0, |m| m.len());

    println!(
        "{name}: journal {} bytes, {} lines, {} keys: cycle {:?}, keys_processed={}, \
         keys_skipped={}, entries_consolidated={}, journal after {} bytes, bytes read {} \
         ({:.2} x journal), peak heap growth {} KiB ({:.1} bytes per line)",
        journal_bytes,
        journal_lines,
        keys,
        elapsed,
        result.keys_processed,
        result.keys_skipped,
        result.entries_consolidated,
        journal_after,
        read,
        read as f64 / journal_bytes as f64,
        growth >> 10,
        growth as f64 / journal_lines as f64
    );
    Arm {
        journal_bytes,
        journal_lines,
        journal_after,
        read,
        keys_run: result.keys_processed + result.keys_skipped,
        entries_consolidated: result.entries_consolidated,
        peak_growth: growth,
        peak_span_reads,
    }
}

/// One consolidation cycle over a 16 MiB journal of 256 interleaved keys reads under 4 x the
/// journal when nothing is applied, and under 5 x when half the keys are applied and cleanup
/// rewrites the journal.
///
/// # What is asserted, and why
///
/// - **Every key was read**, in both arms: `keys_processed + keys_skipped` equals `KEYS`, so
///   every key's read is inside the measured window.
/// - **Pending arm: bytes read are under 4 x the journal.** Every entry stays pending, so
///   cleanup removes nothing. Discovery reads the journal once and the per-key reads read each
///   line once between them, so about 2 x is inherent (plus `/proc/self/io` itself and the
///   cycle's small state files). Scanning every file per key reads about `KEYS` x.
/// - **Applied arm: cleanup ran, and bytes read are under 5 x the journal.** Every entry of
///   the even-numbered keys has its range file, so the cycle applies half the lines and the
///   two-pass cleanup runs: pass 1 reads the journal to find the kept lines and pass 2 copies
///   them, about 1.5 x on top of the 2 x above, plus the `.meta` files the applied keys read.
///   The journal must end shorter than it started and not empty, so both passes ran.
///
/// The cycle's peak heap growth is printed, not asserted (`consolidation_journal_read_memory_test`
/// bounds it); here it is the offset index's footprint at this scale plus the entries in flight.
///
/// - **Busy arms: the span batch buffers stay under 2 x `SPAN_BATCH_MAX` per key**.
///   `BUSY_KEYS` keys, every key's read in flight at once, over 8 MiB of entries: the
///   dense arm's reads batch a key's lines into reads of up to 1 MiB, and the sparse arm, the same
///   entries with a 64 KiB line that is no entry after each round, reads one line at a time. The
///   index and the entries in flight are the same in both, so the difference between the two
///   cycles' peak heap growth is the batch buffers'. A key's read reuses one buffer, and `Vec`
///   growth keeps it under twice the longest read, so the bound is 64 MiB at 32 keys. These
///   buffers are not counted by `max_inflight_buffer_bytes`.
///
/// # RED SIDE
///
/// With PR #23 alone (the per-key read is `get_entries_from_files` over the key's file list)
/// the pending arm's bound fails at about 257 x the journal. With no `SPAN_BATCH_MAX` rule a
/// dense key's buffer grows to its whole 8 MiB span, and the busy bound fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cycle_reads_about_one_journal_not_one_per_key() {
    let pending = run_arm("pending", |d| seed_journal(d, false), KEYS).await;
    let applied = run_arm("applied", |d| seed_journal(d, true), KEYS).await;
    let dense = run_arm("busy dense", |d| seed_busy_journal(d, false), BUSY_KEYS).await;
    let sparse = run_arm("busy sparse", |d| seed_busy_journal(d, true), BUSY_KEYS).await;

    for (name, arm) in [("pending", &pending), ("applied", &applied)] {
        assert_eq!(
            arm.keys_run, KEYS,
            "fixture precondition ({name}): the cycle must run the per-key read for every key"
        );
    }
    for (name, arm) in [("busy dense", &dense), ("busy sparse", &sparse)] {
        assert_eq!(
            arm.keys_run, BUSY_KEYS,
            "fixture precondition ({name}): the cycle must run the per-key read for every key"
        );
    }
    assert_eq!(
        pending.journal_after, pending.journal_bytes as u64,
        "fixture precondition (pending): nothing is applied, so cleanup leaves the journal whole"
    );
    assert!(
        applied.entries_consolidated > 0
            && applied.journal_after > 0
            && applied.journal_after < applied.journal_bytes as u64,
        "fixture precondition (applied): half the lines applied, so cleanup must rewrite the \
         journal to its kept half: {} entries consolidated of {} lines, journal {} -> {} bytes",
        applied.entries_consolidated,
        applied.journal_lines,
        applied.journal_bytes,
        applied.journal_after
    );
    assert!(
        pending.read < 4 * pending.journal_bytes as u64,
        "one cycle read {} bytes for a {} byte journal ({:.1} x): each key scans the whole \
         journal rather than its own lines",
        pending.read,
        pending.journal_bytes,
        pending.read as f64 / pending.journal_bytes as f64
    );
    assert!(
        applied.read < 5 * applied.journal_bytes as u64,
        "one cycle with cleanup read {} bytes for a {} byte journal ({:.1} x)",
        applied.read,
        applied.journal_bytes,
        applied.read as f64 / applied.journal_bytes as f64
    );
    let buffers = dense.peak_growth.saturating_sub(sparse.peak_growth);
    let bound = BUSY_KEYS * 2 * SPAN_BATCH_MAX;
    println!(
        "span batch buffers at {} keys: dense peak growth {} KiB, sparse {} KiB, difference {} KiB \
         ({:.2} MiB per key), bound {} KiB",
        BUSY_KEYS,
        dense.peak_growth >> 10,
        sparse.peak_growth >> 10,
        buffers >> 10,
        buffers as f64 / BUSY_KEYS as f64 / (1024.0 * 1024.0),
        bound >> 10
    );
    println!(
        "span reads in flight at {} busy keys: dense peak {}, sparse peak {}",
        BUSY_KEYS, dense.peak_span_reads, sparse.peak_span_reads
    );
    // The bound above assumes up to 32 (KEY_CONCURRENCY_LIMIT) reads at once; the buffers it
    // measures are only the figure at that concurrency if several really overlapped.
    assert!(
        (2..=32).contains(&dense.peak_span_reads),
        "THE DENSE ARM RAN {} SPAN READS AT ONCE AT PEAK, OUTSIDE 2..=32, SO THE \
         BUFFER BOUND IS NOT THE FIGURE AT KEY_CONCURRENCY_LIMIT",
        dense.peak_span_reads
    );
    assert!(
        buffers <= bound,
        "THE SPAN BATCH BUFFERS HELD {} BYTES AT {} KEYS, OVER {} (2 x SPAN_BATCH_MAX \
         PER KEY)",
        buffers,
        BUSY_KEYS,
        bound
    );
}
