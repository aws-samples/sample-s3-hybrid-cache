//! A consolidation cycle's heap stays proportional to the journal, not to the journal times
//! the key concurrency.
//!
//! Each key a cycle consolidates reads its entries from the journal files discovery listed for
//! it, under the key's lock. That read loaded each whole file into one `String` and parsed every
//! line to keep the key's own, and the cycle runs `KEY_CONCURRENCY_LIMIT` (32) keys at once, so
//! a cycle held up to 32 copies of the journal. Once a journal has fallen behind, that is the
//! process's largest allocation by far: a 723 MiB journal held about 23 GiB on a 32 GiB host,
//! none of it counted by `max_inflight_buffer_bytes`, and the proxy was killed for memory.
//!
//! The test measures the peak heap growth across one cycle with a counting global allocator
//! (pattern from `tests/buffered_serve_memory_bound_test.rs`). The allocator counters are
//! process-global, so this binary holds one test.

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
// Counting allocator
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

const INSTANCE: &str = "journal-read-memory-test-instance";
/// More keys than `KEY_CONCURRENCY_LIMIT`, so the cycle runs a full 32 at once.
const KEYS: usize = 256;
/// Journal size the fixture writes up to.
const JOURNAL_BYTES: usize = 32 * 1024 * 1024;

/// A range-fill `Add` whose range file does not exist. With a fresh timestamp it is neither
/// valid nor stale, so the cycle reads it and keeps it pending: the cycle does the per-key
/// journal reads and nothing else that allocates in proportion to the journal.
fn pending_add(cache_key: &str, start: u64) -> JournalEntry {
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

/// Write `KEYS` keys' entries, interleaved, into this instance's journal until it holds at
/// least `JOURNAL_BYTES`. Returns the journal's size in bytes.
fn seed_journal(cache_dir: &Path) -> usize {
    let journals = cache_dir.join("metadata").join("_journals");
    std::fs::create_dir_all(&journals).unwrap();
    let path = journals.join(format!("{INSTANCE}.journal"));
    let mut out = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
    let mut written = 0usize;
    let mut round = 0u64;
    while written < JOURNAL_BYTES {
        for k in 0..KEYS {
            let key = format!("fixture-bucket/object-{k:04}");
            let line = serde_json::to_string(&pending_add(&key, round * 100)).unwrap();
            writeln!(out, "{line}").unwrap();
            written += line.len() + 1;
        }
        round += 1;
    }
    out.flush().unwrap();
    written
}

/// One consolidation cycle over a 32 MiB journal of 256 keys grows the heap by a small multiple
/// of the journal, not by `KEY_CONCURRENCY_LIMIT` multiples of it.
///
/// # What is asserted, and why
///
/// - **Every key was read.** `keys_processed + keys_skipped` equals `KEYS`, so the cycle ran the
///   per-key path for every key and the peak below covers it, rather than passing because the
///   cycle stopped early.
/// - **Peak heap growth is under 4 × the journal.** Discovery reads each journal file once, so
///   about 1 × is inherent to this cycle. Reading the whole file per key held up to 32 ×.
///
/// # RED SIDE
///
/// On v3.0.0, `JournalManager::get_entries_from_files` reads the whole file per key and the
/// second assertion fails with a peak growth of roughly 20-32 × the journal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cycle_does_not_hold_one_journal_copy_per_concurrent_key() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let journal_bytes = seed_journal(dir);

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
            // 0 disables eviction, so no detached eviction pass allocates during the cycle.
            max_cache_size: 0,
            // Long enough that no key times out on a slow test host: the assertion needs every
            // key's read inside the measured window.
            consolidation_cycle_timeout: Duration::from_secs(60),
            max_keys_per_cycle: 5000,
            ..ConsolidationConfig::default()
        },
    ));
    consolidator.initialize().await.unwrap();
    // Let the maintenance passes `initialize` and the first cycle spawn finish against an empty
    // cache before measuring, so their allocations are not counted against the journal read.
    consolidator.await_maintenance_passes().await;

    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let started = Instant::now();
    let result = consolidator.run_consolidation_cycle().await.unwrap();
    let elapsed = started.elapsed();
    let growth = PEAK.load(Ordering::Relaxed).saturating_sub(base);
    consolidator.await_maintenance_passes().await;

    println!(
        "journal {} MiB, {} keys: cycle {:?}, keys_processed={}, keys_skipped={}, peak heap \
         growth {} MiB ({:.1} x journal)",
        journal_bytes >> 20,
        KEYS,
        elapsed,
        result.keys_processed,
        result.keys_skipped,
        growth >> 20,
        growth as f64 / journal_bytes as f64
    );

    assert_eq!(
        result.keys_processed + result.keys_skipped,
        KEYS,
        "fixture precondition: the cycle must run the per-key read for every key"
    );
    assert!(
        growth < 4 * journal_bytes,
        "one cycle grew the heap by {} MiB for a {} MiB journal ({:.1} x): the per-key reads \
         hold whole copies of the journal at once",
        growth >> 20,
        journal_bytes >> 20,
        growth as f64 / journal_bytes as f64
    );
}
