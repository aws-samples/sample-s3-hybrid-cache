//! What one pending-journal lookup costs the process, against a journal that has fallen behind.
//!
//! A cache lookup whose range no `.meta` lists yet (a fill or upload the consolidator has not
//! folded) falls back to the journals: `DiskCacheManager::find_pending_journal_ranges` and its
//! siblings read every journal file for the key's entries. Each read loaded the whole file into
//! one `String` and parsed every line as JSON, in the request's own task, so a lookup held a
//! copy of the journal and occupied a runtime worker thread for the whole parse. Once uploads
//! outran folding, every such lookup in the process paid that, and the requests sharing the
//! worker waited. The test measures both on one lookup: peak heap growth with a counting
//! allocator (pattern from `tests/buffered_serve_memory_bound_test.rs`), and the longest gap a
//! 1 ms ticker on the same, single worker thread sees while the lookup runs.
//!
//! The allocator counters are process-global, so this binary holds one test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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

const INSTANCE: &str = "pending-lookup-cost-instance";
const KEYS: usize = 256;
const JOURNAL_BYTES: usize = 32 * 1024 * 1024;
const TARGET: &str = "lookup-bucket/object-0042";

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
/// over a journal of `KEYS` keys' entries, interleaved, of at least `JOURNAL_BYTES`. Returns the
/// manager, the journal size, and the cache directory.
async fn manager_over_large_journal() -> (DiskCacheManager, usize, TempDir) {
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
    while written < JOURNAL_BYTES {
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

/// One pending-journal lookup over a 32 MiB journal grows the heap by a small fraction of the
/// journal and leaves its worker thread free for other tasks.
///
/// # What is asserted, and why
///
/// - **The lookup finds the key's pending `Add`** at offset 0, so it read the journal rather
///   than returning early, and the measurements below cover a real scan.
/// - **Peak heap growth is under a quarter of the journal.** Loading the file whole is at
///   least one journal by itself.
/// - **A ticker on the same single worker thread never waits more than 20 ms.** Parsing the
///   journal in the lookup's task holds the worker for the whole parse.
///
/// # RED SIDE
///
/// On 3.0.0 (and on the branch of #23, which changed only the consolidator's reads) the lookup
/// reads the file with `read_to_string` and parses every line in its own task: the heap grows by
/// more than the journal and the ticker waits for the parse.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_pending_journal_lookup_holds_neither_the_journal_nor_its_worker() {
    let (manager, journal_bytes, _dir) = manager_over_large_journal().await;

    // A task sharing the lookup's only worker thread, recording the longest gap between ticks.
    let stop = Arc::new(AtomicBool::new(false));
    let longest_gap_us = Arc::new(AtomicU64::new(0));
    let ticker = {
        let stop = Arc::clone(&stop);
        let longest = Arc::clone(&longest_gap_us);
        tokio::spawn(async move {
            let mut last = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let now = Instant::now();
                longest.fetch_max((now - last).as_micros() as u64, Ordering::Relaxed);
                last = now;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    longest_gap_us.store(0, Ordering::Relaxed);

    // Spawned, so the lookup runs on the runtime's one worker thread beside the ticker: the
    // test body itself runs on the test's own thread, outside the runtime's workers.
    let (found, elapsed, growth) = tokio::spawn(async move {
        let base = LIVE.load(Ordering::Relaxed);
        PEAK.store(base, Ordering::Relaxed);
        let started = Instant::now();
        let found = manager
            .find_pending_journal_ranges(TARGET, 0, 99)
            .await
            .expect("journal lookup");
        let elapsed = started.elapsed();
        let growth = PEAK.load(Ordering::Relaxed).saturating_sub(base);
        (found, elapsed, growth)
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    stop.store(true, Ordering::Relaxed);
    ticker.await.unwrap();
    let longest_gap = Duration::from_micros(longest_gap_us.load(Ordering::Relaxed));

    println!(
        "journal {} MiB, {KEYS} keys: lookup {elapsed:?}, peak heap growth {} KiB ({:.2} x \
         journal), longest ticker gap {longest_gap:?}",
        journal_bytes >> 20,
        growth >> 10,
        growth as f64 / journal_bytes as f64
    );

    assert!(
        found.iter().any(|spec| spec.start == 0 && spec.end == 99),
        "the lookup must find {TARGET}'s pending Add at 0-99, got {found:?}"
    );
    assert!(
        growth < journal_bytes / 4,
        "one lookup grew the heap by {} KiB for a {} MiB journal",
        growth >> 10,
        journal_bytes >> 20
    );
    assert!(
        longest_gap < Duration::from_millis(20),
        "a task on the lookup's worker thread waited {longest_gap:?} while the lookup ran"
    );
}
