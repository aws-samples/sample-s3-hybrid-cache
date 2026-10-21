//! The danger-band capacity guard, driven to a **real** disk boundary.
//!
//! Spec: `cache-eviction-at-scale` task 36 (A5). `capacity_guard_publication_boundaries_test.rs`
//! forces a decline by raising the floor above any volume's free space. That moves the
//! threshold to meet the data, so it tests the comparison and nothing that feeds it. This file
//! does the opposite: the floor is fixed before the first write, and the data fills a small
//! fixed-size filesystem until the real free-space reading reaches it. That exercises the
//! comparison, the free-space reading feeding it, and the behaviour on the far side.
//!
//! # Why it is ignored, and how to run it
//!
//! It needs a small filesystem of its own. A shared or network volume either reports a
//! near-infinite size or is shared with other writers, and a temp directory on the build
//! host's root filesystem would mean filling that filesystem. So the test takes its cache
//! directory from `S3_PROXY_REAL_FLOOR_DIR` and skips with a message when that is unset. The
//! intended target is a freshly made ext4 loopback image of about 256 MiB, mounted and owned
//! by the user running the test:
//!
//! ```text
//! S3_PROXY_REAL_FLOOR_DIR=/path/to/mnt cargo test --release \
//!     --test capacity_guard_real_floor_test -- --include-ignored --exact \
//!     guard_refuses_at_the_real_floor_of_a_small_filesystem --nocapture
//! ```
//!
//! # The floor: a fixed fraction of the volume, not the default
//!
//! `cache.disk_safety_free_space_floor` defaults to 1 GiB, which is larger than the image, so
//! the default would decline the very first write and test nothing. The floor used here is
//! one eighth of the volume's total size, read once before any write. One eighth makes the
//! danger band (`DANGER_BAND_FLOOR_MULTIPLIER` x floor, half the volume) start well after
//! the first write, so the run crosses from "outside the band" into the band on real data and
//! then reaches the floor itself.
//!
//! # Pacing, and why it is part of the contract rather than a fudge
//!
//! Outside the band the guard admits against a cached reading up to
//! `FREE_SPACE_READING_MAX_AGE` (5 s) old. `capacity_guard.rs` documents the sizing rule that
//! makes this safe: `floor >= (5 s x fill rate + largest body) / 4`. A writer faster than that
//! can transit the band between refreshes, and that is a stated property of the design, not
//! a defect. So the writer here is paced, and the test asserts the pace satisfies the rule
//! before it relies on it.

use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::capacity_guard::{
    guard_for, DANGER_BAND_FLOOR_MULTIPLIER, FREE_SPACE_READING_MAX_AGE,
};
use s3_proxy::disk_cache::DiskCacheManager;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The environment variable naming the small filesystem's cache directory.
const DIR_VAR: &str = "S3_PROXY_REAL_FLOOR_DIR";

/// Bytes per cache entry. 1 MiB, incompressible, written with compression off, so the on-disk
/// `.bin` is the body plus a small LZ4 store-mode framing overhead.
const ENTRY_BYTES: u64 = 1024 * 1024;

/// Delay between writes. 50 ms per MiB is 20 MiB/s.
const WRITE_PACE: Duration = Duration::from_millis(50);

/// Writes after the first refusal, all of which must also be refused.
const POST_REFUSAL_ATTEMPTS: usize = 5;

/// Entries deleted to free space before asserting admission resumes.
const ENTRIES_TO_DELETE: usize = 16;

fn key(i: usize) -> String {
    format!("real-floor-bucket/entry-{i:05}")
}

/// Deterministic incompressible bytes for entry `i` (xorshift64), so a read-back can be
/// compared without holding every body in memory.
fn body(i: usize) -> Vec<u8> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ ((i as u64 + 1) * 0x2545_F491_4F6C_DD1D);
    let mut out = Vec::with_capacity(ENTRY_BYTES as usize);
    while out.len() < ENTRY_BYTES as usize {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(ENTRY_BYTES as usize);
    out
}

fn metadata() -> ObjectMetadata {
    ObjectMetadata::new(
        "real-floor-etag".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        ENTRY_BYTES,
        Some("application/octet-stream".to_string()),
    )
}

fn available(dir: &Path) -> u64 {
    fs4::available_space(dir).expect("statvfs on the small filesystem")
}

/// `df -B1` for the record. Printed, never asserted on: the assertions read `statvfs`
/// directly, which is the figure the guard reads.
fn df(dir: &Path, label: &str) {
    let out = std::process::Command::new("df")
        .arg("-B1")
        .arg(dir)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        .unwrap_or_else(|e| format!("df failed: {e}"));
    println!("DF {label}:\n{out}");
}

/// Every file under `dir`, recursively.
fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// No `.tmp` survivor anywhere, and every published JSON document (`.meta`, `.resv`, other
/// `.json`) parses. Returns the number of documents checked, so a caller can assert it is not
/// vacuous.
fn assert_no_partial_files(cache_dir: &Path, stage: &str) -> usize {
    let mut files = Vec::new();
    walk(cache_dir, &mut files);
    let mut documents = 0;
    for path in &files {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            !(name.ends_with(".tmp") || name.contains(".tmp.")),
            "{stage}: a temp file survived: {}",
            path.display()
        );
        let ext = path.extension().map(|e| e.to_string_lossy().into_owned());
        if matches!(ext.as_deref(), Some("meta" | "resv" | "json")) {
            let text = std::fs::read_to_string(path)
                .unwrap_or_else(|e| panic!("{stage}: unreadable {}: {e}", path.display()));
            serde_json::from_str::<serde_json::Value>(&text).unwrap_or_else(|e| {
                panic!(
                    "{stage}: published document does not parse ({e}): {} ({} bytes)",
                    path.display(),
                    text.len()
                )
            });
            documents += 1;
        }
    }
    documents
}

/// Read entry `i` back through `get_metadata` + `load_range_data`, and check the `.bin` on
/// disk is exactly the size the `.meta` says it is (a truncated `.bin` fails here).
async fn assert_readable(m: &DiskCacheManager, cache_dir: &Path, i: usize) {
    let k = key(i);
    let meta = m
        .get_metadata(&k)
        .await
        .unwrap_or_else(|e| panic!("get_metadata({k}) errored: {e}"))
        .unwrap_or_else(|| panic!("admitted entry {k} has no metadata"));
    assert_eq!(meta.ranges.len(), 1, "{k}: expected exactly one range");
    let spec = &meta.ranges[0];
    let bin = cache_dir.join("ranges").join(&spec.file_path);
    let on_disk = std::fs::metadata(&bin)
        .unwrap_or_else(|e| panic!("{k}: .bin {} missing: {e}", bin.display()))
        .len();
    assert_eq!(
        on_disk, spec.compressed_size,
        "{k}: .bin is {on_disk} bytes but the .meta records {}",
        spec.compressed_size
    );
    let data = m
        .load_range_data(spec)
        .await
        .unwrap_or_else(|e| panic!("load_range_data({k}) errored: {e}"));
    assert!(
        data == body(i),
        "{k}: read-back bytes differ from what was written"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a small dedicated filesystem; set S3_PROXY_REAL_FLOOR_DIR (see module doc)"]
async fn guard_refuses_at_the_real_floor_of_a_small_filesystem() {
    let Some(dir) = std::env::var_os(DIR_VAR) else {
        println!(
            "SKIP: {DIR_VAR} is unset. This test fills a filesystem to its free-space floor \
             and must be pointed at a small dedicated one (see the module doc)."
        );
        return;
    };
    let mount = PathBuf::from(dir);
    let cache_dir = mount.join("cache");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir on the small filesystem");

    let total = fs4::total_space(&cache_dir).expect("statvfs total");
    let start_available = available(&cache_dir);
    assert!(
        total <= 1024 * 1024 * 1024,
        "{DIR_VAR} must be a small dedicated filesystem; this one is {total} bytes"
    );

    // The floor is fixed here, before any data moves, and never changes.
    let floor = total / 8;
    let band = DANGER_BAND_FLOOR_MULTIPLIER * floor;
    assert!(
        start_available > band + ENTRY_BYTES,
        "the run must start outside the danger band: available {start_available}, band {band}"
    );

    // The sizing rule from `DANGER_BAND_FLOOR_MULTIPLIER`'s doc, checked for this writer.
    let rate_bytes_per_sec = ENTRY_BYTES as f64 / WRITE_PACE.as_secs_f64();
    let exposure =
        FREE_SPACE_READING_MAX_AGE.as_secs_f64() * rate_bytes_per_sec + ENTRY_BYTES as f64;
    assert!(
        (band as f64) >= exposure,
        "pacing violates the documented sizing rule: band {band} < exposure {exposure}"
    );
    println!(
        "FIXTURE total={total} start_available={start_available} floor={floor} band={band} \
         entry={ENTRY_BYTES} pace={WRITE_PACE:?} exposure={exposure:.0}"
    );
    df(&mount, "start");

    let mut m = DiskCacheManager::new(cache_dir.clone(), false, 1024, false, 1_048_576);
    m.set_disk_safety_free_space_floor(floor);
    m.initialize().await.expect("initialize");
    // The process-wide guard the store path consults: same directory, same floor.
    let guard = guard_for(&cache_dir, floor);
    assert_eq!(guard.declines(), 0);

    // ---- Phase 1: fill through the normal admission path until the guard refuses. ----
    // Bounded at twice the volume's worth of entries so a guard that never refuses ends in
    // an assertion rather than an infinite loop.
    let max_writes = (2 * total / ENTRY_BYTES) as usize;
    let mut admitted: Vec<usize> = Vec::new();
    let mut refused_at: Option<(usize, u64)> = None;
    let fill_start = Instant::now();
    for i in 0..max_writes {
        let before = available(&cache_dir);
        let declines_before = guard.declines();
        let result = m
            .store_range(
                &key(i),
                0,
                ENTRY_BYTES - 1,
                &body(i),
                metadata(),
                Duration::from_secs(3600),
                false,
            )
            .await;
        if let Err(e) = result {
            df(&mount, "error");
            panic!(
                "store_range #{i} returned an error before the guard refused \
                 (available before it: {before}, floor {floor}): {e}"
            );
        }
        let bin_exists = m
            .get_new_range_file_path(&key(i), 0, ENTRY_BYTES - 1)
            .exists();
        let declined = guard.declines() > declines_before;
        assert!(
            bin_exists != declined,
            "write #{i}: .bin exists={bin_exists} but guard declined={declined}; exactly one \
             must hold"
        );
        if declined {
            refused_at = Some((i, before));
            break;
        }
        admitted.push(i);
        tokio::time::sleep(WRITE_PACE).await;
    }
    let (refused_index, available_before_refusal) = refused_at.unwrap_or_else(|| {
        df(&mount, "no-refusal");
        panic!(
            "the guard never refused in {max_writes} writes; available now {}",
            available(&cache_dir)
        )
    });
    let available_at_refusal = available(&cache_dir);
    println!(
        "REFUSAL at write #{refused_index} after {:?}: admitted={} available_before={} \
         available_now={available_at_refusal} floor={floor} declines={}",
        fill_start.elapsed(),
        admitted.len(),
        available_before_refusal,
        guard.declines()
    );
    df(&mount, "refusal");

    // The refusal is at the floor: not early (the volume still had room for more than one
    // entry above the floor would be "early"), and not late (free space never went below the
    // floor by more than one entry's metadata/framing overhead).
    assert!(
        available_before_refusal < floor + 2 * ENTRY_BYTES,
        "the guard refused early: {available_before_refusal} free against a {floor} floor"
    );
    assert!(
        available_at_refusal + ENTRY_BYTES / 4 >= floor,
        "free space went below the floor before the guard refused: {available_at_refusal} \
         against {floor}"
    );
    assert!(
        admitted.len() as u64 * ENTRY_BYTES >= (start_available - floor) / 2,
        "the data must have moved most of the way to the floor: {} entries admitted",
        admitted.len()
    );

    // ---- Phase 2: further admissions are refused too, and nothing is published. ----
    for j in 0..POST_REFUSAL_ATTEMPTS {
        let i = refused_index + 1 + j;
        let declines_before = guard.declines();
        m.store_range(
            &key(i),
            0,
            ENTRY_BYTES - 1,
            &body(i),
            metadata(),
            Duration::from_secs(3600),
            false,
        )
        .await
        .unwrap_or_else(|e| panic!("post-refusal store #{i} errored: {e}"));
        assert_eq!(
            guard.declines(),
            declines_before + 1,
            "post-refusal write #{i} was not declined"
        );
        assert!(
            !m.get_new_range_file_path(&key(i), 0, ENTRY_BYTES - 1)
                .exists(),
            "post-refusal write #{i} published a .bin"
        );
        assert!(
            !m.get_new_metadata_file_path(&key(i)).exists(),
            "post-refusal write #{i} published a .meta"
        );
    }
    let declines_after_refusal = guard.declines();
    assert_eq!(declines_after_refusal, 1 + POST_REFUSAL_ATTEMPTS as u64);

    // ---- Phase 3: no partial files, every document parses, every admitted entry reads. ----
    let documents = assert_no_partial_files(&cache_dir, "at refusal");
    assert!(
        documents >= admitted.len(),
        "expected at least one parsed .meta per admitted entry: {documents} documents, {} \
         entries",
        admitted.len()
    );
    for &i in &admitted {
        assert_readable(&m, &cache_dir, i).await;
    }

    // ---- Phase 4: free space through the normal path; admission resumes. ----
    let to_delete: Vec<usize> = admitted.iter().copied().take(ENTRIES_TO_DELETE).collect();
    for &i in &to_delete {
        m.invalidate_cache_entry(&key(i))
            .await
            .unwrap_or_else(|e| panic!("invalidate {} errored: {e}", key(i)));
        assert!(
            m.get_metadata(&key(i)).await.unwrap().is_none(),
            "{} still has metadata after invalidation",
            key(i)
        );
    }
    // Wait (bounded) for the freed blocks to show in statvfs, on the figure the guard reads.
    let wait_start = Instant::now();
    while available(&cache_dir) < floor + 2 * ENTRY_BYTES
        && wait_start.elapsed() < Duration::from_secs(30)
    {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let available_after_delete = available(&cache_dir);
    assert!(
        available_after_delete >= floor + 2 * ENTRY_BYTES,
        "deleting {} entries did not free space: {available_after_delete} against floor {floor}",
        to_delete.len()
    );

    let resumed = refused_index + 1 + POST_REFUSAL_ATTEMPTS;
    let declines_before = guard.declines();
    m.store_range(
        &key(resumed),
        0,
        ENTRY_BYTES - 1,
        &body(resumed),
        metadata(),
        Duration::from_secs(3600),
        false,
    )
    .await
    .expect("store after freeing space");
    assert_eq!(
        guard.declines(),
        declines_before,
        "the guard still declined after space was freed ({available_after_delete} available, \
         floor {floor})"
    );
    assert_readable(&m, &cache_dir, resumed).await;
    for &i in admitted.iter().skip(ENTRIES_TO_DELETE) {
        assert_readable(&m, &cache_dir, i).await;
    }
    assert_no_partial_files(&cache_dir, "at end");

    println!(
        "END available_after_delete={available_after_delete} available_now={} declines={}",
        available(&cache_dir),
        guard.declines()
    );
    df(&mount, "end");
}
