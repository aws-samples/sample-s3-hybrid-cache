//! What a write's invalidation reads from the journals, against their size.
//!
//! An acknowledged PUT or DELETE runs `CacheManager::invalidate_cache_unified_for_operation`,
//! which stamps the key's write-order marker and removes its entry. An enumeration of the key's
//! unfolded journal `Add`s there would read every instance's journal files whole, so each write
//! would cost a read of every journal line on the shared volume, and more when consolidation
//! falls behind. The stamp already retires every such `Add` that carries a fill witness, so the
//! write reads no journal. A version change or a `404` seen on the GET path
//! (`invalidate_after_origin_change`) stamps the marker too, so it reads none either. A removal
//! that leaves the marker alone (`invalidate_cache_hierarchy`: a bucket-rule purge, a local
//! repair) still enumerates; it is the control that shows the counter sees a journal scan. The
//! `412` and recovery-refetch invalidation (`DiskCacheManager::invalidate_all_ranges`) stamps and
//! still enumerates, for `Add`s no stamp retires; it is printed, not bounded.
//!
//! The observable is `rchar` in `/proc/self/io`: bytes the process's `read`/`pread` calls
//! returned, independent of the page cache. It is process-wide, so the two tests here hold one
//! lock and this file is a target of its own. Linux only.
//!
//! The last two tests measure what the write-order stamp itself adds to a GET that S3 showed a
//! version change or a `404` (`invalidate_after_origin_change`), against the same removal
//! without it (`invalidate_cache_hierarchy`), on an entry that is cached: the file operations
//! it adds, counted from `syscw` and `wchar`, and its latency.
//!
//! The timing tests are `#[ignore]`d: run them alone with
//! `cargo test --release --test write_invalidation_journal_cost_test -- --ignored --nocapture`.
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{ObjectMetadata, RangeSpec, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::journal_manager::{JournalEntry, JournalOperation};
use tempfile::TempDir;

static PROCESS_IO: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `rchar` from `/proc/self/io`: bytes returned by this process's read system calls.
fn rchar() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .expect("/proc/self/io")
        .lines()
        .find_map(|l| l.strip_prefix("rchar: "))
        .and_then(|v| v.trim().parse().ok())
        .expect("rchar line in /proc/self/io")
}

/// A range-fill `Add` of another key, shaped as a GET miss writes one: S3's response headers
/// in `object_metadata` and a fill witness.
fn peer_add(i: usize) -> JournalEntry {
    let now = SystemTime::now();
    let cache_key = format!("bucket/peer-objects/part-{i:08}.parquet");
    let file_path = format!(
        "bucket/{:02x}/{:03x}/part-{i:08}.parquet_0-8388607.bin",
        i % 256,
        i % 4096
    );
    let mut response_headers = HashMap::new();
    for (k, v) in [
        ("etag", "\"9b2cf535f27731c974343645a3985328\""),
        ("last-modified", "Tue, 24 Nov 2026 09:00:00 GMT"),
        ("content-length", "8388608"),
        ("content-type", "application/octet-stream"),
        ("accept-ranges", "bytes"),
        ("x-amz-server-side-encryption", "AES256"),
        ("x-amz-request-id", "4442587FB7D0A2F9"),
    ] {
        response_headers.insert(k.to_string(), v.to_string());
    }
    JournalEntry {
        timestamp: now,
        instance_id: "peer-instance".to_string(),
        cache_key,
        range_spec: RangeSpec {
            start: 0,
            end: 8_388_607,
            file_path: file_path.clone(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 8_388_608,
            uncompressed_size: 8_388_608,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        },
        operation: JournalOperation::Add,
        range_file_path: String::new(),
        metadata_version: 1,
        new_ttl_secs: None,
        object_ttl_secs: Some(86_400),
        access_increment: None,
        object_metadata: Some(ObjectMetadata {
            etag: "\"9b2cf535f27731c974343645a3985328\"".to_string(),
            last_modified: "Tue, 24 Nov 2026 09:00:00 GMT".to_string(),
            content_length: 8_388_608,
            content_type: Some("application/octet-stream".to_string()),
            response_headers,
            upload_state: UploadState::Complete,
            cumulative_size: 8_388_608,
            ..ObjectMetadata::default()
        }),
        fill_witness: Some("5c1f3b0e8a7d4c2b9e6f1a0d3c5b7e9f".to_string()),
    }
}

/// Grow a peer's journal file to at least `bytes`. Returns one line's length.
fn grow_journal(cache_dir: &Path, bytes: u64) -> usize {
    let dir = cache_dir.join("metadata").join("_journals");
    std::fs::create_dir_all(&dir).expect("journals dir");
    let path = dir.join("peer-instance.journal");
    let mut line = serde_json::to_vec(&peer_add(0)).expect("serialize");
    line.push(b'\n');
    let line_len = line.len();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open journal");
    let mut len = file.metadata().expect("metadata").len();
    let mut i = (len as usize) / line_len;
    let mut buf = Vec::with_capacity(1 << 20);
    while len < bytes {
        buf.clear();
        while buf.len() < (1 << 20) && len + (buf.len() as u64) < bytes {
            let mut l = serde_json::to_vec(&peer_add(i)).expect("serialize");
            l.push(b'\n');
            buf.extend_from_slice(&l);
            i += 1;
        }
        file.write_all(&buf).expect("write journal");
        len += buf.len() as u64;
    }
    file.sync_all().expect("sync journal");
    line_len
}

fn manager(cache_dir: &Path) -> Arc<CacheManager> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir.to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.metadata_cache.enabled = false;
    let c = &config.cache;
    Arc::new(CacheManager::new_with_shared_storage(
        c.cache_dir.clone(),
        false,
        0,
        c.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        false,
        c.get_ttl,
        c.head_ttl,
        c.put_ttl,
        c.actively_remove_cached_data,
        c.shared_storage.clone(),
        c.write_cache_percent,
        c.write_cache_enabled,
        c.incomplete_upload_ttl,
        c.metadata_cache.clone(),
        c.eviction_trigger_percent,
        c.eviction_target_percent,
        c.read_cache_enabled,
        c.bucket_settings_staleness_threshold,
        c.compression_batch_size,
        c.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ))
}

async fn setup() -> (TempDir, Arc<CacheManager>, DiskCacheManager) {
    let temp = TempDir::new().expect("tempdir");
    let cm = manager(temp.path());
    let disk = cm.create_configured_disk_cache_manager();
    cm.initialize().await.expect("cache init");
    (temp, cm, disk)
}

#[derive(Clone, Copy, Debug)]
enum Invalidation {
    Put,
    Delete,
    /// A GET or HEAD that S3 showed another version or a `404`: stamps.
    OriginChange,
    /// A `412` on the proxy's own pin or a recovery refetch naming another version: stamps.
    RecoveryVersionChange,
    /// A removal that leaves the marker alone, so it enumerates the journals.
    UnstampedRemoval,
}
async fn run(cm: &Arc<CacheManager>, disk: &DiskCacheManager, how: Invalidation, key: &str) {
    match how {
        Invalidation::Put => cm.invalidate_cache_unified_for_operation(key, "PUT").await,
        Invalidation::Delete => {
            cm.invalidate_cache_unified_for_operation(key, "DELETE")
                .await
        }
        Invalidation::OriginChange => cm.invalidate_after_origin_change_for_tests(key).await,
        Invalidation::RecoveryVersionChange => {
            let stamped = disk.invalidate_all_ranges(key).await.expect("invalidation");
            assert!(stamped, "fixture: the recovery invalidation must stamp");
            Ok(())
        }
        Invalidation::UnstampedRemoval => cm.invalidate_cache_hierarchy(key).await,
    }
    .expect("invalidation");
}

/// Bytes read by one invalidation of a fresh key.
async fn bytes_read(
    cm: &Arc<CacheManager>,
    disk: &DiskCacheManager,
    how: Invalidation,
    n: usize,
) -> u64 {
    let key = format!("bucket/written/{how:?}-{n}.bin");
    let before = rchar();
    run(cm, disk, how, &key).await;
    rchar() - before
}

const SIZES: [u64; 3] = [0, 4 << 20, 32 << 20];
/// Most bytes a stamping invalidation may read at any journal size: its own `.meta` probe, the
/// marker and the slot lock are a few KiB. A scan of the smallest non-empty journal is 4 MiB.
const WRITE_READ_CEILING: u64 = 256 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stamping_invalidation_reads_no_journal_bytes_at_any_journal_size() {
    let _io = PROCESS_IO.lock().await;
    let (temp, cm, disk) = setup().await;
    let mut rows = Vec::new();
    let mut over = Vec::new();
    for (n, &size) in SIZES.iter().enumerate() {
        grow_journal(temp.path(), size);
        let journal =
            std::fs::metadata(temp.path().join("metadata/_journals/peer-instance.journal"))
                .map(|m| m.len())
                .unwrap_or(0);
        let put = bytes_read(&cm, &disk, Invalidation::Put, n).await;
        let delete = bytes_read(&cm, &disk, Invalidation::Delete, n).await;
        let origin_change = bytes_read(&cm, &disk, Invalidation::OriginChange, n).await;
        let recovery = bytes_read(&cm, &disk, Invalidation::RecoveryVersionChange, n).await;
        let unstamped = bytes_read(&cm, &disk, Invalidation::UnstampedRemoval, n).await;
        rows.push(format!(
            "journal={journal} put_read={put} delete_read={delete} \
             origin_change_read={origin_change} recovery_read={recovery} \
             unstamped_removal_read={unstamped}"
        ));
        for (what, read) in [
            ("PUT", put),
            ("DELETE", delete),
            ("GET-path version change or 404", origin_change),
        ] {
            if read > WRITE_READ_CEILING {
                over.push(format!(
                    "{what} read {read} bytes against a {journal}-byte journal"
                ));
            }
        }
        // The control: a removal that enumerates reads the whole journal, so the counter sees
        // a scan when one happens.
        assert!(
            unstamped >= journal,
            "APPARATUS: an unstamped removal read {unstamped} bytes of a {journal}-byte journal; \
             the counter does not observe a journal scan\n{}",
            rows.join("\n")
        );
    }
    println!("{}", rows.join("\n"));
    assert!(
        over.is_empty(),
        "A STAMPING INVALIDATION READ THE JOURNALS: {}\n{}",
        over.join("; "),
        rows.join("\n")
    );
}

/// Median wall time of each invalidation against journal size, on this host's local disk, page
/// cache warm. Prints one line per size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "wall-time measurement: run alone"]
async fn write_invalidation_latency_against_journal_size() {
    let _io = PROCESS_IO.lock().await;
    let (temp, cm, disk) = setup().await;
    let mut line_len = 0;
    for &size in &[0u64, 8 << 20, 64 << 20, 256 << 20] {
        line_len = grow_journal(temp.path(), size).max(line_len);
        let mut out = Vec::new();
        for how in [
            Invalidation::Put,
            Invalidation::Delete,
            Invalidation::OriginChange,
            Invalidation::RecoveryVersionChange,
            Invalidation::UnstampedRemoval,
        ] {
            let mut times = Vec::new();
            let mut read = 0;
            for i in 0..7 {
                let key = format!("bucket/timed/{how:?}-{size}-{i}.bin");
                let before = rchar();
                let t = Instant::now();
                run(&cm, &disk, how, &key).await;
                times.push(t.elapsed());
                read = rchar() - before;
            }
            times.sort();
            out.push(format!("{how:?} median={:?} read={read}", times[3]));
        }
        println!(
            "journal_bytes={size} line_bytes={line_len} {}",
            out.join(" ")
        );
    }
}

/// One `/proc/self/io` field.
fn io_field(name: &str) -> u64 {
    let prefix = format!("{name}: ");
    std::fs::read_to_string("/proc/self/io")
        .expect("/proc/self/io")
        .lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()).map(str::to_string))
        .and_then(|v| v.trim().parse().ok())
        .expect("field in /proc/self/io")
}
/// A cached object for `key`: a `.meta` and one range file, as a write-through PUT leaves them.
async fn seed_entry(cm: &Arc<CacheManager>, key: &str) {
    cm.store_put_as_write_cached_range(
        key,
        &[7u8; 4096],
        "\"5c1f3b0e8a7d4c2b\"".to_string(),
        "Tue, 24 Nov 2026 09:00:00 GMT".to_string(),
        Some("application/octet-stream".to_string()),
        HashMap::new(),
    )
    .await
    .expect("seed a cached entry");
    assert!(
        cm.get_new_metadata_file_path(key).exists(),
        "fixture: the seeded entry has a .meta"
    );
}
/// The removal a GET runs when S3 showed a version change or a `404`, with the stamp or, for the
/// comparison, without it.
async fn remove_entry(cm: &Arc<CacheManager>, key: &str, stamp: bool) {
    if stamp {
        cm.invalidate_after_origin_change_for_tests(key).await
    } else {
        cm.invalidate_cache_hierarchy(key).await
    }
    .expect("invalidation");
}
fn marker_files(dir: &Path) -> usize {
    let mut n = 0;
    let mut stack = vec![dir.join("metadata")];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|x| x.to_str()) == Some("wtok") {
                n += 1;
            }
        }
    }
    n
}
/// The stamp adds one small file write per removal: one `write` system call of the marker's
/// JSON (`std::fs::write`: create, write, close; no fsync and no rename, by reading), and the
/// marker file it leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stamp_adds_one_small_file_write_per_removal() {
    let _io = PROCESS_IO.lock().await;
    let (temp, cm, _disk) = setup().await;
    const N: usize = 200;
    let mut measured = Vec::new();
    for stamp in [false, true] {
        let keys: Vec<String> = (0..N)
            .map(|i| format!("bucket/stamp-ops/{stamp}-{i:04}.bin"))
            .collect();
        for k in &keys {
            seed_entry(&cm, k).await;
        }
        let (calls, bytes) = (io_field("syscw"), io_field("wchar"));
        for k in &keys {
            remove_entry(&cm, k, stamp).await;
        }
        measured.push((io_field("syscw") - calls, io_field("wchar") - bytes));
    }
    let markers = marker_files(temp.path());
    let ((plain_calls, plain_bytes), (stamped_calls, stamped_bytes)) = (measured[0], measured[1]);
    let extra_calls = (stamped_calls as f64 - plain_calls as f64) / N as f64;
    let extra_bytes = (stamped_bytes as f64 - plain_bytes as f64) / N as f64;
    println!(
        "removals={N} unstamped: write_calls={plain_calls} bytes={plain_bytes}; stamped: \
         write_calls={stamped_calls} bytes={stamped_bytes}; per stamp: write_calls={extra_calls:.2} \
         bytes={extra_bytes:.1}; marker files={markers}"
    );
    assert_eq!(
        markers, N,
        "every stamped removal leaves one marker file, and only those"
    );
    assert!(
        (0.9..=2.0).contains(&extra_calls) && extra_bytes < 1024.0,
        "A STAMP ADDED {extra_calls:.2} WRITE CALLS AND {extra_bytes:.1} BYTES PER REMOVAL, \
         against one write of the marker's JSON"
    );
}
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}
/// Latency of the removal a GET that S3 showed a version change or a `404` runs, with the stamp
/// and without it, on a cached entry, local disk, page cache warm, empty journals: 1,000 of
/// each, interleaved. Prints p50 and p99 of each and their difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "wall-time measurement: run alone"]
async fn stamp_latency_on_a_version_change_or_404_removal() {
    let _io = PROCESS_IO.lock().await;
    let (_temp, cm, _disk) = setup().await;
    const N: usize = 1000;
    let mut times = [Vec::with_capacity(N), Vec::with_capacity(N)];
    for i in 0..N {
        for stamp in [false, true] {
            let key = format!("bucket/stamp-latency/{stamp}-{i:05}.bin");
            seed_entry(&cm, &key).await;
            let t = Instant::now();
            remove_entry(&cm, &key, stamp).await;
            times[stamp as usize].push(t.elapsed());
        }
    }
    for t in &mut times {
        t.sort();
    }
    let ((u50, u99), (s50, s99)) = (p50p99(&times[0]), p50p99(&times[1]));
    println!(
        "removals={N} each: unstamped p50={u50:?} p99={u99:?}; stamped p50={s50:?} p99={s99:?}; \
         added p50={:?} p99={:?}",
        s50.saturating_sub(u50),
        s99.saturating_sub(u99)
    );
}
fn p50p99(t: &[Duration]) -> (Duration, Duration) {
    (percentile(t, 0.50), percentile(t, 0.99))
}
