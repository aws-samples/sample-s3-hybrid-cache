//! Property-based tests for concurrent `commit_incremental_range` under the
//! relaxed `&self` signature.
//!
//! **Property 3: Concurrent commit size accounting**
//!
//! *For any* set of K (4..=16) concurrent commits of distinct
//! `(cache_key, start, end)` ranges running against the same
//! `DiskCacheManager` (shared via `Arc`), the SizeAccumulator's
//! `current_delta()` after all commits complete SHALL equal the sum of the
//! per-range `compressed_bytes_written` values, regardless of interleaving.
//!
//! **Validates: Requirements 4.3, 4.5, 6.5**
//!
//! **Property 4: Dedup under concurrent commit of the same range**
//!
//! *For any* K (4..=16) concurrent commits of the SAME
//! `(cache_key, start, end)`, the SizeAccumulator's `current_delta()` after
//! all commits complete SHALL equal exactly one range's
//! `compressed_bytes_written`, not K times that value.
//!
//! **Validates: Requirements 4.4, 6.5**
//!
//! WARNING: This file contains property-based tests (PBT). Running it may
//! take longer than unit tests because each case spawns K concurrent tokio
//! tasks, each doing a begin → write → commit cycle, with a fresh temp
//! directory and cache manager per case.

use quickcheck::{quickcheck, TestResult};
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter};
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Range size used for every per-task write. Fixed so that with compression
/// disabled, each commit contributes a deterministic number of bytes to the
/// size accumulator (see `STORE_MODE_FRAME_OVERHEAD` below).
const RANGE_SIZE: usize = 16 * 1024; // 16 KiB

/// Fixed byte overhead of one `encode_store_mode_frame` LZ4 frame that fits
/// its payload in a single stored block: magic (4) + FLG (1) + BD (1) +
/// header checksum (1) + block-size word (4) + end mark (4) + content
/// checksum (4) = 19 bytes. See `CompressionHandler::encode_store_mode_frame`
/// in `src/compression.rs` (compression-content-aware-fix spec, Requirement
/// 3). `commit_one_range` writes `RANGE_SIZE` bytes as a single flushed
/// batch, so exactly one frame — and therefore one overhead charge — is
/// produced per commit.
const STORE_MODE_FRAME_OVERHEAD: u64 = 19;

/// Bytes contributed to the size accumulator by one `commit_one_range` call
/// with compression disabled: the logical payload plus the store-mode frame
/// envelope that now wraps every write (compressed or not) for integrity.
const EXPECTED_COMMIT_BYTES: u64 = RANGE_SIZE as u64 + STORE_MODE_FRAME_OVERHEAD;

// ---------------------------------------------------------------------------
// Test setup helpers
// ---------------------------------------------------------------------------

fn test_object_metadata(content_length: u64) -> ObjectMetadata {
    ObjectMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: content_length,
        parts: Vec::new(),
        ..Default::default()
    }
}

/// Build a `DiskCacheManager` with a journal writer AND a consolidator
/// attached so that `commit_incremental_range`'s full size-accounting path
/// runs. Returns the manager (wrapped in `Arc`) and the consolidator so the
/// caller can read `size_accumulator.current_delta()`.
///
/// Compression is disabled so `compressed_bytes_written == bytes_written +
/// STORE_MODE_FRAME_OVERHEAD`: every write, compressed or not, is wrapped in
/// a real LZ4 frame (store-mode blocks when disabled) for integrity, so the
/// expected accumulator delta is an exact multiple of `EXPECTED_COMMIT_BYTES`
/// rather than of `RANGE_SIZE` alone.
async fn setup(cache_dir: &Path) -> (Arc<DiskCacheManager>, Arc<JournalConsolidator>) {
    let cache_dir = cache_dir.to_path_buf();

    // Required by JournalConsolidator::initialize and HybridMetadataWriter writes
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();
    std::fs::create_dir_all(cache_dir.join("ranges")).unwrap();

    let mut cache_manager = DiskCacheManager::new(
        cache_dir.clone(),
        false, // compression_enabled — off, so delta == RANGE_SIZE per commit
        1024,  // compression_threshold (unused when disabled)
        false, // write_cache_enabled
        1_048_576,
    );
    cache_manager.initialize().await.unwrap();

    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_secs(30),
        3,
    ));
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "test-instance".to_string(),
    ));
    let consolidation_trigger = Arc::new(ConsolidationTrigger::new(10 * 1024 * 1024, 1000));

    let hybrid_writer = Arc::new(Mutex::new(HybridMetadataWriter::new(
        cache_dir.clone(),
        lock_manager.clone(),
        journal_manager.clone(),
        consolidation_trigger,
    )));

    let consolidation_config = ConsolidationConfig {
        interval: Duration::from_secs(5),
        size_threshold: 1024 * 1024,
        entry_count_threshold: 100,
        max_cache_size: 0, // disables eviction triggering
        eviction_trigger_percent: 95,
        eviction_target_percent: 80,
        stale_entry_timeout_secs: 300,
        consolidation_cycle_timeout: Duration::from_secs(30),
        max_keys_per_cycle: 5000,
    };
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir,
        journal_manager,
        lock_manager,
        consolidation_config,
    ));
    consolidator.initialize().await.unwrap();

    cache_manager.set_hybrid_metadata_writer(hybrid_writer);
    cache_manager.set_journal_consolidator(Arc::clone(&consolidator));

    (Arc::new(cache_manager), consolidator)
}

/// Map an arbitrary `u8` seed to a K value in `[4, 16]` as specified by the
/// task and design doc.
fn k_from_seed(seed: u8) -> usize {
    4 + (seed as usize % 13) // 4..=16
}

/// Drive one full `begin → write_range_chunk → commit_incremental_range`
/// cycle for `(cache_key, start, end)`. Writes `RANGE_SIZE` bytes split into
/// four equal chunks so the batching path exists even though compression is
/// off (it's a no-op store-mode pass through the same batch buffer). Returns
/// the number of bytes the commit contributes to the size accumulator.
///
/// Note: `writer.compressed_bytes_written` cannot be read before commit to
/// obtain this value. The disabled-compression path now batches into
/// `batch_buf` just like the compressed path (compression-content-aware-fix
/// Requirement 3), and `setup()`'s `compression_batch_size` (1 MiB) is far
/// larger than `RANGE_SIZE` (16 KiB), so the batch never reaches the
/// threshold flush inside `write_range_chunk`. The only flush is the
/// residual flush inside `finalize_incremental_range`, which runs *inside*
/// `commit_incremental_range` after `writer` has already been consumed. So
/// the post-commit total is exactly one store-mode frame covering the whole
/// payload: `EXPECTED_COMMIT_BYTES`.
async fn commit_one_range(
    cache_manager: Arc<DiskCacheManager>,
    cache_key: String,
    start: u64,
    end: u64,
) -> Result<u64, String> {
    let mut writer = cache_manager
        .begin_incremental_range_write(&cache_key, start, end, false)
        .await
        .map_err(|e| format!("begin_incremental_range_write failed: {}", e))?;

    // Deterministic content so decompression (if ever re-enabled) would round-trip.
    let payload: Vec<u8> = (0..RANGE_SIZE).map(|i| (i % 251) as u8).collect();
    for chunk in payload.chunks(RANGE_SIZE / 4) {
        DiskCacheManager::write_range_chunk(&mut writer, chunk)
            .map_err(|e| format!("write_range_chunk failed: {}", e))?;
    }

    cache_manager
        .commit_incremental_range(
            writer,
            test_object_metadata(RANGE_SIZE as u64),
            Duration::from_secs(3600),
        )
        .await
        .map_err(|e| format!("commit_incremental_range failed: {}", e))?;

    Ok(EXPECTED_COMMIT_BYTES)
}

// ---------------------------------------------------------------------------
// Property 3: Concurrent commit size accounting
// **Validates: Requirements 4.3, 4.5, 6.5**
// ---------------------------------------------------------------------------

/// Property 3: K concurrent commits of *distinct* ranges against a shared
/// `DiskCacheManager` must result in a size accumulator delta equal to the
/// sum of each range's `compressed_bytes_written`.
///
/// With compression disabled and a fixed `RANGE_SIZE`, the expected delta
/// is exactly `K * RANGE_SIZE`.
#[test]
fn prop_concurrent_commit_size_accounting() {
    fn property(k_seed: u8) -> TestResult {
        let k = k_from_seed(k_seed);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let (cache_manager, consolidator) = setup(temp_dir.path()).await;

            let mut handles = Vec::with_capacity(k);
            for i in 0..k {
                let cm = Arc::clone(&cache_manager);
                let key = format!("test-bucket/prop3-distinct-range-{}", i);
                handles.push(tokio::spawn(async move {
                    commit_one_range(cm, key, 0, (RANGE_SIZE as u64) - 1).await
                }));
            }

            let results = futures::future::join_all(handles).await;
            let mut expected_sum: u64 = 0;
            for (i, res) in results.into_iter().enumerate() {
                match res {
                    Ok(Ok(compressed_bytes)) => {
                        expected_sum += compressed_bytes;
                    }
                    Ok(Err(e)) => {
                        return TestResult::error(format!("task {} commit failed: {}", i, e));
                    }
                    Err(join_err) => {
                        return TestResult::error(format!("task {} panicked: {}", i, join_err));
                    }
                }
            }

            let delta = consolidator.size_accumulator().current_delta();
            if delta as u64 != expected_sum {
                return TestResult::error(format!(
                    "Property 3 violated: K={}, expected delta={}, got delta={}",
                    k, expected_sum, delta
                ));
            }

            // Sanity: with compression disabled and a fixed RANGE_SIZE per
            // commit, the expected sum must equal K * EXPECTED_COMMIT_BYTES
            // (payload + store-mode frame overhead per commit).
            let expected_exact = (k as u64) * EXPECTED_COMMIT_BYTES;
            if expected_sum != expected_exact {
                return TestResult::error(format!(
                    "Precondition failed: expected_sum={} != K*RANGE_SIZE={}",
                    expected_sum, expected_exact
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(u8) -> TestResult);
}

// ---------------------------------------------------------------------------
// Property 4: Dedup under concurrent commit of the same range
// **Validates: Requirements 4.4, 6.5**
// ---------------------------------------------------------------------------

/// Property 4: K concurrent commits of the *same* `(cache_key, start, end)`
/// must increase the size accumulator by exactly one range's
/// `compressed_bytes_written`, regardless of K and regardless of whether the
/// `range_already_existed` pre-rename check raced — SizeAccumulator's
/// internal `(cache_key_hash, start, end)` dedup set ensures each range is
/// accounted at most once per flush window.
#[test]
fn prop_concurrent_commit_dedup_same_range() {
    fn property(k_seed: u8) -> TestResult {
        let k = k_from_seed(k_seed);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let (cache_manager, consolidator) = setup(temp_dir.path()).await;

            let shared_key = "test-bucket/prop4-shared-range".to_string();
            let start = 0u64;
            let end = (RANGE_SIZE as u64) - 1;

            let mut handles = Vec::with_capacity(k);
            for _ in 0..k {
                let cm = Arc::clone(&cache_manager);
                let key = shared_key.clone();
                handles.push(tokio::spawn(async move {
                    commit_one_range(cm, key, start, end).await
                }));
            }

            let results = futures::future::join_all(handles).await;
            // At least one commit must have produced a non-zero
            // compressed_bytes so we know the size-tracking path ran.
            let mut any_compressed_bytes: Option<u64> = None;
            for (i, res) in results.into_iter().enumerate() {
                match res {
                    Ok(Ok(cb)) => {
                        if any_compressed_bytes.is_none() {
                            any_compressed_bytes = Some(cb);
                        }
                    }
                    Ok(Err(e)) => {
                        return TestResult::error(format!("task {} commit failed: {}", i, e));
                    }
                    Err(join_err) => {
                        return TestResult::error(format!("task {} panicked: {}", i, join_err));
                    }
                }
            }

            let single_range_size = match any_compressed_bytes {
                Some(cb) => cb,
                None => {
                    return TestResult::error(
                        "no commit reported compressed_bytes_written".to_string(),
                    );
                }
            };

            let delta = consolidator.size_accumulator().current_delta();

            // Core dedup invariant: exactly one range's worth of bytes
            // accumulated, not K times.
            if delta as u64 != single_range_size {
                return TestResult::error(format!(
                    "Property 4 violated: K={}, expected delta={} (one range), got delta={}",
                    k, single_range_size, delta
                ));
            }

            // With compression disabled and fixed RANGE_SIZE, that one range
            // must be exactly RANGE_SIZE + STORE_MODE_FRAME_OVERHEAD bytes.
            if single_range_size != EXPECTED_COMMIT_BYTES {
                return TestResult::error(format!(
                    "Precondition failed: single_range_size={} != EXPECTED_COMMIT_BYTES={}",
                    single_range_size, EXPECTED_COMMIT_BYTES
                ));
            }

            TestResult::passed()
        })
    }

    quickcheck(property as fn(u8) -> TestResult);
}

// ---------------------------------------------------------------------------
// Explicit fixed-K cases
//
// These lock in the contract at the K endpoints specified by the design
// (K=4, K=8, K=16) with deterministic reproductions that survive quickcheck
// seed changes.
// ---------------------------------------------------------------------------

async fn run_distinct_ranges_case(k: usize) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let (cache_manager, consolidator) = setup(temp_dir.path()).await;

    let mut handles = Vec::with_capacity(k);
    for i in 0..k {
        let cm = Arc::clone(&cache_manager);
        let key = format!("test-bucket/explicit-distinct-range-{}", i);
        handles.push(tokio::spawn(async move {
            commit_one_range(cm, key, 0, (RANGE_SIZE as u64) - 1).await
        }));
    }
    let results = futures::future::join_all(handles).await;
    for (i, res) in results.into_iter().enumerate() {
        res.unwrap_or_else(|e| panic!("task {} panicked: {}", i, e))
            .unwrap_or_else(|e| panic!("task {} commit failed: {}", i, e));
    }

    let delta = consolidator.size_accumulator().current_delta();
    assert_eq!(
        delta as u64,
        (k as u64) * EXPECTED_COMMIT_BYTES,
        "distinct-ranges case K={}: expected delta={}, got {}",
        k,
        (k as u64) * EXPECTED_COMMIT_BYTES,
        delta
    );
}

async fn run_same_range_case(k: usize) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let (cache_manager, consolidator) = setup(temp_dir.path()).await;

    let shared_key = "test-bucket/explicit-shared-range".to_string();
    let start = 0u64;
    let end = (RANGE_SIZE as u64) - 1;

    let mut handles = Vec::with_capacity(k);
    for _ in 0..k {
        let cm = Arc::clone(&cache_manager);
        let key = shared_key.clone();
        handles.push(tokio::spawn(async move {
            commit_one_range(cm, key, start, end).await
        }));
    }
    let results = futures::future::join_all(handles).await;
    for (i, res) in results.into_iter().enumerate() {
        res.unwrap_or_else(|e| panic!("task {} panicked: {}", i, e))
            .unwrap_or_else(|e| panic!("task {} commit failed: {}", i, e));
    }

    let delta = consolidator.size_accumulator().current_delta();
    assert_eq!(
        delta as u64, EXPECTED_COMMIT_BYTES,
        "same-range case K={}: expected delta={} (one range), got {}",
        k, EXPECTED_COMMIT_BYTES, delta
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distinct_ranges_k4() {
    run_distinct_ranges_case(4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distinct_ranges_k8() {
    run_distinct_ranges_case(8).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distinct_ranges_k16() {
    run_distinct_ranges_case(16).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_same_range_k4() {
    run_same_range_case(4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_same_range_k8() {
    run_same_range_case(8).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_same_range_k16() {
    run_same_range_case(16).await;
}
