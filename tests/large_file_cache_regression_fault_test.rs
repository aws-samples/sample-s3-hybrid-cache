// Feature: large-file-cache-regression
//
// High Concurrency Cache Write Tests
//
// Validates that buffered store_range under high concurrency (150 tasks)
// commits all ranges reliably. The buffered approach accumulates chunks
// in a Vec<u8> then calls store_range once (single write lock acquisition),
// avoiding the channel/lock contention that affects incremental writes.
//
// **Validates: Requirements 1.1, 1.2, 1.3, 1.4**

use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use walkdir::WalkDir;

const NUM_CONCURRENT_TASKS: usize = 150;
const RANGE_SIZE: usize = 16 * 1024; // 16 KiB per range

fn make_object_metadata(content_length: u64) -> ObjectMetadata {
    ObjectMetadata {
        etag: "test-etag-fault".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: content_length,
        parts: Vec::new(),
        ..Default::default()
    }
}

/// Set up a DiskCacheManager with full journal infrastructure (HybridMetadataWriter).
/// Required for commit_incremental_range to write journal entries.
async fn setup_disk_cache_with_journal(temp_dir: &std::path::Path) -> DiskCacheManager {
    let cache_dir = temp_dir.to_path_buf();

    let mut cache_manager = DiskCacheManager::new(cache_dir.clone(), true, 1024, false, 1_048_576);
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
        cache_dir,
        lock_manager,
        journal_manager,
        consolidation_trigger,
    )));

    cache_manager.set_hybrid_metadata_writer(hybrid_writer);
    cache_manager
}

/// Count .bin files recursively under a directory.
fn count_bin_files(dir: &std::path::Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "bin"))
        .count()
}

/// Companion test: High concurrency buffered store_range
///
/// Spawns 150 concurrent tokio::spawn tasks, each accumulating data in a Vec<u8>
/// then calling store_range once (single write lock acquisition).
///
/// This validates the fix approach: buffered accumulation + single store_range
/// does NOT suffer from the same contention issue.
///
/// EXPECTED: PASS on unfixed code
#[tokio::test(flavor = "multi_thread")]
async fn test_buffered_store_range_high_concurrency_all_committed() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let cache_manager = setup_disk_cache_with_journal(temp_dir.path()).await;
    let disk_cache = Arc::new(RwLock::new(cache_manager));

    let mut handles: Vec<(tokio::task::JoinHandle<usize>, tokio::task::JoinHandle<()>)> =
        Vec::with_capacity(NUM_CONCURRENT_TASKS);

    for i in 0..NUM_CONCURRENT_TASKS {
        let dc = Arc::clone(&disk_cache);

        // Create mpsc channel simulating TeeStream → background cache writer
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);

        let writer_handle = tokio::spawn(async move {
            let cache_key = format!("test-bucket/buffered-range-{}", i);
            let start = 0u64;
            let end = (RANGE_SIZE - 1) as u64;

            // Buffered approach: accumulate all chunks first
            let mut data = Vec::new();
            while let Some(chunk) = rx.recv().await {
                data.extend_from_slice(&chunk);
            }

            let metadata = make_object_metadata(RANGE_SIZE as u64);

            // Single write lock acquisition — buffered store_range
            {
                let mut cache = dc.write().await;
                cache
                    .store_range(
                        &cache_key,
                        start,
                        end,
                        &data,
                        metadata,
                        Duration::from_secs(3600),
                        true,
                    )
                    .await
                    .unwrap();
            }

            i
        });

        // Simulate TeeStream: spawn a sender task that sends all chunks.
        // Unlike the incremental test, the buffered receiver uses rx.recv().await
        // which blocks until data arrives, so the sender can use .send().await
        // to ensure all chunks are delivered. This mirrors the fix approach where
        // accumulation happens reliably in the receiver task.
        let sender_handle = tokio::spawn(async move {
            let data: Vec<u8> = (0..RANGE_SIZE).map(|b| ((i + b) % 256) as u8).collect();

            let chunk_size = RANGE_SIZE / 4;
            for chunk_start in (0..RANGE_SIZE).step_by(chunk_size) {
                let chunk_end = (chunk_start + chunk_size).min(RANGE_SIZE);
                let _ = tx.send(data[chunk_start..chunk_end].to_vec()).await;
            }
            // tx dropped here — signals end of stream
        });

        handles.push((writer_handle, sender_handle));
    }

    // Collect results
    let mut committed_count = 0usize;
    let mut failed_count = 0usize;

    for (writer_handle, sender_handle) in handles {
        // Wait for sender to complete first
        let _ = sender_handle.await;
        match writer_handle.await {
            Ok(_) => committed_count += 1,
            Err(e) => {
                eprintln!("Task failed: {:?}", e);
                failed_count += 1;
            }
        }
    }

    // Verify all range files exist on disk
    let ranges_dir = temp_dir.path().join("ranges");
    let files_on_disk = count_bin_files(&ranges_dir);

    eprintln!(
        "Buffered results: committed={}, failed={}, files_on_disk={}, expected={}",
        committed_count, failed_count, files_on_disk, NUM_CONCURRENT_TASKS
    );

    // Assert ALL ranges were committed — this SHOULD PASS even on unfixed code
    assert_eq!(
        committed_count, NUM_CONCURRENT_TASKS,
        "Expected all {} buffered store_range tasks to commit, but only {} succeeded.",
        NUM_CONCURRENT_TASKS, committed_count
    );
    assert_eq!(
        files_on_disk, NUM_CONCURRENT_TASKS,
        "Expected {} .bin files on disk from buffered store_range, but found {}.",
        NUM_CONCURRENT_TASKS, files_on_disk
    );
}
