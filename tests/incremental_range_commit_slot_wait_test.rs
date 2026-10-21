//! A streamed GET-miss fill must not hold the `DiskCacheManager` read guard while its commit
//! waits for the key's slot lock, and must still cache the fill once the slot frees.
//!
//! The streamed fill paths in `http_proxy.rs` commit through
//! `DiskCacheManager::commit_incremental_range_shared`. With a hybrid writer the publish takes
//! the key's slot lock and waits up to `lock_acquire_wait_budget` (5 s default). tokio's
//! `RwLock` is write-preferring, so a read guard held through that wait lets one queued
//! `write()` block every later `read()` behind it. This test holds the slot from outside,
//! queues a writer and then a reader while the commit waits, asserts both complete promptly,
//! then frees the slot and asserts the commit publishes its `.bin`.
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const KEY: &str = "test-bucket/scn1/streamed-range.bin";
const LEN: u64 = 4096;
const ETAG: &str = "\"55555555555555555555555555555555\"";
/// Well under the 5 s default slot-wait budget, well over an uncontended guard hold.
const BOUND: Duration = Duration::from_millis(1500);

fn setup(cache_dir: &Path) -> Arc<tokio::sync::RwLock<DiskCacheManager>> {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let manager = Arc::new(CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false,
        0,
        CacheEvictionAlgorithm::TinyLFU,
    ));
    Arc::new(tokio::sync::RwLock::new(
        manager.create_configured_disk_cache_manager(),
    ))
}

fn metadata() -> ObjectMetadata {
    ObjectMetadata {
        etag: ETAG.to_string(),
        content_length: LEN,
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        upload_state: UploadState::Complete,
        cumulative_size: LEN,
        ..Default::default()
    }
}

fn bin_count(cache_dir: &Path) -> usize {
    fn walk(dir: &Path, n: &mut usize) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, n);
                } else if p.extension().and_then(|x| x.to_str()) == Some("bin") {
                    *n += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(&cache_dir.join("ranges"), &mut n);
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slot_wait_in_streamed_commit_does_not_stall_a_queued_writer_or_later_reader() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().to_path_buf();
    let dcm = setup(&cache_dir);

    let mut writer = dcm
        .read()
        .await
        .begin_incremental_range_write(KEY, 0, LEN - 1, false)
        .await
        .expect("begin")
        .expect("fixture: the write must be admitted");
    DiskCacheManager::write_range_chunk(&mut writer, &vec![0x55; LEN as usize]).expect("chunk");

    let slot = s3_proxy::metadata_lock_manager::try_acquire_slot_lock(&cache_dir, KEY)
        .expect("fixture: take the key's slot lock");

    let commit = {
        let dcm = Arc::clone(&dcm);
        tokio::spawn(async move {
            DiskCacheManager::commit_incremental_range_shared(
                &dcm,
                writer,
                metadata(),
                Duration::from_secs(3600),
            )
            .await
        })
    };
    // Give the commit time to finalize and reach the slot wait.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !commit.is_finished(),
        "fixture: the commit must still be waiting on the held slot lock"
    );

    // A writer queues first, then a reader arrives behind it.
    let queued_writer = {
        let dcm = Arc::clone(&dcm);
        tokio::spawn(async move {
            let _w = dcm.write().await;
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let later_reader = {
        let dcm = Arc::clone(&dcm);
        tokio::spawn(async move {
            let _r = dcm.read().await;
        })
    };
    let writer_ok = tokio::time::timeout(BOUND, queued_writer).await.is_ok();
    let reader_ok = tokio::time::timeout(BOUND, later_reader).await.is_ok();
    let commit_still_waiting = !commit.is_finished();

    // Free the slot before asserting, so a red run does not leave the commit wedged.
    drop(slot);

    assert!(
        commit_still_waiting,
        "fixture: the commit must have been waiting on the slot throughout"
    );
    assert!(
        writer_ok,
        "STALL: a queued disk_cache.write() waited over {:?} while the streamed commit \
         waited on the slot lock",
        BOUND
    );
    assert!(
        reader_ok,
        "STALL: a disk_cache.read() queued behind a writer waited over {:?} while the \
         streamed commit waited on the slot lock",
        BOUND
    );
    tokio::time::timeout(Duration::from_secs(10), commit)
        .await
        .expect("commit must return once the slot is free")
        .expect("join")
        .expect("commit");
    assert_eq!(
        bin_count(&cache_dir),
        1,
        "a streamed fill that waited for the slot is published, not skipped"
    );
}
