//! Measurement: what a 1 KiB slice of a 256 MiB single-extent range costs to read from
//! the cache, compressed (LZ4 frames) and uncompressed (a raw file), first and second read.
//!
//! Bytes read from the file are this process's `rchar` (`/proc/self/io`) across the read: the
//! test is the only thing running in its binary, so the delta is the slice read's own reads.
//! Run alone: `cargo test --release --test slice_read_cost_test -- --nocapture`.
use futures::StreamExt;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const EXTENT: u64 = 256 * 1024 * 1024;
const SLICE_AT: u64 = 3 * 1024 * 1024;
const SLICE: usize = 1024;
const KEY: &str = "test-bucket/scn1/cost.bin";

fn rchar() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("rchar: "))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn setup(cache_dir: &Path) -> Arc<CacheManager> {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    Arc::new(CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false,
        0,
        CacheEvictionAlgorithm::TinyLFU,
    ))
}

fn data() -> Vec<u8> {
    // Low-entropy enough to compress, varied enough not to collapse.
    (0..EXTENT as usize)
        .map(|i| ((i / 7) % 251) as u8 ^ ((i >> 13) as u8))
        .collect()
}

async fn listed(manager: &CacheManager) -> s3_proxy::cache_types::RangeSpec {
    manager
        .get_journal_consolidator()
        .await
        .expect("consolidator")
        .consolidate_object(KEY)
        .await
        .expect("consolidation");
    let text = std::fs::read_to_string(manager.get_new_metadata_file_path(KEY)).unwrap();
    let meta: NewCacheMetadata = serde_json::from_str(&text).unwrap();
    meta.ranges[0].clone()
}

async fn slice_once(
    dcm: &s3_proxy::disk_cache::DiskCacheManager,
    spec: &s3_proxy::cache_types::RangeSpec,
    expected: &[u8],
) -> (u64, Duration) {
    let before = rchar();
    let t = Instant::now();
    let (stream, skipped) = dcm
        .stream_range_data_from(spec, 256 * 1024, SLICE_AT)
        .await
        .expect("slice stream");
    let need = (SLICE_AT - skipped) as usize + SLICE;
    let mut stream = Box::pin(stream);
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        got.extend_from_slice(&item.expect("no error"));
        if got.len() >= need {
            break;
        }
    }
    drop(stream); // the client has its slice and stops reading
    let elapsed = t.elapsed();
    // Let the reader task observe the dropped receiver and stop.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let read = rchar() - before;
    assert_eq!(&got[(SLICE_AT - skipped) as usize..need], expected);
    (read, elapsed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slice_of_a_256_mib_extent_reads_only_up_to_the_slice() {
    let data = data();
    let expected = &data[SLICE_AT as usize..SLICE_AT as usize + SLICE];
    for compressed in [true, false] {
        let temp = TempDir::new().unwrap();
        let manager = setup(temp.path());
        manager
            .create_configured_disk_cache_manager()
            .store_range(
                KEY,
                0,
                EXTENT - 1,
                &data,
                ObjectMetadata {
                    etag: "\"cost\"".to_string(),
                    content_length: EXTENT,
                    upload_state: UploadState::Complete,
                    cumulative_size: EXTENT,
                    ..Default::default()
                },
                Duration::from_secs(3600),
                compressed,
            )
            .await
            .expect("fill");
        let mut spec = listed(&manager).await;
        let label = if compressed {
            "compressed (LZ4 frames)"
        } else {
            // Every writer stores LZ4 frames; an uncompressed file is a legacy raw `.bin`,
            // written here directly with its listing pointed at it.
            let path = temp.path().join("ranges").join(&spec.file_path);
            std::fs::write(&path, &data).unwrap();
            spec.compression_algorithm = CompressionAlgorithm::None;
            spec.compressed_size = EXTENT;
            spec.file_identity = std::fs::metadata(&path)
                .ok()
                .and_then(|m| s3_proxy::cache_types::RangeFileIdentity::of(&m));
            "uncompressed (raw file)"
        };
        let dcm = manager.create_configured_disk_cache_manager();
        let (read1, t1) = slice_once(&dcm, &spec, expected).await;
        let (read2, t2) = slice_once(&dcm, &spec, expected).await;
        println!(
            "SCN1 MEASURE {}: file {} bytes, 1 KiB slice at {}: first read {} bytes in {:?}, \
             second read {} bytes in {:?}",
            label, spec.compressed_size, SLICE_AT, read1, t1, read2, t2
        );
        assert!(
            read1 < 64 * 1024 * 1024 && read2 < 64 * 1024 * 1024,
            "a 1 KiB slice read {} / {} bytes of a {} byte extent",
            read1,
            read2,
            EXTENT
        );
    }
}
