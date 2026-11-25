//! `cache.shard_sum_bytes` and `cache.shard_sum_drift_bytes`: the sum of the per-shard `byte_sum`
//! figures, published only when every shard document carries one, and its signed difference from
//! the accounted total.

use s3_proxy::st_size_fold::collect_st_size_fold_gauges;
use s3_proxy::staged_fold::{read_shard_checkpoints, Shard, StagedShardCheckpoint};
use std::path::Path;
use tempfile::TempDir;

/// Install a document for `shard`, with `byte_sum` when given.
fn install(dir: &Path, shard: &Shard, byte_sum: Option<u64>) {
    let mut c = StagedShardCheckpoint::unfolded_for(shard);
    if let Some(b) = byte_sum {
        c.other.insert("byte_sum".to_string(), b.into());
    }
    c.install(dir).unwrap();
}

/// (a) Every shard carries `byte_sum`: the gauge is their sum.
///
/// Red side: omit the sum (publish `None`, or `0`, or only the first shard's figure).
#[test]
fn the_gauge_is_the_sum_of_every_shards_byte_sum() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    install(dir, &Shard::new("bucket", "aa"), Some(1_000));
    install(dir, &Shard::new("bucket", "bb"), Some(20_000));
    install(dir, &Shard::new("other", "cc"), Some(300_000));
    let set = read_shard_checkpoints(dir);
    assert!(set.is_complete(), "the seeded documents must all be read");
    assert_eq!(
        collect_st_size_fold_gauges(&set).shard_sum_bytes,
        Some(321_000)
    );
}

/// (b) One shard lacks `byte_sum`: the gauge is absent, not the other shards' partial sum.
///
/// Red side: skip the shard that has no figure instead of returning `None`.
#[test]
fn one_shard_without_byte_sum_makes_the_gauge_absent_not_partial() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    install(dir, &Shard::new("bucket", "aa"), Some(1_000));
    install(dir, &Shard::new("bucket", "bb"), None);
    install(dir, &Shard::new("bucket", "cc"), Some(300_000));
    let set = read_shard_checkpoints(dir);
    assert!(set.is_complete(), "the seeded documents must all be read");
    assert_eq!(
        collect_st_size_fold_gauges(&set).shard_sum_bytes,
        None,
        "a partial sum of 301000 would read as a smaller cache"
    );
}

/// An empty volume has no sum to publish: `None`, never `0`.
#[test]
fn no_documents_is_absent_not_zero() {
    let tmp = TempDir::new().unwrap();
    let set = read_shard_checkpoints(tmp.path());
    assert_eq!(collect_st_size_fold_gauges(&set).shard_sum_bytes, None);
}

/// (c) The drift is sum minus total: positive when the shards hold more than the accounted total.
///
/// Red side: swap the operands (`total - sum`).
#[test]
fn the_drift_is_the_sum_minus_the_total_and_signed() {
    use s3_proxy::st_size_fold::shard_sum_drift_bytes;
    assert_eq!(shard_sum_drift_bytes(Some(1_500), 1_000), Some(500));
    assert_eq!(shard_sum_drift_bytes(Some(1_000), 1_500), Some(-500));
    assert_eq!(shard_sum_drift_bytes(Some(7), 7), Some(0));
    assert_eq!(shard_sum_drift_bytes(None, 1_000), None);
    assert_eq!(shard_sum_drift_bytes(Some(u64::MAX), 0), Some(i64::MAX));
    assert_eq!(shard_sum_drift_bytes(Some(0), u64::MAX), Some(i64::MIN));
}
/// An unreadable document makes the sum absent, not the readable documents' partial sum.
///
/// Red side: remove the `set.is_complete()` guard in `shard_sum_from_checkpoints`; the one readable
/// document then yields `Some(1000)`.
#[test]
fn one_unreadable_document_makes_the_gauge_absent_not_partial() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    install(dir, &Shard::new("bucket", "aa"), Some(1_000));
    install(dir, &Shard::new("bucket", "bb"), Some(20_000));
    let bad = dir
        .join("metadata")
        .join("_shard_state")
        .join("checkpoints")
        .join("bucket")
        .join("bb.json");
    assert!(
        bad.is_file(),
        "PRECONDITION: the seeded document is at {bad:?}"
    );
    std::fs::write(&bad, b"{ not json").unwrap();
    let set = read_shard_checkpoints(dir);
    assert!(
        !set.is_complete(),
        "PRECONDITION: the corrupted document must make the set incomplete"
    );
    assert_eq!(
        collect_st_size_fold_gauges(&set).shard_sum_bytes,
        None,
        "a partial sum of 1000 would read as a smaller cache"
    );
}
/// The drift is taken against `SizeState.total_size`, not `total_cache_size`, so an upload in flight
/// does not move it.
///
/// Drives a real `CacheManager` in the fold epoch (the only epoch in which `total_cache_size` holds
/// the in-progress part bytes P) and reads the drift back through `collect_metrics`, the way `/metrics`
/// does. The shard document says 1,000,000 bytes; the volume holds one part record of 4,096 bytes under
/// `mpus_in_progress/`; the persisted `SizeState.total_size` is seeded to 7,000, so the operand is
/// neither 0 nor the sum.
///
/// Red sides: subtract `sizes.total_cache_size` at the call site in `collect_cache_metrics` (the drift
/// then reads 1,000,000 - 7,000 - 4,096), or subtract `0` there (it then reads 1,000,000).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_drift_ignores_in_progress_part_bytes() {
    use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
    use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
    use s3_proxy::metrics::MetricsManager;
    use std::sync::Arc;
    use std::time::Duration;
    const SUM: u64 = 1_000_000;
    const P: u64 = 4_096;
    const TOTAL_SIZE: u64 = 7_000;
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    // A previous run's persisted range-file total, read back as `SizeState.total_size`.
    std::fs::create_dir_all(dir.join("size_tracking")).unwrap();
    let previous = s3_proxy::journal_consolidator::SizeState {
        total_size: TOTAL_SIZE,
        ..Default::default()
    };
    std::fs::write(
        dir.join("size_tracking").join("size_state.json"),
        serde_json::to_string_pretty(&previous).unwrap(),
    )
    .unwrap();
    // The fold epoch, before the first read of it.
    let shard_state = dir.join("metadata").join("_shard_state");
    std::fs::create_dir_all(&shard_state).unwrap();
    std::fs::write(
        shard_state.join("staged_authority_epoch.json"),
        br#"{"schema":1,"epoch":1,"activated_at":1,"activated_by":"test","binary_version":"test"}"#,
    )
    .unwrap();
    let upload = dir.join("mpus_in_progress").join("upload-1");
    std::fs::create_dir_all(&upload).unwrap();
    std::fs::write(
        upload.join("part1.json"),
        format!(r#"{{"compressed_size":{P}}}"#),
    )
    .unwrap();
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        dir.to_path_buf(),
        false,
        0,
        1024 * 1024 * 1024,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(315_360_000),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        false,
        SharedStorageConfig::default(),
        10.0,
        false,
        Duration::from_secs(86400),
        MetadataCacheConfig::default(),
        95,
        80,
        true,
        Duration::from_secs(60),
        1_048_576,
        false,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));
    let _ = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await.expect("cache init");
    install(dir, &Shard::new("bucket", "aa"), Some(SUM));
    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_manager(Arc::clone(&cache_manager));
    let cache = metrics_manager
        .collect_metrics()
        .await
        .cache
        .expect("the cache section");
    assert_eq!(
        cache.in_progress_part_bytes,
        Some(P),
        "PRECONDITION: the walk must see the part record, or P is 0 and this proves nothing"
    );
    assert_eq!(
        cache.st_size_fold.shard_sum_bytes,
        Some(SUM),
        "PRECONDITION: the seeded document must be read"
    );
    assert_eq!(
        cache.total_cache_size,
        TOTAL_SIZE + P,
        "PRECONDITION: total_cache_size must hold total_size plus P, or the operand cannot matter"
    );
    assert_eq!(
        cache.st_size_fold.shard_sum_drift_bytes,
        Some((SUM - TOTAL_SIZE) as i64),
        "drift is the sum minus total_size (7000), whatever P is"
    );
}
