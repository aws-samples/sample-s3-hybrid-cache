//! Startup writes no `size_tracking/validation.json`, in any state.
//!
//! The document used to have a second producer, the startup consistency pass, which wrote it
//! with `drift_bytes: 0` on the premise that the startup census installed one phase later. The
//! census is gone (a proxy does not count the cache on start), and so is that writer: only the
//! validation pass under `validation.lock` writes the document. An absent or overdue document
//! makes startup ask the scheduler for that pass instead.
use s3_proxy::cache_initialization_coordinator::CacheInitializationCoordinator;
use s3_proxy::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker, ValidationRequestKind};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{CacheConfig, EvictionAlgorithm, InitializationConfig, SharedStorageConfig};
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

async fn build_size_tracker(cache_dir: &std::path::Path) -> Arc<CacheSizeTracker> {
    std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
    std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
    std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.to_path_buf(),
        "validation-shape-test".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.join("locks"),
        Duration::from_secs(30),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig {
            // 0 disables eviction, so a cycle cannot spawn a pass that competes with the
            // assertions below.
            max_cache_size: 0,
            ..ConsolidationConfig::default()
        },
    ));
    consolidator.initialize().await.unwrap();

    let size_config = CacheSizeConfig {
        validation_time_of_day: "00:00".to_string(),
        validation_enabled: true,
        validation_max_duration: Duration::from_secs(4 * 3600),
        ..Default::default()
    };
    Arc::new(
        CacheSizeTracker::new(cache_dir.to_path_buf(), size_config, false, consolidator)
            .await
            .unwrap(),
    )
}

fn cache_config(cache_dir: std::path::PathBuf) -> CacheConfig {
    CacheConfig {
        cache_dir,
        max_cache_size: 1024 * 1024 * 1024,
        ram_cache_enabled: false,
        max_ram_cache_size: 0,
        eviction_algorithm: EvictionAlgorithm::TinyLFU,
        write_cache_enabled: false,
        write_cache_percent: 0.5,
        write_cache_max_object_size: 1024 * 1024 * 100,
        disk_safety_free_space_floor: 1024 * 1024 * 1024,
        put_ttl: Duration::from_secs(86400),
        get_ttl: Duration::from_secs(3600),
        head_ttl: Duration::from_secs(300),
        actively_remove_cached_data: false,
        shared_storage: SharedStorageConfig::default(),
        range_merge_gap_threshold: 1024 * 1024,
        eviction_buffer_percent: 5,
        ram_cache_flush_interval: Duration::from_secs(60),
        ram_cache_verification_interval: Duration::from_secs(1),
        incomplete_upload_ttl: Duration::from_secs(86400),
        initialization: InitializationConfig::default(),
        cache_bypass_headers_enabled: true,
        metadata_cache: s3_proxy::config::MetadataCacheConfig::default(),
        eviction_trigger_percent: 95,
        eviction_target_percent: 80,
        full_object_check_threshold: 67_108_864,
        disk_streaming_threshold: 1_048_576,
        compression_batch_size: 1_048_576,
        read_cache_enabled: true,
        bucket_settings_staleness_threshold: Duration::from_secs(60),
        download_coordination: s3_proxy::config::DownloadCoordinationConfig::default(),
        evaluate_conditions_from_cache: false,
        ram_cache_shard_count: 64,
        max_complete_body_bytes: 10 * 1024 * 1024,
        max_metadata_file_bytes: 4 * 1024 * 1024,
        metadata_io_concurrency: 32,
        partial_range_commit_ratio: 0.5,
    }
}

fn seeded_metadata(cache_key: &str, compressed: u64, uncompressed: u64) -> NewCacheMetadata {
    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: "shape-test-etag".to_string(),
            content_type: Some("application/octet-stream".to_string()),
            content_length: uncompressed,
            last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
            is_write_cached: false,
            ..Default::default()
        },
        ranges: vec![RangeSpec {
            start: 0,
            end: uncompressed - 1,
            file_path: "shape-test.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: compressed,
            uncompressed_size: uncompressed,
            created_at: SystemTime::now(),
            last_accessed: SystemTime::now(),
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        }],
        created_at: SystemTime::now(),
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    }
}

fn seed_meta_files(cache_dir: &std::path::Path) {
    let metadata_dir = cache_dir.join("metadata");
    std::fs::create_dir_all(&metadata_dir).unwrap();
    for (name, compressed, uncompressed) in [
        ("shape-one", 512u64, 1024u64),
        ("shape-two", 4096, 8192),
        ("shape-three", 1024, 2048),
    ] {
        let metadata = seeded_metadata(&format!("bucket/{name}"), compressed, uncompressed);
        std::fs::write(
            metadata_dir.join(format!("{name}.meta")),
            serde_json::to_string(&metadata).unwrap(),
        )
        .unwrap();
    }
}

/// No `validation.json` and no `size_state.json` on a cache holding three objects: startup
/// counts nothing, writes no `validation.json`, and requests a full validation.
///
/// # RED SIDE
///
/// On the earlier tree the absent document fails the freshness gate, startup walks the three
/// `.meta` files and the consistency pass writes `validation.json`.
#[tokio::test]
async fn startup_writes_no_validation_json_and_requests_a_pass() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    seed_meta_files(&cache_dir);
    let tracker = build_size_tracker(&cache_dir).await;
    let validation_path = cache_dir.join("size_tracking").join("validation.json");
    assert!(
        !validation_path.exists(),
        "FIXTURE: validation.json must not exist before the run"
    );
    let mut size_tracker = Some(tracker.clone());
    let summary = CacheInitializationCoordinator::new(
        cache_dir.clone(),
        false,
        cache_config(cache_dir.clone()),
    )
    .initialize_with_locking(None, &mut size_tracker)
    .await
    .expect("coordinated initialization must succeed");

    assert_eq!(
        summary.performance_metrics.files_processed, 0,
        "STARTUP-WALK: startup processed .meta files"
    );
    assert!(
        !validation_path.exists(),
        "STARTUP-WROTE-VALIDATION: startup wrote validation.json, which only the validation \
         pass under validation.lock may write"
    );
    assert_eq!(
        tracker.validation_request().map(|r| r.kind),
        Some(ValidationRequestKind::FigureUnvalidated),
        "a missing size_state.json on a cache holding data must request a full validation"
    );
}

/// A fresh `validation.json` and a readable `size_state.json`: startup leaves the document
/// byte-identical, requests nothing, and reports the persisted figure.
#[tokio::test]
async fn startup_leaves_a_fresh_validation_json_untouched() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    seed_meta_files(&cache_dir);
    let tracker = build_size_tracker(&cache_dir).await;
    let size_tracking = cache_dir.join("size_tracking");
    std::fs::write(
        size_tracking.join("size_state.json"),
        serde_json::json!({
            "total_size": 5632u64,
            "write_cache_size": 0u64,
            "cached_objects": 3u64,
            "last_consolidation": 1704067200u64,
            "consolidation_count": 1u64,
            "last_updated_by": "shape-test"
        })
        .to_string(),
    )
    .unwrap();
    let now_secs = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let fresh = serde_json::json!({
        "last_validation": now_secs,
        "scanned_size": 5632u64,
        "tracked_size": 5632u64,
        "drift_bytes": 0,
        "scan_duration_ms": 0,
        "metadata_files_scanned": 3,
    })
    .to_string();
    std::fs::write(size_tracking.join("validation.json"), &fresh).unwrap();

    let mut size_tracker = Some(tracker.clone());
    let summary = CacheInitializationCoordinator::new(
        cache_dir.clone(),
        false,
        cache_config(cache_dir.clone()),
    )
    .initialize_with_locking(None, &mut size_tracker)
    .await
    .expect("coordinated initialization must succeed");

    assert_eq!(summary.performance_metrics.files_processed, 0);
    assert_eq!(summary.scan_summary.total_size, 5632);
    assert_eq!(
        std::fs::read_to_string(size_tracking.join("validation.json")).unwrap(),
        fresh,
        "startup rewrote a fresh validation.json"
    );
    assert!(
        tracker.validation_request().is_none(),
        "a fresh validation and a readable size_state.json must request nothing"
    );
}
