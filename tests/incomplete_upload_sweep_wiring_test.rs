//! The incomplete-upload sweep is wired to `cache.incomplete_upload_ttl` at both of its call
//! sites: the pass `CacheManager::initialize` runs at startup, and the consolidator's
//! maintenance cycle. `write_cache_manager::incomplete_upload_sweep_tests` tests the sweep
//! function; these tests start from a configured `CacheManager`, so a sweep that is never
//! called, or called with the wrong TTL, fails them.
//!
//! Each test places two upload directories under `mpus_in_progress/`, one idle past the TTL and
//! one live, and asserts the idle one is reaped and counted while the live one survives.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tempfile::TempDir;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;

const TTL: Duration = Duration::from_secs(3600);

fn manager(dir: &Path) -> Arc<CacheManager> {
    let mut config = Config::default();
    config.cache.cache_dir = dir.to_path_buf();
    config.cache.ram_cache_enabled = false;
    config.cache.write_cache_enabled = true;
    config.cache.incomplete_upload_ttl = TTL;
    Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        config.compression.threshold,
        config.compression.enabled,
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        config.cache.ram_cache_flush_interval,
        config.cache.ram_cache_shard_count,
        config.connection_pool.upstream_first_byte_timeout,
    ))
}

/// An upload directory with a tracker and one part, last active `age` ago.
fn upload(dir: &Path, name: &str, age: Duration) -> PathBuf {
    let path = dir.join("mpus_in_progress").join(name);
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("upload.meta"), b"{}").unwrap();
    std::fs::write(path.join("part1.bin"), vec![1u8; 512]).unwrap();
    let when = filetime::FileTime::from_system_time(SystemTime::now() - age);
    filetime::set_file_mtime(path.join("upload.meta"), when).unwrap();
    filetime::set_file_mtime(path.join("part1.bin"), when).unwrap();
    filetime::set_file_mtime(&path, when).unwrap();
    path
}

/// The startup pass reaps an upload idle past the TTL and keeps a live one, and counts it.
///
/// # RED SIDE
///
/// With the sweep call removed from `CacheManager::initialize`, the idle directory survives
/// until the first maintenance cycle, so the check right after `initialize` finds it.
#[tokio::test]
async fn the_startup_pass_reaps_an_upload_idle_past_the_ttl() {
    let tmp = TempDir::new().unwrap();
    let idle = upload(tmp.path(), "idle-at-startup", Duration::from_secs(7200));
    let live = upload(tmp.path(), "live-at-startup", Duration::from_secs(10));
    let cache = manager(tmp.path());
    let _disk = cache.create_configured_disk_cache_manager();
    cache.initialize().await.expect("cache init");
    assert_eq!(
        (
            idle.exists(),
            live.exists(),
            cache.get_statistics().incomplete_uploads_evicted
        ),
        (false, true, 1),
        "(idle upload still present, live upload still present, uploads counted as evicted) \
         after initialize with incomplete_upload_ttl {:?}",
        TTL
    );
}

/// The consolidator's maintenance cycle reaps an upload that went idle after startup, using the
/// configured TTL.
///
/// # RED SIDE
///
/// With `CacheManager` not handing `incomplete_upload_ttl` to its consolidator, the consolidator
/// keeps its one-day default and an upload idle for two hours survives every cycle.
#[tokio::test]
async fn a_maintenance_cycle_reaps_an_upload_idle_past_the_configured_ttl() {
    let tmp = TempDir::new().unwrap();
    let cache = manager(tmp.path());
    let _disk = cache.create_configured_disk_cache_manager();
    cache.initialize().await.expect("cache init");
    cache.set_cache_manager_in_consolidator().await;
    let idle = upload(tmp.path(), "idle-after-startup", Duration::from_secs(7200));
    let live = upload(tmp.path(), "live-after-startup", Duration::from_secs(10));
    assert!(
        idle.exists() && live.exists(),
        "fixture: both uploads exist before the cycle"
    );
    let consolidator = cache
        .get_journal_consolidator()
        .await
        .expect("the cache manager owns a consolidator");
    consolidator
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    consolidator.await_maintenance_passes().await;
    assert_eq!(
        (
            idle.exists(),
            live.exists(),
            cache.get_statistics().incomplete_uploads_evicted
        ),
        (false, true, 1),
        "(idle upload still present, live upload still present, uploads counted as evicted) \
         after a maintenance cycle with incomplete_upload_ttl {:?}",
        TTL
    );
}
