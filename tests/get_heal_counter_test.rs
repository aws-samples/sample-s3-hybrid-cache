//! A streamed GET fill whose publish removes an unparseable `.meta` counts one heal on
//! `cache.metadata_heal_overwrite_total`, whether the refill publishes on the same request or
//! declines because the final path holds a range file under 600 s old. A GET of a key whose
//! `.meta` parses counts nothing.
//!
//! The counter is process-global and the assertions are exact deltas, so this file is its own
//! test target and holds one test.
#[path = "common/proxy_rig.rs"]
mod proxy_rig;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hyper::StatusCode;
use proxy_rig::{Rig, RigOptions, STEP};

const LEN: usize = 4096;

/// `cache.metadata_heal_overwrite_total` as `/metrics` reports it.
async fn heals(rig: &Rig) -> u64 {
    let mut metrics = s3_proxy::metrics::MetricsManager::new();
    metrics.set_cache_manager(Arc::clone(&rig.cache_manager));
    metrics
        .collect_metrics()
        .await
        .cache
        .expect("cache metrics")
        .metadata_heal_overwrite_total
}

/// Cache `path`, set its range file's mtime `age` in the past, and replace its `.meta` with
/// text that does not parse. Returns the object's bytes and the `.meta` path.
async fn corrupt(rig: &Rig, path: &str, age: Duration) -> (Bytes, PathBuf) {
    let object = rig.seed_object(path, LEN).await;
    let key = Rig::cache_key(path);
    let bin = rig
        .disk
        .read()
        .await
        .get_new_range_file_path(&key, 0, LEN as u64 - 1);
    std::fs::File::options()
        .write(true)
        .open(&bin)
        .expect("open the cached range file")
        .set_modified(std::time::SystemTime::now() - age)
        .expect("age the cached range file");
    let meta = rig.disk.read().await.get_new_metadata_file_path(&key);
    std::fs::write(&meta, b"{ not a cache entry").expect("write .meta");
    rig.cache_manager.invalidate_metadata_cache(&key).await;
    (object, meta)
}

/// Whether the `.meta` at `meta` parses and lists the whole object.
fn refilled(meta: &PathBuf) -> bool {
    std::fs::read_to_string(meta)
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .is_some_and(|m| {
            m["ranges"].as_array().is_some_and(|r| {
                r.iter().any(|r| {
                    r["start"].as_u64() == Some(0) && r["end"].as_u64() == Some(LEN as u64 - 1)
                })
            })
        })
}

/// Wait at most [`STEP`] for `done`, then return the heals counted since `before`.
async fn heals_after(rig: &Rig, before: u64, done: impl Fn() -> bool) -> u64 {
    let until = tokio::time::Instant::now() + STEP;
    while !(done() && heals(rig).await > before) && tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    heals(rig).await - before
}

#[tokio::test]
async fn a_get_fill_that_removes_an_unparseable_meta_counts_one_heal() {
    let rig = Rig::new(RigOptions::default()).await;

    // Control: a hit on a key whose `.meta` parses.
    let clean = "/bucket/heal-clean";
    let clean_bytes = rig.seed_object(clean, LEN).await;
    let before = heals(&rig).await;
    let (clean_status, clean_body) = rig.get(clean, &[]).await;
    let control = heals(&rig).await - before;

    // The old range file is past the publish window: the refill publishes on the same request.
    let published_path = "/bucket/heal-published";
    let (published_bytes, published_meta) =
        corrupt(&rig, published_path, Duration::from_secs(3600)).await;
    let before = heals(&rig).await;
    let (published_status, published_body) = rig.get(published_path, &[]).await;
    let published = heals_after(&rig, before, || refilled(&published_meta)).await;

    // The range file is under 600 s old: the publish removes the `.meta` and declines.
    let declined_path = "/bucket/heal-declined";
    let (declined_bytes, declined_meta) = corrupt(&rig, declined_path, Duration::ZERO).await;
    let before = heals(&rig).await;
    let (declined_status, declined_body) = rig.get(declined_path, &[]).await;
    let declined = heals_after(&rig, before, || !declined_meta.exists()).await;

    assert_eq!(
        (
            clean_status,
            clean_body == clean_bytes,
            control,
            published_status,
            published_body == published_bytes,
            refilled(&published_meta),
            published,
            declined_status,
            declined_body == declined_bytes,
            declined_meta.exists(),
            declined,
        ),
        (
            StatusCode::OK,
            true,
            0,
            StatusCode::OK,
            true,
            true,
            1,
            StatusCode::OK,
            true,
            false,
            1
        ),
        "(control hit: status, bytes, heals; old range file: status, bytes, refill published, \
         heals; young range file: status, bytes, .meta still present, heals)"
    );
}
