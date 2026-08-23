//! Demonstration: `invalidate_all_ranges` does not clear the RAM range tier.
//!
//! This is the red test for the finding recorded in
//! `.kiro/specs/page-widening-freshness/` § "The actual defect", item 2.
//!
//! **What it proves.** `range_handler.rs`'s ETag-mismatch arm responds to a changed
//! object by calling `DiskCacheManager::invalidate_all_ranges` (`range_handler.rs:696`).
//! That method removes the `.bin` range files and the `.meta`, but it lives on
//! `DiskCacheManager` and has no handle on the RAM range cache, which lives on
//! `CacheManager`. The only three functions that clear RAM ranges —
//! `invalidate_cache_hierarchy`, `force_invalidate_cache`, `invalidate_write_cache_entry`
//! — are all on `CacheManager` and none is reachable from that arm.
//!
//! So after the proxy has detected a version change and invalidated the disk copy, a
//! stale RAM range entry for the old version remains readable. `ShardedRamCache::get`
//! has no expiry concept, so nothing ages it out; it survives until LRU eviction or
//! restart.
//!
//! **Why this matters more on the widened path.** On the mainline ranged path the ETag
//! comparison inside `find_cached_ranges` runs BEFORE `serve_range_from_cache`, so a
//! changed object is caught before any RAM serve. `fill_page` inverts that order: it
//! consults `get_range_from_ram_cache` (`http_proxy.rs:5919`) before
//! `find_page_overlap` (`5944`), which is where the ETag arm lives. The inversion is
//! what makes the gap below reachable as a stale serve rather than merely untidy.
//!
//! **Status.** This test was written red, against the defect described above, and was
//! confirmed failing with the exact `DEFECT:` message at the end of this file. The fix
//! adds `CacheManager::invalidate_ram_ranges` and calls it at each of the three sites
//! that invalidate a stale object (`range_handler.rs`'s ETag-mismatch arm and the two
//! proxy-injected-412 retry paths in `http_proxy.rs`). It is now a regression guard:
//! if it fails again, range invalidation has stopped reaching the RAM tier.

use std::sync::Arc;
use std::time::Duration;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::RangeHandler;
use tempfile::TempDir;

const KEY: &str = "test-bucket/etag-gap-object";
const RANGE_START: u64 = 0;
const RANGE_END: u64 = 1023;
const ETAG_A: &str = "\"version-a-etag\"";
const ETAG_B: &str = "\"version-b-etag\"";

fn version_a_bytes() -> Vec<u8> {
    (0..=(RANGE_END - RANGE_START))
        .map(|i| (i % 251) as u8)
        .collect()
}

/// Count `.bin` range files anywhere under the cache dir. Filesystem-level so the
/// disk-invalidation check cannot be confused by the journal fallback.
fn count_range_files(root: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, acc: &mut usize) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, acc);
                } else if p.extension().and_then(|s| s.to_str()) == Some("bin") {
                    *acc += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(root, &mut n);
    n
}

/// Delete every `.bin` range file under the cache dir.
fn remove_range_files(root: &std::path::Path) {
    fn walk(dir: &std::path::Path) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p);
                } else if p.extension().and_then(|s| s.to_str()) == Some("bin") {
                    let _ = std::fs::remove_file(&p);
                }
            }
        }
    }
    walk(root);
}

fn object_metadata(etag: &str, len: u64) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag.to_string(),
        last_modified: "Fri, 22 Aug 2026 00:00:00 GMT".to_string(),
        content_length: len,
        content_type: Some("application/octet-stream".to_string()),
        ..ObjectMetadata::default()
    }
}

async fn make_infra() -> (
    TempDir,
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
) {
    let mut config = Config::default();
    // The RAM range tier must be on — it is the subject of the test.
    config.cache.ram_cache_enabled = true;

    let temp_dir = TempDir::new().expect("tempdir");
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,
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
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");

    (temp_dir, cache_manager, disk_cache_manager)
}

// `multi_thread` is required, not cosmetic: `get_range_from_ram_cache` and
// `promote_range_to_ram_cache_frame` reach the async RAM cache through
// `block_in_place` (`cache.rs:8321`), which panics on the current-thread runtime
// that `#[tokio::test]` uses by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn etag_invalidation_leaves_stale_ram_range_readable() {
    let (_tmp, cache_manager, disk_cache_manager) = make_infra().await;
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));

    let body = version_a_bytes();
    let len = body.len() as u64;

    // --- Seed version A on disk (the ordinary warm state) -------------------
    range_handler
        .store_range_new_storage(
            KEY,
            RANGE_START,
            RANGE_END,
            &body,
            object_metadata(ETAG_A, len),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("seed disk range");

    // --- Promote the same range to RAM, as a serve would -------------------
    let promoted = cache_manager.promote_range_to_ram_cache_frame(
        KEY,
        (RANGE_START, RANGE_END),
        body.clone(),
        s3_proxy::compression::CompressionAlgorithm::None,
        ETAG_A.to_string(),
        String::new(),
    );
    assert!(
        promoted,
        "PRECONDITION FAILED: the range was not promoted to RAM, so this test would \
         prove nothing. Check ram_cache_enabled and max_ram_cache_size."
    );

    // --- Precondition: both tiers hold version A --------------------------
    let ram_before = cache_manager.get_range_from_ram_cache(KEY, RANGE_START, RANGE_END);
    assert_eq!(
        ram_before.as_deref(),
        Some(body.as_slice()),
        "PRECONDITION FAILED: RAM should hold version A before invalidation"
    );

    let bins_before = count_range_files(_tmp.path());
    assert!(
        bins_before > 0,
        "PRECONDITION FAILED: expected at least one .bin range file on disk, found none"
    );

    // --- The event: drive the real ETag-mismatch arm ----------------------
    // Deliberately NOT a direct `invalidate_all_ranges` call. That function is on
    // `DiskCacheManager` and cannot clear RAM by construction, so asserting against
    // it directly would test a contract nothing can satisfy. The behaviour that
    // matters is the one callers get, so this drives `find_cached_ranges` with a
    // `current_etag` that disagrees with the cached one — the real path at
    // `range_handler.rs`'s mismatch arm.
    //
    // `preloaded_metadata` is supplied because the arm only runs when metadata is
    // present, and a range written moments ago is still in the journal, so a disk
    // read would return `None` and skip the check entirely (see OBSERVATION 1).
    let preloaded = s3_proxy::cache_types::NewCacheMetadata {
        cache_key: KEY.to_string(),
        object_metadata: object_metadata(ETAG_A, len),
        ranges: vec![s3_proxy::cache_types::RangeSpec {
            start: RANGE_START,
            end: RANGE_END,
            file_path: String::new(),
            compression_algorithm: s3_proxy::compression::CompressionAlgorithm::None,
            compressed_size: len,
            uncompressed_size: len,
            created_at: std::time::SystemTime::now(),
            last_accessed: std::time::SystemTime::now(),
            access_count: 0,
        }],
        ..Default::default()
    };

    let overlap = range_handler
        .find_cached_ranges(
            KEY,
            &s3_proxy::range_handler::RangeSpec {
                start: RANGE_START,
                end: RANGE_END,
            },
            Some(ETAG_B),
            Some(&preloaded),
        )
        .await
        .expect("find_cached_ranges with mismatching etag");

    assert!(
        !overlap.can_serve_from_cache && overlap.cached_ranges.is_empty(),
        "PRECONDITION FAILED: the ETag-mismatch arm did not fire, so nothing was \
         invalidated and this test would prove nothing. Got can_serve={} cached={}",
        overlap.can_serve_from_cache,
        overlap.cached_ranges.len()
    );

    // --- Disk is correctly invalidated -----------------------------------
    // Checked at the filesystem level rather than through `find_cached_ranges`,
    // deliberately. `find_cached_ranges` falls back to pending journal entries
    // when the `.meta` is absent (`disk_cache.rs`, the `metadata.is_none()`
    // branch), and a range written moments earlier is still in the journal, so
    // that call keeps reporting the range after invalidation. That is a separate
    // observation — noted below — and using it here would make this test fail for
    // a reason unrelated to the RAM tier.
    // Recorded as observations, not asserted. Both were discovered while building
    // this test and are separate from its subject, so asserting on them here would
    // make the test fail for reasons unrelated to the RAM tier. Each deserves its
    // own investigation.
    //
    // OBSERVATION 1 — within the journal window, `invalidate_all_ranges` removes no
    // range files at all. `remove_all_ranges` locates `.bin` files by reading the
    // object's metadata; for a range written moments earlier that metadata is still
    // in the per-instance journal rather than a consolidated `.meta`, so
    // `get_metadata` returns `None` and the function returns `(0, 0)` early. The
    // ETag-mismatch arm is therefore a no-op for a range in that window.
    //
    // OBSERVATION 2 — `find_cached_ranges` keeps reporting the range afterwards,
    // because it falls back to pending journal entries when the `.meta` is absent.
    //
    // Scope caveat on both: no consolidator task runs in this test, so the window is
    // unbounded HERE. In production it is bounded by the consolidation interval. The
    // mechanism is real; the duration observed here is a fixture artefact and must
    // not be quoted as the production exposure.
    let bins_after = count_range_files(_tmp.path());
    let journal_visible = disk_cache_manager
        .read()
        .await
        .find_cached_ranges(KEY, RANGE_START, RANGE_END, None)
        .await
        .map(|r| !r.is_empty())
        .unwrap_or(false);
    println!(
        "OBSERVATION: .bin files before={bins_before} after={bins_after} \
         (0 after would mean invalidation reached the files); \
         journal-fallback still reports the range: {journal_visible}"
    );

    // Because of OBSERVATION 1 the invalidation call may have left the range files
    // in place, which would leave the end state ambiguous. Remove them directly so
    // disk genuinely holds nothing for this key by any means. This stands in for a
    // completed invalidation outside the journal window, and makes the assertion
    // below unambiguous: every disk trace of version A is gone.
    remove_range_files(_tmp.path());
    assert_eq!(
        count_range_files(_tmp.path()),
        0,
        "fixture: range files should be gone after direct removal"
    );

    // --- THE DEFECT ------------------------------------------------------
    // The proxy has established that the cached version is superseded and has
    // removed it from disk. The RAM copy of the same bytes, under the same key,
    // is still readable and has no expiry.
    //
    // This assertion states the CORRECT behaviour and FAILS on current code.
    // A fix must make range invalidation reach the RAM tier — e.g. by routing
    // the ETag-mismatch arm through `CacheManager::invalidate_cache_hierarchy`
    // (which already calls `remove_from_ram_cache_unified`) instead of calling
    // `DiskCacheManager::invalidate_all_ranges` directly.
    let ram_after = cache_manager.get_range_from_ram_cache(KEY, RANGE_START, RANGE_END);
    assert!(
        ram_after.is_none(),
        "DEFECT: the ETag-mismatch arm fired and every disk trace of version A is \
         gone, yet the RAM range tier still serves {} stale bytes for {} under the \
         superseded ETag {}. `invalidate_all_ranges` lives on DiskCacheManager and \
         has no handle on the RAM tier, and `ShardedRamCache::get` has no expiry \
         concept, so this entry survives until LRU eviction or process restart. Fix: \
         the arm must also call `CacheManager::invalidate_ram_ranges`.",
        ram_after.as_ref().map(|b| b.len()).unwrap_or(0),
        KEY,
        ETAG_A
    );
}
