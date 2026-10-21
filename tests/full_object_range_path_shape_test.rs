//! Where does a full-object GET store put its `.bin`, and under what name?
//!
//! Spec: cache-eviction-at-scale. Establishes a precondition for the
//! filesystem-as-index design (readdir of `ranges/`, extent parsed from the
//! filename) rather than testing a defect.
//!
//! # The question, and why reading the code does not settle it
//!
//! There are **two** inherent producers of the `_{start}-{end}.bin` name and only
//! one of them shards:
//!
//! Citations below are by SYMBOL NAME, not line number. Three line citations in this
//! file drifted by ~1,200-2,200 lines between 2026-08-30 and 2026-09-11 and one of
//! them resolved to unrelated code; symbol names cannot drift the same way.
//!
//! - `DiskCacheManager::get_new_range_file_path` (`src/disk_cache.rs`) builds
//!   the suffix and routes it through `get_sharded_path`, giving
//!   `ranges/{bucket}/{XX}/{YYY}/{name}`.
//! - `CacheManager::get_new_range_file_path` (`src/cache.rs`) joined `ranges/`
//!   with `{sanitized_cache_key}_{start}-{end}.bin` directly. No sharding. **This is
//!   the producer that was fixed** — it now routes through the same
//!   `disk_cache::get_sharded_path`, with the flat layout kept only as a fallback for
//!   a cache key that will not parse.
//!
//! `CacheManager::store_full_object_as_range_new` (`src/cache.rs`) calls
//! `self.get_new_range_file_path(...)`, so it resolved to the `CacheManager` one. But
//! the comment a few dozen lines below that call — "Must use full relative path
//! (bucket/XX/YYY/object_0-1023.bin), not just filename" — asserted the opposite, and
//! `strip_prefix` on an unsharded path yields a single component, not three.
//!
//! Code and comment disagree, so this reads the filesystem instead. Per
//! the cache tier verification guidance, the
//! resolution for a contested provenance claim is a runtime signature, not more
//! reading.
//!
//! # What it constrains
//!
//! Two independent properties, asserted separately so a failure names which one broke:
//!
//! 1. **The extent is in the filename.** `_{start}-{end}.bin`, for a full object
//!    `_0-{len-1}.bin`. This is what lets a readdir-only sweep recover each range's
//!    extent without opening anything, measured at 100% of 107,649 files
//!    (`requirements.md` § Measured state).
//! 2. **Where the file sits under `ranges/`.** A shard-rooted sweep
//!    (`ranges/{bucket}/{XX}/{YYY}`) is only complete if every producer shards.
//!
//! It drives the **public** `CacheManager::store_response_with_headers`, which reaches
//! `store_full_object_as_range_new` unconditionally. Driving the real entry point
//! rather than the private helper is deliberate: it is the production GET-store path,
//! so the answer cannot be an artefact of calling an inner function directly.
//! `store_full_object_as_range_new` has exactly two production callers,
//! `CacheManager::store_response_in_hierarchy` and
//! `CacheManager::store_response_with_headers`; this test drives the second.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const KEY: &str = "test-bucket/path/to/object.dat";
const LEN: u64 = 4096;

fn setup(cache_dir: &Path) -> CacheManager {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let manager = CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false, // ram_cache_enabled: off, so the disk write is the only store
        0,
        CacheEvictionAlgorithm::TinyLFU,
    );
    let _ = manager.create_configured_disk_cache_manager();
    manager
}

/// Every `.bin` under `dir`, recursively. Deliberately not filtered by depth — the
/// depth is the thing under test.
fn collect_bins(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_bins(&path, out);
            } else if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                out.push(path);
            }
        }
    }
}

/// # STATUS: FIXED — this test is the red-then-green evidence
///
/// Measured 2026-08-30, before the fix. Property 1 (extent in the filename)
/// **passed**. Property 2 (sharded location) **failed**, with this output:
///
/// ```text
///   .bin relative to ranges/ : test-bucket%2Fpath%2Fto%2Fobject.dat_0-4095.bin
///   path components          : 1
///   sharded producer would be: test-bucket/29/cc1/path%2Fto%2Fobject.dat_0-4095.bin
///   .meta records file_path  : test-bucket%2Fpath%2Fto%2Fobject.dat_0-4095.bin
///   .meta relative to cache_ : metadata/test-bucket/29/cc1/path%2Fto%2Fobject.dat.meta
/// ```
///
/// So a full-object GET store wrote its `.bin` **flat at the `ranges/` root**, with
/// the entire cache key percent-encoded into a single filename, while its `.meta`
/// went into the **sharded** `metadata/` tree. The `.meta` recorded the flat relative
/// path, so reads stayed self-consistent and nothing was user-visibly broken — every
/// read joins `ranges/` onto `file_path` and finds the file regardless of shard depth.
///
/// What it broke was any traversal scoped to `ranges/{bucket}/{XX}/{YYY}`:
///
/// 1. A shard-rooted readdir sweep never saw these files.
/// 2. Orphan reclamation as specified in `design.md` § 6.8 diffs a shard's `.meta`
///    extents against a readdir of the sibling `ranges/` leaf. For a full-object
///    entry the `.meta` is found, claims its extent, and the leaf is empty — so the
///    range read as a **missing `.bin`** when it was merely elsewhere, and the flat
///    file was simultaneously unreachable by any shard-scoped orphan scan.
/// 3. The same applied to `background_recovery.rs::cleanup_orphan_bin_files`, which
///    walks `ranges/bucket/XX/YYY`.
///
/// Fixed by routing `CacheManager::get_new_range_file_path` (`src/cache.rs`) through
/// the same `crate::disk_cache::get_sharded_path` the `DiskCacheManager` producer uses,
/// with the same flat-path fallback on a parse failure. Existing flat `.bin` files
/// from before this fix remain readable — see `CHANGELOG.md`'s `**Upgrade impact:**`
/// note — but are no longer produced.
#[tokio::test]
async fn full_object_get_store_writes_extent_in_filename_and_reports_its_shard_depth() {
    let temp = tempfile::TempDir::new().unwrap();
    let cache_dir = temp.path();
    let manager = setup(cache_dir);

    let metadata = CacheMetadata {
        etag: "\"probe\"".to_string(),
        last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
        content_length: LEN,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };
    let mut headers = HashMap::new();
    headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );

    manager
        .store_response_with_headers(KEY, &vec![9u8; LEN as usize], headers, metadata)
        .await
        .expect("full-object GET store should succeed");

    let ranges_dir = cache_dir.join("ranges");
    let mut bins = Vec::new();
    collect_bins(&ranges_dir, &mut bins);

    assert_eq!(
        bins.len(),
        1,
        "expected exactly one .bin for a single full-object store, found {:?}",
        bins
    );
    let rel = bins[0].strip_prefix(&ranges_dir).unwrap();
    let depth = rel.components().count();
    let file_name = rel.file_name().unwrap().to_string_lossy().to_string();

    // What the SHARDED producer would have chosen for the same key and extent.
    let disk_cache = manager.create_configured_disk_cache_manager();
    let sharded = disk_cache.get_new_range_file_path(KEY, 0, LEN - 1);
    let sharded_rel = sharded.strip_prefix(&ranges_dir).unwrap();

    // The relative path recorded in the `.meta`, which is what every read joins onto
    // `ranges/`. If this and the on-disk path disagree the entry is unreadable, so it
    // is worth reporting alongside.
    let meta_path = manager.get_new_metadata_file_path(KEY);
    let meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
    let meta_file_path = meta["ranges"][0]["file_path"]
        .as_str()
        .unwrap_or("<absent>");

    println!("--- full-object GET store: on-disk shape ---");
    println!("  .bin relative to ranges/ : {}", rel.display());
    println!("  path components          : {}", depth);
    println!("  filename                 : {}", file_name);
    println!("  sharded producer would be: {}", sharded_rel.display());
    println!("  .meta records file_path  : {}", meta_file_path);
    println!(
        "  .meta relative to cache_ : {}",
        meta_path.strip_prefix(cache_dir).unwrap().display()
    );

    // Property 1: the extent is recoverable from the filename alone.
    assert!(
        file_name.ends_with(&format!("_0-{}.bin", LEN - 1)),
        "a full-object store must carry its extent in the filename as _0-{}.bin, \
         so a readdir-only sweep can recover it without opening the file; got {:?}",
        LEN - 1,
        file_name
    );

    // Property 2: the file is where a shard-rooted sweep would look. Asserted against
    // the sharded producer's own output rather than against a hardcoded depth, so the
    // assertion cannot drift from the sharding scheme.
    assert_eq!(
        rel, sharded_rel,
        "a full-object GET store must land at the same sharded path a ranged store \
         would use, or a sweep rooted at ranges/{{bucket}}/{{XX}}/{{YYY}} is \
         incomplete. `CacheManager::store_full_object_as_range_new` derives its `.bin` \
         path through `CacheManager::get_new_range_file_path`, which must shard through \
         `disk_cache::get_sharded_path` exactly as `DiskCacheManager::get_new_range_file_path` \
         does — a flat `ranges/`-root path here means one of the two has stopped sharding"
    );
}
