//! `refresh_cache_ttl` must not replace a live `.meta` with an obsolete format.
//!
//! # What this pins
//!
//! `CacheManager::refresh_cache_ttl` is best-effort TTL bookkeeping on the
//! download-coordination revalidation paths (`serve_from_cache_validated`,
//! `serve_range_from_cache_validated`, `serve_cached_part_validated`, each of which
//! has production callers in `http_proxy`). It used to run a second block, commented
//! "Also try old format for backward compatibility", which — whenever
//! `get_cached_response` returned `Some` and the `locks/{key}.lock` sentinel was
//! acquired — serialised that `CacheEntry` onto `get_new_metadata_file_path`, the
//! **current** `.meta` path. Three consequences, all on the common case of a
//! non-expired fully-cached entry:
//!
//! * `NewCacheMetadata::object_metadata` is a required field and `CacheEntry` has no
//!   such field, so the published document did not parse as `NewCacheMetadata`.
//! * `CacheEntry::ranges` is a different shape and empty on that path, so every
//!   `.bin` the entry owned was dereferenced — and therefore orphaned, with no
//!   accounting debit.
//! * `CacheEntry::body` inlines the whole object as a JSON integer array, so the
//!   `.meta` grew to several times the object's size.
//!
//! # The predicate these tests assert
//!
//! **The document on disk**, not `refresh_cache_ttl`'s return value. Every failure
//! inside that block was a `warn!` and the function returns `Ok(())` either way, so
//! an assertion on the result cannot fail and cannot detect this.
//!
//! # Fixture shape, and why it must be this shape
//!
//! The deleted block was gated on `get_cached_response` returning `Some`, which needs
//! a non-expired **fully cached** entry — a partial-range entry returns `None` and the
//! block never ran, so a fixture built from partial ranges would exercise nothing.
//! These tests therefore store a whole object through `store_response` and assert as a
//! precondition that the `.meta` names at least one range and that the range file
//! exists on disk. Without a real `.bin` the orphaning half of the defect is
//! unobservable.
//!
//! The manager is deliberately built **without** `initialize()`: that starts a census
//! which deletes an unparseable `.meta` as soon as it finds one, which would race the
//! very state under test.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{CacheMetadata, NewCacheMetadata};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// A manager with background tasks left unstarted (no `initialize()`), RAM caching
/// off so `get_cached_response` is answered from disk, and a generous `get_ttl` so the
/// seeded entry is stored-fresh and the guard is reached.
fn manager_without_background_tasks(cache_dir: PathBuf) -> Arc<CacheManager> {
    let manager = Arc::new(CacheManager::new_with_all_ttls(
        cache_dir,
        false, // RAM cache disabled: force the disk answer
        0,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        true,
        Duration::from_secs(3600),
        Duration::from_secs(1800),
        Duration::from_secs(3600),
        false,
    ));
    // The journal consolidator has to exist for the store path, but nothing here
    // needs the census or the other background tasks `initialize()` starts.
    let _disk_cache = manager.create_configured_disk_cache_manager();
    manager
}

fn object_metadata(len: usize) -> CacheMetadata {
    CacheMetadata {
        etag: "\"meta-format-etag\"".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: len as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    }
}

/// Every `.bin` under the cache directory, by relative path.
fn range_files(cache_dir: &Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut stack = vec![cache_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("bin") {
                found.insert(
                    path.strip_prefix(cache_dir)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
    }
    found
}

/// The `(start, end, file_path)` triples the `.meta` names.
fn range_references(metadata: &NewCacheMetadata) -> BTreeSet<(u64, u64, String)> {
    metadata
        .ranges
        .iter()
        .map(|r| (r.start, r.end, r.file_path.clone()))
        .collect()
}

/// Parse, or fail with the diagnosis rather than with serde's message alone. A
/// legacy `CacheEntry` is identifiable on the wire: it carries `body` and
/// `is_put_cached` and lacks `object_metadata` — the same markers `disk_cache`'s heal
/// path matches on.
fn parse_or_explain(raw: &str, when: &str) -> NewCacheMetadata {
    match serde_json::from_str::<NewCacheMetadata>(raw) {
        Ok(parsed) => parsed,
        Err(e) => {
            let legacy_shape = raw.contains("\"is_put_cached\"")
                && raw.contains("\"body\"")
                && !raw.contains("\"object_metadata\"");
            panic!(
                "the .meta {when} does not parse as NewCacheMetadata: {e}\n\
                 legacy CacheEntry shape detected (body + is_put_cached, no object_metadata): {legacy_shape}\n\
                 bytes on disk: {}\n\
                 first 400 bytes: {}",
                raw.len(),
                &raw[..raw.len().min(400)]
            );
        }
    }
}

/// A TTL refresh of a fully cached object leaves the `.meta` parseable as the current
/// format and leaves every range it owns still referenced.
#[tokio::test]
async fn refresh_cache_ttl_keeps_the_meta_in_the_current_format_and_keeps_its_ranges() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().to_path_buf();
    let manager = manager_without_background_tasks(cache_dir.clone());

    let cache_key = "meta-format-bucket/whole-object.txt";
    // Compressible, and comfortably above the 1 KiB compression threshold, so the
    // stored range is a real file rather than an edge case.
    let body = vec![b'q'; 8192];

    manager
        .store_response(cache_key, &body, object_metadata(body.len()))
        .await
        .expect("seeding a whole-object entry must succeed");

    let meta_path = manager.get_new_metadata_file_path(cache_key);
    let before_raw = std::fs::read_to_string(&meta_path).expect("the seed must publish a .meta");
    let before = parse_or_explain(&before_raw, "written by the seed");
    let before_refs = range_references(&before);
    let bins_before = range_files(&cache_dir);

    // Fixture preconditions, asserted rather than assumed: the block under test was
    // reachable only for a fully cached entry, and the orphaning is only observable
    // if a real range file exists.
    assert!(
        !before_refs.is_empty(),
        "PRECONDITION: the seeded .meta must name at least one range, else the guard \
         is never reached and this test asserts nothing"
    );
    assert!(
        !bins_before.is_empty(),
        "PRECONDITION: the seed must leave at least one .bin on disk, else the \
         orphaning half of the defect is unobservable"
    );
    assert!(
        manager
            .get_cached_response(cache_key)
            .await
            .expect("cache lookup must succeed")
            .is_some(),
        "PRECONDITION: get_cached_response must return Some, which is the gate the \
         deleted legacy block sat behind"
    );

    manager
        .refresh_cache_ttl(cache_key)
        .await
        .expect("refresh_cache_ttl is best-effort and returns Ok either way");

    let after_raw = std::fs::read_to_string(&meta_path).expect("the .meta must still exist");
    let after = parse_or_explain(&after_raw, "left behind by refresh_cache_ttl");

    assert_eq!(
        before_refs,
        range_references(&after),
        "refresh_cache_ttl changed the ranges the .meta references; the range files on \
         disk are {bins_before:?}, which would now be orphaned and undebited"
    );
    assert_eq!(
        bins_before,
        range_files(&cache_dir),
        "refresh_cache_ttl changed which range files exist on disk"
    );
    assert_eq!(
        before.object_metadata.etag, after.object_metadata.etag,
        "refresh_cache_ttl must preserve the object metadata, not republish it"
    );
    assert!(
        !after_raw.contains("\"is_put_cached\""),
        "the .meta carries a legacy CacheEntry marker after a TTL refresh: {}",
        &after_raw[..after_raw.len().min(400)]
    );
    assert!(
        after_raw.len() < body.len(),
        "the .meta is {} bytes for an {}-byte object, so the body has been inlined \
         into the metadata document",
        after_raw.len(),
        body.len()
    );
}

/// The refresh still does its job: both TTLs move forward. This is the other side of
/// the deletion — the modern block, which ran first all along, is what performs the
/// refresh, so removing the legacy block must not reduce the behaviour.
#[tokio::test]
async fn refresh_cache_ttl_still_extends_both_ttls() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().to_path_buf();
    let manager = manager_without_background_tasks(cache_dir.clone());

    let cache_key = "meta-format-bucket/ttl-moves.txt";
    let body = vec![b'r'; 8192];

    manager
        .store_response(cache_key, &body, object_metadata(body.len()))
        .await
        .expect("seeding a whole-object entry must succeed");

    let meta_path = manager.get_new_metadata_file_path(cache_key);
    let before = parse_or_explain(
        &std::fs::read_to_string(&meta_path).expect("the seed must publish a .meta"),
        "written by the seed",
    );

    // The TTLs are absolute instants computed from `now`, so the clock has to move.
    tokio::time::sleep(Duration::from_millis(20)).await;

    manager
        .refresh_cache_ttl(cache_key)
        .await
        .expect("refresh_cache_ttl is best-effort and returns Ok either way");

    let after = parse_or_explain(
        &std::fs::read_to_string(&meta_path).expect("the .meta must still exist"),
        "left behind by refresh_cache_ttl",
    );

    assert!(
        after.expires_at > before.expires_at,
        "object expires_at did not move: before={:?} after={:?}",
        before.expires_at,
        after.expires_at
    );
    assert!(
        after.head_expires_at > before.head_expires_at,
        "head_expires_at did not move: before={:?} after={:?}",
        before.head_expires_at,
        after.head_expires_at
    );
}
