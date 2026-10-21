//! Does `CacheValidator::validate_consistency_integrity` check anything?
//!
//! It is reached on a production boot — `CacheManager::initialize` →
//! `CacheInitializationCoordinator::initialize_with_locking` → `initialize` →
//! `scan_cache_metadata_with_consistency_validation` →
//! `perform_metadata_consistency_validation`, once per scanned `.meta` — but only when
//! `size_tracking/validation.json` is absent, unparseable, or older than 23 hours. See
//! the module note in `src/cache_validator.rs` on `get_metadata_path_integrity`.
//!
//! Citations below are by SYMBOL NAME, never line number.
//!
//! # The two defects this pins, and why one hides the other
//!
//! `validate_consistency_integrity` resolved a `.meta` for itself instead of asking the
//! production helper, and then resolved the range files it references against the wrong
//! base directory. Both are the same mistake — rebuilding the cache layout locally —
//! and the first one masked the second, because a metadata load failure returns early.
//!
//! 1. **`get_metadata_path_integrity` built a flat path.**
//!    `metadata/{cache_key with / : \ replaced by _}.meta`, where the production
//!    producers write `metadata/{bucket}/{XX}/{YYY}/{percent-encoded key}.meta` via
//!    `disk_cache::get_sharded_path`. So `load_metadata_integrity` took its `Err` arm
//!    for **every** entry, the function pushed
//!    `MetadataIntegrityError::InvalidJson("Failed to load metadata: …")` and returned
//!    `is_consistent: false`, and `perform_metadata_consistency_validation` counted a
//!    corruption and logged `"… 0 missing files, 0 orphaned files"` — two zero counts
//!    from a check that had not run.
//!
//! 2. **The range-file existence check joined the wrong base.**
//!    `self.cache_dir.join(&range.file_path)`, where `RangeSpec::file_path` is relative
//!    to `cache_dir/ranges` — the convention every read site in `src/cache.rs` uses, and
//!    the one `cache_types::classify_new_range_as_staged`'s doc comment records as
//!    holding for every live write path (`strip_prefix(cache_dir/ranges)`). So fixing
//!    only defect 1 moves the verdict from `InvalidJson` to `RangeFileMissing` and
//!    leaves `is_consistent: false` for every entry. Asserting only on defect 1 would
//!    therefore have shipped a check that still reported corruption for the whole cache.
//!
//! A third member of the family — `orphaned_range_files`, bound with `let` and never
//! pushed to — was deleted rather than implemented. Orphan reclamation already exists at
//! `background_recovery::cleanup_orphan_bin_files`, an orphan verdict taken from one
//! instance's view of a shared volume is the shape behind two prior deletion incidents,
//! and enumerating a shard per validated entry would be quadratic over a startup sweep.
//!
//! # Fixture discipline
//!
//! The entry is written by the **production** GET-store path,
//! `CacheManager::store_response_with_headers` (which reaches
//! `store_full_object_as_range_new` unconditionally), not hand-placed at a path chosen
//! here — otherwise the test would assert this file's own arithmetic rather than the
//! producer's. `KEY` deliberately contains `/`, so the retired flat name and the real
//! sharded name genuinely differ; a key with no separator can collapse to a name that
//! resolves by accident and would make a passing assertion prove nothing.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::cache_validator::{CacheValidator, MetadataIntegrityError};
use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;

const KEY: &str = "test-bucket/path/to/object.dat";
const LEN: u64 = 4096;

fn setup(cache_dir: &Path) -> CacheManager {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let manager = CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false, // ram_cache_enabled off, so the disk write is the only store
        0,
        CacheEvictionAlgorithm::TinyLFU,
    );
    let _ = manager.create_configured_disk_cache_manager();
    manager
}

async fn seed(manager: &CacheManager) {
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
}

/// The fixture's own precondition, asserted so neither assertion below can pass
/// vacuously: the producer really did write a sharded `.meta`, and the flat path the
/// retired helper computed really is absent. If this fails, the producer changed and
/// the two assertions afterwards are measuring something else.
#[tokio::test]
async fn production_writer_puts_the_meta_where_the_sharded_helper_says() {
    let temp = tempfile::TempDir::new().unwrap();
    let cache_dir = temp.path();
    let manager = setup(cache_dir);
    seed(&manager).await;

    let produced = manager.get_new_metadata_file_path(KEY);
    let sharded =
        s3_proxy::disk_cache::get_sharded_path(&cache_dir.join("metadata"), KEY, ".meta").unwrap();
    let flat = cache_dir
        .join("metadata")
        .join(format!("{}.meta", KEY.replace(['/', ':', '\\'], "_")));

    println!("  produced .meta : {}", produced.display());
    println!("  sharded helper : {}", sharded.display());
    println!("  retired flat   : {}", flat.display());

    assert!(
        produced.exists(),
        "the production GET store must have written a .meta at {:?}",
        produced
    );
    assert_eq!(
        produced, sharded,
        "the production .meta path must be the one disk_cache::get_sharded_path builds"
    );
    assert!(
        !flat.exists(),
        "no producer writes the flat layout the retired helper built; if this exists \
         the fixture is not discriminating"
    );
}

/// Defect 1's discriminator. Pre-fix this reported
/// `InvalidJson("Failed to load metadata: …")` because the flat path did not resolve.
///
/// It asserts on the error VARIANT, not merely on `is_consistent`, because the code
/// branches on `load_metadata_integrity`'s `Ok`/`Err` — asserting only the boolean
/// would not distinguish a resolved-but-genuinely-inconsistent entry from a `.meta`
/// that was never opened.
#[tokio::test]
async fn consistency_check_resolves_the_production_metadata_path() {
    let temp = tempfile::TempDir::new().unwrap();
    let cache_dir = temp.path();
    let manager = setup(cache_dir);
    seed(&manager).await;

    let result = CacheValidator::new(cache_dir.to_path_buf())
        .validate_consistency_integrity(KEY)
        .await
        .expect("validation must not error");

    let load_failures: Vec<&MetadataIntegrityError> = result
        .metadata_errors
        .iter()
        .filter(|e| matches!(e, MetadataIntegrityError::InvalidJson(_)))
        .collect();

    assert!(
        load_failures.is_empty(),
        "the consistency check must open the .meta the production writer created; \
         it reported {} load failure(s) instead: {:?}",
        load_failures.len(),
        load_failures
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
    );
}

/// Defect 2's discriminator, and the reason defect 1's fix is not sufficient on its own.
/// With only defect 1 fixed this reports one `RangeFileMissing` for the single range the
/// producer wrote, against a `.bin` that is present on disk.
#[tokio::test]
async fn consistency_check_finds_the_range_file_the_producer_wrote() {
    let temp = tempfile::TempDir::new().unwrap();
    let cache_dir = temp.path();
    let manager = setup(cache_dir);
    seed(&manager).await;

    let result = CacheValidator::new(cache_dir.to_path_buf())
        .validate_consistency_integrity(KEY)
        .await
        .expect("validation must not error");

    assert!(
        result.missing_range_files.is_empty(),
        "every range the producer wrote is on disk under cache_dir/ranges/, so none may \
         be reported missing; got {:?} (errors: {:?})",
        result.missing_range_files,
        result
            .metadata_errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        result.is_consistent,
        "an entry written by the production GET store and left untouched must validate \
         as consistent; errors: {:?}",
        result
            .metadata_errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
    );
}

/// A malformed cache key must be reported as what it is. The retired helper accepted
/// anything, so an unparseable key produced the same `InvalidJson` as a resolvable one
/// whose file was genuinely absent — and `get_sharded_path` is the thing that rejects it.
#[tokio::test]
async fn malformed_cache_key_is_reported_as_a_path_failure_not_as_invalid_json() {
    let temp = tempfile::TempDir::new().unwrap();
    let cache_dir = temp.path();
    std::fs::create_dir_all(cache_dir.join("metadata")).unwrap();

    let result = CacheValidator::new(cache_dir.to_path_buf())
        .validate_consistency_integrity("noslash")
        .await
        .expect("validation must not error");

    assert!(!result.is_consistent);
    let rendered: Vec<String> = result
        .metadata_errors
        .iter()
        .map(|e| e.to_string())
        .collect();
    println!("  malformed-key errors: {:?}", rendered);
    assert!(
        result.metadata_errors.iter().any(|e| matches!(
            e,
            MetadataIntegrityError::InvalidFieldValue { field, .. } if field == "cache_key"
        )),
        "a key the sharded helper cannot parse must be reported against cache_key, not \
         as unparseable JSON; got {:?}",
        rendered
    );
}
