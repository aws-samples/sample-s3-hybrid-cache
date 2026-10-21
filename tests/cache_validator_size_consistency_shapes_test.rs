//! Does `CacheValidator::validate_size_consistency_integrity` report shapes this
//! codebase's own writers publish deliberately?
//!
//! It is reached on a production boot — `CacheManager::initialize` →
//! `CacheInitializationCoordinator::initialize_with_locking` → `initialize` →
//! `scan_cache_metadata_with_consistency_validation` →
//! `perform_metadata_consistency_validation`, once per scanned `.meta`, via
//! `CacheValidator::validate_metadata_integrity` — but only when
//! `size_tracking/validation.json` is absent, unparseable, or older than 23 hours
//! (`check_validation_freshness` in `cache_initialization_coordinator.rs`). Every error it
//! reports increments the same `corruption_count` the "found N corrupted metadata files"
//! `warn!` names. Nothing else acts on it.
//!
//! Citations below are by SYMBOL NAME, never line number.
//!
//! # The two arms this pins, and the producers that refuted them
//!
//! **Arm one — empty ranges must mean a zero-length object.** The function required
//! `object_metadata.content_length == 0` whenever `metadata.ranges.is_empty()`, reporting
//! `SizeInconsistency { expected: 0, actual: <length> }` otherwise.
//! `CacheManager::create_head_only_metadata` — reached from the `pub`
//! `store_head_cache_entry_unified` whenever the deciding read finds no document on disk —
//! publishes `ranges: Vec::new()` beside the object's **real** `content_length`, and
//! renames it into place itself. So every HEAD-only entry on a volume tripped this. The
//! premise was simply false for this cache: empty `ranges` means "no body data cached", not
//! "the object is empty", which is also how the coordinator's own `stale_head_only_entry`
//! helper defines the shape.
//!
//! **Arm two — a 0-anchored entry must cover the whole object.** When
//! `ranges.first().start == 0` the function required
//! `content_length == last_range.end + 1`. `DiskCacheManager::store_range` (the
//! range GET-miss fill, driven from `handle_signed_range_s3_response` and three other
//! `http_proxy` sites) publishes a partial range beside the **total** object length:
//! `S3Client::extract_object_metadata_from_response` reads the total out of
//! `Content-Range`, and the call site carries an in-source comment saying that total must
//! NOT be overridden with the range's own length. So a 4 KiB range cached out of a 1 MiB
//! object reported `expected: 4096, actual: 1048576`, and any client whose first read
//! starts at offset 0 — a header read, or a transfer client's first chunk — produced it.
//!
//! # What replaced them
//!
//! Arm one is **deleted**. Its whole population is the HEAD-only shape, and the only
//! version of it that would be wrong — an entry whose `ranges` array was emptied by a bad
//! write while a real length survived — is byte-identical to the legitimate one, so no
//! check here can separate them. Such an entry is also self-correcting at read time:
//! `find_cached_ranges` reports no coverage, the request forwards to S3, and the entry is
//! re-cached.
//!
//! Arm two keeps the only half of its comparison that is decidable, spelled the way the
//! tree already spells it. `RangeHandler::validate_range_request` rejects a request whose
//! `range.end >= content_length` as "Range end exceeds content length"; the validator now
//! reports a cached range on the same condition. Under-coverage is dropped because a
//! partially cached object and a complete object that lost a range record are the same
//! document. Over-coverage cannot be produced by a legitimate write — the request path
//! rejects it, and `DiskCacheManager::store_range` additionally clamps `end` to the bytes
//! actually received — so it remains real corruption. The `first.start == 0` gate is gone
//! with the equality: over-coverage is corruption wherever coverage begins, which makes the
//! check strictly wider than the one it replaces at the thing that is detectable. It
//! carries `validate_json_structure_integrity`'s `no_validator_at_all` guard, because a
//! document the orphan sweep re-adopted holds `ObjectMetadata::default()` — a length of 0
//! that is unknown rather than zero — and every range in it would otherwise read as
//! over-coverage.
//!
//! # Assertion discipline
//!
//! Each arm has its OWN producer-driven fixture, so a failure names which arm. Every
//! assertion tests the whole error list first — a single-claim assertion panics before the
//! rest of the list is read — and then names `MetadataIntegrityError::SizeInconsistency`
//! specifically. `is_valid` is false for any of several unrelated reasons and is never the
//! subject.
//!
//! `a_range_claiming_bytes_past_the_end_of_the_object_is_still_reported` passes both before
//! and after the change: it is the width guard on the relaxation, **not** evidence for it.
//! Its entry is hand-built rather than produced, deliberately — no writer in the tree emits
//! over-coverage, which is the whole reason it is still worth reporting.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{
    CacheMetadata, CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec,
};
use s3_proxy::cache_validator::{CacheValidator, MetadataIntegrityError};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::ConnectionPoolConfig;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::s3_client::S3Client;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// The whole object, of which arm two's fixture caches only the first 4 KiB.
const OBJECT_LEN: u64 = 1_048_576;
const RANGE_LEN: u64 = 4_096;

fn setup(cache_dir: &Path) -> Arc<CacheManager> {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    let manager = Arc::new(CacheManager::new_with_eviction_algorithm(
        cache_dir.to_path_buf(),
        false, // ram_cache_enabled off, so the disk document is the only store
        0,
        CacheEvictionAlgorithm::TinyLFU,
    ));
    let _ = manager.create_configured_disk_cache_manager();
    manager
}

fn test_pool_config() -> ConnectionPoolConfig {
    ConnectionPoolConfig {
        ip_distribution_enabled: false,
        ..Default::default()
    }
}

/// Read back what the PRODUCER wrote rather than reconstructing it here.
async fn published(cache_dir: &Path, cache_key: &str) -> NewCacheMetadata {
    let path =
        s3_proxy::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
            .expect("sharded metadata path");
    serde_json::from_str(
        &tokio::fs::read_to_string(&path)
            .await
            .unwrap_or_else(|e| panic!("published .meta at {:?}: {}", path, e)),
    )
    .expect("parse published .meta")
}

fn size_inconsistencies(errors: &[MetadataIntegrityError]) -> Vec<(u64, u64)> {
    errors
        .iter()
        .filter_map(|e| match e {
            MetadataIntegrityError::SizeInconsistency { expected, actual } => {
                Some((*expected, *actual))
            }
            _ => None,
        })
        .collect()
}

fn rendered(errors: &[MetadataIntegrityError]) -> Vec<String> {
    errors.iter().map(|e| e.to_string()).collect()
}

// ─────────────────────────── ARM ONE: the HEAD-only shape ───────────────────────────

/// RED pre-fix: `SizeInconsistency { expected: 0, actual: 1048576 }`.
///
/// Driven through the `pub` production entry point, not by calling the private writer or
/// by hand-writing a document: `store_head_cache_entry_unified` on a key with nothing on
/// disk takes its `Err` arm and publishes through `create_head_only_metadata`.
#[tokio::test]
async fn head_only_entry_from_the_production_writer_is_not_reported_size_inconsistent() {
    const KEY: &str = "test-bucket/head-only/object.dat";

    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let manager = setup(cache_dir);

    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), "application/pdf".to_string());
    headers.insert(
        "etag".to_string(),
        "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string(),
    );
    manager
        .store_head_cache_entry_unified(
            KEY,
            headers,
            CacheMetadata {
                etag: "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string(),
                last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
                content_length: OBJECT_LEN,
                part_number: None,
                cache_control: None,
                access_count: 0,
                last_accessed: SystemTime::now(),
            },
        )
        .await
        .expect("HEAD cache store should succeed");

    let entry = published(cache_dir, KEY).await;

    // The fixture's own precondition. Without it a producer change could leave this test
    // measuring an entry that has ranges, where neither arm applies.
    assert!(
        entry.ranges.is_empty() && entry.object_metadata.content_length == OBJECT_LEN,
        "fixture: the HEAD-only writer must publish empty ranges beside the object's REAL \
         length — that combination IS the shape under test; got ranges={} content_length={}",
        entry.ranges.len(),
        entry.object_metadata.content_length
    );

    let result = CacheValidator::new(cache_dir.to_path_buf())
        .validate_metadata_integrity(&entry)
        .await
        .expect("validation must not error");

    // The whole list first, so a red run records everything this shape trips rather than
    // stopping at one family and leaving the others unproven.
    assert!(
        result.errors.is_empty(),
        "a HEAD-only document the production writer itself published is healthy — it has a \
         known length and no cached body by definition — and must produce no integrity \
         errors; got {:?}",
        rendered(&result.errors)
    );
    assert!(
        size_inconsistencies(&result.errors).is_empty(),
        "an entry with no cached body data must not be reported size-inconsistent against \
         an expected length of 0; got {:?}",
        size_inconsistencies(&result.errors)
    );
}

// ───────────────────── ARM TWO: an object cached in part from 0 ─────────────────────

/// The basis of arm two, asserted at the producer rather than taken from a comment: a 206
/// response's `ObjectMetadata` carries the object's TOTAL length, not the range's length.
/// If this ever stopped being true, arm two's population would be empty and the test below
/// would be measuring something else.
#[tokio::test]
async fn the_range_response_producer_carries_the_total_object_length() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = S3Client::new(&test_pool_config(), None).expect("S3Client");

    let produced = client.extract_object_metadata_from_response(&s3_range_response_headers());

    assert_eq!(
        produced.content_length, OBJECT_LEN,
        "extract_object_metadata_from_response must take the total size out of \
         Content-Range, not the response's own Content-Length; got {}",
        produced.content_length
    );
}

/// The headers a real S3 `206` carries for `Range: bytes=0-4095` of a 1 MiB object.
fn s3_range_response_headers() -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        "ETag".to_string(),
        "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string(),
    );
    headers.insert(
        "Last-Modified".to_string(),
        "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
    );
    headers.insert("content-length".to_string(), RANGE_LEN.to_string());
    headers.insert(
        "content-range".to_string(),
        format!("bytes 0-{}/{}", RANGE_LEN - 1, OBJECT_LEN),
    );
    headers
}

/// RED pre-fix: `SizeInconsistency { expected: 4096, actual: 1048576 }`.
///
/// Driven through `DiskCacheManager::store_range`, which is the range GET-miss
/// fill path (`capacity_guard`'s P5), with the `ObjectMetadata` built by the same
/// `extract_object_metadata_from_response` the `http_proxy` call site uses.
#[tokio::test]
async fn partially_cached_object_from_the_production_range_writer_is_not_reported_size_inconsistent(
) {
    const KEY: &str = "test-bucket/partial/object.dat";

    let _ = rustls::crypto::ring::default_provider().install_default();
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let manager = setup(cache_dir);

    let client = S3Client::new(&test_pool_config(), None).expect("S3Client");
    let mut object_metadata =
        client.extract_object_metadata_from_response(&s3_range_response_headers());
    // What the http_proxy call site does after extracting: mark the object complete and
    // record how much has been CACHED, leaving content_length as the total.
    object_metadata.upload_state = s3_proxy::cache_types::UploadState::Complete;
    object_metadata.cumulative_size = RANGE_LEN;

    let disk_cache_manager: Arc<tokio::sync::RwLock<DiskCacheManager>> = Arc::new(
        tokio::sync::RwLock::new(manager.create_configured_disk_cache_manager()),
    );
    let range_handler = RangeHandler::new(manager.clone(), disk_cache_manager);
    range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            0,
            RANGE_LEN - 1,
            &vec![7u8; RANGE_LEN as usize],
            object_metadata,
            Duration::from_secs(3600),
            false, // store-mode frame; compression is irrelevant to the size arms
        )
        .await
        .expect("range GET-miss fill should succeed");

    // `DiskCacheManager::store_range` writes `WriteMode::JournalOnly`, so the `.meta` the
    // validation scan reads is the one the background `JournalConsolidator` folds the
    // journal into. Drive that same production consolidator rather than reaching past it:
    // the document under test is the consolidated one, not the journal entry.
    manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .consolidate_object(KEY)
        .await
        .expect("consolidation should succeed");

    let entry = published(cache_dir, KEY).await;

    // The fixture's own precondition, and the answer to "is a first range starting at 0
    // what this arm fires on": it is exactly this document.
    let first = entry
        .ranges
        .first()
        .expect("fixture: a range must be published");
    let last = entry
        .ranges
        .last()
        .expect("fixture: a range must be published");
    assert!(
        first.start == 0
            && last.end == RANGE_LEN - 1
            && entry.object_metadata.content_length == OBJECT_LEN,
        "fixture: the range writer must publish coverage anchored at 0 that stops short of \
         the object's recorded total length — that IS the shape under test; got \
         first.start={} last.end={} content_length={}",
        first.start,
        last.end,
        entry.object_metadata.content_length
    );

    let result = CacheValidator::new(cache_dir.to_path_buf())
        .validate_metadata_integrity(&entry)
        .await
        .expect("validation must not error");

    assert!(
        result.errors.is_empty(),
        "an object cached in part from offset 0 is the ordinary product of a range GET \
         miss and must produce no integrity errors; got {:?}",
        rendered(&result.errors)
    );
    assert!(
        size_inconsistencies(&result.errors).is_empty(),
        "cached coverage is allowed to be a prefix of the object, so a partial entry must \
         not be reported size-inconsistent; got {:?}",
        size_inconsistencies(&result.errors)
    );
}

// ───────────────────────────────── WIDTH GUARDS ─────────────────────────────────

/// WIDTH GUARD on the relaxation. Passes before and after the change, and is therefore
/// NOT evidence for it — it exists so a future widening of the relaxation fails here.
///
/// Hand-built on purpose: no writer in this tree emits over-coverage, because
/// `RangeHandler::validate_range_request` rejects such a request and
/// `DiskCacheManager::store_range` clamps `end` to the bytes received. That is precisely
/// why it is still worth reporting.
#[tokio::test]
async fn a_range_claiming_bytes_past_the_end_of_the_object_is_still_reported() {
    let temp = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp.path().to_path_buf());

    // Per-range arithmetic is internally consistent (uncompressed_size == end - start + 1)
    // so `validate_ranges_integrity` contributes nothing and the only SizeInconsistency
    // below is the one under test.
    let entry = entry_with(
        ObjectMetadata {
            etag: "\"observed-etag\"".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: RANGE_LEN,
            ..Default::default()
        },
        vec![range(0, 2 * RANGE_LEN - 1)],
    );

    let result = validator
        .validate_metadata_integrity(&entry)
        .await
        .expect("validation must not error");

    // The variant's field convention is unchanged by this work: `expected` is the figure
    // derived from the ranges, `actual` is the length recorded on the object. Keeping it
    // is what lets this guard pass identically on both sides of the change.
    assert_eq!(
        size_inconsistencies(&result.errors),
        vec![(2 * RANGE_LEN, RANGE_LEN)],
        "a range claiming bytes beyond the end of the object is corruption no writer can \
         produce and must still be reported exactly once; all errors: {:?}",
        rendered(&result.errors)
    );
}

/// The widening, stated as its own assertion so it is visible rather than incidental:
/// over-coverage is now reported wherever coverage begins. RED pre-fix, because the
/// retired arm only looked at entries whose first range started at 0.
#[tokio::test]
async fn over_coverage_is_reported_even_when_coverage_does_not_start_at_zero() {
    let temp = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp.path().to_path_buf());

    let entry = entry_with(
        ObjectMetadata {
            etag: "\"observed-etag\"".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: RANGE_LEN,
            ..Default::default()
        },
        vec![range(RANGE_LEN, 2 * RANGE_LEN - 1)],
    );

    let result = validator
        .validate_metadata_integrity(&entry)
        .await
        .expect("validation must not error");

    assert_eq!(
        size_inconsistencies(&result.errors),
        vec![(2 * RANGE_LEN, RANGE_LEN)],
        "coverage that lies entirely past the end of the object is corruption whatever \
         offset it starts at; all errors: {:?}",
        rendered(&result.errors)
    );
}

/// WIDTH GUARD on the `no_validator_at_all` skip. Passes before and after, so it is not
/// evidence either: a re-adopted orphan document carries `ObjectMetadata::default()`, whose
/// `content_length: 0` is unknown rather than zero, and must not be reported merely because
/// every range in it exceeds that zero.
#[tokio::test]
async fn a_re_adopted_orphan_shape_is_not_reported_as_over_coverage() {
    let temp = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp.path().to_path_buf());

    let entry = entry_with(
        ObjectMetadata::default(),
        vec![range(RANGE_LEN, 2 * RANGE_LEN - 1)],
    );

    let result = validator
        .validate_metadata_integrity(&entry)
        .await
        .expect("validation must not error");

    assert!(
        size_inconsistencies(&result.errors).is_empty(),
        "a document with no observed object metadata has an unknown length, not a length \
         of zero, so its ranges cannot be judged against it; got {:?} (all errors: {:?})",
        size_inconsistencies(&result.errors),
        rendered(&result.errors)
    );
}

fn range(start: u64, end: u64) -> RangeSpec {
    let len = end - start + 1;
    RangeSpec::new_staged(
        start,
        end,
        format!("test-bucket/aa/bbb/object.dat_{}-{}.bin", start, end),
        CompressionAlgorithm::Lz4,
        len,
        len,
        false,
    )
}

fn entry_with(object_metadata: ObjectMetadata, ranges: Vec<RangeSpec>) -> NewCacheMetadata {
    let now = SystemTime::now();
    NewCacheMetadata {
        cache_key: "test-bucket/object.dat".to_string(),
        object_metadata,
        ranges,
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}
