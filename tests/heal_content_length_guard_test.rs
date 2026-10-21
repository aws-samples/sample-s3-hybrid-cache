//! A range attached to a correctly-published zero-byte object must not have its length
//! derived into that object's document.
//!
//! # The defect
//!
//! Two metadata writers derive a full object's `content_length` from its single range when
//! the document's own `content_length` is `0`:
//! `HybridMetadataWriter::add_or_update_range` and
//! `JournalConsolidator::apply_journal_entries`. The blocks were byte-identical, and
//! `content_length == 0` is ambiguous — it means "unknown" on a defaulted document and
//! "this object is zero bytes" on a published one.
//!
//! On a published zero-byte object the derivation writes the SUPERSEDED object's length into
//! a document carrying the NEW, empty object's `ETag`. The result is internally CONSISTENT
//! and describes the previous object, so every freshness and authorisation gate admits it.
//! That is the shape `CacheManager::is_empty_object_etag_entry` detects on the read path;
//! these tests assert the producer refuses to create it.
//!
//! # Why the consolidator's copy is the one that mattered
//!
//! `CacheManager::is_empty_object_etag_entry`'s doc comment states the producer is "closed
//! on two independent sides: the zero-length publish now removes the range files it
//! dereferences, and the orphan sweep now declines to re-adopt a range a
//! `content_length: 0` document cannot contain."
//!
//! Neither side covers the journal. `dereference_all_ranges_for_empty_publish` removes only
//! the ranges the EXISTING `.meta` names, and `disk_cache::store_range` renames a `.bin`
//! into place BEFORE writing its journal `Add` entry — so a range whose entry is still
//! pending is invisible to the publish, survives it, validates on the next consolidation
//! cycle (its file exists), and is folded into the now-empty document. The sweep's guard is
//! nowhere on that route. `unpublished_document_still_derives_its_length_from_its_range`
//! below is the positive control that the legitimate derivation is preserved.
//!
//! Each site is driven through its OWN production entry point: the consolidator through
//! `consolidate_object`, the writer through `write_range_metadata` with the
//! `WriteMode::Hybrid` the orphan sweep's `add_range_to_metadata` uses. A test driving one
//! proves nothing about the other.

use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter, WriteMode};
use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// The MD5 of the empty body, which S3 returns as the ETag of every zero-byte object.
const EMPTY_OBJECT_ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";

const SUPERSEDED_LEN: u64 = 262_144;

/// The absolute on-disk path of the superseded object's range file, as
/// `JournalConsolidator::get_range_file_path` resolves it: sharded under `ranges/` from the
/// cache key plus the `_{start}-{end}.bin` suffix.
fn range_file_path(cache_dir: &Path, cache_key: &str) -> std::path::PathBuf {
    s3_proxy::disk_cache::get_sharded_path(
        &cache_dir.join("ranges"),
        cache_key,
        &format!("_0-{}.bin", SUPERSEDED_LEN - 1),
    )
    .unwrap()
}

/// `RangeSpec::file_path` holds the `{bucket}/{XX}/{YYY}/{file}` form relative to `ranges/`,
/// which is what every writer stores and what the read path joins back on. Derived from the
/// real sharded path rather than hand-built, so it cannot drift from the sharding helper.
fn superseded_range(cache_dir: &Path, cache_key: &str) -> RangeSpec {
    let now = SystemTime::now();
    let relative = range_file_path(cache_dir, cache_key)
        .strip_prefix(cache_dir.join("ranges"))
        .unwrap()
        .to_string_lossy()
        .to_string();
    RangeSpec {
        start: 0,
        end: SUPERSEDED_LEN - 1,
        file_path: relative,
        compression_algorithm: CompressionAlgorithm::Lz4,
        compressed_size: SUPERSEDED_LEN,
        uncompressed_size: SUPERSEDED_LEN,
        created_at: now,
        last_accessed: now,
        access_count: 1,
        staged: Some(false),
        unservable: false,
        file_identity: None,
    }
}

/// A document as `store_full_object_as_range_new`'s zero-length branch publishes one: the
/// origin's `ObjectMetadata` verbatim, so a real `ETag` beside `content_length: 0`, and no
/// ranges.
///
/// This is the fixture that matters. An ordinary full-object publish also has
/// `content_length == 0` at the moment the derivation fires, and a fixture built from it
/// exercises the derivation's GOOD path while asserting nothing about the guard — see
/// `unpublished_document_still_derives_its_length_from_its_range`, which is that case and is
/// kept as the control rather than as the subject.
fn published_empty_object(cache_key: &str) -> NewCacheMetadata {
    let now = SystemTime::now();
    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata {
            etag: EMPTY_OBJECT_ETAG.to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 0,
            ..Default::default()
        },
        ranges: Vec::new(),
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        ..Default::default()
    }
}

async fn write_meta(cache_dir: &Path, metadata: &NewCacheMetadata) -> std::path::PathBuf {
    let base = cache_dir.join("metadata");
    let path = s3_proxy::disk_cache::get_sharded_path(&base, &metadata.cache_key, ".meta").unwrap();
    tokio::fs::create_dir_all(path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&path, serde_json::to_string_pretty(metadata).unwrap())
        .await
        .unwrap();
    path
}

/// Put the superseded object's `.bin` on disk. This is the state the zero-length publish
/// cannot clear: `dereference_all_ranges_for_empty_publish` removes only what the existing
/// `.meta` names, and this range's entry is still in the journal.
async fn seed_range_file(cache_dir: &Path, cache_key: &str) -> std::path::PathBuf {
    let path = range_file_path(cache_dir, cache_key);
    tokio::fs::create_dir_all(path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&path, vec![0u8; 64]).await.unwrap();
    path
}

async fn read_meta(path: &Path) -> NewCacheMetadata {
    serde_json::from_str(&tokio::fs::read_to_string(path).await.unwrap()).unwrap()
}

fn add_entry(
    cache_dir: &Path,
    cache_key: &str,
    object_metadata: Option<ObjectMetadata>,
) -> JournalEntry {
    JournalEntry {
        timestamp: SystemTime::now(),
        instance_id: "guard-test-instance".to_string(),
        cache_key: cache_key.to_string(),
        range_spec: superseded_range(cache_dir, cache_key),
        operation: JournalOperation::Add,
        range_file_path: String::new(),
        metadata_version: 1,
        new_ttl_secs: None,
        object_ttl_secs: Some(3600),
        access_increment: None,
        object_metadata,
        fill_witness: None,
    }
}

fn consolidator(cache_dir: &Path) -> JournalConsolidator {
    JournalConsolidator::new(
        cache_dir.to_path_buf(),
        Arc::new(JournalManager::new(
            cache_dir.to_path_buf(),
            "guard-test-instance".to_string(),
        )),
        Arc::new(MetadataLockManager::new(
            cache_dir.to_path_buf(),
            Duration::from_secs(30),
            3,
        )),
        ConsolidationConfig::default(),
    )
}

/// SITE 1 — `JournalConsolidator::apply_journal_entries`, via `consolidate_object`.
///
/// The live, unguarded route. Asserts the DECISION and its INPUTS, not only the output:
/// `content_length == 0` alone cannot distinguish "the guard refused" from "the derivation
/// fired and computed 0", so the range is asserted present (proving the `Add` really was
/// applied and the derivation was therefore reached) and the ETag is asserted unchanged.
#[tokio::test]
async fn consolidation_does_not_derive_a_length_for_a_published_empty_object() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let cache_key = "guard-bucket/consolidator-empty-object";

    let meta_path = write_meta(cache_dir, &published_empty_object(cache_key)).await;
    seed_range_file(cache_dir, cache_key).await;

    let journal = JournalManager::new(cache_dir.to_path_buf(), "guard-test-instance".to_string());
    journal
        .append_range_entry(cache_key, add_entry(cache_dir, cache_key, None))
        .await
        .unwrap();

    let before = read_meta(&meta_path).await;
    assert_eq!(
        before.object_metadata.content_length, 0,
        "precondition: the published document must record content_length 0"
    );
    assert!(
        before.ranges.is_empty(),
        "precondition: the published document must record no ranges"
    );
    assert_eq!(
        before.object_metadata.etag, EMPTY_OBJECT_ETAG,
        "precondition: the published document must carry the empty object's ETag"
    );

    let result = consolidator(cache_dir)
        .consolidate_object(cache_key)
        .await
        .unwrap();
    assert!(result.success, "fixture: consolidation must succeed");

    let after = read_meta(&meta_path).await;

    // The derivation must have been REACHED, or this test proves nothing. Its shape gate is
    // `ranges.len() == 1 && ranges[0].start == 0 && content_length == 0`, so a fold that
    // dropped the entry would satisfy the assertion below vacuously.
    assert_eq!(
        after.ranges.len(),
        1,
        "fixture: the Add entry must have been folded in, or the derivation was never \
         reached and the assertion below is vacuous. ranges={:?}",
        after.ranges
    );
    assert_eq!(
        after.ranges[0].start, 0,
        "fixture: the range must start at 0"
    );

    assert_eq!(
        after.object_metadata.content_length, 0,
        "A range that outlived a zero-length publish had its length derived into the \
         published empty object's document. content_length={} was taken from range 0-{}, \
         while the document still carries the empty object's ETag {} — an internally \
         CONSISTENT document describing the SUPERSEDED object, which every freshness and \
         authorisation gate admits.",
        after.object_metadata.content_length, after.ranges[0].end, after.object_metadata.etag
    );
    assert_eq!(
        after.object_metadata.etag, EMPTY_OBJECT_ETAG,
        "the published ETag must be left alone"
    );
}

/// POSITIVE CONTROL for site 1, with a DISTINCT SIGNATURE from the defect.
///
/// An unpublished document — the `ObjectMetadata::default()` that
/// `load_or_create_metadata_with_journal_entries` falls back to — has `content_length: 0`
/// because the length is UNKNOWN, and deriving it from the range is the case the code was
/// written for. An over-broad guard reds HERE and not at the defect, so the two failures are
/// not confusable.
#[tokio::test]
async fn unpublished_document_still_derives_its_length_from_its_range() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let cache_key = "guard-bucket/consolidator-unpublished-object";

    let mut unpublished = published_empty_object(cache_key);
    unpublished.object_metadata = ObjectMetadata::default();
    let meta_path = write_meta(cache_dir, &unpublished).await;
    seed_range_file(cache_dir, cache_key).await;

    let journal = JournalManager::new(cache_dir.to_path_buf(), "guard-test-instance".to_string());
    journal
        .append_range_entry(cache_key, add_entry(cache_dir, cache_key, None))
        .await
        .unwrap();

    let before = read_meta(&meta_path).await;
    assert!(
        before.object_metadata.etag.is_empty(),
        "precondition: this document must carry NO published identity"
    );
    assert_eq!(
        before.object_metadata.content_length, 0,
        "precondition: content_length must be 0 because it is unknown, not published"
    );

    consolidator(cache_dir)
        .consolidate_object(cache_key)
        .await
        .unwrap();

    let after = read_meta(&meta_path).await;
    assert_eq!(
        after.object_metadata.content_length, SUPERSEDED_LEN,
        "an unpublished document must still have its length derived from its single \
         range — refusing here would break every legitimately defaulted document. \
         ranges={:?}",
        after.ranges
    );
}

/// SITE 2 — `HybridMetadataWriter::add_or_update_range`, via `write_range_metadata` with
/// the `WriteMode::Hybrid` that the orphan sweep's `add_range_to_metadata` passes.
///
/// This site's ONLY production route is that sweep, which already declines a range a
/// published `content_length` cannot contain, so this is defence in depth rather than a live
/// hole. It is asserted anyway because the two copies of this logic drifted once already —
/// one guarded, one not — which is what the shared predicate exists to prevent.
#[tokio::test]
async fn the_writer_does_not_derive_a_length_for_a_published_empty_object() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let cache_key = "guard-bucket/writer-empty-object";

    let meta_path = write_meta(cache_dir, &published_empty_object(cache_key)).await;

    let mut writer = HybridMetadataWriter::new(
        cache_dir.to_path_buf(),
        Arc::new(MetadataLockManager::new(
            cache_dir.to_path_buf(),
            Duration::from_secs(30),
            3,
        )),
        Arc::new(JournalManager::new(
            cache_dir.to_path_buf(),
            "guard-test-instance".to_string(),
        )),
        Arc::new(ConsolidationTrigger::new(1024, 10)),
    );

    let before = read_meta(&meta_path).await;
    assert_eq!(
        before.object_metadata.content_length, 0,
        "precondition: the published document must record content_length 0"
    );
    assert_eq!(
        before.object_metadata.etag, EMPTY_OBJECT_ETAG,
        "precondition: the published document must carry the empty object's ETag"
    );

    writer
        .write_range_metadata(
            cache_key,
            superseded_range(cache_dir, cache_key),
            WriteMode::Hybrid,
            None,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

    let after = read_meta(&meta_path).await;
    assert_eq!(
        after.ranges.len(),
        1,
        "fixture: the range must have been added, or the derivation was never reached \
         and the assertion below is vacuous. ranges={:?}",
        after.ranges
    );
    assert_eq!(
        after.ranges[0].start, 0,
        "fixture: the range must start at 0"
    );
    assert_eq!(
        after.object_metadata.content_length, 0,
        "the writer derived content_length={} from range 0-{} for a document that had \
         already published content_length 0 under the empty object's ETag {}.",
        after.object_metadata.content_length, after.ranges[0].end, after.object_metadata.etag
    );
    assert_eq!(
        after.object_metadata.etag, EMPTY_OBJECT_ETAG,
        "the published ETag must be left alone"
    );
}

/// POSITIVE CONTROL for site 2, same distinct signature as site 1's control.
#[tokio::test]
async fn the_writer_still_derives_a_length_for_an_unpublished_document() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path();
    let cache_key = "guard-bucket/writer-unpublished-object";

    let mut writer = HybridMetadataWriter::new(
        cache_dir.to_path_buf(),
        Arc::new(MetadataLockManager::new(
            cache_dir.to_path_buf(),
            Duration::from_secs(30),
            3,
        )),
        Arc::new(JournalManager::new(
            cache_dir.to_path_buf(),
            "guard-test-instance".to_string(),
        )),
        Arc::new(ConsolidationTrigger::new(1024, 10)),
    );

    // No `.meta` at all, so `load_or_create_metadata` builds one from
    // `ObjectMetadata::default()` — the unpublished case, reached here through the same
    // production call the sweep makes.
    writer
        .write_range_metadata(
            cache_key,
            superseded_range(cache_dir, cache_key),
            WriteMode::Hybrid,
            None,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

    let base = cache_dir.join("metadata");
    let meta_path = s3_proxy::disk_cache::get_sharded_path(&base, cache_key, ".meta").unwrap();
    let after = read_meta(&meta_path).await;
    assert!(
        after.object_metadata.etag.is_empty(),
        "precondition: a freshly created document must carry no published identity"
    );
    assert_eq!(
        after.object_metadata.content_length, SUPERSEDED_LEN,
        "a document with no published identity must still have its length derived from \
         its single full-object range. ranges={:?}",
        after.ranges
    );
}
