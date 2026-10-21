//! Does `CacheValidator::validate_json_structure_integrity` require fields the
//! codebase's own writers do not publish?
//!
//! It is reached on a production boot — `CacheManager::initialize` →
//! `CacheInitializationCoordinator::initialize_with_locking` → `initialize` →
//! `scan_cache_metadata_with_consistency_validation` →
//! `perform_metadata_consistency_validation`, once per scanned `.meta`, via
//! `CacheValidator::validate_metadata_integrity` — but only when
//! `size_tracking/validation.json` is absent, unparseable, or older than 23 hours
//! (`check_validation_freshness`). Every error it reports increments the same
//! `corruption_count` the "found N corrupted metadata files" `warn!` names.
//!
//! Citations below are by SYMBOL NAME, never line number.
//!
//! # The two requirements this pins, and the shapes that refuted them
//!
//! `validate_json_structure_integrity` pushed `MissingRequiredField` for an empty
//! `object_metadata.etag` and for an empty `object_metadata.last_modified`, and
//! `InvalidFieldValue` for `content_length == 0` while ranges exist. Three production
//! writers publish `.meta` documents that trip those:
//!
//! 1. **Every write-through PUT and CompleteMultipartUpload entry** carries an empty
//!    `last_modified`. `S3Client::extract_object_metadata_from_response` reads the header
//!    with `.unwrap_or_default()`, and S3 does not send `Last-Modified` on a write
//!    response — its own doc comment says so, and the field is learned later by
//!    `CacheManager::backfill_write_cache_last_modified` from a `304`. The two
//!    single-part sites (`build_object_metadata_from_streamed_put`,
//!    `spawn_buffered_put_cache_write_task`) and the multipart completion site read the
//!    same header the same way. `write_through_put_producer_publishes_an_empty_last_modified`
//!    below establishes that at the producer rather than from a fixture comment.
//!
//! 2. **Any origin that omits `ETag`** yields an empty stored etag through the same
//!    `.unwrap_or_default()`. `DiskCacheManager::check_object_expiration` records this
//!    explicitly, as "an ordinary, correctly cached object" for such an origin, which is
//!    why its own guard requires BOTH validators to be absent rather than testing the
//!    etag alone.
//!
//! 3. **Every orphaned range an earlier release re-adopted.** That release's orphan sweep
//!    published with `object_metadata: None` (the sweep is removed; volumes upgraded from it
//!    can still hold its documents), and
//!    `HybridMetadataWriter::load_or_create_metadata`'s create-new branch fills that with
//!    `ObjectMetadata::default()` — empty etag, empty `last_modified`, `content_length: 0`
//!    — while `add_or_update_range` attaches the adopted range. Nothing on that path ever
//!    derives `content_length` from the range, so the published document trips all three
//!    checks at once. The test drives that same `write_range_metadata` call to publish it.
//!
//! # What replaced them
//!
//! The two field requirements are gone. The `content_length` check is now shape-aware on
//! the tree's own predicate: `DiskCacheManager::check_object_expiration` computes
//! `no_validator_at_all` as "empty etag AND no effective Last-Modified", which is exactly
//! the re-adoption signature, and refuses to serve such an entry until it has been
//! revalidated. The validator now skips the length requirement for that same shape and
//! keeps it for every document that carries a validator — i.e. for every document an
//! origin was actually observed to produce.
//!
//! # Assertion discipline
//!
//! Every assertion names the specific `MetadataIntegrityError` variant and field.
//! `is_valid` is false for any of several unrelated reasons and cannot distinguish this
//! defect from another, so it is never the subject. `a_document_with_a_validator_that_claims_zero_length_with_ranges_is_still_reported`
//! and `an_empty_cache_key_is_still_reported` pass both before and after the change: they
//! are the width guard on the relaxation, not evidence for it.

use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::cache_validator::{CacheValidator, MetadataIntegrityError};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::ConnectionPoolConfig;
use s3_proxy::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter, WriteMode};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use s3_proxy::s3_client::S3Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// The headers a real S3 `PUT` response carries: an `ETag`, and no `Last-Modified`.
fn s3_put_response_headers(content_length: u64) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        "ETag".to_string(),
        "\"9a0364b9e99bb480dd25e1f0284c8555\"".to_string(),
    );
    headers.insert("content-length".to_string(), content_length.to_string());
    headers.insert("x-amz-request-id".to_string(), "REQUESTID123".to_string());
    headers
}

fn test_pool_config() -> ConnectionPoolConfig {
    ConnectionPoolConfig {
        ip_distribution_enabled: false,
        ..Default::default()
    }
}

fn contiguous_range(len: u64) -> RangeSpec {
    RangeSpec::new_staged(
        0,
        len - 1,
        "test-bucket/aa/bbb/object.dat_0-4095.bin".to_string(),
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

fn missing_fields(errors: &[MetadataIntegrityError]) -> Vec<String> {
    errors
        .iter()
        .filter_map(|e| match e {
            MetadataIntegrityError::MissingRequiredField(f) => Some(f.clone()),
            _ => None,
        })
        .collect()
}

fn invalid_fields(errors: &[MetadataIntegrityError]) -> Vec<String> {
    errors
        .iter()
        .filter_map(|e| match e {
            MetadataIntegrityError::InvalidFieldValue { field, .. } => Some(field.clone()),
            _ => None,
        })
        .collect()
}

/// PRECONDITION, not evidence. Establishes at the producer that a write-through PUT
/// publishes an empty `last_modified` with a populated `etag` — the claim a fixture
/// comment in `src/cache.rs` asserts and cannot prove. Passes before and after.
#[tokio::test]
async fn write_through_put_producer_publishes_an_empty_last_modified() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = S3Client::new(&test_pool_config(), None).expect("S3Client");

    let produced = client.extract_object_metadata_from_response(&s3_put_response_headers(4096));

    assert!(
        !produced.etag.is_empty(),
        "fixture: an S3 write response carries an ETag, so the producer must populate it; \
         got an empty etag, which would make this test cover the wrong shape"
    );
    assert!(
        produced.last_modified.is_empty(),
        "the write-through PUT producer must publish an EMPTY last_modified — S3 sends no \
         Last-Modified on a write response and the field is read with unwrap_or_default; \
         got {:?}",
        produced.last_modified
    );
    assert!(
        produced.effective_last_modified().is_none(),
        "neither the typed field nor the header map carries a Last-Modified for a write \
         response, so effective_last_modified must be None; got {:?}",
        produced.effective_last_modified()
    );
}

/// RED pre-fix: `MissingRequiredField("object_metadata.last_modified")`.
#[tokio::test]
async fn a_write_through_put_entry_is_not_reported_corrupt() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = S3Client::new(&test_pool_config(), None).expect("S3Client");
    let mut object_metadata =
        client.extract_object_metadata_from_response(&s3_put_response_headers(4096));
    object_metadata.is_write_cached = true;

    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());
    let result = validator
        .validate_metadata_integrity(&entry_with(object_metadata, vec![contiguous_range(4096)]))
        .await
        .expect("validation must not error");

    assert!(
        result.errors.is_empty(),
        "an entry whose object_metadata came from the write-through PUT producer, with a \
         range consistent with its content_length, is healthy and must produce no integrity \
         errors; got {:?}",
        result.errors
    );
}

/// RED pre-fix: `MissingRequiredField("object_metadata.etag")`,
/// `MissingRequiredField("object_metadata.last_modified")` and
/// `InvalidFieldValue { field: "object_metadata.content_length" }`, from the document an
/// earlier release's orphan re-adoption sweep wrote. A volume upgraded from that release can
/// still hold it, so it is published here the way the sweep published it: one range through
/// `HybridMetadataWriter::write_range_metadata` in `WriteMode::Hybrid` with no object
/// metadata, which fills `ObjectMetadata::default()`.
#[tokio::test]
async fn a_re_adopted_orphan_document_is_not_reported_corrupt() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    let cache_key = "test-bucket/path/to/readopted.dat";

    // Plant a range file under `ranges/{bucket}/{XX}/{YYY}/`, named by the same helper
    // every writer uses, and with NO `.meta` for the key. The range deliberately does NOT start at 0, so
    // `validate_size_consistency_integrity`'s full-object arm (which only fires when
    // `ranges.first().start == 0`) stays out of the result and the errors below are
    // attributable to `validate_json_structure_integrity` alone.
    let ranges_dir = cache_dir.join("ranges");
    let range_file =
        s3_proxy::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_4096-8191.bin")
            .expect("sharded range path");
    tokio::fs::create_dir_all(range_file.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&range_file, vec![7u8; 4096])
        .await
        .unwrap();

    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_secs(30),
        3,
    ));
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.clone(),
        "vrfm-instance".to_string(),
    ));
    let mut writer = HybridMetadataWriter::new(
        cache_dir.clone(),
        lock_manager.clone(),
        journal_manager.clone(),
        Arc::new(ConsolidationTrigger::new(1024, 5)),
    );
    let relative = range_file
        .strip_prefix(&ranges_dir)
        .expect("range file under ranges/")
        .to_string_lossy()
        .to_string();
    writer
        .write_range_metadata(
            cache_key,
            RangeSpec::new(4096, 8191, relative, CompressionAlgorithm::None, 4096, 4096),
            WriteMode::Hybrid,
            None,
            Duration::ZERO,
        )
        .await
        .expect("fixture: publishing the re-adopted range must succeed");

    // Read back what the PRODUCER wrote, rather than reconstructing it here.
    let meta_path =
        s3_proxy::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
            .expect("sharded metadata path");
    let published: NewCacheMetadata = serde_json::from_str(
        &tokio::fs::read_to_string(&meta_path)
            .await
            .expect("published .meta"),
    )
    .expect("parse published .meta");
    assert!(
        published.object_metadata.etag.is_empty()
            && published.object_metadata.last_modified.is_empty()
            && published.object_metadata.content_length == 0
            && !published.ranges.is_empty(),
        "fixture: the re-adopted document must carry ObjectMetadata::default() beside a \
         real range — that combination IS the shape under test; got etag={:?} \
         last_modified={:?} content_length={} ranges={}",
        published.object_metadata.etag,
        published.object_metadata.last_modified,
        published.object_metadata.content_length,
        published.ranges.len()
    );

    let validator = CacheValidator::new(cache_dir.clone());
    let result = validator
        .validate_metadata_integrity(&published)
        .await
        .expect("validation must not error");

    // The whole list first, so a red run records every error this shape trips rather than
    // stopping at the first family and leaving the others unproven.
    assert!(
        result.errors.is_empty(),
        "the document re-adoption published, with a range that does not start at \
         0, is healthy and must produce no integrity errors; got {:?}",
        result.errors
    );
    assert!(
        missing_fields(&result.errors).is_empty(),
        "the document re-adoption published must not be reported as missing \
         required fields; got {:?}",
        missing_fields(&result.errors)
    );
    assert!(
        !invalid_fields(&result.errors).contains(&"object_metadata.content_length".to_string()),
        "a re-adopted document carries no validator and no known length by construction, so \
         its content_length must not be reported as an invalid field value; got {:?}",
        invalid_fields(&result.errors)
    );
}

/// WIDTH GUARD on the relaxation. Passes before and after: a document that DOES carry a
/// validator — so an origin was observed to produce it — and still claims zero length
/// beside a range must keep being reported.
#[tokio::test]
async fn a_document_with_a_validator_that_claims_zero_length_with_ranges_is_still_reported() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let object_metadata = ObjectMetadata {
        etag: "\"observed-etag\"".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: 0,
        ..Default::default()
    };
    // Starts at 4096, so the full-object size arm is not what reports this.
    let range = RangeSpec::new_staged(
        4096,
        8191,
        "test-bucket/aa/bbb/object.dat_4096-8191.bin".to_string(),
        CompressionAlgorithm::Lz4,
        4096,
        4096,
        false,
    );

    let result = validator
        .validate_metadata_integrity(&entry_with(object_metadata, vec![range]))
        .await
        .expect("validation must not error");

    assert!(
        invalid_fields(&result.errors).contains(&"object_metadata.content_length".to_string()),
        "the length check must stay live for a document that carries a validator; got {:?}",
        result.errors
    );
}

/// WIDTH GUARD. The `cache_key` requirement is untouched and every writer populates it.
#[tokio::test]
async fn an_empty_cache_key_is_still_reported() {
    let temp_dir = TempDir::new().unwrap();
    let validator = CacheValidator::new(temp_dir.path().to_path_buf());

    let mut entry = entry_with(
        ObjectMetadata {
            etag: "\"observed-etag\"".to_string(),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length: 4096,
            ..Default::default()
        },
        vec![contiguous_range(4096)],
    );
    entry.cache_key = String::new();

    let result = validator
        .validate_metadata_integrity(&entry)
        .await
        .expect("validation must not error");

    assert!(
        missing_fields(&result.errors).contains(&"cache_key".to_string()),
        "an empty cache_key is produced by no writer and must stay reported; got {:?}",
        result.errors
    );
}
