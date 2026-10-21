//! Part 3 — `DiskCacheManager::store_range` must not invalidate a key's ranges over a
//! QUOTING difference in one ETag.
//!
//! # The defect
//!
//! `store_range` compares the stored `object_metadata.etag` against the incoming one with a bare
//! `!=`. S3 records a multipart ETag in the CompleteMultipartUpload XML body rather than in a
//! response header, so entries written by the pre-fix producer carry the **bare** `abc-2` form
//! while a later GET of the same, unchanged object carries the wire form `"abc-2"`. The bare `!=`
//! reads that as a changed object and invalidates **every** existing range for the key — the
//! largest blast radius of the seven comparison sites, and the same shape as the reported
//! multipart defect.
//!
//! The direction is cost rather than incorrectness: the entry is correct after the refetch. But
//! it fires once per affected key and throws away every range the key had.
//!
//! # Why each case here is two-sided, and what the discriminator is
//!
//! **Both operands quoted, or both bare, passes with the defect present.** So the surviving case
//! deliberately mixes the spellings — stored bare `abc-2` against an incoming quoted `"abc-2"` —
//! and each surviving case is paired with a case whose ETag differs in **content** rather than in
//! quoting, asserting that invalidation still happens. Without that pair, routing the comparison
//! through a normalising helper could be a silent widening into "never invalidate", which would
//! be a far worse defect than the one being fixed.
//!
//! Two content-differing shapes are used, because they are different edits: `abd-2` changes the
//! opaque hex, and `abc-3` changes the part count. A helper that stripped too much — the trailing
//! `-<parts>` suffix, say — passes the first and fails the second.
//!
//! # What is asserted, and why it is the range SET
//!
//! The count of `.bin` range files on disk for the key. `store_range` deletes the existing range
//! files on the mismatch branch, so survival is observable as a count and nothing softer: a
//! second `store_range` adds one range, so two ranges means the first survived and one means it
//! was invalidated. Reading the count rather than a metric avoids asserting on an adjacent figure.

mod common;

use s3_proxy::disk_cache::DiskCacheManager;
use tempfile::TempDir;

/// The on-disk form a pre-fix multipart producer wrote: bare hex, `-<parts>`.
const STORED_BARE: &str = "3858f62230ac3c915f300c664312c11f-2";
/// The identical ETag as S3 sends it on the wire.
const SAME_QUOTED: &str = "\"3858f62230ac3c915f300c664312c11f-2\"";
/// A genuinely different object: the opaque part differs (`...c11f` -> `...c11e`).
const DIFFERENT_OPAQUE: &str = "\"3858f62230ac3c915f300c664312c11e-2\"";
/// A genuinely different object: the part count differs.
const DIFFERENT_PARTS: &str = "\"3858f62230ac3c915f300c664312c11f-3\"";

fn object_metadata(etag: &str, len: u64) -> s3_proxy::cache_types::ObjectMetadata {
    s3_proxy::cache_types::ObjectMetadata {
        etag: etag.to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: len,
        content_type: Some("application/octet-stream".to_string()),
        response_headers: std::collections::HashMap::new(),
        upload_state: s3_proxy::cache_types::UploadState::Complete,
        cumulative_size: len,
        parts: Vec::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
        compressed_size: 0,
        parts_count: None,
        part_ranges: std::collections::HashMap::new(),
        upload_id: None,
        is_write_cached: false,
        write_cache_expires_at: None,
        write_cache_created_at: None,
        write_cache_last_accessed: None,
        graduation_accounted: false,
    }
}

/// Every `.bin` under `ranges/`, recursively — the set `store_range` deletes from.
fn range_files(cache_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "bin") {
                out.push(p);
            }
        }
    }
    let mut out = Vec::new();
    walk(&cache_dir.join("ranges"), &mut out);
    out.sort();
    out
}

/// Store range 0-1023 under `first_etag`, then range 2048-3071 under `second_etag`, and report
/// how many range files survive.
async fn ranges_after_second_store(first_etag: &str, second_etag: &str) -> usize {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().to_path_buf();
    let mut disk = DiskCacheManager::new(cache_dir.clone(), true, 1024, false, 1_048_576);
    let key = "/test-bucket/multipart-object.bin";
    let ttl = std::time::Duration::from_secs(315_360_000);

    disk.store_range(
        key,
        0,
        1023,
        &vec![b'A'; 1024],
        object_metadata(first_etag, 4096),
        ttl,
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        range_files(&cache_dir).len(),
        1,
        "fixture: the first store must leave exactly one range on disk, or the counts below \
         measure nothing"
    );

    disk.store_range(
        key,
        2048,
        3071,
        &vec![b'B'; 1024],
        object_metadata(second_etag, 4096),
        ttl,
        false,
    )
    .await
    .unwrap();
    range_files(&cache_dir).len()
}

/// The fix: a quoting-only difference must leave the existing range alone.
#[tokio::test]
async fn a_quoting_only_difference_keeps_the_existing_ranges() {
    let surviving = ranges_after_second_store(STORED_BARE, SAME_QUOTED).await;
    assert_eq!(
        surviving, 2,
        "stored `{STORED_BARE}` against incoming `{SAME_QUOTED}` is ONE object spelled two ways, \
         so the first range must still be on disk beside the second. {surviving} range(s) means \
         store_range treated a quoting difference as a changed object and invalidated every \
         range the key had"
    );
}

/// The same, with the operands the other way round: an entry stored from the wire form, read
/// back against a bare one. A helper applied to only one side would pass the test above.
#[tokio::test]
async fn the_quoting_difference_is_symmetric() {
    let surviving = ranges_after_second_store(SAME_QUOTED, STORED_BARE).await;
    assert_eq!(
        surviving, 2,
        "quote-insensitivity must not depend on which side carries the quotes"
    );
}

/// The pair that stops the fix being a widening: a different opaque value still invalidates.
#[tokio::test]
async fn a_different_opaque_etag_still_invalidates() {
    let surviving = ranges_after_second_store(STORED_BARE, DIFFERENT_OPAQUE).await;
    assert_eq!(
        surviving, 1,
        "`{DIFFERENT_OPAQUE}` is a DIFFERENT object from `{STORED_BARE}`, so the existing range \
         must be invalidated and only the newly stored one remain. {surviving} ranges means the \
         comparison stopped detecting real changes, which is a worse defect than the one being \
         fixed"
    );
}

/// And a different part count, which is a different edit from a different opaque value: a helper
/// that stripped the `-<parts>` suffix would pass the case above and fail this one.
#[tokio::test]
async fn a_different_part_count_still_invalidates() {
    let surviving = ranges_after_second_store(STORED_BARE, DIFFERENT_PARTS).await;
    assert_eq!(
        surviving, 1,
        "`{DIFFERENT_PARTS}` differs from `{STORED_BARE}` only in the part count, and that is a \
         different object: {surviving} ranges means the suffix is being ignored"
    );
}
