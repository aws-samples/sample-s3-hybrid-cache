//! The per-instance RAM metadata tier against the shared `.meta` (V3-GH-PORT R4).
//!
//! Each peer write below is made by writing the shared `.meta` (and its range files)
//! directly, the way another instance's write appears, without driving any local
//! code path that would evict this instance's RAM copy. Local hygiene therefore
//! cannot pass the cross-instance test (`cache-coherency-invariants.md`).

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::expired_fixture::{test_config, Fixture, SeedSpec, StoredExpiry};
use s3_proxy::cache_types::{ObjectMetadata, RangeLookupPurpose};
use s3_proxy::range_handler::RangeSpec;

const OBJECT_SIZE: u64 = 4096;

fn fresh(extents: Vec<(u64, u64)>, etag: &str) -> SeedSpec {
    let mut spec = SeedSpec::expired(extents, OBJECT_SIZE, etag);
    spec.stored_expiry = StoredExpiry::Fresh;
    spec.created_age = Duration::from_secs(1);
    spec
}

fn whole() -> RangeSpec {
    RangeSpec {
        start: 0,
        end: OBJECT_SIZE - 1,
    }
}

async fn ram_snapshot_etag(fixture: &Fixture, key: &str) -> Option<String> {
    fixture
        .cache_manager
        .get_metadata_cache()
        .peek(key)
        .await
        .map(|m| m.object_metadata.etag)
}

async fn load_into_ram(fixture: &Fixture, key: &str) -> s3_proxy::cache_types::NewCacheMetadata {
    fixture.invalidate_metadata_cache(key).await;
    let loaded = fixture
        .cache_manager
        .get_metadata_cached(key)
        .await
        .expect("read")
        .expect("seeded");
    assert!(
        ram_snapshot_etag(fixture, key).await.is_some(),
        "fixture: the document must be resident in the RAM metadata tier"
    );
    loaded
}

async fn consolidate(fixture: &Fixture) {
    fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("shared-storage consolidator")
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
}

/// R4b. A peer overwrote the object: the shared `.meta` names V2 and its two range files,
/// while this instance's RAM snapshot still names V1. `current_etag` is read from the
/// shared `.meta` (V2). The stale side is the RAM snapshot, so the lookup must keep the
/// peer's ranges, serve them, and drop the snapshot. Mutation that must break this: remove
/// the shared re-read in `RangeHandler::find_cached_ranges`, which invalidates on the
/// snapshot's word and unlinks the NEW version's range files.
#[tokio::test]
async fn a_stale_ram_snapshot_never_deletes_a_peers_new_version() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/r4b-peer-overwrite.bin";
    fixture.seed(key, &fresh(vec![(0, OBJECT_SIZE - 1)], "\"v1\""));
    let snapshot = load_into_ram(&fixture, key).await;
    assert_eq!(snapshot.object_metadata.etag, "\"v1\"");

    // The peer's write: a new version with a different extent layout.
    fixture.seed(
        key,
        &fresh(vec![(0, 2047), (2048, OBJECT_SIZE - 1)], "\"v2\""),
    );
    let current_etag = fixture.production_current_etag(key);
    assert_eq!(current_etag.as_deref(), Some("\"v2\""), "fixture");

    let overlap = fixture
        .range_handler
        .find_cached_ranges(
            key,
            &whole(),
            current_etag.as_deref(),
            Some(&snapshot),
            RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("lookup");

    assert!(
        fixture.bin_path(key, 0, 2047).exists()
            && fixture.bin_path(key, 2048, OBJECT_SIZE - 1).exists(),
        "the lookup deleted the peer's new-version range files on a stale RAM snapshot's word"
    );
    assert_eq!(
        fixture
            .read_meta(key)
            .map(|m| m.object_metadata.etag)
            .as_deref(),
        Some("\"v2\""),
        "the peer's .meta must survive"
    );
    assert!(
        overlap.can_serve_from_cache,
        "the shared document covers the request and must be served: {} cached range(s)",
        overlap.cached_ranges.len()
    );
    assert!(
        overlap.cached_ranges.iter().all(|r| r.etag == "\"v2\""),
        "only the shared document's ranges may be returned"
    );
    assert_eq!(
        ram_snapshot_etag(&fixture, key).await,
        None,
        "the stale V1 snapshot must be dropped"
    );
}

/// R4b, other half. When the shared `.meta` itself disagrees with `current_etag`, the
/// version change is real: the ranges are invalidated as before, and the RAM snapshot of
/// the invalidated document must go with them. Mutation that must break this: drop the
/// `invalidate_metadata_cache` in the ETag-mismatch arm.
#[tokio::test]
async fn a_real_version_change_drops_the_ram_snapshot_with_the_ranges() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/r4b-real-change.bin";
    fixture.seed(key, &fresh(vec![(0, OBJECT_SIZE - 1)], "\"v2\""));
    let snapshot = load_into_ram(&fixture, key).await;

    let overlap = fixture
        .range_handler
        .find_cached_ranges(
            key,
            &whole(),
            Some("\"v3\""),
            Some(&snapshot),
            RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("lookup");

    assert!(
        overlap.cached_ranges.is_empty(),
        "a real change serves nothing"
    );
    assert!(
        !fixture.bin_path(key, 0, OBJECT_SIZE - 1).exists(),
        "a real version change still invalidates the superseded ranges"
    );
    assert_eq!(
        ram_snapshot_etag(&fixture, key).await,
        None,
        "the snapshot of the invalidated document survived the invalidation"
    );
}

/// R4c. `evict_range` rewrites the `.meta`; its RAM snapshot must not outlive that.
/// Mutation that must break this: drop the invalidation at the end of `evict_range`.
#[tokio::test]
async fn evicting_a_range_drops_the_ram_snapshot() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/r4c-evict-range.bin";
    fixture.seed(
        key,
        &fresh(vec![(0, 2047), (2048, OBJECT_SIZE - 1)], "\"e\""),
    );
    load_into_ram(&fixture, key).await;

    fixture
        .cache_manager
        .evict_range(key, 2048, OBJECT_SIZE - 1)
        .await
        .expect("evict");

    assert_eq!(
        fixture.read_meta(key).map(|m| m.ranges.len()),
        Some(1),
        "fixture: one range must remain"
    );
    assert_eq!(ram_snapshot_etag(&fixture, key).await, None);
}

/// R4d. A consolidation that rewrites a `.meta` drops this instance's RAM snapshot of it.
/// Local hygiene only (a peer's snapshot is bounded by its own refresh interval).
/// Mutation that must break this: drop the invalidation in `write_metadata_to_disk`.
#[tokio::test]
async fn consolidating_a_meta_drops_the_ram_snapshot() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    // The wiring production does at proxy start (`HttpProxy` -> `set_cache_manager_in_consolidator`).
    fixture
        .cache_manager
        .set_cache_manager_in_consolidator()
        .await;
    let key = "test-bucket/r4d-consolidate.bin";
    fixture.seed(key, &fresh(vec![(0, 2047)], "\"c\""));
    let headers = HashMap::from([
        ("etag".to_string(), "\"c\"".to_string()),
        (
            "content-range".to_string(),
            format!("bytes 2048-{}/{}", OBJECT_SIZE - 1, OBJECT_SIZE),
        ),
    ]);
    fixture
        .range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            key,
            2048,
            OBJECT_SIZE - 1,
            &vec![b'C'; 2048],
            ObjectMetadata::new_with_headers(
                "\"c\"".to_string(),
                String::new(),
                OBJECT_SIZE,
                None,
                headers,
            ),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("journalled fill");
    // Resident immediately before the rewrite, whatever the store path did to it.
    load_into_ram(&fixture, key).await;

    consolidate(&fixture).await;

    assert_eq!(
        fixture.read_meta(key).map(|m| m.ranges.len()),
        Some(2),
        "fixture: consolidation must have rewritten the .meta with the new range"
    );
    assert_eq!(ram_snapshot_etag(&fixture, key).await, None);
}

async fn store(fixture: &Fixture, key: &str, start: u64, etag: &str) {
    let mut headers = HashMap::from([(
        "content-range".to_string(),
        format!("bytes {}-{}/20", start, start + 9),
    )]);
    if !etag.is_empty() {
        headers.insert("etag".to_string(), etag.to_string());
    }
    fixture
        .range_handler
        .get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            key,
            start,
            start + 9,
            &[b'E'; 10],
            ObjectMetadata::new_with_headers(etag.to_string(), String::new(), 20, None, headers),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("journalled fill");
    tokio::time::sleep(Duration::from_millis(20)).await;
}

/// R4e. A `.meta` created by consolidation takes its identity from the newest fill that
/// carries an ETag: an ETag-less fill never defines it, and an older version's fill never
/// displaces the newer version's. Mutation that must break this: take the first entry
/// with any `object_metadata` (the previous rule), or the first ETag-carrying one.
#[tokio::test]
async fn a_created_meta_takes_the_newest_etag_carrying_fill() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;

    let etagless_first = "test-bucket/r4e-etagless-first.bin";
    store(&fixture, etagless_first, 0, "").await;
    store(&fixture, etagless_first, 10, "\"known\"").await;

    let older_first = "test-bucket/r4e-older-first.bin";
    store(&fixture, older_first, 0, "\"old\"").await;
    store(&fixture, older_first, 10, "\"new\"").await;

    consolidate(&fixture).await;

    assert_eq!(
        fixture
            .read_meta(etagless_first)
            .map(|m| m.object_metadata.etag)
            .as_deref(),
        Some("\"known\""),
        "an ETag-less fill defined the object's identity"
    );
    let meta = fixture.read_meta(older_first).expect("created .meta");
    assert_eq!(
        meta.object_metadata.etag, "\"new\"",
        "an older version's fill defined the object's identity"
    );
    assert_eq!(
        meta.ranges
            .iter()
            .map(|r| (r.start, r.end))
            .collect::<Vec<_>>(),
        vec![(10, 19)],
        "only the newer version's fill may be listed under its ETag"
    );
}

/// A stale RAM snapshot replaced by the shared document contributes no extents: the lookup's
/// coverage comes from the document its ranges are labelled from. Snapshot V1 lists one
/// whole-object extent; the peer's V2 `.meta` lists two halves. Mutation that must break this:
/// pass the caller's preload into `DiskCacheManager::find_cached_ranges` again, which lists
/// the snapshot's extent under V2's ETag.
#[tokio::test]
async fn a_replaced_snapshot_contributes_no_extents_to_the_lookup() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/scn1-replaced-snapshot.bin";
    fixture.seed(key, &fresh(vec![(0, OBJECT_SIZE - 1)], "\"v1\""));
    let snapshot = load_into_ram(&fixture, key).await;
    fixture.seed(
        key,
        &fresh(vec![(0, 2047), (2048, OBJECT_SIZE - 1)], "\"v2\""),
    );
    let current_etag = fixture.production_current_etag(key);
    assert_eq!(current_etag.as_deref(), Some("\"v2\""), "fixture");

    let overlap = fixture
        .range_handler
        .find_cached_ranges(
            key,
            &whole(),
            current_etag.as_deref(),
            Some(&snapshot),
            RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("lookup");

    let mut extents: Vec<(u64, u64)> = overlap
        .cached_ranges
        .iter()
        .map(|r| (r.start, r.end))
        .collect();
    extents.sort_unstable();
    assert_eq!(
        extents,
        vec![(0, 2047), (2048, OBJECT_SIZE - 1)],
        "the overlap's extents must be the shared document's, the one its ETag label comes from"
    );
    assert!(overlap.cached_ranges.iter().all(|r| r.etag == "\"v2\""));
}

/// Control: when the snapshot agrees with the shared document, the lookup is unchanged and the
/// snapshot stays resident.
#[tokio::test]
async fn an_agreeing_snapshot_gives_the_same_extents() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "test-bucket/scn1-agreeing-snapshot.bin";
    fixture.seed(
        key,
        &fresh(vec![(0, 2047), (2048, OBJECT_SIZE - 1)], "\"v2\""),
    );
    let snapshot = load_into_ram(&fixture, key).await;

    let overlap = fixture
        .range_handler
        .find_cached_ranges(
            key,
            &whole(),
            Some("\"v2\""),
            Some(&snapshot),
            RangeLookupPurpose::FreshServe,
        )
        .await
        .expect("lookup");

    let mut extents: Vec<(u64, u64)> = overlap
        .cached_ranges
        .iter()
        .map(|r| (r.start, r.end))
        .collect();
    extents.sort_unstable();
    assert_eq!(extents, vec![(0, 2047), (2048, OBJECT_SIZE - 1)]);
    assert!(overlap.can_serve_from_cache);
    assert_eq!(
        ram_snapshot_etag(&fixture, key).await.as_deref(),
        Some("\"v2\""),
        "an agreeing snapshot must stay resident"
    );
}
