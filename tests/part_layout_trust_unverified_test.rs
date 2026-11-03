//! `cache.trust_unverified_ranges` keeps a part layout recorded before 3.0.1: with the
//! override on, an unmarked layout is served as 3.0.0 served it. Its own `[[test]]` target,
//! because the override is process-global.

mod common;

use std::time::Duration;

use common::expired_fixture::{test_config, Fixture, SeedSpec, StoredExpiry};
use s3_proxy::origin_identity::OriginVerdict;

const OBJECT_SIZE: u64 = 4096;

#[tokio::test]
async fn an_unmarked_part_layout_is_served_with_the_override_on() {
    s3_proxy::disk_cache::set_trust_unverified_ranges(true);
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let key = "bucket/unmarked-layout-override.bin";
    let spec = SeedSpec {
        extents: vec![(0, OBJECT_SIZE - 1)],
        etag: "\"unmarked-v2\"".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: OBJECT_SIZE,
        stored_expiry: StoredExpiry::Fresh,
        created_age: Duration::from_secs(5),
        is_write_cached: Some(false),
    };
    fixture.seed(key, &spec);
    // The layout as 3.0.0 wrote it: no marker.
    let meta_path = fixture.meta_path(key);
    let mut value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&meta_path).expect("read .meta"))
            .expect("parse .meta");
    let om = value["object_metadata"]
        .as_object_mut()
        .expect("object_metadata");
    om.remove("part_layout_verified");
    om.insert("parts_count".to_string(), serde_json::json!(2));
    om.insert(
        "part_ranges".to_string(),
        serde_json::json!({"1": [0, 2047], "2": [2048, 4095]}),
    );
    std::fs::write(&meta_path, serde_json::to_string_pretty(&value).unwrap()).expect("write");
    fixture.invalidate_metadata_cache(key).await;
    let part = fixture
        .cache_manager
        .lookup_part(key, 2, OriginVerdict::Matches)
        .await
        .expect("lookup_part must not error");
    assert!(
        part.as_ref()
            .is_some_and(|p| p.start == 2048 && p.end == 4095),
        "with cache.trust_unverified_ranges on, an unmarked part layout must be served as \
         3.0.0 served it: {:?}",
        part.as_ref().map(|p| (p.start, p.end))
    );
    let after = fixture.read_meta(key).expect(".meta");
    assert_eq!(
        after.object_metadata.part_ranges.len(),
        2,
        "the override keeps the unmarked layout"
    );
    assert!(!after.object_metadata.part_layout_verified);
}
