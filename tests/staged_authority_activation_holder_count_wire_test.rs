//! `write_cache.staged_authority_activation_blocking_instance_count` on the `/metrics` wire.
//!
//! Field name is a literal string on purpose, as in `metrics_wire_shape_test.rs`: deriving it
//! from the struct would restate the thing under test.
//!
//! Spec: cache-eviction-at-scale § 16.6.6 B9.
use s3_proxy::metrics::WriteCacheMetrics;

fn wire(m: &WriteCacheMetrics) -> serde_json::Value {
    serde_json::from_str(&serde_json::to_string_pretty(m).unwrap()).unwrap()
}

/// The count reaches the wire under its name, as a number when set and as an explicit `null`
/// when not, beside the blocker pair it counts for.
///
/// # RED SIDE
///
/// With the field removed from `WriteCacheMetrics` this does not compile; with a
/// `#[serde(skip)]` on it, the key is absent and both assertions fail.
#[test]
fn the_activation_holder_count_reaches_the_wire_and_is_null_when_unset() {
    let m = WriteCacheMetrics {
        staged_authority_activation_blocker: Some("ledger_without_ready_record".to_string()),
        staged_authority_activation_blocking_instance: Some("host-a:101".to_string()),
        staged_authority_activation_blocking_instance_count: Some(32),
        ..Default::default()
    };
    assert_eq!(
        wire(&m)["staged_authority_activation_blocking_instance_count"],
        serde_json::json!(32)
    );

    let unset = wire(&WriteCacheMetrics::default());
    let obj = unset.as_object().expect("an object");
    assert!(
        obj.contains_key("staged_authority_activation_blocking_instance_count"),
        "the key must be on the wire even when null"
    );
    assert!(obj["staged_authority_activation_blocking_instance_count"].is_null());
}
