//! `write_cache.staged_authority_activation_blocker` and
//! `write_cache.staged_authority_activation_blocking_instance` on the `/metrics` wire.
//!
//! Field names are literal strings on purpose, as in `metrics_wire_shape_test.rs`: deriving them
//! from the struct would restate the thing under test.
//!
//! Spec: cache-eviction-at-scale § 16.6.6 (2026-09-25 amendment, B6).

use s3_proxy::metrics::WriteCacheMetrics;

fn wire(m: &WriteCacheMetrics) -> serde_json::Value {
    serde_json::from_str(&serde_json::to_string_pretty(m).unwrap()).unwrap()
}

/// Both fields reach the wire under their names, carrying the label and the instance.
///
/// # RED SIDE
///
/// With either field removed from `WriteCacheMetrics` this does not compile; with a
/// `#[serde(skip)]` on either, the lookup is absent and the assertion fails.
#[test]
fn the_activation_blocker_pair_reaches_the_wire() {
    let m = WriteCacheMetrics {
        staged_authority_activation_blocker: Some("ledger_without_ready_record".to_string()),
        staged_authority_activation_blocking_instance: Some("host-a:101".to_string()),
        ..Default::default()
    };
    let v = wire(&m);
    assert_eq!(
        v["staged_authority_activation_blocker"],
        serde_json::json!("ledger_without_ready_record")
    );
    assert_eq!(
        v["staged_authority_activation_blocking_instance"],
        serde_json::json!("host-a:101")
    );
}

/// With no blocker, both keys are present and `null`, never absent and never an empty string.
#[test]
fn no_blocker_is_null_rather_than_absent_or_empty() {
    let v = wire(&WriteCacheMetrics::default());
    let obj = v.as_object().unwrap();
    for key in [
        "staged_authority_activation_blocker",
        "staged_authority_activation_blocking_instance",
    ] {
        assert!(obj.contains_key(key), "`{key}` must be on the wire");
        assert!(obj[key].is_null(), "`{key}` must be null with no blocker");
    }
}
