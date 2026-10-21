//! The `st_size` pass has a production caller, and a consolidation cycle reaches it.
//!
//! # The subject is the WIRING, not the pass
//!
//! `tests/st_size_fold_test.rs` already drives `run_st_size_fold` and
//! `maybe_run_st_size_fold` directly, ten times over. Every one of those tests passes
//! identically whether or not anything in production ever calls the pass, so none of them can
//! detect the state this file exists to rule out: a maintenance pass that is complete,
//! unit-tested, entirely `pub` — so the `dead_code` lint cannot fire on it and clippy stays
//! green — and called by nothing.
//!
//! So the fixture here drives `JournalConsolidator::run_consolidation_cycle` and asserts the
//! pass's fields appeared. Calling the pass would prove nothing.
//!
//! # It asserts field VALUES, and the document parses on BOTH sides
//!
//! A per-shard document with every one of the pass's fields absent parses perfectly — that is
//! exactly what the `#[serde(flatten)]` `other` slot guarantees — so "it parsed" discriminates
//! nothing. The fixture therefore **installs a valid, parseable document for each shard first**,
//! with the pass's keys absent, and then asserts the transition:
//!
//! | | before the cycle | after the cycle |
//! |---|---|---|
//! | the document | parses | parses |
//! | `Map::get("byte_sum")` | `None` (missing key) | `Some(the fixture's total)` |
//! | the stalest-scan-age gauge | `None` (no shard folded) | `Some(a small age)` |
//!
//! Absence is a **missing key**, not a zero. A folded zero is a key present holding `0`, and
//! `serde_json::Map::get` is what separates them.
//!
//! # The cache is IDLE, and that is deliberate
//!
//! Nothing in the fixture writes a journal entry or a size delta, so the cycle takes its idle
//! early return. The pass must still have run — it is called above that return, for the same
//! reason the rolling pass above it is. A wiring line placed below the return passes every
//! other assertion in the repo and fails this one.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use s3_proxy::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
use s3_proxy::journal_manager::JournalManager;
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use s3_proxy::st_size_fold::collect_st_size_fold_gauges;
use s3_proxy::staged_fold::{read_shard_checkpoints, Shard, StagedShardCheckpoint};
use tempfile::TempDir;

/// Seed one range file and return its size.
fn seed_range(cache_dir: &Path, bucket: &str, l1: &str, l2: &str, name: &str, size: usize) -> u64 {
    let dir = cache_dir.join("ranges").join(bucket).join(l1).join(l2);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), vec![0u8; size]).unwrap();
    size as u64
}

/// Seed one metadata file.
fn seed_meta(cache_dir: &Path, bucket: &str, l1: &str, l2: &str, name: &str) {
    let dir = cache_dir.join("metadata").join(bucket).join(l1).join(l2);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), b"{}").unwrap();
}

async fn consolidator_for(cache_dir: &Path) -> Arc<JournalConsolidator> {
    let journal_manager = Arc::new(JournalManager::new(
        cache_dir.to_path_buf(),
        "wiring-test-instance".to_string(),
    ));
    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.to_path_buf(),
        Duration::from_secs(5),
        3,
    ));
    let consolidator = Arc::new(JournalConsolidator::new(
        cache_dir.to_path_buf(),
        journal_manager,
        lock_manager,
        ConsolidationConfig {
            interval: Duration::from_secs(5),
            size_threshold: 1024 * 1024,
            entry_count_threshold: 100,
            // 0 disables eviction, so a cycle cannot spawn an eviction pass that would
            // compete with the assertions below.
            max_cache_size: 0,
            eviction_trigger_percent: 95,
            eviction_target_percent: 80,
            stale_entry_timeout_secs: 300,
            consolidation_cycle_timeout: Duration::from_secs(30),
            max_keys_per_cycle: 5000,
            ..ConsolidationConfig::default()
        },
    ));
    consolidator.initialize().await.unwrap();
    consolidator
}

/// A consolidation cycle installs the pass's own fields on every shard, with the fixture's
/// independently computed values.
///
/// # THIS IS THE RED SIDE
///
/// Remove the `maybe_run_st_size_fold` call from `run_consolidation_cycle` and this test fails
/// naming the absent field. It cannot be satisfied by the pass being correct, only by the pass
/// being *reached*.
///
/// # Two shards, not one
///
/// One shard makes "installed per shard as the pass proceeds" and "installed once at the end"
/// indistinguishable, and the sizes are distinct and non-multiples so a figure that summed the
/// wrong shard cannot land on the right total by coincidence.
///
/// A failure implicates the call in `JournalConsolidator::run_consolidation_cycle` — check
/// first that it is still above the idle early return and above the global-lock acquisition,
/// because a cycle that returns early reaches nothing below it.
#[tokio::test]
async fn a_consolidation_cycle_installs_the_st_size_folds_fields_on_every_shard() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();

    // Shard bucket/aa — two leaves, three range files, two objects.
    let shard_a = Shard::new("bucket", "aa");
    let a_bytes = seed_range(dir, "bucket", "aa", "111", "one_0-1099.bin", 1_100)
        + seed_range(dir, "bucket", "aa", "111", "one_1100-4399.bin", 3_300)
        + seed_range(dir, "bucket", "aa", "222", "two_0-6999.bin", 7_000);
    seed_meta(dir, "bucket", "aa", "111", "one.meta");
    seed_meta(dir, "bucket", "aa", "222", "two.meta");

    // Shard bucket/bb — two leaves, two range files, one object.
    let shard_b = Shard::new("bucket", "bb");
    let b_bytes = seed_range(dir, "bucket", "bb", "333", "three_0-511.bin", 512)
        + seed_range(dir, "bucket", "bb", "444", "orphan_0-99.bin", 100);
    seed_meta(dir, "bucket", "bb", "333", "three.meta");

    // A valid, parseable document per shard with NONE of the pass's keys. This is what makes
    // the assertions below value assertions rather than parse assertions.
    for shard in [&shard_a, &shard_b] {
        StagedShardCheckpoint::unfolded_for(shard)
            .install(dir)
            .unwrap();
    }

    let before = read_shard_checkpoints(dir);
    assert!(
        before.is_complete(),
        "the seeded documents must be readable, or the before/after comparison is vacuous"
    );
    for shard in [&shard_a, &shard_b] {
        let c = before
            .get(shard)
            .unwrap_or_else(|| panic!("no seeded document for shard {shard}"));
        for key in [
            "entry_count",
            "range_count",
            "byte_sum",
            "histogram",
            "scanned_at",
        ] {
            assert!(
                c.other.get(key).is_none(),
                "shard {shard} must start with `{key}` ABSENT — a missing key, not a zero — or \
                 this test cannot tell the pass having run from the seed already carrying it"
            );
        }
    }
    assert_eq!(
        collect_st_size_fold_gauges(&read_shard_checkpoints(dir))
            .st_size_fold_oldest_shard_scan_age_secs,
        None,
        "with no shard folded the stalest-scan-age gauge must be the None sentinel, never a zero"
    );

    // The subject: one cycle, on a cache with no pending journal work at all.
    let consolidator = consolidator_for(dir).await;
    consolidator.run_consolidation_cycle().await.unwrap();
    // The cycle starts the pass on its own task rather than awaiting it, so let it land.
    consolidator.await_maintenance_passes().await;

    let after = read_shard_checkpoints(dir);
    assert!(
        after.is_complete(),
        "the installed documents must be readable"
    );

    for (shard, expected_bytes, expected_ranges, expected_entries) in [
        (&shard_a, a_bytes, 3u64, 2u64),
        (&shard_b, b_bytes, 2u64, 1u64),
    ] {
        let c = after
            .get(shard)
            .unwrap_or_else(|| panic!("no document for shard {shard} after the cycle"));
        let field = |key: &str| -> u64 {
            c.other
                .get(key)
                .unwrap_or_else(|| {
                    panic!(
                        "field `{key}` is ABSENT from shard {shard}'s document after a \
                         consolidation cycle — the cycle did not reach the st_size pass"
                    )
                })
                .as_u64()
                .unwrap_or_else(|| panic!("field `{key}` on shard {shard} is not an integer"))
        };
        assert_eq!(field("byte_sum"), expected_bytes, "byte_sum for {shard}");
        assert_eq!(
            field("range_count"),
            expected_ranges,
            "range_count for {shard}"
        );
        assert_eq!(
            field("entry_count"),
            expected_entries,
            "entry_count for {shard}"
        );
        assert!(
            c.other.get("scanned_at").and_then(|v| v.as_u64()).is_some(),
            "shard {shard} must carry the provenance timestamp the age gauge reads"
        );
        // Provenance, and the identity is the one the caller holds rather than a placeholder.
        // It is the write ledger's instance id, NOT the journal manager's, because that is
        // what the sibling pass on the line above is given — so the two passes stamp the same
        // shard document with the same identity. Asserting the literal would pin the derivation
        // (hostname and pid) rather than the wiring.
        assert_eq!(
            c.other.get("scanned_by").and_then(|v| v.as_str()),
            Some(consolidator.write_ledger().instance_id()),
            "the pass must be stamped with THIS instance's own id by its caller"
        );
    }

    // And the gauge the publication exists for now reads a figure rather than the sentinel.
    let gauges = collect_st_size_fold_gauges(&read_shard_checkpoints(dir));
    let age = gauges.st_size_fold_oldest_shard_scan_age_secs.expect(
        "every shard now carries a scan timestamp, so the stalest-age gauge must report a \
         figure rather than the incomplete-membership sentinel",
    );
    assert!(
        age < 300,
        "the age must be that of a scan taken during this test, not an epoch-derived figure: {age}s"
    );
    assert_eq!(
        gauges.st_size_folds_declined_overlap_total, 0,
        "nothing else in this process runs the pass, so no tick can have been declined"
    );
}
