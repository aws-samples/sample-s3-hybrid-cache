//! Group test binary: journal. Every tests/*.rs file is either listed below or has a [[test]]
//! entry of its own in Cargo.toml; a file in neither place is never built or run. To add a test
//! file, add a `#[path = "<file>.rs"]` / `mod <stem>;` pair to the group for its subject. Give it a
//! [[test]] entry instead when its verdict depends on process-global state.
//!
//! Each line below compiles an existing tests/<file>.rs as a module of this one binary, so
//! the files stay where they are and their contents are unchanged. A test's name gains the
//! file stem as a path prefix (`<stem>::<test>`). `cargo test --test <this target> -- <stem>::`
//! selects every test whose full name contains `<stem>::`, which is that file's tests plus
//! those of any file whose stem ends in this one's.
//!
//! Several of the files below declare `mod common;`, so tests/common/mod.rs and the
//! fixtures it declares are loaded as a module more than once in this one crate.
//! That is what grouping means, and the included files are deliberately unchanged,
//! so clippy::duplicate_mod is allowed here rather than worked around by editing the
//! 58 test files that declare it.
#![allow(clippy::duplicate_mod)]

#[path = "batch_append_equivalence_property_test.rs"]
mod batch_append_equivalence_property_test;
#[path = "batched_incremental_write_property_test.rs"]
mod batched_incremental_write_property_test;
#[path = "coordinator_validation_json_shape_test.rs"]
mod coordinator_validation_json_shape_test;
#[path = "fold_drops_removed_staged_objects_test.rs"]
mod fold_drops_removed_staged_objects_test;
#[path = "idle_cycle_peer_delta_test.rs"]
mod idle_cycle_peer_delta_test;
#[path = "inflight_ledger_integration_test.rs"]
mod inflight_ledger_integration_test;
#[path = "journal_based_metadata_updates_test.rs"]
mod journal_based_metadata_updates_test;
#[path = "journal_size_tracking_property_test.rs"]
mod journal_size_tracking_property_test;
#[path = "range_consolidation_test.rs"]
mod range_consolidation_test;
#[path = "range_invalidation_journal_window_test.rs"]
mod range_invalidation_journal_window_test;
#[path = "rolling_validation_scan_integration_test.rs"]
mod rolling_validation_scan_integration_test;
#[path = "size_state_unreadable_sentinel_test.rs"]
mod size_state_unreadable_sentinel_test;
#[path = "staged_authority_activation_blocker_wire_test.rs"]
mod staged_authority_activation_blocker_wire_test;
#[path = "staged_authority_activation_holder_count_wire_test.rs"]
mod staged_authority_activation_holder_count_wire_test;
#[path = "staged_range_predicate_test.rs"]
mod staged_range_predicate_test;
#[path = "staging_tier_fail_open_and_read_tier_coverage_test.rs"]
mod staging_tier_fail_open_and_read_tier_coverage_test;
#[path = "write_ledger_append_throughput_test.rs"]
mod write_ledger_append_throughput_test;
#[path = "write_ledger_candidate_merge_test.rs"]
mod write_ledger_candidate_merge_test;
#[path = "write_ledger_shard_layout_test.rs"]
mod write_ledger_shard_layout_test;
