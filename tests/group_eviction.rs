//! Group test binary: eviction. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "cache_cleanup_new_format_test.rs"]
mod cache_cleanup_new_format_test;
#[path = "cache_size_metrics_test.rs"]
mod cache_size_metrics_test;
#[path = "cache_size_tracking_integration_test.rs"]
mod cache_size_tracking_integration_test;
#[path = "cache_size_tracking_performance_test.rs"]
mod cache_size_tracking_performance_test;
#[path = "cache_validation_cleanup_test.rs"]
mod cache_validation_cleanup_test;
#[path = "cache_validator_consistency_path_test.rs"]
mod cache_validator_consistency_path_test;
#[path = "cache_validator_required_field_shapes_test.rs"]
mod cache_validator_required_field_shapes_test;
#[path = "cache_validator_size_consistency_shapes_test.rs"]
mod cache_validator_size_consistency_shapes_test;
#[path = "cache_validator_test.rs"]
mod cache_validator_test;
#[path = "capacity_guard_publication_boundaries_test.rs"]
mod capacity_guard_publication_boundaries_test;
#[path = "capacity_guard_real_floor_test.rs"]
mod capacity_guard_real_floor_test;
#[path = "distributed_eviction_lock_error_handling_test.rs"]
mod distributed_eviction_lock_error_handling_test;
#[path = "distributed_eviction_lock_property_test.rs"]
mod distributed_eviction_lock_property_test;
#[path = "evicted_ranges_bypass_timeout_property_test.rs"]
mod evicted_ranges_bypass_timeout_property_test;
#[path = "eviction_activity_set_safety_test.rs"]
mod eviction_activity_set_safety_test;
#[path = "eviction_buffer_test.rs"]
mod eviction_buffer_test;
#[path = "eviction_coordination_metrics_test.rs"]
mod eviction_coordination_metrics_test;
#[path = "eviction_fence_off_worker_test.rs"]
mod eviction_fence_off_worker_test;
#[path = "eviction_lock_fencing_test.rs"]
mod eviction_lock_fencing_test;
#[path = "eviction_pass_witness_test.rs"]
mod eviction_pass_witness_test;
#[path = "eviction_r13_extremes_and_cross_instance_test.rs"]
mod eviction_r13_extremes_and_cross_instance_test;
#[path = "eviction_rate_wiring_test.rs"]
mod eviction_rate_wiring_test;
#[path = "eviction_target_percentage_property_test.rs"]
mod eviction_target_percentage_property_test;
#[path = "eviction_threshold_validation_property_test.rs"]
mod eviction_threshold_validation_property_test;
#[path = "eviction_trigger_threshold_property_test.rs"]
mod eviction_trigger_threshold_property_test;
#[path = "fs4_lock_call_site_test.rs"]
mod fs4_lock_call_site_test;
#[path = "fs4_lock_semantics_property_test.rs"]
mod fs4_lock_semantics_property_test;
#[path = "global_eviction_lock_test.rs"]
mod global_eviction_lock_test;
#[path = "graded_fixture_generator_test.rs"]
mod graded_fixture_generator_test;
#[path = "hash_distribution_test.rs"]
mod hash_distribution_test;
#[path = "ram_metadata_head_only_eviction_test.rs"]
mod ram_metadata_head_only_eviction_test;
#[path = "range_based_eviction_property_test.rs"]
mod range_based_eviction_property_test;
#[path = "slice_read_cost_test.rs"]
mod slice_read_cost_test;
#[path = "stale_data_removal_test.rs"]
mod stale_data_removal_test;
#[path = "stale_entry_detection_property_test.rs"]
mod stale_entry_detection_property_test;
#[path = "temp_file_cleanup_test.rs"]
mod temp_file_cleanup_test;
#[path = "write_cache_capacity_test.rs"]
mod write_cache_capacity_test;
