//! Group test binary: ramtier. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "coordinated_cache_initialization_integration_test.rs"]
mod coordinated_cache_initialization_integration_test;
#[path = "coordinated_cache_initialization_property_test.rs"]
mod coordinated_cache_initialization_property_test;
#[path = "cross_instance_ram_version_mix_test.rs"]
mod cross_instance_ram_version_mix_test;
#[path = "metadata_update_operations_test.rs"]
mod metadata_update_operations_test;
#[path = "mixed_version_range_store_test.rs"]
mod mixed_version_range_store_test;
#[path = "multi_instance_atomic_metadata_writes_integration_test.rs"]
mod multi_instance_atomic_metadata_writes_integration_test;
#[path = "multi_instance_integration_test.rs"]
mod multi_instance_integration_test;
#[path = "ram_cache_sharding_property_test.rs"]
mod ram_cache_sharding_property_test;
#[path = "ram_metadata_absence_confirmation_test.rs"]
mod ram_metadata_absence_confirmation_test;
#[path = "ram_metadata_admission_unification_test.rs"]
mod ram_metadata_admission_unification_test;
#[path = "ram_metadata_consistency_test.rs"]
mod ram_metadata_consistency_test;
#[path = "ram_metadata_masking_regression_test.rs"]
mod ram_metadata_masking_regression_test;
#[path = "ram_tier_standing_guards_test.rs"]
mod ram_tier_standing_guards_test;
#[path = "ram_verify_decision_table_test.rs"]
mod ram_verify_decision_table_test;
#[path = "ram_verify_locks_bound_test.rs"]
mod ram_verify_locks_bound_test;
#[path = "shared_cache_coordination_test.rs"]
mod shared_cache_coordination_test;
