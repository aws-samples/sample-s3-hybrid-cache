//! Group test binary: write. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "bulk_write_invalidation_e2e_test.rs"]
mod bulk_write_invalidation_e2e_test;
#[path = "commit_concurrency_property_test.rs"]
mod commit_concurrency_property_test;
#[path = "incremental_range_commit_slot_wait_test.rs"]
mod incremental_range_commit_slot_wait_test;
#[path = "incremental_write_property_test.rs"]
mod incremental_write_property_test;
#[path = "large_file_cache_regression_fault_test.rs"]
mod large_file_cache_regression_fault_test;
#[path = "large_file_cache_regression_preservation_test.rs"]
mod large_file_cache_regression_preservation_test;
#[path = "metadata_heal_overwrite_test.rs"]
mod metadata_heal_overwrite_test;
#[path = "put_cache_delay_budget_test.rs"]
mod put_cache_delay_budget_test;
#[path = "put_cache_invalidation_property_test.rs"]
mod put_cache_invalidation_property_test;
#[path = "put_conflict_invalidation_test.rs"]
mod put_conflict_invalidation_test;
#[path = "put_router_read_bounds_test.rs"]
mod put_router_read_bounds_test;
#[path = "put_then_get_integration_test.rs"]
mod put_then_get_integration_test;
#[path = "response_overrides_and_non_object_puts_test.rs"]
mod response_overrides_and_non_object_puts_test;
#[path = "signed_delete_cache_invalidation_test.rs"]
mod signed_delete_cache_invalidation_test;
#[path = "test_signed_put_handler.rs"]
mod test_signed_put_handler;
#[path = "test_write_cache_flag.rs"]
mod test_write_cache_flag;
#[path = "write_cache_add_accounting_test.rs"]
mod write_cache_add_accounting_test;
#[path = "write_cache_get_ttl_revalidation_test.rs"]
mod write_cache_get_ttl_revalidation_test;
#[path = "write_cache_last_modified_backfill_contention_test.rs"]
mod write_cache_last_modified_backfill_contention_test;
#[path = "write_cache_last_modified_test.rs"]
mod write_cache_last_modified_test;
#[path = "write_cache_max_object_size_reach_test.rs"]
mod write_cache_max_object_size_reach_test;
#[path = "write_cache_path_verification_test.rs"]
mod write_cache_path_verification_test;
#[path = "write_cache_range_test.rs"]
mod write_cache_range_test;
#[path = "write_cache_test.rs"]
mod write_cache_test;
#[path = "write_cache_ttl_exploration_test.rs"]
mod write_cache_ttl_exploration_test;
#[path = "write_cache_ttl_preservation_test.rs"]
mod write_cache_ttl_preservation_test;
#[path = "write_cache_unit_test.rs"]
mod write_cache_unit_test;
#[path = "write_path_internal_headers_test.rs"]
mod write_path_internal_headers_test;
