//! Group test binary: revalidation. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "cached_read_etag_quoting_test.rs"]
mod cached_read_etag_quoting_test;
#[path = "changed_range_revalidation_stale_serve_test.rs"]
mod changed_range_revalidation_stale_serve_test;
#[path = "check_object_expiration_property_test.rs"]
mod check_object_expiration_property_test;
#[path = "coalesced_waiter_client_conditional_test.rs"]
mod coalesced_waiter_client_conditional_test;
#[path = "conditional_range_caching_test.rs"]
mod conditional_range_caching_test;
#[path = "etag_mismatch_invalidation_test.rs"]
mod etag_mismatch_invalidation_test;
#[path = "expired_entry_revalidation_regression_test.rs"]
mod expired_entry_revalidation_regression_test;
#[path = "expired_full_object_mainline_test.rs"]
mod expired_full_object_mainline_test;
#[path = "failed_revalidation_read_lock_test.rs"]
mod failed_revalidation_read_lock_test;
#[path = "fill_capture_before_send_test.rs"]
mod fill_capture_before_send_test;
#[path = "head_expiration_property_test.rs"]
mod head_expiration_property_test;
#[path = "head_ttl_zero_pin_test.rs"]
mod head_ttl_zero_pin_test;
#[path = "object_ttl_refresh_property_test.rs"]
mod object_ttl_refresh_property_test;
#[path = "range_spec_ttl_test.rs"]
mod range_spec_ttl_test;
#[path = "recovery_refetch_version_pin_test.rs"]
mod recovery_refetch_version_pin_test;
#[path = "refresh_object_ttl_property_test.rs"]
mod refresh_object_ttl_property_test;
#[path = "revalidated_hit_read_lock_test.rs"]
mod revalidated_hit_read_lock_test;
#[path = "revalidation_304_reanchor_test.rs"]
mod revalidation_304_reanchor_test;
#[path = "revalidation_etag_authority_test.rs"]
mod revalidation_etag_authority_test;
#[path = "uncacheable_miss_marker_test.rs"]
mod uncacheable_miss_marker_test;
#[path = "validator_less_revalidation_test.rs"]
mod validator_less_revalidation_test;
#[path = "zero_ttl_if_match_revalidation_test.rs"]
mod zero_ttl_if_match_revalidation_test;
#[path = "zero_ttl_if_match_variants_test.rs"]
mod zero_ttl_if_match_variants_test;
