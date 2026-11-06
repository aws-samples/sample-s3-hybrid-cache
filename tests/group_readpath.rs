//! Group test binary: readpath. Every tests/*.rs file is either listed below or has a [[test]]
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
//! In this group a stem is the tail of another file's stem, so selecting it alone also runs
//! the other file's tests unless they are skipped:
//!   cargo test --test <this target> -- integration_test:: --skip download_coordination_integration_test:: --skip s3_integration_test::
//!
//! Several of the files below declare `mod common;`, so tests/common/mod.rs and the
//! fixtures it declares are loaded as a module more than once in this one crate.
//! That is what grouping means, and the included files are deliberately unchanged,
//! so clippy::duplicate_mod is allowed here rather than worked around by editing the
//! 58 test files that declare it.
#![allow(clippy::duplicate_mod)]

#[path = "download_coordination_integration_test.rs"]
mod download_coordination_integration_test;
#[path = "download_coordination_stampede_test.rs"]
mod download_coordination_stampede_test;
#[path = "fetch_time_freshness_test.rs"]
mod fetch_time_freshness_test;
#[path = "forward_request_pinned_test.rs"]
mod forward_request_pinned_test;
#[path = "http_proxy_handlers_test.rs"]
mod http_proxy_handlers_test;
#[path = "integration_test.rs"]
mod integration_test;
#[path = "lz4_roundtrip_preservation_test.rs"]
mod lz4_roundtrip_preservation_test;
#[path = "range_invalidation_divergent_instance_test.rs"]
mod range_invalidation_divergent_instance_test;
#[path = "range_invalidation_marker_suppression_test.rs"]
mod range_invalidation_marker_suppression_test;
#[path = "range_slice_bug_fix_test.rs"]
mod range_slice_bug_fix_test;
#[path = "s3_integration_test.rs"]
mod s3_integration_test;
#[path = "spurious_error_status_test.rs"]
mod spurious_error_status_test;
#[path = "streamed_hit_promotion_test.rs"]
mod streamed_hit_promotion_test;
#[path = "streaming_decompression_property_test.rs"]
mod streaming_decompression_property_test;
#[path = "unified_storage_consistency_property_test.rs"]
mod unified_storage_consistency_property_test;
