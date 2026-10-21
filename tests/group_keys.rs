//! Group test binary: keys. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "access_log_and_serve_path_test.rs"]
mod access_log_and_serve_path_test;
#[path = "cache_bypass_headers_integration_test.rs"]
mod cache_bypass_headers_integration_test;
#[path = "cache_bypass_headers_property_test.rs"]
mod cache_bypass_headers_property_test;
#[path = "cache_key_access_point_test.rs"]
mod cache_key_access_point_test;
#[path = "cache_key_collision_test.rs"]
mod cache_key_collision_test;
#[path = "cache_key_length_test.rs"]
mod cache_key_length_test;
#[path = "cache_key_virtual_hosted_test.rs"]
mod cache_key_virtual_hosted_test;
#[path = "host_header_ipv6_integration_test.rs"]
mod host_header_ipv6_integration_test;
#[path = "injected_precondition_ingress_test.rs"]
mod injected_precondition_ingress_test;
#[path = "list_operation_cache_bypass_test.rs"]
mod list_operation_cache_bypass_test;
#[path = "path_traversal_integration_test.rs"]
mod path_traversal_integration_test;
#[path = "s3_request_context_literal_guard_test.rs"]
mod s3_request_context_literal_guard_test;
#[path = "sanitization_consistency_test.rs"]
mod sanitization_consistency_test;
#[path = "unshardable_cache_key_bypass_test.rs"]
mod unshardable_cache_key_bypass_test;
#[path = "versioned_request_bypass_test.rs"]
mod versioned_request_bypass_test;
#[path = "via_loop_detection_test.rs"]
mod via_loop_detection_test;
