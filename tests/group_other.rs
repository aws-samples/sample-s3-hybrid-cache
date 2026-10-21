//! Group test binary: other. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "connection_keepalive_property_test.rs"]
mod connection_keepalive_property_test;
#[path = "connection_keepalive_test.rs"]
mod connection_keepalive_test;
#[path = "connection_max_lifetime_test.rs"]
mod connection_max_lifetime_test;
#[path = "connection_pool_validation_property_test.rs"]
mod connection_pool_validation_property_test;
#[path = "dns_resolution_preservation_test.rs"]
mod dns_resolution_preservation_test;
#[path = "edge_case_handling_test.rs"]
mod edge_case_handling_test;
#[path = "error_handling_test.rs"]
mod error_handling_test;
#[path = "ip_distribution_property_test.rs"]
mod ip_distribution_property_test;
#[path = "retry_exponential_backoff_property_test.rs"]
mod retry_exponential_backoff_property_test;
#[path = "s3_client_test.rs"]
mod s3_client_test;
#[path = "test_presigned_url.rs"]
mod test_presigned_url;
#[path = "test_tls_proxy_listener.rs"]
mod test_tls_proxy_listener;
#[path = "tls_handshake_preservation_test.rs"]
mod tls_handshake_preservation_test;
#[path = "tls_listener_http_destination_policy_test.rs"]
mod tls_listener_http_destination_policy_test;
#[path = "upstream_first_byte_timeout_test.rs"]
mod upstream_first_byte_timeout_test;
#[path = "upstream_idle_midstream_test.rs"]
mod upstream_idle_midstream_test;
