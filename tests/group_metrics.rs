//! Group test binary: metrics. Every tests/*.rs file is either listed below or has a [[test]]
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
//!   cargo test --test <this target> -- logging_test:: --skip access_logging_test:: --skip conditional_request_logging_test::
//!
//! Several of the files below declare `mod common;`, so tests/common/mod.rs and the
//! fixtures it declares are loaded as a module more than once in this one crate.
//! That is what grouping means, and the included files are deliberately unchanged,
//! so clippy::duplicate_mod is allowed here rather than worked around by editing the
//! 58 test files that declare it.
#![allow(clippy::duplicate_mod)]

#[path = "access_logging_test.rs"]
mod access_logging_test;
#[path = "bench_metadata_cli_test.rs"]
mod bench_metadata_cli_test;
#[path = "cache_hit_buffer_metrics_test.rs"]
mod cache_hit_buffer_metrics_test;
#[path = "cache_statistics_test.rs"]
mod cache_statistics_test;
#[path = "conditional_request_logging_test.rs"]
mod conditional_request_logging_test;
#[path = "dashboard_error_handling_unit_test.rs"]
mod dashboard_error_handling_unit_test;
#[path = "dashboard_integration_test.rs"]
mod dashboard_integration_test;
#[path = "dashboard_property_test.rs"]
mod dashboard_property_test;
#[path = "logging_test.rs"]
mod logging_test;
#[path = "metrics_wire_shape_test.rs"]
mod metrics_wire_shape_test;
#[path = "objects_scan_results_test.rs"]
mod objects_scan_results_test;
#[path = "test_otlp.rs"]
mod test_otlp;
