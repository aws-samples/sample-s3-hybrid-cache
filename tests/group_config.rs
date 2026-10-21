//! Group test binary: config. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "bounded_shutdown_exit_test.rs"]
mod bounded_shutdown_exit_test;
#[path = "cache_match_patterns_behavior_test.rs"]
mod cache_match_patterns_behavior_test;
#[path = "cache_match_patterns_resolve_once_test.rs"]
mod cache_match_patterns_resolve_once_test;
#[path = "common_harness_compiles.rs"]
mod common_harness_compiles;
#[path = "compression_batch_size_config_test.rs"]
mod compression_batch_size_config_test;
#[path = "dependency_advisories_bug_condition_test.rs"]
mod dependency_advisories_bug_condition_test;
#[path = "rules_reload_off_request_path_test.rs"]
mod rules_reload_off_request_path_test;
#[path = "startup_config_warning_log_test.rs"]
mod startup_config_warning_log_test;
