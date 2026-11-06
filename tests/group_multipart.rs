//! Group test binary: multipart. Every tests/*.rs file is either listed below or has a [[test]]
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

#[path = "incomplete_upload_sweep_wiring_test.rs"]
mod incomplete_upload_sweep_wiring_test;
#[path = "mpu_upload_dir_test.rs"]
mod mpu_upload_dir_test;
#[path = "part_number_caching_property_test.rs"]
mod part_number_caching_property_test;
#[path = "select_refusal_test.rs"]
mod select_refusal_test;
#[path = "signed_write_upstream_timeout_test.rs"]
mod signed_write_upstream_timeout_test;
#[path = "write_unknown_outcome_test.rs"]
mod write_unknown_outcome_test;
