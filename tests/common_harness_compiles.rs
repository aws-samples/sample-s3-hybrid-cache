//! Ensures `tests/common/mod.rs` (the shared StubS3Client harness introduced
//! by Task 0 of the `download-coordination-ttl-correctness` spec) compiles
//! alongside the rest of the integration test suite.
//!
//! No tests are authored here yet. Later tasks will import `common::StubS3Client`
//! from their own integration-test files.

mod common;

// Touch the symbols so the module is not considered dead code by the compiler
// when no other test file references them. `#[allow(dead_code)]` inside
// `tests/common/mod.rs` covers the API surface; this assertion guards the
// trait-object conversion so stub routing stays wired up correctly.
#[test]
fn stub_harness_trait_object_conversion_compiles() {
    let stub = common::StubS3Client::new().with_default(common::StubResponse::forbidden());
    let _trait_object: std::sync::Arc<dyn s3_proxy::S3ClientApi + Send + Sync> =
        stub.into_trait_object();
    assert!(common::test_tls_config().is_none());
}
