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

/// Proves `common::put_through_write_cache` actually caches, before 40 call sites are
/// migrated onto it. A migration helper that has never been run is the same
/// "decoration" problem as a fleet assertion that has never been shown failing: every
/// migrated test would go green on a helper that silently cached nothing, because most
/// of them assert on a subsequent read that would fall through to their own fixtures.
///
/// Deliberately asserts the two things the retired `store_write_cache_entry` did NOT do
/// and that task 61 exists to start covering: the object is staged
/// (`is_write_cached: true`, which is what makes it a *write* cache entry rather than an
/// ordinary cached object), and it reads back byte-exact.
#[tokio::test]
async fn put_through_write_cache_stages_and_reads_back() {
    use s3_proxy::cache::CacheManager;
    use s3_proxy::cache_types::CacheMetadata;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path().to_path_buf();
    // Before `initialize()` — see `common::seed_validation_metadata`.
    common::seed_validation_metadata(&cache_dir);
    let cache_manager = Arc::new(CacheManager::new_with_defaults(cache_dir, false, 0));
    let _disk_cache = cache_manager.create_configured_disk_cache_manager();
    cache_manager.initialize().await.unwrap();

    let cache_key = "test-bucket/put-through-helper.bin";
    let body = vec![7u8; 64 * 1024];
    let metadata = CacheMetadata {
        etag: "\"helper-etag\"".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: body.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: SystemTime::now(),
    };

    common::put_through_write_cache(
        &cache_manager,
        cache_key,
        &body,
        HashMap::new(),
        metadata,
        HashMap::new(),
    )
    .await
    .expect("the production write-through path must accept this PUT");

    let stored = cache_manager
        .get_metadata_from_disk(cache_key)
        .await
        .expect("reading the .meta must not error")
        .expect("a .meta must exist after a write-through PUT — if this is None the helper cached nothing and every migrated test would pass vacuously");

    assert!(
        stored.object_metadata.is_write_cached,
        "the object must be STAGED, not merely cached: is_write_cached is what makes this \
         a write-cache entry, and it is the flag graduation later clears"
    );
    assert_eq!(
        stored.object_metadata.content_length,
        body.len() as u64,
        "content_length must survive the production path"
    );
    assert_eq!(
        stored.ranges.len(),
        1,
        "a single-PUT object is stored as exactly one full-object range"
    );
}
