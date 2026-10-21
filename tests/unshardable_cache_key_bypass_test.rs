//! A request whose cache key has no `bucket/object` split must be forwarded
//! uncached, never written to a flat, non-sharded path.
//!
//! Fleet evidence (2026-09-27, one proxy): `[PATH_RESOLUTION] Failed to get sharded
//! path for range, using flat structure: cache_key=dvplain`. A path-style request to a
//! bucket root (`GET /dvplain` is ListObjects v1, `HEAD /dvplain` is HeadBucket,
//! `PUT /dvplain` is CreateBucket) yields a one-segment cache key. `parse_cache_key`
//! rejects it, and before this change `get_new_metadata_file_path` /
//! `get_new_range_file_path` fell back to `metadata/{key}.meta` and
//! `ranges/{key}_{s}-{e}.bin` at the tier roots, outside every shard-rooted walk.
//!
//! The test drives the real `HttpProxy::handle_request` and asserts on the cache
//! directory itself: no file anywhere in it may carry the unshardable key, and every
//! request must still reach S3 and be served.

mod common;

use std::path::Path;

use hyper::StatusCode;
use s3_proxy::cache::CacheManager;

use common::expired_fixture::{proxy_get, proxy_head, test_config, Fixture};
use common::{StubResponse, StubS3Client};

const BODY: &[u8] = b"<ListBucketResult><Name>dvplain</Name></ListBucketResult>";

/// Every file under `root` whose name contains `needle`, recursively.
fn files_named_like(root: &Path, needle: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p
                .file_name()
                .map(|n| n.to_string_lossy().contains(needle))
                .unwrap_or(false)
            {
                out.push(p);
            }
        }
    }
    out
}

#[tokio::test]
async fn bucket_root_get_and_head_are_served_and_leave_no_flat_cache_file() {
    let fixture = Fixture::new(test_config(BODY.len() as u64)).await;
    let cache_dir = fixture.cache_manager.get_cache_dir().to_path_buf();
    let stub = StubS3Client::new().with_default(StubResponse::ok(BODY.to_vec()));
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    for i in 1..=2 {
        let (status, _h, body) = proxy_get(server.addr, "/dvplain", &[]).await;
        assert_eq!(status, StatusCode::OK, "GET #{} not served", i);
        assert_eq!(body, BODY, "GET #{} body differs from S3's", i);
    }
    let (status, _h, _b) = proxy_head(server.addr, "/dvplain", &[]).await;
    assert_eq!(status, StatusCode::OK, "HEAD not served");

    // Give any spawned background cache write time to land before judging absence.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let leaked = files_named_like(&cache_dir, "dvplain");
    assert!(
        leaked.is_empty(),
        "a one-segment cache key was written to the cache directory, outside the \
         sharded tree: {:?}",
        leaked
    );
    let to_s3 = stub
        .captured()
        .iter()
        .filter(|r| r.uri.contains("dvplain"))
        .count();
    assert_eq!(
        to_s3, 3,
        "every bucket-root request must be forwarded to S3 (none served from a cache \
         entry that must not exist); forwarded={}",
        to_s3
    );
}

/// The predicate every handler guard uses, over each request shape that reaches it.
///
/// The PUT guard has no end-to-end red side in this harness: the signed forward dials
/// the real upstream rather than the stub, so a bucket-root PUT gets a 502 with or
/// without the guard and write-through (which caches only on S3 success) never runs.
#[test]
fn shardable_predicate_over_request_shapes() {
    let k = |path: &str, host: &str| CacheManager::generate_cache_key(path, Some(host));
    // Path-style bucket root: ListObjects v1 / HeadBucket / CreateBucket.
    assert!(!CacheManager::is_shardable_cache_key(&k(
        "/dvplain",
        "10.0.41.100:9000"
    )));
    assert!(!CacheManager::is_shardable_cache_key(&k(
        "/dvplain/",
        "10.0.41.100:9000"
    )));
    // Virtual-hosted bucket root: key "bucket/" with an empty object part.
    assert!(!CacheManager::is_shardable_cache_key(&k(
        "/",
        "dvplain.s3.us-west-2.amazonaws.com"
    )));
    // Object reads are unaffected.
    assert!(CacheManager::is_shardable_cache_key(&k(
        "/dvplain/a.txt",
        "10.0.41.100:9000"
    )));
    assert!(CacheManager::is_shardable_cache_key(&k(
        "/a.txt",
        "dvplain.s3.us-west-2.amazonaws.com"
    )));
}
