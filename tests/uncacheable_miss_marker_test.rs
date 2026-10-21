//! A GET or HEAD miss that S3 answers with a status the proxy does not cache leaves nothing on
//! the shared volume.
//!
//! Made every GET/HEAD miss capture a fill witness before its S3 request, creating the
//! key's `{object}.wtok` marker (and its shard directories) when none existed. A miss answered
//! with 404 or 403 then left that file behind until the reaper's rotation reached it, so any
//! client could create one file per distinct key name. These tests drive the real
//! `HttpProxy::handle_request` against a stub S3 that answers every request with the given
//! status, then assert that no marker file and no shard directory for the bucket remain. The
//! bucket's `_origin.json` origin marker (`origin_identity`), written on a miss before the change as
//! well, is the one entry allowed under the bucket's metadata directory.
mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::expired_fixture::signed_authorization_without_range;
use common::expired_fixture::{
    proxy_get, proxy_get_with_auth, proxy_head, test_config_full_object_checks, Fixture,
};
use common::{StubResponse, StubS3Client};
use hyper::StatusCode;

/// Every file under `root` whose extension is `wtok`.
fn markers_under(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "wtok") {
                found.push(path);
            }
        }
    }
    found
}

/// Entries directly under the bucket's metadata directory other than its origin marker.
fn shard_entries(bucket_meta_dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(bucket_meta_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.file_name().is_none_or(|n| n != "_origin.json"))
        .collect()
}

/// Wait (bounded) until no marker remains anywhere in the cache and the bucket's metadata
/// directory holds nothing but its origin marker. Returns what was left at the deadline.
async fn left_on_disk(bucket_meta_dir: &Path, cache_dir: &Path) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let markers = markers_under(cache_dir);
        let shards = shard_entries(bucket_meta_dir);
        if (markers.is_empty() && shards.is_empty()) || Instant::now() >= deadline {
            return (markers, shards);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run(status: StatusCode, bucket: &str, shape: &str) {
    let fixture = Fixture::new(test_config_full_object_checks(1024)).await;
    let stub = StubS3Client::new().with_default(StubResponse::with_status(status));
    let server = fixture.spawn_proxy(stub.into_trait_object()).await;
    let path = format!("/{}/no-such-key", bucket);
    let (got, _, _) = match shape {
        "get" => {
            proxy_get_with_auth(
                server.addr,
                &path,
                &signed_authorization_without_range(),
                &[],
            )
            .await
        }
        "range" => proxy_get(server.addr, &path, &[("Range", "bytes=0-99")]).await,
        "head" => proxy_head(server.addr, &path, &[]).await,
        other => panic!("unknown shape {}", other),
    };
    assert_eq!(
        got, status,
        "fixture: the proxy passes S3's {} through",
        status
    );
    let cache_dir = fixture.cache_manager.get_cache_dir().to_path_buf();
    let bucket_meta_dir = cache_dir.join("metadata").join(bucket);
    let (markers, shards) = tokio::time::timeout(
        Duration::from_secs(10),
        left_on_disk(&bucket_meta_dir, &cache_dir),
    )
    .await
    .expect("the bounded wait must finish");
    assert!(
        markers.is_empty() && shards.is_empty(),
        "a {} miss answered {} left files on the shared volume: markers={:?}, \
         shard directories={:?}",
        shape,
        status,
        markers,
        shards
    );
}

#[tokio::test]
async fn a_get_miss_answered_404_leaves_no_marker_or_directory() {
    run(StatusCode::NOT_FOUND, "scn1-get-404", "get").await;
}

#[tokio::test]
async fn a_range_get_miss_answered_404_leaves_no_marker_or_directory() {
    run(StatusCode::NOT_FOUND, "scn1-range-404", "range").await;
}

#[tokio::test]
async fn a_head_miss_answered_404_leaves_no_marker_or_directory() {
    run(StatusCode::NOT_FOUND, "scn1-head-404", "head").await;
}

#[tokio::test]
async fn a_get_miss_answered_403_leaves_no_marker_or_directory() {
    run(StatusCode::FORBIDDEN, "scn1-get-403", "get").await;
}

/// Control: a miss whose response is cached keeps the marker its capture created, so the
/// assertions above cannot pass because no capture ran at all. The marker's release is
/// asynchronous and retried for about 0.6 s, so the control samples it repeatedly for 2 s:
/// a cached publisher that stopped consulting its witness would have the marker removed after
/// the response, and a single sample taken at once would miss that.
#[tokio::test]
async fn control_a_cached_get_miss_keeps_its_marker() {
    let fixture = Fixture::new(test_config_full_object_checks(1024)).await;
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_body(bytes::Bytes::from(vec![7u8; 1024]))
            .with_header("etag", "\"scn1-control\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream"),
    );
    let server = fixture.spawn_proxy(stub.into_trait_object()).await;
    let (got, _, body) = proxy_get_with_auth(
        server.addr,
        "/scn1-control/cached-key",
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    assert_eq!(got, StatusCode::OK);
    assert_eq!(body.len(), 1024);
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut samples = 0u32;
    let mut lost = None;
    while Instant::now() < deadline {
        let markers = markers_under(fixture.cache_manager.get_cache_dir());
        samples += 1;
        if markers.len() != 1 {
            lost = Some(markers);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        lost.is_none(),
        "control: a cached GET miss must keep the one marker its capture created for as long \
         as its release could run, found {:?} at sample {}",
        lost,
        samples
    );
}
