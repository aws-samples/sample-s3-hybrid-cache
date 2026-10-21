//! Every GET/HEAD-miss fetching shape captures its fill witness before its S3 request is sent
//!
//! A fill witness taken after the send could hold the token of a write acknowledged while the
//! request was in flight, and the fill would then publish the old version after it. On a cold
//! key the capture creates the key's `{object}.wtok` marker, so "captured before the send" is
//! observable from the S3 side: when the stub receives the request, the marker already exists.
//! Each test drives the real `HttpProxy::handle_request` for one shape against a stub that
//! records, at every upstream call, whether the key's marker was on disk.
mod common;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common::expired_fixture::{
    proxy_get, proxy_get_with_auth, proxy_head, signed_authorization_without_range,
    test_config_full_object_checks, Fixture,
};
use common::{StubResponse, StubS3Client};
use hyper::StatusCode;
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::{S3ClientApi, S3RequestContext, S3Response};

const SIZE: usize = 1024;

/// Delegates to a `StubS3Client`, recording at each `forward_request` whether `marker` exists.
#[derive(Clone)]
struct ObservingStub {
    inner: StubS3Client,
    marker: PathBuf,
    seen: Arc<Mutex<Vec<bool>>>,
}

#[async_trait::async_trait]
impl S3ClientApi for ObservingStub {
    async fn forward_request(&self, context: S3RequestContext) -> s3_proxy::Result<S3Response> {
        self.seen.lock().unwrap().push(self.marker.exists());
        self.inner.forward_request(context).await
    }
    fn extract_metadata_from_response(&self, headers: &HashMap<String, String>) -> CacheMetadata {
        self.inner.extract_metadata_from_response(headers)
    }
    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> ObjectMetadata {
        self.inner.extract_object_metadata_from_response(headers)
    }
    fn get_connection_pool(&self) -> Arc<tokio::sync::RwLock<ConnectionPoolManager>> {
        self.inner.get_connection_pool()
    }
    fn has_endpoint_overrides(&self) -> bool {
        false
    }
    async fn set_metrics_manager(
        &self,
        _metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
    }
    async fn register_endpoint(&self, _endpoint: &str) {}
    async fn refresh_dns(&self) -> s3_proxy::Result<()> {
        Ok(())
    }
}

fn ok_full() -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_body(Bytes::from(vec![3u8; SIZE]))
        .with_header("etag", "\"scn1-capture\"")
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        .with_header("content-type", "application/octet-stream")
}

fn partial(start: usize, end: usize) -> StubResponse {
    StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(vec![3u8; end - start + 1]))
        .with_header("etag", "\"scn1-capture\"")
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        .with_header("content-type", "application/octet-stream")
        .with_header("content-range", format!("bytes {}-{}/{}", start, end, SIZE))
}

/// Drive one request shape for `key` (`bucket/object`) and return the marker observations.
async fn observe(key: &str, response: StubResponse, shape: &str) -> Vec<bool> {
    let fixture = Fixture::new(test_config_full_object_checks(SIZE as u64)).await;
    let marker = fixture.meta_path(key).with_extension("wtok");
    assert!(!marker.exists(), "fixture: a cold key has no marker");
    let stub = ObservingStub {
        inner: StubS3Client::new().with_default(response),
        marker,
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let seen = Arc::clone(&stub.seen);
    let server = fixture.spawn_proxy(Arc::new(stub)).await;
    let path = format!("/{}", key);
    let (status, _, _) = match shape {
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
        "part" => {
            proxy_get_with_auth(
                server.addr,
                &format!("{}?partNumber=1", path),
                &signed_authorization_without_range(),
                &[],
            )
            .await
        }
        other => panic!("unknown shape {}", other),
    };
    assert!(
        status.is_success(),
        "fixture: {} answered {}",
        shape,
        status
    );
    let seen = seen.lock().unwrap().clone();
    assert!(!seen.is_empty(), "fixture: the {} miss reached S3", shape);
    seen
}

fn assert_captured_before_send(shape: &str, seen: &[bool]) {
    assert!(
        seen.iter().all(|&m| m),
        "a {} miss sent an S3 request before capturing its fill witness: marker present at \
         each upstream call = {:?}",
        shape,
        seen
    );
}

#[tokio::test]
async fn a_full_object_get_miss_captures_before_its_s3_request() {
    let seen = observe("scn1-capture/full", ok_full(), "get").await;
    assert_captured_before_send("full-object GET", &seen);
}

#[tokio::test]
async fn a_range_get_miss_captures_before_its_s3_request() {
    let seen = observe("scn1-capture/range", partial(0, 99), "range").await;
    assert_captured_before_send("range GET", &seen);
}

#[tokio::test]
async fn a_head_miss_captures_before_its_s3_request() {
    let seen = observe(
        "scn1-capture/head",
        StubResponse::with_status(StatusCode::OK)
            .with_header("etag", "\"scn1-capture\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-length", SIZE.to_string()),
        "head",
    )
    .await;
    assert_captured_before_send("HEAD", &seen);
}

#[tokio::test]
async fn a_part_number_get_miss_captures_before_its_s3_request() {
    let seen = observe(
        "scn1-capture/part",
        partial(0, SIZE - 1).with_header("x-amz-mp-parts-count", "1"),
        "part",
    )
    .await;
    assert_captured_before_send("?partNumber= GET", &seen);
}
