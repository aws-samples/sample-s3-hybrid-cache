//! A listing marked unservable over another version's file, and a newer journal `Add` for the
//! same extent with no file identity (a 2.x writer's), at the default
//! `cache.trust_unverified_ranges: false`. The read goes to S3 and serves this version's bytes.
//! The control for `trust_unverified_ranges_test`'s case with the override on.
mod common;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};

use common::expired_fixture::{
    signed_authorization_without_range, Fixture, SeedSpec, StoredExpiry,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::cache_types::{ObjectMetadata, RangeSpec, UploadState};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::Config;
use s3_proxy::journal_manager::{JournalEntry, JournalManager, JournalOperation};

const MIB: u64 = 1024 * 1024;
const OBJECT_LEN: u64 = 16 * MIB;
const ETAG: &str = "\"a11ca11ca11ca11ca11ca11ca11ca11c\"";
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";
const OFF: u64 = 4 * MIB;
const TEST_BOUND: Duration = Duration::from_secs(90);

fn object_bytes(start: u64, end_excl: u64) -> Vec<u8> {
    (start..end_excl).map(|i| (i % 251) as u8).collect()
}

fn config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.get_ttl = Duration::from_secs(3600);
    assert!(!config.cache.trust_unverified_ranges, "the default refuses");
    Arc::new(config)
}

/// Move the key's one listing to the sharded path production writers use, which is where the
/// consolidator unlinks an extent's file (it resolves the path from the key and the extent).
/// Returns the new path.
async fn list_at_sharded_path(fixture: &Fixture, key: &str) -> std::path::PathBuf {
    let ranges_dir = fixture.cache_manager.get_cache_dir().join("ranges");
    let path = fixture.meta_path(key);
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("the .meta")).unwrap();
    let listing = &doc["ranges"][0];
    let (start, end) = (
        listing["start"].as_u64().unwrap(),
        listing["end"].as_u64().unwrap(),
    );
    let from = ranges_dir.join(listing["file_path"].as_str().unwrap());
    let to =
        s3_proxy::disk_cache::get_sharded_path(&ranges_dir, key, &format!("_{start}-{end}.bin"))
            .expect("the sharded path");
    std::fs::create_dir_all(to.parent().unwrap()).unwrap();
    std::fs::rename(&from, &to).expect("fixture: move the .bin");
    doc["ranges"][0]["file_path"] = serde_json::Value::String(
        to.strip_prefix(&ranges_dir)
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    );
    std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
    fixture.invalidate_metadata_cache(key).await;
    to
}

/// One listing at `start`, its file holding `data`, with no file identity and marked unservable.
async fn marked_identity_less_listing(fixture: &Fixture, key: &str, start: u64, data: &[u8]) {
    let end = start + data.len() as u64 - 1;
    fixture.seed(
        key,
        &SeedSpec {
            extents: vec![(start, end)],
            etag: ETAG.to_string(),
            last_modified: LAST_MODIFIED.to_string(),
            content_length: OBJECT_LEN,
            stored_expiry: StoredExpiry::Fresh,
            created_age: Duration::ZERO,
            is_write_cached: Some(false),
        },
    );
    std::fs::write(fixture.bin_path(key, start, end), data).expect("fixture: write the .bin");
    list_at_sharded_path(fixture, key).await;
    let path = fixture.meta_path(key);
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("the .meta")).unwrap();
    let range = doc["ranges"][0].as_object_mut().expect("one listing");
    range.remove("file_identity");
    range.insert("unservable".to_string(), serde_json::Value::Bool(true));
    std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
    fixture.invalidate_metadata_cache(key).await;
    let meta = fixture.read_meta(key).expect("the key is listed");
    assert!(
        meta.ranges.len() == 1
            && meta.ranges[0].unservable
            && meta.ranges[0].file_identity.is_none(),
        "fixture: one marked identity-less listing"
    );
}

/// A journal `Add` for this version as a 2.x writer left it: no file identity, no witness.
async fn journal_a_2x_add(fixture: &Fixture, key: &str, start: u64, len: u64) {
    let end = start + len - 1;
    let ranges_dir = fixture.cache_manager.get_cache_dir().join("ranges");
    let bin =
        s3_proxy::disk_cache::get_sharded_path(&ranges_dir, key, &format!("_{start}-{end}.bin"))
            .expect("the sharded path");
    let relative = bin
        .strip_prefix(&ranges_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let now = std::time::SystemTime::now();
    let entry = JournalEntry {
        timestamp: now,
        instance_id: "peer-2x".to_string(),
        cache_key: key.to_string(),
        range_spec: RangeSpec {
            start,
            end,
            file_path: relative.clone(),
            compression_algorithm: CompressionAlgorithm::None,
            compressed_size: len,
            uncompressed_size: len,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            staged: None,
            unservable: false,
            file_identity: None,
        },
        operation: JournalOperation::Add,
        range_file_path: relative,
        metadata_version: 1,
        new_ttl_secs: None,
        object_ttl_secs: Some(3600),
        access_increment: None,
        object_metadata: Some(ObjectMetadata {
            etag: ETAG.to_string(),
            content_length: OBJECT_LEN,
            last_modified: LAST_MODIFIED.to_string(),
            upload_state: UploadState::Complete,
            cumulative_size: OBJECT_LEN,
            ..Default::default()
        }),
        fill_witness: None,
    };
    JournalManager::new(
        fixture.cache_manager.get_cache_dir().to_path_buf(),
        "peer-2x".to_string(),
    )
    .append_range_entry(key, entry)
    .await
    .expect("append the journal Add");
}

/// S3's answer to a forwarded `bytes=start-end`: this version's bytes.
fn origin(start: u64, end: u64) -> StubS3Client {
    StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(object_bytes(start, end + 1)))
            .with_header("etag", ETAG)
            .with_header("last-modified", LAST_MODIFIED)
            .with_header("content-length", (end - start + 1).to_string())
            .with_header(
                "content-range",
                format!("bytes {}-{}/{}", start, end, OBJECT_LEN),
            ),
    )
}

async fn get(addr: SocketAddr, key: &str, start: u64, end: u64) -> (StatusCode, Vec<u8>) {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("http://{}/{}", addr, key))
        .header("authorization", signed_authorization_without_range())
        .header("range", format!("bytes={}-{}", start, end))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(20), client.request(req))
        .await
        .expect("the request must not hang")
        .expect("the request");
    let status = resp.status();
    let body = tokio::time::timeout(Duration::from_secs(20), resp.into_body().collect())
        .await
        .expect("the response body must not hang")
        .expect("the response body")
        .to_bytes()
        .to_vec();
    (status, body)
}

/// At the default, the fold does not list the identity-less `Add` and the read refuses any
/// identity-less listing, so the marked extent's file is never served. Passes with or without
/// `resolve_conflicts`' unservable check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marked_listing_and_an_identity_less_add_read_from_s3_at_the_default() {
    tokio::time::timeout(TEST_BOUND, async {
        let fixture = Fixture::new(config()).await;
        let key = "test-bucket/default-marked-listing.bin";
        let other_version = vec![0xA5u8; MIB as usize];
        marked_identity_less_listing(&fixture, key, OFF, &other_version).await;
        journal_a_2x_add(&fixture, key, OFF, MIB).await;
        fixture
            .cache_manager
            .get_journal_consolidator()
            .await
            .expect("journal consolidator must be configured")
            .consolidate_object(key)
            .await
            .expect("consolidation");
        fixture.invalidate_metadata_cache(key).await;

        let (a, b) = (OFF + 100, OFF + 199);
        let stub = Arc::new(origin(a, b));
        let server = fixture.spawn_proxy(stub.clone()).await;
        let (status, body) = get(server.addr, key, a, b).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            body,
            object_bytes(a, b + 1),
            "at the default, the marked extent's file was served"
        );
    })
    .await
    .expect("the case must finish inside its bound");
}
