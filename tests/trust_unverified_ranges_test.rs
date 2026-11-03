//! `cache.trust_unverified_ranges` (release 3.0.1 R13.2, R13.6, R15.4). A range listing with
//! no file identity (every 2.x writer's, and what a 2.8 consolidator leaves) is refused by
//! default; with the override on it is served again, as 3.0.0 served it. The override never
//! switches off 3.0.0's stored-error-reply check, and a streamed hit from an over-long file
//! still never completes. The override is a process-global, so this file has its own target.
mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};

use common::expired_fixture::{signed_authorization_without_range, Fixture};
use common::{StubResponse, StubS3Client};
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::config::Config;

const MIB: u64 = 1024 * 1024;
const OBJECT_LEN: u64 = 16 * MIB;
const ETAG: &str = "\"d665d665d665d665d665d665d665d665\"";
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";
/// Below the over-long case's response, above the error-body case's.
const STREAM_AT: u64 = 65_536;
/// Every listing here starts above 0, so the offset-0 rule does not refuse it.
const OFF: u64 = 4 * MIB;
const TEST_BOUND: Duration = Duration::from_secs(90);
/// The body S3 returns to a ranged GET whose signature no longer matches (abbreviated).
const S3_ERROR_BODY: &str = r#"<?xml version="1.0" encoding="UTF-8"?> <Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does not match the signature you provided. Check your key and signing method.</Message><StringToSign>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</StringToSign></Error>"#;

fn object_byte(i: u64) -> u8 {
    (i % 251) as u8
}

fn object_bytes(start: u64, end_excl: u64) -> Vec<u8> {
    (start..end_excl).map(object_byte).collect()
}

/// The confounds off, as in `streamed_damage_never_completes_test`, with the override on.
fn config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = 1;
    config.cache.disk_streaming_threshold = STREAM_AT;
    config.cache.get_ttl = Duration::from_secs(3600);
    config.cache.trust_unverified_ranges = true;
    Arc::new(config)
}

/// Apply the override as `HttpProxy::new` does; the fixture serves `handle_request` directly.
fn apply_override(config: &Config) {
    s3_proxy::disk_cache::set_trust_unverified_ranges(config.cache.trust_unverified_ranges);
}

fn object_metadata() -> ObjectMetadata {
    ObjectMetadata {
        etag: ETAG.to_string(),
        content_length: OBJECT_LEN,
        last_modified: LAST_MODIFIED.to_string(),
        upload_state: UploadState::Complete,
        cumulative_size: OBJECT_LEN,
        ..Default::default()
    }
}

/// Write `data` at `start` through the incremental writer with compression off, consolidate,
/// then strip the listing's file identity, as a 2.x writer (or a 2.8 consolidator) leaves it.
async fn identity_less_listing(fixture: &Fixture, key: &str, start: u64, data: &[u8]) {
    let dcm = fixture.cache_manager.create_configured_disk_cache_manager();
    let mut writer = dcm
        .begin_incremental_range_write(key, start, start + data.len() as u64 - 1, false)
        .await
        .expect("begin incremental write")
        .expect("the write is admitted");
    for chunk in data.chunks(64 * 1024) {
        s3_proxy::disk_cache::DiskCacheManager::write_range_chunk(&mut writer, chunk)
            .expect("write chunk");
    }
    dcm.commit_incremental_range(writer, object_metadata(), Duration::from_secs(3600))
        .await
        .expect("commit");
    fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .consolidate_object(key)
        .await
        .expect("consolidation");
    let path = fixture.meta_path(key);
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("the .meta")).unwrap();
    let ranges = doc["ranges"].as_array_mut().expect("ranges");
    assert_eq!(ranges.len(), 1, "fixture: one listing");
    assert!(
        ranges[0].get("file_identity").is_some_and(|v| !v.is_null()),
        "fixture: the writer recorded an identity"
    );
    ranges[0].as_object_mut().unwrap().remove("file_identity");
    std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
    fixture.invalidate_metadata_cache(key).await;
    let meta = fixture.read_meta(key).expect("the key is listed");
    assert!(
        meta.ranges.len() == 1
            && meta.ranges[0].start == start
            && meta.ranges[0].file_identity.is_none(),
        "fixture: one identity-less listing at {}",
        start
    );
}

/// Write `data` at `start` through the incremental writer with compression off, leaving its
/// journal `Add` unconsolidated, and strip the `Add`'s file identity, as a 2.x writer leaves it.
async fn identity_less_journal_add(fixture: &Fixture, key: &str, start: u64, data: &[u8]) {
    let dcm = fixture.cache_manager.create_configured_disk_cache_manager();
    let mut writer = dcm
        .begin_incremental_range_write(key, start, start + data.len() as u64 - 1, false)
        .await
        .expect("begin incremental write")
        .expect("the write is admitted");
    for chunk in data.chunks(64 * 1024) {
        s3_proxy::disk_cache::DiskCacheManager::write_range_chunk(&mut writer, chunk)
            .expect("write chunk");
    }
    dcm.commit_incremental_range(writer, object_metadata(), Duration::from_secs(3600))
        .await
        .expect("commit");
    let journals = fixture
        .cache_manager
        .get_cache_dir()
        .join("metadata")
        .join("_journals");
    let mut stripped = 0;
    for dirent in std::fs::read_dir(&journals).expect("the journal directory") {
        let path = dirent.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("journal") {
            continue;
        }
        let mut out = String::new();
        for line in std::fs::read_to_string(&path).unwrap().lines() {
            let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
            if v["cache_key"] == key && v["operation"] == "Add" {
                assert!(
                    !v["range_spec"]["file_identity"].is_null(),
                    "fixture: the writer recorded an identity"
                );
                v["range_spec"]
                    .as_object_mut()
                    .unwrap()
                    .remove("file_identity");
                stripped += 1;
            }
            out.push_str(&v.to_string());
            out.push('\n');
        }
        std::fs::write(&path, out).unwrap();
    }
    assert_eq!(stripped, 1, "fixture: the fill's one Add lost its identity");
}

async fn consolidate(fixture: &Fixture, key: &str) {
    fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured")
        .consolidate_object(key)
        .await
        .expect("consolidation");
}

fn listed_bin(fixture: &Fixture, key: &str) -> std::path::PathBuf {
    let meta = fixture.read_meta(key).expect("the key is listed");
    fixture
        .cache_manager
        .get_cache_dir()
        .join("ranges")
        .join(&meta.ranges[0].file_path)
}

/// S3's answer to a forwarded `bytes=start-end`: the object's bytes.
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

#[derive(Debug)]
struct Served {
    status: StatusCode,
    body: Vec<u8>,
    /// The body ended with an error, or the request failed outright.
    broken: bool,
}

/// GET `key` with `range` through the loopback proxy, on a fresh connection.
async fn get(addr: SocketAddr, key: &str, start: u64, end: u64) -> Served {
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
    let resp = match tokio::time::timeout(Duration::from_secs(20), client.request(req))
        .await
        .expect("the request must not hang")
    {
        Ok(resp) => resp,
        Err(_) => {
            return Served {
                status: StatusCode::BAD_GATEWAY,
                body: Vec::new(),
                broken: true,
            }
        }
    };
    let status = resp.status();
    let mut body = resp.into_body();
    let mut bytes = Vec::new();
    let mut broken = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(20), body.frame()).await {
            Err(_) => panic!("the response body must not hang"),
            Ok(None) => break,
            Ok(Some(Err(_))) => {
                broken = true;
                break;
            }
            Ok(Some(Ok(frame))) => {
                if let Ok(data) = frame.into_data() {
                    bytes.extend_from_slice(&data);
                }
            }
        }
    }
    Served {
        status,
        body: bytes,
        broken,
    }
}

fn gets(stub: &StubS3Client) -> usize {
    stub.captured()
        .iter()
        .filter(|r| r.method == Method::GET)
        .count()
}

/// R13.6: with the override on, a 2.8-shaped listing (no file identity, the right bytes) is
/// served from the cache again, as 3.0.0 served it. The default refuses it (the lib cases in
/// `http_proxy.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identity_less_listing_is_served_with_the_override_on() {
    tokio::time::timeout(TEST_BOUND, async {
        let config = config();
        apply_override(&config);
        let key = "test-bucket/override-served.bin";
        let fixture = Fixture::new(config).await;
        identity_less_listing(&fixture, key, OFF, &object_bytes(OFF, OFF + MIB)).await;
        let (a, b) = (OFF + 100, OFF + 199);
        let stub = Arc::new(origin(a, b));
        let server = fixture.spawn_proxy(stub.clone()).await;
        let served = get(server.addr, key, a, b).await;
        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(served.body, object_bytes(a, b + 1));
        assert_eq!(
            gets(&stub),
            0,
            "R13.2: WITH THE OVERRIDE ON, AN IDENTITY-LESS LISTING WAS NOT SERVED FROM THE CACHE"
        );
    })
    .await
    .expect("the case must finish inside its bound");
}

/// With the override on, a journal `Add` with no file
/// identity is folded into a `.meta`, as 3.0.0 folded it. Red side: the fold's predicate
/// (`entry_lists_unverified_range`) without its override check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identity_less_journal_add_is_folded_with_the_override_on() {
    tokio::time::timeout(TEST_BOUND, async {
        let config = config();
        apply_override(&config);
        let key = "test-bucket/override-fold.bin";
        let fixture = Fixture::new(config).await;
        identity_less_journal_add(&fixture, key, OFF, &object_bytes(OFF, OFF + MIB)).await;
        consolidate(&fixture, key).await;
        fixture.invalidate_metadata_cache(key).await;
        let listed: Option<Vec<(u64, u64, bool)>> = fixture.read_meta(key).map(|m| {
            m.ranges
                .iter()
                .map(|r| (r.start, r.end, r.file_identity.is_some()))
                .collect()
        });
        assert_eq!(
            listed,
            Some(vec![(OFF, OFF + MIB - 1, false)]),
            "R13.2: WITH THE OVERRIDE ON, AN IDENTITY-LESS JOURNAL ADD WAS NOT FOLDED"
        );
    })
    .await
    .expect("the case must finish inside its bound");
}

/// With the override on, a streamed hit (a slice
/// above `disk_streaming_threshold`) of an identity-less listing is served from the cache. The
/// streaming serve re-reads the `.meta` for its extents (`reread_extent_spec`). Red side: that
/// re-read without the override check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identity_less_listing_is_streamed_with_the_override_on() {
    tokio::time::timeout(TEST_BOUND, async {
        let config = config();
        apply_override(&config);
        let key = "test-bucket/override-streamed.bin";
        let fixture = Fixture::new(config).await;
        identity_less_listing(&fixture, key, OFF, &object_bytes(OFF, OFF + MIB)).await;
        let (a, b) = (OFF + 1000, OFF + 1000 + 200_000 - 1);
        let stub = Arc::new(origin(a, b));
        let server = fixture.spawn_proxy(stub.clone()).await;
        let served = get(server.addr, key, a, b).await;
        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert!(!served.broken, "the streamed hit must complete");
        assert_eq!(served.body, object_bytes(a, b + 1));
        assert_eq!(
            gets(&stub),
            0,
            "R13.2: WITH THE OVERRIDE ON, A STREAMED HIT OF AN IDENTITY-LESS LISTING WAS NOT \
             SERVED FROM THE CACHE"
        );
    })
    .await
    .expect("the case must finish inside its bound");
}

/// With the override on, the disk check a RAM hit takes
/// (`get_metadata_etag_for_ram_verify`) counts an identity-less listing as servable. Red side:
/// that check without the override.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identity_less_listing_verifies_a_ram_copy_with_the_override_on() {
    tokio::time::timeout(TEST_BOUND, async {
        let config = config();
        apply_override(&config);
        let key = "test-bucket/override-ram-verify.bin";
        let fixture = Fixture::new(config).await;
        identity_less_listing(&fixture, key, OFF, &object_bytes(OFF, OFF + MIB)).await;
        let check = fixture
            .cache_manager
            .get_metadata_etag_for_ram_verify(key)
            .await;
        assert!(
            matches!(
                &check,
                s3_proxy::cache::RamVerifyDiskCheck::Listed { servable, .. }
                    if servable.contains(&(OFF, OFF + MIB - 1))
            ),
            "R13.2: WITH THE OVERRIDE ON, AN IDENTITY-LESS LISTING DOES NOT COVER A RAM COPY: {:?}",
            check
        );
    })
    .await
    .expect("the case must finish inside its bound");
}

/// The whole-object lookup (`CacheManager::get_cached_response`, its
/// `get_from_disk_cache` arm, the part lookup): with the override on,
/// an identity-less listing of the whole object is returned. Red side: that arm without the
/// override check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identity_less_whole_object_listing_is_returned_with_the_override_on() {
    tokio::time::timeout(TEST_BOUND, async {
        let config = config();
        apply_override(&config);
        let key = "test-bucket/override-whole-object.bin";
        let fixture = Fixture::new(config).await;
        identity_less_listing(&fixture, key, 0, &object_bytes(0, OBJECT_LEN)).await;
        let entry = fixture
            .cache_manager
            .get_cached_response(key)
            .await
            .expect("the lookup");
        assert!(
            entry.is_some(),
            "R13.2: WITH THE OVERRIDE ON, AN IDENTITY-LESS WHOLE-OBJECT LISTING WAS NOT RETURNED"
        );
    })
    .await
    .expect("the case must finish inside its bound");
}

/// R13.2: an identity-less listing whose bytes are an S3 XML error reply is refused with the
/// override on (3.0.0's stored-error-reply check), and the client gets the object's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_xml_error_body_under_an_identity_less_listing_is_refused_with_the_override_on() {
    tokio::time::timeout(TEST_BOUND, async {
        let config = config();
        apply_override(&config);
        let key = "test-bucket/override-error-body.bin";
        let fixture = Fixture::new(config).await;
        identity_less_listing(&fixture, key, OFF, S3_ERROR_BODY.as_bytes()).await;
        let (a, b) = (OFF + 10, OFF + 109);
        let stub = Arc::new(origin(a, b));
        let server = fixture.spawn_proxy(stub.clone()).await;
        let served = get(server.addr, key, a, b).await;
        assert!(
            !served.body.windows(7).any(|w| w == b"<Error>")
                && !served.body.windows(6).any(|w| w == b"<Code>"),
            "R13.2: THE STORED ERROR REPLY WAS SERVED WITH THE OVERRIDE ON ({} bytes, status {})",
            served.body.len(),
            served.status
        );
        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(served.body, object_bytes(a, b + 1));
        assert!(gets(&stub) >= 1, "the refused range is fetched from S3");
    })
    .await
    .expect("the case must finish inside its bound");
}

/// R15.4: with the override on, a streamed hit from a file that decodes to more bytes than its
/// identity-less listing names never completes: the client sees a short body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_over_long_file_under_an_identity_less_listing_never_completes() {
    tokio::time::timeout(TEST_BOUND, async {
        let config = config();
        apply_override(&config);
        let key = "test-bucket/override-over-long.bin";
        let fixture = Fixture::new(config).await;
        identity_less_listing(&fixture, key, OFF, &object_bytes(OFF, OFF + MIB)).await;
        let path = listed_bin(&fixture, key);
        let file = std::fs::read(&path).unwrap();
        let mut longer = file.clone();
        longer.extend_from_slice(&file);
        std::fs::write(&path, &longer).unwrap();
        let (a, b) = (OFF, OFF + MIB - 1);
        let stub = Arc::new(origin(a, b));
        let server = fixture.spawn_proxy(stub.clone()).await;
        let served = get(server.addr, key, a, b).await;
        let len = MIB as usize;
        assert_eq!(
            served.status,
            StatusCode::PARTIAL_CONTENT,
            "fixture: expected a 206 from the over-long file, got {}",
            served.status
        );
        assert_eq!(
            gets(&stub),
            0,
            "fixture: the over-long file was the one streamed"
        );
        assert!(
            served.broken || served.body.len() < len,
            "R15.4: A COMPLETE 206 FROM AN OVER-LONG FILE ({} of {} bytes, broken {})",
            served.body.len(),
            len,
            served.broken
        );
    })
    .await
    .expect("the case must finish inside its bound");
}
