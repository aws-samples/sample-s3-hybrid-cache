//! A streamed cache hit never completes a response from a damaged range file (release 3.0.1
//! R15). The proxy's LZ4 frames carry a content checksum and no block checksums, so a bit
//! flip inside a stored block is caught only when the frame's end is read. Each case writes its
//! extents through the production incremental writer (store-mode frames of 1 MiB), flips one
//! bit inside a stored block of the frame under the response's last bytes, keeps the file's
//! length and modification time (so the listing's identity still matches and only the decode
//! can tell), and sends one GET through `HttpProxy::handle_request` on a loopback port. The
//! client must never receive a complete response: a short body or a reset is the only correct
//! outcome once the head is sent. Covered: every response that ends at an extent's end (the
//! exact arm, a chained extent taken to its end, and a middle chained extent). The comment at
//! the end of this file records the shapes that are not covered, and why.
mod common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};

use common::expired_fixture::{signed_authorization_without_range, Fixture};
use common::StubS3Client;
use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::config::Config;

const MIB: u64 = 1024 * 1024;
const OBJECT_LEN: u64 = 16 * MIB;
const ETAG: &str = "\"d663d663d663d663d663d663d663d663\"";
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";
/// Below every response here, so each takes a streamed serve.
const STREAM_AT: u64 = 65_536;
/// Every test body runs under this bound.
const TEST_BOUND: Duration = Duration::from_secs(90);

fn object_byte(i: u64) -> u8 {
    (i % 251) as u8
}

fn object_bytes(start: u64, end_excl: u64) -> Vec<u8> {
    (start..end_excl).map(object_byte).collect()
}

/// The confounds off, as in `request_level_traced_races_test`: no download coordination, no
/// RAM range tier (a RAM hit answers before the disk read), and no full-object shortcut.
fn config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = 1;
    config.cache.disk_streaming_threshold = STREAM_AT;
    config.cache.get_ttl = Duration::from_secs(3600);
    Arc::new(config)
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

/// Write `start..end_excl` through the incremental writer, 64 KiB at a time with compression
/// off, so the file is store-mode frames of `compression_batch_size` (1 MiB) each.
async fn fill(fixture: &Fixture, key: &str, start: u64, end_excl: u64) {
    let dcm = fixture.cache_manager.create_configured_disk_cache_manager();
    let mut writer = dcm
        .begin_incremental_range_write(key, start, end_excl - 1, false)
        .await
        .expect("begin incremental write")
        .expect("the write is admitted");
    let data = object_bytes(start, end_excl);
    for chunk in data.chunks(64 * 1024) {
        s3_proxy::disk_cache::DiskCacheManager::write_range_chunk(&mut writer, chunk)
            .expect("write chunk");
    }
    dcm.commit_incremental_range(writer, object_metadata(), Duration::from_secs(3600))
        .await
        .expect("commit");
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
    fixture.invalidate_metadata_cache(key).await;
}

fn listed_bin(fixture: &Fixture, key: &str, start: u64) -> PathBuf {
    let meta = fixture.read_meta(key).expect("the key is listed");
    let spec = meta
        .ranges
        .iter()
        .find(|r| r.start == start)
        .unwrap_or_else(|| panic!("fixture: no listed extent at {}", start));
    fixture
        .cache_manager
        .get_cache_dir()
        .join("ranges")
        .join(&spec.file_path)
}

/// Flip one bit of the decoded byte at `offset` (relative to the extent), which must lie in a
/// stored block. Keeps the file's length and modification time. Returns the frame index.
fn flip_in_stored_block(path: &Path, offset: u64) -> usize {
    let mtime = filetime::FileTime::from_last_modification_time(&std::fs::metadata(path).unwrap());
    let mut bytes = std::fs::read(path).unwrap();
    let mut pos = 0usize;
    let mut decoded = 0u64;
    let mut frame = 0usize;
    let mut hit = None;
    while pos < bytes.len() {
        let magic = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        assert_eq!(magic, 0x184D_2204, "fixture: LZ4 frame magic at {}", pos);
        let flg = bytes[pos + 4];
        let mut header = 7;
        if flg & 0x08 != 0 {
            header += 8;
        }
        if flg & 0x01 != 0 {
            header += 4;
        }
        pos += header;
        loop {
            let word = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
            pos += 4;
            if word == 0 {
                break;
            }
            let stored = word & 0x8000_0000 != 0;
            let len = (word & 0x7FFF_FFFF) as usize;
            assert!(
                stored,
                "fixture: compression is off, so every block is stored"
            );
            if hit.is_none() && offset >= decoded && offset < decoded + len as u64 {
                bytes[pos + (offset - decoded) as usize] ^= 0x01;
                hit = Some(frame);
            }
            pos += len;
            decoded += len as u64;
            if flg & 0x10 != 0 {
                pos += 4;
            }
        }
        if flg & 0x04 != 0 {
            pos += 4;
        }
        frame += 1;
    }
    let frame = hit.unwrap_or_else(|| panic!("fixture: offset {} is past the file", offset));
    std::fs::write(path, &bytes).unwrap();
    filetime::set_file_mtime(path, mtime).unwrap();
    frame
}

#[derive(Debug)]
struct Served {
    status: StatusCode,
    body: Vec<u8>,
    /// The body ended with an error, or the request failed outright.
    broken: bool,
    x_cache: Option<String>,
}

/// GET `key` with `range` through the loopback proxy, on a fresh connection.
async fn get(addr: SocketAddr, key: &str, range: &str) -> Served {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("http://{}/{}", addr, key))
        .header("authorization", signed_authorization_without_range())
        .header("range", range)
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
                x_cache: None,
            }
        }
    };
    let status = resp.status();
    let x_cache = resp
        .headers()
        .get("x-cache")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
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
        x_cache,
    }
}

/// Fill `extents` (object offsets, end exclusive), damage extent `damaged` at `offset` relative
/// to its start, and GET `start..=end`. The response must not be complete.
async fn case(
    what: &'static str,
    extents: &[(u64, u64)],
    damaged: usize,
    offset: u64,
    start: u64,
    end: u64,
) {
    tokio::time::timeout(TEST_BOUND, async {
        let key = format!("test-bucket/{}.bin", what.replace(' ', "-"));
        let fixture = Fixture::new(config()).await;
        for &(s, e) in extents {
            fill(&fixture, &key, s, e).await;
        }
        consolidate(&fixture, &key).await;
        let path = listed_bin(&fixture, &key, extents[damaged].0);
        let frame = flip_in_stored_block(&path, offset);
        let server = fixture.spawn_proxy(Arc::new(StubS3Client::new())).await;
        let served = get(server.addr, &key, &format!("bytes={}-{}", start, end)).await;
        let len = (end - start + 1) as usize;
        // The head must have been sent as a 206 from the damaged file: any other status (a 500,
        // a miss answered by the origin) would also "not complete" and make the cases vacuous.
        assert_eq!(
            served.status,
            StatusCode::PARTIAL_CONTENT,
            "fixture ({}): expected a 206 whose body fails, got {} (x-cache {:?})",
            what,
            served.status,
            served.x_cache
        );
        let complete = served.status == StatusCode::PARTIAL_CONTENT
            && !served.broken
            && served.body.len() == len;
        let wrong = complete && served.body != object_bytes(start, end + 1);
        assert!(
            !wrong,
            "WRONG BYTES SERVED ({}): a complete 206 of {} bytes from a file damaged in frame {} \
             of extent {}, x-cache {:?}",
            what, len, frame, damaged, served.x_cache
        );
        assert!(
            served.broken || served.body.len() < len,
            "fixture ({}): the 206 body neither broke nor fell short ({} of {} bytes)",
            what,
            served.body.len(),
            len
        );
        assert!(
            !complete,
            "fixture ({}): a complete 206 of the right bytes, so the damaged file was not the \
             one streamed; x-cache {:?}",
            what, served.x_cache
        );
        eprintln!(
            "{}: status {}, {} of {} bytes, broken {}, x-cache {:?}, damage in frame {}",
            what,
            served.status,
            served.body.len(),
            len,
            served.broken,
            served.x_cache,
            frame
        );
    })
    .await
    .expect("the case must finish inside its bound");
}

/// (i) The exact arm on an extent whose size is a multiple of the chunk: 1 MiB, damage in its
/// last block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_arm_at_a_multiple_of_the_chunk_never_completes() {
    case("exact multiple", &[(0, MIB)], 0, MIB - 1000, 0, MIB - 1).await;
}

/// (iii) A middle chained extent, taken to its end: three 4 MiB extents, a 12 MiB request,
/// damage in the middle extent's last frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chained_middle_extent_never_completes() {
    let extents = [(0, 4 * MIB), (4 * MIB, 8 * MIB), (8 * MIB, 12 * MIB)];
    case(
        "chained middle",
        &extents,
        1,
        4 * MIB - 1000,
        0,
        12 * MIB - 1,
    )
    .await;
}

/// (iv) A chained extent taken to its end: the last of three, a 10 MiB request ending where it
/// does, damage in its last frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chained_extent_taken_to_its_end_never_completes() {
    let extents = [(0, 4 * MIB), (4 * MIB, 8 * MIB), (8 * MIB, 12 * MIB)];
    case(
        "chained to end",
        &extents,
        2,
        4 * MIB - 1000,
        2 * MIB,
        12 * MIB - 1,
    )
    .await;
}

/// The cases' fills and requests with no damage complete with the right
/// bytes, so a case above breaks because of its damage, not because every such response does.
async fn control(what: &'static str, extents: &[(u64, u64)], start: u64, end: u64) {
    tokio::time::timeout(TEST_BOUND, async {
        let key = format!("test-bucket/{}.bin", what.replace(' ', "-"));
        let fixture = Fixture::new(config()).await;
        for &(s, e) in extents {
            fill(&fixture, &key, s, e).await;
        }
        consolidate(&fixture, &key).await;
        // control: no damage
        let server = fixture.spawn_proxy(Arc::new(StubS3Client::new())).await;
        let served = get(server.addr, &key, &format!("bytes={}-{}", start, end)).await;
        let len = (end - start + 1) as usize;
        eprintln!(
            "control {}: status {}, {} of {} bytes, broken {}, x-cache {:?}",
            what,
            served.status,
            served.body.len(),
            len,
            served.broken,
            served.x_cache
        );
        assert_eq!(
            served.status,
            StatusCode::PARTIAL_CONTENT,
            "control ({}): expected a 206 from the cache, got {} (x-cache {:?})",
            what,
            served.status,
            served.x_cache
        );
        assert!(
            !served.broken
                && served.body.len() == len
                && served.body == object_bytes(start, end + 1),
            "CONTROL ({}): AN UNDAMAGED STREAMED HIT DID NOT COMPLETE WITH THE RIGHT BYTES: {} of \
             {} bytes, broken {}, x-cache {:?}",
            what,
            served.body.len(),
            len,
            served.broken,
            served.x_cache
        );
    })
    .await
    .expect("the control must finish inside its bound");
}

/// Control for (i): the exact arm on an undamaged 1 MiB extent completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undamaged_exact_arm_completes() {
    control("control exact multiple", &[(0, MIB)], 0, MIB - 1).await;
}

/// Control for (iii): three undamaged 4 MiB extents, a 12 MiB request, complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undamaged_chained_middle_extent_completes() {
    let extents = [(0, 4 * MIB), (4 * MIB, 8 * MIB), (8 * MIB, 12 * MIB)];
    control("control chained middle", &extents, 0, 12 * MIB - 1).await;
}

/// Control for (iv): the 10 MiB request ending at the last undamaged extent's end completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undamaged_chained_extent_taken_to_its_end_completes() {
    let extents = [(0, 4 * MIB), (4 * MIB, 8 * MIB), (8 * MIB, 12 * MIB)];
    control("control chained to end", &extents, 2 * MIB, 12 * MIB - 1).await;
}

// Not detected, accepted (Ed, 2026-10-31): a response that ends inside an extent. A slice of
// one extent, and the chained arm's partly taken last extent, send their last byte before the
// frame under it has been read to its end, so checksum-only damage in that frame reaches the
// client in a complete response. Verifying before sending would mean decoding to the end of
// that frame first; Ed's position is that disk corruption is the operator's
// responsibility, and the proxy detects and corrects only where that is cheap. Before the
// fix both shapes, written as `case` calls here, served a complete 206 of damaged bytes:
// `case("small slice", &[(0, 12 * MIB)], 0, at + 5000, at, at + 16 * 1024 - 1)` with
// `at = 4 * MIB + 100 * 1024` (damage in frame 4), and
// `case("chained part", &[(0, 4 * MIB), (4 * MIB, 8 * MIB), (8 * MIB, 12 * MIB)], 2,
// MIB + MIB / 2, 0, 10 * MIB - 1)` (damage in frame 1 of extent 2). They are not tests, because
// they would fail by design.
