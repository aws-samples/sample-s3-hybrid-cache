//! A key filled by an `x-amz-te: append-md5` request (the AWS SDK for Java 2.x sync default)
//! reads back as the object's own bytes and size for every reader.
//!
//! S3 answers such a request with the 16-byte MD5 of the body appended to it and
//! `x-amz-transfer-encoding: append-md5`, and answers a ranged or `partNumber` request with a
//! `200` naming its slice in `x-amz-content-range` (measured against S3 in eu-central-1 on 2026-11-14). The mock
//! origin in `common/proxy_rig.rs` replays those shapes, and checksum mode's part checksums.
//! Releases before 3.0.7 stored the trailer as object bytes, a part answer as the whole
//! object, and a part's checksum as the object's.
//!
//! Every body is compared with the seeded object's SHA-256, never its length alone, and every
//! read that must be a cache hit is shown to be one by the origin's request count.
#[path = "common/proxy_rig.rs"]
mod proxy_rig;

use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Request, StatusCode};
use hyper_util::rt::TokioIo;
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;

use proxy_rig::{Rig, RigOptions, AUTH, STEP};
use s3_proxy::cache_types::{ObjectMetadata, RangeFileIdentity, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::hybrid_metadata_writer::{HybridMetadataWriter, WriteMode};
use s3_proxy::journal_manager::JournalManager;
use std::sync::Arc;

const TE: (&str, &str) = ("x-amz-te", "append-md5");
const CHECKSUM_MODE: (&str, &str) = ("x-amz-checksum-mode", "ENABLED");

struct Served {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Served {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

fn sha(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// One signed request through the proxy, bounded by [`STEP`], keeping the response headers.
async fn send(rig: &Rig, method: &str, path: &str, extra: &[(&str, &str)]) -> Served {
    let fut = async {
        let stream = TcpStream::connect(rig.proxy).await.expect("connect");
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("handshake");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("host", rig.mock_host.as_str())
            .header("content-length", "0")
            .header("authorization", AUTH);
        for (k, v) in extra {
            builder = builder.header(*k, *v);
        }
        let resp = sender
            .send_request(builder.body(Full::new(Bytes::new())).unwrap())
            .await
            .expect("proxy request");
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.into_body().collect().await.expect("body").to_bytes();
        Served {
            status,
            headers,
            body,
        }
    };
    tokio::time::timeout(STEP, fut)
        .await
        .unwrap_or_else(|_| panic!("{method} {path} timed out"))
}

/// The key's `.meta` as JSON, or `None` when it does not exist.
async fn meta_json(rig: &Rig, path: &str) -> Option<serde_json::Value> {
    let file = rig
        .disk
        .read()
        .await
        .get_new_metadata_file_path(&Rig::cache_key(path));
    let text = std::fs::read_to_string(file).ok()?;
    Some(serde_json::from_str(&text).expect("parse .meta"))
}

fn stored_headers(meta: &serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    meta["object_metadata"]["response_headers"]
        .as_object()
        .cloned()
        .unwrap_or_default()
}

fn has_stored(meta: &serde_json::Value, name: &str) -> bool {
    stored_headers(meta)
        .keys()
        .any(|k| k.eq_ignore_ascii_case(name))
}

/// Rewrite the key's `.meta` the way a release before 3.0.7 left it, with `extra` added to the
/// stored headers, and drop the proxy's RAM copy so the next read sees the file.
async fn plant_headers(rig: &Rig, path: &str, extra: &[(&str, &str)]) {
    let key = Rig::cache_key(path);
    let file = rig.disk.read().await.get_new_metadata_file_path(&key);
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).expect("read .meta")).unwrap();
    let headers = meta["object_metadata"]["response_headers"]
        .as_object_mut()
        .expect("response_headers");
    for (k, v) in extra {
        headers.insert(k.to_string(), serde_json::Value::String(v.to_string()));
    }
    std::fs::write(&file, serde_json::to_vec_pretty(&meta).unwrap()).expect("write .meta");
    rig.cache_manager.invalidate_metadata_cache(&key).await;
}

/// Set or remove the key's `checksums_object_scoped` marker (`None` removes it, as a `.meta`
/// written before 3.0.7 has none), and drop the proxy's RAM copy.
async fn set_checksum_marker(rig: &Rig, path: &str, marker: Option<bool>) {
    let key = Rig::cache_key(path);
    let file = rig.disk.read().await.get_new_metadata_file_path(&key);
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).expect("read .meta")).unwrap();
    let om = meta["object_metadata"]
        .as_object_mut()
        .expect("object_metadata");
    match marker {
        Some(v) => {
            om.insert(
                "checksums_object_scoped".to_string(),
                serde_json::Value::Bool(v),
            );
        }
        None => {
            om.remove("checksums_object_scoped");
        }
    }
    std::fs::write(&file, serde_json::to_vec_pretty(&meta).unwrap()).expect("write .meta");
    rig.cache_manager.invalidate_metadata_cache(&key).await;
}
/// Fill `path` with a whole-object `x-amz-te` GET and wait for the `.meta`.
async fn java_fill(rig: &Rig, path: &str, object: &Bytes) {
    let filled = send(rig, "GET", path, &[TE]).await;
    assert_eq!(filled.status, StatusCode::OK);
    // The client that asked gets S3's answer unchanged: the object, then its MD5.
    assert_eq!(filled.header("x-amz-transfer-encoding"), Some("append-md5"));
    assert_eq!(filled.body.len(), object.len() + 16);
    assert_eq!(sha(&filled.body[..object.len()]), sha(object));
    rig.wait_meta(path).await;
}

/// D19's red side: a non-Java whole-object, range and HEAD read of a key a Java 2.x sync client
/// filled returns the object's own bytes and size.
///
/// Every read is recorded as (request, status, `Content-Length`, `Content-Range`, body
/// SHA-256 matches the object's slice) and the list is compared once, so a failure prints all
/// of them.
#[tokio::test]
async fn reads_of_a_java_filled_key_return_the_objects_own_bytes_and_size() {
    for len in [1024usize, 3 * 1024 * 1024 + 7] {
        let rig = Rig::new(RigOptions::default()).await;
        let path = format!("/bucket/java-filled-{len}");
        let object = rig.mock.put(&path, len);
        java_fill(&rig, &path, &object).await;
        let n = len;
        let before = rig.mock.hits(&path);
        let whole = send(&rig, "GET", &path, &[]).await;
        assert_eq!(rig.mock.hits(&path), before, "the whole read is a hit");
        let mut reads = vec![(
            "whole".to_string(),
            whole.status.as_u16(),
            whole.header("content-length").map(str::to_string),
            whole.header("content-range").map(str::to_string),
            sha(&whole.body) == sha(&object),
        )];
        let mut expected = vec![("whole".to_string(), 200u16, Some(n.to_string()), None, true)];
        for (range, start, end) in [
            ("bytes=100-199", 100, 199),
            ("bytes=-100", n - 100, n - 1),
            ("bytes=0-", 0, n - 1),
            ("bytes=1000-", 1000, n - 1),
        ] {
            let read = send(&rig, "GET", &path, &[("range", range)]).await;
            reads.push((
                range.to_string(),
                read.status.as_u16(),
                read.header("content-length").map(str::to_string),
                read.header("content-range").map(str::to_string),
                sha(&read.body) == sha(&object[start..=end]),
            ));
            expected.push((
                range.to_string(),
                206,
                Some((end - start + 1).to_string()),
                Some(format!("bytes {}-{}/{}", start, end, n)),
                true,
            ));
        }
        let part = send(&rig, "GET", &format!("{path}?partNumber=1"), &[]).await;
        reads.push((
            "partNumber=1".to_string(),
            part.status.as_u16(),
            part.header("content-length").map(str::to_string),
            part.header("content-range").map(str::to_string),
            sha(&part.body) == sha(&object[..n / 2]),
        ));
        expected.push((
            "partNumber=1".to_string(),
            206,
            Some((n / 2).to_string()),
            Some(format!("bytes 0-{}/{}", n / 2 - 1, n)),
            true,
        ));
        let head = send(&rig, "HEAD", &path, &[]).await;
        reads.push((
            "HEAD".to_string(),
            head.status.as_u16(),
            head.header("content-length").map(str::to_string),
            head.header("content-range").map(str::to_string),
            head.header("x-amz-transfer-encoding").is_none(),
        ));
        expected.push(("HEAD".to_string(), 200, Some(n.to_string()), None, true));
        assert_eq!(
            reads, expected,
            "{len}-byte object: (request, status, Content-Length, Content-Range, bytes are the object's)"
        );
        let meta = meta_json(&rig, &path).await.expect(".meta");
        assert_eq!(
            meta["object_metadata"]["content_length"].as_u64(),
            Some(len as u64),
            "stored length, {len}-byte object"
        );
    }
}

/// A hit sent to an `x-amz-te` reader carries the object's bytes and no
/// `x-amz-transfer-encoding`, so the Java 2.x SDK neither strips nor checks a trailer
/// (`ChecksumsEnabledValidator.getObjectChecksumEnabledPerResponse`).
#[tokio::test]
async fn an_x_amz_te_reader_gets_the_object_and_no_trailer_header_from_a_hit() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/java-reader";
    let object = rig.mock.put(path, 4096);
    java_fill(&rig, path, &object).await;
    let before = rig.mock.hits(path);
    let whole = send(&rig, "GET", path, &[TE]).await;
    let range = send(&rig, "GET", path, &[TE, ("range", "bytes=0-99")]).await;
    assert_eq!(rig.mock.hits(path), before, "both reads are hits");
    assert_eq!(whole.status, StatusCode::OK);
    assert_eq!(whole.header("x-amz-transfer-encoding"), None);
    assert_eq!(sha(&whole.body), sha(&object));
    assert_eq!(range.status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(range.header("x-amz-transfer-encoding"), None);
    assert_eq!(range.header("content-range"), Some("bytes 0-99/4096"));
    assert_eq!(sha(&range.body), sha(&object[..100]));
}

/// A `partNumber` answer with a trailer is stored at its own offset under the object's total,
/// never as the whole object.
#[tokio::test]
async fn a_part_answer_with_a_trailer_is_stored_at_its_offset() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/java-part";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    let part = send(&rig, "GET", &format!("{path}?partNumber=2"), &[TE]).await;
    assert_eq!(part.status, StatusCode::OK);
    assert_eq!(
        part.header("x-amz-content-range"),
        Some("bytes 4096-8191/8192")
    );
    assert_eq!(sha(&part.body[..n / 2]), sha(&object[n / 2..]));
    rig.wait_listed(path, (n / 2) as u64, (n - 1) as u64).await;
    let meta = meta_json(&rig, path).await.expect(".meta");
    assert_eq!(
        meta["object_metadata"]["content_length"].as_u64(),
        Some(n as u64)
    );
    assert!(!has_stored(&meta, "x-amz-transfer-encoding"));
    assert!(!has_stored(&meta, "x-amz-content-range"));
    let whole = send(&rig, "GET", path, &[]).await;
    assert_eq!(whole.status, StatusCode::OK);
    assert_eq!(sha(&whole.body), sha(&object));
    let tail = send(&rig, "GET", path, &[("range", "bytes=-100")]).await;
    assert_eq!(sha(&tail.body), sha(&object[n - 100..]));
}

/// A trailer that is not the MD5 of the bytes before it drops the fill: the client gets S3's
/// response and nothing is cached. Both the streamed whole-object arm and the accumulating
/// part arm.
#[tokio::test]
async fn a_trailer_that_does_not_match_is_not_stored() {
    let rig = Rig::new(RigOptions::default()).await;
    rig.mock.set_corrupt_trailer(true);
    for (path, query) in [
        ("/bucket/bad-trailer-whole", ""),
        ("/bucket/bad-trailer-part", "?partNumber=1"),
    ] {
        let object = rig.mock.put(path, 4096);
        let served = send(&rig, "GET", &format!("{path}{query}"), &[TE]).await;
        assert_eq!(served.status, StatusCode::OK, "{path}");
        assert_eq!(served.header("x-amz-transfer-encoding"), Some("append-md5"));
        let slice = if query.is_empty() {
            &object[..]
        } else {
            &object[..2048]
        };
        assert_eq!(
            sha(&served.body[..slice.len()]),
            sha(slice),
            "{path}: S3's bytes forwarded"
        );
        // Five consolidation cycles: a fill that had been stored would be folded by now.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(meta_json(&rig, path).await.is_none(), "{path}: no .meta");
        let ranges = rig
            .disk
            .read()
            .await
            .find_cached_ranges(
                &Rig::cache_key(path),
                0,
                4095,
                None,
                s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
            )
            .await
            .map(|o| o.ranges.len())
            .unwrap_or(0);
        assert_eq!(ranges, 0, "{path}: no cached range");
    }
}

/// D: a `.meta` a release before 3.0.7 wrote from an append-md5 response is invalidated when a
/// GET touches it, and the GET refetches. The fixture is a 3.0.6-shaped document: a stored
/// entry whose headers carry `x-amz-transfer-encoding` or `x-amz-content-range`.
#[tokio::test]
async fn a_get_invalidates_an_entry_carrying_either_trailer_header() {
    let rig = Rig::new(RigOptions::default()).await;
    for (name, value) in [
        ("x-amz-transfer-encoding", "append-md5"),
        ("x-amz-content-range", "bytes 0-2047/4096"),
    ] {
        let path = format!("/bucket/planted-{name}");
        let object = rig.mock.put(&path, 4096);
        let fill = send(&rig, "GET", &path, &[]).await;
        assert_eq!(sha(&fill.body), sha(&object));
        rig.wait_meta(&path).await;
        plant_headers(&rig, &path, &[(name, value)]).await;
        let before = rig.mock.hits(&path);
        let read = send(&rig, "GET", &path, &[]).await;
        assert_eq!(
            rig.mock.hits(&path),
            before + 1,
            "{name}: the entry is not served; the GET refetches"
        );
        assert_eq!(read.header(name), None, "{name}: not replayed");
        assert_eq!(sha(&read.body), sha(&object));
        rig.wait_meta(&path).await;
        let meta = meta_json(&rig, &path).await.expect(".meta");
        assert!(
            !has_stored(&meta, name),
            "{name}: the refilled entry is clean"
        );
    }
}

/// A whole-object fill writes the `checksums_object_scoped` marker, and a marked entry of an
/// object of more than one part keeps a stored checksum without a `-N` suffix (a
/// `FULL_OBJECT` checksum): a GET hit replays it. The same entry without the marker, as an
/// earlier release wrote it, does not.
#[tokio::test]
async fn a_marked_entry_keeps_its_full_object_checksum() {
    let rig = Rig::new(RigOptions::default()).await;
    let n = 8192usize;
    let mut seen = Vec::new();
    for marker in [Some(true), None] {
        let path = format!("/bucket/full-object-checksum-{}", marker.is_some());
        let object = rig.mock.put(&path, n);
        let fill = send(&rig, "GET", &path, &[]).await;
        assert_eq!(sha(&fill.body), sha(&object));
        rig.wait_meta(&path).await;
        let written = meta_json(&rig, &path).await.expect(".meta")["object_metadata"]
            ["checksums_object_scoped"]
            .as_bool();
        plant_headers(
            &rig,
            &path,
            &[
                ("x-amz-checksum-crc64nvme", "AAAAAAAAAAA="),
                ("x-amz-checksum-type", "FULL_OBJECT"),
                ("x-amz-mp-parts-count", "2"),
            ],
        )
        .await;
        // Two parts, by ETag as S3 writes a multipart upload's.
        let key = Rig::cache_key(&path);
        let file = rig.disk.read().await.get_new_metadata_file_path(&key);
        let mut meta: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
        meta["object_metadata"]["parts_count"] = serde_json::json!(2);
        std::fs::write(&file, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
        set_checksum_marker(&rig, &path, marker).await;
        let before = rig.mock.hits(&path);
        let whole = send(&rig, "GET", &path, &[CHECKSUM_MODE]).await;
        seen.push((
            marker,
            written,
            rig.mock.hits(&path) - before,
            sha(&whole.body) == sha(&object),
            whole.header("x-amz-checksum-crc64nvme").map(str::to_string),
        ));
    }
    assert_eq!(
        seen,
        vec![
            (
                Some(true),
                Some(true),
                0,
                true,
                Some("AAAAAAAAAAA=".to_string())
            ),
            (None, Some(true), 0, true, None),
        ],
        "(marker set on the entry, marker the fill wrote, origin requests, the object's bytes, \
         checksum replayed)"
    );
}
/// The entry a 3.0.6 Java fill over a HEAD-first `.meta` left behind: `content_length` N, a
/// range file holding N + 16 bytes (the object, then S3's MD5 trailer), and no fingerprint
/// header. Above `disk_streaming_threshold` and with the RAM tier on, every read, from disk and
/// then from RAM, returns only the object's bytes: every serve slices to `content_length`.
/// Multi-threaded: the RAM tier's lookup uses `block_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unfingerprinted_trailer_entry_serves_only_content_length_bytes() {
    let rig = Rig::new(RigOptions {
        ram_cache: true,
        ..RigOptions::default()
    })
    .await;
    const N: usize = 2 * 1024 * 1024; // above the default 1 MiB disk_streaming_threshold
    let path = "/bucket/phase-f-entry";
    let stored = rig.mock.put(path, N + 16);
    let fill = send(&rig, "GET", path, &[]).await;
    assert_eq!(fill.body.len(), N + 16);
    rig.wait_meta(path).await;
    // S3's object is the first N bytes; the entry says N and its range file holds N + 16.
    let object = stored.slice(..N);
    rig.mock.put_bytes(path, object.clone());
    let key = Rig::cache_key(path);
    let file = rig.disk.read().await.get_new_metadata_file_path(&key);
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
    meta["object_metadata"]["content_length"] = serde_json::json!(N);
    if let Some(headers) = meta["object_metadata"]["response_headers"].as_object_mut() {
        for (k, v) in headers.iter_mut() {
            if k.eq_ignore_ascii_case("content-length") {
                *v = serde_json::Value::String(N.to_string());
            }
        }
    }
    assert_eq!(
        meta["ranges"][0]["end"].as_u64(),
        Some((N + 15) as u64),
        "PRECONDITION: the range file holds the trailer"
    );
    std::fs::write(&file, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
    rig.cache_manager.invalidate_metadata_cache(&key).await;
    let mut wrong = Vec::new();
    for round in ["disk", "ram"] {
        for (range, want) in [
            (None, object.clone()),
            (Some("bytes=-100".to_string()), object.slice(N - 100..)),
            (Some("bytes=0-".to_string()), object.clone()),
            (Some(format!("bytes={}-", N - 10)), object.slice(N - 10..)),
        ] {
            let extra: Vec<(&str, &str)> =
                range.as_deref().map(|r| ("range", r)).into_iter().collect();
            let served = send(&rig, "GET", path, &extra).await;
            if served.body != want {
                wrong.push((
                    round,
                    range.clone(),
                    served.status,
                    served.body.len(),
                    want.len(),
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "reads that did not return the object's bytes (round, range, status, got, want): {wrong:?}; origin requests {}",
        rig.mock.hits(path)
    );
}
/// A GET whose first read of the `.meta` fails and whose classifying re-read parses it judges
/// that document: a 3.0.6-shaped append-md5 entry is invalidated and refetched, not served.
/// The failed first read is the `fail_next_append_md5_probe_read_for_tests` seam.
#[tokio::test]
async fn a_get_whose_entry_reads_on_the_second_attempt_is_judged() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/second-attempt";
    let object = rig.mock.put(path, 4096);
    send(&rig, "GET", path, &[]).await;
    rig.wait_meta(path).await;
    plant_headers(&rig, path, &[("x-amz-transfer-encoding", "append-md5")]).await;
    let key = Rig::cache_key(path);
    // The probe the GET runs, with its first read failed: its verdict, read before any fill.
    s3_proxy::cache::fail_next_append_md5_probe_read_for_tests(&key);
    let verdict = rig.cache_manager.probe_entry_for_append_md5(&key).await;
    s3_proxy::cache::fail_next_append_md5_probe_read_for_tests(&key);
    let before = rig.mock.hits(path);
    let read = send(&rig, "GET", path, &[]).await;
    assert_eq!(
        (
            verdict,
            read.status,
            sha(&read.body) == sha(&object),
            rig.mock.hits(path) - before,
        ),
        (
            s3_proxy::cache::AppendMd5Probe::AppendMd5Entry,
            StatusCode::OK,
            true,
            1
        ),
        "(the probe's verdict after a failed first read, the GET's status, the object's \
         bytes, origin requests)"
    );
}

/// A GET whose `.meta` stays unreadable through the classifier's retries (an empty file) is
/// forwarded uncached, leaves the file for a later read, and counts a `metadata_unreadable`
/// cache bypass.
#[tokio::test]
async fn a_get_whose_entry_stays_unreadable_is_forwarded_uncached_and_counted() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/empty-meta";
    let object = rig.mock.put(path, 4096);
    send(&rig, "GET", path, &[]).await;
    rig.wait_meta(path).await;
    let key = Rig::cache_key(path);
    let file = rig.disk.read().await.get_new_metadata_file_path(&key);
    std::fs::write(&file, b"").expect("empty .meta");
    rig.cache_manager.invalidate_metadata_cache(&key).await;
    let before = rig.mock.hits(path);
    let bypasses = |rig: &Rig| {
        let metrics = rig.metrics.clone();
        async move {
            metrics
                .read()
                .await
                .get_cache_bypass_count("metadata_unreadable")
                .await
        }
    };
    let bypasses_before = bypasses(&rig).await;
    let read = send(&rig, "GET", path, &[]).await;
    // The counter is recorded on a spawned task.
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while bypasses(&rig).await == bypasses_before && tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        (
            read.status,
            sha(&read.body) == sha(&object),
            rig.mock.hits(path) - before,
            std::fs::read(&file).ok(),
            bypasses(&rig).await - bypasses_before,
        ),
        (StatusCode::OK, true, 1, Some(Vec::new()), 1),
        "(status, the object's bytes, origin requests, the empty .meta left as it was, \
         metadata_unreadable bypasses counted)"
    );
}
/// E, and D on a HEAD: a HEAD merge over a 3.0.6-shaped entry stops carrying the trailer
/// headers and clears the entry's ranges.
#[tokio::test]
async fn a_head_merge_drops_the_trailer_headers_and_the_entrys_ranges() {
    let rig = Rig::new(RigOptions::default()).await;
    for (name, value) in [
        ("x-amz-transfer-encoding", "append-md5"),
        ("x-amz-content-range", "bytes 0-2047/4096"),
    ] {
        let path = format!("/bucket/head-merge-{name}");
        rig.mock.put(&path, 4096);
        send(&rig, "GET", &path, &[]).await;
        rig.wait_meta(&path).await;
        plant_headers(&rig, &path, &[(name, value)]).await;
        let head = send(&rig, "HEAD", &path, &[]).await;
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.header(name), None, "{name}: not replayed on the HEAD");
        let meta = meta_json(&rig, &path).await.expect(".meta");
        assert!(
            !has_stored(&meta, name),
            "{name}: the HEAD merge dropped it"
        );
        assert_eq!(
            meta["ranges"].as_array().map(Vec::len),
            Some(0),
            "{name}: the HEAD merge cleared the entry's ranges"
        );
    }
}

/// F at the store: checksum-mode part answers do not leave a part's checksum as the object's,
/// so a whole-object hit assembled from the parts carries no part checksum.
#[tokio::test]
async fn part_checksums_are_not_stored_as_the_objects() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/part-checksums";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    for part in [1, 2] {
        let served = send(
            &rig,
            "GET",
            &format!("{path}?partNumber={part}"),
            &[CHECKSUM_MODE],
        )
        .await;
        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            served.header("x-amz-checksum-crc32"),
            Some(format!("part{part}-crc32==").as_str()),
            "the origin sends part {part}'s checksum"
        );
    }
    rig.wait_listed(path, 0, (n / 2 - 1) as u64).await;
    rig.wait_listed(path, (n / 2) as u64, (n - 1) as u64).await;
    let meta = meta_json(&rig, path).await.expect(".meta");
    assert!(
        stored_headers(&meta)
            .keys()
            .all(|k| !k.to_ascii_lowercase().starts_with("x-amz-checksum-")),
        "stored headers: {:?}",
        stored_headers(&meta)
    );
    let before = rig.mock.hits(path);
    let whole = send(&rig, "GET", path, &[CHECKSUM_MODE]).await;
    assert_eq!(rig.mock.hits(path), before, "the whole read is a hit");
    assert_eq!(sha(&whole.body), sha(&object));
    assert_eq!(whole.header("x-amz-checksum-crc32"), None);
}

/// F on touch: a part's checksum an earlier release stored as the object's (no `-N` suffix on
/// an object of more than one part) is not replayed by a GET or HEAD hit; the object's own
/// composite checksum is.
#[tokio::test]
async fn a_stored_part_checksum_is_dropped_on_touch() {
    let rig = Rig::new(RigOptions::default()).await;
    let n = 8192usize;
    for (value, kept) in [("part1-crc32==", false), ("whole-crc32==-2", true)] {
        let path = format!("/bucket/stale-checksum-{kept}");
        let object = rig.mock.put(&path, n);
        for part in [1, 2] {
            send(&rig, "GET", &format!("{path}?partNumber={part}"), &[]).await;
        }
        rig.wait_listed(&path, 0, (n / 2 - 1) as u64).await;
        rig.wait_listed(&path, (n / 2) as u64, (n - 1) as u64).await;
        // A HEAD from the origin first, so the HEAD below is answered by the HEAD tier.
        send(&rig, "HEAD", &path, &[]).await;
        plant_headers(
            &rig,
            &path,
            &[
                ("x-amz-checksum-crc32", value),
                ("x-amz-checksum-type", "COMPOSITE"),
            ],
        )
        .await;
        // An entry an earlier release wrote: no marker.
        set_checksum_marker(&rig, &path, None).await;
        let before = rig.mock.hits(&path);
        let whole = send(&rig, "GET", &path, &[]).await;
        assert_eq!(
            rig.mock.hits(&path),
            before,
            "{value}: the whole read is a hit"
        );
        assert_eq!(sha(&whole.body), sha(&object));
        let expected = kept.then_some(value);
        assert_eq!(
            whole.header("x-amz-checksum-crc32"),
            expected,
            "{value}: GET"
        );
        // The HEAD tier, and a whole-object `Range` read, which the range path's header
        // builder answers.
        let head = send(&rig, "HEAD", &path, &[]).await;
        let full = format!("bytes=0-{}", n - 1);
        let range = send(&rig, "GET", &path, &[("range", full.as_str())]).await;
        assert_eq!(
            (
                rig.mock.hits(&path) - before,
                head.status,
                head.header("x-amz-checksum-crc32"),
                range.status,
                range.header("x-amz-checksum-crc32"),
                sha(&range.body) == sha(&object),
            ),
            (
                0,
                StatusCode::OK,
                expected,
                StatusCode::PARTIAL_CONTENT,
                expected,
                true
            ),
            "{value}: (origin requests for the GET, HEAD and range reads, the HEAD hit's status \
             and checksum, the whole-object range hit's status and checksum, its bytes)"
        );
    }
}

/// The RAM tier: repeated whole reads of a Java-filled key, promoted to RAM after the first
/// disk hit, return the object's bytes with no trailer header, to an `x-amz-te` reader and to
/// a plain one. Multi-threaded: the RAM tier's lookup uses `block_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ram_tier_hits_serve_the_objects_bytes() {
    let rig = Rig::new(RigOptions {
        ram_cache: true,
        ..RigOptions::default()
    })
    .await;
    let path = "/bucket/ram-java";
    let object = rig.mock.put(path, 1024);
    java_fill(&rig, path, &object).await;
    let before = rig.mock.hits(path);
    // Every read is recorded before any is judged, so a failure shows which reads (the first
    // from disk, the rest from RAM) went wrong and how.
    let mut reads = Vec::new();
    for extra in [&[TE][..], &[TE], &[TE], &[TE], &[], &[]] {
        let read = send(&rig, "GET", path, extra).await;
        reads.push((
            if extra.is_empty() {
                "plain"
            } else {
                "x-amz-te"
            },
            read.status.as_u16(),
            read.body.len(),
            read.header("x-amz-transfer-encoding").map(str::to_string),
            sha(&read.body) == sha(&object),
        ));
    }
    let expected: Vec<_> = reads
        .iter()
        .map(|(who, ..)| (*who, 200u16, object.len(), None, true))
        .collect();
    assert_eq!(
        reads, expected,
        "(reader, status, bytes, x-amz-transfer-encoding, sha matches)"
    );
    assert_eq!(rig.mock.hits(path), before, "every read is a hit");
    // The hit rate is refreshed only on a RAM hit, so a positive rate shows the RAM tier
    // served at least one of the reads above.
    let stats = rig.cache_manager.get_statistics();
    assert!(
        stats.ram_cache_hit_rate > 0.0,
        "no read was a RAM hit (ram_cache_hit_rate {}, ram_cache_size {})",
        stats.ram_cache_hit_rate,
        stats.ram_cache_size
    );
}

/// The instance id of the older proxy the planting helpers stand for.
const OLDER_PEER: &str = "peer-before-3-0-7";

/// Plant what a release before 3.0.7 left after filling `?partNumber={part}` of `path` from an
/// append-md5 answer, as another instance sharing the volume: `body` (the part followed by its
/// MD5) stored as range `0..body.len()-1`, and that instance's journal `Add` for it, whose
/// `object_metadata` carries the trailer fingerprint and the body's length.
async fn plant_append_md5_part_add(rig: &Rig, path: &str, body: &[u8], part: (u64, u64, u64)) {
    let key = Rig::cache_key(path);
    let len = body.len() as u64;
    let bin = rig
        .disk
        .read()
        .await
        .get_new_range_file_path(&key, 0, len - 1);
    std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
    std::fs::write(&bin, body).unwrap();
    let identity = RangeFileIdentity::of(&std::fs::metadata(&bin).unwrap());
    assert!(
        identity.is_some(),
        "fixture: the planted range has a file identity"
    );
    let relative = bin
        .strip_prefix(rig.dir.path().join("ranges"))
        .unwrap()
        .to_string_lossy()
        .to_string();
    let spec = RangeSpec::new(0, len - 1, relative, CompressionAlgorithm::None, len, len)
        .with_file_identity(identity);
    let mut om = ObjectMetadata::new(
        proxy_rig::etag_of(path),
        "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
        len,
        Some("application/octet-stream".to_string()),
    );
    om.response_headers.insert(
        "x-amz-transfer-encoding".to_string(),
        "append-md5".to_string(),
    );
    om.response_headers.insert(
        "x-amz-content-range".to_string(),
        format!("bytes {}-{}/{}", part.0, part.1, part.2),
    );
    let dir = rig.dir.path().to_path_buf();
    let journal = Arc::new(JournalManager::new(dir.clone(), OLDER_PEER.to_string()));
    HybridMetadataWriter::new(journal)
        .write_range_metadata_witnessed(
            &key,
            spec,
            WriteMode::JournalOnly,
            Some(om),
            Duration::from_secs(3600),
            None,
        )
        .await
        .expect("the older instance journals its Add");
}

/// Wait until the rig's consolidator has processed every journal line of `path`'s key the older
/// instance wrote: its journal no longer names the key.
async fn wait_older_journal_drained(rig: &Rig, path: &str) {
    let key = Rig::cache_key(path);
    let file = rig
        .dir
        .path()
        .join("metadata")
        .join("_journals")
        .join(format!("{OLDER_PEER}.journal"));
    let until = tokio::time::Instant::now() + STEP;
    loop {
        let pending = std::fs::read_to_string(&file).is_ok_and(|t| t.contains(key.as_str()));
        if !pending {
            return;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "the older instance's journal still names {key} after {STEP:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Does the key's `.meta` list exactly `start..=end`?
async fn meta_lists(rig: &Rig, path: &str, start: u64, end: u64) -> bool {
    meta_json(rig, path).await.is_some_and(|m| {
        m["ranges"].as_array().is_some_and(|ranges| {
            ranges
                .iter()
                .any(|r| r["start"].as_u64() == Some(start) && r["end"].as_u64() == Some(end))
        })
    })
}

/// An older proxy's journal `Add` for a part it filled from an append-md5 answer is never listed
/// under a clean `.meta` of the same ETag. The consolidator retires it, and a ranged read inside
/// its extent goes to the origin and returns the object's bytes.
#[tokio::test]
async fn a_journalled_append_md5_part_fill_is_never_listed() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/consolidated-append-md5";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    // A clean `.meta` of the object's ETag.
    rig.seed_range(path, 6000, 6099).await;
    // Part 2, then 16 bytes standing for its MD5, at offset 0.
    let mut body = object[n / 2..].to_vec();
    body.extend_from_slice(&[0xEEu8; 16]);
    let end = body.len() as u64 - 1;
    plant_append_md5_part_add(&rig, path, &body, ((n / 2) as u64, n as u64 - 1, n as u64)).await;
    wait_older_journal_drained(&rig, path).await;
    let listed = meta_lists(&rig, path, 0, end).await;
    rig.cache_manager
        .invalidate_metadata_cache(&Rig::cache_key(path))
        .await;
    let before = rig.mock.hits(path);
    let read = send(&rig, "GET", path, &[("range", "bytes=0-99")]).await;
    assert_eq!(
        (
            listed,
            rig.mock.hits(path) - before,
            read.status.as_u16(),
            sha(&read.body) == sha(&object[..100]),
        ),
        (false, 1, 206, true),
        "(the .meta lists the planted 0-{end}, origin requests for bytes=0-99, status, \
         the bytes are the object's)"
    );
}

/// Rewrite the key's `.meta` the way a release before 3.0.7 left it after storing
/// `?partNumber={part_number}` from an append-md5 answer: `body` (the part followed by its MD5)
/// stored and listed as range `0..body.len()-1`, recorded as the part's layout entry, the layout
/// verified for the document's version (3.0.1 on) and, unless `marked`, not marked by 3.0.7.
async fn plant_part_layout(rig: &Rig, path: &str, body: &[u8], part_number: u32, marked: bool) {
    let key = Rig::cache_key(path);
    let len = body.len() as u64;
    let bin = rig
        .disk
        .read()
        .await
        .get_new_range_file_path(&key, 0, len - 1);
    std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
    std::fs::write(&bin, body).unwrap();
    let relative = bin
        .strip_prefix(rig.dir.path().join("ranges"))
        .unwrap()
        .to_string_lossy()
        .to_string();
    let spec = RangeSpec::new(0, len - 1, relative, CompressionAlgorithm::None, len, len)
        .with_file_identity(RangeFileIdentity::of(&std::fs::metadata(&bin).unwrap()));
    let file = rig.disk.read().await.get_new_metadata_file_path(&key);
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&file).expect("read .meta")).unwrap();
    meta["ranges"]
        .as_array_mut()
        .expect("ranges")
        .push(serde_json::to_value(&spec).unwrap());
    let om = meta["object_metadata"]
        .as_object_mut()
        .expect("object_metadata");
    om.insert(
        "part_ranges".to_string(),
        serde_json::json!({ part_number.to_string(): [0, len - 1] }),
    );
    om.insert("parts_count".to_string(), serde_json::json!(2));
    om.insert("part_layout_verified".to_string(), serde_json::json!(true));
    om.insert(
        "part_layout_marked".to_string(),
        if marked {
            serde_json::json!([part_number])
        } else {
            serde_json::json!([])
        },
    );
    std::fs::write(&file, serde_json::to_vec_pretty(&meta).unwrap()).expect("write .meta");
    rig.cache_manager.invalidate_metadata_cache(&key).await;
}

/// The part numbers the key's `.meta` records a layout entry for, and those it marks.
async fn part_layout(rig: &Rig, path: &str) -> (Vec<u64>, Vec<u64>) {
    let meta = meta_json(rig, path).await.expect(".meta");
    let mut parts: Vec<u64> = meta["object_metadata"]["part_ranges"]
        .as_object()
        .map(|m| m.keys().map(|k| k.parse().unwrap()).collect())
        .unwrap_or_default();
    parts.sort();
    let marked = meta["object_metadata"]["part_layout_marked"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
        .unwrap_or_default();
    (parts, marked)
}

/// A layout entry that puts part 2 at offset 0, the shape a release before 3.0.7 recorded for a
/// part it stored with its MD5 trailer, is refused even when marked: the lookup clears it and its
/// listing, and the part comes from the origin with the object's bytes.
#[tokio::test]
async fn a_later_part_at_offset_zero_is_never_served() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/part-two-at-zero";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    rig.seed_range(path, 6000, 6099).await;
    let mut body = object[n / 2..].to_vec();
    body.extend_from_slice(&[0xEEu8; 16]);
    let end = body.len() as u64 - 1;
    plant_part_layout(&rig, path, &body, 2, true).await;
    let before = rig.mock.hits(path);
    let part = send(&rig, "GET", &format!("{path}?partNumber=2"), &[]).await;
    assert_eq!(
        (
            rig.mock.hits(path) - before,
            part.status.as_u16(),
            part.body.len(),
            sha(&part.body) == sha(&object[n / 2..]),
            meta_lists(&rig, path, 0, end).await,
            meta_lists(&rig, path, 6000, 6099).await,
        ),
        (1, 206, n / 2, true, false, true),
        "(origin requests for partNumber=2, status, body length, the bytes are part 2's, \
         the .meta still lists the planted 0-{end}, the other listing kept)"
    );
}

/// A part 1 layout entry of `(0, P+15)` that 3.0.7 did not mark, with its listing holding part 1
/// and its MD5, is cleared with that listing on the first lookup, and part 1 comes from the
/// origin as its own P bytes.
#[tokio::test]
async fn an_unmarked_part_layout_is_cleared_with_its_listing() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/unmarked-part-one";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    rig.seed_range(path, 6000, 6099).await;
    let mut body = object[..n / 2].to_vec();
    body.extend_from_slice(&[0xEEu8; 16]);
    let end = body.len() as u64 - 1;
    plant_part_layout(&rig, path, &body, 1, false).await;
    let before = rig.mock.hits(path);
    let part = send(&rig, "GET", &format!("{path}?partNumber=1"), &[]).await;
    assert_eq!(
        (
            rig.mock.hits(path) - before,
            part.status.as_u16(),
            part.body.len(),
            sha(&part.body) == sha(&object[..n / 2]),
            meta_lists(&rig, path, 0, end).await,
            meta_lists(&rig, path, 6000, 6099).await,
        ),
        (1, 206, n / 2, true, false, true),
        "(origin requests for partNumber=1, status, body length, the bytes are part 1's, \
         the .meta still lists the planted 0-{end}, the other listing kept)"
    );
}

/// The control: a part 3.0.7 filled is recorded marked, and the next read of it is a hit.
#[tokio::test]
async fn a_marked_part_layout_is_kept() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/marked-part";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    // A part's layout is recorded into an existing `.meta` only.
    rig.seed_range(path, 100, 199).await;
    let first = send(&rig, "GET", &format!("{path}?partNumber=2"), &[]).await;
    assert_eq!(sha(&first.body), sha(&object[n / 2..]));
    rig.wait_listed(path, (n / 2) as u64, (n - 1) as u64).await;
    let until = tokio::time::Instant::now() + STEP;
    let mut layout = part_layout(&rig, path).await;
    while layout.0.is_empty() && tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(50)).await;
        layout = part_layout(&rig, path).await;
    }
    rig.cache_manager
        .invalidate_metadata_cache(&Rig::cache_key(path))
        .await;
    let before = rig.mock.hits(path);
    let again = send(&rig, "GET", &format!("{path}?partNumber=2"), &[]).await;
    assert_eq!(
        (
            layout,
            rig.mock.hits(path) - before,
            again.status.as_u16(),
            sha(&again.body) == sha(&object[n / 2..]),
        ),
        ((vec![2], vec![2]), 0, 206, true),
        "((layout entries, marked entries) after the fill, origin requests for the second \
         read, status, the bytes are part 2's)"
    );
}

/// A GET whose `.meta` exists and does not parse continues as a miss. The probe leaves the file
/// in place; the fill's publish classifies it under the slot lock, removes it
/// (`[METADATA_HEAL]`) and publishes the refill on the same request, and the next read is a hit.
/// The old entry's range file is aged past the orphan rotation's publish window, as it is for
/// any entry cached more than ten minutes earlier: a younger file at the fill's path is taken
/// for another fill's pending publish, and the publish declines until the rotation reclaims it.
#[tokio::test]
async fn a_get_heals_an_entry_that_does_not_parse() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/unreadable-meta";
    let object = rig.mock.put(path, 4096);
    let fill = send(&rig, "GET", path, &[]).await;
    assert_eq!(sha(&fill.body), sha(&object));
    rig.wait_meta(path).await;
    let key = Rig::cache_key(path);
    let bin = rig.disk.read().await.get_new_range_file_path(&key, 0, 4095);
    std::fs::File::options()
        .write(true)
        .open(&bin)
        .expect("open the cached range file")
        .set_modified(std::time::SystemTime::now() - Duration::from_secs(3600))
        .expect("age the cached range file");
    let file = rig.disk.read().await.get_new_metadata_file_path(&key);
    const UNREADABLE: &[u8] = b"{ not a cache entry";
    std::fs::write(&file, UNREADABLE).expect("write .meta");
    rig.cache_manager.invalidate_metadata_cache(&key).await;
    let verdict = rig.cache_manager.probe_entry_for_append_md5(&key).await;
    let left_by_probe = std::fs::read(&file).ok();
    let before = rig.mock.hits(path);
    let read = send(&rig, "GET", path, &[]).await;
    let refilled = || {
        std::fs::read_to_string(&file)
            .ok()
            .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
            .is_some_and(|m| {
                m["ranges"].as_array().is_some_and(|r| {
                    r.iter()
                        .any(|r| r["start"].as_u64() == Some(0) && r["end"].as_u64() == Some(4095))
                })
            })
    };
    let until = tokio::time::Instant::now() + STEP;
    while !refilled() && tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let published = refilled();
    rig.cache_manager.invalidate_metadata_cache(&key).await;
    let again = send(&rig, "GET", path, &[]).await;
    assert_eq!(
        (
            verdict,
            left_by_probe.as_deref() == Some(UNREADABLE),
            read.status,
            sha(&read.body) == sha(&object),
            published,
            again.status,
            sha(&again.body) == sha(&object),
            rig.mock.hits(path) - before,
        ),
        (
            s3_proxy::cache::AppendMd5Probe::Corrupt,
            true,
            StatusCode::OK,
            true,
            true,
            StatusCode::OK,
            true,
            1
        ),
        "(the probe's verdict, the unparseable .meta left by the probe, the GET's status, its \
         bytes, the refill published within {STEP:?}, the next read's status, its bytes, origin \
         requests for both reads)"
    );
}

/// With `read_cache_enabled` false for its key, a GET purges an append-md5 entry like any other
/// cached copy, and is forwarded uncached.
#[tokio::test]
async fn a_no_cache_read_purges_an_append_md5_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/nocache/append-md5";
    let object = rig.mock.put(path, 4096);
    send(&rig, "GET", path, &[]).await;
    rig.wait_meta(path).await;
    plant_headers(&rig, path, &[("x-amz-transfer-encoding", "append-md5")]).await;
    std::fs::write(
        rig.dir.path().join("cache_rules.json"),
        r#"{ "rules": [ { "pattern": "**/nocache/**", "read_cache_enabled": false } ] }"#,
    )
    .expect("write cache_rules.json");
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while rig
        .cache_manager
        .resolve_settings(path)
        .await
        .read_cache_enabled
    {
        assert!(
            tokio::time::Instant::now() < until,
            "apparatus: the rule disabling the read cache did not apply within 5 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = rig.mock.hits(path);
    let read = send(&rig, "GET", path, &[]).await;
    assert_eq!(
        (
            read.status,
            sha(&read.body) == sha(&object),
            rig.mock.hits(path) - before,
            meta_json(&rig, path).await.is_none(),
        ),
        (StatusCode::OK, true, 1, true),
        "(status, the object's bytes, origin requests, the append-md5 entry purged)"
    );
}

/// Before any part lookup clears it, the listing paired with an unmarked part 1 layout entry of
/// `(0, P+15)` is no coverage for a ranged read: reads inside it, the MD5's 16 bytes included,
/// come from the origin with the object's bytes.
#[tokio::test]
async fn an_unmarked_part_listing_is_no_coverage_for_a_ranged_read() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/unmarked-part-ranged";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    rig.seed_range(path, 6000, 6099).await;
    let mut body = object[..n / 2].to_vec();
    body.extend_from_slice(&[0xEEu8; 16]);
    plant_part_layout(&rig, path, &body, 1, false).await;
    let mut reads = Vec::new();
    for (range, start, end) in [("bytes=0-99", 0, 99), ("bytes=4090-4105", 4090, 4105)] {
        let before = rig.mock.hits(path);
        let read = send(&rig, "GET", path, &[("range", range)]).await;
        reads.push((
            range,
            rig.mock.hits(path) - before,
            read.status.as_u16(),
            sha(&read.body) == sha(&object[start..=end]),
        ));
    }
    assert_eq!(
        reads,
        vec![
            ("bytes=0-99", 1, 206, true),
            ("bytes=4090-4105", 1, 206, true)
        ],
        "(range, origin requests, status, the bytes are the object's)"
    );
}

/// The first ranged read of exactly the extent of a listing paired with an unmarked part layout
/// entry removes the listing with the entry, so the read's refill of that extent is listed and
/// the next read of it is a hit.
#[tokio::test]
async fn a_ranged_read_of_an_unmarked_part_listing_is_cached_again() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/unmarked-part-refill";
    let n = 8192usize;
    let object = rig.mock.put(path, n);
    rig.seed_range(path, 6000, 6099).await;
    let mut body = object[..n / 2].to_vec();
    body.extend_from_slice(&[0xEEu8; 16]);
    let end = body.len() - 1;
    plant_part_layout(&rig, path, &body, 1, false).await;
    let range = format!("bytes=0-{end}");
    let first = send(&rig, "GET", path, &[("range", range.as_str())]).await;
    rig.wait_listed(path, 0, end as u64).await;
    let before = rig.mock.hits(path);
    let second = send(&rig, "GET", path, &[("range", range.as_str())]).await;
    assert_eq!(
        (
            first.status.as_u16(),
            sha(&first.body) == sha(&object[..=end]),
            rig.mock.hits(path) - before,
            second.status.as_u16(),
            sha(&second.body) == sha(&object[..=end]),
            part_layout(&rig, path).await.0.is_empty(),
        ),
        (206, true, 0, 206, true, true),
        "(first status, its bytes are the object's, origin requests for the second read, its \
         status, its bytes, the unmarked layout entry gone)"
    );
}

/// With `read_cache_enabled` false and Mode B for its key, a GET carrying only `If-Match` purges
/// an append-md5 entry like any other cached copy, and is forwarded uncached.
#[tokio::test]
async fn a_no_cache_mode_b_if_match_read_purges_an_append_md5_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/nocache-modeb/append-md5";
    let object = rig.mock.put(path, 4096);
    send(&rig, "GET", path, &[]).await;
    rig.wait_meta(path).await;
    plant_headers(&rig, path, &[("x-amz-transfer-encoding", "append-md5")]).await;
    std::fs::write(
        rig.dir.path().join("cache_rules.json"),
        r#"{ "rules": [ { "pattern": "**/nocache-modeb/**", "read_cache_enabled": false, "evaluate_conditions_from_cache": true } ] }"#,
    )
    .expect("write cache_rules.json");
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resolved = rig.cache_manager.resolve_settings(path).await;
        if !resolved.read_cache_enabled && resolved.evaluate_conditions_from_cache {
            break;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "apparatus: the rule did not apply within 5 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let etag = proxy_rig::etag_of(path);
    let before = rig.mock.hits(path);
    let read = send(&rig, "GET", path, &[("if-match", etag.as_str())]).await;
    assert_eq!(
        (
            read.status,
            sha(&read.body) == sha(&object),
            rig.mock.hits(path) - before,
            meta_json(&rig, path).await.is_none(),
        ),
        (StatusCode::OK, true, 1, true),
        "(status, the object's bytes, origin requests, the append-md5 entry purged)"
    );
}
