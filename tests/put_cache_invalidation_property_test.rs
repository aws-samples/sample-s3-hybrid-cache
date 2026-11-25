//! What a write that S3 answers with a `5xx` does to the key's cached entry, on every write
//! path other than DELETE, through a real proxy in front of a mock origin.
//!
//! S3's error code decides, not the status. `500 InternalError` may have applied the write, so
//! the entry is kept and expired, with a fresh write-order marker, and the next GET or HEAD asks
//! S3 with the cached ETag: a `304` keeps the entry, a `200` replaces it and a `404` removes it.
//! `503 SlowDown` and a `5xx` with no readable code did not apply it, and the entry is left as it
//! was. The client gets S3's answer unchanged either way. The DELETE paths, which expire on any
//! S3 `5xx`, are in `signed_delete_cache_invalidation_test.rs`.
#[path = "common/proxy_rig.rs"]
mod proxy_rig;

use bytes::Bytes;
use hyper::StatusCode;
use proxy_rig::{Rig, RigOptions, AUTH, INTERNAL_ERROR, NO_CODE, SLOW_DOWN, STEP, UPLOAD_ID};

const LEN: usize = 4096;

/// One write: method, path and query, headers and body, for the object at `path`.
type Write = fn(&str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes);

fn signed_put(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "PUT",
        path.to_string(),
        vec![("authorization", AUTH.to_string())],
        Bytes::new(),
    )
}

fn unsigned_put(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    ("PUT", path.to_string(), vec![], Bytes::new())
}

fn sse_c_put(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "PUT",
        path.to_string(),
        vec![
            ("authorization", AUTH.to_string()),
            (
                "x-amz-server-side-encryption-customer-algorithm",
                "AES256".to_string(),
            ),
        ],
        Bytes::new(),
    )
}

fn copy_object(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "PUT",
        path.to_string(),
        vec![
            ("authorization", AUTH.to_string()),
            ("x-amz-copy-source", "/bucket/copy-source".to_string()),
        ],
        Bytes::new(),
    )
}

fn append_object(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "PUT",
        path.to_string(),
        vec![
            ("authorization", AUTH.to_string()),
            ("x-amz-write-offset-bytes", "0".to_string()),
        ],
        Bytes::new(),
    )
}

fn put_tagging(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "PUT",
        format!("{path}?tagging"),
        vec![("authorization", AUTH.to_string())],
        Bytes::new(),
    )
}

fn signed_complete(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "POST",
        format!("{path}?uploadId=upload-1"),
        vec![("authorization", AUTH.to_string())],
        Bytes::new(),
    )
}

fn unsigned_complete(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "POST",
        format!("{path}?uploadId=upload-1"),
        vec![],
        Bytes::new(),
    )
}

/// A browser form POST to `/bucket` whose `key` field names the object at `path`.
fn form_post(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    let key = path.strip_prefix("/bucket/").expect("a /bucket/ path");
    let boundary = "----w5xxboundary";
    let body = format!(
        "--{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\n{key}\r\n\
         --{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f.txt\"\r\n\
         Content-Type: text/plain\r\n\r\nnew bytes\r\n--{b}--\r\n",
        b = boundary,
    );
    (
        "POST",
        "/bucket".to_string(),
        vec![(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )],
        Bytes::from(body),
    )
}

/// Cache an object, have the origin answer `write` with `status` and `body`, and send it.
/// Asserts the client got the origin's answer unchanged. Returns the cached bytes and the
/// marker before the write.
async fn write_answered(
    rig: &Rig,
    path: &str,
    write: Write,
    status: u16,
    body: &str,
) -> (Bytes, Option<String>) {
    let data = rig.seed_object(path, LEN).await;
    let marker_before = rig.marker(path).await;
    rig.mock.set_write_reply(Some((status, body)));
    let (method, uri, headers, request_body) = write(path);
    let (got_status, got_body) = rig.send_body(method, &uri, &headers, request_body).await;
    rig.mock.set_write_reply(None);
    assert_eq!(
        (got_status.as_u16(), got_body.as_ref()),
        (status, body.as_bytes()),
        "{method} {uri}: the client gets the origin's answer unchanged"
    );
    (data, marker_before)
}

/// `500 InternalError`: the entry is kept, expired and stamped, and the next GET revalidates it
/// with the cached ETag; the origin's `304` keeps it and clears the expiry.
async fn assert_internal_error_expires(rig: &Rig, path: &str, write: Write) {
    let (data, marker_before) = write_answered(rig, path, write, 500, INTERNAL_ERROR).await;
    let entry = rig.entry(path).await;
    let marker_after = rig.marker(path).await;
    assert_eq!(
        (
            entry.as_ref().map(|m| m.write_expired_at.is_some()),
            entry.as_ref().map(|m| !m.ranges.is_empty()),
            marker_after.is_some() && marker_after != marker_before,
        ),
        (Some(true), Some(true), true),
        "{path}: after 500 InternalError (entry expired, its data kept, a fresh marker)"
    );
    let etag = entry.unwrap().object_metadata.etag;
    rig.mock.set_honour_if_none_match(true);
    let hits_before = rig.mock.hits(path);
    let (status, body) = rig.get(path, &[]).await;
    rig.mock.set_honour_if_none_match(false);
    let entry = rig.entry(path).await;
    assert_eq!(
        (
            status,
            body == data,
            rig.mock.hits(path) - hits_before,
            rig.mock.if_none_match(path).last().cloned().flatten(),
            entry.as_ref().map(|m| m.write_expired_at.is_none()),
            entry.as_ref().map(|m| !m.ranges.is_empty()),
        ),
        (StatusCode::OK, true, 1, Some(etag), Some(true), Some(true)),
        "{path}: the next GET (status, the cached bytes, origin requests, its If-None-Match, \
         the expiry cleared by the 304, the data kept)"
    );
}

/// A `5xx` that did not apply the write: the entry and the marker are left, and the next GET
/// is a hit.
async fn assert_left(rig: &Rig, path: &str, write: Write, status: u16, body: &str) {
    let (data, marker_before) = write_answered(rig, path, write, status, body).await;
    let entry = rig.entry(path).await;
    let hits_before = rig.mock.hits(path);
    let (got, got_body) = rig.get(path, &[]).await;
    assert_eq!(
        (
            entry.as_ref().map(|m| m.write_expired_at.is_none()),
            rig.marker(path).await == marker_before,
            got,
            got_body == data,
            rig.mock.hits(path) - hits_before,
        ),
        (Some(true), true, StatusCode::OK, true, 0),
        "{path}: after {status} {body:?} (entry not expired, marker unchanged, the next GET a hit)"
    );
}

/// Every case of the rule for one write path.
async fn assert_rule(rig: &Rig, name: &str, write: Write) {
    assert_internal_error_expires(rig, &format!("/bucket/{name}-internal"), write).await;
    assert_left(
        rig,
        &format!("/bucket/{name}-slowdown"),
        write,
        503,
        SLOW_DOWN,
    )
    .await;
    assert_left(rig, &format!("/bucket/{name}-nocode"), write, 500, NO_CODE).await;
}

#[tokio::test]
async fn a_write_through_put_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "signed-put", signed_put).await;
    assert_rule(&rig, "unsigned-put", unsigned_put).await;
}

#[tokio::test]
async fn an_uncached_put_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions {
        write_cache: false,
        ..RigOptions::default()
    })
    .await;
    assert_rule(&rig, "signed-uncached", signed_put).await;
    assert_rule(&rig, "unsigned-uncached", unsigned_put).await;
}

#[tokio::test]
async fn an_sse_c_put_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "sse-c", sse_c_put).await;
}

#[tokio::test]
async fn a_non_object_put_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "copy", copy_object).await;
    assert_rule(&rig, "append", append_object).await;
    assert_rule(&rig, "tagging", put_tagging).await;
}

#[tokio::test]
async fn a_complete_multipart_upload_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "signed-complete", signed_complete).await;
    assert_rule(&rig, "unsigned-complete", unsigned_complete).await;
}

#[tokio::test]
async fn a_form_post_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "form", form_post).await;
}

/// Expire `path`'s entry with a signed PUT answered `500 InternalError`. Returns its ETag.
async fn expired_by_a_put(rig: &Rig, path: &str) -> String {
    write_answered(rig, path, signed_put, 500, INTERNAL_ERROR).await;
    let entry = rig.entry(path).await.expect("the entry stays");
    assert!(entry.write_expired_at.is_some(), "{path}: expired");
    entry.object_metadata.etag
}

#[tokio::test]
async fn after_a_500_put_a_get_answered_200_replaces_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/get-200";
    let etag = expired_by_a_put(&rig, path).await;
    let new = rig.mock.put(path, LEN + 7);
    rig.mock.set_etag("\"version-2\"");
    rig.mock.set_honour_if_none_match(true);
    let (status, body) = rig.get(path, &[]).await;
    assert_eq!(
        (
            status,
            body == new,
            rig.mock.if_none_match(path).get(1).cloned().flatten()
        ),
        (StatusCode::OK, true, Some(etag)),
        "the GET after the expiry asks with the cached ETag and gets the new object"
    );
    rig.wait_entry(path, "the new version replaces the entry", |m| {
        m.is_some_and(|m| m.object_metadata.etag == "\"version-2\"" && m.write_expired_at.is_none())
    })
    .await;
}

#[tokio::test]
async fn after_a_500_put_a_get_answered_404_removes_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/get-404";
    let etag = expired_by_a_put(&rig, path).await;
    rig.mock.remove(path);
    let (status, _) = rig.get(path, &[]).await;
    assert_eq!(
        (
            status,
            rig.mock.if_none_match(path).first().cloned().flatten()
        ),
        (StatusCode::NOT_FOUND, None),
        "the seed GET carried no If-None-Match"
    );
    assert_eq!(
        rig.mock.if_none_match(path).get(1).cloned().flatten(),
        Some(etag),
        "the GET after the expiry asks with the cached ETag"
    );
    rig.wait_entry(path, "the 404 removes the entry's data", |m| {
        m.is_none_or(|m| m.ranges.is_empty())
    })
    .await;
}

#[tokio::test]
async fn after_a_500_put_a_head_answered_304_keeps_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/head-304";
    let etag = expired_by_a_put(&rig, path).await;
    rig.mock.set_honour_if_none_match(true);
    let hits_before = rig.mock.hits(path);
    let (status, _) = rig
        .send("HEAD", path, &[("authorization", AUTH.to_string())])
        .await;
    let entry = rig.entry(path).await;
    assert_eq!(
        (
            status,
            rig.mock.hits(path) - hits_before,
            rig.mock.if_none_match(path).last().cloned().flatten(),
            entry.as_ref().map(|m| m.write_expired_at.is_none()),
            entry.as_ref().map(|m| !m.ranges.is_empty()),
        ),
        (StatusCode::OK, 1, Some(etag), Some(true), Some(true)),
        "the HEAD after the expiry (status, origin requests, its If-None-Match, the expiry \
         cleared, the data kept)"
    );
}

#[tokio::test]
async fn after_a_500_put_a_head_answered_200_replaces_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/head-200";
    let etag = expired_by_a_put(&rig, path).await;
    rig.mock.put(path, LEN + 7);
    rig.mock.set_etag("\"version-2\"");
    rig.mock.set_honour_if_none_match(true);
    let (status, _) = rig
        .send("HEAD", path, &[("authorization", AUTH.to_string())])
        .await;
    assert_eq!(
        (
            status,
            rig.mock.if_none_match(path).get(1).cloned().flatten()
        ),
        (StatusCode::OK, Some(etag)),
        "the HEAD after the expiry asks with the cached ETag"
    );
    rig.wait_entry(path, "the new version replaces the entry", |m| {
        m.is_some_and(|m| {
            m.object_metadata.etag == "\"version-2\""
                && m.ranges.is_empty()
                && m.write_expired_at.is_none()
        })
    })
    .await;
}

#[tokio::test]
async fn after_a_500_put_a_head_answered_404_removes_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/head-404";
    let etag = expired_by_a_put(&rig, path).await;
    rig.mock.remove(path);
    let (status, _) = rig
        .send("HEAD", path, &[("authorization", AUTH.to_string())])
        .await;
    assert_eq!(
        (
            status,
            rig.mock.if_none_match(path).get(1).cloned().flatten()
        ),
        (StatusCode::NOT_FOUND, Some(etag)),
        "the HEAD after the expiry asks with the cached ETag and is answered 404"
    );
    rig.wait_entry(path, "the 404 removes the entry", |m| m.is_none())
        .await;
}

#[tokio::test]
async fn after_a_500_put_a_part_get_revalidates_the_part() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/part-revalidate";
    let data = rig.seed_object(path, LEN).await;
    let part_uri = format!("{path}?partNumber=1");
    let (status, part) = rig.get(&part_uri, &[]).await;
    assert_eq!(
        (status, part.as_ref()),
        (StatusCode::PARTIAL_CONTENT, &data[..LEN / 2])
    );
    rig.wait_entry(path, "part 1's layout is recorded", |m| {
        m.is_some_and(|m| {
            m.object_metadata.part_ranges.contains_key(&1)
                && m.object_metadata.part_layout_marked.contains(&1)
        })
    })
    .await;
    let hits_before = rig.mock.hits(path);
    let (status, _) = rig.get(&part_uri, &[]).await;
    assert_eq!(
        (status, rig.mock.hits(path) - hits_before),
        (StatusCode::PARTIAL_CONTENT, 0),
        "control: before the expiry the part is a hit"
    );
    let etag = expired_by_a_put(&rig, path).await;
    rig.mock.set_honour_if_none_match(true);
    let hits_before = rig.mock.hits(path);
    let (status, part) = rig.get(&part_uri, &[]).await;
    let entry = rig.entry(path).await;
    assert_eq!(
        (
            status,
            part.as_ref() == &data[..LEN / 2],
            rig.mock.hits(path) - hits_before,
            rig.mock.if_none_match(path).last().cloned().flatten(),
            entry.as_ref().map(|m| m.write_expired_at.is_none()),
        ),
        (StatusCode::PARTIAL_CONTENT, true, 1, Some(etag), Some(true)),
        "the part GET after the expiry (status, part 1's bytes, origin requests, its \
         If-None-Match, the expiry cleared by the 304)"
    );
}

#[tokio::test]
async fn after_a_500_put_a_head_with_range_goes_to_s3() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/head-range";
    rig.seed_object(path, LEN).await;
    let head_range = || async {
        rig.send(
            "HEAD",
            path,
            &[
                ("authorization", AUTH.to_string()),
                ("range", "bytes=0-9".to_string()),
            ],
        )
        .await
    };
    let hits_before = rig.mock.hits(path);
    head_range().await;
    assert_eq!(
        rig.mock.hits(path) - hits_before,
        0,
        "control: before the expiry a HEAD with Range is answered from the cache"
    );
    expired_by_a_put(&rig, path).await;
    let hits_before = rig.mock.hits(path);
    head_range().await;
    assert_eq!(
        rig.mock.hits(path) - hits_before,
        1,
        "after the expiry a HEAD with Range is forwarded to S3"
    );
}

/// A signed CompleteMultipartUpload of a staged upload that S3 answers `500 InternalError`
/// expires the object it replaces and does not finalize: the staged part stays for the client's
/// retry, the entry keeps the old object's ETag and bytes, and the next GET revalidates it.
#[tokio::test]
async fn a_complete_multipart_upload_answered_500_keeps_its_staged_parts() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/bucket/complete-500-staged";
    let data = rig.seed_object(path, LEN).await;
    let old_etag = rig.entry(path).await.expect("seeded").object_metadata.etag;
    let auth = vec![("authorization", AUTH.to_string())];
    rig.mock.set_stage_uploads(true);
    let (status, body) = rig.send("POST", &format!("{path}?uploads"), &auth).await;
    assert_eq!(status, StatusCode::OK, "CreateMultipartUpload: {body:?}");
    let (status, _) = rig
        .send_body(
            "PUT",
            &format!("{path}?partNumber=1&uploadId={UPLOAD_ID}"),
            &auth,
            proxy_rig::object_bytes(LEN + 1),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "UploadPart");
    let dir = s3_proxy::signed_put_handler::mpu_upload_dir(rig.dir.path(), UPLOAD_ID);
    let staged = || dir.join("part1.json").exists() && dir.join("part1.bin").exists();
    let until = tokio::time::Instant::now() + STEP;
    while !staged() {
        assert!(
            tokio::time::Instant::now() < until,
            "fixture: part 1 staged under {dir:?} within {STEP:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let marker_before = rig.marker(path).await;
    rig.mock.set_write_reply(Some((500, INTERNAL_ERROR)));
    let complete = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
                    <ETag>\"part-etag-1\"</ETag></Part></CompleteMultipartUpload>";
    let (status, body) = rig
        .send_body(
            "POST",
            &format!("{path}?uploadId={UPLOAD_ID}"),
            &auth,
            Bytes::from_static(complete.as_bytes()),
        )
        .await;
    rig.mock.set_write_reply(None);
    assert_eq!(
        (status.as_u16(), body.as_ref()),
        (500, INTERNAL_ERROR.as_bytes()),
        "the client gets the origin's answer unchanged"
    );
    // A finalization or a declined completion runs on a detached task the response waits for
    // at most a few seconds; give one the time to act before reading what it would remove.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let entry = rig.entry(path).await;
    let marker_after = rig.marker(path).await;
    assert_eq!(
        (
            staged(),
            entry.as_ref().map(|m| m.write_expired_at.is_some()),
            entry.as_ref().map(|m| m.object_metadata.etag == old_etag),
            entry.as_ref().map(|m| !m.ranges.is_empty()),
            marker_after.is_some() && marker_after != marker_before,
        ),
        (true, Some(true), Some(true), Some(true), true),
        "after 500 InternalError (part 1 still staged, the entry expired, its ETag the old \
         object's, its data kept, a fresh marker)"
    );
    rig.mock.set_honour_if_none_match(true);
    let hits_before = rig.mock.hits(path);
    let (status, got) = rig.get(path, &[]).await;
    assert_eq!(
        (
            status,
            got == data,
            rig.mock.hits(path) - hits_before,
            rig.mock.if_none_match(path).last().cloned().flatten(),
        ),
        (StatusCode::OK, true, 1, Some(old_etag)),
        "the next GET (status, the old object's bytes, origin requests, its If-None-Match)"
    );
}
