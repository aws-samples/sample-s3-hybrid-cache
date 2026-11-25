//! What a DELETE does to the key's cached entry, through a real proxy in front of a mock origin,
//! on the signed DeleteObject, the unsigned (presigned or anonymous) DeleteObject and
//! DeleteObjects.
//!
//! A `2xx` invalidates. Any `5xx` S3 answers a DELETE, whatever its error code, may have deleted
//! the object, so the entry is kept and expired, with a fresh write-order marker, and the next
//! GET asks S3 with the cached ETag: a `304` keeps the entry. A `4xx` leaves it. The client gets
//! S3's answer unchanged. A `5xx` the proxy builds itself, for a DELETE it never sent, leaves the
//! entry (`write_unknown_outcome_test.rs`, `a_presigned_delete_refused_on_every_attempt_keeps_the_cached_copy`,
//! and the unit tests of `signed_request_proxy::write_effect`). A DeleteObjects key S3 reports
//! under `<Error>` in a `2xx` is expired when the code is `InternalError` and left otherwise.
//!
//! After each kind of expiry, a GET and a HEAD revalidate with the cached ETag (a `304` keeps the
//! entry, a `200` replaces it, a `404` removes it), and so does a ranged GET (a `304` keeps the
//! entry, a `206` of a new version replaces it).
#[path = "common/proxy_rig.rs"]
mod proxy_rig;

use bytes::Bytes;
use hyper::StatusCode;
use proxy_rig::{Rig, RigOptions, AUTH, INTERNAL_ERROR, NO_CODE, SLOW_DOWN};

type Entry = s3_proxy::cache_types::NewCacheMetadata;

const LEN: usize = 4096;

const ACCESS_DENIED: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";

/// One DELETE of the object at `path`: method, path and query, headers and body.
type Delete = fn(&str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes);

fn signed_delete(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "DELETE",
        path.to_string(),
        vec![("authorization", AUTH.to_string())],
        Bytes::new(),
    )
}

fn unsigned_delete(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    ("DELETE", path.to_string(), vec![], Bytes::new())
}

/// A DeleteObjects request body naming `keys`.
fn delete_objects_body(keys: &[&str]) -> Bytes {
    let objects: String = keys
        .iter()
        .map(|key| format!("<Object><Key>{key}</Key></Object>"))
        .collect();
    Bytes::from(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Delete>{objects}</Delete>"
    ))
}

/// A signed DeleteObjects naming the object at `path`.
fn delete_objects(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    let key = path.strip_prefix("/bucket/").expect("a /bucket/ path");
    (
        "POST",
        "/bucket?delete".to_string(),
        vec![("authorization", AUTH.to_string())],
        delete_objects_body(&[key]),
    )
}

/// Cache an object, have the origin answer `delete` with `status` and `body`, and send it.
/// Asserts the client got the origin's answer unchanged. Returns the cached bytes and the
/// marker before the DELETE.
async fn delete_answered(
    rig: &Rig,
    path: &str,
    delete: Delete,
    status: u16,
    body: &str,
) -> (Bytes, Option<String>) {
    let data = rig.seed_object(path, LEN).await;
    let marker_before = rig.marker(path).await;
    rig.mock.set_write_reply(Some((status, body)));
    let (method, uri, headers, request_body) = delete(path);
    let (got_status, got_body) = rig.send_body(method, &uri, &headers, request_body).await;
    rig.mock.set_write_reply(None);
    assert_eq!(
        (got_status.as_u16(), got_body.as_ref()),
        (status, body.as_bytes()),
        "{method} {uri}: the client gets the origin's answer unchanged"
    );
    (data, marker_before)
}

/// An S3 `5xx`: the entry is kept, expired and stamped, and the next GET revalidates it with
/// the cached ETag; the origin's `304` keeps it and clears the expiry.
async fn assert_expired(rig: &Rig, path: &str, delete: Delete, status: u16, body: &str) {
    let (data, marker_before) = delete_answered(rig, path, delete, status, body).await;
    let entry = rig.entry(path).await;
    let marker_after = rig.marker(path).await;
    assert_eq!(
        (
            entry.as_ref().map(|m| m.write_expired_at.is_some()),
            entry.as_ref().map(|m| !m.ranges.is_empty()),
            marker_after.is_some() && marker_after != marker_before,
        ),
        (Some(true), Some(true), true),
        "{path}: after {status} {body:?} (entry expired, its data kept, a fresh marker)"
    );
    let etag = entry.unwrap().object_metadata.etag;
    rig.mock.set_honour_if_none_match(true);
    let hits_before = rig.mock.hits(path);
    let (got, got_body) = rig.get(path, &[]).await;
    rig.mock.set_honour_if_none_match(false);
    let entry = rig.entry(path).await;
    assert_eq!(
        (
            got,
            got_body == data,
            rig.mock.hits(path) - hits_before,
            rig.mock.if_none_match(path).last().cloned().flatten(),
            entry.as_ref().map(|m| m.write_expired_at.is_none()),
        ),
        (StatusCode::OK, true, 1, Some(etag), Some(true)),
        "{path}: the next GET (status, the cached bytes, origin requests, its If-None-Match, \
         the expiry cleared by the 304)"
    );
}

/// Every case of the rule for one DELETE path.
async fn assert_rule(rig: &Rig, name: &str, delete: Delete) {
    assert_expired(
        rig,
        &format!("/bucket/{name}-internal"),
        delete,
        500,
        INTERNAL_ERROR,
    )
    .await;
    assert_expired(
        rig,
        &format!("/bucket/{name}-slowdown"),
        delete,
        503,
        SLOW_DOWN,
    )
    .await;
    assert_expired(rig, &format!("/bucket/{name}-nocode"), delete, 500, NO_CODE).await;

    // A 4xx: S3 refused it, and the entry is left.
    let path = format!("/bucket/{name}-denied");
    let (data, marker_before) = delete_answered(rig, &path, delete, 403, ACCESS_DENIED).await;
    let entry = rig.entry(&path).await;
    let hits_before = rig.mock.hits(&path);
    let (got, got_body) = rig.get(&path, &[]).await;
    assert_eq!(
        (
            entry.as_ref().map(|m| m.write_expired_at.is_none()),
            rig.marker(&path).await == marker_before,
            got,
            got_body == data,
            rig.mock.hits(&path) - hits_before,
        ),
        (Some(true), true, StatusCode::OK, true, 0),
        "{path}: after 403 (entry not expired, marker unchanged, the next GET a hit)"
    );

    // A 2xx deletes the object: the entry is invalidated.
    let path = format!("/bucket/{name}-deleted");
    let reply = if name == "delete-objects" {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult><Deleted><Key>{}</Key></Deleted></DeleteResult>",
            path.strip_prefix("/bucket/").unwrap()
        )
    } else {
        String::new()
    };
    let status = if name == "delete-objects" { 200 } else { 204 };
    delete_answered(rig, &path, delete, status, &reply).await;
    assert!(
        rig.entry(&path).await.is_none(),
        "{path}: a {status} invalidates the entry"
    );
}

#[tokio::test]
async fn a_signed_delete_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "signed-delete", signed_delete).await;
}

#[tokio::test]
async fn an_unsigned_delete_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "unsigned-delete", unsigned_delete).await;
}

#[tokio::test]
async fn a_delete_objects_follows_the_5xx_rule() {
    let rig = Rig::new(RigOptions::default()).await;
    assert_rule(&rig, "delete-objects", delete_objects).await;
}

/// A DeleteObjects answered `200` whose result reports one key under `<Error>` with
/// `InternalError`, one with `AccessDenied` and one `<Deleted>`. S3 may have deleted the first, so
/// its entry is expired and stamped, as a single DELETE answered `500 InternalError` is, and the
/// next GET revalidates it. The second is left and the third invalidated.
#[tokio::test]
async fn a_delete_objects_key_reported_internal_error_is_expired() {
    let rig = Rig::new(RigOptions::default()).await;
    let keys = ["per-key-internal", "per-key-denied", "per-key-deleted"];
    let paths: Vec<String> = keys.iter().map(|k| format!("/bucket/{k}")).collect();
    let mut data = Vec::new();
    let mut markers = Vec::new();
    for path in &paths {
        data.push(rig.seed_object(path, LEN).await);
        markers.push(rig.marker(path).await);
    }
    let reply = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult>\
        <Error><Key>per-key-internal</Key><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message></Error>\
        <Error><Key>per-key-denied</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>\
        <Deleted><Key>per-key-deleted</Key></Deleted></DeleteResult>";
    rig.mock.set_write_reply(Some((200, reply)));
    let (status, got) = rig
        .send_body(
            "POST",
            "/bucket?delete",
            &[("authorization", AUTH.to_string())],
            delete_objects_body(&keys),
        )
        .await;
    rig.mock.set_write_reply(None);
    assert_eq!(
        (status.as_u16(), got.as_ref()),
        (200, reply.as_bytes()),
        "the client gets the origin's answer unchanged"
    );
    let internal = rig.entry(&paths[0]).await;
    let denied = rig.entry(&paths[1]).await;
    let internal_marker = rig.marker(&paths[0]).await;
    assert_eq!(
        (
            internal.as_ref().map(|m| m.write_expired_at.is_some()),
            internal.as_ref().map(|m| !m.ranges.is_empty()),
            internal_marker.is_some() && internal_marker != markers[0],
            denied.as_ref().map(|m| m.write_expired_at.is_none()),
            rig.marker(&paths[1]).await == markers[1],
            rig.entry(&paths[2]).await.is_none(),
        ),
        (Some(true), Some(true), true, Some(true), true, true),
        "(InternalError: expired, its data kept, a fresh marker; AccessDenied: not expired, \
         marker unchanged; Deleted: invalidated)"
    );
    let etag = internal.unwrap().object_metadata.etag;
    rig.mock.set_honour_if_none_match(true);
    let hits_before = [rig.mock.hits(&paths[0]), rig.mock.hits(&paths[1])];
    let (internal_status, internal_body) = rig.get(&paths[0], &[]).await;
    let (denied_status, denied_body) = rig.get(&paths[1], &[]).await;
    rig.mock.set_honour_if_none_match(false);
    assert_eq!(
        (
            internal_status,
            internal_body == data[0],
            rig.mock.hits(&paths[0]) - hits_before[0],
            rig.mock.if_none_match(&paths[0]).last().cloned().flatten(),
            rig.entry(&paths[0])
                .await
                .map(|m| m.write_expired_at.is_none()),
            denied_status,
            denied_body == data[1],
            rig.mock.hits(&paths[1]) - hits_before[1],
        ),
        (
            StatusCode::OK,
            true,
            1,
            Some(etag),
            Some(true),
            StatusCode::OK,
            true,
            0
        ),
        "the next GETs (InternalError: status, the cached bytes, origin requests, its \
         If-None-Match, the expiry cleared by the 304; AccessDenied: status, the cached bytes, \
         origin requests)"
    );
}

/// A write S3 answered with a `5xx` that may have applied it, which expires the entry.
#[derive(Clone, Copy, Debug)]
enum Expiry {
    /// A signed DELETE answered `503 SlowDown`.
    SignedDelete,
    /// An unsigned DELETE answered `500 InternalError`.
    UnsignedDelete,
    /// A DeleteObjects answered `500 InternalError`.
    DeleteObjects,
    /// A DeleteObjects answered `200` with `InternalError` for the key.
    DeleteObjectsKey,
    /// A signed PUT answered `500 InternalError`.
    SignedPut,
}

const DELETES: [Expiry; 4] = [
    Expiry::SignedDelete,
    Expiry::UnsignedDelete,
    Expiry::DeleteObjects,
    Expiry::DeleteObjectsKey,
];

fn signed_put(path: &str) -> (&'static str, String, Vec<(&'static str, String)>, Bytes) {
    (
        "PUT",
        path.to_string(),
        vec![("authorization", AUTH.to_string())],
        Bytes::new(),
    )
}

/// Cache `path` and expire its entry by `how`. Returns the cached bytes and the ETag.
async fn expired_by(rig: &Rig, path: &str, how: Expiry) -> (Bytes, String) {
    let key = path.strip_prefix("/bucket/").expect("a /bucket/ path");
    let per_key = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult><Error><Key>{key}</Key>\
         <Code>InternalError</Code><Message>We encountered an internal error. Please try \
         again.</Message></Error></DeleteResult>"
    );
    let (write, status, body): (Delete, u16, &str) = match how {
        Expiry::SignedDelete => (signed_delete, 503, SLOW_DOWN),
        Expiry::UnsignedDelete => (unsigned_delete, 500, INTERNAL_ERROR),
        Expiry::DeleteObjects => (delete_objects, 500, INTERNAL_ERROR),
        Expiry::DeleteObjectsKey => (delete_objects, 200, per_key.as_str()),
        Expiry::SignedPut => (signed_put, 500, INTERNAL_ERROR),
    };
    let (data, _) = delete_answered(rig, path, write, status, body).await;
    let entry = rig.entry(path).await.expect("the entry stays");
    assert!(
        entry.write_expired_at.is_some(),
        "fixture: {path} expired by {how:?}"
    );
    (data, entry.object_metadata.etag)
}

/// The path a test of `how` uses. Its first GET or HEAD is the seed's, so the request after
/// the expiry is the second (`if_none_match(path).get(1)`).
fn path_for(test: &str, how: Expiry) -> String {
    format!("/bucket/{test}-{how:?}")
}

fn signed() -> Vec<(&'static str, String)> {
    vec![("authorization", AUTH.to_string())]
}

#[tokio::test]
async fn after_a_delete_expiry_a_get_answered_304_keeps_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    for how in DELETES {
        let path = path_for("get-304", how);
        let (data, etag) = expired_by(&rig, &path, how).await;
        rig.mock.set_honour_if_none_match(true);
        let hits_before = rig.mock.hits(&path);
        let (status, body) = rig.get(&path, &[]).await;
        rig.mock.set_honour_if_none_match(false);
        let entry = rig.entry(&path).await;
        assert_eq!(
            (
                status,
                body == data,
                rig.mock.hits(&path) - hits_before,
                rig.mock.if_none_match(&path).get(1).cloned().flatten(),
                entry.as_ref().map(|m| m.write_expired_at.is_none()),
                entry.as_ref().map(|m| !m.ranges.is_empty()),
            ),
            (StatusCode::OK, true, 1, Some(etag), Some(true), Some(true)),
            "{how:?}: the next GET (status, the cached bytes, origin requests, its \
             If-None-Match, the expiry cleared, the data kept)"
        );
    }
}

#[tokio::test]
async fn after_a_delete_expiry_a_get_answered_200_replaces_the_entry() {
    for how in DELETES {
        let rig = Rig::new(RigOptions::default()).await;
        let path = path_for("get-200", how);
        let (_, etag) = expired_by(&rig, &path, how).await;
        let new = rig.mock.put(&path, LEN + 7);
        rig.mock.set_etag("\"version-2\"");
        rig.mock.set_honour_if_none_match(true);
        let (status, body) = rig.get(&path, &[]).await;
        assert_eq!(
            (
                status,
                body == new,
                rig.mock.if_none_match(&path).get(1).cloned().flatten()
            ),
            (StatusCode::OK, true, Some(etag)),
            "{how:?}: the GET after the expiry asks with the cached ETag and gets the new object"
        );
        rig.wait_entry(
            &path,
            "the new version replaces the entry",
            |m: Option<&Entry>| {
                m.is_some_and(|m| {
                    m.object_metadata.etag == "\"version-2\"" && m.write_expired_at.is_none()
                })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn after_a_delete_expiry_a_get_answered_404_removes_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    for how in DELETES {
        let path = path_for("get-404", how);
        let (_, etag) = expired_by(&rig, &path, how).await;
        rig.mock.remove(&path);
        let hits_before = rig.mock.hits(&path);
        let (status, _) = rig.get(&path, &[]).await;
        assert_eq!(
            (
                status,
                rig.mock.hits(&path) - hits_before,
                rig.mock.if_none_match(&path).get(1).cloned().flatten()
            ),
            (StatusCode::NOT_FOUND, 1, Some(etag)),
            "{how:?}: the GET after the expiry asks once, with the cached ETag, and is answered \
             404 (status, origin requests, its If-None-Match)"
        );
        rig.wait_entry(
            &path,
            "the 404 removes the entry's data",
            |m: Option<&Entry>| m.is_none_or(|m| m.ranges.is_empty()),
        )
        .await;
    }
}

#[tokio::test]
async fn after_a_delete_expiry_a_head_answered_304_keeps_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    for how in DELETES {
        let path = path_for("head-304", how);
        let (_, etag) = expired_by(&rig, &path, how).await;
        rig.mock.set_honour_if_none_match(true);
        let hits_before = rig.mock.hits(&path);
        let (status, _) = rig.send("HEAD", &path, &signed()).await;
        rig.mock.set_honour_if_none_match(false);
        let entry = rig.entry(&path).await;
        assert_eq!(
            (
                status,
                rig.mock.hits(&path) - hits_before,
                rig.mock.if_none_match(&path).get(1).cloned().flatten(),
                entry.as_ref().map(|m| m.write_expired_at.is_none()),
                entry.as_ref().map(|m| !m.ranges.is_empty()),
            ),
            (StatusCode::OK, 1, Some(etag), Some(true), Some(true)),
            "{how:?}: the HEAD after the expiry (status, origin requests, its If-None-Match, the \
             expiry cleared, the data kept)"
        );
    }
}

#[tokio::test]
async fn after_a_delete_expiry_a_head_answered_200_replaces_the_entry() {
    for how in DELETES {
        let rig = Rig::new(RigOptions::default()).await;
        let path = path_for("head-200", how);
        let (_, etag) = expired_by(&rig, &path, how).await;
        rig.mock.put(&path, LEN + 7);
        rig.mock.set_etag("\"version-2\"");
        rig.mock.set_honour_if_none_match(true);
        let (status, _) = rig.send("HEAD", &path, &signed()).await;
        assert_eq!(
            (
                status,
                rig.mock.if_none_match(&path).get(1).cloned().flatten()
            ),
            (StatusCode::OK, Some(etag)),
            "{how:?}: the HEAD after the expiry asks with the cached ETag"
        );
        rig.wait_entry(
            &path,
            "the new version replaces the entry",
            |m: Option<&Entry>| {
                m.is_some_and(|m| {
                    m.object_metadata.etag == "\"version-2\""
                        && m.ranges.is_empty()
                        && m.write_expired_at.is_none()
                })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn after_a_delete_expiry_a_head_answered_404_removes_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    for how in DELETES {
        let path = path_for("head-404", how);
        let (_, etag) = expired_by(&rig, &path, how).await;
        rig.mock.remove(&path);
        let (status, _) = rig.send("HEAD", &path, &signed()).await;
        assert_eq!(
            (
                status,
                rig.mock.if_none_match(&path).get(1).cloned().flatten()
            ),
            (StatusCode::NOT_FOUND, Some(etag)),
            "{how:?}: the HEAD after the expiry asks with the cached ETag and is answered 404"
        );
        rig.wait_entry(&path, "the 404 removes the entry", |m: Option<&Entry>| {
            m.is_none()
        })
        .await;
    }
}

#[tokio::test]
async fn after_an_expiry_a_ranged_get_answered_304_keeps_the_entry() {
    let rig = Rig::new(RigOptions::default()).await;
    for how in DELETES.into_iter().chain([Expiry::SignedPut]) {
        let path = path_for("range-304", how);
        let (data, etag) = expired_by(&rig, &path, how).await;
        rig.mock.set_honour_if_none_match(true);
        let hits_before = rig.mock.hits(&path);
        let (status, body) = rig
            .get(&path, &[("range", "bytes=100-1123".to_string())])
            .await;
        rig.mock.set_honour_if_none_match(false);
        let entry = rig.entry(&path).await;
        assert_eq!(
            (
                status,
                body == data.slice(100..1124),
                rig.mock.hits(&path) - hits_before,
                rig.mock.if_none_match(&path).get(1).cloned().flatten(),
                entry.as_ref().map(|m| m.write_expired_at.is_none()),
                entry.as_ref().map(|m| !m.ranges.is_empty()),
            ),
            (
                StatusCode::PARTIAL_CONTENT,
                true,
                1,
                Some(etag),
                Some(true),
                Some(true)
            ),
            "{how:?}: the ranged GET after the expiry (status, the cached slice, origin requests, \
             its If-None-Match, the expiry cleared, the data kept)"
        );
        let hits_before = rig.mock.hits(&path);
        let (status, body) = rig
            .get(&path, &[("range", "bytes=100-1123".to_string())])
            .await;
        assert_eq!(
            (
                status,
                body == data.slice(100..1124),
                rig.mock.hits(&path) - hits_before
            ),
            (StatusCode::PARTIAL_CONTENT, true, 0),
            "{how:?}: after the 304 the same range is a hit"
        );
    }
}

#[tokio::test]
async fn after_an_expiry_a_ranged_get_answered_404_asks_the_origin_once() {
    let rig = Rig::new(RigOptions::default()).await;
    for how in DELETES.into_iter().chain([Expiry::SignedPut]) {
        let path = path_for("range-404", how);
        let (_, etag) = expired_by(&rig, &path, how).await;
        rig.mock.remove(&path);
        let hits_before = rig.mock.hits(&path);
        let (status, _) = rig
            .get(&path, &[("range", "bytes=100-1123".to_string())])
            .await;
        assert_eq!(
            (
                status,
                rig.mock.hits(&path) - hits_before,
                rig.mock.if_none_match(&path).get(1).cloned().flatten()
            ),
            (StatusCode::NOT_FOUND, 1, Some(etag)),
            "{how:?}: the ranged GET after the expiry asks once, with the cached ETag, and is \
             answered 404 (status, origin requests, its If-None-Match)"
        );
    }
}
#[tokio::test]
async fn after_an_expiry_a_ranged_get_of_a_new_version_replaces_the_entry() {
    for how in DELETES.into_iter().chain([Expiry::SignedPut]) {
        let rig = Rig::new(RigOptions::default()).await;
        let path = path_for("range-200", how);
        let (data, etag) = expired_by(&rig, &path, how).await;
        let new = proxy_rig::object_bytes(LEN).slice(1..);
        rig.mock.put_bytes(&path, new.clone());
        rig.mock.set_etag("\"version-2\"");
        rig.mock.set_honour_if_none_match(true);
        let (status, body) = rig
            .get(&path, &[("range", "bytes=100-1123".to_string())])
            .await;
        assert_eq!(
            (
                status,
                body == new.slice(100..1124),
                body == data.slice(100..1124),
                rig.mock.if_none_match(&path).get(1).cloned().flatten()
            ),
            (StatusCode::PARTIAL_CONTENT, true, false, Some(etag)),
            "{how:?}: the ranged GET after the expiry (status, the new version's slice, not the \
             cached one, its If-None-Match)"
        );
        rig.wait_entry(
            &path,
            "the new version replaces the entry",
            |m: Option<&Entry>| {
                m.is_some_and(|m| {
                    m.object_metadata.etag == "\"version-2\"" && m.write_expired_at.is_none()
                })
            },
        )
        .await;
    }
}
