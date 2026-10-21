//! The multipart staging directory is a digest of the uploadId, never the uploadId.
//!
//! `signed_put_handler::mpu_upload_dir` builds `mpus_in_progress/<BLAKE3 hex>`, so no byte
//! of a client- or S3-supplied uploadId reaches a filesystem path. It replaced joining the
//! raw uploadId behind `path_safety::is_safe_path_component`, which rejected any `..`
//! substring and so declined write-through caching for about one real S3 upload in ten.
//!
//! This file holds the three properties that change must keep:
//!
//! 1. **Traversal still fails.** An `UploadPart` carrying `../../etc` or `a/b`, driven
//!    through the public handler entry point, creates nothing outside
//!    `mpus_in_progress/<digest>/`. This test compiles against the pre-fix tree too (it
//!    names no new API), and passes there, because the old guard declined the upload.
//! 2. **No raw join returns.** A source scan of `src/` fails if a path is built from an
//!    uploadId by anything other than the helper.
//! 3. **A pre-upgrade directory is still reclaimed.** An upload directory named after the
//!    raw uploadId, as every build before this one wrote it, is removed by the age sweep,
//!    which reads mtimes and never the directory name.
use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response, StatusCode};
use s3_proxy::cache::CacheManager;
use s3_proxy::compression::CompressionHandler;
use s3_proxy::signed_put_handler::SignedPutHandler;
use s3_proxy::signed_request_proxy::UpstreamTransport;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// A genuine `Request<Incoming>` via a loopback HTTP/1 connection. The returned sender is
/// a keep-alive guard: the connection feeding the body lives until it is dropped.
async fn incoming_request(
    method: &str,
    path_and_query: &str,
    body: Vec<u8>,
) -> (
    Request<hyper::body::Incoming>,
    tokio::sync::oneshot::Sender<()>,
) {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use tokio::sync::oneshot;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (req_tx, req_rx) = oneshot::channel::<Request<hyper::body::Incoming>>();
    let req_tx = Arc::new(std::sync::Mutex::new(Some(req_tx)));
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let done_rx = Arc::new(std::sync::Mutex::new(Some(done_rx)));

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
            if let Some(tx) = req_tx.lock().unwrap().take() {
                let _ = tx.send(req);
            }
            let parked = done_rx.lock().unwrap().take();
            async move {
                if let Some(parked) = parked {
                    let _ = parked.await;
                }
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
            }
        });
        let _ = http1::Builder::new().serve_connection(io, service).await;
    });

    let body_len = body.len();
    let path_and_query = path_and_query.to_string();
    let method = method.to_string();
    tokio::spawn(async move {
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;
        let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
        let req = Request::builder()
            .method(method.as_str())
            .uri(format!("http://{}{}", addr, path_and_query))
            .header("content-length", body_len.to_string())
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        let _ = client.request(req).await;
    });

    (req_rx.await.unwrap(), done_tx)
}

/// Mock S3: drain the request headers plus `body_len` body bytes, then reply with an ETag.
async fn accept_and_reply_with_etag(listener: tokio::net::TcpListener, body_len: usize) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 65536];
    let header_end = loop {
        let n = sock.read(&mut tmp).await.unwrap();
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let mut body_read = buf.len() - header_end;
    while body_read < body_len {
        let n = sock.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        body_read += n;
    }
    let _ = sock
        .write_all(b"HTTP/1.1 200 OK\r\nETag: \"scn1-part\"\r\nContent-Length: 0\r\n\r\n")
        .await;
    let _ = sock.flush().await;
}

/// Every path under `root`, directories included, relative to `root`.
fn walk(root: &Path) -> Vec<PathBuf> {
    fn go(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            out.push(path.strip_prefix(root).unwrap().to_path_buf());
            if path.is_dir() {
                go(root, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    go(root, root, &mut out);
    out.sort();
    out
}

fn is_blake3_hex(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Property 1. Drives `UploadPart` through `handle_unsigned_put`, the public entry point,
/// so the request passes through the handler's own uploadId guard and the real staging
/// path. The cache directory is a subdirectory of the temp root, so a `..` that escapes it
/// lands somewhere this test can see.
///
/// Waits on a POSITIVE condition — a `part1.*` file appearing anywhere under the root —
/// bounded at 10 s. On a build whose guard declines the uploadId (every build before the change)
/// nothing appears, the wait times out, and the assertions pass on an untouched tree: that
/// is the "passes before" half, and it is why the test asserts only where files are, never
/// that one was written.
#[tokio::test]
async fn traversal_upload_ids_create_nothing_outside_their_digest_directory() {
    for upload_id in ["../../etc", "a/b", "..", "../escape/../x"] {
        let root = TempDir::new().unwrap();
        let cache_dir = root.path().join("cache");
        std::fs::create_dir_all(cache_dir.join("mpus_in_progress")).unwrap();
        let sentinel = root.path().join("sentinel.txt");
        std::fs::write(&sentinel, b"must survive").unwrap();

        let cache_manager = Arc::new(CacheManager::new(cache_dir.clone(), false, 0, 100, false));
        let mut handler = SignedPutHandler::new(
            cache_dir.clone(),
            CompressionHandler::new(1024, false),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager);

        let body = vec![0x5Au8; 2048];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_and_reply_with_etag(listener, body.len()));
        let (req, _keepalive) = incoming_request(
            "PUT",
            &format!("/scn1-obj?uploadId={}&partNumber=1", upload_id),
            body,
        )
        .await;
        let transport = Arc::new(UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
        });
        let resp = tokio::time::timeout(
            Duration::from_secs(60),
            handler.handle_unsigned_put(
                req,
                "test-bucket/scn1-obj".to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport,
            ),
        )
        .await
        .expect("UploadPart did not finish within 60s")
        .expect("UploadPart must reach the mock upstream");
        assert_eq!(resp.status(), StatusCode::OK, "uploadId={upload_id:?}");
        let _ = tokio::time::timeout(Duration::from_secs(5), upstream).await;

        // Positive wait: the background part task lags the response by design.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut staged = false;
        while std::time::Instant::now() < deadline {
            staged = walk(root.path()).iter().any(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("part1."))
            });
            if staged {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        println!("uploadId={upload_id:?} staged={staged}");

        assert!(
            sentinel.exists(),
            "uploadId={upload_id:?}: sentinel deleted"
        );
        for rel in walk(root.path()) {
            if rel == Path::new("sentinel.txt") {
                continue;
            }
            assert!(
                rel.starts_with("cache"),
                "uploadId={upload_id:?}: {rel:?} was created OUTSIDE the cache directory"
            );
            if let Ok(under) = rel.strip_prefix("cache/mpus_in_progress") {
                if let Some(first) = under.components().next() {
                    let name = first.as_os_str().to_str().unwrap();
                    assert!(
                        is_blake3_hex(name),
                        "uploadId={upload_id:?}: mpus_in_progress/{name} is not a 64-hex \
                         digest — the raw uploadId reached the path"
                    );
                }
            }
        }
    }
}

/// A path built from something named `upload_id`: `.join(x)`, `.push(x)`, `Path::new(x)`,
/// `PathBuf::from(x)` where `x` is a bare identifier or a field path (`a.b.upload_id`),
/// optionally borrowed, whose last segment contains `upload_id`. Matched against code with
/// all whitespace removed. Shared by the tree scan and its self-test, so the self-test
/// exercises the exact pattern the scan uses.
const RAW_JOIN_PATTERN: &str = r"(\.join|\.push|Path::new|PathBuf::from)\(&?(?:[A-Za-z_][A-Za-z0-9_]*\.)*[A-Za-z0-9_]*upload_id[A-Za-z0-9_]*\)";

/// Lines of production-shaped code in `src/`, with `//` comment lines dropped and all
/// whitespace removed so a join split across lines is still one token run.
fn src_code_without_comments() -> Vec<(PathBuf, String)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    collect(&src, &mut files);
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let text = std::fs::read_to_string(&path).unwrap();
            let code: String = text
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .flat_map(|l| l.chars().filter(|c| !c.is_whitespace()))
                .collect();
            (path, code)
        })
        .collect()
}

/// Property 2. Fails if any code in `src/` — production or inline test — builds a path
/// from an uploadId other than through `mpu_upload_dir`. The forms checked are the ones a
/// raw join would take: `.join(x)` / `.push(x)` / `Path::new(x)` / `PathBuf::from(x)` where
/// `x` is an identifier or field path ending in something named `upload_id`, and a
/// `format!` that interpolates into `mpus_in_progress/`.
///
/// Scans the whole tree rather than a list of files, so a new module is covered without
/// anyone remembering to add it.
#[test]
fn no_path_in_src_is_built_from_a_raw_upload_id() {
    let raw_join = regex::Regex::new(RAW_JOIN_PATTERN).unwrap();
    let raw_format = regex::Regex::new(r#""mpus_in_progress/\{"#).unwrap();

    let files = src_code_without_comments();
    assert!(
        files.len() > 50,
        "scan found only {} files under src/ — the walk is broken, so a pass would be vacuous",
        files.len()
    );
    let helper_present = files.iter().any(|(p, code)| {
        p.ends_with("signed_put_handler.rs") && code.contains("pubfnmpu_upload_dir(")
    });
    assert!(
        helper_present,
        "mpu_upload_dir not found in signed_put_handler.rs — the scan would pass vacuously"
    );

    let mut hits = Vec::new();
    for (path, code) in &files {
        for m in raw_join.find_iter(code) {
            hits.push(format!("{}: {}", path.display(), m.as_str()));
        }
        for m in raw_format.find_iter(code) {
            hits.push(format!("{}: {}", path.display(), m.as_str()));
        }
    }
    assert!(
        hits.is_empty(),
        "a filesystem path is built from a raw uploadId; route it through \
         signed_put_handler::mpu_upload_dir:\n{}",
        hits.join("\n")
    );
}

/// The scanner's own red side, independent of the tree: it must flag each shape it claims
/// to cover, including one split across lines, and must not flag the helper's digest join.
#[test]
fn raw_upload_id_scanner_flags_each_shape_it_names() {
    let raw_join = regex::Regex::new(RAW_JOIN_PATTERN).unwrap();
    let norm = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
    for flagged in [
        "dir.join(upload_id)",
        "dir.join(&upload_id)",
        "dir.join(tracker.upload_id)",
        "dir\n    .join(\n        &iter_upload_id\n    )",
        "p.push(upload_id)",
        "Path::new(upload_id)",
        "PathBuf::from(&upload_id)",
    ] {
        assert!(raw_join.is_match(&norm(flagged)), "must flag {flagged:?}");
    }
    for clean in [
        ".join(blake3::hash(upload_id.as_bytes()).to_hex().as_str())",
        "mpu_upload_dir(&cache_dir, upload_id)",
        "dir.join(\"upload.meta\")",
    ] {
        assert!(!raw_join.is_match(&norm(clean)), "must not flag {clean:?}");
    }
}

/// Property 3. An upload begun before the upgrade left `mpus_in_progress/<raw uploadId>/`.
/// After the upgrade nothing looks there by name, so the only thing that removes it is the
/// age sweep — which must still do so. The sweep ages entries by the newest of the
/// directory's and `upload.meta`'s mtimes, so both are backdated past the TTL.
#[tokio::test]
async fn pre_upgrade_raw_named_upload_directory_is_reclaimed_by_the_age_sweep() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().to_path_buf();
    let legacy_id = "m0TmAnJ8zEI3xQ7vR2_kLp9WcHs4Nd6Yb1Gf5Ut8Ze3Ja0Mo7Ri2Xs9Kv4Pw6Dl1Cq8En5Bh3Ty0Fg7Iu2Ok9Sn4Va6Lm1Wr8Hx3Qe5Zc0Jp7Gt2Yd9Mb4Ki6Uf1Aw8";
    let legacy_dir = cache_dir.join("mpus_in_progress").join(legacy_id);
    tokio::fs::create_dir_all(&legacy_dir).await.unwrap();
    tokio::fs::write(legacy_dir.join("part1.bin"), vec![0u8; 1024])
        .await
        .unwrap();
    let tracker = s3_proxy::cache_types::MultipartUploadTracker::new(
        legacy_id.to_string(),
        "test-bucket/pre-upgrade-object".to_string(),
    );
    let upload_meta = legacy_dir.join("upload.meta");
    tokio::fs::write(&upload_meta, tracker.to_json().unwrap())
        .await
        .unwrap();
    let old = filetime::FileTime::from_system_time(
        std::time::SystemTime::now() - Duration::from_secs(3600),
    );
    filetime::set_file_mtime(&upload_meta, old).unwrap();
    filetime::set_file_mtime(&legacy_dir, old).unwrap();

    let manager = s3_proxy::write_cache_manager::WriteCacheManager::new(
        cache_dir.clone(),
        100 * 1024 * 1024,
        10.0,
        Duration::from_secs(86400),
        Duration::from_secs(60), // incomplete-upload TTL, well under the 1 h backdate
        s3_proxy::cache::CacheEvictionAlgorithm::TinyLFU,
        256 * 1024 * 1024,
    );
    manager.evict_incomplete_uploads().await.unwrap();
    assert!(
        !legacy_dir.exists(),
        "a pre-upgrade upload directory named after the raw uploadId must be reclaimed by \
         the age sweep, or every in-flight upload at upgrade time leaks its staged parts"
    );
}
