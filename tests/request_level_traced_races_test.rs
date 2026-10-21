//! Request-level tests for read-path races that reviews traced and earlier units tested
//! only below `http_proxy`. Every test sends a real HTTP request through
//! `HttpProxy::handle_request` on a loopback port and checks what the client receives: the
//! status, and the bytes against the version the response's `ETag` names.
//!
//! Version `v` of the object stores `((i % 251) as u8) ^ v` at offset `i`, so both versions
//! compress to the same length, a body decodes to the version that filled it, and a mixture
//! decodes to none.
//!
//! The origin ([`Origin`]) answers like S3 for one key with a current version: a conditional
//! whose `If-None-Match` names the current ETag gets `304`, anything else gets that version's
//! bytes. An armed hook runs inside the origin's answer to the proxy's conditional request,
//! which the range path sends after its cache lookup and before it reads the cached range, so
//! a hook that renames a file over the listed path lands between the two.
//!
//! A range file replaced between the lookup that resolved its listing and
//!   the read that opens it, on the buffered, streamed and slice serves.
//! The `.meta` lists one fill's identity over another
//!   version's file, on the buffered and streamed serves.
//! A stale RAM snapshot and a peer's newer `.meta` with other extents.
//! - A damaged range file, listed or only journal-pending.
mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Method, Request, StatusCode};

use common::expired_fixture::{
    signed_authorization_without_range, Fixture, SeedSpec, StoredExpiry,
};
use common::StubS3Client;
use s3_proxy::cache_types::{
    CacheMetadata, NewCacheMetadata, ObjectMetadata, RangeFileIdentity, RangeSpec, UploadState,
};
use s3_proxy::config::Config;
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::{Result, S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};

const LEN: u64 = 300_000;
const V1: u8 = 0x11;
const V2: u8 = 0x22;
const ETAG_V1: &str = "\"11111111111111111111111111111111\"";
const ETAG_V2: &str = "\"22222222222222222222222222222222\"";
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";
/// Every test body runs under this bound.
const TEST_BOUND: Duration = Duration::from_secs(90);
/// A small streaming threshold, so a 300,000-byte extent takes the streamed serves.
const STREAM_AT: u64 = 65_536;
/// The modification time a peer's publish carries here: fixed, and years from any fill this
/// test makes, so a replacement differs from every listing by more than the identity's match
/// window whatever the clock tick.
const PEER_MTIME: (i64, u32) = (1_000_000_000, 123_456_789);

fn version_bytes(v: u8) -> Vec<u8> {
    (0..LEN as usize).map(|i| ((i % 251) as u8) ^ v).collect()
}

/// Which version filled `bytes`, read from object offset `offset`, or `None` for a mixture.
fn version_at(bytes: &[u8], offset: u64) -> Option<u8> {
    let first = *bytes.first()?;
    let v = first ^ ((offset as usize % 251) as u8);
    bytes
        .iter()
        .enumerate()
        .all(|(i, b)| *b == (((offset as usize + i) % 251) as u8) ^ v)
        .then_some(v)
}

fn etag_of(v: u8) -> &'static str {
    if v == V1 {
        ETAG_V1
    } else {
        ETAG_V2
    }
}

fn version_named_by(etag: Option<&str>) -> Option<u8> {
    let e = etag?.trim_matches('"');
    if e == ETAG_V1.trim_matches('"') {
        Some(V1)
    } else if e == ETAG_V2.trim_matches('"') {
        Some(V2)
    } else {
        None
    }
}

fn label(v: Option<u8>) -> String {
    match v {
        Some(v) => format!("v{:#x}", v),
        None => "a mixture or no version".to_string(),
    }
}

fn peer_mtime() -> filetime::FileTime {
    filetime::FileTime::from_unix_time(PEER_MTIME.0, PEER_MTIME.1)
}

// ── The origin ───────────────────────────────────────────────────────────────

type Hook = Box<dyn FnOnce() + Send>;

/// S3 for one object with a current version, answering conditionals by ETag.
struct Origin {
    inner: StubS3Client,
    current: Mutex<u8>,
    on_conditional: Mutex<Option<Hook>>,
    log: Mutex<Vec<String>>,
}

impl Origin {
    fn new(current: u8) -> Arc<Self> {
        Arc::new(Self {
            inner: StubS3Client::new(),
            current: Mutex::new(current),
            on_conditional: Mutex::new(None),
            log: Mutex::new(Vec::new()),
        })
    }

    fn set_current(&self, v: u8) {
        *self.current.lock().unwrap() = v;
    }

    /// Run `hook` inside the origin's answer to the next conditional request.
    fn arm(&self, hook: impl FnOnce() + Send + 'static) {
        *self.on_conditional.lock().unwrap() = Some(Box::new(hook));
    }

    fn note(&self, line: String) {
        self.log.lock().unwrap().push(line);
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    fn requests(&self) -> usize {
        self.log()
            .iter()
            .filter(|l| l.starts_with("upstream"))
            .count()
    }
}

fn parse_range(range: &str) -> Option<(u64, u64)> {
    let spec = range.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    let start: u64 = a.parse().ok()?;
    let end: u64 = if b.is_empty() {
        LEN - 1
    } else {
        b.parse::<u64>().ok()?.min(LEN - 1)
    };
    (start <= end).then_some((start, end))
}

#[async_trait]
impl S3ClientApi for Origin {
    async fn forward_request(&self, context: S3RequestContext) -> Result<S3Response> {
        let header = |name: &str| {
            context
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        let inm = header("if-none-match");
        let range = header("range");
        let v = *self.current.lock().unwrap();
        let etag = etag_of(v);
        let mut headers = HashMap::new();
        headers.insert("etag".to_string(), etag.to_string());
        headers.insert("last-modified".to_string(), LAST_MODIFIED.to_string());
        headers.insert("accept-ranges".to_string(), "bytes".to_string());
        let (status, body) = if inm
            .as_deref()
            .is_some_and(|i| i.trim_matches('"') == etag.trim_matches('"'))
        {
            (StatusCode::NOT_MODIFIED, None)
        } else {
            let bytes = version_bytes(v);
            match range.as_deref().and_then(parse_range) {
                Some((s, e)) => {
                    headers.insert(
                        "content-range".to_string(),
                        format!("bytes {}-{}/{}", s, e, LEN),
                    );
                    headers.insert("content-length".to_string(), (e - s + 1).to_string());
                    (
                        StatusCode::PARTIAL_CONTENT,
                        Some(Bytes::copy_from_slice(&bytes[s as usize..=e as usize])),
                    )
                }
                None => {
                    headers.insert("content-length".to_string(), LEN.to_string());
                    (StatusCode::OK, Some(Bytes::from(bytes)))
                }
            }
        };
        let body = if context.method == Method::HEAD {
            None
        } else {
            body
        };
        self.note(format!(
            "upstream {} range={:?} if-none-match={:?} -> {} {}",
            context.method,
            range,
            inm,
            status.as_u16(),
            label(Some(v))
        ));
        if inm.is_some() {
            let hook = self.on_conditional.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
                self.note("hook ran inside the origin's answer to the conditional".to_string());
            }
        }
        Ok(S3Response {
            status,
            headers,
            body: body.map(S3ResponseBody::Buffered),
            request_duration: Duration::from_millis(0),
        })
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

    async fn refresh_dns(&self) -> Result<()> {
        Ok(())
    }
}

// ── The cache ────────────────────────────────────────────────────────────────

/// A config with the confounds off: no download coordination (the waiter path would replace
/// the inline revalidation), no RAM range tier (a RAM hit answers before the disk read), and
/// the full-object shortcut skipped so the range-specific lookup is the one under test.
fn config(streaming_threshold: u64, get_ttl: Duration) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = 1;
    config.cache.disk_streaming_threshold = streaming_threshold;
    config.cache.get_ttl = get_ttl;
    Arc::new(config)
}

fn object_metadata(v: u8) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag_of(v).to_string(),
        content_length: LEN,
        last_modified: LAST_MODIFIED.to_string(),
        upload_state: UploadState::Complete,
        cumulative_size: LEN,
        ..Default::default()
    }
}

/// Fill `start..=end` of version `v` through the production writer (journal-pending).
async fn fill(fixture: &Fixture, key: &str, v: u8, start: u64, end: u64) {
    fixture
        .cache_manager
        .create_configured_disk_cache_manager()
        .store_range(
            key,
            start,
            end,
            &version_bytes(v)[start as usize..=end as usize],
            object_metadata(v),
            Duration::from_secs(3600),
            true,
        )
        .await
        .expect("range fill");
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

fn bin_path(fixture: &Fixture, spec: &RangeSpec) -> PathBuf {
    fixture
        .cache_manager
        .get_cache_dir()
        .join("ranges")
        .join(&spec.file_path)
}

fn identity_at(path: &Path) -> Option<RangeFileIdentity> {
    std::fs::metadata(path)
        .ok()
        .and_then(|m| RangeFileIdentity::of(&m))
}

fn listed(fixture: &Fixture, key: &str) -> Vec<RangeSpec> {
    fixture
        .read_meta(key)
        .map(|m: NewCacheMetadata| m.ranges)
        .unwrap_or_default()
}

async fn pending(fixture: &Fixture, key: &str, start: u64, end: u64) -> Vec<RangeSpec> {
    fixture
        .cache_manager
        .create_configured_disk_cache_manager()
        .find_pending_journal_ranges(key, start, end)
        .await
        .unwrap_or_default()
}

/// The `.bin` another proxy's fill of version `v` publishes for `start..=end`, built in its own
/// cache directory with the same config.
async fn foreign_bin(cfg: &Arc<Config>, key: &str, v: u8, start: u64, end: u64) -> Vec<u8> {
    let other = Fixture::new(Arc::clone(cfg)).await;
    fill(&other, key, v, start, end).await;
    let spec = pending(&other, key, start, end)
        .await
        .into_iter()
        .next()
        .expect("the foreign fill is journal-pending");
    std::fs::read(bin_path(&other, &spec)).expect("foreign .bin")
}

/// Rename `bytes` over `path` as a peer's publish does: a new file, published at
/// [`PEER_MTIME`].
fn rename_over(path: &Path, bytes: &[u8]) {
    let tmp = path.with_extension("scn1tmp");
    std::fs::write(&tmp, bytes).unwrap();
    filetime::set_file_mtime(&tmp, peer_mtime()).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

// ── The client ───────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Served {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
    /// The body ended with an error (a stream the proxy broke off after its head).
    body_error: bool,
}

impl Served {
    fn etag(&self) -> Option<&str> {
        self.headers.get("etag").and_then(|v| v.to_str().ok())
    }

    fn x_cache(&self) -> Option<&str> {
        self.headers.get("x-cache").and_then(|v| v.to_str().ok())
    }
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
    let resp = tokio::time::timeout(Duration::from_secs(20), client.request(req))
        .await
        .expect("the request must not hang")
        .expect("proxy request failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let mut body = resp.into_body();
    let mut bytes = Vec::new();
    let mut body_error = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(20), body.frame()).await {
            Err(_) => panic!("the response body must not hang"),
            Ok(None) => break,
            Ok(Some(Err(_))) => {
                body_error = true;
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
        headers,
        body: bytes,
        body_error,
    }
}

/// The client got a whole `206` for `start..=end` whose bytes are the version its `ETag`
/// names. Returns that version.
fn assert_one_labelled_version(
    served: &Served,
    start: u64,
    end: u64,
    what: &str,
    log: &[String],
) -> u8 {
    let len = (end - start + 1) as usize;
    let body_version = version_at(&served.body, start);
    let named = version_named_by(served.etag());
    assert!(
        served.status == StatusCode::PARTIAL_CONTENT
            && !served.body_error
            && served.body.len() == len
            && body_version.is_some()
            && body_version == named,
        "WRONG BYTES SERVED ({}): status {}, body {} of {} bytes{}, bytes are {}, ETag {:?} names \
         {}, x-cache {:?}; upstream: {:?}",
        what,
        served.status,
        served.body.len(),
        len,
        if served.body_error {
            " ending in an error"
        } else {
            ""
        },
        label(body_version),
        served.etag(),
        label(named),
        served.x_cache(),
        log
    );
    body_version.unwrap()
}

async fn bounded<F: std::future::Future<Output = ()>>(f: F) {
    tokio::time::timeout(TEST_BOUND, f)
        .await
        .expect("the test must finish within its bound");
}

// ── a range file replaced between resolution and open ────────────────

/// Seed V1 over `0..LEN-1` with its identity listed, age it past `get_ttl` so the next read
/// revalidates, and arm the origin so its `304` to that revalidation renames V2's file over
/// the listed path and makes V2 current. Returns the fixture, the origin and the listed spec.
async fn replaced_between_lookup_and_open(
    cfg: Arc<Config>,
    key: &str,
) -> (Fixture, Arc<Origin>, RangeSpec) {
    let fixture = Fixture::new(Arc::clone(&cfg)).await;
    fill(&fixture, key, V1, 0, LEN - 1).await;
    consolidate(&fixture, key).await;
    let spec = listed(&fixture, key).into_iter().next().expect("V1 listed");
    let path = bin_path(&fixture, &spec);
    assert!(
        spec.file_identity.is_some() && spec.file_identity == identity_at(&path),
        "fixture: the listing records the identity of the file its writer published"
    );
    let v2_bin = foreign_bin(&cfg, key, V2, 0, LEN - 1).await;
    assert_eq!(
        v2_bin.len() as u64,
        spec.compressed_size,
        "fixture: both versions are the same length, so only the modification time tells them \
         apart"
    );
    // Live-expired, so the read sends a conditional after its lookup.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let origin = Origin::new(V1);
    let hook_origin = Arc::clone(&origin);
    let hook_path = path.clone();
    origin.arm(move || {
        rename_over(&hook_path, &v2_bin);
        hook_origin.set_current(V2);
    });
    (fixture, origin, spec)
}

async fn item1_case(threshold: u64, range: (u64, u64), what: &'static str) {
    let key = format!("test-bucket/scn1/item1-{}.bin", what.replace(' ', "-"));
    let cfg = config(threshold, Duration::from_secs(1));
    let (fixture, origin, spec) = replaced_between_lookup_and_open(cfg, &key).await;
    let server = fixture.spawn_proxy(origin.clone()).await;
    let served = get(server.addr, &key, &format!("bytes={}-{}", range.0, range.1)).await;
    let log = origin.log();
    assert!(
        log.iter().any(|l| l.starts_with("hook ran")),
        "PRECONDITION ({}): the request never sent the conditional its lookup precedes, so the \
         rename never landed inside the window; client got {} x-cache {:?}; upstream: {:?}",
        what,
        served.status,
        served.x_cache(),
        log
    );
    let v = assert_one_labelled_version(&served, range.0, range.1, what, &log);
    assert_eq!(v, V2, "({}) the file at the path and S3 are both V2", what);
    // The read refused the renamed file: the listing no longer names V1's identity.
    assert!(
        listed(&fixture, &key)
            .iter()
            .all(|r| r.file_identity != spec.file_identity),
        "({}) the refused listing must be healed",
        what
    );
}

/// Buffered serve (`load_range_data`): the extent is below the streaming threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item1_a_file_renamed_after_the_lookup_is_never_served_buffered() {
    bounded(item1_case(1_048_576, (0, LEN - 1), "item1 buffered")).await;
}

/// Exact-match streaming serve (`stream_range_data`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item1_a_file_renamed_after_the_lookup_is_never_served_streamed() {
    bounded(item1_case(STREAM_AT, (0, LEN - 1), "item1 streamed")).await;
}

/// Slice streaming serve (`stream_range_data_from`): a sub-range of the extent at or above the
/// streaming threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item1_a_file_renamed_after_the_lookup_is_never_served_as_a_slice() {
    bounded(item1_case(STREAM_AT, (100_000, 199_999), "item1 slice")).await;
}

/// Control: with no rename, the same request is a cache hit of V1 after the `304`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item1_control_without_the_rename_the_revalidated_range_is_a_hit() {
    bounded(async {
        let key = "test-bucket/scn1/item1-control.bin";
        let cfg = config(1_048_576, Duration::from_secs(1));
        let fixture = Fixture::new(Arc::clone(&cfg)).await;
        fill(&fixture, key, V1, 0, LEN - 1).await;
        consolidate(&fixture, key).await;
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let origin = Origin::new(V1);
        let server = fixture.spawn_proxy(origin.clone()).await;
        let served = get(server.addr, key, &format!("bytes=0-{}", LEN - 1)).await;
        let log = origin.log();
        let v = assert_one_labelled_version(&served, 0, LEN - 1, "item1 control", &log);
        assert_eq!(v, V1);
        assert_eq!(
            origin.requests(),
            1,
            "control: one conditional, then the cached bytes: {:?}",
            log
        );
    })
    .await;
}

// ── the cross-batch fill inversion ───────────────────────────────────

/// The review's schedule. A (V1) prepares first on a peer. B caches V2 here. A's rename lands over B's file before the
/// consolidator takes the slot lock, and A's journal line is not in that batch, so the pass
/// lists B's identity over A's file. A's line then reaches the shared journals and a second
/// pass runs; its older `created_at` makes it the "listed later" case, which drops the entry
/// only. Then a client reads the extent.
async fn item2_case(threshold: u64, what: &'static str) {
    let key = format!("test-bucket/scn1/item2-{}.bin", what.replace(' ', "-"));
    let range = format!("bytes=0-{}", LEN - 1);
    let cfg = config(threshold, Duration::from_secs(3600));
    let fixture = Fixture::new(Arc::clone(&cfg)).await;
    // A, on a peer sharing the volume, prepares first.
    let peer = Fixture::new(Arc::clone(&cfg)).await;
    fill(&peer, &key, V1, 0, LEN - 1).await;
    let a_spec = pending(&peer, &key, 0, LEN - 1).await.remove(0);
    tokio::time::sleep(Duration::from_millis(20)).await;
    // B: this proxy caches V2 through the production writer. (A GET miss cannot make this
    // fill here: the stub's buffered body is passed through uncached on the streamed miss path.)
    fill(&fixture, &key, V2, 0, LEN - 1).await;
    let b_spec = pending(&fixture, &key, 0, LEN - 1)
        .await
        .into_iter()
        .next()
        .expect("fixture: B's fill is journal-pending");
    let path = bin_path(&fixture, &b_spec);
    assert!(
        a_spec.created_at < b_spec.created_at,
        "fixture: A prepared before B"
    );
    // A's rename lands after B's, before the consolidator's pass.
    filetime::set_file_mtime(bin_path(&peer, &a_spec), peer_mtime()).unwrap();
    std::fs::rename(bin_path(&peer, &a_spec), &path).unwrap();
    consolidate(&fixture, &key).await;
    // A's journal line reaches the shared journals; a second pass reads it.
    for entry in std::fs::read_dir(
        peer.cache_manager
            .get_cache_dir()
            .join("metadata/_journals"),
    )
    .unwrap()
    {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            std::fs::copy(
                entry.path(),
                fixture
                    .cache_manager
                    .get_cache_dir()
                    .join("metadata/_journals")
                    .join(format!("peer-a-{}", entry.file_name().to_string_lossy())),
            )
            .unwrap();
        }
    }
    consolidate(&fixture, &key).await;
    let meta = fixture.read_meta(&key).expect("the passes publish a .meta");
    let spec = meta
        .ranges
        .first()
        .cloned()
        .expect("an extent stays listed");
    assert_eq!(meta.object_metadata.etag, ETAG_V2, "fixture: B's version");
    assert!(
        spec.file_identity.is_some() && spec.file_identity != identity_at(&path),
        "fixture: B's identity listed over A's file after both passes (listed {:?}, at the path \
         {:?})",
        spec.file_identity,
        identity_at(&path)
    );
    // The read.
    let origin = Origin::new(V2);
    let server = fixture.spawn_proxy(origin.clone()).await;
    let before = origin.requests();
    let served = get(server.addr, &key, &range).await;
    let log = origin.log();
    let v = assert_one_labelled_version(&served, 0, LEN - 1, what, &log);
    assert_eq!(v, V2, "({}) S3's current version", what);
    assert!(
        origin.requests() > before,
        "({}) A's file was refused, so the bytes came from S3: {:?}",
        what,
        log
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item2_a_cross_batch_inversion_is_never_served_buffered() {
    bounded(item2_case(1_048_576, "item2 buffered")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item2_a_cross_batch_inversion_is_never_served_streamed() {
    bounded(item2_case(STREAM_AT, "item2 streamed")).await;
}

/// Give a journal-pending fill in `fixture` the peer's modification time, as if its writer had
/// published it then: the `.bin`'s mtime and the identity its `Add` records move together.
fn restamp_pending_fill(fixture: &Fixture, spec: &mut RangeSpec) {
    let bin = bin_path(fixture, spec);
    filetime::set_file_mtime(&bin, peer_mtime()).unwrap();
    let stamped = identity_at(&bin).expect("the restamped file's identity");
    let journals = fixture
        .cache_manager
        .get_cache_dir()
        .join("metadata/_journals");
    let mut patched = 0;
    for entry in std::fs::read_dir(journals).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("journal") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut out = String::new();
        for line in text.lines() {
            match serde_json::from_str::<serde_json::Value>(line) {
                Ok(mut v) => {
                    let rs = &mut v["range_spec"];
                    if rs["file_path"] == serde_json::Value::String(spec.file_path.clone())
                        && rs["file_identity"].is_object()
                    {
                        rs["file_identity"] = serde_json::to_value(stamped).unwrap();
                        patched += 1;
                    }
                    out.push_str(&v.to_string());
                }
                Err(_) => out.push_str(line),
            }
            out.push('\n');
        }
        std::fs::write(&path, out).unwrap();
    }
    assert_eq!(
        patched, 1,
        "fixture: the fill's one journal Add was restamped"
    );
    spec.file_identity = Some(stamped);
}

/// Two fills of one extent of the SAME version, from two proxies, whose prepare order inverts
/// their rename order: A prepares first on a peer, B prepares and publishes here, and A's
/// rename lands last. Both `Add`s are in the pass's batch. The consolidator must list the fill
/// whose identity is the file at the path, so the read is a hit. The bytes cannot be wrong
/// here (both files are one version); what the listing rule decides is hit or miss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item2_same_version_fills_inverted_by_rename_are_listed_by_the_file_at_the_path() {
    bounded(async {
        item2_same_version_case(true).await;
        item2_same_version_case(false).await;
    })
    .await;
}

/// `a_renames_last`: A's file is at the path (A's rename landed after B's publish); otherwise
/// B's is (A's rename never landed over it).
async fn item2_same_version_case(a_renames_last: bool) {
    {
        let key = &format!("test-bucket/scn1/item2-same-version-{}.bin", a_renames_last);
        let cfg = config(1_048_576, Duration::from_secs(3600));
        let fixture = Fixture::new(Arc::clone(&cfg)).await;
        let peer = Fixture::new(Arc::clone(&cfg)).await;
        fill(&peer, key, V1, 0, LEN - 1).await;
        let mut a_spec = pending(&peer, key, 0, LEN - 1).await.remove(0);
        restamp_pending_fill(&peer, &mut a_spec);
        tokio::time::sleep(Duration::from_millis(20)).await;
        fill(&fixture, key, V1, 0, LEN - 1).await;
        let b_spec = pending(&fixture, key, 0, LEN - 1).await.remove(0);
        let path = bin_path(&fixture, &b_spec);
        assert!(
            a_spec.created_at < b_spec.created_at,
            "fixture: A prepared first"
        );
        assert_ne!(
            a_spec.file_identity, b_spec.file_identity,
            "fixture: two files"
        );
        for entry in std::fs::read_dir(
            peer.cache_manager
                .get_cache_dir()
                .join("metadata/_journals"),
        )
        .unwrap()
        {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                std::fs::copy(
                    entry.path(),
                    fixture
                        .cache_manager
                        .get_cache_dir()
                        .join("metadata/_journals")
                        .join(format!("peer-a-{}", entry.file_name().to_string_lossy())),
                )
                .unwrap();
            }
        }
        let at_path = if a_renames_last {
            std::fs::rename(bin_path(&peer, &a_spec), &path).unwrap();
            a_spec.file_identity
        } else {
            b_spec.file_identity
        };
        assert_eq!(identity_at(&path), at_path, "fixture: the file at the path");
        consolidate(&fixture, key).await;
        let spec = listed(&fixture, key)
            .into_iter()
            .next()
            .expect("the extent stays listed");
        let origin = Origin::new(V1);
        let server = fixture.spawn_proxy(origin.clone()).await;
        let served = get(server.addr, key, &format!("bytes=0-{}", LEN - 1)).await;
        let log = origin.log();
        let v = assert_one_labelled_version(&served, 0, LEN - 1, "item2 same version", &log);
        assert_eq!(v, V1);
        assert!(
            origin.requests() == 0,
            "LOST HIT (item2 same version, a_renames_last={}): the listing names {:?}, the file \
             at the path is {:?}, so the read refused it and went to S3: {:?}",
            a_renames_last,
            spec.file_identity,
            identity_at(&path),
            log
        );
    }
}

// ── a stale RAM snapshot and a peer's newer `.meta` ──────────────────

const SMALL: u64 = 4096;
const OLD: u8 = b'A';
const NEW: u8 = b'B';

fn fresh(extents: Vec<(u64, u64)>, etag: &str) -> SeedSpec {
    let mut spec = SeedSpec::expired(extents, SMALL, etag);
    spec.stored_expiry = StoredExpiry::Fresh;
    spec.created_age = Duration::from_secs(1);
    spec
}

/// S3 serving the newer object (`"v2"`, all `NEW`) for any request.
fn new_version_origin() -> Arc<StubS3Client> {
    Arc::new(
        StubS3Client::new().with_default(
            common::StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from(vec![NEW; SMALL as usize]))
                .with_header("content-range", format!("bytes 0-{}/{}", SMALL - 1, SMALL))
                .with_header("etag", "\"v2\"")
                .with_header("last-modified", LAST_MODIFIED),
        ),
    )
}

/// This proxy holds a RAM snapshot of V1 (one whole-object extent, `OLD`). A peer's write
/// replaces the shared `.meta` with V2 listing two halves (`NEW`) inside the RAM metadata
/// window, and V1's file is still on the volume. A client's GET must get one version, labelled
/// as itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item3_a_peer_overwrite_inside_the_ram_window_is_served_as_one_labelled_version() {
    bounded(async {
        let mut c = Config::default();
        c.cache.download_coordination.enabled = false;
        c.cache.ram_cache_enabled = false;
        c.cache.full_object_check_threshold = 1;
        let fixture = Fixture::new(Arc::new(c)).await;
        let key = "test-bucket/scn1/item3.bin";
        fixture.seed(key, &fresh(vec![(0, SMALL - 1)], "\"v1\""));
        fixture.invalidate_metadata_cache(key).await;
        let snapshot = fixture
            .cache_manager
            .get_metadata_cached(key)
            .await
            .expect("read")
            .expect("seeded");
        assert_eq!(snapshot.object_metadata.etag, "\"v1\"", "fixture");
        // The peer's write: V2's `.meta` and files, written directly so nothing local evicts
        // this proxy's RAM copy.
        fixture.seed(key, &fresh(vec![(0, 2047), (2048, SMALL - 1)], "\"v2\""));
        for (s, e) in [(0, 2047), (2048, SMALL - 1)] {
            std::fs::write(fixture.bin_path(key, s, e), vec![NEW; (e - s + 1) as usize]).unwrap();
        }
        assert!(
            fixture.bin_path(key, 0, SMALL - 1).exists(),
            "fixture: V1's file is still on the volume"
        );
        assert_eq!(
            fixture
                .cache_manager
                .get_metadata_cache()
                .peek(key)
                .await
                .map(|m| m.object_metadata.etag)
                .as_deref(),
            Some("\"v1\""),
            "PRECONDITION: the request starts inside the RAM metadata window (snapshot V1, \
             shared .meta V2)"
        );
        let origin = new_version_origin();
        let server = fixture.spawn_proxy(origin.clone()).await;
        let served = get(server.addr, key, &format!("bytes=0-{}", SMALL - 1)).await;
        eprintln!(
            "SCN1 item3: status {}, ETag {:?}, x-cache {:?}, body {}, upstream requests {}",
            served.status,
            served.etag(),
            served.x_cache(),
            common::expired_fixture::fill_summary(&served.body),
            origin.captured().len()
        );
        let etag = served.etag().map(|e| e.trim_matches('"').to_string());
        let all =
            |b: u8| served.body.len() == SMALL as usize && served.body.iter().all(|x| *x == b);
        let consistent = match etag.as_deref() {
            Some("v1") => all(OLD),
            Some("v2") => all(NEW),
            _ => false,
        };
        assert!(
            served.status == StatusCode::PARTIAL_CONTENT && !served.body_error && consistent,
            "WRONG BYTES SERVED (item3): status {}, ETag {:?}, x-cache {:?}, body {}; upstream \
             requests {}",
            served.status,
            served.etag(),
            served.x_cache(),
            common::expired_fixture::fill_summary(&served.body),
            origin.captured().len()
        );
    })
    .await;
}

// ── a damaged range file ─────────────────────────────────────────────

/// Damage `path` in place the way a volume does: flip bytes in the middle, keep the length and
/// the modification time, so the listing's identity still matches and only the decode can tell.
fn damage_in_place(path: &Path) {
    let mtime = filetime::FileTime::from_last_modification_time(&std::fs::metadata(path).unwrap());
    let mut bytes = std::fs::read(path).unwrap();
    let mid = bytes.len() / 2;
    for b in &mut bytes[mid..mid + 8] {
        *b ^= 0xA5;
    }
    std::fs::write(path, &bytes).unwrap();
    filetime::set_file_mtime(path, mtime).unwrap();
}

/// A listed range whose file is damaged. The first GET is answered without serving the
/// damaged bytes (a buffered serve fetches from S3; a streamed serve has sent its head and
/// breaks off), and the read heals the range, so the next GET serves the right bytes.
async fn item4_listed_case(threshold: u64, what: &'static str) -> (Served, Served, Vec<String>) {
    let key = format!("test-bucket/scn1/item4-{}.bin", what.replace(' ', "-"));
    let range = format!("bytes=0-{}", LEN - 1);
    let cfg = config(threshold, Duration::from_secs(3600));
    let fixture = Fixture::new(Arc::clone(&cfg)).await;
    fill(&fixture, &key, V1, 0, LEN - 1).await;
    consolidate(&fixture, &key).await;
    let spec = listed(&fixture, &key).into_iter().next().expect("listed");
    let path = bin_path(&fixture, &spec);
    damage_in_place(&path);
    assert_eq!(
        identity_at(&path),
        spec.file_identity,
        "fixture: the damage keeps the identity, so the decode is the detector"
    );
    let origin = Origin::new(V1);
    let server = fixture.spawn_proxy(origin.clone()).await;
    let first = get(server.addr, &key, &range).await;
    let second = get(server.addr, &key, &range).await;
    let log = origin.log();
    assert!(
        !(first.status == StatusCode::PARTIAL_CONTENT
            && !first.body_error
            && first.body.len() == LEN as usize
            && version_at(&first.body, 0).is_none()),
        "WRONG BYTES SERVED ({} first GET): a complete 206 of damaged bytes; upstream {:?}",
        what,
        log
    );
    let v = assert_one_labelled_version(&second, 0, LEN - 1, &format!("{} second GET", what), &log);
    assert_eq!(v, V1);
    (first, second, log)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item4_a_damaged_listed_range_is_refetched_buffered_and_healed() {
    bounded(async {
        let (first, _second, log) = item4_listed_case(1_048_576, "item4 buffered").await;
        let v = assert_one_labelled_version(&first, 0, LEN - 1, "item4 buffered first GET", &log);
        assert_eq!(
            v, V1,
            "the buffered serve refetches the damaged range from S3"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item4_a_damaged_listed_range_breaks_one_streamed_response_and_is_healed() {
    bounded(async {
        let (first, second, log) = item4_listed_case(STREAM_AT, "item4 streamed").await;
        eprintln!(
            "SCN1 item4 streamed first GET: status {}, {} bytes, body_error {}, x-cache {:?}; \
             second GET: status {}, x-cache {:?}; upstream {:?}",
            first.status,
            first.body.len(),
            first.body_error,
            first.x_cache(),
            second.status,
            second.x_cache(),
            log
        );
    })
    .await;
}

/// A damaged range listed only by a pending journal `Add`: the key's `.meta` lists another
/// extent, so the lookup consults the journals for this one. Two GETs inside one consolidation
/// interval (no pass runs between them), then a pass and a third GET.
async fn item4_pending_case(threshold: u64, what: &'static str) -> Vec<Served> {
    let key = format!("test-bucket/scn1/item4p-{}.bin", what.replace(' ', "-"));
    let (s, e) = (100_000u64, 199_999u64);
    let range = format!("bytes={}-{}", s, e);
    let cfg = config(threshold, Duration::from_secs(3600));
    let fixture = Fixture::new(Arc::clone(&cfg)).await;
    fill(&fixture, &key, V1, 0, 99_999).await;
    consolidate(&fixture, &key).await;
    fill(&fixture, &key, V1, s, e).await;
    let spec = pending(&fixture, &key, s, e)
        .await
        .into_iter()
        .next()
        .expect("fixture: the second extent is journal-pending");
    assert!(
        listed(&fixture, &key).iter().all(|r| r.start != s),
        "fixture: the damaged extent is not listed in the .meta"
    );
    damage_in_place(&bin_path(&fixture, &spec));
    let origin = Origin::new(V1);
    let server = fixture.spawn_proxy(origin.clone()).await;
    let mut out = Vec::new();
    out.push(get(server.addr, &key, &range).await);
    out.push(get(server.addr, &key, &range).await);
    consolidate(&fixture, &key).await;
    out.push(get(server.addr, &key, &range).await);
    out.push(get(server.addr, &key, &range).await);
    for (i, served) in out.iter().enumerate() {
        eprintln!(
            "SCN1 {} GET {}: status {}, {} bytes, body_error {}, version {}, ETag {:?}, x-cache \
             {:?}",
            what,
            i + 1,
            served.status,
            served.body.len(),
            served.body_error,
            label(version_at(&served.body, s)),
            served.etag(),
            served.x_cache()
        );
        assert!(
            !(served.status == StatusCode::PARTIAL_CONTENT
                && !served.body_error
                && served.body.len() == (e - s + 1) as usize
                && version_at(&served.body, s).is_none()),
            "WRONG BYTES SERVED ({} GET {}): a complete 206 of damaged bytes",
            what,
            i + 1
        );
    }
    eprintln!("SCN1 {} upstream: {:?}", what, origin.log());
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item4_a_damaged_pending_only_range_is_served_buffered_from_s3() {
    bounded(async {
        let out = item4_pending_case(1_048_576, "item4 pending buffered").await;
        for (i, served) in out.iter().enumerate() {
            let v = assert_one_labelled_version(
                served,
                100_000,
                199_999,
                &format!("item4 pending buffered GET {}", i + 1),
                &[],
            );
            assert_eq!(v, V1);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item4_a_damaged_pending_only_range_streamed() {
    bounded(async {
        let out = item4_pending_case(STREAM_AT, "item4 pending streamed").await;
        let last = out.last().unwrap();
        let v = assert_one_labelled_version(
            last,
            100_000,
            199_999,
            "item4 pending streamed GET after a pass",
            &[],
        );
        assert_eq!(v, V1);
    })
    .await;
}

/// DEFECT, fixed by the change: a damaged range listed only by a pending journal `Add`
/// is not healed by the read that finds it (`heal_damaged_range` returns `NotListed`), but that
/// read marks the file it found damaged, so every later read on this proxy refuses that file
/// before its head and is served from S3. Only the first streamed read breaks: GET 2 (inside
/// the same consolidation interval) and GETs 3 and 4 (after a pass lists the `Add`) each get
/// the whole range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn item4_defect_a_damaged_pending_only_range_breaks_only_one_streamed_read() {
    bounded(async {
        let out = item4_pending_case(STREAM_AT, "item4 pending streamed defect").await;
        for (i, served) in out.iter().enumerate().skip(1) {
            let v = assert_one_labelled_version(
                served,
                100_000,
                199_999,
                &format!(
                    "item4 pending streamed: GET {} after the one that found the damage{}",
                    i + 1,
                    if i == 1 {
                        ", before any consolidation pass"
                    } else {
                        ", after a pass"
                    }
                ),
                &[],
            );
            assert_eq!(v, V1);
        }
    })
    .await;
}
