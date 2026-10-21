//! Is a GET-miss fill servable from cache on the SAME instance that filled it,
//! before any journal consolidation cycle has run?
//!
//! Unit: `GET-MISS-FILL-NOT-SERVABLE-ON-THE-FILLING-PROXY-FOR-5-TO-8-S`
//!
//! # What the fleet measured
//!
//! 2026-09-24: an 8 MiB object seeded direct to
//! S3, then GET through one proxy every ~1.35 s. The first cache hit on that same
//! proxy came at +5.44 s and +8.18 s; every earlier GET was a full miss forwarded
//! to S3. The fleet's `consolidation_interval` is 5 s.
//!
//! # What this test does
//!
//! Serves the real `HttpProxy::handle_request` on loopback against a
//! `StubS3Client` (the `expired_fixture` harness), with **no** background
//! consolidator: `main.rs` spawns that task, and this harness never does, so the
//! consolidator has not run unless a test calls `run_consolidation_cycle`
//! explicitly.
//!
//! 1. GET #1, a whole-object miss that fills.
//! 2. Wait on a POSITIVE condition: a `.bin` for the key exists on disk (the fill
//!    has landed its data), bounded at 5 s.
//! 3. GET #2 for the same key through the same instance.
//!
//! The predicate is the stub's captured upstream request count: a GET served from
//! cache sends nothing upstream. Bytes are correct either way and are NOT the
//! predicate. `x-cache` and `.meta` presence are printed as corroboration.
//!
//! # What it showed (first run, 2026-09-24, on the tree then): ALL THREE PASS
//!
//! GET #2 was a HIT with zero further upstream requests, and the `.meta` was
//! already on disk before GET #2, with no consolidation cycle having run. So the
//! in-process harness does NOT reproduce the fleet's 5-8 s window.
//!
//! That is a finding, and the likely reason is the stub, not the product: the
//! shared `StubS3Client` always returns `S3ResponseBody::Buffered`, so GET #1
//! takes the BUFFERED fill path, which wrote the `.meta` synchronously here. An
//! 8 MiB GET against real S3 returns a streaming body and fills through
//! `DiskCacheManager::commit_incremental_range`, which writes only a journal entry
//! (`WriteMode::JournalOnly`).
//!
//! # Second run, with a STREAMING upstream body: REPRODUCED
//!
//! `StreamingStub` below returns `S3ResponseBody::Streaming`. With it, GET #2 goes
//! upstream (`upstream_after_second=2`, no `x-cache`), the `.meta` is absent and
//! the journal entry for the key is present. One explicit `run_consolidation_cycle`
//! between the two GETs turns GET #2 into a HIT. So a streamed fill is invisible
//! to its own instance until the consolidator folds the journal entry into the
//! `.meta`, and the fleet's window is the time to the next consolidation cycle.
//! The two reproducing tests were `#[ignore]`d while the defect was open; the earlier fill-visibility
//! tracker (`src/fill_visibility_tracker.rs`) closed it on the filling instance and they now run
//! in the gate.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    proxy_get_with_auth, signed_authorization_without_range, test_config_full_object_checks,
    Fixture,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::config::Config;
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::{S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// An `S3ClientApi` that returns `S3ResponseBody::Streaming`, which the shared
/// `StubS3Client` cannot: `Incoming` has no public constructor. Each
/// `forward_request` makes a real loopback HTTP/1 request to a tiny origin
/// serving the object, and hands back the response's `Incoming` body. The client
/// connection task is spawned and lives until the body is drained, so the body
/// keeps yielding (`testing.md` → "A real `Incoming` needs its connection kept
/// alive for the whole body" — here the CLIENT side owns the driver).
///
/// Everything except `forward_request` delegates to a `StubS3Client`, so header
/// and metadata extraction match the buffered tests exactly.
#[derive(Clone)]
struct StreamingStub {
    origin: std::net::SocketAddr,
    calls: Arc<AtomicUsize>,
    inner: StubS3Client,
}

const ETAG: &str = "\"fill-visibility-v1\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

impl StreamingStub {
    async fn start(data: Vec<u8>) -> Self {
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind origin");
        let origin = listener.local_addr().expect("origin addr");
        let data = Bytes::from(data);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let data = data.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |_req: hyper::Request<hyper::body::Incoming>| {
                        let data = data.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(
                                hyper::Response::builder()
                                    .status(200)
                                    .header("content-length", data.len().to_string())
                                    .header("etag", ETAG)
                                    .header("last-modified", LAST_MODIFIED)
                                    .header("content-type", "application/octet-stream")
                                    .body(http_body_util::Full::new(data))
                                    .expect("origin response"),
                            )
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self {
            origin,
            calls: Arc::new(AtomicUsize::new(0)),
            inner: StubS3Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl S3ClientApi for StreamingStub {
    async fn forward_request(&self, _context: S3RequestContext) -> s3_proxy::Result<S3Response> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let stream = tokio::net::TcpStream::connect(self.origin)
            .await
            .expect("connect origin");
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .expect("origin handshake");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = hyper::Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "origin")
            .body(http_body_util::Empty::<Bytes>::new())
            .expect("origin request");
        let resp = sender.send_request(req).await.expect("origin response");
        let status = resp.status();
        let headers: HashMap<String, String> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        Ok(S3Response {
            status,
            headers,
            body: Some(S3ResponseBody::Streaming(resp.into_body())),
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

    async fn refresh_dns(&self) -> s3_proxy::Result<()> {
        Ok(())
    }
}

/// Same size as the fleet measurement.
const OBJECT_SIZE: usize = 8 * 1024 * 1024;
const BUCKET: &str = "test-bucket";

fn body() -> Vec<u8> {
    (0..OBJECT_SIZE).map(|i| (i % 251) as u8).collect()
}

fn stub_for(body: &[u8]) -> StubS3Client {
    StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::OK)
            .with_body(Bytes::from(body.to_vec()))
            .with_header("etag", "\"fill-visibility-v1\"")
            .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .with_header("content-type", "application/octet-stream"),
    )
}

fn find_bins(dir: &Path, needle: &str, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            find_bins(&p, needle, out);
        } else if p.extension().is_some_and(|e| e == "bin") && p.to_string_lossy().contains(needle)
        {
            out.push(p);
        }
    }
}

/// Wait for the fill's range data to land, on a positive condition.
async fn wait_for_bin(fixture: &Fixture, object: &str) -> (bool, Duration) {
    let start = Instant::now();
    let ranges = fixture.cache_manager.get_cache_dir().join("ranges");
    while start.elapsed() < Duration::from_secs(5) {
        let mut bins = Vec::new();
        find_bins(&ranges, object, &mut bins);
        if !bins.is_empty() {
            return (true, start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (false, start.elapsed())
}

struct Outcome {
    upstream_after_first: usize,
    upstream_after_second: usize,
    second_x_cache: Option<String>,
    meta_before_second: bool,
    bin_seen: bool,
    bin_wait: Duration,
}

impl Outcome {
    fn describe(&self) -> String {
        format!(
            "upstream_after_first={} upstream_after_second={} second_x_cache={:?} \
             meta_on_disk_before_second={} bin_seen={} bin_wait={:?}",
            self.upstream_after_first,
            self.upstream_after_second,
            self.second_x_cache,
            self.meta_before_second,
            self.bin_seen,
            self.bin_wait,
        )
    }
}

/// Which upstream body shape the stub returns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BodyShape {
    /// `S3ResponseBody::Buffered` — what the shared `StubS3Client` always returns.
    Buffered,
    /// `S3ResponseBody::Streaming(Incoming)` — what the real `S3Client` returns for
    /// a GET (`build_s3_request_context` sets `allow_streaming: true`).
    Streaming,
}

enum Upstream {
    Buffered(StubS3Client),
    Streaming(StreamingStub),
}

impl Upstream {
    fn count(&self) -> usize {
        match self {
            Upstream::Buffered(s) => s.captured().len(),
            Upstream::Streaming(s) => s.calls.load(Ordering::SeqCst),
        }
    }
    fn client(&self) -> Arc<dyn S3ClientApi + Send + Sync> {
        match self {
            Upstream::Buffered(s) => s.clone().into_trait_object(),
            Upstream::Streaming(s) => Arc::new(s.clone()),
        }
    }
}

/// True once this instance's journal holds an entry naming `cache_key`. The
/// streaming fill publishes the `.bin` BEFORE appending the journal entry
/// (`commit_incremental_range`), so a `.bin` alone does not prove the entry exists.
async fn wait_for_journal_entry(fixture: &Fixture, cache_key: &str) -> bool {
    let dir = fixture
        .cache_manager
        .get_cache_dir()
        .join("metadata")
        .join("_journals");
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.to_string_lossy().ends_with(".journal") {
                    if let Ok(s) = std::fs::read_to_string(&p) {
                        if s.contains(cache_key) {
                            return true;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// GET, fill, optionally consolidate, GET again. Returns what was observed.
async fn run(config: Arc<Config>, object: &str, consolidate_between: bool) -> Outcome {
    run_with(config, object, consolidate_between, BodyShape::Buffered).await
}

async fn run_with(
    config: Arc<Config>,
    object: &str,
    consolidate_between: bool,
    shape: BodyShape,
) -> Outcome {
    let fixture = Fixture::new(config).await;
    let key = format!("{}/{}", BUCKET, object);
    let path = format!("/{}", key);
    let data = body();
    let upstream = match shape {
        BodyShape::Buffered => Upstream::Buffered(stub_for(&data)),
        BodyShape::Streaming => Upstream::Streaming(StreamingStub::start(data.clone()).await),
    };
    let server = fixture.spawn_proxy(upstream.client()).await;
    let auth = signed_authorization_without_range();

    let (s1, _h1, b1) = proxy_get_with_auth(server.addr, &path, &auth, &[]).await;
    assert_eq!(s1, StatusCode::OK, "GET #1 status");
    assert_eq!(b1.len(), OBJECT_SIZE, "GET #1 length");
    let upstream_after_first = upstream.count();

    let (bin_seen, bin_wait) = wait_for_bin(&fixture, object).await;
    // Only the streaming fill journals; the buffered fill writes the `.meta` directly.
    let journal_seen =
        shape == BodyShape::Streaming && wait_for_journal_entry(&fixture, &key).await;
    // The fill-visibility record lands after the journal append, in the same spawned task, so
    // the journal entry alone does not prove it is in place yet. Wait on it positively; bounded,
    // so a build without the tracker times out here and then fails on the predicate below.
    let tracker_recorded = shape == BodyShape::Streaming && wait_for_recorded(&fixture, 1).await;
    eprintln!(
        "{} shape={:?}: journal entry for key present before GET #2 = {}, tracker recorded = {}",
        object, shape, journal_seen, tracker_recorded
    );

    if consolidate_between {
        let consolidator = fixture
            .cache_manager
            .get_journal_consolidator()
            .await
            .expect("shared-storage consolidator present");
        consolidator
            .run_consolidation_cycle()
            .await
            .expect("consolidation cycle");
    }

    let meta_before_second = fixture.meta_path(&key).exists();

    let (s2, h2, b2) = proxy_get_with_auth(server.addr, &path, &auth, &[]).await;
    assert_eq!(s2, StatusCode::OK, "GET #2 status");
    assert_eq!(
        b2, data,
        "GET #2 bytes (correct either way; not the predicate)"
    );

    let outcome = Outcome {
        upstream_after_first,
        upstream_after_second: upstream.count(),
        second_x_cache: h2
            .get("x-cache")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        meta_before_second,
        bin_seen,
        bin_wait,
    };
    // Printed on pass as well, so a pass records WHICH state it passed in.
    eprintln!(
        "{} consolidate_between={}: {}",
        object,
        consolidate_between,
        outcome.describe()
    );
    outcome
}

/// Fleet-like config: the shipped defaults, so download coordination and the RAM
/// tier are ON as they are on the fleet. Only `full_object_check_threshold` is
/// raised so an 8 MiB whole-object GET takes the full-object path.
fn fleet_like_config() -> Arc<Config> {
    let mut config = Config::default();
    config.cache.full_object_check_threshold = (OBJECT_SIZE as u64) * 16;
    Arc::new(config)
}

/// The fleet's shape: no consolidation between the fill and the re-read.
///
/// RAM tier and download coordination off (the harness's confound-free config),
/// so the second GET's verdict comes from the disk/metadata path the fleet's
/// first hits came from.
#[tokio::test]
async fn get_miss_fill_is_servable_on_same_instance_before_consolidation() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let o = run(config, "fill-vis-no-consolidation.bin", false).await;
    assert_eq!(
        o.upstream_after_first,
        1,
        "GET #1 must go upstream once. {}",
        o.describe()
    );
    assert_eq!(
        o.upstream_after_second,
        1,
        "GET #2 went upstream: the fill on this instance was NOT servable before a \
         consolidation cycle ran. {}",
        o.describe()
    );
}

/// Same, with shipped defaults (RAM tier and download coordination on).
#[tokio::test]
async fn get_miss_fill_is_servable_on_same_instance_before_consolidation_fleet_like_config() {
    let o = run(
        fleet_like_config(),
        "fill-vis-no-consolidation-fleet.bin",
        false,
    )
    .await;
    assert_eq!(
        o.upstream_after_first,
        1,
        "GET #1 must go upstream once. {}",
        o.describe()
    );
    assert_eq!(
        o.upstream_after_second,
        1,
        "GET #2 went upstream: the fill on this instance was NOT servable before a \
         consolidation cycle ran (fleet-like config). {}",
        o.describe()
    );
}

/// The control: one explicit consolidation cycle between fill and re-read. If
/// this passes where the first test fails, the lookup depends on consolidation.
#[tokio::test]
async fn get_miss_fill_is_servable_on_same_instance_after_one_consolidation_cycle() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let o = run(config, "fill-vis-after-consolidation.bin", true).await;
    assert_eq!(
        o.upstream_after_first,
        1,
        "GET #1 must go upstream once. {}",
        o.describe()
    );
    assert_eq!(
        o.upstream_after_second,
        1,
        "GET #2 went upstream even after a consolidation cycle. {}",
        o.describe()
    );
}

/// STREAMING body, no consolidation: the fleet's actual shape.
///
/// Reproduced the fleet defect in-process and was `#[ignore]`d while it was open. Measured on
/// the tree at the time, and again on a later one: `upstream_after_second=2 second_x_cache=None
/// meta_on_disk_before_second=false`, with the journal entry for the key present. Passes since
/// the fill-visibility tracker, which serves the fill from this instance's record of it
/// until consolidation writes the `.meta`.
#[tokio::test]
async fn streaming_get_miss_fill_is_servable_on_same_instance_before_consolidation() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let o = run_with(
        config,
        "fill-vis-stream-no-consolidation.bin",
        false,
        BodyShape::Streaming,
    )
    .await;
    assert_eq!(
        o.upstream_after_first,
        1,
        "GET #1 must go upstream once. {}",
        o.describe()
    );
    assert_eq!(
        o.upstream_after_second,
        1,
        "GET #2 went upstream: a STREAMED fill on this instance was NOT servable before a \
         consolidation cycle ran. {}",
        o.describe()
    );
}

/// STREAMING body, shipped defaults (download coordination and RAM tier on).
/// Reproduced the defect exactly as the test above did; passes since the change for the same reason.
#[tokio::test]
async fn streaming_get_miss_fill_is_servable_on_same_instance_before_consolidation_fleet_like_config(
) {
    let o = run_with(
        fleet_like_config(),
        "fill-vis-stream-no-consolidation-fleet.bin",
        false,
        BodyShape::Streaming,
    )
    .await;
    assert_eq!(
        o.upstream_after_first,
        1,
        "GET #1 must go upstream once. {}",
        o.describe()
    );
    assert_eq!(
        o.upstream_after_second,
        1,
        "GET #2 went upstream: a STREAMED fill on this instance was NOT servable before a \
         consolidation cycle ran (fleet-like config). {}",
        o.describe()
    );
}

/// STREAMING body, one explicit consolidation cycle between: the control. PASSES:
/// the same fill becomes a HIT once `run_consolidation_cycle` has folded the
/// journal entry into a `.meta`, so the two ignored tests above fail on the
/// consolidation dependency and not on a broken fixture.
#[tokio::test]
async fn streaming_get_miss_fill_is_servable_on_same_instance_after_one_consolidation_cycle() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let o = run_with(
        config,
        "fill-vis-stream-after-consolidation.bin",
        true,
        BodyShape::Streaming,
    )
    .await;
    assert_eq!(
        o.upstream_after_first,
        1,
        "GET #1 must go upstream once. {}",
        o.describe()
    );
    assert_eq!(
        o.upstream_after_second,
        1,
        "GET #2 went upstream even after a consolidation cycle (streamed fill). {}",
        o.describe()
    );
}

// ---------------------------------------------------------------------------------------------
// The fill-visibility RAM tracker's guards.
//
// The tracker (`src/fill_visibility_tracker.rs`) lets the filling proxy serve a streamed fill
// before the consolidator has written its `.meta`. The four tests below pin what it must NOT do.
// Written and committed before the tracker was wired into the read path (commit order in
// the investigation record); the first three
// pass on that tree because nothing consults the tracker there, and are guards against a fix that
// consults it too eagerly. The fourth needs the tracker to record, so it is red there.
// ---------------------------------------------------------------------------------------------

/// Wait, on a positive condition bounded at 5 s, for this instance's tracker to have recorded at
/// least `n` fills in total. Returns whether it did.
///
/// The tracker records after the journal append, in the fill's spawned cache task, so neither the
/// `.bin` nor the journal entry proves the entry exists yet.
async fn wait_for_recorded(fixture: &Fixture, n: u64) -> bool {
    let tracker = fixture.cache_manager.fill_visibility_tracker();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if tracker.stats().recorded_total >= n {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

/// A streamed fill of `object` through `server`, waited on until its `.bin`, its journal entry
/// and (when `expect_recorded`) its tracker entry are all in place.
async fn streamed_fill(
    fixture: &Fixture,
    server: &common::expired_fixture::ProxyServer,
    object: &str,
    recorded_before: u64,
    expect_recorded: bool,
) {
    let key = format!("{}/{}", BUCKET, object);
    let path = format!("/{}", key);
    let auth = signed_authorization_without_range();
    let (s, _h, b) = proxy_get_with_auth(server.addr, &path, &auth, &[]).await;
    assert_eq!(s, StatusCode::OK, "fill GET status for {}", object);
    assert_eq!(b.len(), OBJECT_SIZE, "fill GET length for {}", object);
    let (bin_seen, _) = wait_for_bin(fixture, object).await;
    let journal_seen = wait_for_journal_entry(fixture, &key).await;
    let recorded = if expect_recorded {
        wait_for_recorded(fixture, recorded_before + 1).await
    } else {
        false
    };
    eprintln!(
        "{}: fill landed bin_seen={} journal_seen={} tracker_recorded={}",
        object, bin_seen, journal_seen, recorded
    );
}

/// A PEER's `.meta` for the same key, carrying a different ETag, is on shared storage when the
/// re-read arrives. Rule 1: the shared `.meta` governs, so the response must not be the tracker's
/// version (this proxy's fill, ETag `fill-visibility-v1`) served from cache.
///
/// The peer entry is laid down with `Fixture::seed`, directly on disk, the way another instance's
/// write appears (`cache-coherency-invariants.md` § "Construct the cross-instance test so local
/// hygiene cannot pass it"). It is fresh and fully covering, so on a correct build the re-read is
/// served from the PEER's entry: its ETag, its bytes.
#[tokio::test]
async fn a_peer_meta_with_a_different_etag_wins_over_this_proxys_fill() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let fixture = Fixture::new(config).await;
    let object = "fill-vis-peer-meta.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    streamed_fill(&fixture, &server, object, 0, true).await;

    let peer_etag = "\"peer-written-v2\"";
    fixture.seed(
        &key,
        &common::expired_fixture::SeedSpec {
            extents: vec![(0, OBJECT_SIZE as u64 - 1)],
            etag: peer_etag.to_string(),
            last_modified: LAST_MODIFIED.to_string(),
            content_length: OBJECT_SIZE as u64,
            stored_expiry: common::expired_fixture::StoredExpiry::Fresh,
            created_age: Duration::from_secs(0),
            is_write_cached: Some(false),
        },
    );

    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    let etag2 = h2
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let served_locally = after == before;
    eprintln!(
        "peer-meta re-read: status={} served_locally={} etag={:?} body_is_this_proxys_fill={} \
         tracker={:?}",
        s2,
        served_locally,
        etag2,
        b2 == data,
        fixture.cache_manager.fill_visibility_tracker().stats()
    );
    assert_eq!(s2, StatusCode::OK);
    assert!(
        !(served_locally && (b2 == data || etag2.contains("fill-visibility-v1"))),
        "the re-read was served locally as THIS proxy's fill (etag {:?}) although a peer's `.meta` \
         with ETag {} was on shared storage: the tracker was consulted over the shared truth",
        etag2,
        peer_etag
    );
    if served_locally {
        assert!(
            etag2.contains("peer-written-v2"),
            "served locally, so it must be the peer's entry; got etag {:?}",
            etag2
        );
    }
}

/// `get_ttl: "0s"` means every read revalidates against S3 (the fleet suite's guarantee, an authorization
/// boundary). A streamed fill under it must not become a local hit before or after the fold.
#[tokio::test]
async fn a_get_ttl_zero_fill_is_not_a_local_hit_on_reread() {
    let config = common::expired_fixture::test_config_full_object_checks_with_get_ttl(
        OBJECT_SIZE as u64,
        Duration::ZERO,
    );
    let fixture = Fixture::new(config).await;
    let object = "fill-vis-ttl-zero.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    streamed_fill(&fixture, &server, object, 0, false).await;

    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    eprintln!(
        "ttl-zero re-read: status={} upstream {} -> {} x_cache={:?} tracker={:?}",
        s2,
        before,
        after,
        h2.get("x-cache"),
        fixture.cache_manager.fill_visibility_tracker().stats()
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, data, "bytes (correct either way; not the predicate)");
    assert_eq!(
        after,
        before + 1,
        "a get_ttl 0s key was answered without contacting S3"
    );
    assert!(
        h2.get("x-cache").is_none(),
        "a get_ttl 0s re-read carried x-cache {:?}",
        h2.get("x-cache")
    );
}

/// A DELETE of the key on this proxy between the fill and the re-read must make the re-read a
/// miss.
///
/// The DELETE is modelled by the invalidation call the signed-DELETE handler makes on a successful
/// S3 response (`invalidate_cache_unified_for_operation(&cache_key, "DELETE")`,
/// `http_proxy.rs:5225`), not by a DELETE request through the loopback proxy: the signed DELETE
/// forwards on its own transport to the request's host, not through `S3ClientApi`, so in this
/// harness it cannot reach a stub and never succeeds.
#[tokio::test]
async fn a_delete_on_this_proxy_between_fill_and_reread_is_a_miss() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let fixture = Fixture::new(config).await;
    let object = "fill-vis-delete.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    streamed_fill(&fixture, &server, object, 0, true).await;

    fixture
        .cache_manager
        .invalidate_cache_unified_for_operation(&key, "DELETE")
        .await
        .expect("DELETE invalidation");

    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    eprintln!(
        "post-DELETE re-read: status={} upstream {} -> {} x_cache={:?} tracker={:?}",
        s2,
        before,
        after,
        h2.get("x-cache"),
        fixture.cache_manager.fill_visibility_tracker().stats()
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, data);
    assert_eq!(
        after,
        before + 1,
        "the re-read after this proxy invalidated the key for a DELETE was answered from cache"
    );
}

/// Filling past the tracker's cap drops the OLDEST entry; that key's re-read is a plain miss (a
/// correct `200` from S3, not an error), and the newer key is still served locally.
///
/// The cap is lowered to one entry through `set_caps` so two fills reach it. Red on a tree where
/// the tracker exists but nothing records into it: `dropped_cap_total` stays 0.
///
/// The NEWEST key is re-read first, and that order is required, not cosmetic. The oldest key's
/// re-read is an upstream miss, and its detached cache commit records that key again
/// (`commit_incremental_range` -> `record_fill_visibility`); at a one-entry cap the tracker then
/// drops the newest by cap. Re-reading the oldest first therefore races that commit against the
/// newest re-read, and on a machine where the commit wins the newest is a miss (measured
/// 2026-09-30, aarch64 Linux: `dropped_cap_total` 1 -> 2 and `holds(newest)` false before the
/// newest re-read in 5 of 5 probe runs, and the old order failed 20 of 20). The cap state is re-read immediately before the newest re-read,
/// so the local serve is asserted against a tracker that has demonstrably dropped the oldest.
#[tokio::test]
async fn filling_past_the_cap_drops_the_oldest_and_its_reread_is_a_miss() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let fixture = Fixture::new(config).await;
    let tracker = fixture.cache_manager.fill_visibility_tracker();
    tracker.set_caps(1, u64::MAX);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;
    let auth = signed_authorization_without_range();

    let oldest = "fill-vis-cap-oldest.bin";
    let newest = "fill-vis-cap-newest.bin";
    streamed_fill(&fixture, &server, oldest, 0, true).await;
    streamed_fill(&fixture, &server, newest, 1, true).await;
    let oldest_key = format!("{}/{}", BUCKET, oldest);
    let newest_key = format!("{}/{}", BUCKET, newest);
    let stats = tracker.stats();
    let holds_oldest = tracker.holds(&oldest_key);
    let holds_newest = tracker.holds(&newest_key);
    eprintln!(
        "after two fills at cap 1, before any re-read: {:?} holds(oldest)={} holds(newest)={}",
        stats, holds_oldest, holds_newest
    );
    assert_eq!(
        stats.dropped_cap_total, 1,
        "two fills against a one-entry cap must drop exactly one entry by cap: {:?}",
        stats
    );
    assert_eq!(stats.entries, 1, "one entry must remain: {:?}", stats);
    assert!(
        !holds_oldest && holds_newest,
        "the cap must have dropped the OLDEST and kept the newest: holds(oldest)={} \
         holds(newest)={}",
        holds_oldest,
        holds_newest
    );

    // Newest first: see the doc comment.
    let before = upstream.calls.load(Ordering::SeqCst);
    let (s_new, h_new, b_new) =
        proxy_get_with_auth(server.addr, &format!("/{}", newest_key), &auth, &[]).await;
    let after_new = upstream.calls.load(Ordering::SeqCst);
    let (s_old, h_old, b_old) =
        proxy_get_with_auth(server.addr, &format!("/{}", oldest_key), &auth, &[]).await;
    let after_old = upstream.calls.load(Ordering::SeqCst);
    // Wait for the oldest re-read's own detached commit, so it does not outlive the test.
    let refill_recorded = wait_for_recorded(&fixture, stats.recorded_total + 1).await;
    eprintln!(
        "newest re-read: status={} upstream {} -> {} x_cache={:?}; oldest re-read: status={} \
         upstream {} -> {} x_cache={:?}; oldest refill recorded={} tracker={:?}",
        s_new,
        before,
        after_new,
        h_new.get("x-cache"),
        s_old,
        after_new,
        after_old,
        h_old.get("x-cache"),
        refill_recorded,
        tracker.stats()
    );

    assert_eq!(s_new, StatusCode::OK);
    assert_eq!(b_new, data);
    assert_eq!(
        after_new, before,
        "the newest key survived the cap and must be served locally"
    );
    assert_eq!(
        s_old,
        StatusCode::OK,
        "the dropped key's re-read must not error"
    );
    assert_eq!(
        b_old, data,
        "the dropped key's re-read must return the object"
    );
    assert_eq!(
        after_old,
        after_new + 1,
        "the dropped (oldest) key's re-read must be a miss to S3"
    );
}

// ---------------------------------------------------------------------------------------------
// The tracker over a HEAD-only `.meta`.
//
// Measured on the fleet that `aws s3 cp` (CRT) sends HeadObject twice before its GET, so the
// HEAD writes a `.meta` with no ranges (`CacheManager::create_head_only_metadata`) before the fill,
// and the earlier "disk wins" rule then dropped every fill. The change lets the tracked fill stand in for a
// zero-range `.meta` describing the same version. Design note:
// The investigation record § "Design note: the tracker over a HEAD-only
// `.meta`". The first test is red on the tree; the other three are guards that pass there
// because nothing consults the tracker over a `.meta`, and must still pass after the relaxation.
// ---------------------------------------------------------------------------------------------

/// The fill-visibility figures for `fixture`'s instance.
fn tracker_stats(fixture: &Fixture) -> s3_proxy::fill_visibility_tracker::FillVisibilityStats {
    fixture.cache_manager.fill_visibility_tracker().stats()
}

/// HEAD `key` through the proxy and assert, on a positive condition, that it left a `.meta` with
/// no ranges: the state measured before every CLI download's GET.
async fn head_leaves_head_only_meta(
    fixture: &Fixture,
    server: &common::expired_fixture::ProxyServer,
    key: &str,
) {
    let (s, _h, b) =
        common::expired_fixture::proxy_head(server.addr, &format!("/{}", key), &[]).await;
    assert_eq!(s, StatusCode::OK, "HEAD status for {}", key);
    assert!(b.is_empty(), "HEAD returned a body of {} bytes", b.len());
    let start = Instant::now();
    let mut meta = fixture.read_meta(key);
    while meta.is_none() && start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        meta = fixture.read_meta(key);
    }
    let meta = meta.unwrap_or_else(|| {
        panic!(
            "PRECONDITION MISSING: the HEAD of {} wrote no .meta within 5 s, so this test cannot \
             reach the HEAD-only state",
            key
        )
    });
    assert!(
        meta.ranges.is_empty(),
        "PRECONDITION MISSING: the HEAD's .meta for {} already has {} range(s); it must be \
         HEAD-only",
        key,
        meta.ranges.len()
    );
    eprintln!(
        "{}: HEAD-only .meta etag={} content_length={} ranges=0",
        key, meta.object_metadata.etag, meta.object_metadata.content_length
    );
}

/// The `RangeSpec` of this instance's journal line for `key`, i.e. the range the fill committed.
fn journal_range_spec(fixture: &Fixture, key: &str) -> s3_proxy::cache_types::RangeSpec {
    let dir = fixture
        .cache_manager
        .get_cache_dir()
        .join("metadata")
        .join("_journals");
    for e in std::fs::read_dir(&dir).expect("journal dir").flatten() {
        let Ok(text) = std::fs::read_to_string(e.path()) else {
            continue;
        };
        for line in text.lines() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if v.get("cache_key").and_then(|k| k.as_str()) == Some(key) {
                if let Some(rs) = v.get("range_spec") {
                    return serde_json::from_value(rs.clone()).expect("journal range_spec");
                }
            }
        }
    }
    panic!("PRECONDITION MISSING: no journal line names {}", key);
}

/// HEAD, then a streamed GET miss that fills, then an immediate re-read on the same instance: the
/// re-read must be a local hit with this fill's bytes, and must count as a tracker hit.
///
/// RED on the tree before the fix: the HEAD's zero-range `.meta` makes every read site drop the
/// entry as `disk_meta`, so the re-read goes to S3.
#[tokio::test]
async fn head_then_streamed_fill_is_a_local_hit_on_reread() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let fixture = Fixture::new(config).await;
    let object = "fill-vis-head-first.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    head_leaves_head_only_meta(&fixture, &server, &key).await;
    let recorded_before = tracker_stats(&fixture).recorded_total;
    streamed_fill(&fixture, &server, object, recorded_before, true).await;
    assert!(
        fixture.read_meta(&key).is_some_and(|m| m.ranges.is_empty()),
        "PRECONDITION MISSING: the .meta must still be HEAD-only before the re-read (no fold ran)"
    );

    let hits_before = tracker_stats(&fixture).hits_total;
    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    let stats = tracker_stats(&fixture);
    eprintln!(
        "head-first re-read: status={} upstream {} -> {} x_cache={:?} etag={:?} tracker={:?}",
        s2,
        before,
        after,
        h2.get("x-cache"),
        h2.get("etag"),
        stats
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, data, "the re-read must return this fill's bytes");
    assert_eq!(
        after, before,
        "the re-read after HEAD then a streamed fill went to S3: the HEAD-only .meta still hid \
         this proxy's own fill (tracker {:?})",
        stats
    );
    assert_eq!(h2.get("x-cache").and_then(|v| v.to_str().ok()), Some("HIT"));
    assert_eq!(
        stats.hits_total,
        hits_before + 1,
        "the local serve must count exactly one tracker hit: {:?}",
        stats
    );
}

/// A HEAD-only `.meta` describing a DIFFERENT version (a peer's HEAD after the object changed)
/// must not let the tracker serve this proxy's fill.
///
/// The `.meta` is written with `store_head_cache_entry_unified`, the function the HEAD path calls,
/// directly on shared storage, the way a peer's HEAD appears here.
#[tokio::test]
async fn a_head_only_meta_with_a_different_etag_does_not_let_the_tracker_serve() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let fixture = Fixture::new(config).await;
    let object = "fill-vis-head-other-etag.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    streamed_fill(&fixture, &server, object, 0, true).await;

    let mut peer_headers = HashMap::new();
    peer_headers.insert("etag".to_string(), "\"peer-head-v2\"".to_string());
    peer_headers.insert("last-modified".to_string(), LAST_MODIFIED.to_string());
    peer_headers.insert("content-length".to_string(), OBJECT_SIZE.to_string());
    peer_headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    let peer_meta = StubS3Client::new().extract_metadata_from_response(&peer_headers);
    fixture
        .cache_manager
        .store_head_cache_entry_unified(&key, peer_headers, peer_meta)
        .await
        .expect("peer HEAD-only .meta");
    let on_disk = fixture.read_meta(&key).expect("peer .meta");
    assert!(
        on_disk.ranges.is_empty() && on_disk.object_metadata.etag.contains("peer-head-v2"),
        "PRECONDITION MISSING: expected a HEAD-only .meta with the peer's ETag, got ranges={} \
         etag={}",
        on_disk.ranges.len(),
        on_disk.object_metadata.etag
    );

    let hits_before = tracker_stats(&fixture).hits_total;
    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    let stats = tracker_stats(&fixture);
    eprintln!(
        "other-etag re-read: status={} upstream {} -> {} x_cache={:?} tracker={:?}",
        s2,
        before,
        after,
        h2.get("x-cache"),
        stats
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, data, "bytes (correct either way; not the predicate)");
    assert_eq!(
        after,
        before + 1,
        "the tracker served this proxy's fill over a HEAD-only .meta of a different version"
    );
    assert_eq!(
        stats.hits_total, hits_before,
        "no tracker hit may be counted over a mismatched .meta: {:?}",
        stats
    );
}

/// A `.meta` that gained a RANGE from a peer (its own fill of the same version, folded) wins: the
/// entry is dropped and the tracker does not serve, even though identity matches.
///
/// The peer's range is a 1 KiB prefix, so its `.bin` does not collide with this proxy's whole-object
/// `.bin` and the tracker's stamp stays valid: only the ranges-on-disk rule can refuse the entry.
/// With that range the full object is not covered and under 10% cached, so the correct re-read is a
/// plain miss to S3.
#[tokio::test]
async fn a_meta_that_gained_a_range_from_a_peer_wins_over_the_tracker() {
    let config = test_config_full_object_checks(OBJECT_SIZE as u64);
    let fixture = Fixture::new(config).await;
    let object = "fill-vis-head-peer-range.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    head_leaves_head_only_meta(&fixture, &server, &key).await;
    let recorded_before = tracker_stats(&fixture).recorded_total;
    streamed_fill(&fixture, &server, object, recorded_before, true).await;

    fixture.seed(
        &key,
        &common::expired_fixture::SeedSpec {
            extents: vec![(0, 1023)],
            etag: ETAG.to_string(),
            last_modified: LAST_MODIFIED.to_string(),
            content_length: OBJECT_SIZE as u64,
            stored_expiry: common::expired_fixture::StoredExpiry::Fresh,
            created_age: Duration::from_secs(0),
            is_write_cached: Some(false),
        },
    );

    let pre = tracker_stats(&fixture);
    assert_eq!(
        pre.entries, 1,
        "the fill's entry must be held before the re-read: {:?}",
        pre
    );
    let hits_before = pre.hits_total;
    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    // The re-read is a miss, and its detached cache commit records its OWN whole-object fill
    // (`commit_incremental_range` -> `record_fill_visibility`), so the tracker legitimately holds
    // a new entry once that commit lands. Reading `entries` straight after the response raced that
    // commit (measured 2026-09-30, aarch64 Linux: `entries` 0 in 4 of 5 probe runs, 1 in the
    // fifth). Wait for the commit on a positive condition instead, then judge the fill's entry by
    // its drop counter and the new one by the record count.
    let refill_recorded = wait_for_recorded(&fixture, pre.recorded_total + 1).await;
    let stats = tracker_stats(&fixture);
    eprintln!(
        "peer-range re-read: status={} upstream {} -> {} x_cache={:?} refill_recorded={} \
         tracker={:?}",
        s2,
        before,
        after,
        h2.get("x-cache"),
        refill_recorded,
        stats
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, data, "bytes (correct either way; not the predicate)");
    assert_eq!(
        stats.hits_total, hits_before,
        "the tracker was used over a .meta that has a range on disk: {:?}",
        stats
    );
    assert_eq!(
        after,
        before + 1,
        "the re-read must be the miss the ranged .meta implies, not a tracker serve"
    );
    assert_eq!(
        stats.dropped_disk_meta_total,
        pre.dropped_disk_meta_total + 1,
        "the fill's entry must be dropped because a ranged .meta is on disk: {:?}",
        stats
    );
    assert!(
        refill_recorded,
        "the re-read's own fill never recorded: {:?}",
        stats
    );
    assert_eq!(
        (stats.entries, stats.recorded_total),
        (1, pre.recorded_total + 1),
        "the only entry left must be the re-read's own fill, not the dropped one: {:?}",
        stats
    );
}

/// `get_ttl: "0s"` with a HEAD-only `.meta`: never a local hit.
///
/// A fill under a zero TTL is not recorded, so to reach the state where a relaxed read site could
/// serve at all the test records the fill into the tracker itself, as a fill committed under a
/// generous TTL before the key was tightened would be (the `.bin` and the journal line are the
/// fill's own). The re-read must still go to S3 with no `x-cache`.
#[tokio::test]
async fn a_get_ttl_zero_key_with_a_head_only_meta_is_not_a_local_hit() {
    let config = common::expired_fixture::test_config_full_object_checks_with_get_ttl(
        OBJECT_SIZE as u64,
        Duration::ZERO,
    );
    let fixture = Fixture::new(config).await;
    let object = "fill-vis-head-ttl-zero.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    head_leaves_head_only_meta(&fixture, &server, &key).await;
    streamed_fill(&fixture, &server, object, 0, false).await;

    let head_meta = fixture.read_meta(&key).expect("HEAD-only .meta");
    let range = journal_range_spec(&fixture, &key);
    let bin = fixture
        .cache_manager
        .get_cache_dir()
        .join("ranges")
        .join(&range.file_path);
    let stamp = s3_proxy::fill_visibility_tracker::RangeFileStamp::of(&bin)
        .expect("PRECONDITION MISSING: the fill's .bin cannot be stat'ed");
    let now = std::time::SystemTime::now();
    let tracked = s3_proxy::cache_types::NewCacheMetadata {
        cache_key: key.clone(),
        object_metadata: head_meta.object_metadata.clone(),
        ranges: vec![range],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        ..Default::default()
    };
    let marker = s3_proxy::fill_visibility_tracker::MarkerStamp::of(
        &fixture.cache_manager.get_new_metadata_file_path(&key),
    )
    .expect("PRECONDITION MISSING: the key's write marker cannot be read");
    fixture
        .cache_manager
        .fill_visibility_tracker()
        .record(tracked, vec![stamp], marker);

    let hits_before = tracker_stats(&fixture).hits_total;
    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    let stats = tracker_stats(&fixture);
    eprintln!(
        "ttl-zero head-only re-read: status={} upstream {} -> {} x_cache={:?} tracker={:?}",
        s2,
        before,
        after,
        h2.get("x-cache"),
        stats
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, data, "bytes (correct either way; not the predicate)");
    assert!(
        after > before,
        "a get_ttl 0s key with a HEAD-only .meta was answered without contacting S3"
    );
    assert!(
        h2.get("x-cache").is_none(),
        "a get_ttl 0s re-read carried x-cache {:?}",
        h2.get("x-cache")
    );
    assert_eq!(stats.hits_total, hits_before, "{:?}", stats);
    // Exactly one upstream request: the plain miss today's `.meta` verdict gives. Two means the
    // tracker was consulted for a zero-TTL key (its `Expired` verdict sent a conditional before
    // the refetch), i.e. a zero-TTL gate at a read site is missing.
    assert_eq!(
        after,
        before + 1,
        "a get_ttl 0s re-read made {} upstream requests, not one: the tracker was consulted for a \
         zero-TTL key",
        after - before
    );
}

// ---------------------------------------------------------------------------------------------
// Part B1: `hits_total` counts the responses the tracker's record made possible.
// ---------------------------------------------------------------------------------------------

/// A Mode B `If-Match` re-read of a tracked fill is served locally on the tracker's record without
/// a live-TTL verdict (`http_proxy.rs`, the full-object Mode B arm), so it must count a tracker hit.
///
/// RED on the tree: the serve happens and `hits_total` does not move.
#[tokio::test]
async fn a_mode_b_if_match_serve_from_the_tracker_counts_a_hit() {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = (OBJECT_SIZE as u64) * 16;
    config.cache.evaluate_conditions_from_cache = true;
    let fixture = Fixture::new(Arc::new(config)).await;
    let object = "fill-vis-mode-b.bin";
    let key = format!("{}/{}", BUCKET, object);
    let data = body();
    let upstream = StreamingStub::start(data.clone()).await;
    let server = fixture.spawn_proxy(Arc::new(upstream.clone())).await;

    streamed_fill(&fixture, &server, object, 0, true).await;
    assert!(
        fixture.read_meta(&key).is_none(),
        "PRECONDITION MISSING: a .meta exists, so the tracker is not the metadata source"
    );

    let hits_before = tracker_stats(&fixture).hits_total;
    let before = upstream.calls.load(Ordering::SeqCst);
    let (s2, h2, b2) = proxy_get_with_auth(
        server.addr,
        &format!("/{}", key),
        &signed_authorization_without_range(),
        &[("if-match", ETAG)],
    )
    .await;
    let after = upstream.calls.load(Ordering::SeqCst);
    let stats = tracker_stats(&fixture);
    eprintln!(
        "mode-b re-read: status={} upstream {} -> {} x_cache={:?} tracker={:?}",
        s2,
        before,
        after,
        h2.get("x-cache"),
        stats
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2, data);
    assert_eq!(
        after, before,
        "PRECONDITION MISSING: the Mode B re-read went to S3, so it was not served from the \
         tracker's record"
    );
    assert_eq!(
        stats.hits_total,
        hits_before + 1,
        "a response served from the tracker's record under Mode B did not count a hit: {:?}",
        stats
    );
}
