//! Cache fixtures for `.kiro/specs/expired-entry-revalidation/`.
//!
//! Builds the three states the spec's requirements are written against, and
//! drives the real mainline handlers against them:
//!
//! - **Candidate_Available** — complete metadata coverage *and* the range files
//!   present for the requested bytes.
//! - **Stored_Expired** — `SystemTime::now() > NewCacheMetadata::expires_at`.
//! - **Live_Expired** — `check_object_expiration` reports expired against the
//!   *currently resolved* `get_ttl`, which it derives from `created_at`.
//!
//! # Why the fixture writes cache files directly
//!
//! Seeding by driving a cold GET does not work: metadata writes go through the
//! per-instance journal, so the `.meta` only materialises once the background
//! consolidator folds the entry in. A fixture that waits for it is gated on
//! `consolidation_interval` — a mechanism unrelated to anything this spec tests,
//! and the failure mode `pre-push-checklist.md` § "Wait on the mechanism you
//! MEASURE" describes. (`download_coordination_stampede_test.rs` hits the same
//! wall and prints a warning before falling through.)
//!
//! Direct writes are also the only way to set `created_at` and `expires_at`
//! **independently**. That is not a convenience: the whole defect is an ordering
//! conflict between two freshness mechanisms with different inputs, so a fixture
//! that cannot drive them apart cannot establish both halves of issue #17. The
//! spec says as much in R7.7 — a test that moves only `expires_at`, or only
//! tightens live TTL while stored expiry stays in the future, proves one side.
//!
//! # Why `CompressionAlgorithm::None`
//!
//! That tag means "raw bytes, no frame, no checksum". It is a legacy read path
//! with no writer in this version, and it is retained precisely so older entries
//! stay readable — which lets a fixture write plain bytes instead of having to
//! produce a valid LZ4 frame.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use tempfile::TempDir;

use s3_proxy::bucket_settings::ResolvedSettings;
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{
    CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;

use super::CapturedRequest;

/// Fill byte for the cached (older) representation.
pub const OLD_FILL: u8 = b'A';
/// Fill byte for the fresh representation S3 returns on a changed object.
pub const NEW_FILL: u8 = b'B';

/// Everything a mainline handler call needs, built by [`Fixture::new`].
pub struct Fixture {
    /// Held so the cache directory outlives the test.
    pub _temp_dir: TempDir,
    pub config: Arc<Config>,
    pub cache_manager: Arc<CacheManager>,
    pub range_handler: Arc<RangeHandler>,
    pub inflight_tracker: Arc<InFlightTracker>,
}

/// How the seeded entry's stored expiry should sit relative to now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredExpiry {
    /// `expires_at` in the future. The entry survives the stored-expiry check in
    /// `DiskCacheManager::find_cached_ranges`, so lookups return its extents.
    Fresh,
    /// `expires_at` in the past. Today this makes the lookup return **no**
    /// ranges, which is the defect: the conditional-revalidation branches sit
    /// behind a non-empty-overlap guard and become unreachable.
    Expired,
}

/// Description of the cache entry to lay down.
pub struct SeedSpec {
    /// `(start, end)` extents in metadata order. Several extents model what a
    /// sequential reader leaves behind, and are the case where invalidating only
    /// `cached_ranges[0]` leaves coverage behind.
    pub extents: Vec<(u64, u64)>,
    pub etag: String,
    pub last_modified: String,
    pub content_length: u64,
    pub stored_expiry: StoredExpiry,
    /// Age of `created_at`. `check_object_expiration` compares this against the
    /// resolved `get_ttl`, so it is what decides Live_Expired for a non-zero TTL.
    pub created_age: Duration,
}

impl SeedSpec {
    /// A single-extent entry that is both Stored_Expired and — for any
    /// `get_ttl` under an hour — Live_Expired. This is the issue #17 state.
    pub fn expired(extents: Vec<(u64, u64)>, content_length: u64, etag: &str) -> Self {
        Self {
            extents,
            etag: etag.to_string(),
            last_modified: "Wed, 01 Jan 2025 00:00:00 GMT".to_string(),
            content_length,
            stored_expiry: StoredExpiry::Expired,
            created_age: Duration::from_secs(7200),
        }
    }

    /// Stored-fresh but live-expired: the state an operator produces by
    /// tightening `get_ttl` on an already-cached key, and the state any prior
    /// `304` leaves behind (`refresh_object_ttl` moves `expires_at` to
    /// `now + ttl` and leaves `created_at` alone).
    pub fn stored_fresh_live_expired(
        extents: Vec<(u64, u64)>,
        content_length: u64,
        etag: &str,
    ) -> Self {
        Self {
            stored_expiry: StoredExpiry::Fresh,
            ..Self::expired(extents, content_length, etag)
        }
    }
}

/// Test config with the confounds this spec's tests must exclude turned off.
///
/// Each line is load-bearing, not hygiene:
///
/// - **`download_coordination.enabled = false`** — the fetcher/waiter layer is
///   R7.8's subject. Left on, a single request may take the waiter path and the
///   inline revalidation under test never runs.
/// - **`ram_cache_enabled = false`** — a RAM range hit answers before the disk
///   lookup, so neither the conditional request nor the serve decision happens.
///   A test that let RAM answer would pass without reaching any of it.
/// - **`full_object_check_threshold`** — set below the object size so
///   `skip_full_object_check` is true. The range path's *early* full-object
///   shortcut can direct-serve with no live-TTL check at all (this spec's R4.1);
///   if it fired, a test aimed at the range-specific lookup would silently
///   measure R4.1 instead.
pub fn test_config(object_size: u64) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = object_size / 8;
    Arc::new(config)
}

/// As [`test_config`], but leaves the early full-object shortcut reachable —
/// for tests that target the shortcut itself (R4.1) or the full-object path.
pub fn test_config_full_object_checks(object_size: u64) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = object_size * 16;
    Arc::new(config)
}

/// As [`test_config`], but with download coordination ON.
///
/// Every other config here disables it, because the fetcher/waiter layer is a
/// confound for tests about the serve decision. This one is for the tests that are
/// about coordination itself (R4.6, R7.8).
pub fn test_config_coordinated(object_size: u64) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = true;
    config.cache.download_coordination.wait_timeout_secs = 10;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = object_size / 8;
    Arc::new(config)
}

/// As [`test_config_full_object_checks`], but with `cache.get_ttl` set.
///
/// # Why this exists, and it is not a convenience
///
/// Tests that call `HttpProxy::handle_range_request` directly pass their own
/// `ResolvedSettings`, so [`zero_ttl`] and [`short_ttl`] control the live-TTL
/// verdict. Tests that go through the **loopback proxy** do not: the real request
/// path calls `CacheManager::resolve_settings` itself and builds its own
/// `ResolvedSettings` from config and `cache_rules.json`, so any value a test
/// constructs is ignored entirely.
///
/// The default `cache.get_ttl` is ~10 years, so a loopback test that relies on a
/// `ResolvedSettings` it cannot inject gets a `Fresh` verdict no matter how old
/// its fixture is, serves from cache, issues no conditional — and reads exactly
/// like the defect it was written to detect. That happened: the first run of
/// `expired_full_object_mainline_test` reported "no conditional upstream request"
/// with `status=200 body_len=4096`, which is a correct cache serve under a 10-year
/// TTL, not a missing revalidation. The status and length in that message are what
/// distinguished the two.
///
/// So: for loopback tests, set the TTL **here**. `Duration::ZERO` means every GET
/// revalidates.
pub fn test_config_full_object_checks_with_get_ttl(
    object_size: u64,
    get_ttl: Duration,
) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = object_size * 16;
    config.cache.get_ttl = get_ttl;
    Arc::new(config)
}

impl Fixture {
    pub async fn new(config: Arc<Config>) -> Self {
        let temp_dir = TempDir::new().expect("tempdir");
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
            cache_dir,
            config.cache.ram_cache_enabled,
            config.cache.max_ram_cache_size,
            config.cache.max_cache_size,
            CacheEvictionAlgorithm::LRU,
            1024,
            config.compression.enabled,
            config.cache.get_ttl,
            config.cache.head_ttl,
            config.cache.put_ttl,
            config.cache.actively_remove_cached_data,
            config.cache.shared_storage.clone(),
            config.cache.write_cache_percent,
            config.cache.write_cache_enabled,
            config.cache.incomplete_upload_ttl,
            config.cache.metadata_cache.clone(),
            config.cache.eviction_trigger_percent,
            config.cache.eviction_target_percent,
            config.cache.read_cache_enabled,
            config.cache.bucket_settings_staleness_threshold,
            config.cache.compression_batch_size,
            config.cache.evaluate_conditions_from_cache,
            Duration::from_secs(10),
            64,
            Duration::from_secs(5),
        ));

        let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
            cache_manager.create_configured_disk_cache_manager(),
        ));
        cache_manager.initialize().await.expect("cache init");
        disk_cache_manager
            .write()
            .await
            .initialize()
            .await
            .expect("disk cache init");

        let range_handler = Arc::new(RangeHandler::new(
            Arc::clone(&cache_manager),
            Arc::clone(&disk_cache_manager),
        ));

        Self {
            _temp_dir: temp_dir,
            config,
            cache_manager,
            range_handler,
            inflight_tracker: Arc::new(InFlightTracker::new()),
        }
    }

    fn bin_relative(cache_key: &str, start: u64, end: u64) -> String {
        let safe = cache_key.replace(['/', ':'], "_");
        format!("bucket/00/000/{}_{}-{}.bin", safe, start, end)
    }

    /// Absolute path of a seeded extent's range file.
    pub fn bin_path(&self, cache_key: &str, start: u64, end: u64) -> std::path::PathBuf {
        self.cache_manager
            .get_cache_dir()
            .join("ranges")
            .join(Self::bin_relative(cache_key, start, end))
    }

    pub fn meta_path(&self, cache_key: &str) -> std::path::PathBuf {
        self.cache_manager.get_new_metadata_file_path(cache_key)
    }

    pub fn read_meta(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        let content = std::fs::read_to_string(self.meta_path(cache_key)).ok()?;
        serde_json::from_str(&content).ok()
    }

    /// The value production passes as `current_etag` on the range path.
    ///
    /// `handle_get_head_request` computes it as
    /// `cache_manager.get_object_etag(&cache_key)`, which reads
    /// `metadata.object_metadata.etag` from the cached `.meta` — the very field
    /// `RangeHandler::find_cached_ranges` then compares it against. So the
    /// ETag-mismatch invalidation on this path compares a value with itself and
    /// cannot fire. Tests pass this rather than `None` so no reader has to take
    /// that equivalence on trust.
    pub fn production_current_etag(&self, cache_key: &str) -> Option<String> {
        self.read_meta(cache_key).map(|m| m.object_metadata.etag)
    }

    /// Write the `.bin` files and `.meta`, then assert Candidate_Available and
    /// the requested stored-expiry state. Returns the ETag as persisted.
    ///
    /// The preconditions are **asserted, not assumed**. A test that proceeded
    /// without complete coverage and present range files would report a verdict
    /// about an entry the cache cannot serve, and would pass or fail for reasons
    /// unrelated to the requirement under test.
    pub fn seed(&self, cache_key: &str, spec: &SeedSpec) -> String {
        assert!(
            !spec.extents.is_empty(),
            "fixture must seed at least one extent"
        );
        let now = SystemTime::now();
        let created_at = now
            .checked_sub(spec.created_age)
            .expect("created_at underflow");

        let mut range_specs = Vec::new();
        for (start, end) in spec.extents.iter().copied() {
            let body = vec![OLD_FILL; (end - start + 1) as usize];
            let relative = Self::bin_relative(cache_key, start, end);
            let path = self
                .cache_manager
                .get_cache_dir()
                .join("ranges")
                .join(&relative);
            std::fs::create_dir_all(path.parent().expect("bin parent")).expect("create ranges dir");
            std::fs::write(&path, &body).expect("write .bin");

            range_specs.push(RangeSpec {
                start,
                end,
                file_path: relative,
                compression_algorithm: CompressionAlgorithm::None,
                compressed_size: body.len() as u64,
                uncompressed_size: body.len() as u64,
                created_at,
                last_accessed: created_at,
                access_count: 1,
                staged: None,
            });
        }

        let expires_at = match spec.stored_expiry {
            StoredExpiry::Fresh => now + Duration::from_secs(3600),
            StoredExpiry::Expired => now
                .checked_sub(Duration::from_secs(3600))
                .expect("expires_at underflow"),
        };

        let metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: ObjectMetadata {
                etag: spec.etag.clone(),
                last_modified: spec.last_modified.clone(),
                content_length: spec.content_length,
                content_type: Some("application/octet-stream".to_string()),
                upload_state: UploadState::Complete,
                cumulative_size: spec.content_length,
                parts: Vec::new(),
                response_headers: HashMap::new(),
                compression_algorithm: CompressionAlgorithm::None,
                compressed_size: range_specs.iter().map(|r| r.compressed_size).sum(),
                parts_count: None,
                part_ranges: HashMap::new(),
                upload_id: None,
                is_write_cached: false,
                write_cache_expires_at: None,
                write_cache_created_at: None,
                write_cache_last_accessed: None,
                graduation_accounted: false,
            },
            ranges: range_specs,
            created_at,
            expires_at,
            compression_info: CompressionInfo::default(),
            head_expires_at: None,
            head_last_accessed: None,
            head_access_count: 0,
            head_cached_at: None,
        };

        let meta_path = self.meta_path(cache_key);
        std::fs::create_dir_all(meta_path.parent().expect("meta parent"))
            .expect("create metadata dir");
        std::fs::write(
            &meta_path,
            serde_json::to_string_pretty(&metadata).expect("serialize .meta"),
        )
        .expect("write .meta");

        // Re-read, so every check below is against what is genuinely on disk.
        let metadata = self.read_meta(cache_key).expect("read back .meta");

        assert!(
            !metadata.ranges.is_empty(),
            "PRECONDITION MISSING: .meta records no ranges, so there is no candidate coverage"
        );
        for r in &metadata.ranges {
            let bin = self
                .cache_manager
                .get_cache_dir()
                .join("ranges")
                .join(&r.file_path);
            assert!(
                bin.exists(),
                "PRECONDITION MISSING: range file absent at {:?}; Candidate_Available is false",
                bin
            );
        }

        let stored_expired = SystemTime::now() > metadata.expires_at;
        match spec.stored_expiry {
            StoredExpiry::Expired => assert!(
                stored_expired,
                "PRECONDITION MISSING: entry is not Stored_Expired (expires_at={:?})",
                metadata.expires_at
            ),
            StoredExpiry::Fresh => assert!(
                !stored_expired,
                "PRECONDITION MISSING: entry is already Stored_Expired, so the lookup returns \
                 nothing and any conditional branch behind a non-empty-overlap guard is \
                 unreachable (expires_at={:?})",
                metadata.expires_at
            ),
        }

        assert!(
            !metadata.object_metadata.etag.is_empty(),
            "PRECONDITION MISSING: no ETag stored, so no validator can be injected"
        );

        metadata.object_metadata.etag
    }

    /// Assert the seeded extents completely cover `[start, end]`.
    ///
    /// Combined coverage, not per-extent: a multi-extent fixture deliberately has
    /// no single extent spanning the request, and it is *complete* coverage that
    /// makes `can_serve_from_cache` true.
    pub fn assert_covers(&self, cache_key: &str, start: u64, end: u64) {
        let metadata = self.read_meta(cache_key).expect(".meta must exist");
        let mut sorted: Vec<(u64, u64)> =
            metadata.ranges.iter().map(|r| (r.start, r.end)).collect();
        sorted.sort_unstable();
        let mut cursor = start;
        for (s, e) in &sorted {
            if *s > cursor {
                break;
            }
            cursor = cursor.max(e.saturating_add(1));
        }
        assert!(
            cursor > end,
            "PRECONDITION MISSING: cached extents {:?} do not completely cover {}-{}",
            sorted,
            start,
            end
        );
    }

    /// Drop the per-instance metadata cache entry so the next lookup reads the
    /// `.meta` from disk. Required after any direct write, because the metadata
    /// tier is refreshed on an interval rather than on change.
    pub async fn invalidate_metadata_cache(&self, cache_key: &str) {
        self.cache_manager
            .invalidate_metadata_cache(cache_key)
            .await;
    }

    /// Drive the real mainline **range** handler.
    #[allow(clippy::too_many_arguments)]
    pub async fn range_get(
        &self,
        cache_key: &str,
        raw_range: &str,
        client_headers: HashMap<String, String>,
        resolved: &ResolvedSettings,
        current_etag: Option<String>,
        s3_client: Arc<dyn s3_proxy::s3_client::S3ClientApi + Send + Sync>,
    ) -> hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>> {
        HttpProxy::handle_range_request(
            Method::GET,
            cache_key.to_string(),
            raw_range,
            client_headers,
            Arc::clone(&self.cache_manager),
            Arc::clone(&self.range_handler),
            s3_client,
            "s3.us-west-2.amazonaws.com".to_string(),
            format!("/{}", cache_key).parse().expect("uri"),
            Arc::clone(&self.config),
            resolved,
            current_etag,
            Arc::clone(&self.inflight_tracker),
            None,
            &None,
            false,
            None,
        )
        .await
        .expect("handle_range_request returns Infallible")
    }
}

/// `ResolvedSettings` with `get_ttl: 0` — every GET revalidates, so Live_Expired
/// holds regardless of `created_at`. This is issue #17's headline configuration.
pub fn zero_ttl() -> ResolvedSettings {
    ResolvedSettings {
        get_ttl: Duration::ZERO,
        ..ResolvedSettings::default()
    }
}

/// `ResolvedSettings` with a short non-zero `get_ttl`, so Live_Expired depends on
/// `created_at` age rather than on the zero-TTL shortcut. R2.7 and R7.3 require
/// this case separately, because a zero TTL can satisfy a test that a real
/// elapsed TTL would not.
pub fn short_ttl() -> ResolvedSettings {
    ResolvedSettings {
        get_ttl: Duration::from_secs(60),
        ..ResolvedSettings::default()
    }
}

pub async fn body_of(
    response: hyper::Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec()
}

/// Requests carrying a proxy-injected validator, i.e. conditional revalidation
/// attempts. Used to prove the branch under test was actually entered — without
/// it, a plain miss that forwarded and returned correct bytes passes while never
/// reaching the code the test names.
pub fn conditional_requests(captured: &[CapturedRequest]) -> Vec<&CapturedRequest> {
    captured
        .iter()
        .filter(|r| r.if_none_match().is_some() || r.if_modified_since().is_some())
        .collect()
}

/// A SigV4 `Authorization` header whose `SignedHeaders` list includes `range`.
///
/// `signed_request_proxy::is_range_signed` reads this, and R3.2 makes raw-Range
/// preservation mandatory precisely when it is present: rewriting a signed header
/// invalidates the signature. The credential and signature values are syntactic
/// filler — nothing in the proxy verifies them — but `SignedHeaders` is real,
/// because that is the field the code branches on.
pub fn signed_range_authorization() -> String {
    "AWS4-HMAC-SHA256 \
     Credential=AKIAIOSFODNN7EXAMPLE/20260830/us-west-2/s3/aws4_request, \
     SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
     Signature=0000000000000000000000000000000000000000000000000000000000000000"
        .to_string()
}

/// Response a stub returns for a `304 Not Modified`, which carries no body.
pub fn not_modified(etag: &str) -> super::StubResponse {
    super::StubResponse::with_status(StatusCode::NOT_MODIFIED).with_header("etag", etag)
}

// =========================================================================
// Loopback proxy harness, for the FULL-OBJECT mainline entry point
// =========================================================================
//
// `handle_get_head_request` cannot be called in-process: it takes a
// `Request<hyper::body::Incoming>`, and `Incoming` has no public constructor.
// `conditional_range_caching_test.rs` records the same constraint and gives up on
// covering that entry at unit level.
//
// The way round it is the one `part_scoped_head_cache_test.rs` and
// `cache_match_patterns_behavior_test.rs` already use: serve the real
// `HttpProxy::handle_request` on a loopback port and send it a genuine HTTP
// request. That goes through the actual dispatch into
// `handle_get_head_request`, so the initial full-object lookup — a *different*
// call site from the range path's — is really exercised.
//
// This matters for attribution. Without it, the full-object half of the fix
// would rest on a test that reaches the full-object *lookup inside the range
// handler*, which is a third call site with its own purpose assignment. Two
// sites that happen to agree today are not evidence about either.

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Request};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Semaphore};

pub struct ProxyServer {
    pub addr: SocketAddr,
    /// Dropping this shuts the accept loop down.
    pub _shutdown_tx: oneshot::Sender<()>,
}

impl Fixture {
    /// Serve the real `HttpProxy::handle_request` on an ephemeral loopback port.
    pub async fn spawn_proxy(
        &self,
        s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync>,
    ) -> ProxyServer {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let request_semaphore =
            Arc::new(Semaphore::new(self.config.server.max_concurrent_requests));

        let config = Arc::clone(&self.config);
        let cache_manager = Arc::clone(&self.cache_manager);
        let range_handler = Arc::clone(&self.range_handler);
        let inflight_tracker = Arc::clone(&self.inflight_tracker);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        let (stream, peer) = match accept {
                            Ok(v) => v,
                            Err(_) => break,
                        };
                        let io = TokioIo::new(stream);
                        let config = Arc::clone(&config);
                        let cache_manager = Arc::clone(&cache_manager);
                        let range_handler = Arc::clone(&range_handler);
                        let s3_client = Arc::clone(&s3_client);
                        let inflight_tracker = Arc::clone(&inflight_tracker);
                        let request_semaphore = Arc::clone(&request_semaphore);

                        tokio::spawn(async move {
                            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                                let config = Arc::clone(&config);
                                let cache_manager = Arc::clone(&cache_manager);
                                let range_handler = Arc::clone(&range_handler);
                                let s3_client = Arc::clone(&s3_client);
                                let inflight_tracker = Arc::clone(&inflight_tracker);
                                let request_semaphore = Arc::clone(&request_semaphore);
                                let inflight_ledger = s3_client.get_inflight_ledger();
                                async move {
                                    HttpProxy::handle_request(
                                        req,
                                        peer,
                                        config,
                                        cache_manager,
                                        s3_client,
                                        range_handler,
                                        request_semaphore,
                                        None,
                                        None,
                                        inflight_tracker,
                                        None,
                                        None,
                                        None,
                                        inflight_ledger,
                                    )
                                    .await
                                }
                            });
                            if let Err(e) = http1::Builder::new().serve_connection(io, service).await
                            {
                                eprintln!("proxy connection error: {}", e);
                            }
                        });
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });

        ProxyServer {
            addr,
            _shutdown_tx: shutdown_tx,
        }
    }
}

/// Send a plain GET (no `Range`) through the loopback proxy.
///
/// An `authorization` header is always attached: without one the proxy takes a
/// different, unsigned path, so its absence would silently change which code is
/// under test.
pub async fn proxy_get(
    addr: SocketAddr,
    path: &str,
    extra_headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Vec<u8>) {
    proxy_get_with_auth(addr, path, &signed_range_authorization(), extra_headers).await
}

/// A SigV4 `Authorization` whose `SignedHeaders` does **not** include `range`.
///
/// This is the realistic shape for a plain GET — a client does not sign a header it
/// is not sending — and it matters because at least one path branches on it. The
/// full-object partial-merge path synthesises a `Range` to fetch only the missing
/// bytes, and refuses to do so when `range` is signed, since adding the header would
/// invalidate the client's signature.
///
/// That is not hypothetical: the first version of
/// `expired_partial_coverage_full_object_merge_is_pinned_to_the_cached_etag` used
/// [`signed_range_authorization`] and the merge path was never entered at all. The
/// captured request carried neither `range` nor `if-match`, which is what exposed it.
/// A test that had asserted only on the response body would have passed while
/// measuring the plain-forward path instead.
pub fn signed_authorization_without_range() -> String {
    "AWS4-HMAC-SHA256 \
     Credential=AKIAIOSFODNN7EXAMPLE/20260830/us-west-2/s3/aws4_request, \
     SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
     Signature=0000000000000000000000000000000000000000000000000000000000000000"
        .to_string()
}

/// As [`proxy_get`], with the `Authorization` header chosen explicitly.
pub async fn proxy_get_with_auth(
    addr: SocketAddr,
    path: &str,
    authorization: &str,
    extra_headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Vec<u8>) {
    use http_body_util::Full;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("http://{}{}", addr, path))
        .header("authorization", authorization);
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let req = builder
        .body(Full::new(Bytes::new()))
        .expect("build GET request");

    let resp = client.request(req).await.expect("proxy GET failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec();
    (status, headers, body)
}

/// A compact description of a body's fill pattern, for assertion messages.
///
/// `assert_eq!` on two 4 KiB `Vec<u8>`s prints both in full — roughly 25,000
/// characters of `65, 65, 65, ...` per side, which buries every other line of the
/// run and made one real failure genuinely hard to read. These fixtures use
/// single-byte fills precisely so the interesting content is "which version, and
/// how long", and this renders exactly that.
///
/// Example: `4096 bytes, all OLD(A)`, or `1024 bytes, mixed: 512 OLD + 512 NEW`.
pub fn fill_summary(body: &[u8]) -> String {
    let old = body.iter().filter(|b| **b == OLD_FILL).count();
    let new = body.iter().filter(|b| **b == NEW_FILL).count();
    let other = body.len() - old - new;
    if body.is_empty() {
        return "0 bytes (empty)".to_string();
    }
    if old == body.len() {
        return format!("{} bytes, all OLD(A) — the CACHED version", body.len());
    }
    if new == body.len() {
        return format!("{} bytes, all NEW(B) — the FRESH version", body.len());
    }
    format!(
        "{} bytes, mixed: {} OLD(A) + {} NEW(B) + {} other",
        body.len(),
        old,
        new,
        other
    )
}

/// Assert a body is entirely the cached (old) version, reporting compactly.
pub fn assert_all_old(body: &[u8], expected_len: usize, context: &str) {
    assert!(
        body.len() == expected_len && body.iter().all(|b| *b == OLD_FILL),
        "{}: expected {} bytes of the CACHED version, got {}",
        context,
        expected_len,
        fill_summary(body)
    );
}

/// Assert a body is entirely the fresh (new) version, reporting compactly.
pub fn assert_all_new(body: &[u8], expected_len: usize, context: &str) {
    assert!(
        body.len() == expected_len && body.iter().all(|b| *b == NEW_FILL),
        "{}: expected {} bytes of the FRESH version, got {}",
        context,
        expected_len,
        fill_summary(body)
    );
}
