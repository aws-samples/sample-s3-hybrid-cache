//! A failed revalidation does not hold the disk cache manager's write lock while it waits for
//! the key's metadata slot.
//!
//! When the conditional request for an expired range returns an unexpected status or fails, the
//! range and full-object paths in `http_proxy.rs` remove the range
//! (`DiskCacheManager::remove_invalidated_range`). That function takes the key's metadata slot
//! lock itself, a budgeted wait (`metadata_lock_manager::slot_acquire_wait_budget`, 5 s by
//! default). The four call sites took `disk_cache.write()` around it, so while a peer held the
//! slot every other user of the manager in the process waited too, and tokio's lock is fair, so
//! new readers queued behind the waiting writer. During an origin outage every revalidating GET
//! then serialised. The `.meta` rewrite is ordered by the slot lock, so the read lock suffices.
//!
//! One test per call site: the range path and the full-object path, each with the conditional
//! request answered 500 (unexpected status) and failed outright (`Err`).

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::expired_fixture::{
    conditional_requests, proxy_get, test_config, test_config_full_object_checks_with_get_ttl,
    zero_ttl,
};
use common::expired_fixture::{Fixture, SeedSpec};
use common::StubS3Client;

const OBJECT_SIZE: u64 = 4096;
/// How long an unrelated `disk_cache.read()` may take while the failed revalidation waits for
/// the slot. Well under the 5 s slot wait budget, so a read queued behind a write guard held
/// across that wait cannot finish inside it.
const READ_BOUND: Duration = Duration::from_secs(1);
/// How long to wait for the conditional request to reach the stub before giving up.
const REVALIDATION_DEADLINE: Duration = Duration::from_secs(10);

/// Records each request on the wrapped stub and, with `fail`, then fails it, so the
/// revalidation takes its `Err` arm; without it the stub answers (500 with no response set).
/// With `peer`, a conditional request first takes the key's metadata slot for the test.
struct FailingS3 {
    stub: StubS3Client,
    fail: bool,
    peer: Option<Peer>,
}

/// The test's hold on the key's metadata slot, from another descriptor
/// (`try_acquire_slot_lock` opens the slot file per call, so `flock` excludes within the
/// process). The full-object sites take it when the conditional request arrives: that path
/// waits for the key's slot itself before it revalidates (and the stub saw no request at all
/// within 10 s while the slot was held from the start), so only a slot taken after that point
/// leaves the post-validation removal as the one waiting for it.
#[derive(Clone)]
struct Peer {
    cache_dir: std::path::PathBuf,
    key: String,
    held: Arc<std::sync::Mutex<Option<Box<dyn std::any::Any + Send>>>>,
}

impl Peer {
    fn take(&self) {
        let guard =
            s3_proxy::metadata_lock_manager::try_acquire_slot_lock(&self.cache_dir, &self.key)
                .expect("PRECONDITION: the test must take the key's slot");
        *self.held.lock().unwrap() = Some(Box::new(guard));
    }

    fn holds(&self) -> bool {
        self.held.lock().unwrap().is_some()
    }

    fn release(&self) {
        self.held.lock().unwrap().take();
    }
}

#[async_trait::async_trait]
impl s3_proxy::S3ClientApi for FailingS3 {
    async fn forward_request(
        &self,
        context: s3_proxy::S3RequestContext,
    ) -> s3_proxy::Result<s3_proxy::S3Response> {
        let conditional = context.headers.iter().any(|(k, _)| {
            k.eq_ignore_ascii_case("if-none-match") || k.eq_ignore_ascii_case("if-modified-since")
        });
        if conditional {
            if let Some(peer) = &self.peer {
                peer.take();
            }
        }
        let response = self.stub.forward_request(context).await;
        if !self.fail {
            return response;
        }
        Err(s3_proxy::ProxyError::ConnectionError(
            "injected upstream failure".to_string(),
        ))
    }

    fn extract_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::CacheMetadata {
        self.stub.extract_metadata_from_response(headers)
    }

    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> s3_proxy::cache_types::ObjectMetadata {
        self.stub.extract_object_metadata_from_response(headers)
    }

    fn get_connection_pool(
        &self,
    ) -> Arc<tokio::sync::RwLock<s3_proxy::connection_pool::ConnectionPoolManager>> {
        self.stub.get_connection_pool()
    }

    fn get_inflight_ledger(&self) -> Arc<s3_proxy::inflight_ledger::InflightLedger> {
        self.stub.get_inflight_ledger()
    }

    fn has_endpoint_overrides(&self) -> bool {
        self.stub.has_endpoint_overrides()
    }

    async fn set_metrics_manager(
        &self,
        metrics_manager: Arc<tokio::sync::RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
        self.stub.set_metrics_manager(metrics_manager).await
    }

    async fn register_endpoint(&self, endpoint: &str) {
        self.stub.register_endpoint(endpoint).await
    }

    async fn refresh_dns(&self) -> s3_proxy::Result<()> {
        self.stub.refresh_dns().await
    }
}

/// Which request revalidates, and how the conditional request fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Site {
    /// `handle_range_request`, the conditional request answered 500.
    RangeUnexpectedStatus,
    /// `handle_range_request`, the conditional request failed.
    RangeErr,
    /// `handle_get_head_request`'s full-object validation, answered 500.
    FullObjectUnexpectedStatus,
    /// `handle_get_head_request`'s full-object validation, failed.
    FullObjectErr,
}

/// An unrelated reader of the disk cache manager is not blocked by a failed revalidation at
/// `site` that waits for the key's metadata slot.
///
/// # What is asserted, and why
///
/// - **Precondition: the revalidation reached S3** (one conditional request captured), so the
///   handler is in the post-validation arm that removes the range.
/// - **Precondition: the slot wait is still in progress** when the read is granted: the request
///   has not returned and the range's `.bin` is still on disk, because the test holds the key's
///   slot from another descriptor (`try_acquire_slot_lock` opens the slot file per call, so
///   `flock` excludes within the process).
/// - **`disk_cache.read()` is granted within `READ_BOUND`.**
/// - **After the peer lets go, the removal completes**: the `.bin` is gone, so the call under
///   test is the one that took the slot.
async fn read_is_granted_while_a_failed_revalidation_waits(site: Site) {
    let full_object = matches!(site, Site::FullObjectUnexpectedStatus | Site::FullObjectErr);
    let fails = matches!(site, Site::RangeErr | Site::FullObjectErr);
    // The full-object GET judges freshness by the config's `get_ttl` (the range cases pass
    // `zero_ttl()` to `range_get`). At the default the seeded expired entry is served 200 from
    // cache with no request to S3, so these cases run at `get_ttl` 0.
    let config = if full_object {
        test_config_full_object_checks_with_get_ttl(OBJECT_SIZE, Duration::ZERO)
    } else {
        test_config(OBJECT_SIZE)
    };
    let fixture = Fixture::new(config).await;
    let cache_key = format!("bucket/failed-revalidation-read-lock-{site:?}.bin");
    let cache_key = cache_key.as_str();
    let (start, end) = if full_object {
        (0u64, OBJECT_SIZE - 1)
    } else {
        (0u64, 1023u64)
    };
    fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"failed-reval-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;
    let bin = fixture.bin_path(cache_key, start, end);
    assert!(
        bin.exists(),
        "PRECONDITION: the seeded range's .bin must exist"
    );
    // With no response configured every request returns 500: the unexpected-status arm.
    // `FailingS3` turns every request into an error instead.
    let stub = StubS3Client::new();
    let peer = Peer {
        cache_dir: fixture.cache_manager.get_cache_dir().to_path_buf(),
        key: cache_key.to_string(),
        held: Arc::default(),
    };
    let s3_client: Arc<dyn s3_proxy::S3ClientApi + Send + Sync> = Arc::new(FailingS3 {
        stub: stub.clone(),
        fail: fails,
        peer: full_object.then(|| peer.clone()),
    });
    let server = if full_object {
        Some(fixture.spawn_proxy(Arc::clone(&s3_client)).await)
    } else {
        None
    };

    // The peer holds the key's slot until released below: from now on the range path, from the
    // conditional request on the full-object path.
    if !full_object {
        peer.take();
    }

    let finished = AtomicBool::new(false);
    let request = async {
        let status = match &server {
            Some(server) => {
                proxy_get(server.addr, &format!("/{cache_key}"), &[])
                    .await
                    .0
            }
            None => fixture
                .range_get(
                    cache_key,
                    &format!("bytes={start}-{end}"),
                    HashMap::new(),
                    &zero_ttl(),
                    fixture.production_current_etag(cache_key),
                    Arc::clone(&s3_client),
                )
                .await
                .status(),
        };
        finished.store(true, Ordering::SeqCst);
        status
    };
    let probe = async {
        let deadline = Instant::now() + REVALIDATION_DEADLINE;
        while conditional_requests(&stub.captured()).is_empty() {
            assert!(
                Instant::now() < deadline,
                "PRECONDITION: the revalidation never reached S3 within {REVALIDATION_DEADLINE:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Give the handler time to reach the slot wait after the conditional request returns.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = Instant::now();
        let granted = tokio::time::timeout(READ_BOUND, fixture.disk_cache_manager.read())
            .await
            .map(drop)
            .is_ok();
        let waited = started.elapsed();
        let still_waiting = !finished.load(Ordering::SeqCst) && bin.exists() && peer.holds();
        peer.release();
        (granted, waited, still_waiting)
    };
    let (status, (granted, waited, still_waiting)) = tokio::join!(request, probe);

    println!(
        "{site:?}: read granted={granted} after {waited:?}; slot wait in progress={still_waiting}; \
         status={status}"
    );
    assert!(
        still_waiting,
        "PRECONDITION ({site:?}): the failed revalidation must still be waiting for the slot \
         when the read is attempted"
    );
    assert!(
        granted,
        "{site:?}: an unrelated disk_cache.read() was not granted within {READ_BOUND:?} while a \
         failed revalidation waited for the key's metadata slot: the write guard is held across \
         the wait"
    );
    assert_eq!(
        conditional_requests(&stub.captured()).len(),
        1,
        "{site:?}: the request must revalidate with S3 once"
    );
    assert!(
        !bin.exists(),
        "{site:?}: the failed revalidation must remove the range once the slot is released"
    );
}

/// # RED SIDE
///
/// With the four call sites on `disk_cache.write().await`, the handler holds the write guard
/// across the slot wait, so the read is not granted until the wait budget expires (5 s) and the
/// read-bound assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrelated_read_is_granted_while_a_failed_revalidation_waits_for_the_slot() {
    read_is_granted_while_a_failed_revalidation_waits(Site::RangeUnexpectedStatus).await;
}

/// # RED SIDE
///
/// With only the range path's `Err`-arm guard on `disk_cache.write().await`, "RangeErr: read
/// granted=false after 1.001219087s; slot wait in progress=true", FAILED, while the
/// unexpected-status test beside it passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_range_revalidation_error_does_not_hold_the_write_lock() {
    read_is_granted_while_a_failed_revalidation_waits(Site::RangeErr).await;
}

/// # RED SIDE
///
/// With only the full-object unexpected-status arm's guard on `disk_cache.write().await`,
/// "FullObjectUnexpectedStatus: read granted=false after 1.000592729s; slot wait in
/// progress=true", FAILED, while the other three cases passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_object_revalidation_unexpected_status_does_not_hold_the_write_lock() {
    read_is_granted_while_a_failed_revalidation_waits(Site::FullObjectUnexpectedStatus).await;
}

/// # RED SIDE
///
/// With only the full-object `Err` arm's guard on `disk_cache.write().await`, "FullObjectErr:
/// read granted=false after 1.001307433s; slot wait in progress=true", FAILED, while the other
/// three cases passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_object_revalidation_error_does_not_hold_the_write_lock() {
    read_is_granted_while_a_failed_revalidation_waits(Site::FullObjectErr).await;
}
