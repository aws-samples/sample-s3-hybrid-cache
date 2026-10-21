//! Signed PUT and Multipart Upload Handler
//!
//! Handles AWS SigV4 signed write requests: single-part PUT plus the four
//! multipart upload operations. Every request is forwarded to S3 unmodified
//! (the proxy holds no credentials and cannot re-sign) and cached in parallel.
//!
//! Handler routing (from `handle_signed_put`):
//!
//! | Request                                   | Handler                               |
//! | ----------------------------------------- | ------------------------------------- |
//! | `PUT /key` (non-multipart)                | [`SignedPutHandler::handle_with_caching`] |
//! | `POST /key?uploads`                       | [`SignedPutHandler::handle_create_multipart_upload`] |
//! | `PUT  /key?uploadId=X&partNumber=N`       | [`SignedPutHandler::handle_upload_part`] |
//! | `POST /key?uploadId=X` (no partNumber)    | [`SignedPutHandler::handle_complete_multipart_upload`] |
//! | `DELETE /key?uploadId=X`                  | [`SignedPutHandler::handle_abort_multipart_upload`] |
//!
//! # Multipart upload invariants
//!
//! For the multipart code paths in particular, see
//! [`docs/MULTIPART_UPLOAD.md`](../../../docs/MULTIPART_UPLOAD.md) for the
//! state machine, correctness gates, concurrency semantics, and threat model.
//! Short version:
//!
//! - In-flight state lives under `{cache_dir}/mpus_in_progress/{uploadId}/`. A
//!   part owns its own `part{N}.bin`, `part{N}.json` and `part{N}.lock`; there is
//!   no shared tracker document that each part appends to.
//! - The streaming part path (`open_multipart_part_sink` + its `finalize`) must
//!   hold `part{N}.lock` across both the part-file rename and the part-record
//!   write — same-part-number concurrent writes rely on this. Per part, not per
//!   upload: see [`SignedPutHandler::record_part_blocking`].
//! - `finalize_multipart_upload` first waits (bounded by
//!   [`MULTIPART_COMPLETE_CACHE_WAIT`]) for the records of the parts its own
//!   request body names, then only retains the cache if S3 succeeded, the request
//!   body parses, every requested part is cached locally, every requested ETag
//!   matches its record, and the assembled part count agrees with S3's ETag. Any
//!   miss → no cache entry; whether staging is also deleted depends on the path
//!   (see [`SignedPutHandler::cleanup_incomplete_multipart_cache`]).
//! - `aws_chunked_decoder` is the one true chunk parser for both this handler
//!   and the non-multipart PUT path.

use crate::aws_chunked_decoder;
use crate::capacity_manager::{
    check_cache_capacity, check_streaming_capacity, log_bypass_decision, CacheDecision,
};
use crate::compression::CompressionHandler;
use crate::metrics::{MetricsManager, RequestType};
use crate::s3_client::S3ClientApi;
use crate::signed_request_proxy::{
    forward_signed_request_streaming, forward_signed_request_streaming_verbatim,
    forward_signed_request_with_body, UpstreamTransport, STREAMED_BODY_CAP,
};
use crate::{ProxyError, Result};

/// The longest the write-through cache may hold up a streamed PUT or `UploadPart`
/// body before caching of that object is abandoned.
///
/// The forward loop reads the client's body only as fast as it can hand frames to
/// the cache tee once the tee's bounded channel is full, so a cache write blocked on
/// shared storage stops the proxy reading the client's socket. Past this bound the
/// tee is dropped, the upload keeps streaming to S3 verbatim, and the partial cache
/// entry is discarded. Five seconds is an order of magnitude inside the AWS CLI's
/// and SDKs' 60 s socket read timeout, and far longer than a healthy per-frame write.
pub(crate) const WRITE_CACHE_TEE_STALL_BOUND: std::time::Duration =
    std::time::Duration::from_secs(5);

/// The longest the write-through cache setup (sink open, capacity reservation,
/// `.tmp` creation, TTL resolution) may delay the upstream forward of a streamed
/// PUT or `UploadPart` before caching of that object is abandoned.
///
/// All of it runs before the proxy reads a byte of the client's body, so a setup
/// blocked on shared storage leaves the body unread and the client's socket read
/// times out (60 s in the AWS CLI and SDKs). Past this bound the request is
/// forwarded uncached. Five seconds matches [`WRITE_CACHE_TEE_STALL_BOUND`]: the two
/// can add, so a client's first body byte is read within 5 s and no later frame is
/// held more than 5 s, both an order of magnitude inside the client timeout, and a
/// healthy setup takes milliseconds.
pub(crate) const WRITE_CACHE_SETUP_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

/// The longest a streamed PUT or `UploadPart` body may be held, once the cache tee's
/// buffer is full, waiting for a write-cache setup that runs beside the forward.
///
/// The buffer holds `write_cache_tee_channel_depth + 2` request-body frames, which a fast
/// client fills in about a millisecond, while a healthy setup on shared storage makes
/// several metadata round trips (`statvfs`, `create_dir_all`, `.tmp` open, publication
/// admission) and takes tens of milliseconds. Dropping the tee the moment the buffer filled
/// left most larger objects uncached on a healthy volume. Half a second covers a healthy
/// setup's tail by roughly an order of magnitude and is more than two orders inside the
/// AWS CLI's and SDKs' 60 s socket read timeout. It is paid once per request, before the
/// writer exists; after that [`WRITE_CACHE_TEE_STALL_BOUND`] bounds each frame as before.
pub(crate) const WRITE_CACHE_SETUP_WAIT_BOUND: std::time::Duration =
    std::time::Duration::from_millis(500);

/// The most write-through cache stores (single PUT, empty object, `UploadPart`) one
/// process lets run behind already-acknowledged requests.
///
/// A cached PUT's response returns as soon as S3 answers and the store finishes in a
/// detached task, so without a bound the acknowledged-but-uncommitted backlog grows at the
/// difference between the request rate and the cache volume's commit rate, with nothing to
/// stop it but host RAM or `RLIMIT_NOFILE`. Each pending store, from setup until its last
/// step, holds one open `.tmp` file and an `IncrementalRangeWriter` batch buffer of
/// `cache.compression_batch_size` capacity, touched up to the object's size. So this bound
/// caps the backlog at 1,024 file descriptors and at most
/// 1,024 × min(object size, `cache.compression_batch_size`) of buffer (1 GiB at the 1 MiB
/// default, up to 16 GiB at the 16 MiB maximum), plus a few KB of headers and metadata each.
/// A setup abandoned at [`WRITE_CACHE_SETUP_BOUND`] keeps its permit until it finishes. A request arriving while
/// all permits are held is forwarded to S3 as normal and not cached
/// (`skipped_puts_total{backlog_full}`); under sustained overload the cached fraction is
/// about the commit rate divided by the request rate.
pub(crate) const MAX_PENDING_CACHE_WRITES: usize = 1024;

/// The process-wide permits behind [`MAX_PENDING_CACHE_WRITES`].
static PENDING_CACHE_WRITES: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(MAX_PENDING_CACHE_WRITES)));

/// `skipped_puts_total` reason for a request forwarded uncached because
/// [`MAX_PENDING_CACHE_WRITES`] stores were already pending.
pub(crate) const BACKLOG_FULL_SKIP_REASON: &str = "backlog_full";

/// `skipped_puts_total` reason for a write forwarded uncached because another live process holds
/// this one's exact `{host}:{pid}`: see [`identity_shared_with_a_live_peer`].
pub(crate) const HOSTNAME_NOT_UNIQUE_SKIP_REASON: &str = "hostname_not_unique";

/// `skipped_puts_total` reason for a store that declined to publish because a newer
/// write or delete of the same key was acknowledged after this PUT.
pub(crate) const SUPERSEDED_SKIP_REASON: &str = "superseded";

/// `skipped_puts_total` reason for a PUT whose body is not the object's bytes: a
/// subresource write or a server-side copy (see [`is_non_object_put`]).
pub(crate) const NON_OBJECT_PUT_SKIP_REASON: &str = "non_object_put";

/// `skipped_puts_total` reason for a single-part PUT whose successful S3 response carried
/// no non-empty `ETag`, so the stored entry would have no validator S3 vouched for.
pub(crate) const NO_ETAG_SKIP_REASON: &str = "no_etag";

/// Object-level PUT subresources, from the Amazon S3 API reference: PutObjectAcl (`acl`),
/// PutObjectLegalHold (`legal-hold`), RenameObject (`renameObject`), PutObjectRetention
/// (`retention`) and PutObjectTagging (`tagging`). Their bodies are configuration documents
/// (or empty), never the object.
const NON_OBJECT_PUT_SUBRESOURCES: [&str; 5] =
    ["acl", "legal-hold", "renameObject", "retention", "tagging"];

/// Request headers that make a PUT's body something other than the whole object, from the
/// same reference: CopyObject and UploadPartCopy (`x-amz-copy-source`, no body),
/// RenameObject (`x-amz-rename-source`), and an append to a directory-bucket object
/// (`x-amz-write-offset-bytes`, whose body is only the appended bytes).
const NON_OBJECT_PUT_HEADERS: [&str; 3] = [
    "x-amz-copy-source",
    "x-amz-rename-source",
    "x-amz-write-offset-bytes",
];

/// Whether a PUT names an object subresource in its query (percent-decoded parameter name,
/// ASCII case ignored) or carries a header that makes its body something other than the
/// object's bytes. Such a PUT is forwarded and invalidates the key, and is never stored.
fn is_non_object_put(query: &str, headers: &HeaderMap) -> bool {
    let names_subresource = query.split('&').any(|pair| {
        let name = pair.split('=').next().unwrap_or("");
        let decoded = percent_encoding::percent_decode_str(name).decode_utf8_lossy();
        NON_OBJECT_PUT_SUBRESOURCES
            .iter()
            .any(|subresource| decoded.eq_ignore_ascii_case(subresource))
    });
    names_subresource
        || NON_OBJECT_PUT_HEADERS
            .iter()
            .any(|name| headers.contains_key(*name))
}

/// Whether an S3 response carries an `ETag` with something other than quotes in it. A
/// single-part store declines without one, as [`part_etags_match`] does for parts.
fn response_carries_etag(headers: &HeaderMap) -> bool {
    headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| !v.trim_matches('"').is_empty())
}

/// Process-wide count of streamed PUT/`UploadPart` cache tees abandoned because the
/// cache writer did not accept a frame within [`WRITE_CACHE_TEE_STALL_BOUND`].
static TEE_ABANDONED_TOTAL_PUT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Cumulative streamed PUT/`UploadPart` cache tees abandoned on a stalled cache
/// writer since process start. See [`TEE_ABANDONED_TOTAL_PUT`].
pub fn tee_abandoned_total_put() -> u64 {
    TEE_ABANDONED_TOTAL_PUT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Process-wide count of streamed PUT/`UploadPart` write-cache setups abandoned
/// because opening the cache sink did not finish within
/// [`WRITE_CACHE_SETUP_BOUND`].
static SETUP_ABANDONED_TOTAL_PUT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cumulative streamed PUT/`UploadPart` write-cache setups abandoned on a slow
/// cache volume since process start. See [`SETUP_ABANDONED_TOTAL_PUT`].
pub(crate) fn setup_abandoned_total_put() -> u64 {
    SETUP_ABANDONED_TOTAL_PUT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Process-wide count of `CompleteMultipartUpload`s not cached because a Write_Ledger append
/// failed before the `.meta` publish (`finalize_multipart_upload`). Each one is an upload that
/// succeeded on S3 and was left uncached; a steady rise means the ledger directory is failing
/// and multipart write-through caching is effectively off.
static MPU_COMPLETION_LEDGER_DECLINES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cumulative multipart completions declined on a failed ledger append since process start.
/// See [`MPU_COMPLETION_LEDGER_DECLINES_TOTAL`].
pub(crate) fn mpu_completion_ledger_declines_total() -> u64 {
    MPU_COMPLETION_LEDGER_DECLINES_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Test-only: the cache key of every abandoned setup, so a test can assert its own
/// keys were abandoned without reading the process-global counter, which parallel
/// tests also move.
#[cfg(test)]
static SETUP_ABANDONED_KEYS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Unix second of the last setup-abandonment WARN, for rate limiting.
static SETUP_ABANDONED_LAST_WARN_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Run a write-cache setup future with at most `bound` of delay to the caller.
///
/// The setup runs on a blocking-pool thread (driven by `Handle::block_on`), not
/// inline, because it makes synchronous `std::fs` calls (`create_dir_all`,
/// `File::create` of the `.tmp`, `statvfs`) that a timeout cannot interrupt on an
/// async worker. Past `bound` the caller gets `None` and forwards uncached; the
/// setup keeps running on its thread, and whatever it eventually returns is dropped
/// with the detached `JoinHandle`. Every value a setup returns owns its cleanup in
/// `Drop` — `WriteCacheRangeSink` and `MultipartPartSink` abort their `.tmp`, and
/// the capacity reservations release themselves — so a late open leaves nothing
/// behind.
pub(crate) async fn bounded_cache_setup<T, F>(
    bound: std::time::Duration,
    cache_key: &str,
    setup: F,
) -> Option<T>
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    let handle = tokio::runtime::Handle::current();
    let task = tokio::task::spawn_blocking(move || handle.block_on(setup));
    match tokio::time::timeout(bound, task).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(e)) => {
            warn!(
                "Streaming write cache: setup task failed, forwarding uncached (upload \
                 unaffected): cache_key={}, error={}",
                cache_key, e
            );
            None
        }
        Err(_) => {
            record_setup_abandoned(&format!("did not finish within {:?}", bound), cache_key);
            None
        }
    }
}

/// Count one abandoned write-cache setup and WARN at most once per minute.
fn record_setup_abandoned(why: &str, cache_key: &str) {
    SETUP_ABANDONED_TOTAL_PUT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    #[cfg(test)]
    SETUP_ABANDONED_KEYS
        .lock()
        .unwrap()
        .push(cache_key.to_string());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = SETUP_ABANDONED_LAST_WARN_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if now.saturating_sub(last) >= 60
        && SETUP_ABANDONED_LAST_WARN_SECS
            .compare_exchange(
                last,
                now,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
    {
        warn!(
            "Streaming write cache: cache setup {}, forwarding uncached (upload \
             unaffected; at most one such WARN per minute, total in \
             write_cache.setup_abandoned_total_put={}): cache_key={}, setup_steps=[{}]",
            why,
            setup_abandoned_total_put(),
            cache_key,
            crate::cache::write_cache_setup_steps_summary(cache_key)
        );
    }
}

/// The four channel ends of a streamed PUT/`UploadPart` cache tee.
///
/// `forward_tx` and `s3_result_tx` go to the forward loop; `cache_rx` and
/// `s3_result_rx` go to the background cache task.
pub(crate) struct BoundedCacheTee {
    pub(crate) forward_tx: tokio::sync::mpsc::Sender<Bytes>,
    pub(crate) cache_rx: tokio::sync::mpsc::Receiver<Bytes>,
    pub(crate) s3_result_tx: tokio::sync::oneshot::Sender<Result<ResponseInfo>>,
    pub(crate) s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
}

/// Build a streamed-write cache tee whose cache side can hold up the forward loop by
/// at most `stall_bound`.
///
/// A relay task sits between the forward loop and the cache task. It moves each
/// frame from a one-slot forward channel into the `depth`-slot cache channel, and
/// gives each move at most `stall_bound`. If the cache task has not made room by
/// then (its blocking write is stuck on shared storage), the relay closes the
/// forward channel, so the forward loop sees `Closed`, drops the tee and keeps
/// streaming the body to S3 verbatim. It also counts the abandonment in
/// [`tee_abandoned_total_put`].
///
/// A gate task sits on the S3 result. It waits for both the S3 result and the
/// relay, and if the relay abandoned, hands the cache task a non-success instead of
/// the real result, so the cache task discards its partial `.tmp` rather than
/// committing a truncated object. The forward loop's own response is untouched.
///
/// Memory: one relay slot is added to the channel depth, so a request buffers at
/// most `depth + 2` frames rather than `depth + 1`.
#[cfg(test)]
pub(crate) fn bounded_cache_tee(
    depth: usize,
    stall_bound: std::time::Duration,
    cache_key: &str,
) -> BoundedCacheTee {
    bounded_cache_tee_gated(
        depth,
        stall_bound,
        cache_key,
        None,
        crate::cache::CacheDelayBudget::with_total(std::time::Duration::MAX),
    )
}

/// [`bounded_cache_tee`] for a cache writer that may not exist yet.
///
/// While `setup_ready` is `Some` and still false, the cache side's setup has not finished,
/// so a frame is buffered if there is room. If the buffer is full, the relay waits for the
/// setup to finish and the writer to take a frame, for at most
/// [`WRITE_CACHE_SETUP_WAIT_BOUND`] in total per request. Past that the tee is
/// dropped, counted in [`setup_abandoned_total_put`], and the forward continues uncached.
/// Once the flag is true the relay behaves exactly like [`bounded_cache_tee`].
///
/// Every wait the relay makes for the cache side (the setup wait, and a full buffer once the
/// writer exists) also draws on `budget`, the write's cache-delay budget. A frame the
/// buffer has room for costs nothing.
pub(crate) fn bounded_cache_tee_gated(
    depth: usize,
    stall_bound: std::time::Duration,
    cache_key: &str,
    setup_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
    budget: crate::cache::CacheDelayBudget,
) -> BoundedCacheTee {
    let (forward_tx, mut relay_rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let (cache_tx, cache_rx) = tokio::sync::mpsc::channel::<Bytes>(depth.max(1));
    let (s3_result_tx, outer_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
    let (inner_tx, s3_result_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

    let relay_key = cache_key.to_string();
    let setup_wait = WRITE_CACHE_SETUP_WAIT_BOUND;
    let relay = tokio::spawn(async move {
        let mut setup_deadline: Option<tokio::time::Instant> = None;
        while let Some(frame) = relay_rx.recv().await {
            let frame = match &setup_ready {
                Some(ready) if !ready.load(std::sync::atomic::Ordering::Acquire) => {
                    match cache_tx.try_send(frame) {
                        Ok(()) => continue,
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            // Setup declined caching and dropped the cache side.
                            relay_rx.close();
                            return false;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(frame)) => {
                            if ready.load(std::sync::atomic::Ordering::Acquire) {
                                frame
                            } else {
                                // The buffer is full and setup is still running. The
                                // cache writer does not exist until setup finishes, so room
                                // appears only once setup has finished and the writer has
                                // started draining. Wait for that, but for at most
                                // `setup_wait` in total across the request.
                                let deadline = *setup_deadline.get_or_insert_with(|| {
                                    tokio::time::Instant::now() + budget.cap(setup_wait)
                                });
                                let waited_from = tokio::time::Instant::now();
                                let sent =
                                    tokio::time::timeout_at(deadline, cache_tx.send(frame)).await;
                                budget.charge(waited_from.elapsed());
                                match sent {
                                    Ok(Ok(())) => continue,
                                    Ok(Err(_)) => {
                                        // Setup declined caching and dropped the cache side.
                                        relay_rx.close();
                                        return false;
                                    }
                                    Err(_) => {
                                        relay_rx.close();
                                        record_setup_abandoned(
                                            &format!(
                                                "had not finished within {:?} of the tee's \
                                                 buffer filling",
                                                setup_wait
                                            ),
                                            &relay_key,
                                        );
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => frame,
            };
            let frame = match cache_tx.try_send(frame) {
                Ok(()) => continue,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    // The cache task already closed its receiver (its own skip path).
                    relay_rx.close();
                    return false;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(frame)) => frame,
            };
            let bound = budget.cap(stall_bound);
            let waited_from = tokio::time::Instant::now();
            let sent = tokio::time::timeout(bound, cache_tx.send(frame)).await;
            budget.charge(waited_from.elapsed());
            match sent {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    // The cache task already closed its receiver (its own skip path).
                    // Not an abandonment: close the forward side and stop.
                    relay_rx.close();
                    return false;
                }
                Err(_) => {
                    relay_rx.close();
                    TEE_ABANDONED_TOTAL_PUT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!(
                        "Streaming write cache: cache writer did not accept a frame within {:?}, \
                         abandoning caching for this object (upload unaffected): cache_key={}",
                        bound, relay_key
                    );
                    return true;
                }
            }
        }
        false
    });

    let gate_key = cache_key.to_string();
    tokio::spawn(async move {
        let result = outer_rx.await;
        let abandoned = relay.await.unwrap_or(true);
        let forwarded = if abandoned {
            Err(ProxyError::CacheError(format!(
                "write-cache tee abandoned on a stalled cache writer: cache_key={}",
                gate_key
            )))
        } else {
            match result {
                Ok(r) => r,
                // The forward loop dropped its result sender; dropping ours tells the
                // cache task the same thing.
                Err(_) => return,
            }
        };
        let _ = inner_tx.send(forwarded);
    });

    BoundedCacheTee {
        forward_tx,
        cache_rx,
        s3_result_tx,
        s3_result_rx,
    }
}

/// How long `CompleteMultipartUpload` will wait for the part records it needs to
/// appear in the upload tracker before giving up and not caching the object.
///
/// The per-part cache task is fire-and-forget and lags the client's response by
/// design, so on a working cache this wait is the normal path rather than an
/// exception — measured stragglers landed within about three seconds of Complete.
/// Ten seconds leaves room for a slow shared volume without letting a genuinely
/// lost part hold a client-visible request open indefinitely.
///
/// The wait runs on the finalization's own task, which the Complete's response waits
/// for at most [`MULTIPART_COMPLETE_RESPONSE_WAIT`], so it costs cache latency and
/// never Complete latency.
///
/// Deliberately a fixed internal constant rather than a config field: no operator
/// has needed to tune it, and the shape of the fix does not depend on the value.
/// If a need appears, a field with this as its default is a compatible change.
const MULTIPART_COMPLETE_CACHE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// The longest a CompleteMultipartUpload response waits for its cache finalization, which
/// continues on its own task after that. Long enough that a completion whose
/// parts are all recorded is usually published before the client sees the response, so an
/// immediate GET is a hit; short enough that a Complete is never held for cache work, such as
/// a part another proxy skipped.
const MULTIPART_COMPLETE_RESPONSE_WAIT: std::time::Duration = std::time::Duration::from_millis(250);

/// Gap between tracker polls while waiting for part records (see
/// [`MULTIPART_COMPLETE_CACHE_WAIT`]). Short enough that the common case adds
/// little latency, long enough not to hammer a network filesystem.
const TRACKER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Shortest and longest async sleep between `upload.lock` attempts in
/// `SignedPutHandler::lock_upload_file`.
const UPLOAD_LOCK_RETRY_MIN: std::time::Duration = std::time::Duration::from_millis(5);
const UPLOAD_LOCK_RETRY_MAX: std::time::Duration = std::time::Duration::from_millis(200);

/// How many part-cache finalizations may hold a `spawn_blocking` thread at
/// once, per process.
///
/// # Why this bound exists, and why the rationale changed
///
/// This started as a fix for a real, measured failure: before per-part records
/// (see [`Self::record_part_blocking`]), every part's finalize took an exclusive
/// cross-instance `flock` on a single shared `upload.lock` and rewrote the WHOLE
/// `upload.meta` tracker, so per-part cost grew with the number of parts already
/// recorded and every in-flight part queued on that one lock. Each waiter occupied
/// a `spawn_blocking` thread for up to its 30-second timeout; the forward path
/// needs that same pool, so the pool was exhausted and the client's upload failed.
/// Measured on a three-proxy fleet at 2,000 parts (2026-08-21, release 2.6.0,
/// against the shared-`upload.lock` design): **1,214 `upload.lock` timeouts and
/// 1,220 part-record failures across the three instances, and the client's upload
/// failed** with `Connection reset by peer` on an `UploadPart`.
///
/// **That mechanism no longer exists.** Finalization now takes a *per-part*
/// `part{N}.lock` (see [`Self::finalize_and_record_cached_part`]), so two writers
/// on different part numbers never contend at all — only same-part-number retries
/// do, which is the correctness gate this was always meant to preserve, not the
/// throughput problem. Re-run at 2,000 parts against the per-part-lock design
/// showed zero lock contention from this cause.
///
/// This semaphore is kept anyway, as a **plain backstop** rather than a fix for a
/// live defect: it caps how many finalizations can occupy a `spawn_blocking`
/// thread simultaneously, independent of why any one of them might be slow (a
/// degraded shared volume, an unusually large part, etc.). It is not load-bearing
/// for the 2,000-part case any more: re-measured against the per-part-lock
/// design, that case succeeds without this semaphore needing to intervene.
///
/// Four is deliberately below the AWS CLI's default upload concurrency of 10, so
/// the queue is bounded for the common client, while leaving enough parallelism
/// that a single slow shared-volume operation does not serialise everything behind
/// it. It does **not** reduce the total serialised time for one upload — see the
/// note on `MULTIPART_COMPLETE_CACHE_WAIT` and the deferred O(n²) work.
static PART_FINALIZE_SLOTS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(4));
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::{HeaderMap, Request, Response, StatusCode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Byte offset of the first occurrence of ASCII `needle` in `haystack`, ignoring ASCII case.
/// The offset is into `haystack` itself: lowercasing a copy changes the byte length of some
/// characters (`K` U+212A, `İ` U+0130), so an offset found in the copy can point anywhere in
/// the original. A match starts on an ASCII byte, so it is a char boundary.
fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let (h, n) = (haystack.as_bytes(), needle.as_bytes());
    if n.is_empty() || h.len() < n.len() {
        return None;
    }
    (0..=h.len() - n.len()).find(|&i| h[i..i + n.len()].eq_ignore_ascii_case(n))
}

/// Request headers S3 stores with an object and returns on every GET and HEAD of it.
/// `x-amz-meta-*` and `x-amz-storage-class` are handled by
/// [`merge_representation_headers`] itself.
const REPRESENTATION_HEADERS: &[&str] = &[
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-type",
    "expires",
    "x-amz-website-redirect-location",
];

/// The `Content-Type` S3 stores, and returns on GET and HEAD, for an object written
/// without one (measured on PutObject and on a multipart upload).
const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

/// Copy a write request's representation headers into the headers a write-through entry
/// stores, so a cache hit carries what S3 returns for the object. A write
/// response describes the write, not the object, so for these names the request wins.
///
/// Rewritten as S3 stores them: `aws-chunked` is dropped from `content-encoding` (it frames
/// the upload), `x-amz-storage-class: STANDARD` is dropped (S3 omits it on reads),
/// `x-amz-tagging` becomes `x-amz-tagging-count`, and a request with no `Content-Type` is
/// stored with [`DEFAULT_CONTENT_TYPE`].
///
/// Never copied: signing and payload headers (`authorization`, `x-amz-date`,
/// `x-amz-content-sha256`, `x-amz-decoded-content-length`, `content-length`), which describe
/// the request; ACL, grant, object-lock and `x-amz-expected-bucket-owner` headers, which S3
/// does not return on a read; SSE-C headers (such a PUT is never cached); and SSE, version
/// and checksum headers, which come from S3's response instead.
fn merge_representation_headers(
    request_headers: &HashMap<String, String>,
    stored: &mut HashMap<String, String>,
) {
    for (key, value) in request_headers {
        let lower = key.to_ascii_lowercase();
        let value = if lower == "content-encoding" {
            let kept: Vec<&str> = value
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty() && !t.eq_ignore_ascii_case("aws-chunked"))
                .collect();
            if kept.is_empty() {
                continue;
            }
            kept.join(",")
        } else if lower == "x-amz-storage-class" {
            if value.eq_ignore_ascii_case("STANDARD") {
                continue;
            }
            value.clone()
        } else if lower.starts_with("x-amz-meta-")
            || REPRESENTATION_HEADERS.contains(&lower.as_str())
        {
            value.clone()
        } else {
            continue;
        };
        stored.retain(|k, _| !k.eq_ignore_ascii_case(key));
        stored.insert(key.clone(), value);
    }
    if let Some((_, tags)) = request_headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-tagging"))
    {
        let count = tags.split('&').filter(|t| !t.is_empty()).count();
        if count > 0 {
            stored.retain(|k, _| !k.eq_ignore_ascii_case("x-amz-tagging-count"));
            stored.insert("x-amz-tagging-count".to_string(), count.to_string());
        }
    }
    if !request_headers
        .keys()
        .any(|k| k.eq_ignore_ascii_case("content-type"))
    {
        stored.retain(|k, _| !k.eq_ignore_ascii_case("content-type"));
        stored.insert("content-type".to_string(), DEFAULT_CONTENT_TYPE.to_string());
    }
}

/// Simple response info for background tasks (status + headers only, no body)
#[derive(Clone, Debug)]
pub(crate) struct ResponseInfo {
    status: StatusCode,
    headers: HeaderMap,
    /// What the response path stamped for the store to check.
    write_guard: WriteGuard,
}

/// The write-order claim a PUT's detached store carries. See
/// `cache::WRITE_MARKER_EXTENSION`.
#[derive(Clone, Debug)]
pub(crate) enum WriteGuard {
    /// Nothing was stamped: an `UploadPart` (which publishes no `.meta`) or a test driving a
    /// store directly. The store runs as it did before the fix.
    Unguarded,
    /// The response path stamped this token; the store publishes only while it is current.
    Token(crate::cache::WriteToken),
    /// The response-path invalidation failed, so the store cannot show it is the latest write
    /// of the key, and declines.
    Unavailable,
}

impl ResponseInfo {
    fn status(&self) -> StatusCode {
        self.status
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn write_guard(&self) -> &WriteGuard {
        &self.write_guard
    }
}
use tracing::{debug, error, info, warn};

/// Test-only pause a streaming store passes after S3 answered and before it touches the
/// cache. Each arming pauses once, so of two stores of one key only the first is held.
#[cfg(test)]
pub(crate) mod pre_store_pause {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::Notify;

    #[derive(Clone)]
    pub(crate) struct Pause {
        pub(crate) reached: Arc<Notify>,
        pub(crate) resume: Arc<Notify>,
    }

    fn registry() -> &'static Mutex<HashMap<String, Pause>> {
        static R: OnceLock<Mutex<HashMap<String, Pause>>> = OnceLock::new();
        R.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(crate) fn arm(cache_key: &str) -> Pause {
        let p = Pause {
            reached: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        registry()
            .lock()
            .unwrap()
            .insert(cache_key.to_string(), p.clone());
        p
    }

    pub(crate) async fn hit(cache_key: &str) {
        let p = registry().lock().unwrap().remove(cache_key);
        if let Some(p) = p {
            p.reached.notify_one();
            p.resume.notified().await;
        }
    }
}

/// Outcome of the streaming write-cache task
/// ([`SignedPutHandler::run_streaming_cache_write`]).
///
/// Lets callers and unit tests observe whether the streamed object was cached or
/// skipped (and why) without affecting the upload. The spawned wrapper
/// ([`SignedPutHandler::spawn_streaming_cache_write_task`]) discards this value —
/// per Req 7 a cache skip/failure never alters the upload result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamingCacheOutcome {
    /// The object was streamed to the sink and committed to the cache.
    Committed,
    /// Caching was skipped without failing the upload. The string is a short,
    /// stable reason for diagnostics/tests (e.g. `"decoded_length_mismatch"`,
    /// `"s3_error"`, `"cache_write_error"`).
    Skipped(&'static str),
}

/// Cache-capacity budget carried by an `UploadPart` whose body length was not
/// known up front, so that the staging write can be bounded while it streams.
///
/// [`check_cache_capacity`] returns [`CacheDecision::StreamWithCapacityCheck`]
/// when the request has no usable Content-Length, which is the only decision that
/// reaches the part path with **no** size gate applied: the part sink is not
/// pre-sized, does not reserve write-cache capacity, and
/// `CacheManager::open_multipart_part_sink` performs no disk-safety refusal. This
/// budget is the missing gate.
///
/// It is `Some` only for that decision. The reason is **not** that a
/// [`CacheDecision::Cache`] part is adequately gated — R16.1/R16.2: what
/// [`check_cache_capacity`] contributes on that arm is one subtraction against a
/// `current_usage` snapshot that cannot see staged parts and flattens to `0` on a
/// stats read error, so it is weaker than "already gated" implies. The reason is
/// that a declared Content-Length is a figure the part path's own guards can act
/// on: it reaches `open_multipart_part_sink` as `part_size_hint` and thence the
/// danger-band publication guard (cache-eviction-at-scale task 36, P3c/P8), which
/// is the per-part control on that arm. This budget exists for the case where no
/// such figure exists — the guard is handed `0` and admits — and re-checking it
/// per frame would duplicate a bound already applied with a real size.
///
/// Both figures are snapshots taken at handler construction, so the bound is a
/// bound on *this* part rather than a fleet-wide accounting fix; concurrent parts
/// each read the same `current_usage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamingCapacityBudget {
    /// Cache usage in bytes excluding this upload, as observed by the handler.
    pub(crate) current_usage: u64,
    /// Maximum cache capacity in bytes.
    pub(crate) max_capacity: u64,
}

/// Read the body length of an `UploadPart` request from its headers.
///
/// Returns `None` both when the header is absent and when it is present but does
/// not parse as a `u64`. Either way [`check_cache_capacity`] returns
/// [`CacheDecision::StreamWithCapacityCheck`], which is the decision that reaches
/// the part-staging path with no size gate of its own — hence the
/// [`StreamingCapacityBudget`] (R16.8). Extracted from `handle_upload_part` so both
/// `None` cases are testable directly; the AWS CLI always sets a valid
/// Content-Length, so neither is reachable through it.
fn part_content_length(request_headers: &HashMap<String, String>) -> Option<u64> {
    request_headers
        .get("content-length")
        .or_else(|| request_headers.get("Content-Length"))
        .and_then(|v| v.parse::<u64>().ok())
}

/// Represents a part from the CompleteMultipartUpload request body XML.
/// Used to parse and validate which parts the client wants to include in the final object.
///
/// # Requirements
/// - Requirement 4.2: Extract the list of (PartNumber, ETag) pairs from the request
#[derive(Debug)]
struct RequestedPart {
    /// The part number (1-indexed) as specified in the request
    part_number: u32,
    /// The ETag of the part, used for validation against cached parts
    etag: String,
}

/// Parse the CompleteMultipartUpload request body XML to extract the list of parts.
///
/// # Arguments
/// * `body` - The raw request body bytes containing the XML
///
/// # Returns
/// * `Ok(Vec<RequestedPart>)` - List of parts with their part numbers and ETags
/// * `Err(ProxyError)` - If the XML is malformed or contains invalid data
///
/// # Requirements
/// - Requirement 4.1: Parse the XML request body before forwarding to S3
/// - Requirement 4.2: Extract the list of (PartNumber, ETag) pairs from the request
/// - Requirement 4.3: If request body is empty or malformed, skip cache finalization and log warning
///
/// # Example XML Format
/// ```xml
/// <CompleteMultipartUpload>
///   <Part>
///     <PartNumber>1</PartNumber>
///     <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
///   </Part>
/// </CompleteMultipartUpload>
/// ```
fn parse_complete_mpu_request(body: &[u8]) -> Result<Vec<RequestedPart>> {
    // Handle empty body gracefully - return empty list
    if body.is_empty() {
        return Ok(Vec::new());
    }

    let body_str = std::str::from_utf8(body)
        .map_err(|e| ProxyError::InvalidRequest(format!("Invalid UTF-8 in request body: {}", e)))?;

    let mut parts = Vec::new();

    // Split by <Part> and skip the first segment (before the first <Part>)
    for part_match in body_str.split("<Part>").skip(1) {
        if let Some(end_idx) = part_match.find("</Part>") {
            let part_xml = &part_match[..end_idx];

            let part_number = extract_xml_value(part_xml, "PartNumber")?
                .parse::<u32>()
                .map_err(|e| {
                    ProxyError::InvalidRequest(format!("Invalid PartNumber value: {}", e))
                })?;

            let etag = extract_xml_value(part_xml, "ETag")?;

            parts.push(RequestedPart { part_number, etag });
        }
    }

    Ok(parts)
}

/// Extract the value of an XML tag from a string.
///
/// # Arguments
/// * `xml` - The XML string to search in
/// * `tag` - The tag name to extract (without angle brackets)
///
/// # Returns
/// * `Ok(String)` - The trimmed value between the opening and closing tags
/// * `Err(ProxyError)` - If the tag is not found
///
/// # Requirements
/// - Requirement 4.2: Extract the list of (PartNumber, ETag) pairs from the request
fn extract_xml_value(xml: &str, tag: &str) -> Result<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let start = xml
        .find(&start_tag)
        .ok_or_else(|| ProxyError::InvalidRequest(format!("Missing <{}> tag", tag)))?
        + start_tag.len();

    let end = xml
        .find(&end_tag)
        .ok_or_else(|| ProxyError::InvalidRequest(format!("Missing </{}> tag", tag)))?;

    Ok(xml[start..end].trim().to_string())
}
/// Compare a client-requested part ETag against the cached part's recorded ETag.
///
/// S3 ETags may or may not have surrounding quotes depending on the source, so the
/// comparison tolerates one balanced pair — and nothing else.
///
/// # Arguments
/// * `etag` - The ETag string to normalize
///
/// # Returns
/// Whether the two part ETags describe the same part.
///
/// Replaces this module's former private `normalize_etag`, which was
/// `trim_matches('"')`: it stripped **unbalanced and repeated** quotes and mapped both
/// `""` and the empty string to empty, so `"abc` compared equal to `abc` and two
/// empty-after-trimming validators compared equal to each other. Equality there admits a
/// multipart finalization into the cache without its parts having been verified, and it
/// is REACHABLE from both sides: the cached ETag comes from the `UploadPart` response via
/// `unwrap_or_default()` in `Self::stream_and_cache_upload_part`'s Phase 4, so an origin
/// that omits the header yields the empty string with no guard anywhere between there and
/// this comparison; the requested ETag comes from `extract_xml_value(part_xml, "ETag")`,
/// which returns `Ok("")` for `<ETag></ETag>`.
///
/// Two rules, and the second is not supplied by the shared helper:
///
/// - Quote tolerance comes from [`crate::revalidation::etags_equal_ignoring_quotes`],
///   which removes at most one BALANCED pair and therefore keeps `"abc` and `abc`
///   different.
/// - An empty or quote-only validator on EITHER side is never a match. The shared helper
///   refuses to create a *new* equality for one, but it short-circuits on `a == b`, so
///   two literally-empty ETags would still compare equal through it. That case is exactly
///   the reachable one above, so it is refused here rather than left to the caller.
fn part_etags_match(requested: &str, cached: &str) -> bool {
    // The emptiness VETO is the one job `trim_matches('"')` is still right for: it asks
    // "is there anything here but quotes?", and a lone `"`, a `""` and the empty string
    // all answer no. Using the strict `strip_etag_quotes` here instead would let `"`
    // through as a non-empty opaque tag, and `"` on both sides would then compare equal
    // through the byte-identical shortcut — which is the same hole in a smaller shape.
    if requested.trim_matches('"').is_empty() || cached.trim_matches('"').is_empty() {
        return false;
    }
    // The MATCH is the strict helper's job: at most one balanced pair removed, `W/` never
    // stripped, no new equality for an empty tag.
    crate::revalidation::etags_equal_ignoring_quotes(requested, cached)
}

/// Format bytes into human-readable string (MB with 1 decimal)
fn format_size(bytes: u64) -> String {
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = MB * 1024.0;

    if bytes as f64 >= GB {
        format!("{:.1}GB", bytes as f64 / GB)
    } else if bytes as f64 >= MB {
        format!("{:.1}MB", bytes as f64 / MB)
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Truncate upload ID for logging (first 12 chars + ...)
///
/// Cuts on a `char` boundary, not a byte offset: the rejection branches below log an
/// uploadId that failed [`is_cacheable_upload_id`], which can be non-ASCII, and a byte
/// slice through a multi-byte character panics.
fn truncate_upload_id(upload_id: &str) -> String {
    match upload_id.char_indices().nth(12) {
        Some((cut, _)) => format!("{}...", &upload_id[..cut]),
        None => upload_id.to_string(),
    }
}

/// The in-progress directory for one multipart upload:
/// `cache_dir/mpus_in_progress/<BLAKE3 hex digest of upload_id>`.
///
/// **The only place a filesystem path is derived from an uploadId.** The uploadId is
/// an opaque token chosen by S3 (and echoed by the client on every `UploadPart`,
/// `CompleteMultipartUpload` and `AbortMultipartUpload`), so it is never used as a
/// path component itself: its digest is. A digest is 64 lowercase hex characters
/// whatever the input, so no uploadId, however hostile, can name a parent directory,
/// a separator, or a sibling outside `mpus_in_progress/`.
///
/// This replaced joining the raw uploadId behind an `is_safe_path_component` check.
/// That check rejects any `..` substring, and real S3 uploadIds contain one about one
/// time in ten (4 of 40 measured, 2026-09-25), so those uploads were never
/// write-through cached. Hashing is strictly no weaker for traversal — no client- or
/// S3-supplied byte reaches the path — and it caches every uploadId.
///
/// Nothing reads the directory name back as an uploadId: the sweeps that walk
/// `mpus_in_progress/` age entries by mtime and take the uploadId, where they log it,
/// from `upload.meta` inside the directory. `tests/mpu_upload_dir_test.rs` fails if a
/// raw uploadId join reappears anywhere in `src/`.
pub fn mpu_upload_dir(cache_dir: &std::path::Path, upload_id: &str) -> std::path::PathBuf {
    cache_dir
        .join("mpus_in_progress")
        .join(blake3::hash(upload_id.as_bytes()).to_hex().as_str())
}

/// Whether an uploadId is admitted to write-through caching.
///
/// No longer a path-safety check — [`mpu_upload_dir`] hashes the uploadId, so none of
/// its bytes reach a path. What this still guards is everything else the uploadId is
/// written into: the many `upload_id={}` log lines (a control character would let a
/// client forge or split a log record) and the tracker JSON. Empty is refused because
/// `extract_upload_id(..).unwrap_or_default()` yields `""` for a request with no
/// uploadId, and every such request would otherwise share one directory.
///
/// Printable ASCII only (`0x21..=0x7E`). S3 uploadIds are base64url-style tokens, so no
/// real one is refused; `..`, `/` and `\` are accepted because they no longer matter.
fn is_cacheable_upload_id(upload_id: &str) -> bool {
    !upload_id.is_empty() && upload_id.bytes().all(|b| (0x21..=0x7E).contains(&b))
}

/// Truncate ETag for logging (first 12 chars + suffix if multipart)
fn truncate_etag(etag: &str) -> String {
    // Remove quotes if present
    let etag = etag.trim_matches('"');

    // Check for multipart suffix (e.g., "-10" at the end)
    if let Some(dash_pos) = etag.rfind('-') {
        let suffix = &etag[dash_pos..];
        // If suffix looks like a part count (e.g., "-10"), preserve it
        if suffix.len() > 1 && suffix[1..].chars().all(|c| c.is_ascii_digit()) {
            let hash_part = &etag[..dash_pos];
            if hash_part.len() > 8 {
                return format!("{}...{}", &hash_part[..8], suffix);
            }
            return etag.to_string();
        }
    }

    // Single-part ETag - just truncate
    if etag.len() > 12 {
        format!("{}...", &etag[..12])
    } else {
        etag.to_string()
    }
}

/// Extract bucket and key from cache_key (format: "bucket/key")
fn parse_cache_key(cache_key: &str) -> (&str, &str) {
    match cache_key.split_once('/') {
        Some((bucket, key)) => (bucket, key),
        None => (cache_key, ""),
    }
}

/// Whether `query` carries a parameter named `name`. The name is the text before `=`, so the
/// value of another parameter (`x-id=partNumber`) does not count.
pub(crate) fn query_has_param(query: &str, name: &str) -> bool {
    query
        .split('&')
        .any(|pair| pair.split('=').next() == Some(name))
}

/// SignedPutHandler orchestrates signed PUT request caching
///
/// This handler coordinates the streaming of signed PUT requests to both
/// S3 and the cache simultaneously, ensuring signature preservation while
/// enabling efficient caching.
///
/// # Requirements
///
/// - Requirement 1.1: Stream request body to both S3 and cache simultaneously
/// - Requirement 1.2: Write data in chunks as received
/// - Requirement 1.3: Commit cached data on S3 success
/// - Requirement 1.4: Discard cached data on S3 error
/// - Requirement 8.1: Handle cache write failures gracefully
/// - Requirement 8.2: Clean up cached data on S3 error
pub struct SignedPutHandler {
    /// Base directory for cache storage
    cache_dir: PathBuf,
    /// Compression handler for cache writes
    compression_handler: CompressionHandler,
    /// Current cache usage in bytes
    current_cache_usage: u64,
    /// Maximum cache capacity in bytes
    max_cache_capacity: u64,
    /// Metrics manager for tracking PUT caching operations
    metrics_manager: Option<Arc<RwLock<MetricsManager>>>,
    /// Cache manager for HEAD cache invalidation
    cache_manager: Option<Arc<crate::cache::CacheManager>>,
    /// S3 client for comprehensive response header extraction
    s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
    /// Proxy identification Referer header value (None when disabled)
    proxy_referer: Option<String>,
    /// Maximum CompleteMultipartUpload body size (default: 10 MiB).
    /// The Complete XML body is bounded to this cap; bodies exceeding it are rejected
    /// with HTTP 413, preventing unbounded memory consumption.
    max_complete_body_bytes: u64,
    /// Bounded depth (in frames) of the streaming write-cache tee channel
    /// (`server.write_cache_tee_channel_depth`). One in-flight frame plus this many
    /// queued frames is the whole per-request streaming cache memory budget — see
    /// the streaming-write-path design (Req 1.4, 2.2, 2.3).
    write_cache_tee_channel_depth: usize,
    /// Permits bounding pending write-through cache stores: the process-wide
    /// [`PENDING_CACHE_WRITES`], or a test's own small pool.
    pending_cache_writes: Arc<tokio::sync::Semaphore>,
    /// What this request's cache waits may still delay its response. The router hands
    /// over what its own reads left; a detached clone has no limit, since nothing waits on it.
    cache_delay_budget: crate::cache::CacheDelayBudget,
}

impl SignedPutHandler {
    /// Create a new SignedPutHandler
    ///
    /// # Arguments
    ///
    /// * `cache_dir` - Base directory for cache storage
    /// * `compression_handler` - Handler for compressing cached data
    /// * `current_cache_usage` - Current cache usage in bytes
    /// * `max_cache_capacity` - Maximum cache capacity in bytes
    /// * `max_complete_body_bytes` - Maximum CompleteMultipartUpload body size
    pub fn new(
        cache_dir: PathBuf,
        compression_handler: CompressionHandler,
        current_cache_usage: u64,
        max_cache_capacity: u64,
        proxy_referer: Option<String>,
        max_complete_body_bytes: u64,
        write_cache_tee_channel_depth: usize,
    ) -> Self {
        Self {
            cache_dir,
            compression_handler,
            current_cache_usage,
            max_cache_capacity,
            metrics_manager: None,
            cache_manager: None,
            s3_client: None,
            proxy_referer,
            max_complete_body_bytes,
            write_cache_tee_channel_depth,
            pending_cache_writes: Arc::clone(&PENDING_CACHE_WRITES),
            cache_delay_budget: crate::cache::CacheDelayBudget::new(),
        }
    }
    /// Use `budget`, the write's cache-delay budget after the router's reads.
    pub(crate) fn set_cache_delay_budget(&mut self, budget: crate::cache::CacheDelayBudget) {
        self.cache_delay_budget = budget;
    }

    /// Test seam: bound this handler's pending cache stores with `permits` instead of the
    /// process-wide pool, so a test can fill the bound without starving parallel tests.
    #[cfg(test)]
    fn set_pending_cache_writes(&mut self, permits: Arc<tokio::sync::Semaphore>) {
        self.pending_cache_writes = permits;
    }

    /// A handler with this one's configuration, for cache work that outlives the request.
    fn detached_clone(&self) -> Self {
        Self {
            cache_dir: self.cache_dir.clone(),
            compression_handler: self.compression_handler.clone(),
            current_cache_usage: self.current_cache_usage,
            max_cache_capacity: self.max_cache_capacity,
            metrics_manager: self.metrics_manager.clone(),
            cache_manager: self.cache_manager.clone(),
            s3_client: self.s3_client.clone(),
            proxy_referer: self.proxy_referer.clone(),
            max_complete_body_bytes: self.max_complete_body_bytes,
            write_cache_tee_channel_depth: self.write_cache_tee_channel_depth,
            pending_cache_writes: Arc::clone(&self.pending_cache_writes),
            cache_delay_budget: crate::cache::CacheDelayBudget::with_total(
                std::time::Duration::MAX,
            ),
        }
    }

    /// Admit one write-through cache store against [`MAX_PENDING_CACHE_WRITES`], before
    /// anything it retains (the setup task, the `.tmp` file, the batch buffer) exists. The
    /// permit moves into the detached store and is released when that task ends.
    ///
    /// `None` means the bound is full: the caller forwards uncached, and this records
    /// `skipped_puts_total{backlog_full}`.
    ///
    /// It also means another live process holds this one's exact `{host}:{pid}`
    /// ([`identity_shared_with_a_live_peer`]), in which case nothing is admitted for the rest of
    /// this process's life and the reason is `hostname_not_unique`. Two processes on one identity
    /// share the per-instance files that carry the staged accounting's sequence, so a staged
    /// range admitted here could be credited with a number a `Fold_Cut` has already passed and
    /// then counted by neither term. Not caching is the bounded outcome; the upload itself is
    /// forwarded and served exactly as it would be with write caching switched off.
    async fn admit_pending_cache_write(
        &self,
        cache_key: &str,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        if identity_shared_with_a_live_peer(&self.cache_dir) {
            debug!(
                "Another process holds this proxy's identity, forwarding uncached: cache_key={}",
                cache_key
            );
            if let Some(metrics) = &self.metrics_manager {
                metrics
                    .read()
                    .await
                    .record_skipped_put(HOSTNAME_NOT_UNIQUE_SKIP_REASON)
                    .await;
            }
            return None;
        }
        match Arc::clone(&self.pending_cache_writes).try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                debug!(
                    "Write-through cache backlog full ({} stores pending), forwarding uncached: cache_key={}",
                    MAX_PENDING_CACHE_WRITES, cache_key
                );
                if let Some(metrics) = &self.metrics_manager {
                    metrics
                        .read()
                        .await
                        .record_skipped_put(BACKLOG_FULL_SKIP_REASON)
                        .await;
                }
                None
            }
        }
    }

    /// Set the metrics manager for tracking PUT caching operations
    ///
    /// # Arguments
    ///
    /// * `metrics_manager` - Metrics manager instance
    pub fn set_metrics_manager(&mut self, metrics_manager: Arc<RwLock<MetricsManager>>) {
        self.metrics_manager = Some(metrics_manager);
    }

    /// Set the cache manager for HEAD cache invalidation
    ///
    /// # Arguments
    ///
    /// * `cache_manager` - Cache manager instance
    pub fn set_cache_manager(&mut self, cache_manager: Arc<crate::cache::CacheManager>) {
        self.cache_manager = Some(cache_manager);
    }

    /// Set the S3 client for comprehensive response header extraction
    ///
    /// # Arguments
    ///
    /// * `s3_client` - S3 client instance
    pub(crate) fn set_s3_client(&mut self, s3_client: Arc<dyn S3ClientApi + Send + Sync>) {
        self.s3_client = Some(s3_client);
    }

    /// Handle a signed PUT request with caching
    ///
    /// This is the main orchestration method that:
    /// 1. Decides whether to cache based on capacity
    /// 2. Streams the request body to both S3 and cache
    /// 3. Commits or discards the cache based on S3 response
    ///
    /// # Arguments
    ///
    /// * `req` - The incoming HTTP request
    /// * `cache_key` - Cache key for storing the object
    /// * `target_host` - Target S3 hostname
    /// * `transport` - Resolved upstream transport (connect IP/port + TLS-or-plaintext)
    ///
    /// # Returns
    ///
    /// Returns the S3 response, with caching handled transparently
    ///
    /// # Requirements
    ///
    /// - Requirement 1.1: Stream to both S3 and cache simultaneously
    /// - Requirement 1.3: Commit on S3 success
    /// - Requirement 1.4: Discard on S3 error
    /// - Requirement 2.1: Check capacity before caching
    /// - Requirement 8.1: Handle errors gracefully
    /// - Requirement 5.1: Detect and cache UploadPart requests
    /// - Requirement 5.3: Handle CompleteMultipartUpload
    /// - Requirement 4.1: Handle CreateMultipartUpload
    pub(crate) async fn handle_signed_put(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        self.handle_put(req, cache_key, target_host, transport)
            .await
    }

    /// Handle a PUT without an Authorization header using the same streaming cache
    /// pipeline as signed PUTs. A cache-capacity bypass still streams verbatim: the
    /// request is forward-only, so buffering it would recreate the unsigned-write
    /// memory cliff.
    pub async fn handle_unsigned_put(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        self.handle_put(req, cache_key, target_host, transport)
            .await
    }

    async fn handle_put(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Check if this is a multipart upload request
        let uri = req.uri();
        let query = uri.query().unwrap_or("");

        // A PUT that does not carry the object's bytes (a subresource such as `?tagging`, a
        // CopyObject or an UploadPartCopy) is never stored. Checked before the multipart
        // routing so an UploadPartCopy does not record an empty part.
        if req.method() == hyper::Method::PUT && is_non_object_put(query, req.headers()) {
            return self
                .forward_non_object_put(req, cache_key, target_host, transport)
                .await;
        }

        // Detect CreateMultipartUpload request (Requirement 4.1)
        // POST with ?uploads query parameter initiates a multipart upload
        if Self::is_create_multipart_upload(query) {
            return self
                .handle_create_multipart_upload(req, cache_key, target_host, transport)
                .await;
        }

        // Detect UploadPart request (Requirement 5.1)
        if let Some((upload_id, part_number)) = Self::parse_upload_part_query(query) {
            return self
                .handle_upload_part(
                    req,
                    cache_key,
                    target_host,
                    transport,
                    upload_id,
                    part_number,
                )
                .await;
        }

        // Detect AbortMultipartUpload request (Requirement 4.5)
        // AbortMultipartUpload is a DELETE request with uploadId
        // Must check before CompleteMultipartUpload since both have uploadId without partNumber
        if req.method() == hyper::Method::DELETE && Self::is_abort_multipart_upload(query) {
            let upload_id = Self::extract_upload_id(query).unwrap_or_default();
            return self
                .handle_abort_multipart_upload(req, cache_key, target_host, transport, upload_id)
                .await;
        }

        // Detect CompleteMultipartUpload request (Requirement 5.3)
        // CompleteMultipartUpload is a POST request with uploadId
        if Self::is_complete_multipart_upload(query) {
            let upload_id = Self::extract_upload_id(query).unwrap_or_default();
            return self
                .handle_complete_multipart_upload(req, cache_key, target_host, transport, upload_id)
                .await;
        }

        info!("Handling signed PUT request for cache key: {}", cache_key);

        // Extract request headers for metadata and capacity checking
        let request_headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Extract Content-Length for capacity checking
        let content_length = request_headers
            .get("content-length")
            .or_else(|| request_headers.get("Content-Length"))
            .and_then(|v| v.parse::<u64>().ok());

        debug!(
            "Signed PUT request: cache_key={}, content_length={:?}",
            cache_key, content_length
        );

        // Compute bytes_uploaded for per-bucket traffic accounting (Design §4b).
        // Use x-amz-decoded-content-length for aws-chunked bodies; else Content-Length.
        let is_aws_chunked = aws_chunked_decoder::is_aws_chunked(&request_headers);
        let body_bytes_uploaded = if is_aws_chunked {
            aws_chunked_decoder::get_decoded_content_length(&request_headers).unwrap_or(0)
        } else {
            content_length.unwrap_or(0)
        };

        // Save bucket before cache_key is consumed by the match arms.
        let (bucket_ref, _) = parse_cache_key(&cache_key);
        let bucket_owned = bucket_ref.to_string();

        // Decide whether to cache based on capacity (Requirement 2.1)
        let cache_decision = self.should_cache(content_length);

        let result = match cache_decision {
            CacheDecision::Cache => {
                info!("Caching signed PUT request: {}", cache_key);
                // Stream to both S3 and cache
                self.handle_with_caching(
                    req,
                    cache_key,
                    target_host,
                    transport,
                    request_headers,
                    content_length,
                )
                .await
            }
            CacheDecision::Bypass(reason) => {
                // Log bypass decision (Requirement 2.5)
                log_bypass_decision(&cache_key, &reason);
                debug!("Bypassing cache for signed PUT: {}", cache_key);
                // Record bypassed PUT (Requirement 9.2)
                if let Some(metrics) = &self.metrics_manager {
                    metrics.read().await.record_bypassed_put().await;
                }
                // A cache-capacity bypass is forward-only. Stream it verbatim so
                // cache misses do not buffer an object the cache will not retain.
                // If the client disconnects mid-upload, S3 sees a partial upload and
                // rejects it, matching the cached signed PUT streaming path.
                let response = forward_signed_request_streaming_verbatim(
                    req,
                    &target_host,
                    &transport,
                    self.proxy_referer.as_deref(),
                    STREAMED_BODY_CAP,
                )
                .await;
                // Nothing else invalidates on this arm: no cache task is spawned.
                if matches!(&response, Ok(resp) if resp.status().is_success()) {
                    self.invalidate_prior_version_before_response(&cache_key, "PUT")
                        .await;
                }
                response
            }
            CacheDecision::StreamWithCapacityCheck => {
                info!("Streaming signed PUT with capacity check: {}", cache_key);
                // Stream with capacity checking during upload
                self.handle_with_streaming_capacity_check(
                    req,
                    cache_key,
                    target_host,
                    transport,
                    request_headers,
                )
                .await
            }
        };

        // Per-bucket traffic accounting — Spec: per-bucket-metrics, Req 2.1, 2.2, 2.3
        // Record once at the completion of the full request-response cycle (Req 2.3).
        // Skip if bucket is empty (Req 2.4). PutObject response body is empty (bytes_served = 0).
        if !bucket_owned.is_empty() {
            if let Some(metrics) = &self.metrics_manager {
                metrics
                    .read()
                    .await
                    .record_bucket_traffic(
                        &bucket_owned,
                        None, // prefix: no per-bucket prefix config in this handler; bucket-level
                        RequestType::Put,
                        0, // bytes_served: PutObject response body is empty
                        0, // bytes_saved: PUT is not a cache hit
                        body_bytes_uploaded,
                    )
                    .await;
            }
        }

        result
    }

    /// Forward a PUT whose body is not the object's bytes (see [`is_non_object_put`])
    /// verbatim and uncached, and on success invalidate and stamp the key on the response
    /// path, as an object PUT does. An ACL or tagging change leaves the bytes alone while a
    /// copy replaces them; invalidating on all of them is the one rule that is right for
    /// every case.
    async fn forward_non_object_put(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        debug!(
            "Non-object PUT (subresource, copy or append) forwarded without caching: cache_key={}",
            cache_key
        );
        if let Some(metrics) = &self.metrics_manager {
            metrics
                .read()
                .await
                .record_skipped_put(NON_OBJECT_PUT_SKIP_REASON)
                .await;
        }
        let response = forward_signed_request_streaming_verbatim(
            req,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
        )
        .await;
        if matches!(&response, Ok(resp) if resp.status().is_success()) {
            self.invalidate_prior_version_before_response(&cache_key, "PUT")
                .await;
        }
        response
    }

    /// Determine whether a PUT request should be cached
    ///
    /// # Arguments
    ///
    /// * `content_length` - Optional Content-Length from request headers
    ///
    /// # Returns
    ///
    /// Returns a CacheDecision indicating whether to cache, bypass, or stream with checks
    ///
    /// # Requirements
    ///
    /// - Requirement 2.1: Check if Content-Length fits within available capacity
    /// - Requirement 2.2: Bypass if Content-Length exceeds capacity
    /// - Requirement 2.3: Stream with capacity check if no Content-Length
    fn should_cache(&self, content_length: Option<u64>) -> CacheDecision {
        check_cache_capacity(
            content_length,
            self.current_cache_usage,
            self.max_cache_capacity,
        )
    }

    /// Handle signed PUT with caching (Content-Length known and fits).
    ///
    /// Streams the request body to the upstream **verbatim** while teeing it to the
    /// write cache incrementally (streaming-write-path Component 5), instead of
    /// buffering the whole object in RAM. `should_cache` already decided this object
    /// fits, so a cache tee is opened when caching is viable; with no tee the body
    /// still streams to the upstream.
    ///
    /// # Requirements
    ///
    /// - Requirement 1.1: Stream the body to S3 without holding the whole object in RAM
    /// - Requirement 6.1: Single-part PUT streams per Requirements 1–5
    /// - Requirement 7.2: A cache skip never alters the forwarded bytes or response
    #[allow(clippy::too_many_arguments)]
    async fn handle_with_caching(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        request_headers: HashMap<String, String>,
        content_length: Option<u64>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        self.stream_put_to_upstream(
            req,
            cache_key,
            target_host,
            transport,
            request_headers,
            content_length,
        )
        .await
    }

    /// Stream a single-part PUT body to the upstream verbatim, optionally teeing it
    /// to the write cache. Shared by [`Self::handle_with_caching`] (Content-Length
    /// known and fits) and [`Self::handle_with_streaming_capacity_check`] (no
    /// Content-Length).
    ///
    /// This replaces the former buffer-then-forward implementation
    /// (`read_request_body_bounded` + inline `raw_request` assembly +
    /// `forward_raw_request_to_s3`) with [`forward_signed_request_streaming`]: the
    /// client body frames flow straight to the upstream (the awaited socket write is
    /// the primary backpressure), and the same frames are tee'd to a bounded channel
    /// feeding the incremental write-cache task when caching is viable. The upstream
    /// always receives the original bytes byte-for-byte (SigV4 intact); only the
    /// cache branch decodes aws-chunked (now done incrementally inside the cache
    /// task, not up front).
    ///
    /// Cache viability is decided by [`Self::setup_put_cache_tee`]; when it returns
    /// no tee, `tee = None` is passed and the body still streams to the upstream
    /// (Req 7.2). After the forward returns, the S3 `ResponseInfo`/error is delivered
    /// to the background cache task (if any) exactly as the buffered path did, and
    /// the S3 response is returned to the client unchanged.
    #[allow(clippy::too_many_arguments)]
    async fn stream_put_to_upstream(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        request_headers: HashMap<String, String>,
        content_length: Option<u64>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Extract request components before consuming the body into a stream.
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();
        let body = req.into_body();

        // The cache branch decodes aws-chunked incrementally; the decoded object
        // length comes from `x-amz-decoded-content-length` for aws-chunked, else the
        // Content-Length. The upstream always receives the original bytes verbatim.
        let is_aws_chunked = aws_chunked_decoder::is_aws_chunked(&request_headers);
        let decoded_len = if is_aws_chunked {
            aws_chunked_decoder::get_decoded_content_length(&request_headers)
        } else {
            content_length
        };

        // Decide whether to cache and, if so, open the sink + spawn the cache task.
        // `tee` is the send side handed to the streaming forward; `s3_result_tx` is
        // the channel the cache task waits on for the S3 result.
        let (tee, s3_result_tx) = self
            .setup_put_cache_tee(&cache_key, &request_headers, is_aws_chunked, decoded_len)
            .await;

        // Stream the body to the upstream verbatim (Req 1.1, 4.1). `tee = None`
        // means no caching, but the body still streams (Req 7.2). The body-size cap
        // is enforced without buffering the whole body inside the streaming forward.
        let s3_response = forward_signed_request_streaming(
            &method,
            &uri,
            &headers,
            version,
            body,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
            tee,
        )
        .await;

        // Invalidate the key's previous cached version before the client gets its 200,
        // whether or not this PUT ends up cached. It runs before the S3 result is sent to
        // the cache task, so the task (which waits for that result before it invalidates
        // and publishes) cannot have published yet, and this cannot remove the new entry.
        // The same call stamps the key's write-order marker; its token goes to the cache task
        // with the S3 result, so the task can publish only while no newer write of the key
        // has been acknowledged.
        let write_guard = if matches!(&s3_response, Ok(resp) if resp.status().is_success()) {
            self.invalidate_prior_version_before_response(&cache_key, "PUT")
                .await
        } else {
            WriteGuard::Unavailable
        };

        // Deliver the S3 result (status + headers, or error) to the background cache
        // task through the oneshot, exactly as the buffered path did. On success the
        // task finalizes + commits the cached range; on error/non-success it discards.
        if let Some(s3_result_tx) = s3_result_tx {
            let response_info = match &s3_response {
                Ok(resp) => Ok(ResponseInfo {
                    status: resp.status(),
                    headers: resp.headers().clone(),
                    write_guard,
                }),
                Err(e) => Err(e.clone()),
            };
            let _ = s3_result_tx.send(response_info);
        }

        // Return the S3 response to the client unchanged (Req 5.5).
        s3_response
    }

    /// Invalidate `cache_key`'s previously cached version on the response path, after S3
    /// accepted the write and before its response is returned to the client.
    ///
    /// The background cache task invalidates too, just before it publishes, but it runs
    /// after the client's 200 and does not run at all when the PUT is not cached (capacity
    /// bypass, declined or abandoned setup, a full backlog) or when the task is lost to a
    /// restart. Without this call each of those leaves the old bytes served from cache, on
    /// every proxy sharing the volume, until their TTL, because hits at TTL > 0 never
    /// contact S3. This is the same call the `write_cache_enabled: false` path makes.
    ///
    /// A failure is logged and the response still returns: S3 holds the new object and the
    /// client's write succeeded.
    ///
    /// The same slot-lock hold stamps the key's write-order marker; the returned guard
    /// is what this write's store must carry. On failure it is `Unavailable` and the store
    /// declines.
    async fn invalidate_prior_version_before_response(
        &self,
        cache_key: &str,
        operation: &str,
    ) -> WriteGuard {
        let Some(cache_manager) = &self.cache_manager else {
            return WriteGuard::Unavailable;
        };
        // Within the write's cache-delay budget; a timeout is an `Err` like any other.
        match cache_manager
            .invalidate_for_write_within(cache_key, &self.cache_delay_budget)
            .await
        {
            Ok(token) => WriteGuard::Token(token),
            Err(e) => {
                warn!(
                    "Failed to invalidate the prior cached version before returning a successful \
                     {}: cache_key={}, error={}; retrying in the background, and until it lands \
                     an older write may be served from cache",
                    operation, cache_key, e
                );
                // The marker did not move, so an older write's pending store can still publish.
                // The retry's stamp, when it lands, removes that publish and supersedes the
                // store.
                cache_manager.spawn_write_stamp_retry(cache_key);
                WriteGuard::Unavailable
            }
        }
    }

    /// Set up the streaming write-cache tee for a single-part PUT, when caching is
    /// viable. Returns `(tee_sender, s3_result_sender)`:
    ///
    /// - `tee_sender: Some` when a streaming cache sink was opened and a background
    ///   [`Self::run_streaming_cache_write`] task spawned to consume it; pass it to
    ///   [`forward_signed_request_streaming`].
    /// - `s3_result_sender: Some` whenever a background cache task (streaming, or the
    ///   empty-object metadata-only task) is waiting for the S3 result; the caller
    ///   must send the `ResponseInfo`/error into it after the forward returns.
    ///
    /// Both `None` means no caching for this request — the body still streams to the
    /// upstream verbatim (Req 7.2). Caching is skipped (no tee) when: there is no
    /// cache manager; the decoded object length is unknown (e.g. non-chunked with no
    /// Content-Length, or aws-chunked without `x-amz-decoded-content-length`, since
    /// the sink must be sized up front); or write-cache capacity cannot be reserved.
    /// Empty objects (decoded length 0) are cached via the metadata-only buffered
    /// path (the streaming sink rejects a zero-length open), preserving the buffered
    /// path's empty-object cache-hit behaviour.
    async fn setup_put_cache_tee(
        &self,
        cache_key: &str,
        request_headers: &HashMap<String, String>,
        is_aws_chunked: bool,
        decoded_len: Option<u64>,
    ) -> (
        Option<tokio::sync::mpsc::Sender<Bytes>>,
        Option<tokio::sync::oneshot::Sender<Result<ResponseInfo>>>,
    ) {
        // Caching requires a cache manager.
        let cache_manager = match &self.cache_manager {
            Some(cm) => cm.clone(),
            None => return (None, None),
        };

        let decoded_len = match decoded_len {
            Some(n) => n,
            None => {
                // Decoded object length unknown: we cannot size the streaming sink,
                // so skip caching (parity with today's capacity decisions). The body
                // still streams to the upstream.
                debug!(
                    "Streaming PUT: decoded object length unknown, skipping cache: cache_key={}",
                    cache_key
                );
                if let Some(metrics) = &self.metrics_manager {
                    metrics
                        .read()
                        .await
                        .record_skipped_put("unknown_length")
                        .await;
                }
                return (None, None);
            }
        };

        // Bound the acknowledged-but-uncommitted backlog. Taken before the setup task is
        // spawned, because that task opens the `.tmp` and allocates the batch buffer, and
        // may hold a blocking thread while it does; the permit moves into the store task
        // and is released when that task ends.
        let Some(permit) = self.admit_pending_cache_write(cache_key).await else {
            return (None, None);
        };

        // Empty object: there is no range to stream. Cache via the metadata-only
        // buffered path (the streaming sink rejects a zero-length open), so an
        // immediate post-PUT GET/HEAD still hits, matching the buffered path.
        if decoded_len == 0 {
            let (s3_result_tx, s3_result_rx) =
                tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
            Self::spawn_cache_write_task(
                cache_key.to_string(),
                Bytes::new(),
                s3_result_rx,
                self.cache_dir.clone(),
                self.compression_handler.clone(),
                Some(0),
                request_headers.clone(),
                self.metrics_manager.clone(),
                Some(cache_manager),
                self.s3_client.clone(),
                permit,
            );
            return (None, Some(s3_result_tx));
        }

        // Reserve write-cache capacity and open the streaming sink. A failed
        // reservation (insufficient capacity) or open simply skips caching — the
        // body still streams verbatim (Req 7.2).
        //
        // The client's body read must not wait on cache bookkeeping. The tee is handed
        // to the forward at once and the sink open (every shared-storage await of the setup)
        // runs concurrently on its own task, still under `WRITE_CACHE_SETUP_BOUND`.
        // Frames the forward sends meanwhile wait in the tee's channel, `write_cache_tee_
        // channel_depth` deep. If setup is still running when that buffer is full, the relay
        // waits for it at most `WRITE_CACHE_SETUP_WAIT_BOUND`, then drops the tee and
        // the PUT forwards uncached (`setup_abandoned_total_put`). Invalidation of the key's previous version is not
        // part of this setup: `stream_put_to_upstream` runs it after S3 succeeds and before
        // the response returns, and the cache task runs it again before the new `.bin` is
        // published (`run_streaming_cache_write`).
        let setup_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let BoundedCacheTee {
            forward_tx: tee_tx,
            cache_rx: tee_rx,
            s3_result_tx,
            s3_result_rx,
        } = bounded_cache_tee_gated(
            self.write_cache_tee_channel_depth,
            WRITE_CACHE_TEE_STALL_BOUND,
            cache_key,
            Some(setup_ready.clone()),
            self.cache_delay_budget.clone(),
        );
        let task_key = cache_key.to_string();
        let task_headers = request_headers.clone();
        let metrics_manager = self.metrics_manager.clone();
        let s3_client = self.s3_client.clone();
        tokio::spawn(async move {
            let setup_manager = cache_manager.clone();
            let setup_key = task_key.clone();
            // The permit lives inside the setup future, so a setup abandoned at the bound
            // (which runs on and may still open the `.tmp` and batch buffer) holds it until
            // it finishes; the late result, permit included, drops with the detached handle.
            // On success it comes back out and moves into the store task. Every early return
            // below releases it.
            let setup = bounded_cache_setup(WRITE_CACHE_SETUP_BOUND, &task_key, async move {
                let permit = permit;
                let result = match setup_manager
                    .open_write_cache_sink(&setup_key, decoded_len)
                    .await
                {
                    Ok(Some(sink)) => {
                        crate::cache::write_cache_setup_step(&setup_key, "ttl");
                        let ttl = setup_manager.get_effective_put_ttl(&setup_key).await;
                        Ok(Some((sink, ttl)))
                    }
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                };
                crate::cache::write_cache_setup_finished(&setup_key);
                // Tuple fields drop in order, so a late sink aborts its `.tmp` before the
                // permit is released.
                (result, permit)
            })
            .await;
            // Every early return below drops `tee_rx` and `s3_result_rx`: the relay sees a
            // closed cache side and the forward continues uncached (Req 7.2).
            let Some((setup, permit)) = setup else {
                return;
            };
            let (sink, ttl) = match setup {
                Ok(Some(opened)) => opened,
                Ok(None) => {
                    debug!(
                        "Streaming PUT: write-cache capacity unavailable, skipping cache: cache_key={}",
                        task_key
                    );
                    if let Some(metrics) = &metrics_manager {
                        // `open_write_cache_sink` returns `Ok(None)` for two distinct
                        // reasons, distinguished here rather than inside it so its control
                        // flow stays untouched. `capacity_refused` is deliberately not one of
                        // them: the Staging_Bound stopped being an admission gate in 2.7.0
                        // (R3.1), so the remaining refusals are the per-object size cap and the
                        // Disk_Safety_Bound.
                        let max_object_size = cache_manager.get_write_cache_max_object_size().await;
                        let reason = if decoded_len > max_object_size {
                            "object_too_large"
                        } else {
                            crate::cache::DISK_SAFETY_SKIP_REASON
                        };
                        metrics.read().await.record_skipped_put(reason).await;
                    }
                    return;
                }
                Err(e) => {
                    warn!(
                        "Streaming PUT: failed to open write-cache sink, skipping cache (upload unaffected): cache_key={}, error={}",
                        task_key, e
                    );
                    return;
                }
            };
            Self::spawn_streaming_cache_write_task(
                task_key,
                sink,
                tee_rx,
                s3_result_rx,
                is_aws_chunked,
                Some(decoded_len),
                ttl,
                task_headers,
                metrics_manager,
                Some(cache_manager),
                s3_client,
                permit,
            );
            setup_ready.store(true, std::sync::atomic::Ordering::Release);
        });

        (Some(tee_tx), Some(s3_result_tx))
    }

    /// Handle signed PUT with streaming capacity check (no Content-Length).
    ///
    /// Streams the request body to the upstream verbatim while teeing it to the
    /// write cache incrementally, instead of buffering the whole object in RAM.
    /// With no Content-Length up front, caching is gated on
    /// `x-amz-decoded-content-length` (aws-chunked, needed to size the sink) and an
    /// atomic write-cache capacity reservation; an unsizable body skips caching but
    /// still streams to the upstream.
    ///
    /// # Requirements
    ///
    /// - Requirement 1.1: Stream the body to S3 without holding the whole object in RAM
    /// - Requirement 6.1: Single-part PUT streams per Requirements 1–5
    /// - Requirement 7.2: A cache skip never alters the forwarded bytes or response
    async fn handle_with_streaming_capacity_check(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        request_headers: HashMap<String, String>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // No Content-Length up front: pass `None`, so caching is gated on
        // `x-amz-decoded-content-length` (aws-chunked) — needed to size the sink —
        // and an atomic write-cache reservation. A non-chunked body with no
        // Content-Length cannot be sized, so it skips caching while still streaming
        // verbatim to the upstream. The former up-front
        // `available_capacity` comparison is subsumed by `try_reserve_write_cache`
        // inside `setup_put_cache_tee` → `open_write_cache_sink`.
        self.stream_put_to_upstream(
            req,
            cache_key,
            target_host,
            transport,
            request_headers,
            None,
        )
        .await
    }

    // ============================================================================
    // Multipart Upload Handling Methods
    // ============================================================================

    /// Parse query string to detect UploadPart request
    ///
    /// Returns (upload_id, part_number) if this is an UploadPart request
    ///
    /// # Requirements
    ///
    /// - Requirement 5.1: Detect UploadPart requests
    fn parse_upload_part_query(query: &str) -> Option<(String, u32)> {
        let mut upload_id: Option<String> = None;
        let mut part_number: Option<u32> = None;

        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                match key {
                    "uploadId" => upload_id = Some(value.to_string()),
                    "partNumber" => part_number = value.parse().ok(),
                    _ => {}
                }
            }
        }

        match (upload_id, part_number) {
            (Some(id), Some(num)) => Some((id, num)),
            _ => None,
        }
    }

    /// Check if this is a CompleteMultipartUpload request
    ///
    /// # Requirements
    ///
    /// - Requirement 5.3: Handle CompleteMultipartUpload
    fn is_complete_multipart_upload(query: &str) -> bool {
        query_has_param(query, "uploadId") && !query_has_param(query, "partNumber")
    }

    /// Extract upload ID from query string
    fn extract_upload_id(query: &str) -> Option<String> {
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                if key == "uploadId" {
                    return Some(value.to_string());
                }
            }
        }
        None
    }

    /// Check if this is a CreateMultipartUpload request
    ///
    /// CreateMultipartUpload is a POST request with ?uploads query parameter
    /// (no uploadId yet, as that's returned by S3)
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: Detect CreateMultipartUpload requests
    ///
    /// An `uploads` parameter and no `uploadId` parameter, by name. `HttpProxy`'s router
    /// calls this too, so the route and the dispatch cannot disagree.
    pub(crate) fn is_create_multipart_upload(query: &str) -> bool {
        let mut uploads = false;
        for name in query.split('&').map(|p| p.split('=').next().unwrap_or("")) {
            if name == "uploadId" {
                return false;
            }
            uploads |= name == "uploads";
        }
        uploads
    }

    /// Check if this is an AbortMultipartUpload request
    ///
    /// AbortMultipartUpload is a DELETE request with uploadId query parameter
    ///
    /// # Requirements
    ///
    /// - Requirement 4.5: Detect AbortMultipartUpload requests
    fn is_abort_multipart_upload(query: &str) -> bool {
        // AbortMultipartUpload has uploadId in query
        // The query typically contains "uploadId=..."
        query_has_param(query, "uploadId") && !query_has_param(query, "partNumber")
    }

    /// Extract upload ID from CreateMultipartUpload XML response
    ///
    /// S3 returns the uploadId in the XML body:
    /// ```xml
    /// <InitiateMultipartUploadResult>
    ///   <Bucket>bucket-name</Bucket>
    ///   <Key>object-key</Key>
    ///   <UploadId>upload-id-value</UploadId>
    /// </InitiateMultipartUploadResult>
    /// ```
    fn extract_upload_id_from_xml(xml: &str) -> Option<String> {
        debug!("Attempting to extract UploadId from CreateMultipartUpload XML response (length: {} bytes)", xml.len());

        // Simple XML parsing to extract UploadId value
        // Look for <UploadId>value</UploadId> pattern (case-insensitive)
        if let Some(start_pos) = find_ascii_case_insensitive(xml, "<uploadid>") {
            let after_tag = &xml[start_pos + "<uploadid>".len()..];
            if let Some(end_pos) = find_ascii_case_insensitive(after_tag, "</uploadid>") {
                let upload_id = after_tag[..end_pos].trim().to_string();
                info!("Successfully extracted UploadId from XML: {}", upload_id);
                return Some(upload_id);
            }
        }

        // Fallback: return None if UploadId not found
        warn!(
            "Failed to extract UploadId from CreateMultipartUpload response XML. XML content: {}",
            xml.get(..500).unwrap_or(xml)
        );
        None
    }

    /// Handle CreateMultipartUpload request
    ///
    /// This method:
    /// 1. Forwards the request to S3
    /// 2. Parses the uploadId from S3 response XML
    /// 3. Creates mpus_in_progress/{uploadId}/upload.meta to track the upload
    /// 4. Returns S3 response unchanged to client
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: Record uploadId and start time when multipart upload is initiated
    async fn handle_create_multipart_upload(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        let (bucket, key) = parse_cache_key(&cache_key);

        // R16.10 (cache-eviction-at-scale, reinstated per design-review-2026-08-30
        // write-through caching of this upload, not the upload itself, is refused
        // when the cache volume lacks free space, because staged parts land under
        // `mpus_in_progress/`, which eviction cannot reach. The check reads the shared volume
        // (`size_state.json`, then `statvfs`), so it runs on the tracker task below, inside the
        // same bound as the tracker write, and never in front of the forward. A refusal writes
        // no tracker; parts may still stage, and CompleteMultipartUpload then declines to cache
        // the upload and removes them. Checked with a zero incoming-bytes reservation: the
        // object size is unknown at CreateMultipartUpload.
        // Spec: cache-eviction-at-scale. Requirements: 16.9, 16.10, 16.11, 16.14.

        // Extract Content-Type from request headers before forwarding
        // This is optional - if provided, we cache it for use in CompleteMultipartUpload
        let content_type = req
            .headers()
            .get("content-type")
            .or_else(|| req.headers().get("Content-Type"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // The completed object's representation headers are the ones this request sets.
        let representation_headers = {
            let request_headers: HashMap<String, String> = req
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            let mut recorded = HashMap::new();
            merge_representation_headers(&request_headers, &mut recorded);
            recorded
        };

        // CreateMultipartUpload is forward-only on the request side; only its
        // response XML is inspected below. Stream its request verbatim. A client
        // disconnect can therefore reach S3 as a partial request, matching the main
        // signed PUT streaming path.
        let s3_response = forward_signed_request_streaming_verbatim(
            req,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
        )
        .await?;

        let status = s3_response.status();
        let response_headers = s3_response.headers().clone();

        // Read response body to extract uploadId from XML
        let response_body_bytes = s3_response
            .into_body()
            .collect()
            .await
            .map(|collected| collected.to_bytes())
            .unwrap_or_default();

        if status.is_success() {
            // Parse uploadId from XML response
            let body_str = String::from_utf8_lossy(&response_body_bytes);

            if let Some(upload_id) = Self::extract_upload_id_from_xml(&body_str) {
                // The disk-safety check and the tracker write go to the shared volume, so they
                // run on their own task, and the response waits for them at most
                // `WRITE_CACHE_SETUP_WAIT_BOUND`, as UploadPart setup does. The task holds a
                // pending-store permit; with none free no tracker is written. CompleteMultipartUpload
                // does not cache an upload without a tracker, since the entry would lack the
                // representation headers recorded here. A tracker that lands after the response
                // stopped waiting is withdrawn for the same reason: the client may already be
                // completing the upload. Either way the client gets S3's answer.
                match self.admit_pending_cache_write(&cache_key).await {
                    Some(pending_permit) => {
                        let writer = self.detached_clone();
                        let task_key = cache_key.clone();
                        let (written_tx, written_rx) = tokio::sync::oneshot::channel::<()>();
                        tokio::spawn(async move {
                            let _pending_permit = pending_permit;
                            let (bucket, key) = parse_cache_key(&task_key);
                            if writer.create_disk_safety_refused().await {
                                if let Some(metrics) = &writer.metrics_manager {
                                    metrics
                                        .read()
                                        .await
                                        .record_skipped_put(crate::cache::DISK_SAFETY_SKIP_REASON)
                                        .await;
                                }
                                info!(
                                    "CreateMultipartUpload: declining write-through caching for this upload — cache \
                                     volume disk-safety bound breached: bucket={}, key={}. The upload itself is \
                                     unaffected and streams to S3.",
                                    bucket, key
                                );
                                return;
                            }
                            // Held until the tracker write ends (MAX_TRACKED_MULTIPART_ENTRIES).
                            let _tracking_slot =
                                match tracked_multipart_budget::admit(&writer.cache_dir, 1).await {
                                    Ok(slot) => slot,
                                    Err(refusal) => {
                                        note_tracked_multipart_refusal(
                                            &writer.metrics_manager,
                                            &writer.cache_dir,
                                            "CreateMultipartUpload",
                                            refusal,
                                            &task_key,
                                            &upload_id,
                                        )
                                        .await;
                                        return;
                                    }
                                };
                            let written = match writer
                                .create_multipart_upload_tracker_with_content_type(
                                    &task_key,
                                    &upload_id,
                                    content_type,
                                    &representation_headers,
                                )
                                .await
                            {
                                // The S3 operation succeeded; only the cache record failed.
                                Err(e) => {
                                    error!(
                                        "CreateMultipartUpload tracker failed: bucket={}, key={}, error={}",
                                        bucket, key, e
                                    );
                                    false
                                }
                                Ok(()) => {
                                    info!(
                                        "CreateMultipartUpload: bucket={}, key={}, upload_id={}",
                                        bucket,
                                        key,
                                        truncate_upload_id(&upload_id)
                                    );
                                    true
                                }
                            };
                            if written_tx.send(()).is_err() && written {
                                writer.withdraw_multipart_upload_tracker(&upload_id).await;
                                warn!(
                                    "CreateMultipartUpload: upload tracker landed after the client was answered and was withdrawn; the upload will not be cached: bucket={}, key={}",
                                    bucket, key
                                );
                            }
                        });
                        if self
                            .cache_delay_budget
                            .run(WRITE_CACHE_SETUP_WAIT_BOUND, written_rx)
                            .await
                            .is_none()
                        {
                            warn!(
                                "CreateMultipartUpload: cache setup still running after {:?}, answering the client; the upload will not be cached: bucket={}, key={}",
                                WRITE_CACHE_SETUP_WAIT_BOUND, bucket, key
                            );
                        }
                    }
                    None => warn!(
                        "CreateMultipartUpload: write-through cache backlog full, upload tracker not written, so the upload will not be cached (the upload itself is unaffected): bucket={}, key={}",
                        bucket, key
                    ),
                }
            } else {
                warn!(
                    "Could not extract uploadId from CreateMultipartUpload response: cache_key={}",
                    cache_key
                );
            }
        } else {
            debug!(
                "CreateMultipartUpload failed at S3: cache_key={}, status={}",
                cache_key, status
            );
        }

        // Rebuild response with the body we read (return S3 response unchanged)
        let mut response_builder = Response::builder().status(status);
        for (name, value) in response_headers.iter() {
            response_builder = response_builder.header(name, value);
        }
        let rebuilt_response = response_builder
            .body(
                Full::new(response_body_bytes)
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .map_err(|e| ProxyError::HttpError(format!("Failed to rebuild response: {}", e)))?;

        // CreateMultipartUpload is a POST and is out of scope for per-bucket traffic
        // (only GET object reads and PUT object/part writes are counted). The part data
        // bytes are recorded on the UploadPart path; the control POSTs are not counted.

        Ok(rebuilt_response)
    }

    /// R16.10's disk-safety verdict for a CreateMultipartUpload, `true` when caching the upload
    /// must be declined. `disk_safety_refusal` reads `size_state.json` and calls `statvfs`
    /// inline, so it runs on a blocking thread: a stalled volume must not hold a runtime
    /// worker. Fails open, as the check itself does (R7.5), if no verdict comes back.
    async fn create_disk_safety_refused(&self) -> bool {
        let Some(cache_manager) = self.cache_manager.clone() else {
            return false;
        };
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            handle.block_on(async move { cache_manager.disk_safety_refusal(0).await.is_some() })
        })
        .await
        .unwrap_or(false)
    }

    /// Remove an `upload.meta` that landed after its CreateMultipartUpload was answered, and
    /// the upload directory if nothing else is in it. A directory a part has staged into is
    /// left; CompleteMultipartUpload then finds no tracker, declines and removes it.
    async fn withdraw_multipart_upload_tracker(&self, upload_id: &str) {
        let upload_dir = mpu_upload_dir(&self.cache_dir, upload_id);
        let _ = tokio::fs::remove_file(upload_dir.join("upload.meta")).await;
        let _ = tokio::fs::remove_dir(&upload_dir).await;
    }

    /// Create a multipart upload tracker file with optional content-type
    ///
    /// Creates mpus_in_progress/{uploadId}/upload.meta with:
    /// - upload_id
    /// - cache_key
    /// - started_at timestamp
    /// - content_type (if provided in CreateMultipartUpload request)
    /// - empty parts list
    ///
    /// # Requirements
    ///
    /// - Requirement 4.1: Record uploadId and start time
    async fn create_multipart_upload_tracker_with_content_type(
        &self,
        cache_key: &str,
        upload_id: &str,
        content_type: Option<String>,
        representation_headers: &HashMap<String, String>,
    ) -> Result<()> {
        use crate::cache_types::MultipartUploadTracker;

        // Validate upload_id from upstream response. The path is a digest of it (see
        // `mpu_upload_dir`), so this no longer guards traversal; it keeps control
        // characters out of the log lines and tracker that do carry it.
        if !is_cacheable_upload_id(upload_id) {
            return Err(ProxyError::CacheError(format!(
                "upload_id from upstream response is empty or not printable ASCII: {}",
                truncate_upload_id(upload_id).escape_debug()
            )));
        }

        // Create directory for this upload
        let upload_dir = mpu_upload_dir(&self.cache_dir, upload_id);
        tokio::fs::create_dir_all(&upload_dir).await.map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to create multipart upload directory: {}",
                e
            ))
        })?;

        // Create tracker with content-type if provided
        let tracker = MultipartUploadTracker::new_with_content_type(
            upload_id.to_string(),
            cache_key.to_string(),
            content_type,
        );

        // Write tracker to file
        let tracker_path = upload_dir.join("upload.meta");
        // `representation_headers` sits beside the tracker's own fields: the tracker's reader
        // ignores it, and [`Self::recorded_representation_headers`] reads it at Complete.
        let tracker_json = serde_json::to_value(&tracker)
            .and_then(|mut value| {
                value["representation_headers"] = serde_json::to_value(representation_headers)?;
                serde_json::to_string_pretty(&value)
            })
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to serialize multipart upload tracker: {}",
                    e
                ))
            })?;

        tokio::fs::write(&tracker_path, tracker_json)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to write multipart upload tracker: {}", e))
            })?;

        debug!(
            "Created multipart upload tracker: upload_id={}, cache_key={}, content_type={:?}, path={:?}",
            upload_id, cache_key, tracker.content_type, tracker_path
        );

        Ok(())
    }

    /// Handle an `UploadPart` request by streaming the part body to the upstream
    /// verbatim while teeing it to a part-staging cache sink.
    ///
    /// Mirrors the single-part PUT streaming path ([`Self::stream_put_to_upstream`]):
    /// the client body frames flow straight to the upstream (the awaited socket
    /// write is the primary backpressure) and the same frames are tee'd to a bounded
    /// channel feeding [`Self::run_streaming_part_cache_write`], which incrementally
    /// decodes aws-chunked (cache branch only) and stages the part into
    /// `mpus_in_progress/{upload_id}/part{N}.bin`. The upstream always receives the
    /// original bytes byte-for-byte (SigV4 intact). The per-part correctness gate is
    /// preserved: the staged part is finalized and recorded in its own `part{N}.json`
    /// under `part{N}.lock` ([`Self::part_lock_path`]) only on S3 success.
    ///
    /// # Requirements
    ///
    /// - Requirement 6.2: `UploadPart` streams per Requirements 1–5 (bounded memory)
    /// - Requirement 5.1/5.2: cache each part as a range file at its byte offset
    /// - Requirement 7.1/7.2: a cache skip/failure never alters the forwarded bytes
    /// - Requirement 8.1/8.2: handle cache write failures gracefully
    #[allow(clippy::too_many_arguments)]
    async fn handle_upload_part(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        upload_id: String,
        part_number: u32,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // The staging path is a digest of the uploadId (`mpu_upload_dir`), so this is not
        // a traversal guard; it keeps control characters out of the logs and tracker.
        // On reject, forward to S3 unmodified (preserve SigV4 response) and skip cache work.
        if !is_cacheable_upload_id(&upload_id) {
            warn!(
                "UploadPart: uploadId={} is empty or not printable ASCII, forwarding to S3 without caching",
                truncate_upload_id(&upload_id).escape_debug()
            );
            // This reject path does not inspect the body. Stream it verbatim so a
            // client disconnect reaches S3 as a partial request, matching other
            // signed forward-only paths.
            return forward_signed_request_streaming_verbatim(
                req,
                &target_host,
                &transport,
                self.proxy_referer.as_deref(),
                STREAMED_BODY_CAP,
            )
            .await;
        }

        // Extract request headers
        let request_headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let content_length = part_content_length(&request_headers);

        // Check capacity
        let cache_decision = self.should_cache(content_length);

        // Compute bytes_uploaded for per-bucket traffic accounting (Design §4b).
        // Use x-amz-decoded-content-length for aws-chunked bodies; else Content-Length.
        // This is a header-only check — no I/O, safe to do before the match.
        let body_bytes_uploaded = {
            let is_chunked = aws_chunked_decoder::is_aws_chunked(&request_headers);
            if is_chunked {
                aws_chunked_decoder::get_decoded_content_length(&request_headers).unwrap_or(0)
            } else {
                content_length.unwrap_or(0)
            }
        };

        // Save bucket before cache_key is consumed by the match arms.
        let (bucket_ref, _) = parse_cache_key(&cache_key);
        let bucket_owned = bucket_ref.to_string();

        // R16.8: the two non-bypass decisions no longer share a single arm.
        // `Cache` was gated by `should_cache`'s Content-Length arithmetic in
        // `check_cache_capacity`. `StreamWithCapacityCheck` means the
        // Content-Length was absent or unparseable (the parse above is
        // `.and_then(|v| v.parse::<u64>().ok())`), so no size gate applied to it at
        // all — that arm carries a `StreamingCapacityBudget` which
        // `check_streaming_capacity` enforces per frame as the part stages.
        let result = match cache_decision {
            CacheDecision::Cache => {
                self.stage_and_forward_upload_part(
                    req,
                    &cache_key,
                    &target_host,
                    &transport,
                    &upload_id,
                    part_number,
                    &request_headers,
                    content_length,
                    None,
                )
                .await
            }
            CacheDecision::StreamWithCapacityCheck => {
                let budget = StreamingCapacityBudget {
                    current_usage: self.current_cache_usage,
                    max_capacity: self.max_cache_capacity,
                };
                self.stage_and_forward_upload_part(
                    req,
                    &cache_key,
                    &target_host,
                    &transport,
                    &upload_id,
                    part_number,
                    &request_headers,
                    content_length,
                    Some(budget),
                )
                .await
            }
            CacheDecision::Bypass(reason) => {
                log_bypass_decision(&cache_key, &reason);
                // Record bypassed PUT (Requirement 9.2)
                if let Some(metrics) = &self.metrics_manager {
                    metrics.read().await.record_bypassed_put().await;
                }
                // This bypass arm is forward-only. Stream the original frames
                // instead of retaining an object the cache declined to store. A
                // mid-upload disconnect now reaches S3 as a partial upload, matching
                // the cached UploadPart path.
                forward_signed_request_streaming_verbatim(
                    req,
                    &target_host,
                    &transport,
                    self.proxy_referer.as_deref(),
                    STREAMED_BODY_CAP,
                )
                .await
            }
        };
        self.finish_upload_part(result, &bucket_owned, body_bytes_uploaded)
            .await
    }

    /// Stream an `UploadPart` body to the upstream while teeing it to the
    /// part-staging cache sink (streaming-write-path Req 6.2).
    ///
    /// Shared by the two caching decisions that reach the part path. They differ
    /// only in `capacity_budget`: `None` for [`CacheDecision::Cache`], whose
    /// Content-Length was already checked against available capacity, and `Some`
    /// for [`CacheDecision::StreamWithCapacityCheck`], whose length was unknown and
    /// which is therefore bounded while it streams (R16.8).
    ///
    /// The client body frames flow straight to the upstream (the awaited socket
    /// write is the primary backpressure), and the same frames are tee'd to a
    /// bounded channel feeding the incremental part-cache task. The signed bytes
    /// forwarded to the upstream are byte-for-byte the client's (SigV4 intact);
    /// only the cache branch decodes aws-chunked, incrementally inside the cache
    /// task rather than up front. The upstream leg's WIRE FRAMING, however, is
    /// not always a byte-for-byte relay — when the outbound request advertises
    /// `Transfer-Encoding: chunked`, `forward_signed_request_streaming`
    /// (`src/signed_request_proxy.rs`) re-establishes HTTP chunk framing around
    /// each frame rather than passing hyper's already-unwrapped bytes straight
    /// through (chunked-request-framing, GitHub issue #19). The tee here is
    /// unaffected: it always receives the frame's original unframed bytes,
    /// regardless of what the upstream write does with them.
    #[allow(clippy::too_many_arguments)]
    async fn stage_and_forward_upload_part(
        &self,
        req: Request<hyper::body::Incoming>,
        cache_key: &str,
        target_host: &str,
        transport: &Arc<UpstreamTransport>,
        upload_id: &str,
        part_number: u32,
        request_headers: &HashMap<String, String>,
        content_length: Option<u64>,
        capacity_budget: Option<StreamingCapacityBudget>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();
        let body = req.into_body();

        // The cache branch decodes aws-chunked incrementally; the decoded
        // part length comes from `x-amz-decoded-content-length` for
        // aws-chunked, else the Content-Length. Used only to validate the
        // decoded length at finish (Req 3.4); the upstream always receives
        // the original bytes verbatim.
        let is_aws_chunked = aws_chunked_decoder::is_aws_chunked(request_headers);
        let decoded_len = if is_aws_chunked {
            aws_chunked_decoder::get_decoded_content_length(request_headers)
        } else {
            content_length
        };

        // Open the part sink + spawn the incremental part-cache task when
        // caching is viable. `tee = None` means no caching, but the body
        // still streams to the upstream (Req 7.2).
        let (tee, s3_result_tx, setup_done) = self
            .setup_upload_part_cache_tee_tracked(
                cache_key,
                upload_id,
                part_number,
                is_aws_chunked,
                decoded_len,
                capacity_budget,
            )
            .await;

        // Stream the original body to the upstream verbatim (Req 1.1, 4.1),
        // enforcing the body-size cap without buffering the whole body.
        let s3_response = forward_signed_request_streaming(
            &method,
            &uri,
            &headers,
            version,
            body,
            target_host,
            transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
            tee,
        )
        .await;

        // Deliver the S3 result (status + headers, or error) to the
        // background part-cache task. On success it finalizes the part under
        // `part{N}.lock` and writes its `part{N}.json` record with the response ETag; on
        // error/non-success/skip it discards the staged part — the per-part
        // correctness gate (commit only on S3 success) is preserved.
        if let Some(s3_result_tx) = s3_result_tx {
            let response_info = match &s3_response {
                Ok(resp) => Ok(ResponseInfo {
                    status: resp.status(),
                    headers: resp.headers().clone(),
                    write_guard: WriteGuard::Unguarded,
                }),
                Err(e) => Err(e.clone()),
            };
            let _ = s3_result_tx.send(response_info);
        }
        // The part's setup runs beside the body. A part small enough to fit in the
        // tee's buffer can finish before setup has created its upload directory, and a
        // `CompleteMultipartUpload` sent on this response would then find no directory and
        // leave the object uncached. Hold the response until setup has finished, for at most
        // `WRITE_CACHE_SETUP_WAIT_BOUND`.
        if let Some(setup_done) = setup_done {
            let _ = self
                .cache_delay_budget
                .run(WRITE_CACHE_SETUP_WAIT_BOUND, setup_done)
                .await;
        }
        // Return the S3 response to the client unchanged (Req 5.5).
        s3_response
    }

    /// Record per-bucket traffic for a completed `UploadPart` and return its
    /// response unchanged. Split out of [`Self::handle_upload_part`] when its
    /// caching arms were separated (R16.8); the accounting is identical for every
    /// arm, including bypass.
    async fn finish_upload_part(
        &self,
        result: Result<Response<BoxBody<Bytes, hyper::Error>>>,
        bucket_owned: &str,
        body_bytes_uploaded: u64,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // Per-bucket traffic accounting — Spec: per-bucket-metrics, Req 2.1, 2.2, 2.3
        // UploadPart maps to RequestType::Put (Design §4a). Record once at the completion
        // of the full request-response cycle (Req 2.3). Skip if bucket empty (Req 2.4).
        // Response body for UploadPart is empty (bytes_served = 0).
        if !bucket_owned.is_empty() {
            if let Some(metrics) = &self.metrics_manager {
                metrics
                    .read()
                    .await
                    .record_bucket_traffic(
                        bucket_owned,
                        None,             // prefix: no per-bucket prefix config in this handler
                        RequestType::Put, // UploadPart is a PUT operation
                        0,                // bytes_served: UploadPart response body is empty
                        0,                // bytes_saved: PUT is not a cache hit
                        body_bytes_uploaded,
                    )
                    .await;
            }
        }

        result
    }

    /// Cache an upload part as a range file. **Test-support only** (`#[cfg(test)]`):
    /// production caches parts through the streaming part sink
    /// (`open_multipart_part_sink` + `MultipartPartSink::finalize`), which
    /// consults per-bucket cache rules and streams rather than buffering. This
    /// buffered one-shot writer is retained solely as the part-population helper
    /// for the multipart test suite (cleanup, finalize, GET-from-cache, and the
    /// same-part-race concurrency regression); it holds `part{N}.lock` across the
    /// part-file rename and the part-record write, the same correctness gate the
    /// sink enforces. It is compiled only under `cfg(test)` and never ships in
    /// the production binary. Spec: compression-followup-fixes Requirement 4.
    #[cfg(test)]
    pub(crate) async fn cache_upload_part(
        &mut self,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        etag: &str,
    ) -> Result<()> {
        use crate::cache_types::CachedPartInfo;

        // Ensure multipart tracking directory exists
        let multipart_dir = mpu_upload_dir(&self.cache_dir, upload_id);
        tokio::fs::create_dir_all(&multipart_dir)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to create multipart directory: {}", e))
            })?;

        // Store part data in the upload-specific directory (isolated per upload_id)
        let part_file_path = multipart_dir.join(format!("part{}.bin", part_number));

        // Compress the part data (no shared state touched, safe outside the lock).
        let should_compress = self.compression_handler.is_compression_enabled();
        let compression_result =
            self.compression_handler
                .compress_with_metadata(data, cache_key, should_compress);

        // Acquire the PER-PART lock before writing the part file and its record, so
        // a racing same-part-number write cannot leave the on-disk bytes out of sync
        // with the recorded ETag (the invariant exercised by the concurrency test).
        // Per part rather than per upload: the invariant is per part, and a
        // per-upload lock serialised every part of every concurrent upload.
        let lock_file_path = Self::part_lock_path(&multipart_dir, part_number);

        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_file_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to open lock file: {}", e)))?;

        fs4::FileExt::lock(&lock_file).map_err(|e| {
            ProxyError::CacheError(format!("Failed to acquire per-part lock: {}", e))
        })?;
        // Unlocked explicitly when it drops: see `journal_consolidator::unlock_lock_file`.
        let lock_file = crate::journal_consolidator::UnlockOnDrop(lock_file);

        // Write part file atomically using temp file + rename (inside the lock)
        let temp_part_file_path = part_file_path.with_extension("tmp");
        // Synced before the rename: this part becomes a range the completion `.meta` names.
        let file_identity = crate::disk_cache::write_synced_identity(
            &temp_part_file_path,
            &compression_result.data,
        )
        .map_err(|e| {
            ProxyError::CacheError(format!("Failed to write temporary part file: {}", e))
        })?;

        tokio::fs::rename(&temp_part_file_path, &part_file_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to rename part file: {}", e)))?;

        // Compressed on-disk length, `stat`-ed from the published `part{N}.bin` inside
        // this same `part{N}.lock` hold — the same measurement, at the same point in the
        // lifecycle, as the production streaming path in
        // `finalize_and_record_cached_part`. Taken from the file rather than from
        // `compression_result.data.len()` so that this helper cannot drift from the
        // figure the product measures.
        let compressed_size = match tokio::fs::metadata(&part_file_path).await {
            Ok(m) => m.len(),
            Err(e) => {
                warn!(
                    "Could not stat finalized part file for compressed size: path={}, error={}",
                    part_file_path.display(),
                    e
                );
                0
            }
        };

        // Create part info for tracker (path is deterministic from upload_id + part_number)
        // `size` stays UNCOMPRESSED (raw body length) — the S3-facing figure —
        // while `compressed_size` carries the on-disk length. Two units, both named.
        let part_info = CachedPartInfo::new(
            part_number,
            data.len() as u64,
            etag.to_string(),
            compression_result.algorithm.clone(),
        )
        .with_compressed_size(compressed_size)
        .with_file_identity(file_identity);

        // Read existing tracker or create new one
        // One small file per part. Nothing shared is read or rewritten, so this
        // costs the same at ten parts and at ten thousand.
        let multipart_dir_owned = multipart_dir.clone();
        let part_info_owned = part_info.clone();
        tokio::task::spawn_blocking(move || {
            Self::record_part_blocking(&multipart_dir_owned, part_number, &part_info_owned)
        })
        .await
        .map_err(|e| ProxyError::CacheError(format!("Part record task panicked: {}", e)))??;

        drop(lock_file);
        Ok(())
    }

    /// Path of the per-part record for `part_number`.
    ///
    /// Sits beside the part's data file (`part{N}.bin`) and holds that part's
    /// [`CachedPartInfo`] — size, ETag and compression algorithm — as its own small
    /// JSON document.
    fn part_record_path(multipart_dir: &std::path::Path, part_number: u32) -> std::path::PathBuf {
        multipart_dir.join(format!("part{}.json", part_number))
    }

    /// Path of the per-part lock for `part_number`.
    fn part_lock_path(multipart_dir: &std::path::Path, part_number: u32) -> std::path::PathBuf {
        multipart_dir.join(format!("part{}.lock", part_number))
    }

    /// Take `upload.lock` exclusively for [`Self::finalize_multipart_upload`] without blocking
    /// a runtime worker or parking a blocking-pool thread.
    ///
    /// A blocking `flock` called on the async worker pinned that worker for the whole wait
    /// whenever the lock was contended (the incomplete-upload sweep's hold, or a retried
    /// Complete for the same upload on another instance). Each attempt here is a
    /// non-blocking `try_lock` on a blocking thread that returns at once; between attempts
    /// the task sleeps asynchronously. Unbounded, like the blocking `flock` it replaces.
    async fn lock_upload_file(
        lock_file_path: &std::path::Path,
    ) -> Result<crate::journal_consolidator::UnlockOnDrop> {
        let mut backoff = UPLOAD_LOCK_RETRY_MIN;
        loop {
            let path = lock_file_path.to_path_buf();
            let attempt = tokio::task::spawn_blocking(move || -> Result<Option<std::fs::File>> {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&path)
                    .map_err(|e| {
                        ProxyError::CacheError(format!("Failed to open lock file: {}", e))
                    })?;
                match fs4::FileExt::try_lock(&file) {
                    Ok(()) => Ok(Some(file)),
                    Err(fs4::TryLockError::WouldBlock) => Ok(None),
                    Err(fs4::TryLockError::Error(e)) => Err(ProxyError::CacheError(format!(
                        "Failed to acquire lock for upload.meta: {}",
                        e
                    ))),
                }
            })
            .await
            .map_err(|e| ProxyError::CacheError(format!("upload.lock task failed: {}", e)))??;
            if let Some(file) = attempt {
                return Ok(crate::journal_consolidator::UnlockOnDrop(file));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(UPLOAD_LOCK_RETRY_MAX);
        }
    }

    /// Record one part, without rewriting anything shared.
    ///
    /// # Why per-part files rather than one tracker
    ///
    /// Every `UploadPart` used to take an exclusive cross-instance `flock` on
    /// `upload.lock`, read the WHOLE `upload.meta`, append one entry, and write the
    /// whole file back. That is O(n²) bytes over an upload and, worse, it serialises
    /// every part of every concurrent upload on one lock on a network filesystem.
    ///
    /// Measured at 2,000 parts on a three-proxy fleet: **1,214 `upload.lock`
    /// timeouts, 1,220 part-record failures, and the client's upload FAILED** with
    /// `Connection reset by peer`, because each timed-out finalize held a
    /// `spawn_blocking` thread for its full 30-second timeout and the forward path
    /// needs that same pool. At ten concurrent parts the same queueing left three
    /// records unlanded after ten seconds, so the object was not cached at all.
    ///
    /// A part now owns its own filenames — `part{N}.bin`, `part{N}.json`,
    /// `part{N}.lock` — so per-part cost is O(1) and no part contends with any
    /// other. Finalisation reads the directory instead of one growing document
    /// ([`Self::load_tracker`]).
    ///
    /// # The correctness gate is preserved, at per-part scope
    ///
    /// The § 2 invariant is that a retried part with different bytes must never
    /// leave the on-disk file and the recorded ETag disagreeing. That still holds:
    /// the caller publishes `part{N}.bin` and writes `part{N}.json` inside one
    /// critical section, now guarded by `part{N}.lock`. Two writers racing the SAME
    /// part number still serialise; two writers on DIFFERENT part numbers no longer
    /// serialise at all, which is the whole point. The lock is per part rather than
    /// per upload because the invariant is per part — nothing about it ever needed
    /// to exclude a different part number.
    fn record_part_blocking(
        multipart_dir: &std::path::Path,
        part_number: u32,
        part_info: &crate::cache_types::CachedPartInfo,
    ) -> Result<()> {
        use std::io::Write;

        let record_path = Self::part_record_path(multipart_dir, part_number);
        let json = serde_json::to_string(part_info).map_err(|e| {
            ProxyError::CacheError(format!("Failed to serialize part record: {}", e))
        })?;

        // Atomic: tmp + fsync + rename (Req 6.2). The tmp name carries the part
        // number so two parts cannot collide on it.
        let tmp_path = multipart_dir.join(format!("part{}.json.tmp", part_number));
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| {
            ProxyError::CacheError(format!("Failed to create temp part record: {}", e))
        })?;
        f.write_all(json.as_bytes()).map_err(|e| {
            ProxyError::CacheError(format!("Failed to write temp part record: {}", e))
        })?;
        f.sync_all()
            .map_err(|e| ProxyError::CacheError(format!("Failed to fsync part record: {}", e)))?;
        drop(f);

        std::fs::rename(&tmp_path, &record_path)
            .map_err(|e| ProxyError::CacheError(format!("Failed to rename part record: {}", e)))?;
        // The record is durable, so this process's own in-progress figure owns its bytes from
        // here. Inside the `part{N}.lock` hold, so a replacement of the same part cannot
        // interleave with another writer's.
        if let Some(cache_dir) = cache_dir_of_upload_dir(multipart_dir) {
            note_part_record_written(cache_dir, &record_path, part_info.compressed_size);
        }
        Ok(())
    }

    /// Which of `wanted` have a record on disk.
    ///
    /// Stats the specific files rather than listing the directory. A staging directory
    /// holds up to three files per part (`.bin`, `.json`, `.lock`), so at 2,000 parts a
    /// full listing walks 6,000 entries on a network filesystem — and the poll loop
    /// that needs this answer runs every 100 ms. Statting `wanted` is bounded by what
    /// the caller asked about, and short-circuits as soon as one is missing, which is
    /// the common case early in the wait.
    fn present_parts_blocking(
        multipart_dir: &std::path::Path,
        wanted: &std::collections::HashSet<u32>,
    ) -> std::collections::HashSet<u32> {
        wanted
            .iter()
            .copied()
            .filter(|n| Self::part_record_path(multipart_dir, *n).exists())
            .collect()
    }

    /// Which part numbers have a record on disk.
    ///
    /// Existence only — no JSON parsing — because the one hot caller
    /// ([`Self::await_tracker_parts`]) asks nothing else, and it asks repeatedly.
    fn recorded_part_numbers(multipart_dir: &std::path::Path) -> std::collections::HashSet<u32> {
        let mut present = std::collections::HashSet::new();
        if let Ok(entries) = std::fs::read_dir(multipart_dir) {
            for entry in entries.flatten() {
                if let Some(n) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("part"))
                    .and_then(|rest| rest.strip_suffix(".json"))
                    .and_then(|digits| digits.parse::<u32>().ok())
                {
                    present.insert(n);
                }
            }
        }
        present
    }

    /// Assemble a [`MultipartUploadTracker`] from the upload-level `upload.meta`
    /// plus the per-part records on disk.
    ///
    /// `upload.meta` is written once, at `CreateMultipartUpload`, and carries only
    /// upload-level facts (upload id, cache key, start time, content type). The
    /// parts come from the directory. So the returned value is the same shape every
    /// existing caller expects, assembled rather than parsed from one document.
    ///
    /// A record that fails to parse is skipped with a `warn!` rather than failing
    /// the whole load: one unreadable part should cost that part, not the object.
    /// The missing-parts guard in `finalize_multipart_upload` then declines to
    /// finalise, which is the correct outcome and the same one as if the part had
    /// never landed.
    fn load_tracker_blocking(
        multipart_dir: &std::path::Path,
        upload_id: &str,
        cache_key: &str,
    ) -> Result<crate::cache_types::MultipartUploadTracker> {
        use crate::cache_types::{CachedPartInfo, MultipartUploadTracker};

        // A missing or unparseable `upload.meta` is not fatal here; the parts come from disk.
        // Production does not reach this without one: CompleteMultipartUpload declines to cache
        // an upload whose `upload.meta` records no representation headers, because the entry
        // would be served without them. The synthesised tracker serves the tests that drive a
        // finalization directly.
        let upload_meta_file = multipart_dir.join("upload.meta");
        let mut tracker = match std::fs::read_to_string(&upload_meta_file) {
            Ok(content) => match MultipartUploadTracker::from_json(&content) {
                Ok(tracker) => tracker,
                Err(e) => {
                    warn!(
                        "Unparseable upload.meta at {:?}: {} — rebuilding upload-level fields from the request (parts are unaffected; they come from disk)",
                        upload_meta_file, e
                    );
                    MultipartUploadTracker::new(upload_id.to_string(), cache_key.to_string())
                }
            },
            Err(_) => {
                debug!(
                    "No upload.meta at {:?}; rebuilding upload-level fields from the request",
                    upload_meta_file
                );
                MultipartUploadTracker::new(upload_id.to_string(), cache_key.to_string())
            }
        };

        // Upload-level file is authoritative for everything except the parts, which
        // are rebuilt from disk so a stale `parts` array in an old-format file
        // cannot contribute.
        tracker.parts.clear();
        tracker.total_size = 0;

        for part_number in Self::recorded_part_numbers(multipart_dir) {
            let record_path = Self::part_record_path(multipart_dir, part_number);
            match std::fs::read_to_string(&record_path)
                .map_err(|e| e.to_string())
                .and_then(|s| serde_json::from_str::<CachedPartInfo>(&s).map_err(|e| e.to_string()))
            {
                Ok(info) => tracker.add_part(info),
                Err(e) => warn!(
                    "Skipping unreadable part record {:?}: {} (this part will count as not cached)",
                    record_path, e
                ),
            }
        }

        Ok(tracker)
    }

    /// The representation headers CreateMultipartUpload recorded in `upload.meta`. `None` when
    /// the file is absent or unreadable, or was written before they were recorded: the
    /// Create's tracker was never written (backlog full, disk-safety refusal, a failed write),
    /// was withdrawn, or the Create was never seen by this fleet.
    async fn recorded_representation_headers(
        multipart_dir: &std::path::Path,
    ) -> Option<HashMap<String, String>> {
        #[derive(serde::Deserialize)]
        struct Recorded {
            #[serde(default)]
            representation_headers: Option<HashMap<String, String>>,
        }
        let text = tokio::fs::read_to_string(multipart_dir.join("upload.meta"))
            .await
            .ok()?;
        serde_json::from_str::<Recorded>(&text)
            .ok()?
            .representation_headers
    }

    /// Handle CompleteMultipartUpload by creating metadata linking all parts
    ///
    /// # Requirements
    ///
    /// - Requirement 5.3: Handle CompleteMultipartUpload
    /// - Requirement 5.4: Create metadata linking all cached parts as ranges
    /// - Requirement 5.5: Mark upload as incomplete on failure without deleting parts
    /// - Requirement 8.1: Handle cache write failures gracefully
    /// - Requirement 8.2: Clean up cached data on S3 error
    /// - Requirement 9.3: Log detailed error information
    async fn handle_complete_multipart_upload(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        upload_id: String,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // The staging path is a digest of the uploadId (`mpu_upload_dir`), so this is not
        // a traversal guard; it keeps control characters out of the logs and tracker.
        // On reject, forward to S3 unmodified (preserve SigV4 response) and skip cache work.
        if !is_cacheable_upload_id(&upload_id) {
            warn!(
                "CompleteMultipartUpload: uploadId={} is empty or not printable ASCII, forwarding to S3 without caching",
                truncate_upload_id(&upload_id).escape_debug()
            );
            // This reject path does not inspect the body. Stream it verbatim so a
            // client disconnect reaches S3 as a partial request, matching other
            // signed forward-only paths.
            return forward_signed_request_streaming_verbatim(
                req,
                &target_host,
                &transport,
                self.proxy_referer.as_deref(),
                STREAMED_BODY_CAP,
            )
            .await;
        }

        let (bucket, key) = parse_cache_key(&cache_key);

        // Buffer the request body before forwarding to S3 (Requirement 4.1)
        // This allows us to parse the XML to extract the requested parts list
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();
        let version = req.version();

        // Read the request body with a bounded cap (Security: prevent unbounded memory
        // consumption from an oversized CompleteMultipartUpload body). The Complete XML
        // lists part numbers and ETags — a few MiB is ample for even the largest uploads
        // (10,000 parts). On overflow, reject with HTTP 413 before forwarding to S3.
        let max_bytes = self.max_complete_body_bytes;
        let content_length_hint = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        // Fast reject if Content-Length already exceeds cap
        if let Some(cl) = content_length_hint {
            if cl > max_bytes {
                warn!(
                    content_length = cl,
                    max_bytes = max_bytes,
                    "CompleteMultipartUpload body exceeds max_complete_body_bytes (Content-Length)"
                );
                return Err(ProxyError::RequestBodyTooLarge {
                    content_length: Some(cl),
                    max_bytes,
                });
            }
        }

        // Reserve against the in-flight ledger for consistency in the ledger
        // total, even though this cap is already correct and does not change
        // (design.md → "Already-bounded sites, unchanged"). The Complete XML
        // is capped at `max_complete_body_bytes` (10 MiB default) with an
        // up-front Content-Length check above and a mid-collect guard below,
        // so this reservation is taken for the declared length (or grown per
        // chunk when absent) purely for observability parity with the other
        // Buffering_Sites — it is not the primary defense against an oversized
        // body, which `RequestBodyTooLarge`/413 above and below remains.
        let ledger = self
            .s3_client
            .as_ref()
            .map(|c| c.get_inflight_ledger())
            .unwrap_or_else(|| Arc::new(crate::inflight_ledger::InflightLedger::disabled()));
        let mut mpu_reservation = match ledger.try_reserve(content_length_hint.unwrap_or(0)) {
            Some(r) => r,
            None => {
                return Err(ProxyError::InflightCeilingExceeded {
                    ceiling_bytes: ledger.ceiling_bytes(),
                    requested_bytes: content_length_hint.unwrap_or(0),
                });
            }
        };

        let mut body = req.into_body();
        let mut accumulated = Vec::with_capacity(
            content_length_hint
                .unwrap_or(8192)
                .min(max_bytes)
                .min(1024 * 1024) as usize,
        );

        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| {
                ProxyError::HttpError(format!(
                    "Failed to read CompleteMultipartUpload body: {}",
                    e
                ))
            })?;
            if let Ok(data) = frame.into_data() {
                if accumulated.len() as u64 + data.len() as u64 > max_bytes {
                    warn!(
                        accumulated_bytes = accumulated.len(),
                        chunk_bytes = data.len(),
                        max_bytes = max_bytes,
                        "CompleteMultipartUpload body exceeds max_complete_body_bytes"
                    );
                    return Err(ProxyError::RequestBodyTooLarge {
                        content_length: content_length_hint,
                        max_bytes,
                    });
                }
                let already_reserved = mpu_reservation.held_bytes();
                let new_total = accumulated.len() as u64 + data.len() as u64;
                if new_total > already_reserved {
                    let growth = new_total - already_reserved;
                    if !mpu_reservation.try_grow(growth) {
                        ledger.record_aborted_accumulation();
                        return Err(ProxyError::InflightCeilingExceeded {
                            ceiling_bytes: ledger.ceiling_bytes(),
                            requested_bytes: new_total,
                        });
                    }
                }
                accumulated.extend_from_slice(&data);
            }
        }
        let request_body_bytes = Bytes::from(accumulated);

        // Parse the request body to extract the requested parts list (Requirement 4.1, 4.2)
        let requested_parts = match parse_complete_mpu_request(&request_body_bytes) {
            Ok(parts) => {
                debug!(
                    "Parsed CompleteMultipartUpload request: cache_key={}, parts_count={}",
                    cache_key,
                    parts.len()
                );
                Some(parts)
            }
            Err(e) => {
                // If request body is empty or malformed, skip cache finalization (Requirement 4.3)
                warn!(
                    "Failed to parse CompleteMultipartUpload request body: cache_key={}, error={}, will skip cache finalization",
                    cache_key, e
                );
                None
            }
        };

        // Forward the original request body to S3 using the pre-buffered body
        let s3_response = forward_signed_request_with_body(
            method,
            uri,
            headers,
            version,
            request_body_bytes,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
        )
        .await?;

        let status = s3_response.status();

        if status.is_success() {
            // Before finalization, so every skip below (unparsed body, no staging dir,
            // missing parts, ETag mismatch, a lock error) still leaves the replaced object
            // unservable. `finalize_multipart_upload` invalidates again before its renames,
            // which is idempotent. Its token orders this completion against any other
            // acknowledged write of the key.
            let write_guard = self
                .invalidate_prior_version_before_response(&cache_key, "CompleteMultipartUpload")
                .await;

            // Read response body to extract ETag from XML
            // S3 returns CompleteMultipartUpload response as XML with ETag in the body

            let (parts, body) = s3_response.into_parts();

            // Extract response headers for cache metadata
            let response_headers: std::collections::HashMap<String, String> = parts
                .headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();

            // Collect the body bytes
            let body_bytes = body
                .collect()
                .await
                .map_err(|e| ProxyError::HttpError(format!("Failed to read response body: {}", e)))?
                .to_bytes();

            // Extract ETag from XML response body
            // Format: <CompleteMultipartUploadResult><ETag>"etag-value"</ETag>...</CompleteMultipartUploadResult>
            let body_str = String::from_utf8_lossy(&body_bytes);
            let etag = Self::extract_etag_from_xml(&body_str);

            debug!(
                "Extracted ETag from CompleteMultipartUpload response: cache_key={}, etag={}",
                cache_key, etag
            );

            // A `200` can carry an `<Error>` document when S3 fails while assembling the object,
            // and an empty ETag leaves nothing to validate the entry against. Neither describes
            // an object to cache, so decline and remove the staging. A real
            // result's `<Key>` is XML-escaped, so it cannot contain `<Error>`.
            let error_document = body_str.contains("<Error>");

            // What the cache does with this completion. A parse failure skips finalization:
            // without the requested part list there is no statement of what the completed
            // object contains.
            enum Finish {
                Finalize(Vec<RequestedPart>),
                Decline,
                Skip,
            }
            let finish = if error_document || etag.is_empty() {
                warn!(
                    "CompleteMultipartUpload answered 200 without a usable result (error document: {}, empty ETag: {}): cache_key={}, upload_id={}, declining cache finalization (response passed to the client unchanged)",
                    error_document,
                    etag.is_empty(),
                    cache_key,
                    upload_id
                );
                Finish::Decline
            } else if let Some(requested) = requested_parts {
                Finish::Finalize(requested)
            } else {
                warn!(
                    "Skipping multipart cache finalization because the CompleteMultipartUpload body did not parse: cache_key={}, upload_id={} (upload unaffected — S3 already accepted it)",
                    cache_key, upload_id
                );
                Finish::Skip
            };

            // The client is answered when S3 answers. The cache work runs on
            // its own task, carrying the write-order claim stamped above, and the response
            // waits for it at most `MULTIPART_COMPLETE_RESPONSE_WAIT`. The task holds a
            // pending-store permit, so detached Complete tasks are bounded with the other
            // write-through stores. With none free the cache work is declined
            // (`skipped_puts_total{backlog_full}`): the prior version is already invalidated,
            // and the staged parts are left for the incomplete-upload sweep.
            let finish_permit = match finish {
                Finish::Skip => None,
                Finish::Finalize(_) | Finish::Decline => {
                    self.admit_pending_cache_write(&cache_key).await
                }
            };
            let Some(finish_permit) = finish_permit else {
                if !matches!(finish, Finish::Skip) {
                    warn!(
                        "CompleteMultipartUpload: write-through cache backlog full, not finalizing the cache entry (the upload is unaffected; staged parts are left for the incomplete-upload sweep): cache_key={}, upload_id={}",
                        cache_key, upload_id
                    );
                }
                drop(write_guard);
                return Ok(Response::from_parts(
                    parts,
                    Full::new(body_bytes)
                        .map_err(|never| match never {})
                        .boxed(),
                ));
            };
            let mut finisher = self.detached_clone();
            let task_key = cache_key.clone();
            let task_upload = upload_id.clone();
            let (finished_tx, finished_rx) = tokio::sync::oneshot::channel::<()>();
            tokio::spawn(async move {
                let _finish_permit = finish_permit;
                let _finished = finished_tx;
                match finish {
                    Finish::Finalize(requested) => {
                        // An upload whose CreateMultipartUpload recorded no representation
                        // headers is not cached, since its entry would be served without them.
                        // Its staged parts are removed as for a declined completion.
                        let dir = mpu_upload_dir(&finisher.cache_dir, &task_upload);
                        match Self::recorded_representation_headers(&dir).await {
                            Some(recorded) => {
                                if let Err(e) = finisher
                                    .finalize_multipart_upload_guarded(
                                        &task_key,
                                        &task_upload,
                                        &etag,
                                        &response_headers,
                                        &requested,
                                        write_guard,
                                        Some(recorded),
                                    )
                                    .await
                                {
                                    let (bucket, key) = parse_cache_key(&task_key);
                                    error!(
                                        "CompleteMultipartUpload cache failed: bucket={}, key={}, error={}",
                                        bucket, key, e
                                    );
                                }
                            }
                            None => {
                                info!(
                                    "CompleteMultipartUpload: no upload tracker recording the Create's representation headers, not caching the object (the upload is unaffected): cache_key={}, upload_id={}",
                                    task_key,
                                    truncate_upload_id(&task_upload)
                                );
                                drop(write_guard);
                                if dir.exists() {
                                    finisher
                                        .cleanup_incomplete_multipart_cache(&dir, &task_upload)
                                        .await;
                                }
                            }
                        }
                    }
                    Finish::Decline => {
                        let dir = mpu_upload_dir(&finisher.cache_dir, &task_upload);
                        if dir.exists() {
                            finisher
                                .cleanup_incomplete_multipart_cache(&dir, &task_upload)
                                .await;
                        }
                    }
                    Finish::Skip => {}
                }
            });
            let _ = self
                .cache_delay_budget
                .run(MULTIPART_COMPLETE_RESPONSE_WAIT, finished_rx)
                .await;
            // Success log is in finalize_multipart_upload with full details

            // Reconstruct response to return to client
            let response = Response::from_parts(
                parts,
                Full::new(body_bytes)
                    .map_err(|never| match never {})
                    .boxed(),
            );
            Ok(response)
        } else {
            // Mark upload as incomplete but don't delete parts (Requirement 5.5, 8.2, 9.3)
            error!(
                "CompleteMultipartUpload S3 error: bucket={}, key={}, status={}",
                bucket,
                key,
                status.as_u16()
            );
            Ok(s3_response)
        }
    }

    /// Handle AbortMultipartUpload request
    ///
    /// This method:
    /// 1. Forwards the request to S3
    /// 2. Immediately evicts all cached parts for that uploadId
    /// 3. Deletes mpus_in_progress/{uploadId}/
    /// 4. Returns S3 response unchanged to client
    ///
    /// # Requirements
    ///
    /// - Requirement 4.5: Forward to S3 and immediately evict all cached parts for that uploadId
    /// - Requirement 4.6: Return S3 response unchanged to client
    async fn handle_abort_multipart_upload(
        &mut self,
        req: Request<hyper::body::Incoming>,
        cache_key: String,
        target_host: String,
        transport: Arc<UpstreamTransport>,
        upload_id: String,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>> {
        // `remove_dir_all` below targets `mpu_upload_dir`, a digest of the uploadId, so it
        // cannot leave `mpus_in_progress/` whatever the uploadId holds. This check keeps
        // control characters out of the logs, and `""` (no uploadId) away from the cache.
        // On reject, forward to S3 unmodified (preserve SigV4 response) and skip cache work.
        if !is_cacheable_upload_id(&upload_id) {
            warn!(
                "AbortMultipartUpload: uploadId={} is empty or not printable ASCII, forwarding to S3 without caching",
                truncate_upload_id(&upload_id).escape_debug()
            );
            // This reject path does not inspect the body. Stream it verbatim so a
            // client disconnect reaches S3 as a partial request, matching other
            // signed forward-only paths.
            return forward_signed_request_streaming_verbatim(
                req,
                &target_host,
                &transport,
                self.proxy_referer.as_deref(),
                STREAMED_BODY_CAP,
            )
            .await;
        }

        info!(
            "Handling AbortMultipartUpload: cache_key={}, upload_id={}",
            cache_key, upload_id
        );

        // AbortMultipartUpload is forward-only; cleanup happens after the response,
        // not from its request body. Stream it verbatim so a mid-upload disconnect
        // reaches S3 as a partial request, consistent with signed PUT forwarding.
        let s3_response = forward_signed_request_streaming_verbatim(
            req,
            &target_host,
            &transport,
            self.proxy_referer.as_deref(),
            STREAMED_BODY_CAP,
        )
        .await?;

        // Always clean up cached parts, regardless of S3 response status
        // This ensures we don't leave orphaned cache data
        // the removal is cache work after S3 answered, so it runs on its own task and the
        // response waits for it at most what remains of the write's cache-delay budget. One
        // still running then finishes in the background.
        let mut cleaner = self.detached_clone();
        let cleanup_id = upload_id.clone();
        let cleanup =
            tokio::spawn(async move { cleaner.cleanup_multipart_upload(&cleanup_id).await });
        match self
            .cache_delay_budget
            .run(crate::cache::WRITE_CACHE_DELAY_BUDGET, cleanup)
            .await
        {
            Some(Ok(Err(e))) => error!(
                "Failed to cleanup multipart upload cache: cache_key={}, upload_id={}, error={}",
                cache_key, upload_id, e
            ),
            Some(Ok(Ok(()))) => info!(
                "Successfully cleaned up multipart upload cache: cache_key={}, upload_id={}",
                cache_key, upload_id
            ),
            Some(Err(e)) => error!(
                "Multipart upload cache cleanup task failed: cache_key={}, upload_id={}, error={}",
                cache_key, upload_id, e
            ),
            None => debug!(
                "Multipart upload cache cleanup still running when the write's cache-delay \
                 budget ran out; it finishes in the background: cache_key={}, upload_id={}",
                cache_key, upload_id
            ),
        }
        // Return S3 response unchanged (Requirement 4.6)
        Ok(s3_response)
    }

    /// Clean up all cached parts and tracking metadata for a multipart upload
    ///
    /// This method:
    /// 1. Acquires lock on upload.meta
    /// 2. Reads all cached parts from tracking metadata
    /// 3. Deletes each part's range file from ranges/{bucket}/{XX}/{YYY}/
    /// 4. Deletes the upload.meta file
    /// 5. Deletes the mpus_in_progress/{uploadId}/ directory
    ///
    /// # Requirements
    ///
    /// - Requirement 4.5: Delete all cached parts for uploadId
    /// - Requirement 8.5: Clean up tracking metadata
    pub async fn cleanup_multipart_upload(&mut self, upload_id: &str) -> Result<()> {
        let multipart_dir = mpu_upload_dir(&self.cache_dir, upload_id);

        if !multipart_dir.exists() {
            debug!(
                "Multipart directory not found during cleanup: upload_id={}",
                upload_id
            );
            return Ok(());
        }

        // Parts are stored inside the upload directory, so a single remove_dir_all cleans everything
        if let Err(e) = tokio::fs::remove_dir_all(&multipart_dir).await {
            warn!(
                "Failed to remove multipart directory: upload_id={}, error={}",
                upload_id, e
            );
        } else {
            info!(
                "Cleaned up multipart upload directory: upload_id={}",
                upload_id
            );
        }

        Ok(())
    }

    /// Finalize multipart upload by creating metadata linking all parts as ranges
    ///
    /// This method:
    /// 1. Acquires lock on upload.meta
    /// 2. Reads all parts, sorts by part number
    /// 3. Calculates byte offsets for each part
    /// 4. Renames part files with final offsets
    /// 5. Creates object metadata with final ETag from S3 XML
    /// 6. Sets is_write_cached=true, write_cache_expires_at
    /// 7. Deletes mpus_in_progress/{uploadId}/
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The cache key for the object
    /// * `upload_id` - The multipart upload ID
    /// * `etag` - The final ETag from S3 response
    /// * `response_headers` - Headers from the S3 response
    /// * `requested_parts` - Optional list of parts from the CompleteMultipartUpload request body.
    ///   If provided, only these parts will be included in the final object (Requirement 5.1).
    ///   If None, all cached parts will be used (backward compatibility).
    ///
    /// # Requirements
    ///
    /// - Requirement 3.1: Create object metadata linking all cached parts as ranges
    /// - Requirement 3.2: Calculate final byte offsets for each part
    /// - Requirement 3.3: Store the final ETag from S3 response
    /// - Requirement 3.4: Set the write cache TTL on the completed object
    /// - Requirement 5.1: Use only parts listed in the CompleteMultipartUpload request body
    ///
    /// Unguarded: production calls [`Self::finalize_multipart_upload_guarded`] with the
    /// token its response path stamped. This form is kept for the tests that drive a
    /// finalization directly.
    #[cfg(test)]
    async fn finalize_multipart_upload(
        &mut self,
        cache_key: &str,
        upload_id: &str,
        etag: &str,
        response_headers: &std::collections::HashMap<String, String>,
        requested_parts: &[RequestedPart],
    ) -> Result<()> {
        self.finalize_multipart_upload_guarded(
            cache_key,
            upload_id,
            etag,
            response_headers,
            requested_parts,
            WriteGuard::Unguarded,
            None,
        )
        .await
    }

    /// [`Self::finalize_multipart_upload`] carrying the completion's write-order claim:
    /// with a token it publishes only while the key's marker still holds that token, checked
    /// under the slot lock before the replaced object is removed and the parts are renamed.
    ///
    /// `recorded_headers` are the representation headers CreateMultipartUpload recorded, as
    /// the caller already read them; `None` reads them from `upload.meta` here, and an absent
    /// record then contributes none.
    #[allow(clippy::too_many_arguments)]
    async fn finalize_multipart_upload_guarded(
        &mut self,
        cache_key: &str,
        upload_id: &str,
        etag: &str,
        response_headers: &std::collections::HashMap<String, String>,
        requested_parts: &[RequestedPart],
        write_guard: WriteGuard,
        recorded_headers: Option<HashMap<String, String>>,
    ) -> Result<()> {
        // `Unavailable` carries a token that was never stamped, so it fails the check below and
        // declines through the same cleanup as a superseded completion.
        let (token, guard_unavailable) = match write_guard {
            WriteGuard::Unguarded => (None, false),
            WriteGuard::Token(token) => (Some(token), false),
            WriteGuard::Unavailable => (Some(crate::cache::WriteToken::fresh()), true),
        };
        use crate::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState};
        use crate::compression::CompressionAlgorithm;

        let multipart_dir = mpu_upload_dir(&self.cache_dir, upload_id);

        // Early validation - if we don't have the upload directory, skip caching entirely
        if !multipart_dir.exists() {
            warn!(
                "CompleteMultipartUpload succeeded on S3 but no local upload directory found: cache_key={}, upload_id={}, skipping cache finalization",
                cache_key, upload_id
            );
            return Ok(());
        }

        // Wait for the parts this Complete names to appear in the tracker before
        // evaluating it (the lifecycle race).
        //
        // The per-part cache task is fire-and-forget and deliberately lags the
        // client's response: the forward path answers the client, and only then
        // does the cache task drain the tee, await the S3 result and finalize.
        // Clients send Complete as soon as the last part is acknowledged — 147 ms
        // after the first part was recorded, in the measured case — so Complete
        // used to read a tracker holding 2 of 10 parts, declare the other 8
        // "not cached locally", and `remove_dir_all` the staging directory out
        // from under the 8 tasks still writing into it. Those tasks then failed
        // ENOENT on rename or ESTALE on `upload.lock`, which is where the error
        // flood came from. The loser was predetermined.
        //
        // WHY POLL THE TRACKER RATHER THAN TRACK IN-FLIGHT TASKS. Parts of one
        // upload can be served by several proxies and Complete by a fourth, so
        // an in-process registry of spawned tasks fixes only the case where they
        // happen to share an instance — which on a three-proxy fleet is the
        // uncommon one. The tracker already lives on the shared volume and is
        // already updated under `upload.lock` by whichever instance ran the part
        // task, and this Complete already knows exactly which parts it needs from
        // its own request body. So waiting for the tracker to contain them is
        // fleet-correct with no new shared state, no marker files, and no
        // dependence on process memory. It is also why the parse-failure path
        // above must skip outright: without the requested part list there is no
        // well-defined set to wait for.
        Self::await_tracker_parts(&multipart_dir, cache_key, upload_id, requested_parts).await;

        // Acquire lock on upload.meta
        let lock_file_path = multipart_dir.join("upload.lock");
        let lock_file = Self::lock_upload_file(&lock_file_path).await?;

        // Assemble the tracker: upload-level facts from `upload.meta`, parts from
        // the per-part records on disk. This is the only place that pays O(n) for
        // the part set, once per upload, instead of every part paying it.
        // Off the runtime: this reads one small file per part, so at high part counts
        // it is thousands of synchronous round trips to a shared volume.
        let tracker_load = {
            let dir = multipart_dir.clone();
            let upload_id_owned = upload_id.to_string();
            let cache_key_owned = cache_key.to_string();
            tokio::task::spawn_blocking(move || {
                Self::load_tracker_blocking(&dir, &upload_id_owned, &cache_key_owned)
            })
            .await
            .unwrap_or_else(|e| {
                Err(ProxyError::CacheError(format!(
                    "Tracker load task panicked: {}",
                    e
                )))
            })
        };
        let tracker = match tracker_load {
            Ok(tracker) => tracker,
            Err(e) => {
                warn!(
                    "CompleteMultipartUpload succeeded on S3 but failed to load the upload tracker: cache_key={}, upload_id={}, error={}, cleaning up and skipping cache finalization",
                    cache_key, upload_id, e
                );
                drop(lock_file);
                self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                    .await;
                return Ok(());
            }
        };

        if tracker.parts.is_empty() {
            warn!(
                "CompleteMultipartUpload succeeded on S3 but no parts found in upload tracker: cache_key={}, upload_id={}, cleaning up and skipping cache finalization",
                cache_key, upload_id
            );
            drop(lock_file);
            self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                .await;
            return Ok(());
        }

        // Build HashSet of requested part numbers for efficient lookup (Requirements 5.1, 5.2, 5.3)
        //
        // Always the set S3 was asked to assemble, never "whatever the tracker
        // happens to hold". The old `None` fallback to the tracker's own contents
        // made the filter below a no-op and the guards that follow vacuous, so a
        // partially-recorded upload finalized as a short object.
        let requested_part_numbers: std::collections::HashSet<u32> =
            requested_parts.iter().map(|p| p.part_number).collect();

        // Filter cached parts to only those in the request and sort by part number
        let all_cached_parts = tracker.get_sorted_parts();
        let filtered_parts: Vec<&crate::cache_types::CachedPartInfo> = all_cached_parts
            .into_iter()
            .filter(|p| requested_part_numbers.contains(&p.part_number))
            .collect();

        // Check if any requested parts are not cached locally (Requirement 5.4)
        // If a requested part is not in our cache, skip cache finalization
        {
            let cached_part_numbers: std::collections::HashSet<u32> =
                tracker.parts.iter().map(|p| p.part_number).collect();
            let missing_requested: Vec<u32> = requested_parts
                .iter()
                .filter(|p| !cached_part_numbers.contains(&p.part_number))
                .map(|p| p.part_number)
                .collect();

            if !missing_requested.is_empty() {
                // A genuine straggler after the bounded wait above. Degrade to
                // "not cached" quietly and leave the staging directory to the TTL
                // sweep.
                //
                // Deleting it here is what produced the error flood: the parts
                // this instance is declaring missing may still be mid-write, on
                // this instance or another, and `remove_dir_all` pulls the
                // directory (including `upload.lock` and any `.tmp`) out from
                // under them, so they fail ENOENT on rename or ESTALE on the
                // lock. Those errors read like disk faults and are not.
                warn!(
                    "CompleteMultipartUpload succeeded on S3 but parts {:?} were still not recorded after waiting {:?}: cache_key={}, upload_id={}, skipping cache finalization (upload unaffected; staging left for the TTL sweep)",
                    missing_requested, MULTIPART_COMPLETE_CACHE_WAIT, cache_key, upload_id
                );
                drop(lock_file);
                return Ok(());
            }
        }

        // Validate that all filtered parts exist on disk before proceeding
        let ranges_dir = self.cache_dir.join("ranges");
        let sorted_parts = filtered_parts;
        let mut missing_parts = Vec::new();

        for part in &sorted_parts {
            // Parts are stored in the upload directory: mpus_in_progress/{upload_id}/part{N}.bin
            let part_file = multipart_dir.join(format!("part{}.bin", part.part_number));

            if !part_file.exists() {
                missing_parts.push(part.part_number);
            }
        }

        // If any parts are missing, skip caching and clean up
        if !missing_parts.is_empty() {
            warn!(
                "CompleteMultipartUpload succeeded on S3 but missing local parts {:?}: cache_key={}, upload_id={}, cleaning up and skipping cache finalization",
                missing_parts, cache_key, upload_id
            );
            drop(lock_file);
            self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                .await;
            return Ok(());
        }
        // A zero-length part (allowed by S3 only as the final or only part) has no byte
        // range to file, so the object is not cached. The upload to S3 is unaffected.
        if let Some(empty) = sorted_parts.iter().find(|p| p.size == 0) {
            info!(
                "CompleteMultipartUpload includes zero-length part {}: cache_key={}, upload_id={}, skipping cache finalization",
                empty.part_number, cache_key, upload_id
            );
            drop(lock_file);
            self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                .await;
            return Ok(());
        }

        // Validate ETags match between request and cached parts (Requirements 9.1, 9.2, 9.3, 9.4)
        // If any ETag mismatches, skip cache finalization but still forward to S3 (already done)
        {
            for requested_part in requested_parts {
                // Find the corresponding cached part
                if let Some(cached_part) = sorted_parts
                    .iter()
                    .find(|p| p.part_number == requested_part.part_number)
                {
                    // Tolerate one balanced pair of quotes and nothing else; refuse an
                    // empty or quote-only validator on either side (see `part_etags_match`).
                    let request_etag = requested_part.etag.as_str();
                    let cached_etag = cached_part.etag.as_str();
                    if !part_etags_match(request_etag, cached_etag) {
                        warn!(
                            "ETag mismatch for part {}: request_etag={}, cached_etag={}, cache_key={}, upload_id={}, skipping cache finalization",
                            requested_part.part_number,
                            request_etag,
                            cached_etag,
                            cache_key,
                            upload_id
                        );
                        drop(lock_file);
                        self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                            .await;
                        return Ok(());
                    }
                }
            }
        }

        // Delete unreferenced parts - parts cached but not in the CompleteMultipartUpload request
        // (Requirements 6.1, 6.2, 6.3, 6.4)
        let unreferenced_parts: Vec<&crate::cache_types::CachedPartInfo> = tracker
            .parts
            .iter()
            .filter(|p| !requested_part_numbers.contains(&p.part_number))
            .collect();

        if !unreferenced_parts.is_empty() {
            info!(
                "Cleaning up {} unreferenced parts not in CompleteMultipartUpload request: cache_key={}, upload_id={}, parts={:?}",
                unreferenced_parts.len(),
                cache_key,
                upload_id,
                unreferenced_parts.iter().map(|p| p.part_number).collect::<Vec<_>>()
            );

            for part in &unreferenced_parts {
                // Parts are in the upload directory
                let part_file = multipart_dir.join(format!("part{}.bin", part.part_number));
                if part_file.exists() {
                    match tokio::fs::remove_file(&part_file).await {
                        Ok(()) => {
                            debug!(
                                "Deleted unreferenced part {}: cache_key={}, upload_id={}",
                                part.part_number, cache_key, upload_id
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to delete unreferenced part {}: cache_key={}, upload_id={}, error={}",
                                part.part_number, cache_key, upload_id, e
                            );
                        }
                    }
                }
            }
        }

        // Calculate byte offsets from filtered parts (Requirements 5.3, 7.1)
        // This ensures we only use the parts specified in the request
        let byte_offsets: Vec<(u32, u64, u64)> = {
            let mut offsets = Vec::with_capacity(sorted_parts.len());
            let mut current_offset: u64 = 0;
            for part in &sorted_parts {
                let start = current_offset;
                let Some(end) = (current_offset + part.size).checked_sub(1) else {
                    continue; // zero-length parts were declined above
                };
                offsets.push((part.part_number, start, end));
                current_offset += part.size;
            }
            offsets
        };

        // Invalidate existing cache entries before creating new object metadata (Requirements 4.1, 4.2, 4.3, 5.1, 5.2)
        //
        // A guarded completion's response path already invalidated and stamped its token, so
        // this re-invalidation must not stamp again (that would supersede itself), and it removes
        // nothing once the token is replaced: a newer write's object is not ours to remove. That
        // check runs under the same slot-lock hold as the removal.
        if let Some(cache_mgr) = &self.cache_manager {
            let invalidated = match &token {
                None => {
                    cache_mgr
                        .invalidate_cache_unified_for_operation(
                            cache_key,
                            "CompleteMultipartUpload",
                        )
                        .await
                }
                Some(token) => {
                    cache_mgr
                        .invalidate_if_write_current(cache_key, token)
                        .await
                }
            };
            if let Err(e) = invalidated {
                warn!(
                    "Failed to invalidate cache during CompleteMultipartUpload: cache_key={}, upload_id={}, error={}",
                    cache_key, upload_id, e
                );
                // Continue with operation - don't fail CompleteMultipartUpload due to cache invalidation failure
            } else {
                debug!(
                    "Successfully invalidated cache for CompleteMultipartUpload: cache_key={}, upload_id={}",
                    cache_key, upload_id
                );
            }
        }

        // Test-only injection: a peer republishing this key's `.meta` (and its range files)
        // in the window between the invalidation above and the renames below.
        #[cfg(test)]
        republish_after_invalidation(cache_key);

        // Remove the object this completion replaces BEFORE the part renames, not after.
        //
        // Range files are named `{key}_{start}-{end}.bin`, so a new range with the same extent
        // as an old one lands at the same path. With the renames first, deleting the old
        // `.meta`'s `file_path`s removed the NEW object's file at every shared extent, and the
        // new `.meta` then named a missing file while its credit counted the bytes; a re-upload
        // with the same part size collides on every part. Deleting first leaves the renames
        // the last writer of every path. The old `.meta` goes too, so nothing reading it after
        // this point can name the new object's files. Staging eviction's
        // `evict_write_cached_object` reads the `.meta` and deletes what it names under the
        // same slot lock, so it runs wholly before this removal or wholly after the
        // publish. The replaced object is removed at once either way.
        // `a_replaced_multipart_object_does_not_delete_the_new_objects_shared_extent`;
        // `an_eviction_that_read_the_replaced_meta_does_not_delete_the_new_objects_files`.
        let metadata_dir = self.cache_dir.join("metadata");

        // Use sharded path for metadata file to match the rest of the codebase
        // Path format: metadata/{bucket}/{XX}/{YYY}/{sanitized_key}.meta
        let metadata_file = crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta")
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to get sharded metadata path for cache_key={}: {}",
                    cache_key, e
                ))
            })?;

        // ── The `.meta` transaction: read the old document, dereference what this
        //    completion supersedes, rename the parts into place, publish the new one. ──────
        //
        // The key's metadata slot lock spans all four. `upload.lock`, held above, is keyed on
        // `upload_id` and lives in a different namespace, so it excludes no `.meta` writer of
        // this key — a second Complete of a different upload overwriting the same key, a
        // consolidation, or a HEAD-metadata merge are all outside it. Taking the slot lock
        // here is the documented order (`upload.lock`, then the slot lock innermost — a
        // per-key `locks/{key}.lock` sentinel used to sit between them and is retired), and
        // it is taken AFTER
        // `invalidate_cache_unified_for_operation` above, which acquires and releases the same
        // slot itself — acquiring before that call would self-deadlock on the same slot file.
        //
        // Off the runtime for the acquisition, with a bound, matching
        // `disk_cache::batch_delete_ranges`: a contended slot must not park a runtime worker.
        // The bound is `cache.shared_storage.lock_acquire_wait_budget`, whose default is the
        // five seconds this site used to hardcode.
        let meta_lock = crate::metadata_lock_manager::acquire_slot_lock_within_budget(
            &self.cache_dir,
            cache_key,
        )
        .await?;

        // The write-order check, under the slot lock this publish holds to the end and
        // before anything is removed or renamed: a newer acknowledged write or delete of the key
        // replaced this completion's token, so it must not publish. Nor while this proxy holds a
        // pending stamp for the key: a newer write here was acknowledged without moving
        // the marker, so the token still holds.
        if let Some(token) = &token {
            if !crate::cache::write_marker_holds(&metadata_file, token)
                || self
                    .cache_manager
                    .as_ref()
                    .is_some_and(|m| m.pending_stamp_probe(cache_key).declines())
            {
                info!(
                    "CompleteMultipartUpload not cached: a newer write of the key was \
                     acknowledged first, or its pre-response invalidation failed: cache_key={}, \
                     upload_id={}",
                    cache_key, upload_id
                );
                drop(meta_lock);
                drop(lock_file);
                self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                    .await;
                if let Some(m) = &self.metrics_manager {
                    m.read()
                        .await
                        .record_skipped_put(if guard_unavailable {
                            "commit_failed"
                        } else {
                            SUPERSEDED_SKIP_REASON
                        })
                        .await;
                }
                return Ok(());
            }
        }

        // Whether a parseable `.meta` this publish replaces was unlinked under the slot lock.
        // `cached_objects` counts one per parseable `.meta`, so the publish credits only when
        // no counted document was there.
        let mut replaced_counted_meta = false;
        if metadata_file.exists() {
            // Read old metadata to get list of old range files. UNDER the lock, so the range
            // list deleted below is the one this publish actually supersedes.
            if let Ok(old_metadata_content) = tokio::fs::read_to_string(&metadata_file).await {
                if let Ok(old_metadata) =
                    serde_json::from_str::<NewCacheMetadata>(&old_metadata_content)
                {
                    info!(
                        "Deleting {} old range files for overwritten object: cache_key={}",
                        old_metadata.ranges.len(),
                        cache_key
                    );

                    // The deletion is ROUTED THROUGH THE DEBITING PATH, which it was not
                    // before (cache-eviction-at-scale). It used to be a bare
                    // `tokio::fs::remove_file` loop with no `subtract`, `subtract_write_cache`
                    // or `Remove` journal entry anywhere in this function, so bytes that left
                    // the disk here stayed counted in `total_size` — and, where the
                    // overwritten entry was itself write-cached, in `write_cache_size` — while
                    // the completion credited the new ranges on top. Same pair
                    // `dereference_superseded_full_object_ranges` uses, so the classification
                    // and the already-absent filter cannot drift between the two overwrite
                    // paths.
                    //
                    // REACHABILITY, because it bounds how much this was costing.
                    // `invalidate_cache_unified_for_operation` above normally deletes this
                    // `.meta` and its range files already, debiting both channels through
                    // `delete_object_entry_locked`. So this block runs only when that
                    // invalidation did NOT remove the entry — it logs a warning and continues
                    // on failure, and slot contention is now one of the ways it can fail — or
                    // when a peer republished the `.meta` in the window between. The
                    // accounting error was therefore conditional on a failed invalidation
                    // rather than incurred on every overwrite, which is a smaller finding than
                    // "every multipart overwrite leaks a credit". It is still unbounded when it
                    // does happen: nothing else ever debits those bytes.
                    //
                    // `batch_delete_ranges` would also debit, and is deliberately NOT used: it
                    // acquires this key's slot lock itself, which would deadlock against the
                    // guard held here.
                    match &self.cache_manager {
                        Some(cache_mgr) => {
                            let (removed, failed) = cache_mgr.remove_range_files(
                                cache_key,
                                &old_metadata.ranges,
                                old_metadata.object_metadata.is_write_cached,
                            );
                            cache_mgr.debit_removed_ranges(cache_key, &removed).await;
                            info!(
                                "Superseded ranges of overwritten multipart object debited: cache_key={}, removed={}, failed={}",
                                cache_key,
                                removed.len(),
                                failed
                            );
                        }
                        None => {
                            // No cache manager wired, so no accounting exists to keep straight
                            // and the raw deletion is the whole of the correct behaviour. The
                            // disk is still freed.
                            for old_range in &old_metadata.ranges {
                                let old_range_file = ranges_dir.join(&old_range.file_path);
                                if let Err(e) = tokio::fs::remove_file(&old_range_file).await {
                                    warn!(
                                        "Failed to delete old range file: file={}, error={}",
                                        old_range.file_path, e
                                    );
                                } else {
                                    debug!("Deleted old range file: {}", old_range.file_path);
                                }
                            }
                        }
                    }
                    // The replaced `.meta` goes now as well, so no reader between here and the
                    // publish below can take its `file_path`s for the old object's. The publish
                    // would replace it by rename anyway; an early return below now leaves the
                    // key uncached rather than naming files that are gone or that hold the new
                    // object's bytes.
                    match tokio::fs::remove_file(&metadata_file).await {
                        // A parseable document, so counted in `cached_objects`; the publish
                        // below replaces it and must not credit a second time.
                        Ok(()) => replaced_counted_meta = true,
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::NotFound {
                                warn!(
                                    "Failed to remove replaced metadata file: cache_key={}, file={:?}, error={}",
                                    cache_key, metadata_file, e
                                );
                            }
                        }
                    }
                }
            }
        }

        // Cross-check the assembled part count against what S3 itself reports
        // (Requirement 5.3).
        //
        // Offsets and `content_length` below are derived purely by summing the
        // tracker's part sizes and are otherwise never compared with S3, so a
        // tracker holding a subset yields self-consistent metadata describing a
        // SHORTER object — and a later GET then serves the wrong length and the
        // wrong bytes. The guards above make that unreachable, but they are
        // guards on our own bookkeeping; this is the one check anchored to S3.
        //
        // A multipart ETag has the form `"<md5-of-md5s>-<part-count>"`, and that
        // suffix is the only size-related fact S3 returns here — the
        // CompleteMultipartUpload response carries no object length, so a byte
        // comparison would need an extra HEAD on a client-visible path. The count
        // is enough to catch the truncation signature, which is a missing part.
        //
        // BEFORE the part renames and AFTER the replaced object's removal. After the
        // renames, declining left every renamed part in `ranges/` named by no `.meta` and
        // counted nowhere until orphan recovery reached it. Before the removal, declining would
        // leave the replaced object cached although S3 now holds a different one. So the old
        // object is gone, no part has moved, and the staging directory is removed with its
        // parts, as the ETag-mismatch decline does.
        if let Some((_, s3_part_count)) = etag.trim_matches('"').rsplit_once('-') {
            if let Ok(s3_part_count) = s3_part_count.parse::<usize>() {
                if s3_part_count != sorted_parts.len() {
                    warn!(
                        "CompleteMultipartUpload part-count disagrees with S3: S3 reports {} part(s) via the ETag, we assembled {}: cache_key={}, upload_id={}, skipping cache finalization to avoid caching a truncated object",
                        s3_part_count,
                        sorted_parts.len(),
                        cache_key,
                        upload_id
                    );
                    drop(meta_lock);
                    drop(lock_file);
                    self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                        .await;
                    return Ok(());
                }
            }
        }

        #[cfg(test)]
        remove_part_before_finalize_renames(&multipart_dir);

        // Rename part files with final byte offsets and create range specs
        let mut range_specs = Vec::new();
        // Range files this loop has already moved into `ranges/`. Every return between the first
        // rename and the `.meta` publish (a declined finalization or any `?` below) drops it armed,
        // which removes them, so nothing is left named by no `.meta`.
        let mut moved_range_files = UnpublishedRangeFiles::default();

        for (part_number, start, end) in &byte_offsets {
            // Find the part info
            let part_info = sorted_parts
                .iter()
                .find(|p| p.part_number == *part_number)
                .ok_or_else(|| {
                    ProxyError::CacheError(format!("Part {} not found in tracker", part_number))
                })?;

            // Part file is in the upload directory
            let old_part_file = multipart_dir.join(format!("part{}.bin", part_number));

            // New range file path (with byte offset suffix) in the sharded ranges directory
            let suffix = format!("_{}-{}.bin", start, end);
            let new_range_file_path =
                crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, &suffix).map_err(
                    |e| {
                        ProxyError::CacheError(format!(
                            "Failed to get sharded path for part {}: {}",
                            part_number, e
                        ))
                    },
                )?;

            // Ensure parent directories exist for the destination
            #[cfg(test)]
            if !moved_range_files.is_empty() {
                finalize_fault(cache_key, FinalizeFault::RangeDirCreate)?;
            }
            if let Some(parent) = new_range_file_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Failed to create parent directories for range file: {}",
                        e
                    ))
                })?;
            }

            // Move part file from upload dir to final range location
            if old_part_file.exists() {
                #[cfg(test)]
                if !moved_range_files.is_empty() {
                    finalize_fault(cache_key, FinalizeFault::PartRename)?;
                }
                tokio::fs::rename(&old_part_file, &new_range_file_path)
                    .await
                    .map_err(|e| {
                        ProxyError::CacheError(format!(
                            "Failed to move part file to range file: {}",
                            e
                        ))
                    })?;

                debug!(
                    "Moved part file: {:?} -> {:?}",
                    old_part_file, new_range_file_path
                );
                moved_range_files.push(new_range_file_path.clone());
            } else {
                // The part was present at the existence check above and is gone now: something
                // removed the staging directory (or this part) between the two. Publishing would
                // name a range that does not exist and credit its recorded bytes, so the cache
                // finalization is declined. S3 already holds the object; only caching is skipped.
                warn!(
                    "Part file vanished during finalization, declining to cache: cache_key={}, upload_id={}, part={:?}",
                    cache_key, upload_id, old_part_file
                );
                moved_range_files.remove_all();
                drop(meta_lock);
                drop(lock_file);
                self.cleanup_incomplete_multipart_cache(&multipart_dir, upload_id)
                    .await;
                return Ok(());
            }

            // Calculate relative path from ranges directory
            let range_file_relative_path = new_range_file_path
                .strip_prefix(&ranges_dir)
                .map_err(|e| {
                    ProxyError::CacheError(format!(
                        "Failed to compute relative path for part {}: {}",
                        part_number, e
                    ))
                })?
                .to_string_lossy()
                .to_string();

            // The range's on-disk length is the one its WRITER measured, carried on the part
            // record (`CachedPartInfo::compressed_size`, stat-ed inside the `part{N}.lock` hold
            // one statement after the rename that published the file). Not this instance's own
            // `stat`.
            //
            // The part may have been written by another proxy. `finalize_incremental_part`
            // closes the file before it renames it into place, so its bytes have reached the
            // server by the rename, but a `stat` from here is answered from this proxy's NFS
            // attribute cache, which need not have caught up. Before the close moved ahead of
            // the rename, the earlier fleet test recorded a cross-proxy part 2 at 0 bytes against
            // 8,388,760 on disk, and every credit and the fold's `staged_compressed_size()` took
            // the 0. The writer's measurement is the figure the file has on the server, which is
            // the only figure that stays true.
            //
            // The `stat` is kept as the fallback for a record that carries no measurement (a
            // pre-§ 16.6.2 record, or a writer whose own `stat` failed, both `0`).
            //`a_part_finalized_on_another_proxy_is_recorded_at_its_writers_size`.
            let observed = tokio::fs::metadata(&new_range_file_path)
                .await
                .map(|m| m.len());
            let compressed_size = if part_info.compressed_size > 0 {
                if let Ok(observed) = &observed {
                    if *observed != part_info.compressed_size {
                        debug!(
                            "Part {} of {} observed at {} bytes here and recorded at {} by its \
                             writer; using the writer's figure",
                            part_number, cache_key, observed, part_info.compressed_size
                        );
                    }
                }
                part_info.compressed_size
            } else {
                observed.unwrap_or(part_info.size)
            };

            // Create range spec using the actual compression algorithm recorded
            // for the part when it was written
            // The part's `.bin` is renamed into place unchanged (same length and modification
            // time), so the identity
            // its writer recorded on the part record describes this range. A record with none
            // (written before the field existed) leaves the range unverified.
            let range_spec = RangeSpec::new(
                *start,
                *end,
                range_file_relative_path,
                part_info.compression_algorithm.clone(), // actual algorithm recorded at part write
                compressed_size,
                part_info.size,
            )
            .with_file_identity(part_info.file_identity);

            range_specs.push(range_spec);

            debug!(
                "Created range for part {}: start={}, end={}, size={} bytes",
                part_number, start, end, part_info.size
            );
        }

        // Calculate total size from filtered parts (Requirements 5.3)
        // This ensures the object size matches what S3 returns (only requested parts)
        let total_size: u64 = sorted_parts.iter().map(|p| p.size).sum();
        let now = std::time::SystemTime::now();
        let write_ttl = std::time::Duration::from_secs(86400); // 1 day default

        // Build part_ranges from byte_offsets (Requirements 7.1, 7.2)
        // Maps part number to (start_offset, end_offset) byte range
        let part_ranges: std::collections::HashMap<u32, (u64, u64)> = byte_offsets
            .iter()
            .map(|(part_number, start, end)| (*part_number, (*start, *end)))
            .collect();

        // Create object metadata with write cache fields (Requirements 3.1, 3.3, 3.4)
        // Note: S3 CompleteMultipartUpload doesn't return Last-Modified or the object's Content-Type
        // The XML response has content-type: application/xml which is NOT the object's content-type
        // The object's Content-Type and other representation headers are the ones its
        // CreateMultipartUpload recorded, merged below; Complete does not cache an upload that
        // recorded none.

        // Filter out content-type from response headers - it's the XML response type, not the object type
        // Also filter out content-length as it's the XML response length, not the object size
        let mut filtered_response_headers: std::collections::HashMap<String, String> =
            response_headers
                .iter()
                .filter(|(k, _)| {
                    let key_lower = k.to_lowercase();
                    key_lower != "content-type" && key_lower != "content-length"
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
        // The object's own representation headers, recorded at CreateMultipartUpload.
        filtered_response_headers.extend(match recorded_headers {
            Some(recorded) => recorded,
            None => Self::recorded_representation_headers(&multipart_dir)
                .await
                .unwrap_or_default(),
        });

        // Read the header from the response with an empty default, matching the
        // two single-part sites (`build_object_metadata_from_streamed_put`,
        // `spawn_buffered_put_cache_write_task`). Behaviour-neutral against real
        // S3 today — task 0.0 measured all three write responses as omitting
        // this header — so this closes an asymmetry rather than fixing a live
        // defect: without it, an S3-compatible origin that DOES send
        // Last-Modified on CompleteMultipartUpload would have it silently
        // retained in `filtered_response_headers` (which strips only
        // content-type/content-length) while the typed field stayed empty
        // forever, disagreeing with itself.
        // Spec: write-cache-last-modified. Requirements: 1.6, 1.7
        let last_modified = response_headers
            .get("last-modified")
            .or_else(|| response_headers.get("Last-Modified"))
            .cloned()
            .unwrap_or_default();

        let object_metadata = ObjectMetadata {
            etag: etag.to_string(),
            last_modified,
            content_length: total_size,
            content_type: tracker.content_type.clone(), // Use content-type from CreateMultipartUpload if provided
            response_headers: filtered_response_headers,
            upload_state: UploadState::Complete,
            cumulative_size: total_size,
            parts: Vec::new(),
            compression_algorithm: CompressionAlgorithm::Lz4, // Multipart uses per-range compression
            compressed_size: range_specs.iter().map(|r| r.compressed_size).sum(),
            parts_count: Some(sorted_parts.len() as u32), // Use filtered parts count (Requirement 7.3)
            part_ranges,
            upload_id: None,
            is_write_cached: true, // Mark as write-cached
            write_cache_expires_at: Some(now + write_ttl),
            write_cache_created_at: Some(now),
            write_cache_last_accessed: Some(now),
            graduation_accounted: false,
        };

        // Record per-range staging membership before the `.meta` is written.
        //
        // A FIFTH credit site, not in R12.4's list of four, and it is the one that
        // matters most for the defect. R12.4 names
        // `write_multipart_journal_entries` as the multipart credit site, which is
        // where the *accounting* credit happens — but this function writes the
        // `.meta` **directly and first** (for atomicity, see the comment on the
        // journal call below), so the persisted range is this copy, not the journal's.
        // Stamping only the journal copy would leave every multipart part on disk with
        // `staged: None` and therefore on the object-flag fallback.
        //
        // That is precisely the reachable shape: R12's mixed state needs a flagged
        // object with incomplete range coverage that then takes a GET range-miss, and
        // task 74 records multipart write-through as its most likely production route.
        // Leaving this site out would have fixed the defect everywhere except where it
        // actually occurs.
        //
        // Derived from the object flag rather than hardcoded `Some(true)`, so a future
        // change to `is_write_cached` above cannot silently mis-tier every part.
        // Spec: write-cache-accounting-and-eviction. Requirements: 12.2
        for range_spec in &mut range_specs {
            range_spec.staged = Some(crate::cache_types::classify_new_range_as_staged(
                &range_spec.file_path,
                object_metadata.is_write_cached,
            ));
        }

        let cache_metadata = NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata,
            ranges: range_specs,
            created_at: now,
            expires_at: now + write_ttl, // Use write cache TTL
            compression_info: crate::cache_types::CompressionInfo::default(),
            ..Default::default()
        };

        // Write metadata file - create parent directories for sharded path
        #[cfg(test)]
        finalize_fault(cache_key, FinalizeFault::MetaDirCreate)?;
        if let Some(parent) = metadata_file.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!("Failed to create metadata directory: {}", e))
            })?;
        }

        let metadata_json = serde_json::to_string_pretty(&cache_metadata)
            .map_err(|e| ProxyError::CacheError(format!("Failed to serialize metadata: {}", e)))?;

        // Write metadata file atomically using temp file + rename.
        //
        // The temp path is instance-scoped (`{key}.meta.tmp.{hostname}.{pid}`). It used to be
        // `metadata_file.with_extension("tmp")`, which was worse than merely fleet-wide fixed:
        // `with_extension` REPLACES the `.meta` extension, so the name was `{key}.tmp` and
        // matched neither temp-file sweep — both test for `.meta.tmp.` — leaving orphans of a
        // shape nothing reaps. See `cache::instance_scoped_meta_tmp_path`.
        let temp_metadata_file = crate::cache::instance_scoped_meta_tmp_path(&metadata_file);
        #[cfg(test)]
        finalize_fault(cache_key, FinalizeFault::MetaTmpWrite)?;
        // Synced before the rename below publishes it; see `disk_cache::write_synced`.
        crate::disk_cache::write_synced(&temp_metadata_file, &metadata_json).map_err(|e| {
            ProxyError::CacheError(format!("Failed to write temporary metadata file: {}", e))
        })?;

        // ── Ledger first, publish second (cache-eviction-at-scale § 16.6.12 L3). ─────────
        //
        // Every staged range gets its Write_Ledger entry BEFORE the rename makes the `.meta`
        // visible. A crash between the two therefore leaves entries naming a `.meta` that does
        // not exist, which `assess_staged_candidate` reads as `MetadataAbsent` and retires once
        // they are older than `write_ledger::ABSENT_RETIREMENT_GRACE`. The old order (rename,
        // then append inside `write_multipart_journal_entries`) left a published staged `.meta`
        // with no entry: invisible to staging eviction, uncounted by the fold, an under-count
        // of `write_cache_size` that biases the eviction trigger LATE until the ledger-repair
        // rotation reached the shard.
        //
        // Only the LEDGER append moves. The journal `Add` entries and the size credits stay
        // after the rename, because consolidation creates a `.meta` from `Add` entries when
        // none exists, so writing them first would let a crash publish the object anyway.
        //
        // No consolidator wired means no ledger and no accounting to keep straight, so the
        // publish goes ahead exactly as before.
        let consolidator = match &self.cache_manager {
            Some(cache_mgr) => cache_mgr.get_journal_consolidator().await,
            None => None,
        };
        let ledger_recorded = match &consolidator {
            Some(consolidator) => {
                consolidator
                    .record_staged_ranges_before_publish(
                        cache_key,
                        &cache_metadata.ranges,
                        cache_metadata.object_metadata.is_write_cached,
                    )
                    .await
            }
            None => true,
        };

        // Test-only crash point between the two steps: returning here is what a process that
        // died between them leaves on shared storage.
        #[cfg(test)]
        if crash_between_ledger_and_publish(cache_key) {
            // A crash runs no cleanup.
            moved_range_files.disarm();
            return Ok(());
        }

        if ledger_recorded {
            // Atomically rename to final location. On failure the guard removes the moved
            // range files; ledger entries already appended name an absent `.meta` and retire.
            #[cfg(test)]
            finalize_fault(cache_key, FinalizeFault::MetaRename)?;
            #[cfg(test)]
            reseed_before_meta_rename(cache_key, self.cache_manager.as_ref()).await;
            tokio::fs::rename(&temp_metadata_file, &metadata_file)
                .await
                .map_err(|e| {
                    ProxyError::CacheError(format!("Failed to rename metadata file: {}", e))
                })?;
            moved_range_files.disarm();
            // The published `.meta` now stands for this completion; its marker is cleared in
            // the same slot-lock hold. The slot lock is still held here.
            if token.is_some() {
                let marker = crate::cache::write_marker_path(&metadata_file);
                if let Err(e) = tokio::fs::remove_file(&marker).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        debug!("Failed to remove write marker {:?}: {}", marker, e);
                    }
                }
            }
            // A reader racing between the pre-publish invalidation above and this rename can
            // have re-seeded this instance's RAM metadata tier from the superseded `.meta`.
            // Drop it now that the new document is in place. Local hygiene: a peer's RAM copy
            // is bounded by its own `metadata_cache.refresh_interval`.
            if let Some(cache_mgr) = &self.cache_manager {
                cache_mgr.invalidate_metadata_cache(cache_key).await;
            }
        } else {
            // A ledger append failed, so the completion is NOT cached: no `.meta` is
            // published and the range files it would have named are removed.
            //
            // Publishing it staged without an entry is the loss mode this ordering exists to
            // remove. Publishing it read-cached instead was rejected: a multipart completion
            // carries no `Last-Modified`, and the path that learns one
            // (write-cache-last-modified's GET revalidation trigger) is keyed on
            // `is_write_cached`, so a read-cached copy would be served without it
            // indefinitely. Declining costs one cache miss.
            //
            // Nothing below has credited these ranges yet (the credits run after the
            // publish), so removing them needs no debit. Entries that landed before the
            // failure name this unpublished `.meta` and take the `MetadataAbsent` path.
            MPU_COMPLETION_LEDGER_DECLINES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(
                "CompleteMultipartUpload not cached: a Write_Ledger append failed before the \
                 .meta publish: cache_key={}, upload_id={}, ranges={}",
                cache_key,
                upload_id,
                cache_metadata.ranges.len()
            );
            if let Err(e) = tokio::fs::remove_file(&temp_metadata_file).await {
                warn!(
                    "Failed to remove unpublished temp metadata file: file={:?}, error={}",
                    temp_metadata_file, e
                );
            }
            for range in &cache_metadata.ranges {
                let range_file = ranges_dir.join(&range.file_path);
                if let Err(e) = tokio::fs::remove_file(&range_file).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!(
                            "Failed to remove range file of an uncached completion: file={}, error={}",
                            range.file_path, e
                        );
                    }
                }
            }
            moved_range_files.disarm();
        }

        // The `.meta` transaction ends at the rename. Everything below — part-record deletion,
        // journalling, the staged-entry gauge, logging — neither reads nor writes this `.meta`,
        // and the slot is one of 4,096 shared with unrelated keys, so it is released here
        // rather than at the end of the function.
        drop(meta_lock);

        // The completion's `.meta` credit. Its `Add` entries fold onto a document that already
        // lists every part, so consolidation does not credit it. After the release because the
        // increment takes `size_state.lock`.
        if ledger_recorded {
            if let Some(cache_mgr) = &self.cache_manager {
                cache_mgr
                    .credit_put_cached_object(cache_key, replaced_counted_meta)
                    .await;
            }
        }

        debug!(
            "Created metadata file atomically: {} ({} ranges)",
            metadata_file.display(),
            cache_metadata.ranges.len()
        );

        // THE EXACTLY-ONCE HANDOFF BOUNDARY (cache-eviction-at-scale § 16.6.2).
        //
        // The `part{N}.json` records and the `.meta` just published describe the SAME
        // bytes, so for as long as both exist the in-progress term and the staged term
        // both count them. Deleting the records here — still under `upload.lock`,
        // immediately after the publish — bounds that window to one `.meta` write inside
        // one lock hold. Until now the records survived until the `remove_dir_all` after
        // the lock was dropped, i.e. the window was bounded by nothing at all.
        //
        // The window is a deliberate OVER-count, and the direction is the safety rule,
        // not an accident. The alternative ordering (delete the records first, then
        // publish) leaves a window counted by neither term — an under-count, which makes
        // an eviction trigger fire LATE. Do not "improve" this by swapping the two.
        //
        // Crash between the publish and this deletion leaves orphaned `part{N}.json`
        // records: a persistent over-count of one object. The bound on it is
        // `incomplete_upload_ttl`, a configured value, NOT a fold or consolidation
        // interval — `write_cache_manager::sweep_incomplete_uploads` is what reclaims the
        // directory. So the symptom is `in_progress_part_bytes` failing to fall after a
        // completion, and it persists for a TTL rather than clearing on the next pass.
        let records_dir = multipart_dir.clone();
        match tokio::task::spawn_blocking(move || Self::delete_part_records_blocking(&records_dir))
            .await
        {
            Ok(removed) => debug!(
                "Deleted {} part record(s) under upload.lock: upload_id={}",
                removed, upload_id
            ),
            Err(e) => warn!(
                "Part record deletion task panicked: upload_id={}, error={} \
                 (in_progress_part_bytes will over-count until incomplete_upload_ttl)",
                upload_id, e
            ),
        }

        // Write journal entries for size tracking
        // The metadata file is written directly for atomicity, but we need journal entries
        // so the consolidator can track the size delta for this multipart upload. The
        // ledger entries were appended before the publish above, so this credits without
        // appending them again. Skipped entirely when the completion was declined.
        if ledger_recorded {
            if let Some(cache_mgr) = &self.cache_manager {
                if let Some(consolidator) = &consolidator {
                    consolidator
                        .write_multipart_journal_entries_prerecorded(
                            cache_key,
                            cache_metadata.ranges.clone(),
                            cache_metadata.object_metadata.clone(),
                        )
                        .await;
                }

                // Best-effort staged-entry gauge: a new write-cached (is_write_cached:
                // true) `.meta` was just committed atomically above.
                // Spec: write-cache-accounting-and-eviction. Requirements: 8.2, 8.3
                cache_mgr.increment_write_cache_staged_entries().await;
            }
        }

        // Log the final summary with human-readable format
        let (bucket, key) = parse_cache_key(cache_key);
        info!(
            "CompleteMultipartUpload: bucket={}, key={}, parts={}, total_size={}, etag={}, cached={}",
            bucket,
            key,
            cache_metadata.ranges.len(),
            format_size(total_size),
            truncate_etag(etag),
            ledger_recorded
        );

        // Last observation point at which `upload.lock` is still held. See
        // `PRE_UNLOCK_OBSERVER`: the part-record deletion above must be complete by
        // here, and only a probe on this side of the `drop` can tell that apart from a
        // deletion performed by the `remove_dir_all` below.
        #[cfg(test)]
        {
            // Poison-tolerant: a panicking test elsewhere must not turn this probe
            // into a second failure in an unrelated test.
            let observer = PRE_UNLOCK_OBSERVER
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(observe) = observer.as_ref() {
                observe(&multipart_dir);
            }
        }

        // Release lock before cleanup
        drop(lock_file);

        // Clean up multipart directory (delete mpus_in_progress/{uploadId}/)
        if let Err(e) = tokio::fs::remove_dir_all(&multipart_dir).await {
            warn!(
                "Failed to clean up multipart directory: upload_id={}, error={}",
                upload_id, e
            );
        } else {
            debug!("Cleaned up multipart directory: upload_id={}", upload_id);
        }

        Ok(())
    }

    /// Wait, up to [`MULTIPART_COMPLETE_CACHE_WAIT`], for every part named by this
    /// `CompleteMultipartUpload` to appear in the on-disk tracker.
    ///
    /// Returns once the tracker holds them all, or once the bound elapses —
    /// never an error. Exceeding the bound is not a failure of the upload (S3
    /// has already accepted it) and not a failure of this request; it degrades to
    /// "not cached", which the caller's existing missing-parts guard then reports.
    ///
    /// The tracker is read WITHOUT taking `upload.lock`. That is deliberate: the
    /// read is a monotonic "have the parts I need landed yet" poll, and taking the
    /// exclusive lock on every attempt would contend with the very part tasks
    /// being waited for and serialise them behind the waiter. A torn or
    /// half-written tracker simply fails to parse and is treated as "not yet",
    /// which is the correct answer for a poll. The authoritative read still
    /// happens under the lock in the caller.
    async fn await_tracker_parts(
        multipart_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
        requested_parts: &[RequestedPart],
    ) {
        let wanted: std::collections::HashSet<u32> =
            requested_parts.iter().map(|p| p.part_number).collect();

        let deadline = std::time::Instant::now() + MULTIPART_COMPLETE_CACHE_WAIT;
        let mut waited_ms: u64 = 0;

        loop {
            // Existence of `part{N}.json` is the whole question, so this checks for
            // files rather than parsing anything. It used to parse the entire
            // `upload.meta` on every poll, which grew with the part count — so the
            // poll itself got more expensive the more parts there were to wait for,
            // while holding up the very Complete that was waiting.
            //
            // ON A BLOCKING THREAD, and that is not incidental. These are synchronous
            // filesystem calls against a shared network volume, in a loop that ticks
            // every 100 ms. Called directly from this async fn they pin a runtime
            // worker for the duration of every poll, and at high part counts a
            // 2,000-entry staging directory on EFS made that long enough to starve
            // the forward path and fail the client's upload — trading the lock
            // contention this design removed for a different way to lose the same way.
            let dir = multipart_dir.to_path_buf();
            let wanted_now = wanted.clone();
            let present = match tokio::task::spawn_blocking(move || {
                Self::present_parts_blocking(&dir, &wanted_now)
            })
            .await
            {
                Ok(present) => present,
                Err(e) => {
                    warn!(
                        "Part-record poll task failed: {} — treating as not yet landed",
                        e
                    );
                    std::collections::HashSet::new()
                }
            };

            if wanted.is_subset(&present) {
                if waited_ms > 0 {
                    debug!(
                        "Multipart Complete waited {}ms for {} part record(s) to land: cache_key={}, upload_id={}",
                        waited_ms,
                        wanted.len(),
                        cache_key,
                        upload_id
                    );
                }
                return;
            }

            if std::time::Instant::now() >= deadline {
                let missing = wanted.len() - wanted.intersection(&present).count();
                warn!(
                    "Multipart Complete waited {:?} and {} of {} part record(s) still had not landed: cache_key={}, upload_id={} — this object will not be cached (upload unaffected)",
                    MULTIPART_COMPLETE_CACHE_WAIT,
                    missing,
                    wanted.len(),
                    cache_key,
                    upload_id
                );
                return;
            }

            tokio::time::sleep(TRACKER_POLL_INTERVAL).await;
            waited_ms += TRACKER_POLL_INTERVAL.as_millis() as u64;
        }
    }

    /// Clean up incomplete multipart cache data
    ///
    /// This method removes partial cache data when CompleteMultipartUpload succeeds on S3
    /// but the proxy doesn't have complete local state. This prevents serving corrupted
    /// data from incomplete cache entries.
    ///
    /// Note the missing-parts path no longer calls this: deleting the staging
    /// directory while part-cache tasks may still be writing into it is what
    /// produced the ENOENT/ESTALE error cascade. Remaining callers are the ones
    /// where the tracker itself is unusable.
    ///
    /// # Arguments
    ///
    /// * `multipart_dir` - Path to the multipart upload directory
    /// * `upload_id` - The upload ID for logging
    async fn cleanup_incomplete_multipart_cache(
        &self,
        multipart_dir: &std::path::Path,
        upload_id: &str,
    ) {
        // Parts are stored inside the upload directory, so a single remove_dir_all cleans everything
        if let Err(e) = tokio::fs::remove_dir_all(multipart_dir).await {
            warn!(
                "Failed to remove multipart directory during incomplete cleanup: upload_id={}, error={}",
                upload_id, e
            );
        } else {
            info!(
                "Cleaned up incomplete multipart cache: upload_id={}",
                upload_id
            );
        }
    }

    /// Sanitize a cache key for safe path construction
    ///
    /// Removes leading slashes to prevent PathBuf::join() from treating
    /// the key as an absolute path, which would replace the cache directory.
    /// Also handles very long paths by hashing them to ensure filesystem
    /// compatibility.
    ///
    /// # Background
    ///
    /// Rust's `PathBuf::join()` has special behavior with absolute paths:
    /// ```rust
    /// use std::path::PathBuf;
    /// let base = PathBuf::from("/var/cache");
    /// let absolute = "/bucket/key";
    /// let result = base.join(absolute);
    /// // result = "/bucket/key" (NOT "/var/cache/bucket/key")
    /// ```
    ///
    /// This function strips leading slashes to ensure paths are always
    /// constructed relative to the cache directory. It also hashes very
    /// long paths to stay within filesystem limits (typically 255 bytes
    /// per path component).
    ///
    /// # Arguments
    ///
    /// * `cache_key` - The raw cache key (e.g., "/bucket/object")
    ///
    /// # Returns
    ///
    /// A sanitized cache key safe for path joining (e.g., "bucket/object")
    ///
    /// # Examples
    ///
    /// ```
    /// # // This is a private method, so we can't test it directly in doctests
    /// # // The functionality is tested in unit tests
    /// ```
    ///
    /// Extract ETag from CompleteMultipartUpload XML response
    ///
    /// S3 returns the ETag in the XML body, not in headers:
    /// ```text
    /// <CompleteMultipartUploadResult>
    ///   <ETag>"etag-value"</ETag>
    ///   ...
    /// </CompleteMultipartUploadResult>
    /// ```
    fn extract_etag_from_xml(xml: &str) -> String {
        debug!(
            "Attempting to extract ETag from XML response (length: {} bytes)",
            xml.len()
        );

        // Simple XML parsing to extract ETag value
        // Look for <ETag>value</ETag> pattern (case-insensitive)
        if let Some(start_pos) = find_ascii_case_insensitive(xml, "<etag>") {
            let after_tag = &xml[start_pos + "<etag>".len()..];
            if let Some(end_pos) = find_ascii_case_insensitive(after_tag, "</etag>") {
                let etag = after_tag[..end_pos].trim();
                // Store the QUOTED form, which is what S3 returns on GET/HEAD and what
                // every other cache writer stores. This used to strip the quotes, so
                // every multipart entry compared unequal to the next HEAD/GET validator
                // and was invalidated. The XML may carry the quotes literally or as an
                // entity, so decode the entity forms before normalising to one balanced
                // pair; an ETag's opaque tag cannot itself contain a `"`.
                let decoded = etag
                    .replace("&quot;", "\"")
                    .replace("&#34;", "\"")
                    .replace("&#x22;", "\"");
                let opaque = decoded.trim_matches('"');
                if opaque.is_empty() {
                    // No validator. Stored as empty, which every comparison site treats
                    // as "nothing to compare", exactly as before.
                    debug!("Extracted an empty ETag from XML");
                    return String::new();
                }
                let quoted_etag = format!("\"{}\"", opaque);
                debug!("Extracted ETag from XML: {}", quoted_etag);
                return quoted_etag;
            }
        }

        // Fallback: return empty string if ETag not found
        warn!(
            "Failed to extract ETag from CompleteMultipartUpload response XML. XML content: {}",
            xml.get(..500).unwrap_or(xml)
        );
        String::new()
    }

    /// Spawn a background task to handle cache writing asynchronously
    ///
    /// This function spawns a tokio task that:
    /// 1. Waits for the S3 result
    /// 2. On S3 success, stores data directly as a single range (0 to content-length-1)
    /// 3. Sets is_write_cached=true and write_cache_expires_at in metadata
    /// 4. On S3 failure, discards any cached data
    ///
    /// # Arguments
    ///
    /// * `cache_key` - Cache key (will be sanitized internally)
    /// * `body_data` - The request body data (already read)
    /// * `s3_result_rx` - Channel receiver for S3 operation result
    /// * `cache_dir` - Cache directory path
    /// * `compression_handler` - Compression handler for cache writes
    /// * `content_length` - Optional content length
    /// * `request_headers` - Request headers for metadata extraction
    /// * `metrics` - Optional metrics manager
    /// * `cache_manager` - Optional cache manager for storing as range with write cache metadata
    ///
    /// # Requirements (write-through-cache-finalization)
    ///
    /// - Requirement 1.1: Store object data as single range (0 to content-length-1)
    /// - Requirement 1.2: Create metadata with ETag and Content-Type from S3 response (Last-Modified learned on first cache-miss GET or first HEAD after PUT)
    /// - Requirement 1.3: Set write cache TTL (default: 1 day)
    /// - Requirement 1.5: Return S3 response unchanged to client (handled by caller)
    /// - Requirement 9.1: Don't cache on S3 failure
    #[allow(clippy::too_many_arguments)]
    fn spawn_cache_write_task(
        cache_key: String,
        body_data: Bytes,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        _cache_dir: PathBuf,
        _compression_handler: CompressionHandler,
        _content_length: Option<u64>,
        request_headers: HashMap<String, String>,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        cache_manager: Option<Arc<crate::cache::CacheManager>>,
        s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
        pending_permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        tokio::spawn(async move {
            // Released when this task ends, after its last step.
            let _pending_permit = pending_permit;
            // Track streaming start time
            let start_time = std::time::Instant::now();
            let body_len = body_data.len() as u64;

            // Wait for S3 result first (Requirement 9.1: Don't cache on S3 failure)
            match s3_result_rx.await {
                Ok(Ok(response)) => {
                    let status = response.status();

                    if status.is_success() && !response_carries_etag(response.headers()) {
                        debug!(
                            "PUT response carried no ETag, not caching: cache_key={}",
                            cache_key
                        );
                        if let Some(m) = &metrics {
                            m.read().await.record_skipped_put(NO_ETAG_SKIP_REASON).await;
                        }
                        return;
                    }
                    if status.is_success() {
                        // S3 success - store as single range with write cache metadata
                        // (Requirements 1.1, 1.2, 1.3)

                        // Extract metadata from S3 response headers
                        let response_headers: HashMap<String, String> = response
                            .headers()
                            .iter()
                            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                            .collect();

                        // Use S3 client's comprehensive header extraction if available
                        let (etag, last_modified, mut comprehensive_headers) =
                            if let Some(s3_client) = &s3_client {
                                let object_metadata = s3_client
                                    .extract_object_metadata_from_response(&response_headers);
                                (
                                    object_metadata.etag,
                                    object_metadata.last_modified,
                                    object_metadata.response_headers,
                                )
                            } else {
                                // Fallback to manual extraction
                                let etag = response_headers
                                    .get("etag")
                                    .or_else(|| response_headers.get("ETag"))
                                    .cloned()
                                    .unwrap_or_default();

                                // S3 PUT responses don't include Last-Modified - leave empty
                                let last_modified = response_headers
                                    .get("last-modified")
                                    .or_else(|| response_headers.get("Last-Modified"))
                                    .cloned()
                                    .unwrap_or_default();

                                (etag, last_modified, response_headers.clone())
                            };

                        // Merge checksum headers from request if not present in response
                        // Always prefer response headers, but include request checksums as fallback
                        for (key, value) in &request_headers {
                            let key_lower = key.to_lowercase();
                            if (key_lower.starts_with("x-amz-checksum-")
                                || key_lower == "content-md5")
                                && !comprehensive_headers.contains_key(key)
                            {
                                debug!("Adding checksum header from PUT request: {}", key);
                                comprehensive_headers.insert(key.clone(), value.clone());
                            }
                        }

                        merge_representation_headers(&request_headers, &mut comprehensive_headers);

                        // Get Content-Type from request headers (S3 echoes what was sent)
                        let content_type = request_headers
                            .get("content-type")
                            .or_else(|| request_headers.get("Content-Type"))
                            .cloned();

                        // Store directly as range using CacheManager
                        // (Requirements 3.1, 3.2, 3.3 - unified storage only)
                        if let Some(cache_mgr) = &cache_manager {
                            // Test-only: the same hold the streamed store passes.
                            #[cfg(test)]
                            pre_store_pause::hit(&cache_key).await;

                            // The write-order claim the response path stamped.
                            let token = match response.write_guard() {
                                WriteGuard::Unguarded => None,
                                WriteGuard::Token(token) => Some(token.clone()),
                                WriteGuard::Unavailable => {
                                    if let Some(m) = &metrics {
                                        m.read().await.record_skipped_put("commit_failed").await;
                                    }
                                    return;
                                }
                            };

                            // Invalidate existing cache entries first. A guarded store's response
                            // path already did, and stamped its token: re-stamping here would
                            // supersede itself, and a replaced token means the entry now belongs
                            // to a newer write (checked under the removal's slot-lock hold,).
                            let invalidated = match &token {
                                None => {
                                    cache_mgr
                                        .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                                        .await
                                }
                                Some(t) => {
                                    cache_mgr.invalidate_if_write_current(&cache_key, t).await
                                }
                            };
                            if let Err(e) = invalidated {
                                warn!(
                                    "Failed to invalidate cache before PUT caching: cache_key={}, error={}",
                                    cache_key, e
                                );
                            }

                            // Store as single range with write cache metadata
                            // (Requirements 1.1, 1.2, 1.3)
                            let stored = match token {
                                None => cache_mgr
                                    .store_put_as_write_cached_range(
                                        &cache_key,
                                        &body_data,
                                        etag.clone(),
                                        last_modified.clone(),
                                        content_type.clone(),
                                        comprehensive_headers.clone(),
                                    )
                                    .await
                                    .map(|()| crate::cache::PublishOutcome::Published),
                                Some(token) => {
                                    cache_mgr
                                        .store_put_as_write_cached_range_guarded(
                                            &cache_key,
                                            &body_data,
                                            etag.clone(),
                                            last_modified.clone(),
                                            content_type.clone(),
                                            comprehensive_headers.clone(),
                                            token,
                                        )
                                        .await
                                }
                            };
                            match stored {
                                Ok(crate::cache::PublishOutcome::Superseded) => {
                                    debug!(
                                        "PUT store superseded by a newer write of the key: cache_key={}",
                                        cache_key
                                    );
                                    if let Some(m) = &metrics {
                                        m.read()
                                            .await
                                            .record_skipped_put(SUPERSEDED_SKIP_REASON)
                                            .await;
                                    }
                                }
                                Ok(_) => {
                                    let streaming_duration_ms =
                                        start_time.elapsed().as_millis() as u64;
                                    // Get the effective PUT TTL for logging - Requirement 11.1
                                    let effective_ttl =
                                        cache_mgr.get_effective_put_ttl(&cache_key).await;

                                    info!(
                                        "Successfully stored PUT as write-cached range: cache_key={}, size={} bytes, etag={}, ttl={:?}",
                                        cache_key, body_len, etag, effective_ttl
                                    );

                                    // Record successful cache
                                    if let Some(m) = &metrics {
                                        m.read()
                                            .await
                                            .record_cached_put(body_len, streaming_duration_ms)
                                            .await;
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to store PUT as write-cached range: cache_key={}, error={}",
                                        cache_key, e
                                    );
                                    // Record cache failure
                                    if let Some(m) = &metrics {
                                        m.read().await.record_put_cache_failure().await;
                                    }
                                }
                            }
                        } else {
                            // No cache_manager available - cannot cache without unified storage
                            warn!(
                                "Cannot cache PUT: no cache_manager available for unified storage: cache_key={}",
                                cache_key
                            );
                            if let Some(m) = &metrics {
                                m.read().await.record_put_cache_failure().await;
                            }
                        }
                    } else {
                        // S3 returned error - don't cache (Requirement 9.1)
                        debug!(
                            "S3 error response, not caching PUT: cache_key={}, status={}",
                            cache_key, status
                        );
                    }
                }
                Ok(Err(e)) => {
                    // S3 failure - don't cache (Requirement 9.1)
                    debug!(
                        "S3 error, not caching PUT: cache_key={}, error={}",
                        cache_key, e
                    );
                }
                Err(e) => {
                    // Channel closed unexpectedly - don't cache
                    // Expected on a client disconnect; counted by the metric
                    // below rather than shouted about in the log (Req 6.7).
                    debug!(
                        "S3 result channel closed unexpectedly: cache_key={}, error={:?}",
                        cache_key, e
                    );
                    if let Some(m) = &metrics {
                        m.read().await.record_put_cache_failure().await;
                    }
                }
            }
        });
    }

    /// Spawn the streaming write-cache task that consumes the bounded tee channel
    /// fed by `forward_signed_request_streaming` and writes the cached object
    /// incrementally through a [`crate::cache::WriteCacheRangeSink`]
    /// (Component 4 of the streaming-write-path design).
    ///
    /// This is the streaming analog of [`Self::spawn_cache_write_task`]: instead
    /// of receiving a fully-buffered decoded body, it drains `tee_rx`
    /// frame-by-frame as the body streams to the upstream, decoding aws-chunked
    /// incrementally on the cache branch only (the upstream always receives the
    /// original bytes verbatim — Req 4). Per-request cache memory stays bounded by
    /// one in-flight frame plus the channel capacity (Req 1, 2).
    ///
    /// Cache-failure isolation (Req 7): every skip/error path discards the sink
    /// and closes the tee receiver, so the forward loop drops the tee and keeps
    /// streaming verbatim — a cache problem never fails an upload S3 would accept.
    ///
    /// The fire-and-forget return value is intentionally ignored; tests call
    /// [`Self::run_streaming_cache_write`] directly to observe the
    /// [`StreamingCacheOutcome`].
    //
    // Wired into the single-part PUT path via `SignedPutHandler::setup_put_cache_tee`
    // (task 5.1); `handle_upload_part` adopts it in task 6.1.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_streaming_cache_write_task(
        cache_key: String,
        sink: crate::cache::WriteCacheRangeSink,
        tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        ttl: std::time::Duration,
        request_headers: HashMap<String, String>,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        cache_manager: Option<Arc<crate::cache::CacheManager>>,
        s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
        pending_permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        tokio::spawn(async move {
            // Released when the store's last step (`record_cached_put`, a failure path,
            // or `drop(sink)`) has run, i.e. when this task ends.
            let _pending_permit = pending_permit;
            let _ = Self::run_streaming_cache_write(
                cache_key,
                sink,
                tee_rx,
                s3_result_rx,
                is_aws_chunked,
                expected_decoded_len,
                ttl,
                request_headers,
                metrics,
                cache_manager,
                s3_client,
            )
            .await;
        });
    }

    /// Drive the streaming write-cache pipeline to completion (the receiver side
    /// of the streaming forward's bounded tee channel).
    ///
    /// Phases:
    /// 1. Drain `tee_rx` as frames arrive. Non-chunked frames are object bytes and
    ///    are written straight to the sink; aws-chunked frames are `push`-ed
    ///    through an [`aws_chunked_decoder::IncrementalAwsChunkedDecoder`] and the
    ///    decoded bytes are written to the sink. A decode or sink-write error skips
    ///    caching (discard) without failing the upload (Req 3.4, 7.1, 7.2).
    /// 2. Once the channel closes (the forward loop finished streaming the body),
    ///    await the S3 result via the existing oneshot result channel.
    /// 3. On S3 success, finish the decoder (chunked case) and validate the decoded
    ///    length against `x-amz-decoded-content-length` when present; on mismatch,
    ///    discard the sink and skip caching (Req 3.4).
    /// 4. Build the write-cache [`crate::cache_types::ObjectMetadata`] from the S3
    ///    response (etag / last-modified / content-type / checksum headers) and
    ///    `commit` the sink with the resolved TTL.
    ///
    /// On any S3 failure / non-success / skip, the sink is discarded (its `.tmp`
    /// is cleaned up) and nothing else happens — the upload already streamed
    /// verbatim and its response is returned by the forward loop, untouched.
    //
    // Wired into the single-part PUT path (task 5.1); also exercised directly by
    // the streaming write-cache task tests below.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_streaming_cache_write(
        cache_key: String,
        sink: crate::cache::WriteCacheRangeSink,
        mut tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        ttl: std::time::Duration,
        request_headers: HashMap<String, String>,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        cache_manager: Option<Arc<crate::cache::CacheManager>>,
        s3_client: Option<Arc<dyn S3ClientApi + Send + Sync>>,
    ) -> StreamingCacheOutcome {
        let start_time = std::time::Instant::now();

        // The cache branch decodes aws-chunked incrementally; the upstream leg
        // (the forward loop) always receives the original bytes verbatim.
        let decoder = if is_aws_chunked {
            Some(aws_chunked_decoder::IncrementalAwsChunkedDecoder::new())
        } else {
            None
        };
        // ---- Phase 1: drain the bounded tee channel, decode, write to sink ----
        //
        // `sink.write` performs BLOCKING std::fs I/O (LZ4 compress + `File::write_all`
        // + flush). Running it inline on the async worker pins a Tokio worker thread
        // for the entire upload; on a 2-worker runtime (the default on a 2-vCPU host)
        // two concurrent large PUTs starve the whole runtime — including the /health
        // task and the shutdown handler. We therefore drain the channel and do the
        // blocking writes on a dedicated blocking thread via `blocking_recv`, the same
        // way the rest of the proxy offloads blocking cache I/O. The async worker is
        // free to poll other tasks while this thread blocks on EFS writeback.
        // One-shot return from the drain task (once per upload, not a per-frame hot
        // path), so the size gap between `Continue` and `Skip` is immaterial; boxing
        // the sink would only add a needless allocation.
        #[allow(clippy::large_enum_variant)]
        enum DrainOutcome {
            Continue {
                sink: crate::cache::WriteCacheRangeSink,
                decoder: Option<aws_chunked_decoder::IncrementalAwsChunkedDecoder>,
                decoded_written: u64,
            },
            Skip {
                reason: &'static str,
                record_failure: bool,
            },
        }

        let drain_key = cache_key.clone();
        let drain = tokio::task::spawn_blocking(move || {
            let mut sink = sink;
            let mut decoder = decoder;
            let mut decoded_written: u64 = 0;
            while let Some(frame) = tee_rx.blocking_recv() {
                let write_result = match decoder.as_mut() {
                    Some(dec) => match dec.push(frame.as_ref()) {
                        Ok(decoded) => {
                            let n = decoded.len() as u64;
                            sink.write(&decoded).map(|()| n)
                        }
                        Err(e) => {
                            // aws-chunked decode error → skip caching, keep forwarding
                            // (Req 3.4, 7.2). Not a recorded failure: mirrors the
                            // buffered handler, which logs and bypasses on decode error.
                            warn!(
                                "Streaming cache: aws-chunked decode error, skipping cache (upload unaffected): cache_key={}, error={}",
                                drain_key, e
                            );
                            sink.discard();
                            // Close the receiver so the forward loop drops the tee and
                            // keeps streaming verbatim (no deadlock on a full channel).
                            tee_rx.close();
                            return DrainOutcome::Skip {
                                reason: "decode_error",
                                record_failure: false,
                            };
                        }
                    },
                    None => {
                        let n = frame.len() as u64;
                        sink.write(frame.as_ref()).map(|()| n)
                    }
                };

                match write_result {
                    Ok(n) => decoded_written += n,
                    Err(e) => {
                        // Cache write error (disk full, etc.) → skip caching, keep
                        // forwarding (Req 7.1).
                        warn!(
                            "Streaming cache: sink write error, skipping cache (upload unaffected): cache_key={}, error={}",
                            drain_key, e
                        );
                        sink.discard();
                        tee_rx.close();
                        return DrainOutcome::Skip {
                            reason: "cache_write_error",
                            record_failure: true,
                        };
                    }
                }
            }
            DrainOutcome::Continue {
                sink,
                decoder,
                decoded_written,
            }
        })
        .await
        .unwrap_or_else(|join_err| {
            error!(
                "Streaming cache: drain task panicked, skipping cache (upload unaffected): cache_key={}, error={}",
                cache_key, join_err
            );
            DrainOutcome::Skip {
                reason: "drain_task_panic",
                record_failure: true,
            }
        });

        let (sink, decoder, decoded_written) = match drain {
            DrainOutcome::Continue {
                sink,
                decoder,
                decoded_written,
            } => (sink, decoder, decoded_written),
            DrainOutcome::Skip {
                reason,
                record_failure,
            } => {
                if record_failure {
                    if let Some(m) = &metrics {
                        m.read().await.record_put_cache_failure().await;
                    }
                }
                return StreamingCacheOutcome::Skipped(reason);
            }
        };

        // ---- Phase 2: channel closed (body fully streamed). Await S3 result ----
        let response = match s3_result_rx.await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                // S3 failure → don't cache (Req 7.1). The upload already returned
                // the S3 error to the client via the forward loop.
                debug!(
                    "Streaming cache: S3 error, not caching: cache_key={}, error={}",
                    cache_key, e
                );
                sink.discard();
                return StreamingCacheOutcome::Skipped("s3_error");
            }
            Err(e) => {
                // The forward loop dropped the result sender without sending — treat
                // as a cache failure but never touch the upload.
                // Expected on a client disconnect; counted by the metric below.
                debug!(
                    "Streaming cache: S3 result channel closed unexpectedly: cache_key={}, error={:?}",
                    cache_key, e
                );
                sink.discard();
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                }
                return StreamingCacheOutcome::Skipped("s3_channel_closed");
            }
        };

        if !response.status().is_success() {
            // S3 returned an error status → don't cache (Req 7.1).
            debug!(
                "Streaming cache: S3 non-success status, not caching: cache_key={}, status={}",
                cache_key,
                response.status()
            );
            sink.discard();
            return StreamingCacheOutcome::Skipped("s3_non_success");
        }
        if !response_carries_etag(response.headers()) {
            debug!(
                "Streaming cache: S3 response carried no ETag, not caching: cache_key={}",
                cache_key
            );
            sink.discard();
            if let Some(m) = &metrics {
                m.read().await.record_skipped_put(NO_ETAG_SKIP_REASON).await;
            }
            return StreamingCacheOutcome::Skipped(NO_ETAG_SKIP_REASON);
        }

        // ---- Phase 3 (chunked only): finish + decoded-length validation ----
        if let Some(dec) = decoder {
            match dec.finish() {
                Ok(trailers) => {
                    if let Some(expected) = expected_decoded_len {
                        if trailers.decoded_len != expected {
                            // Decoded length disagrees with x-amz-decoded-content-length:
                            // skip caching, do NOT reject the request, and let the
                            // original bytes (already forwarded verbatim) stand — S3
                            // remains the content-length authority (Req 3.4).
                            warn!(
                                "Streaming cache: decoded-length mismatch, skipping cache (upload unaffected): cache_key={}, expected={}, actual={}",
                                cache_key, expected, trailers.decoded_len
                            );
                            sink.discard();
                            return StreamingCacheOutcome::Skipped("decoded_length_mismatch");
                        }
                    }
                }
                Err(e) => {
                    // Body framing was incomplete at end-of-stream → skip caching.
                    warn!(
                        "Streaming cache: aws-chunked framing incomplete at finish, skipping cache (upload unaffected): cache_key={}, error={}",
                        cache_key, e
                    );
                    sink.discard();
                    return StreamingCacheOutcome::Skipped("decode_finish_error");
                }
            }
        }

        // ---- Phase 4: finalize the range bytes, then write `.meta` immediately ----
        //
        // Read-after-write parity — an immediate GET after a PUT must hit: the buffered
        // write-cache path (`store_put_as_write_cached_range_with_ttl`) finalizes the
        // `.bin` and then writes the `.meta` synchronously via `store_new_metadata`,
        // so an immediate post-PUT GET is a cache hit. `WriteCacheRangeSink::commit`
        // is journal-only — it defers the `.meta` until consolidation, which would
        // make that GET a miss. We therefore mirror the buffered path here:
        // `sink.finalize()` (publish `.bin`) → build metadata → store `.meta` now via
        // `CacheManager::store_streamed_write_cache_metadata`.
        let object_metadata = Self::build_streaming_write_cache_metadata(
            decoded_written,
            &response,
            &request_headers,
            &s3_client,
            ttl,
        );

        // Test-only: hold this store after S3 answered and before it touches the cache, so a
        // test can order two acknowledged PUTs' stores of one key.
        #[cfg(test)]
        pre_store_pause::hit(&cache_key).await;

        // The write-order claim the response path stamped.
        let token = match response.write_guard() {
            WriteGuard::Unguarded => None,
            WriteGuard::Token(token) => Some(token.clone()),
            WriteGuard::Unavailable => {
                debug!(
                    "Streaming cache: pre-response invalidation failed, so this store cannot be \
                     ordered against other writes of the key; not caching: cache_key={}",
                    cache_key
                );
                sink.discard();
                if let Some(m) = &metrics {
                    m.read().await.record_skipped_put("commit_failed").await;
                }
                return StreamingCacheOutcome::Skipped("commit_error");
            }
        };

        // Invalidate any existing cache entry for this key BEFORE publishing the new
        // `.bin`, so invalidation cannot delete the range file we are about to
        // finalize. Mirrors the buffered write-cache path's PUT invalidation so a
        // re-PUT replaces stale ranges/metadata.
        //
        // A guarded store's response path already invalidated and stamped its token, so this
        // re-invalidation must not stamp (that would supersede itself), and it removes nothing
        // once the token is replaced: the entry then belongs to a newer write. That check runs
        // under the removal's own slot-lock hold; the publish re-checks under its own.
        if let Some(cache_mgr) = &cache_manager {
            let invalidated = match &token {
                None => {
                    cache_mgr
                        .invalidate_cache_unified_for_operation(&cache_key, "PUT")
                        .await
                }
                Some(t) => cache_mgr.invalidate_if_write_current(&cache_key, t).await,
            };
            if let Err(e) = invalidated {
                warn!(
                    "Streaming cache: failed to invalidate before commit: cache_key={}, error={}",
                    cache_key, e
                );
            }
        }

        // Finalize the bytes (flush residual batch, validate length, publish the
        // `.bin`). This is blocking std::fs (flush + rename), so run it on a blocking
        // thread rather than the async worker. The sink — and its capacity
        // reservation — is moved in and returned so it stays alive until the explicit
        // `drop(sink)` below, after the `.meta` write, matching the buffered path's
        // reservation lifetime.
        // The rename to the final `.bin` is NOT done here: `store_streamed_write_cache_metadata`
        // runs it inside the slot-lock hold that publishes the `.meta`.
        let (sink, finalize_res) = match tokio::task::spawn_blocking(move || {
            let mut sink = sink;
            let res = sink.finalize_unpublished();
            (sink, res)
        })
        .await
        {
            Ok(pair) => pair,
            Err(join_err) => {
                error!(
                    "Streaming cache: finalize task panicked, object not cached (upload unaffected): cache_key={}, error={}",
                    cache_key, join_err
                );
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                    m.read().await.record_skipped_put("commit_failed").await;
                }
                return StreamingCacheOutcome::Skipped("commit_error");
            }
        };
        let (range_spec, pending_publish) = match finalize_res {
            Ok(pair) => pair,
            Err(e) => {
                // Finalize failed (disk error, length mismatch, etc.). The writer was
                // consumed and its `.tmp` cleaned up; the upload is unaffected
                // (Req 7.1).
                error!(
                    "Streaming cache: finalize failed, object not cached (upload unaffected): cache_key={}, error={}",
                    cache_key, e
                );
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                    m.read().await.record_skipped_put("commit_failed").await;
                }
                return StreamingCacheOutcome::Skipped("commit_error");
            }
        };

        let outcome = match &cache_manager {
            Some(cache_mgr) => {
                match cache_mgr
                    .store_streamed_write_cache_metadata(
                        &cache_key,
                        range_spec,
                        object_metadata,
                        ttl,
                        pending_publish,
                        token,
                    )
                    .await
                {
                    Ok(crate::cache::PublishOutcome::Superseded) => {
                        debug!(
                            "Streaming cache: store superseded by a newer write of the key, not \
                             published: cache_key={}",
                            cache_key
                        );
                        if let Some(m) = &metrics {
                            m.read()
                                .await
                                .record_skipped_put(SUPERSEDED_SKIP_REASON)
                                .await;
                        }
                        StreamingCacheOutcome::Skipped(SUPERSEDED_SKIP_REASON)
                    }
                    Ok(_) => {
                        let duration_ms = start_time.elapsed().as_millis() as u64;
                        info!(
                            "Streaming cache: committed write-cached range (.meta written immediately): cache_key={}, size={} bytes, ttl={:?}",
                            cache_key, decoded_written, ttl
                        );
                        if let Some(m) = &metrics {
                            m.read()
                                .await
                                .record_cached_put(decoded_written, duration_ms)
                                .await;
                        }
                        StreamingCacheOutcome::Committed
                    }
                    Err(e) => {
                        // Metadata write failed; the `.bin` is published but
                        // unreferenced (a future consolidation/GC reclaims it). The
                        // upload is unaffected (Req 7.1).
                        error!(
                            "Streaming cache: metadata store failed, object not cached (upload unaffected): cache_key={}, error={}",
                            cache_key, e
                        );
                        if let Some(m) = &metrics {
                            m.read().await.record_put_cache_failure().await;
                            m.read().await.record_skipped_put("commit_failed").await;
                        }
                        StreamingCacheOutcome::Skipped("commit_error")
                    }
                }
            }
            None => {
                // No cache manager wired (unit tests exercise the sink without a
                // manager): the `.bin` is published, but the `.meta` cannot be
                // written without a manager, so there is no slot-lock hold to publish
                // inside. The range bytes are committed.
                match pending_publish.publish() {
                    Ok(_) => StreamingCacheOutcome::Committed,
                    Err(e) => {
                        error!(
                            "Streaming cache: range publish failed, object not cached (upload unaffected): cache_key={}, error={}",
                            cache_key, e
                        );
                        StreamingCacheOutcome::Skipped("commit_error")
                    }
                }
            }
        };

        // Drop the sink now — after the `.meta` write — to release the capacity
        // reservation, matching the buffered path's reservation lifetime.
        drop(sink);
        outcome
    }

    /// Build the write-cache [`crate::cache_types::ObjectMetadata`] for a streamed
    /// PUT from the S3 response and request headers, identically to the buffered
    /// cache task: ETag and Last-Modified come from the S3 response (via the S3
    /// client's comprehensive header extraction when available), Content-Type from
    /// the request (S3 echoes what was sent), and request checksum headers are
    /// merged in as a fallback. The write-cache tracking fields are stamped with
    /// `now` + the resolved TTL.
    ///
    /// `compressed_size`/`compression_algorithm` are left at their defaults here:
    /// the true per-range compressed size and algorithm are recorded on the
    /// `RangeSpec` that `commit_incremental_range` derives from the sink and writes
    /// to the journal; the object-level fields are not used for size accounting.
    fn build_streaming_write_cache_metadata(
        content_length: u64,
        response: &ResponseInfo,
        request_headers: &HashMap<String, String>,
        s3_client: &Option<Arc<dyn S3ClientApi + Send + Sync>>,
        ttl: std::time::Duration,
    ) -> crate::cache_types::ObjectMetadata {
        let response_headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let (etag, last_modified, mut comprehensive_headers) = if let Some(s3_client) = s3_client {
            let object_metadata =
                s3_client.extract_object_metadata_from_response(&response_headers);
            (
                object_metadata.etag,
                object_metadata.last_modified,
                object_metadata.response_headers,
            )
        } else {
            let etag = response_headers
                .get("etag")
                .or_else(|| response_headers.get("ETag"))
                .cloned()
                .unwrap_or_default();
            // S3 PUT responses don't include Last-Modified - leave empty.
            let last_modified = response_headers
                .get("last-modified")
                .or_else(|| response_headers.get("Last-Modified"))
                .cloned()
                .unwrap_or_default();
            (etag, last_modified, response_headers.clone())
        };

        // Merge checksum headers from the request if not present in the response.
        for (key, value) in request_headers {
            let key_lower = key.to_lowercase();
            if (key_lower.starts_with("x-amz-checksum-") || key_lower == "content-md5")
                && !comprehensive_headers.contains_key(key)
            {
                comprehensive_headers.insert(key.clone(), value.clone());
            }
        }

        merge_representation_headers(request_headers, &mut comprehensive_headers);

        let content_type = request_headers
            .get("content-type")
            .or_else(|| request_headers.get("Content-Type"))
            .cloned();

        let now = std::time::SystemTime::now();
        crate::cache_types::ObjectMetadata {
            etag,
            last_modified,
            content_length,
            content_type,
            response_headers: comprehensive_headers,
            upload_state: crate::cache_types::UploadState::Complete,
            cumulative_size: content_length,
            is_write_cached: true,
            write_cache_expires_at: Some(now + ttl),
            write_cache_created_at: Some(now),
            write_cache_last_accessed: Some(now),
            graduation_accounted: false,
            ..Default::default()
        }
    }

    /// Set up the streaming part-cache tee for an `UploadPart`, when caching is
    /// viable. Returns `(tee_sender, s3_result_sender)`:
    ///
    /// - `tee_sender: Some` when a part-staging sink was opened and a background
    ///   [`Self::run_streaming_part_cache_write`] task spawned to consume it; pass
    ///   it to [`forward_signed_request_streaming`].
    /// - `s3_result_sender: Some` whenever that task is waiting for the S3 result;
    ///   the caller sends the `ResponseInfo`/error into it after the forward returns.
    ///
    /// Both `None` means no caching for this part — the body still streams to the
    /// upstream verbatim (Req 7.2). Caching is skipped (no tee) when there is no
    /// cache manager or the part sink cannot be opened.
    ///
    /// Unlike the single-part PUT sink, a part is not pre-sized and not
    /// write-cache-capacity-reserved: `CacheManager::open_multipart_part_sink`
    /// applies none of the three gates `open_write_cache_sink` applies — no
    /// `write_cache_max_object_size` check via `WriteCacheManager::try_reserve`, no
    /// sizing reservation, and no `disk_safety_refusal`. What the handler's
    /// `should_cache` decision contributes is narrower than that absence suggests:
    /// it is `check_cache_capacity`'s arithmetic on `content_length`,
    /// `current_cache_usage` and `max_cache_capacity` only — no free-space check, no
    /// per-object cap, no part count, and staged parts under `mpus_in_progress/` do
    /// not appear in the `current_cache_usage` snapshot, so concurrent parts each
    /// read the same pre-upload total. When the Content-Length was absent or
    /// unparseable it contributes no size gate at all, which is why
    /// `capacity_budget` is `Some` in that case and the drain loop bounds the part
    /// as it stages (R16.8).
    #[cfg(test)]
    async fn setup_upload_part_cache_tee(
        &self,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        is_aws_chunked: bool,
        decoded_len: Option<u64>,
        capacity_budget: Option<StreamingCapacityBudget>,
    ) -> (
        Option<tokio::sync::mpsc::Sender<Bytes>>,
        Option<tokio::sync::oneshot::Sender<Result<ResponseInfo>>>,
    ) {
        let (tee, result, _setup_done) = self
            .setup_upload_part_cache_tee_tracked(
                cache_key,
                upload_id,
                part_number,
                is_aws_chunked,
                decoded_len,
                capacity_budget,
            )
            .await;
        (tee, result)
    }

    /// [`Self::setup_upload_part_cache_tee`], also returning a receiver that resolves when
    /// the background setup has finished, whether it opened the sink or declined.
    ///
    /// `handle_upload_part` waits on it, for at most [`WRITE_CACHE_SETUP_WAIT_BOUND`],
    /// before answering the client. The part's upload directory is created by that setup,
    /// and `CompleteMultipartUpload` declines to cache an upload whose directory does not
    /// exist, so answering a small part before its setup finished would let a client's
    /// immediate Complete find no directory and leave the object uncached.
    async fn setup_upload_part_cache_tee_tracked(
        &self,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        is_aws_chunked: bool,
        decoded_len: Option<u64>,
        capacity_budget: Option<StreamingCapacityBudget>,
    ) -> (
        Option<tokio::sync::mpsc::Sender<Bytes>>,
        Option<tokio::sync::oneshot::Sender<Result<ResponseInfo>>>,
        Option<tokio::sync::oneshot::Receiver<()>>,
    ) {
        let cache_manager = match &self.cache_manager {
            Some(cm) => cm.clone(),
            None => return (None, None, None),
        };
        // The same backlog bound as a single PUT, taken before the setup task that opens the
        // part's `.tmp`. A part forwarded uncached here leaves its upload uncached, as any
        // other part skip does.
        let Some(permit) = self.admit_pending_cache_write(cache_key).await else {
            return (None, None, None);
        };

        // Open the part-staging sink. A failed open simply skips caching this part —
        // the body still streams verbatim to the upstream (Req 7.2).
        //
        // `decoded_len` is the part's decoded `Content-Length` when the client declared one,
        // and is passed as the danger-band guard's size hint (cache-eviction-at-scale
        // task 36, P3c/P8). It is exactly the case `check_streaming_capacity` skips — that
        // check fires only when Content-Length is *absent* — so the two are complementary.
        //
        // The open is every shared-storage await before the body read. As for a PUT (
        //The tee is handed to the forward at once and the open runs on its own task
        // under `WRITE_CACHE_SETUP_BOUND`. Frames the forward sends meanwhile wait in
        // the tee's buffer; once it is full the forward waits at most
        // `WRITE_CACHE_SETUP_WAIT_BOUND` for setup, then the part forwards uncached. Any
        // setup decline drops the cache side, so the forward continues uncached.
        let setup_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let BoundedCacheTee {
            forward_tx: tee_tx,
            cache_rx: tee_rx,
            s3_result_tx,
            s3_result_rx,
        } = bounded_cache_tee_gated(
            self.write_cache_tee_channel_depth,
            WRITE_CACHE_TEE_STALL_BOUND,
            cache_key,
            Some(setup_ready.clone()),
            self.cache_delay_budget.clone(),
        );
        let task_key = cache_key.to_string();
        let task_upload = upload_id.to_string();
        let cache_dir = self.cache_dir.clone();
        let metrics_manager = self.metrics_manager.clone();
        let (setup_done_tx, setup_done_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            // Dropped when this task ends, on every path, which resolves `setup_done_rx`.
            let _setup_done = setup_done_tx;
            let setup_manager = cache_manager.clone();
            let setup_key = task_key.clone();
            let setup_upload = task_upload.clone();
            // As for a PUT: the permit lives inside the setup future, so an abandoned setup
            // holds it until it finishes, and on success it moves into the store task.
            // Every early return below releases it.
            let budget_dir = cache_dir.clone();
            let opened = bounded_cache_setup(WRITE_CACHE_SETUP_BOUND, &task_key, async move {
                let permit = permit;
                // Admitted before the sink opens, so a refused part creates nothing under
                // `mpus_in_progress/` (MAX_TRACKED_MULTIPART_ENTRIES).
                // A part whose upload directory does not exist yet creates it as well as its
                // record, so it is charged both entries.
                let directory_exists =
                    tokio::fs::try_exists(mpu_upload_dir(&budget_dir, &setup_upload))
                        .await
                        .unwrap_or(false);
                let charged = if directory_exists { 1 } else { 2 };
                let tracking_slot =
                    match tracked_multipart_budget::admit(&budget_dir, charged).await {
                        Ok(slot) => slot,
                        Err(refusal) => return (Err(refusal), permit),
                    };
                let opened = setup_manager
                    .open_multipart_part_sink(&setup_key, &setup_upload, part_number, decoded_len)
                    .await;
                (Ok((opened, tracking_slot)), permit)
            })
            .await;
            // Every early return below drops `tee_rx` and `s3_result_rx`: the relay sees a
            // closed cache side and the forward continues uncached (Req 7.2).
            let Some((opened, permit)) = opened else {
                return;
            };
            let (opened, tracking_slot) = match opened {
                Ok(admitted) => admitted,
                Err(refusal) => {
                    note_tracked_multipart_refusal(
                        &metrics_manager,
                        &cache_dir,
                        "UploadPart",
                        refusal,
                        &task_key,
                        &task_upload,
                    )
                    .await;
                    drop(permit);
                    // Full: the upload stops being write-cached. Its directory, recorded parts
                    // included, is removed so the walk no longer pays for it, and its Complete
                    // then finds no tracker and forwards uncached. A part of the same upload
                    // already past admission may recreate the directory; Complete, Abort or the
                    // sweep removes it.
                    //
                    // Unknown: the count could not be read, which says nothing about the volume
                    // being full, so the parts already staged stay. This part is not staged, so
                    // the upload's Complete finds it missing and declines to build an entry.
                    if refusal == tracked_multipart_budget::Refusal::Full {
                        let upload_dir = mpu_upload_dir(&cache_dir, &task_upload);
                        tokio::spawn(async move {
                            let _ = tokio::fs::remove_dir_all(&upload_dir).await;
                        });
                    }
                    return;
                }
            };
            let sink = match opened {
                Ok(Some(sink)) => sink,
                // Declined by the danger-band publication guard. The part still uploads to
                // S3; it is simply not staged locally, so `CompleteMultipartUpload` will find
                // no cached part for it and decline to build a cache entry — the same outcome
                // as `write_cache_enabled: false` for this key.
                Ok(None) => {
                    debug!(
                        "Streaming UploadPart: part staging declined by the capacity guard, \
                         skipping cache (upload unaffected): cache_key={}, upload_id={}, \
                         part_number={}",
                        task_key, task_upload, part_number
                    );
                    return;
                }
                Err(e) => {
                    warn!(
                        "Streaming UploadPart: failed to open part sink, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                        task_key, task_upload, part_number, e
                    );
                    return;
                }
            };
            Self::spawn_streaming_part_cache_write_task(
                task_key,
                task_upload,
                part_number,
                sink,
                tee_rx,
                s3_result_rx,
                is_aws_chunked,
                decoded_len,
                cache_dir,
                metrics_manager,
                capacity_budget,
                permit,
                tracking_slot,
            );
            setup_ready.store(true, std::sync::atomic::Ordering::Release);
        });

        (Some(tee_tx), Some(s3_result_tx), Some(setup_done_rx))
    }

    /// Spawn the background streaming part-cache task. Fire-and-forget: a cache
    /// problem never fails an upload the upstream would accept (Req 7). Tests call
    /// [`Self::run_streaming_part_cache_write`] directly to observe the outcome.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_streaming_part_cache_write_task(
        cache_key: String,
        upload_id: String,
        part_number: u32,
        sink: crate::cache::MultipartPartSink,
        tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        cache_dir: PathBuf,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        capacity_budget: Option<StreamingCapacityBudget>,
        pending_permit: tokio::sync::OwnedSemaphorePermit,
        tracking_slot: tracked_multipart_budget::Slot,
    ) {
        tokio::spawn(async move {
            // Counted against MAX_TRACKED_MULTIPART_ENTRIES until this task ends.
            let _tracking_slot = tracking_slot;
            // Released when the part store's last step has run: when this task ends, or,
            // if the part finalize outlives its timeout, when that blocking step ends.
            let _ = Self::run_streaming_part_cache_write(
                cache_key,
                upload_id,
                part_number,
                sink,
                tee_rx,
                s3_result_rx,
                is_aws_chunked,
                expected_decoded_len,
                cache_dir,
                metrics,
                capacity_budget,
                Some(pending_permit),
            )
            .await;
        });
    }

    /// Drive the streaming part-cache pipeline (the receiver side of the
    /// `UploadPart` forward's bounded tee channel). Mirrors
    /// [`Self::run_streaming_cache_write`] but stages the bytes as a multipart part
    /// and, on S3 success, finalizes the part and writes its `part{N}.json` record
    /// under `part{N}.lock` (the per-part correctness gate) instead of committing
    /// object metadata. On any S3 failure / non-success / skip, the staged part is
    /// discarded — the upload already streamed verbatim and its response is returned
    /// by the forward loop, untouched (Req 7).
    ///
    /// `capacity_budget` is `Some` only when the request carried no usable
    /// Content-Length, so no size gate was applied before staging began (R16.8). In
    /// that case every frame written to the sink is checked with
    /// [`check_streaming_capacity`] against the bytes staged so far, and the part is
    /// discarded with reason `"capacity_exceeded"` the moment the budget is
    /// breached. The upload itself is unaffected either way (Req 7.1).
    ///
    /// `pending_permit` is the pending-store permit. It is held until this function
    /// returns, except that [`Self::finalize_and_record_cached_part`] takes it, so a
    /// finalize that outlives its timeout keeps it until that step actually ends.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_streaming_part_cache_write(
        cache_key: String,
        upload_id: String,
        part_number: u32,
        sink: crate::cache::MultipartPartSink,
        mut tee_rx: tokio::sync::mpsc::Receiver<Bytes>,
        s3_result_rx: tokio::sync::oneshot::Receiver<Result<ResponseInfo>>,
        is_aws_chunked: bool,
        expected_decoded_len: Option<u64>,
        cache_dir: PathBuf,
        metrics: Option<Arc<RwLock<MetricsManager>>>,
        capacity_budget: Option<StreamingCapacityBudget>,
        pending_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> StreamingCacheOutcome {
        // The cache branch decodes aws-chunked incrementally; the upstream leg always
        // receives the original bytes verbatim.
        let decoder = if is_aws_chunked {
            Some(aws_chunked_decoder::IncrementalAwsChunkedDecoder::new())
        } else {
            None
        };

        // ---- Phase 1: drain the bounded tee channel, decode, write to sink ----
        //
        // `sink.write` is BLOCKING std::fs I/O; draining + writing on a blocking
        // thread keeps the async workers free. Inline blocking here can pin a Tokio
        // worker for the whole part upload and wedge a 2-worker runtime (see
        // `run_streaming_cache_write` for the full rationale).
        // One-shot return from the drain task (once per part, not a per-frame hot
        // path), so the size gap between `Continue` and `Skip` is immaterial; boxing
        // the sink would only add a needless allocation.
        #[allow(clippy::large_enum_variant)]
        enum PartDrainOutcome {
            Continue {
                sink: crate::cache::MultipartPartSink,
                decoder: Option<aws_chunked_decoder::IncrementalAwsChunkedDecoder>,
            },
            Skip {
                reason: &'static str,
                record_failure: bool,
            },
        }

        let drain_key = cache_key.clone();
        let drain_upload = upload_id.clone();
        let drain = tokio::task::spawn_blocking(move || {
            let mut sink = sink;
            let mut decoder = decoder;
            // Bytes staged into the sink so far — the figure `check_streaming_capacity`
            // evaluates. Decoded bytes, not wire bytes, since that is what occupies
            // the cache.
            let mut staged_bytes: u64 = 0;
            while let Some(frame) = tee_rx.blocking_recv() {
                let write_result = match decoder.as_mut() {
                    Some(dec) => match dec.push(frame.as_ref()) {
                        Ok(decoded) => {
                            staged_bytes = staged_bytes.saturating_add(decoded.len() as u64);
                            sink.write(&decoded)
                        }
                        Err(e) => {
                            // aws-chunked decode error → skip caching, keep forwarding
                            // (Req 3.4, 7.2).
                            warn!(
                                "Streaming part cache: aws-chunked decode error, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                                drain_key, drain_upload, part_number, e
                            );
                            sink.discard();
                            tee_rx.close();
                            return PartDrainOutcome::Skip {
                                reason: "decode_error",
                                record_failure: false,
                            };
                        }
                    },
                    None => {
                        staged_bytes = staged_bytes.saturating_add(frame.len() as u64);
                        sink.write(frame.as_ref())
                    }
                };

                if let Err(e) = write_result {
                    // Cache write error (disk full, etc.) → skip caching, keep
                    // forwarding (Req 7.1).
                    warn!(
                        "Streaming part cache: sink write error, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                        drain_key, drain_upload, part_number, e
                    );
                    sink.discard();
                    tee_rx.close();
                    return PartDrainOutcome::Skip {
                        reason: "cache_write_error",
                        record_failure: true,
                    };
                }

                // R16.8: bound the staging write when nothing sized this part up
                // front. `check_streaming_capacity` is the verdict; a breach discards
                // the staged part and leaves the upload untouched (Req 7.1). Checked
                // after the write so an I/O failure keeps reporting its own cause.
                if let Some(budget) = capacity_budget {
                    if let Err(e) = check_streaming_capacity(
                        staged_bytes,
                        budget.current_usage,
                        budget.max_capacity,
                    ) {
                        warn!(
                            "Streaming part cache: capacity budget exceeded, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, staged_bytes={}, current_usage={}, max_capacity={}, error={}",
                            drain_key,
                            drain_upload,
                            part_number,
                            staged_bytes,
                            budget.current_usage,
                            budget.max_capacity,
                            e
                        );
                        sink.discard();
                        tee_rx.close();
                        return PartDrainOutcome::Skip {
                            reason: "capacity_exceeded",
                            record_failure: false,
                        };
                    }
                }
            }
            PartDrainOutcome::Continue { sink, decoder }
        })
        .await
        .unwrap_or_else(|join_err| {
            error!(
                "Streaming part cache: drain task panicked, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                cache_key, upload_id, part_number, join_err
            );
            PartDrainOutcome::Skip {
                reason: "drain_task_panic",
                record_failure: true,
            }
        });

        let (sink, decoder) = match drain {
            PartDrainOutcome::Continue { sink, decoder } => (sink, decoder),
            PartDrainOutcome::Skip {
                reason,
                record_failure,
            } => {
                if record_failure {
                    if let Some(m) = &metrics {
                        m.read().await.record_put_cache_failure().await;
                    }
                }
                return StreamingCacheOutcome::Skipped(reason);
            }
        };

        // ---- Phase 2: channel closed (part body fully streamed). Await S3 result --
        let response = match s3_result_rx.await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                debug!(
                    "Streaming part cache: S3 error, not caching: cache_key={}, upload_id={}, part_number={}, error={}",
                    cache_key, upload_id, part_number, e
                );
                sink.discard();
                return StreamingCacheOutcome::Skipped("s3_error");
            }
            Err(e) => {
                // Expected on a client disconnect; counted by the metric below.
                debug!(
                    "Streaming part cache: S3 result channel closed unexpectedly: cache_key={}, upload_id={}, part_number={}, error={:?}",
                    cache_key, upload_id, part_number, e
                );
                sink.discard();
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                }
                return StreamingCacheOutcome::Skipped("s3_channel_closed");
            }
        };

        if !response.status().is_success() {
            debug!(
                "Streaming part cache: S3 non-success status, not caching: cache_key={}, upload_id={}, part_number={}, status={}",
                cache_key, upload_id, part_number, response.status()
            );
            sink.discard();
            return StreamingCacheOutcome::Skipped("s3_non_success");
        }

        // ---- Phase 3 (chunked only): finish + decoded-length validation ----
        if let Some(dec) = decoder {
            match dec.finish() {
                Ok(trailers) => {
                    if let Some(expected) = expected_decoded_len {
                        if trailers.decoded_len != expected {
                            // Decoded length disagrees with x-amz-decoded-content-length:
                            // skip caching, do NOT reject (S3 is the length authority),
                            // original bytes already forwarded verbatim (Req 3.4).
                            warn!(
                                "Streaming part cache: decoded-length mismatch, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, expected={}, actual={}",
                                cache_key, upload_id, part_number, expected, trailers.decoded_len
                            );
                            sink.discard();
                            return StreamingCacheOutcome::Skipped("decoded_length_mismatch");
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Streaming part cache: aws-chunked framing incomplete at finish, skipping cache (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                        cache_key, upload_id, part_number, e
                    );
                    sink.discard();
                    return StreamingCacheOutcome::Skipped("decode_finish_error");
                }
            }
        }

        // ---- Phase 4: extract ETag, finalize the part + record tracker under lock -
        let response_headers_map: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let etag = response_headers_map
            .get("etag")
            .or_else(|| response_headers_map.get("ETag"))
            .cloned()
            .unwrap_or_default();

        match Self::finalize_and_record_cached_part(
            &cache_dir,
            &cache_key,
            &upload_id,
            part_number,
            &etag,
            sink,
            pending_permit,
        )
        .await
        {
            Ok(size) => {
                let (bucket, key) = parse_cache_key(&cache_key);
                info!(
                    "UploadPart: bucket={}, key={}, part={}, size={}",
                    bucket,
                    key,
                    part_number,
                    format_size(size)
                );
                StreamingCacheOutcome::Committed
            }
            Err(e) => {
                // WARN, not ERROR, matching every sibling cache-skip in this
                // function. The client's upload is unaffected — S3 has already
                // accepted the part — so this is a missed caching opportunity, not
                // a failure. It was ERROR, and during the multipart lifecycle race
                // it produced a per-part flood that read like a disk fault on the
                // shared volume; the level implied an operator action that does not
                // exist.
                warn!(
                    "Streaming part cache: failed to record cached part (upload unaffected): cache_key={}, upload_id={}, part_number={}, error={}",
                    cache_key, upload_id, part_number, e
                );
                if let Some(m) = &metrics {
                    m.read().await.record_put_cache_failure().await;
                    m.read().await.record_skipped_put("commit_failed").await;
                }
                StreamingCacheOutcome::Skipped("part_record_error")
            }
        }
    }

    /// Finalize a streamed part and write its `part{N}.json` record, holding
    /// `part{N}.lock` across **both** the part-file publish (atomic `.tmp` →
    /// `part{N}.bin` rename, inside [`crate::cache::MultipartPartSink::finalize`])
    /// AND the record write. This is the same correctness gate as
    /// [`Self::cache_upload_part`]: a racing same-part-number write cannot leave the
    /// on-disk bytes and the tracker ETag out of sync. Returns the part's
    /// uncompressed size for logging.
    ///
    /// The `PART_FINALIZE_SLOTS` slot and `pending_permit` move into the blocking
    /// closure, so a finalize abandoned at its 30-second timeout keeps both until the
    /// closure ends rather than releasing them while its thread is still busy.
    async fn finalize_and_record_cached_part(
        cache_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        etag: &str,
        sink: crate::cache::MultipartPartSink,
        pending_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<u64> {
        use crate::cache_types::CachedPartInfo;

        // Bound how many finalizations occupy a `spawn_blocking` thread at once, so the
        // cache path cannot exhaust the blocking pool and fail the client's upload
        // (see `PART_FINALIZE_SLOTS`). Held for the whole critical section below and
        // moved into the blocking task, so it is released when that task ends, even
        // after the timeout has fired, and on every earlier exit path by `Drop`.
        //
        // `acquire()` only fails if the semaphore is closed, which never happens
        // for a process-lifetime static; treat it as a non-fatal cache skip rather
        // than unwrapping.
        let slot = match PART_FINALIZE_SLOTS.acquire().await {
            Ok(permit) => permit,
            Err(e) => {
                return Err(ProxyError::CacheError(format!(
                    "part finalize slot unavailable: {}",
                    e
                )))
            }
        };

        let multipart_dir = mpu_upload_dir(cache_dir, upload_id);
        // The sink open already created this directory; ensure it regardless.
        tokio::fs::create_dir_all(&multipart_dir)
            .await
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to create multipart directory: {}", e))
            })?;

        // PER-PART lock, not per-upload. The invariant being protected is per part
        // (this part's bytes must agree with this part's recorded ETag), so nothing
        // here ever needed to exclude a different part number — and excluding them
        // is what serialised every part of every concurrent upload on one lock and
        // failed client uploads at high part counts. See `record_part_blocking`.
        let lock_file_path = Self::part_lock_path(&multipart_dir, part_number);
        let etag_owned = etag.to_string();
        let cache_key_owned = cache_key.to_string();
        let upload_id_owned = upload_id.to_string();

        // The whole critical section — the blocking `flock`, the blocking
        // `sink.finalize()` (.tmp flush + rename), and the part-record write — runs on
        // a blocking thread under a timeout. The advisory lock is on the shared volume;
        // acquiring it (and holding it across the blocking finalize) on an async worker
        // would pin that worker and, across instances, could wedge a 2-worker runtime.
        // This mirrors the timeout+spawn_blocking lock pattern used elsewhere
        // (`disk_cache.rs`). The flock is held across the part publish AND its record
        // write, preserving the per-part correctness gate.
        let join = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::task::spawn_blocking(move || -> Result<u64> {
                // Dropped when this closure ends, not when the timeout fires.
                let _slot = slot;
                let _pending_permit = pending_permit;
                let lock_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&lock_file_path)
                    .map_err(|e| {
                        ProxyError::CacheError(format!("Failed to open lock file: {}", e))
                    })?;
                fs4::FileExt::lock(&lock_file).map_err(|e| {
                    ProxyError::CacheError(format!("Failed to acquire per-part lock: {}", e))
                })?;
                // Unlocked explicitly when it drops: see `journal_consolidator::unlock_lock_file`.
                let lock_file = crate::journal_consolidator::UnlockOnDrop(lock_file);

                // Publish the staged part bytes (flush residual batch + atomic rename)
                // UNDER the lock, so the on-disk part file and its recorded ETag are
                // updated as one critical section — identical correctness gate to
                // `cache_upload_part`.
                let info = sink.finalize()?;
                let uncompressed_size = info.uncompressed_size;

                // Compressed on-disk length of the part just published, measured HERE:
                // still inside the `part{N}.lock` hold, one statement after the rename
                // that made `part{N}.bin` visible. Two reasons it is taken here and not
                // when `in_progress_part_bytes` is summed:
                //
                //  - It is the only honest source. `info.uncompressed_size` is decoded
                //    object bytes; the file is LZ4-framed. Deriving one from the other
                //    is not possible, and substituting `uncompressed_size` would put
                //    `in_progress_part_bytes` in a different unit from every other byte
                //    figure it will eventually be summed with.
                //  - It is stable afterwards. On a retried CompleteMultipartUpload the
                //    `.bin` has already been renamed into `ranges/`, so a `stat` taken
                //    at sum time would fail for exactly the uploads that matter.
                //
                // Spec: cache-eviction-at-scale § 16.6.2.
                let part_file_path = multipart_dir.join(format!("part{}.bin", part_number));
                let compressed_size = match std::fs::metadata(&part_file_path) {
                    Ok(m) => m.len(),
                    Err(e) => {
                        // Deliberately NOT `unwrap_or(uncompressed_size)`: a wrong unit
                        // is worse than a missing figure. 0 reads as "not measured" and
                        // under-counts this part until the upload completes or is
                        // reclaimed on `incomplete_upload_ttl`.
                        warn!(
                            "Could not stat finalized part file for compressed size: \
                             path={}, error={} (in_progress_part_bytes will under-count \
                             this part)",
                            part_file_path.display(),
                            e
                        );
                        0
                    }
                };

                let part_info = CachedPartInfo::new(
                    part_number,
                    uncompressed_size,
                    etag_owned,
                    info.compression_algorithm,
                )
                .with_compressed_size(compressed_size)
                .with_file_identity(info.file_identity);

                // One small file, written once. Nothing shared is read or rewritten,
                // so this costs the same whether the upload has ten parts or ten
                // thousand.
                Self::record_part_blocking(&multipart_dir, part_number, &part_info).map_err(
                    |e| {
                        ProxyError::CacheError(format!(
                            "{} (cache_key={}, upload_id={}, part={})",
                            e, cache_key_owned, upload_id_owned, part_number
                        ))
                    },
                )?;

                // Release lock (also released on drop / early `?` return).
                drop(lock_file);
                Ok(uncompressed_size)
            }),
        )
        .await;

        match join {
            Ok(Ok(Ok(size))) => Ok(size),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(join_err)) => Err(ProxyError::CacheError(format!(
                "Part finalize task panicked: {}",
                join_err
            ))),
            Err(_elapsed) => Err(ProxyError::CacheError(format!(
                "Timed out (30s) acquiring part{}.lock / finalizing streamed part",
                part_number
            ))),
        }
    }

    /// Delete every `part{N}.json` record under `multipart_dir`. Returns how many
    /// were removed.
    ///
    /// Called by [`Self::finalize_multipart_upload`] **while `upload.lock` is still
    /// held**, immediately after the object `.meta` is published. That position is the
    /// point of the function: it closes the handoff window in which both the part
    /// records and the `.meta` describe the same bytes. See the call site for the
    /// ordering argument.
    ///
    /// Only the `.json` records are removed here. `part{N}.bin` has already been
    /// renamed into `ranges/`, and the rest of the scratch directory (`.lock` files,
    /// `upload.meta`) carries no byte figure, so it is left to the `remove_dir_all`
    /// after the lock is released.
    ///
    /// A failed unlink is logged and skipped rather than failing the completion: S3
    /// has already accepted the upload, and a surviving record is an over-count
    /// reclaimed on `incomplete_upload_ttl`, not a correctness failure.
    fn delete_part_records_blocking(multipart_dir: &std::path::Path) -> usize {
        let mut removed = 0usize;
        for part_number in Self::recorded_part_numbers(multipart_dir) {
            let path = Self::part_record_path(multipart_dir, part_number);
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    removed += 1;
                    if let Some(cache_dir) = cache_dir_of_upload_dir(multipart_dir) {
                        note_part_record_removed(cache_dir, &path);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => warn!(
                    "Failed to delete part record during completion: path={}, error={}",
                    path.display(),
                    e
                ),
            }
        }
        removed
    }
}

/// The most entries `mpus_in_progress/` may hold for write-through caching of multipart
/// uploads, across every proxy sharing the cache volume: one per upload directory plus one per
/// recorded part (`part{N}.json`).
///
/// [`walk_in_progress`] lists every upload directory and reads every part record, so its cost is
/// O(upload directories + part records) and the cap counts both; a cap on uploads alone would not
/// bound it (one upload of 10,000 parts is 10,000 reads). Without a cap the count was set by
/// clients: a client that started many uploads without completing them made every walk read all
/// of them until the incomplete-upload sweep reaped them, a day later by default.
///
/// No `/metrics` scrape or dashboard refresh waits on the walk: the background metrics collector
/// walks once per `metrics.collection_interval` and both serve its result. The readers that
/// decide on `P` (the staged authority figure, the PUT usage figure) no longer walk at all in the
/// steady state: they compose the collector's walk with the per-proxy published figures
/// ([`compose_in_progress_part_bytes`]).
///
/// **What still bounds the cap is [`tracked_multipart_budget`]'s own census**, not those
/// deciders. The census needs an entry **count**, and the published documents carry bytes, so it
/// takes a walk no older than [`TRACKED_MULTIPART_CENSUS_MAX_AGE`] whenever multipart admissions
/// are arriving — which is exactly the state the cap is sized for. So the walk cadence the cap is
/// set against is unchanged at one per 5 s, and the cap stays where the earlier measurement put it:
/// the figure below is the arithmetic, and raising it needs the census off the walk first, which
/// needs entry counts in the published document.
///
/// Why 5,000: measured on 2026-10-13 on the costliest shape (many one-part uploads), a cold walk
/// spread over [`IN_PROGRESS_WALK_THREADS`] threads cost 0.123 ms per entry on EFS once it read
/// only the records whose `stat` had changed (5% new records, the per-record `lstat` included),
/// against 0.268 ms reading every record; on a minimum-size FSx for OpenZFS volume 0.075 ms
/// against 0.103. Call it 0.13 ms per entry on EFS with the per-upload-directory `lstat`
/// [`UploadDirFingerprint`] adds. Each proxy stops admitting at its own figure, so three proxies
/// hold at most 3 x cap entries (see [`tracked_multipart_budget`]): 15,000 entries walk in about
/// 1.95 s, inside the 2 s a walk may take out of the 5 s between walks, and about 1.1 s on FSx
/// for OpenZFS. What it buys: at the AWS CLI's 8 MiB parts a 1 GiB upload is 129 entries, so 38
/// such uploads are write-cached at once across the fleet (11 at the 1,500 this replaces) and
/// further ones go to S3 uncached.
///
/// That figure is the shape where the incremental walk holds: one new upload directory is one new
/// record. An upload still receiving parts has its directory's `stat` move, so its records are all
/// read again, and a volume of nothing but growing many-part uploads walks at the full-walk cost
/// (0.283 ms per entry cold on EFS, 0.140 warm), which 15,000 entries would not finish inside 2 s.
/// Closing that needs the walk to stop re-reading those records, not a lower cap (see
/// [`UploadDirFingerprint`]).
///
/// Past the cap a `CreateMultipartUpload` writes no tracker and an `UploadPart` stages nothing;
/// both are forwarded to S3 unchanged. An upload one of whose parts is refused stops being
/// cached: its upload directory, recorded parts included, is removed, and its
/// `CompleteMultipartUpload` finds no tracker and is forwarded uncached, invalidating any cached
/// copy of the key as every uncached write does. Each refusal counts
/// `skipped_puts_total{multipart_tracking_full}`. See [`tracked_multipart_budget`] for how the
/// figure is kept fleet-wide.
pub(crate) const MAX_TRACKED_MULTIPART_ENTRIES: u64 = 5_000;

/// `skipped_puts_total` reason for a `CreateMultipartUpload` or `UploadPart` not write-through
/// cached because [`MAX_TRACKED_MULTIPART_ENTRIES`] was reached.
pub(crate) const MULTIPART_TRACKING_FULL_SKIP_REASON: &str = "multipart_tracking_full";

/// `skipped_puts_total` reason for a `CreateMultipartUpload` or `UploadPart` not write-through
/// cached because the count of in-progress multipart entries could not be read (a listing of
/// `mpus_in_progress/` failed). Unlike [`MULTIPART_TRACKING_FULL_SKIP_REASON`] the upload's
/// already-staged parts are kept: nothing says the volume is full.
pub(crate) const MULTIPART_TRACKING_UNKNOWN_SKIP_REASON: &str = "multipart_tracking_unknown";

/// How old the walk behind [`tracked_multipart_budget`] may be before an admission starts a
/// fresh one in the background. The same age every reader that reuses a walk accepts.
const TRACKED_MULTIPART_CENSUS_MAX_AGE: std::time::Duration = IN_PROGRESS_OBSERVATION_REUSE;

/// A refusal against [`MAX_TRACKED_MULTIPART_ENTRIES`] is logged at WARN at most once per this
/// interval per cache directory, and at debug otherwise.
const TRACKED_MULTIPART_REFUSAL_WARN_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Admission against [`MAX_TRACKED_MULTIPART_ENTRIES`], fleet-wide.
///
/// The figure is `census + landed since the census's walk began + in flight`.
///
/// - `census` is the entry count of the shared walk ([`observe_in_progress`]): upload
///   directories plus `part{N}.json` names, read from the shared volume, so it sees every
///   proxy's uploads and survives a restart. The budget takes a walk no older than
///   [`TRACKED_MULTIPART_CENSUS_MAX_AGE`], so in the fold epoch it usually reuses the walk a
///   deciding reader of `P` already paid for, and otherwise starts one. It never runs in front
///   of a forward: in the background when the last one is older than that age, and awaited only
///   when this process has none yet, on the tracker or part-setup task, which the client waits
///   for at most `WRITE_CACHE_SETUP_WAIT_BOUND`.
/// - `landed` counts this process's admitted entries whose write ended since the walk began, and
///   `in flight` those still running. Both count an admission whether or not it left an entry,
///   so this process's own error is an over-count until the next walk, the safe direction.
/// - An admission is charged every entry it can create: one for a tracker write, one for a part
///   record, and one more when the part's upload directory does not exist yet, because the part
///   store creates it.
///
/// Overshoot: another proxy's admissions since this proxy's walk began are not seen until the
/// next walk. Each proxy stops admitting once its own figure reaches the cap, so between two
/// walks each other proxy adds at most `cap - census`, and with `P` proxies the directory holds
/// at most `P x cap` entries in the worst case. In steady state the overshoot is the other
/// proxies' admissions during one walk's age plus its duration, at most about 7 s at the cap
/// while admissions arrive. Entries removed by Complete, Abort or the sweep stay counted until
/// the next walk, which refuses early rather than late.
///
/// A walk whose listing failed leaves the figure unknown. Admission then refuses with
/// [`Refusal::Unknown`], counted as `multipart_tracking_unknown`, and keeps the upload's staged
/// parts, since an unreadable listing says nothing about the volume being full.
mod tracked_multipart_budget {
    use super::{
        MAX_TRACKED_MULTIPART_ENTRIES, TRACKED_MULTIPART_CENSUS_MAX_AGE,
        TRACKED_MULTIPART_REFUSAL_WARN_INTERVAL,
    };
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Instant;
    use tracing::warn;

    struct Census {
        /// `None` when a listing failed. Admission refuses as unknown until the next walk.
        entries: Option<u64>,
        /// `DirState::landed` when the walk began.
        landed_base: u64,
        began_at: Instant,
    }

    #[derive(Default)]
    struct DirState {
        census: Option<Census>,
        landed: u64,
        in_flight: u64,
        /// Held while a census is taken, so one runs at a time.
        census_lock: Arc<tokio::sync::Mutex<()>>,
        last_refusal_warn: Option<Instant>,
    }

    /// Why an admission was refused.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Refusal {
        /// The figure reached [`MAX_TRACKED_MULTIPART_ENTRIES`].
        Full,
        /// The figure is unknown: the last walk's listing failed.
        Unknown,
    }

    /// Keyed by cache directory: one proxy process has one, and tests each own theirs.
    static STATE: LazyLock<Mutex<HashMap<PathBuf, DirState>>> = LazyLock::new(Default::default);

    #[cfg(test)]
    static CAP_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> = LazyLock::new(Default::default);

    fn with_state<R>(cache_dir: &Path, f: impl FnOnce(&mut DirState) -> R) -> R {
        let mut map = STATE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(map.entry(cache_dir.to_path_buf()).or_default())
    }

    fn cap(cache_dir: &Path) -> u64 {
        #[cfg(test)]
        if let Some(cap) = CAP_FOR_TEST
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(cache_dir)
        {
            return *cap;
        }
        let _ = cache_dir;
        MAX_TRACKED_MULTIPART_ENTRIES
    }

    /// This process's landed admissions for `cache_dir`, read when a walk begins so the census
    /// it produces knows which admissions it may already have seen.
    pub(crate) fn landed(cache_dir: &Path) -> u64 {
        with_state(cache_dir, |state| state.landed)
    }

    /// One admitted tracker write or part store, charged `entries`: in flight until dropped,
    /// then landed.
    pub(crate) struct Slot {
        cache_dir: PathBuf,
        entries: u64,
    }

    impl Drop for Slot {
        fn drop(&mut self) {
            let entries = self.entries;
            with_state(&self.cache_dir, |state| {
                state.in_flight = state.in_flight.saturating_sub(entries);
                state.landed = state.landed.saturating_add(entries);
            });
        }
    }

    /// Upload directories plus `part{N}.json` names under `mpus_in_progress/`, from a fresh walk.
    #[cfg(test)]
    pub(crate) fn count_entries(cache_dir: &Path) -> Option<u64> {
        super::walk_in_progress(cache_dir).entries
    }

    async fn run_census(cache_dir: PathBuf, _census_lock: tokio::sync::OwnedMutexGuard<()>) {
        let observation =
            super::observe_in_progress(&cache_dir, TRACKED_MULTIPART_CENSUS_MAX_AGE).await;
        let entries = observation.walk.entries;
        if entries.is_none() {
            warn!(
                "Could not count {} for the in-progress multipart budget; multipart uploads are \
                 forwarded uncached, counted as multipart_tracking_unknown, until the next count \
                 succeeds. Parts already staged are kept.",
                cache_dir.join("mpus_in_progress").display()
            );
        }
        with_state(&cache_dir, |state| {
            state.census = Some(Census {
                entries,
                landed_base: observation.landed_at_start,
                began_at: observation.began_at,
            })
        });
    }

    /// Admit one tracker write or part store that can create `entries` walk entries, or say
    /// why not. The caller then forwards uncached.
    pub(crate) async fn admit(cache_dir: &Path, entries: u64) -> Result<Slot, Refusal> {
        let (have_census, stale, census_lock) = with_state(cache_dir, |state| {
            (
                state.census.is_some(),
                state
                    .census
                    .as_ref()
                    .is_none_or(|c| c.began_at.elapsed() > TRACKED_MULTIPART_CENSUS_MAX_AGE),
                state.census_lock.clone(),
            )
        });
        if !have_census {
            let guard = census_lock.lock_owned().await;
            if with_state(cache_dir, |state| state.census.is_none()) {
                run_census(cache_dir.to_path_buf(), guard).await;
            }
        } else if stale {
            if let Ok(guard) = census_lock.try_lock_owned() {
                tokio::spawn(run_census(cache_dir.to_path_buf(), guard));
            }
        }
        let cap = cap(cache_dir);
        with_state(cache_dir, |state| {
            let census = state.census.as_ref().ok_or(Refusal::Unknown)?;
            let figure = census
                .entries
                .ok_or(Refusal::Unknown)?
                .saturating_add(state.landed.saturating_sub(census.landed_base))
                .saturating_add(state.in_flight);
            if figure.saturating_add(entries) > cap {
                return Err(Refusal::Full);
            }
            state.in_flight += entries;
            Ok(Slot {
                cache_dir: cache_dir.to_path_buf(),
                entries,
            })
        })
    }

    /// Whether a refusal is the first in [`TRACKED_MULTIPART_REFUSAL_WARN_INTERVAL`].
    pub(crate) fn first_refusal_in_interval(cache_dir: &Path) -> bool {
        with_state(cache_dir, |state| {
            let due = state
                .last_refusal_warn
                .is_none_or(|at| at.elapsed() >= TRACKED_MULTIPART_REFUSAL_WARN_INTERVAL);
            if due {
                state.last_refusal_warn = Some(Instant::now());
            }
            due
        })
    }

    /// Test seam: admit at most `cap` entries for `cache_dir`.
    #[cfg(test)]
    pub(crate) fn set_cap_for_test(cache_dir: &Path, cap: u64) {
        CAP_FOR_TEST
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(cache_dir.to_path_buf(), cap);
    }

    /// Test seam: drop this process's state for `cache_dir`, its last walk included, as a
    /// restarted proxy or another proxy on the same volume starts.
    #[cfg(test)]
    pub(crate) fn forget_for_test(cache_dir: &Path) {
        STATE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(cache_dir);
        super::forget_in_progress_observation_for_test(cache_dir);
    }
}

/// Count and log a refusal against [`MAX_TRACKED_MULTIPART_ENTRIES`].
async fn note_tracked_multipart_refusal(
    metrics: &Option<Arc<RwLock<MetricsManager>>>,
    cache_dir: &std::path::Path,
    operation: &str,
    refusal: tracked_multipart_budget::Refusal,
    cache_key: &str,
    upload_id: &str,
) {
    let reason = match refusal {
        tracked_multipart_budget::Refusal::Full => MULTIPART_TRACKING_FULL_SKIP_REASON,
        tracked_multipart_budget::Refusal::Unknown => MULTIPART_TRACKING_UNKNOWN_SKIP_REASON,
    };
    if let Some(metrics) = metrics {
        metrics.read().await.record_skipped_put(reason).await;
    }
    if !tracked_multipart_budget::first_refusal_in_interval(cache_dir) {
        debug!(
            "{}: in-progress multipart budget refused ({}), forwarding uncached: cache_key={}, \
             upload_id={}",
            operation,
            reason,
            cache_key,
            truncate_upload_id(upload_id)
        );
        return;
    }
    match refusal {
        tracked_multipart_budget::Refusal::Full => warn!(
            "{}: the cache volume already holds {} in-progress multipart entries (upload \
             directories plus recorded parts, across every proxy), so this upload is forwarded \
             to S3 uncached. The upload is unaffected. Further refusals in the next {:?} are \
             logged at debug and counted in skipped_puts_total{{{}}}: cache_key={}, upload_id={}",
            operation,
            MAX_TRACKED_MULTIPART_ENTRIES,
            TRACKED_MULTIPART_REFUSAL_WARN_INTERVAL,
            reason,
            cache_key,
            truncate_upload_id(upload_id)
        ),
        tracked_multipart_budget::Refusal::Unknown => warn!(
            "{}: the count of in-progress multipart entries could not be read, so this upload \
             is forwarded to S3 uncached. The upload is unaffected and its parts already staged \
             are kept. Further refusals in the next {:?} are logged at debug and counted in \
             skipped_puts_total{{{}}}: cache_key={}, upload_id={}",
            operation,
            TRACKED_MULTIPART_REFUSAL_WARN_INTERVAL,
            reason,
            cache_key,
            truncate_upload_id(upload_id)
        ),
    }
}

/// Sum of `compressed_size` over every `mpus_in_progress/*/part{N}.json` record —
/// the `P` term of cache-eviction-at-scale § 16.6.2.
///
/// Published as its own `/metrics` gauge, `cache.in_progress_part_bytes`, in **compressed
/// on-disk bytes**: the same unit as the range bytes under `ranges/`, and a disjoint
/// population from them. It is not (yet) a term in `total_cache_size`,
/// `read_cache_size` or `write_cache_size`; wiring it into those waits on the verified
/// ledger fold, without which the published identity would break.
///
/// # Why this does not inherit the O(staged) scan problem
///
/// The cost is a readdir of `mpus_in_progress/` plus one readdir and a small JSON parse
/// per upload directory, so it is O(concurrent uploads x parts of those uploads). That
/// bound has nothing to do with cache population: it is set by how many multipart
/// uploads are in flight right now, and each one's directory is deleted at completion.
/// A petabyte cache holding O(100 million) ranges has the same in-progress cardinality
/// as an empty one. This is precisely why § 16.6.4's per-shard partitioning — which
/// exists for the `.meta`-resident staged subset, whose cardinality *does* scale with
/// population — is not needed here and must not be copied onto this term.
/// [`MAX_TRACKED_MULTIPART_ENTRIES`] bounds the cardinality clients can set.
///
/// # Who walks, and how often
///
/// This is the synchronous walk, spread over [`IN_PROGRESS_WALK_THREADS`] threads of its own,
/// so it must run on the blocking pool. Async readers go through [`observe_in_progress`], which
/// runs it there, one walk at a time per cache directory, and shares each result: the
/// background metrics collector once per `metrics.collection_interval` (which `/metrics` and
/// the dashboard serve, with its age), and the deciding readers of `P`, which accept a walk at
/// most [`IN_PROGRESS_OBSERVATION_REUSE`] old or none at all.
///
/// # Failure posture: `None` is the UNREADABLE sentinel
///
/// This returned `u64` when it was introduced and produced `0` on an unreadable
/// `mpus_in_progress/`, which is also the value for "no upload is in flight" — the same
/// collapse `WriteLedger::count_entries` had, and the same reason it is wrong: a failed
/// traversal published the healthiest possible number. `None` now means the figure is
/// unknown, and the staged fold treats it as an incomplete input and refuses to install
/// (§ 16.6.3, clause 3).
///
/// A directory that does not exist is a genuine `Some(0)`: no multipart upload has ever
/// been staged here, which is a real observation rather than a failed one.
///
/// **Per-record** failures are `None` too, as of the same change that added the sentinel to
/// the traversal. An unreadable or unparseable `part{N}.json` used to be skipped with a
/// `debug!`, which under-counts `P` — and § 16.6.2 is explicit that under-counting is the
/// unsafe direction, because it makes the eviction trigger fire **late** rather than early.
/// The narrower posture was justified only by "nothing consumes `P` in a decision yet",
/// which is a statement about today's callers rather than about the figure, and the unit
/// that wires `P` into the trigger would inherit the defect rather than notice it.
///
/// The cost of the stricter posture is that one bad record makes the whole staged fold
/// retain rather than install, so `staged_authority_age_secs` rises and
/// `staged_fold_incomplete_total` increments. That is the intended behaviour: a figure that
/// is knowably wrong in the dangerous direction should not be installed, and the retain path
/// exists precisely to absorb this.
///
/// A record whose `compressed_size` is **absent** — one written by a build predating that
/// field — is also `None`, and that changed when `P` was wired into the trigger figure. Read
/// through [`PartRecordSizeProbe`], not through `CachedPartInfo`, because the latter's
/// `#[serde(default)]` collapses "not measured" into a measured `0` and under-counts. See
/// the `Ok(Ok(None))` arm of [`read_part_record_bytes`] for the direction-safety argument.
///
/// Spreading the work over threads changes none of this: every record is read, any failure
/// makes the whole figure `None`, and the sum of the same records is the same in any order.
pub fn in_progress_part_bytes(cache_dir: &std::path::Path) -> Option<u64> {
    walk_in_progress(cache_dir).bytes
}

/// Threads one [`walk_in_progress`] spreads its upload-directory listings, and then its
/// part-record reads, across.
///
/// Measured on 2026-10-13 against a walk of many one-part uploads: 32 threads made a cold walk
/// about 20 times faster than one thread on EFS and about 11 times faster on a minimum-size FSx
/// for OpenZFS volume, and 64 added little on EFS and nothing on FSx for OpenZFS. On network
/// storage each listing and read is a round trip, so the threads wait rather than compute. They
/// are the walk's own, started and joined inside one call on the blocking pool, so no Tokio
/// worker waits on them.
pub(crate) const IN_PROGRESS_WALK_THREADS: usize = 32;

/// How old a walk of `mpus_in_progress/` may be for the readers that reuse one: the PUT usage
/// figure, the eviction pass's published result and the multipart budget. The same age as the
/// staged authority figure's memo, which already bounds how old the `P` inside that figure may be.
pub(crate) const IN_PROGRESS_OBSERVATION_REUSE: std::time::Duration =
    std::time::Duration::from_secs(5);

/// One walk of `mpus_in_progress/`: `P`, and the entry count the
/// [`MAX_TRACKED_MULTIPART_ENTRIES`] budget needs, from the same listings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InProgressWalk {
    /// `P`, with [`in_progress_part_bytes`]'s UNREADABLE rules.
    pub(crate) bytes: Option<u64>,
    /// Upload directories plus `part{N}.json` names. `None` when a listing failed; an upload
    /// directory removed between the two listings is not counted.
    pub(crate) entries: Option<u64>,
}

/// Whether a `mpus_in_progress/` entry is a directory, with [`std::path::Path::is_dir`]'s meaning.
///
/// `DirEntry::file_type` answers from the `d_type` the listing already returned, so the common
/// case costs no `stat`; std falls back to `lstat` only when the file system reports
/// `DT_UNKNOWN`. It does not follow a symlink, where `Path::is_dir` does, so a symlink is resolved
/// with `is_dir` to keep that meaning, and a `file_type` error falls back to `is_dir` too.
fn listed_entry_is_dir(entry: &std::fs::DirEntry) -> bool {
    match entry.file_type() {
        Ok(t) if t.is_dir() => true,
        Ok(t) if t.is_symlink() => entry.path().is_dir(),
        Ok(_) => false,
        Err(_) => entry.path().is_dir(),
    }
}

/// Whether a file name is a part record, `part{N}.json` with `N` a `u32`.
fn is_part_record_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .and_then(|n| n.strip_prefix("part"))
        .and_then(|rest| rest.strip_suffix(".json"))
        .is_some_and(|digits| digits.parse::<u32>().is_ok())
}

/// Run `work` over every item, spread over up to `threads` threads (the calling thread is
/// one), and return the results in item order. A thread that cannot be started
/// leaves its share to the others. A panic in `work` is re-raised on the calling thread.
fn spread_over_walk_threads<T: Sync, R: Send>(
    items: &[T],
    threads: usize,
    work: impl Fn(&T) -> R + Sync,
) -> Vec<R> {
    let threads = threads.min(items.len());
    if threads <= 1 {
        return items.iter().map(work).collect();
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let drain = || {
        let mut done = Vec::new();
        loop {
            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let Some(item) = items.get(i) else { break };
            done.push((i, work(item)));
        }
        done
    };
    let mut slots: Vec<Option<R>> = std::iter::repeat_with(|| None).take(items.len()).collect();
    std::thread::scope(|scope| {
        let helpers: Vec<_> = (1..threads)
            .filter_map(|_| {
                std::thread::Builder::new()
                    .name("mpu-walk".to_string())
                    .spawn_scoped(scope, drain)
                    .ok()
            })
            .collect();
        for (i, r) in drain() {
            slots[i] = Some(r);
        }
        for helper in helpers {
            match helper.join() {
                Ok(done) => {
                    for (i, r) in done {
                        slots[i] = Some(r);
                    }
                }
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
    });
    // Every index below `items.len()` is taken by exactly one `fetch_add`, so every slot is set.
    slots.into_iter().flatten().collect()
}

/// One upload directory's share of a walk.
struct UploadListing {
    /// This upload's entries for the budget's count: 1 for the directory plus its part-record
    /// names; `Some(0)` when it is not counted, `None` when its listing failed.
    entries: Option<u64>,
    /// Its part records for `P`, or `None` when `P` is unknown because of this upload.
    records: Option<Vec<std::path::PathBuf>>,
    /// Its own `stat`, taken before its listing. `None` (not a directory, or the `lstat`
    /// failed) leaves every remembered size in it unusable, so they are read again.
    dir: Option<(std::path::PathBuf, UploadDirFingerprint)>,
}

/// List one entry of `mpus_in_progress/` for both figures.
///
/// The two figures keep the rules each had as a separate walk. For `P`: an entry that is a
/// directory by [`Path::is_dir`](std::path::Path::is_dir)'s meaning is listed, and any listing
/// error, a directory removed since the first listing included, makes `P` unknown, because
/// skipping it would under-count by a whole upload. For the count: only a real directory counts,
/// one removed since the first listing is not counted, and any other error makes the count
/// unknown.
fn list_upload_for_walk(upload: &std::fs::DirEntry) -> UploadListing {
    let counted = match upload.file_type() {
        Ok(t) => Some(t.is_dir()),
        Err(_) => None,
    };
    if !listed_entry_is_dir(upload) {
        return UploadListing {
            entries: counted.map(|_| 0),
            records: Some(Vec::new()),
            dir: None,
        };
    }
    // Before the listing, so a rename that lands between the two is not covered by the
    // fingerprint this walk stores: the next walk sees the directory as changed and reads its
    // records again, rather than trusting a size measured before that rename.
    let dir = fingerprint_upload_dir(&upload.path()).map(|fp| (upload.path(), fp));
    let listing = match std::fs::read_dir(upload.path()) {
        Ok(listing) => listing,
        Err(e) => {
            // Same class as an unreadable record: this upload's parts are unknown, so `P` is
            // unknown. `continue` here would under-count by a whole upload.
            warn!(
                "Could not read upload directory {}: {}. Reporting UNREADABLE rather \
                 than skipping the upload's bytes.",
                upload.path().display(),
                e
            );
            let entries = match (counted, e.kind()) {
                // A symlink is not counted, so its listing does not matter to the count.
                (Some(false), _) => Some(0),
                (Some(true), std::io::ErrorKind::NotFound) => Some(0),
                _ => None,
            };
            return UploadListing {
                entries,
                records: None,
                dir: None,
            };
        }
    };
    let mut names_known = true;
    let mut part_names: u64 = 0;
    let mut records = Vec::new();
    for record in listing {
        let Ok(record) = record else {
            // `P` skips an entry the listing could not return, as it always has; the count
            // cannot say how many there were.
            names_known = false;
            continue;
        };
        if is_part_record_name(&record.file_name()) {
            part_names += 1;
            records.push(record.path());
        }
    }
    let entries = match counted {
        Some(true) if names_known => Some(1 + part_names),
        Some(true) => None,
        Some(false) => Some(0),
        None => None,
    };
    UploadListing {
        entries,
        records: Some(records),
        dir,
    }
}

/// Read one part record's `compressed_size`, or `None` (after logging why) when it cannot be
/// measured.
fn read_part_record_bytes(record: &std::path::Path) -> Option<u64> {
    match std::fs::read_to_string(record)
        .map(|s| serde_json::from_str::<PartRecordSizeProbe>(&s).map(|p| p.compressed_size))
    {
        Ok(Ok(Some(bytes))) => Some(bytes),
        Ok(Ok(None)) => {
            // A part record written by a build that predates `compressed_size`.
            //
            // THE MIXED-VERSION UNDER-COUNT, CLOSED HERE. `CachedPartInfo`'s field
            // is `#[serde(default)]`, so parsing such a record through that type
            // yields `0` — indistinguishable from a measured zero, and an
            // **under**-count of `P`. § 16.6.2 chose an over-count at the
            // completion handoff specifically because an under-count makes the
            // eviction trigger fire LATE, and this is the same asymmetry: silently
            // dropping an upload's bytes makes the staged figure smaller than the
            // truth, which is the direction that fills a volume.
            //
            // UNREADABLE, not zero, and not a skip: the fold treats `None` as an
            // incomplete input and RETAINS the previously installed figure
            // (§ 16.6.3, clause 3), so a mixed-version window costs authority
            // **age** — visible on `staged_shard_authority_age_secs` — rather than
            // authority **accuracy** in the unsafe direction. The window is bounded
            // by the upload completing or by `incomplete_upload_ttl` reclaiming its
            // directory, both of which delete the record.
            //
            // Detected by field PRESENCE rather than by value, so a legitimately
            // zero-length part — which S3 does not permit but which nothing here
            // relies on — is still counted as the zero it is.
            warn!(
                "Part record {} carries no `compressed_size` field, so it was \
                 written by a build predating cache-eviction-at-scale § 16.6.2. \
                 Reporting UNREADABLE rather than counting it as 0 bytes, which \
                 would under-count in_progress_part_bytes and make the staging \
                 eviction trigger fire late. This clears when the upload completes \
                 or is reclaimed on incomplete_upload_ttl.",
                record.display()
            );
            None
        }
        Ok(Err(e)) => {
            warn!(
                "Unparseable part record {}: {}. Reporting UNREADABLE rather than \
                 silently under-counting in_progress_part_bytes, which would make \
                 the eviction trigger fire late.",
                record.display(),
                e
            );
            None
        }
        Err(e) => {
            warn!(
                "Unreadable part record {}: {}. Reporting UNREADABLE rather than \
                 silently under-counting in_progress_part_bytes.",
                record.display(),
                e
            );
            None
        }
    }
}

/// Walk `mpus_in_progress/` once for `P` and the budget's entry count.
///
/// The top-level listing is one `readdir` stream and stays serial. The upload directories are
/// then listed spread over [`IN_PROGRESS_WALK_THREADS`] threads, and every part record of every
/// upload is read spread over them again, so one upload with many parts gains as much as many
/// uploads with one part each. Synchronous: call it on the blocking pool.
pub(crate) fn walk_in_progress(cache_dir: &std::path::Path) -> InProgressWalk {
    walk_in_progress_spread(cache_dir, IN_PROGRESS_WALK_THREADS)
}

/// [`walk_in_progress`] over `threads` threads; `1` is the serial walk.
fn walk_in_progress_spread(cache_dir: &std::path::Path, threads: usize) -> InProgressWalk {
    #[cfg(test)]
    {
        note_in_progress_walk_for_test(cache_dir);
        if in_progress_listing_fails_for_test(cache_dir) {
            return InProgressWalk {
                bytes: None,
                entries: None,
            };
        }
    }
    let mpus_dir = cache_dir.join("mpus_in_progress");
    let listing = match std::fs::read_dir(&mpus_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return InProgressWalk {
                bytes: Some(0),
                entries: Some(0),
            }
        }
        Err(e) => {
            warn!(
                "Could not read {} for in_progress_part_bytes: {}. Reporting UNREADABLE \
                 rather than 0, which is also the no-uploads-in-flight value.",
                mpus_dir.display(),
                e
            );
            return InProgressWalk {
                bytes: None,
                entries: None,
            };
        }
    };
    // `P` skips an entry the listing could not return, as it always has; the count cannot.
    let mut entries_known = true;
    let uploads: Vec<std::fs::DirEntry> = listing
        .filter_map(|entry| {
            entries_known &= entry.is_ok();
            entry.ok()
        })
        .collect();

    let listings = spread_over_walk_threads(&uploads, threads, list_upload_for_walk);
    let mut entries: Option<u64> = entries_known.then_some(0);
    let mut records: Option<Vec<std::path::PathBuf>> = Some(Vec::new());
    let mut dirs: std::collections::HashMap<std::path::PathBuf, UploadDirFingerprint> =
        std::collections::HashMap::new();
    for listing in listings {
        if let Some((dir, fingerprint)) = listing.dir {
            dirs.insert(dir, fingerprint);
        }
        entries = match (entries, listing.entries) {
            (Some(total), Some(n)) => Some(total.saturating_add(n)),
            _ => None,
        };
        records = match (records, listing.records) {
            (Some(mut all), Some(mut more)) => {
                all.append(&mut more);
                Some(all)
            }
            _ => None,
        };
    }

    let bytes = records.and_then(|records| {
        // Past the bound nothing is remembered and every record is read, as before.
        let remember = records.len() <= MAX_REMEMBERED_PART_RECORDS;
        let remembered = if remember {
            take_remembered_part_records(cache_dir)
        } else {
            RememberedPartRecords::default()
        };
        // The upload directories whose own `stat` is what it was when their records' sizes were
        // measured. Only those sizes may be reused; see [`UploadDirFingerprint`].
        let mut unchanged_dirs: std::collections::HashSet<std::path::PathBuf> =
            std::collections::HashSet::new();
        for (dir, fingerprint) in &dirs {
            if remembered.dirs.get(dir) == Some(fingerprint) {
                unchanged_dirs.insert(dir.clone());
            }
        }
        // After the first record that cannot be measured the figure is `None` whatever the rest
        // say, so the threads stop reading.
        let failed = std::sync::atomic::AtomicBool::new(false);
        let sizes = spread_over_walk_threads(&records, threads, |record| {
            #[cfg(test)]
            note_record_read_thread_for_test(cache_dir);
            if failed.load(std::sync::atomic::Ordering::Relaxed) {
                return None;
            }
            let measured = measure_part_record(cache_dir, record, &remembered, &unchanged_dirs);
            if measured.is_none() {
                failed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            measured
        });
        if remember {
            // Only this walk's measured records are kept, so a record or upload that has gone
            // is forgotten, and one that could not be measured is read again next walk.
            let kept = RememberedPartRecords {
                dirs,
                records: records
                    .iter()
                    .zip(sizes.iter())
                    .filter_map(|(record, measured)| measured.map(|m| (record.clone(), m)))
                    .collect(),
            };
            store_remembered_part_records(cache_dir, kept);
        }
        if failed.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        // Saturating, as the serial walk was; for non-negative terms the saturated sum does not
        // depend on the order.
        sizes.into_iter().try_fold(0u64, |total, measured| {
            measured.map(|(_, size)| total.saturating_add(size))
        })
    });
    InProgressWalk { bytes, entries }
}

/// The most part records one walk remembers sizes for, per cache directory.
///
/// Three proxies each stop admitting at [`MAX_TRACKED_MULTIPART_ENTRIES`], so a volume holds at
/// most three times the cap; this is well above that. A walk that lists more records remembers
/// none and reads every one, as a walk did before sizes were remembered: slower, never a wrong
/// sum. At about 300 bytes an entry the map stays under 20 MiB.
const MAX_REMEMBERED_PART_RECORDS: usize = 65_536;

/// What a part record's `stat` says about which file it is. A remembered size is reused only
/// when all of it matches.
///
/// The only writer, [`SignedPutHandler::record_part_blocking`], writes a temporary file and
/// renames it over the record, so a changed record is a different inode. The inode number alone
/// is not enough: once the replaced file is unlinked its number can be given to the next
/// temporary file, so a record retried twice between two walks could come back with the number
/// it had. The change time is set when that new file is written, and the length differs whenever
/// the content's length does, so a reused number with the same change time to the nanosecond and
/// the same length would be needed to reuse a wrong size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartRecordFingerprint {
    ino: u64,
    ctime: i64,
    ctime_nsec: i64,
    len: u64,
}

/// What an upload directory's `stat` says about which directory it is and when it last changed.
///
/// This is what closes [`PartRecordFingerprint`]'s residual. A record replaced ONCE cannot come
/// back with its own inode number, because [`SignedPutHandler::record_part_blocking`] creates
/// `part{N}.json.tmp` while the old record is still linked, so the old number is not free when
/// the new file is allocated. Replaced TWICE between two walks it can: the first rename frees the
/// number and the second temporary file may be given it. The record's own `stat` cannot then tell
/// the two apart if the change time collides to the nanosecond and the length is equal, so no
/// check on the record alone is enough.
///
/// POSIX requires `rename` into a directory to update that directory's `mtime` and `ctime`, and
/// a part record is only ever replaced by a rename into its upload directory, so a replacement
/// always moves these. A remembered size is therefore reused only from a directory whose own
/// fingerprint is unchanged as well. It costs one extra `lstat` per upload directory, not per
/// record; what it costs in reads is that every remembered size in a directory that gained, lost
/// or replaced a record is read again, so an upload still receiving parts is re-read in full on
/// every walk.
///
/// Not closed by `mtime` on the record instead: the record's `mtime` and `ctime` come from the
/// same write, so a clock coarse enough to collide one collides the other. The deterministic
/// close from `stat` data alone is `statx(STATX_CHANGE_COOKIE)` (NFSv4's mandatory monotonic
/// change attribute, Linux 6.6+) on the upload directory, which would also end the re-reads;
/// that is a later change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UploadDirFingerprint {
    ino: u64,
    ctime: i64,
    ctime_nsec: i64,
    mtime: i64,
    mtime_nsec: i64,
}

/// One upload directory's `stat`, or `None` when it could not be taken.
fn fingerprint_upload_dir(dir: &std::path::Path) -> Option<UploadDirFingerprint> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(dir).ok()?;
    Some(UploadDirFingerprint {
        ino: meta.ino(),
        ctime: meta.ctime(),
        ctime_nsec: meta.ctime_nsec(),
        mtime: meta.mtime(),
        mtime_nsec: meta.mtime_nsec(),
    })
}

/// Remembered `compressed_size` per part record path, with the `stat` of the upload directories
/// those sizes were measured in, per cache directory.
#[derive(Default)]
struct RememberedPartRecords {
    dirs: std::collections::HashMap<std::path::PathBuf, UploadDirFingerprint>,
    records: std::collections::HashMap<std::path::PathBuf, (PartRecordFingerprint, u64)>,
}

static REMEMBERED_PART_RECORDS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, RememberedPartRecords>>,
> = std::sync::LazyLock::new(Default::default);

/// Take `cache_dir`'s remembered sizes. Two walks running at once each take what is there (the
/// second takes nothing and reads every record); each stores only what it measured, so whichever
/// stores last leaves sizes that match their fingerprints.
fn take_remembered_part_records(cache_dir: &std::path::Path) -> RememberedPartRecords {
    REMEMBERED_PART_RECORDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(cache_dir)
        .unwrap_or_default()
}

fn store_remembered_part_records(cache_dir: &std::path::Path, kept: RememberedPartRecords) {
    REMEMBERED_PART_RECORDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(cache_dir.to_path_buf(), kept);
}

/// One part record's fingerprint and `compressed_size`: the remembered size when the record's
/// `stat` matches the one it was read under, otherwise read now. `None`, with
/// [`read_part_record_bytes`]'s rules, when it cannot be measured; a record whose `stat` fails
/// (removed since the listing) is `None` too, as its read would have been.
///
/// The `stat` comes before the read, so a record replaced in between is remembered under the
/// older fingerprint and read again on the next walk.
///
/// A size is reused only from an upload directory in `unchanged_dirs`, which is what makes a
/// record replaced twice between two walks safe; see [`UploadDirFingerprint`].
fn measure_part_record(
    cache_dir: &std::path::Path,
    record: &std::path::Path,
    remembered: &RememberedPartRecords,
    unchanged_dirs: &std::collections::HashSet<std::path::PathBuf>,
) -> Option<(PartRecordFingerprint, u64)> {
    use std::os::unix::fs::MetadataExt;
    let _ = cache_dir;
    let meta = match std::fs::symlink_metadata(record) {
        Ok(meta) => meta,
        Err(e) => {
            warn!(
                "Unreadable part record {}: {}. Reporting UNREADABLE rather than \
                 silently under-counting in_progress_part_bytes.",
                record.display(),
                e
            );
            return None;
        }
    };
    let fingerprint = PartRecordFingerprint {
        ino: meta.ino(),
        ctime: meta.ctime(),
        ctime_nsec: meta.ctime_nsec(),
        len: meta.len(),
    };
    #[cfg(test)]
    let fingerprint = if remember_by_name_only_for_test() {
        PartRecordFingerprint {
            ino: 0,
            ctime: 0,
            ctime_nsec: 0,
            len: 0,
        }
    } else {
        fingerprint
    };
    let reusable = record
        .parent()
        .is_some_and(|dir| unchanged_dirs.contains(dir));
    if reusable {
        if let Some((seen, size)) = remembered.records.get(record) {
            if *seen == fingerprint {
                return Some((fingerprint, *size));
            }
        }
    }
    #[cfg(test)]
    note_part_record_open_for_test(cache_dir);
    read_part_record_bytes(record).map(|size| (fingerprint, size))
}

/// Part records opened by walks of `cache_dir` in this test binary.
#[cfg(test)]
static PART_RECORD_OPENS_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, u64>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
fn note_part_record_open_for_test(cache_dir: &std::path::Path) {
    *PART_RECORD_OPENS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(cache_dir.to_path_buf())
        .or_insert(0) += 1;
}

#[cfg(test)]
fn part_record_opens_for_test(cache_dir: &std::path::Path) -> u64 {
    PART_RECORD_OPENS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(cache_dir)
        .copied()
        .unwrap_or(0)
}

/// Red-side lever for: key remembered sizes by name only. Always `false` on the tree.
#[cfg(test)]
fn remember_by_name_only_for_test() -> bool {
    false
}

/// Test seam: remember `size` for `record` under the record's CURRENT `stat`, leaving the
/// remembered upload directories as the last walk stored them. That is exactly what a record
/// replaced twice between two walks looks like when the second replacement is given the inode
/// number the first freed with the same change time and length — the one schedule
/// [`PartRecordFingerprint`] cannot see, which no file system can be asked to produce on demand.
#[cfg(test)]
fn forge_remembered_part_record_for_test(
    cache_dir: &std::path::Path,
    record: &std::path::Path,
    size: u64,
) {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(record).unwrap();
    let fingerprint = PartRecordFingerprint {
        ino: meta.ino(),
        ctime: meta.ctime(),
        ctime_nsec: meta.ctime_nsec(),
        len: meta.len(),
    };
    let mut remembered = take_remembered_part_records(cache_dir);
    remembered
        .records
        .insert(record.to_path_buf(), (fingerprint, size));
    store_remembered_part_records(cache_dir, remembered);
}

/// The result of one [`walk_in_progress`], and when it began.
#[derive(Debug, Clone)]
pub(crate) struct InProgressObservation {
    pub(crate) walk: InProgressWalk,
    /// Every instance's `own_added_ever_bytes` as it stood before this walk listed anything;
    /// `None` when the published documents could not be read. The base of
    /// [`compose_in_progress_part_bytes`]'s growth term.
    pub(crate) added_ever_at_start: Option<std::sync::Arc<AddedEverSnapshot>>,
    /// Taken BEFORE the walk, so an age read from it never claims the figure is younger than
    /// its inputs.
    pub(crate) began_at: std::time::Instant,
    /// This process's landed budget admissions when the walk began; see
    /// [`tracked_multipart_budget`].
    landed_at_start: u64,
}

impl InProgressObservation {
    /// Whole seconds since the walk began.
    pub(crate) fn age_secs(&self) -> u64 {
        self.began_at.elapsed().as_secs()
    }
}

/// One cache directory's last walk and the walk in flight, if any.
#[derive(Default)]
struct InProgressObservationSlot {
    last: Option<InProgressObservation>,
    /// The walk before `last`. The reconciliation needs two walks and the snapshots they were
    /// dated by; see [`reconcile_in_progress_figures`].
    previous: Option<InProgressObservation>,
    inflight: Option<tokio::sync::watch::Receiver<Option<InProgressObservation>>>,
    /// The last reconciliation's verdict and when it was taken. `true` means the composition
    /// read below the walk, so deciders take the walk until the next one replaces it.
    reconciliation: Option<(bool, std::time::Instant)>,
}

/// Keyed by cache directory: production has one, a test binary one per fixture.
static IN_PROGRESS_OBSERVATIONS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, InProgressObservationSlot>>,
> = std::sync::LazyLock::new(Default::default);

/// Clears a directory's in-flight walk when the task running it ends by panic or cancellation,
/// so a lost task cannot wedge the single flight. Disarmed when the walk is recorded, because by
/// then the mark may belong to the next walk.
struct InProgressWalkInflight {
    cache_dir: std::path::PathBuf,
    armed: bool,
}

impl Drop for InProgressWalkInflight {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut map = IN_PROGRESS_OBSERVATIONS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(slot) = map.get_mut(&self.cache_dir) {
            slot.inflight = None;
        }
    }
}

/// The observation a walk that never reported stands for: both figures unknown.
fn unreported_in_progress_observation() -> InProgressObservation {
    InProgressObservation {
        walk: InProgressWalk {
            bytes: None,
            entries: None,
        },
        added_ever_at_start: None,
        began_at: std::time::Instant::now(),
        landed_at_start: 0,
    }
}

async fn run_in_progress_walk(
    cache_dir: std::path::PathBuf,
    done: tokio::sync::watch::Sender<Option<InProgressObservation>>,
) {
    let mut inflight = InProgressWalkInflight {
        cache_dir: cache_dir.clone(),
        armed: true,
    };
    let landed_at_start = tracked_multipart_budget::landed(&cache_dir);
    let began_at = std::time::Instant::now();
    let dir = cache_dir.clone();
    // The snapshot is taken inside the same blocking call and BEFORE the walk: see
    // [`added_ever_snapshot_blocking`] for why that order is what bounds the composition. The
    // two statements below are in that order deliberately, and the hook between them is how a
    // test puts a part record inside the window to tell the two orders apart.
    let walked = tokio::task::spawn_blocking(move || {
        let at_start = added_ever_snapshot_blocking(&dir);
        #[cfg(test)]
        run_between_snapshot_and_walk_hook_for_tests(&dir);
        let walk = walk_in_progress(&dir);
        (at_start, walk)
    })
    .await;
    let (added_ever_at_start, walk) = match walked {
        Ok((at_start, walk)) => (at_start.map(std::sync::Arc::new), walk),
        Err(e) => {
            warn!(
                "The walk of {} did not complete: {}. Reporting UNREADABLE rather than 0, which \
                 is also the no-uploads-in-flight value.",
                cache_dir.join("mpus_in_progress").display(),
                e
            );
            (
                None,
                InProgressWalk {
                    bytes: None,
                    entries: None,
                },
            )
        }
    };
    #[cfg(test)]
    let began_at = match take_walk_backdate_for_test(&cache_dir) {
        Some(by) => began_at.checked_sub(by).unwrap_or(began_at),
        None => began_at,
    };
    let observation = InProgressObservation {
        walk,
        added_ever_at_start,
        began_at,
        landed_at_start,
    };
    {
        let mut map = IN_PROGRESS_OBSERVATIONS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let slot = map.entry(cache_dir).or_default();
        // The walk this one replaces is kept as the reconciliation's base: see
        // [`reconcile_in_progress_figures`].
        slot.previous = slot.last.take();
        slot.last = Some(observation.clone());
        slot.inflight = None;
        inflight.armed = false;
    }
    let _ = done.send(Some(observation));
}

/// A walk of `cache_dir`'s `mpus_in_progress/` no older than `max_age`, run on the blocking pool.
///
/// One walk per cache directory is in flight in this process at a time, and every walk's result
/// is kept, so readers share walks:
///
/// - the last walk is returned when it began at most `max_age` ago;
/// - otherwise a walk already running is joined, **and the walk it produced is accepted only if
///   it too began at most `max_age` ago, or began after this call**; a walk that was already in
///   flight when this caller arrived may have begun arbitrarily long before, so accepting it
///   would let a caller that asked for a 5 s figure act on a three-minute-old one. One further
///   walk is then started and awaited, and that one is accepted whatever it cost.
/// - otherwise one is started, and accepted whatever it cost, for the same reason. It runs to
///   completion and is kept even if this caller is dropped first.
///
/// A walk that began after the call is the freshest figure a walk can produce, so where a walk
/// of the volume takes longer than `max_age` the caller acts on it rather than start another:
/// rejecting it on its age would pay a second walk of the same length per call and return
/// nothing newer, which on a volume whose walk is slower than the bound is continuous walking.
/// The age is still enforced against a walk that began before the call, which is the case where
/// a fresher one can exist.
///
/// `max_age` of zero means "walk now": only a walk already in flight is shared, and its age is
/// not re-checked, because a zero bound no caller could satisfy would mean a walk per caller.
/// The one caller that passes zero, the metrics collector, dates its result by `began_at`. A walk
/// that failed is kept as the failure it was (`None` figures), never as a zero.
pub(crate) async fn observe_in_progress(
    cache_dir: &std::path::Path,
    max_age: std::time::Duration,
) -> InProgressObservation {
    let called_at = std::time::Instant::now();
    // At most two: the second is started by this call, so its `began_at` is after it.
    for attempt in 0..2u32 {
        let mut done = {
            let mut map = IN_PROGRESS_OBSERVATIONS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let slot = map.entry(cache_dir.to_path_buf()).or_default();
            if let Some(last) = slot.last.as_ref() {
                if !max_age.is_zero() && last.began_at.elapsed() <= max_age {
                    return last.clone();
                }
            }
            match &slot.inflight {
                Some(done) => done.clone(),
                None => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    slot.inflight = Some(rx.clone());
                    tokio::spawn(run_in_progress_walk(cache_dir.to_path_buf(), tx));
                    rx
                }
            }
        };
        let observation = match done.wait_for(|observation| observation.is_some()).await {
            Ok(observation) => observation
                .clone()
                .unwrap_or_else(unreported_in_progress_observation),
            // The walk's task ended without reporting (cancelled at shutdown).
            Err(_) => unreported_in_progress_observation(),
        };
        let too_old = !max_age.is_zero()
            && observation.began_at.elapsed() > max_age
            && observation.began_at < called_at;
        if !too_old || attempt == 1 {
            return observation;
        }
        debug!(
            "The in-flight walk of {} began {:?} ago, past the {:?} this reader asked for, so it \
             walks again rather than act on it.",
            cache_dir.join("mpus_in_progress").display(),
            observation.began_at.elapsed(),
            max_age
        );
    }
    unreachable!("the loop returns on its second attempt")
}

/// The last walk of `cache_dir`'s `mpus_in_progress/` this process kept, without starting one.
/// `None` until a walk has finished.
///
/// For a reader that must never walk: a scrape's staged-authority refresh pairs a fresh `F` with
/// this `P` and dates the result by `began_at`.
pub(crate) fn last_in_progress_observation(
    cache_dir: &std::path::Path,
) -> Option<InProgressObservation> {
    IN_PROGRESS_OBSERVATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(cache_dir)
        .and_then(|slot| slot.last.clone())
}
// ---------------------------------------------------------------------------------------------
// The published per-proxy in-progress figure
// ---------------------------------------------------------------------------------------------

/// `{cache_dir}/metadata/_shard_state/in_progress_parts`: one `{instance}.inprog` document per
/// proxy, carrying the in-progress part bytes that proxy wrote.
///
/// Additive, like every other `_shard_state/` directory: a binary that does not know about it
/// writes and reads nothing here, and a rollback leaves the documents behind harmlessly.
pub(crate) fn in_progress_figures_dir(cache_dir: &std::path::Path) -> std::path::PathBuf {
    cache_dir
        .join("metadata")
        .join(IN_PROGRESS_FIGURE_SHARD_STATE_DIR)
        .join(IN_PROGRESS_FIGURES_DIR)
}

const IN_PROGRESS_FIGURE_SHARD_STATE_DIR: &str = "_shard_state";
const IN_PROGRESS_FIGURES_DIR: &str = "in_progress_parts";
const IN_PROGRESS_FIGURE_EXT: &str = "inprog";
const IN_PROGRESS_FIGURE_SCHEMA: u32 = 2;

/// How many of the publisher's own `consolidation_interval`s a published figure is trusted for.
///
/// The lease is derived per document, from the interval its writer publishes on, rather than
/// fixed: the consolidation cycle republishes on any cycle that moved the figure and refreshes an
/// unchanged document once per interval, so a healthy peer's document is one interval plus one
/// cycle's latency old. Three intervals of slack is what makes the under-count bound a bound. A
/// fixed lease could not: at the 5 s default a peer whose cycle ran long (eviction under the
/// global lock) was trusted for twelve cycles, so its new parts were in no figure for that long,
/// where before these documents existed a decider walked the volume every 5 s and saw them.
pub(crate) const PUBLISHED_FIGURE_LEASE_CYCLES: u64 = 3;

/// The floor under the derived lease.
///
/// `consolidation_interval` may be as low as 1 s. `written_at` has one-second granularity and a
/// shared volume's clocks are not that close, so a lease of a few seconds would read a healthy
/// peer as dead and put the whole fleet on the walk.
pub(crate) const PUBLISHED_FIGURE_LEASE_FLOOR_SECS: u64 = 15;

/// How old a published figure may be before a decider stops trusting it and falls back to a
/// walk: [`PUBLISHED_FIGURE_LEASE_CYCLES`] of the interval its writer published on, floored at
/// [`PUBLISHED_FIGURE_LEASE_FLOOR_SECS`].
pub(crate) fn published_figure_lease_secs(publish_interval_secs: u64) -> u64 {
    publish_interval_secs
        .saturating_mul(PUBLISHED_FIGURE_LEASE_CYCLES)
        .max(PUBLISHED_FIGURE_LEASE_FLOOR_SECS)
}

/// How old the full walk a decider composes on top of may be.
///
/// The background metrics collector walks once per `metrics.collection_interval` (60 s by
/// default), so three intervals lets one missed collection pass without dropping every decider
/// onto the 5 s walk. The base walk's age costs nothing in the unsafe direction: new parts come
/// from the growth term, and a removal the base has not seen yet only over-states `P`.
pub(crate) const PUBLISHED_WALK_BASE_MAX_AGE: std::time::Duration =
    std::time::Duration::from_secs(180);

/// How old the base walk may be for the staged fold's install guard.
///
/// The guard reads the composition's readability and nothing else, but a readability verdict is
/// still a verdict about a moment: on [`PUBLISHED_WALK_BASE_MAX_AGE`] it would be three minutes
/// old where it used to be 5 s. The guard runs on every fold install, which is once per fold
/// pass (`shard_fold_interval`, an hour by default) **and** once per staging-eviction decision,
/// which reaches the same install through `cache::CacheManager`'s fold over shards. Neither is
/// on the per-request path, and the walk it may cost is the shared one, so a burst of eviction
/// decisions inside one [`IN_PROGRESS_OBSERVATION_REUSE`] pays one walk between them. The
/// deciders on the hot path still cost none.
pub(crate) const INSTALL_GUARD_BASE_MAX_AGE: std::time::Duration = IN_PROGRESS_OBSERVATION_REUSE;

/// The oldest input a deciding reader of the staged authority figure can act on, in seconds:
/// the base walk, plus a peer's publication lag inside its lease, plus the authority memo's own
/// TTL. Stated as one figure because the three stack, and tested against the constants that
/// make it up (`the_total_staleness_of_a_composed_decision_is_the_sum_of_its_three_bounds`).
/// Test-only because nothing branches on the total; it is a statement about three constants, and
/// the test is what stops one of them moving without the statement moving with it.
#[cfg(test)]
pub(crate) fn total_composed_staleness_bound_secs(publish_interval_secs: u64) -> u64 {
    PUBLISHED_WALK_BASE_MAX_AGE
        .as_secs()
        .saturating_add(published_figure_lease_secs(publish_interval_secs))
        .saturating_add(crate::staged_fold::AUTHORITY_SNAPSHOT_TTL_SECS)
}

/// How recently an instance's journal must have been written for a missing published figure to
/// force the walk fallback. The same window `eviction_worklist`'s capability roster uses, for the
/// same reason: it is long enough that a live instance is never left out of the roster.
const JOURNAL_ROSTER_WINDOW_SECS: u64 = 3600;

/// One proxy's published in-progress part bytes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct InProgressFigureDoc {
    schema: u32,
    instance_id: String,
    /// Bytes of part records this instance wrote that are still on the volume, as of this
    /// instance's last publication. Diagnostic only; no decider reads it.
    ///
    /// A removal performed by **another** proxy (a Complete or Abort landing elsewhere, or the
    /// incomplete-upload sweep) never reaches this instance's removal hook, so the figure is
    /// brought back down by the publication-time prune
    /// ([`prune_own_in_progress_records_blocking`]): a record whose upload directory is gone is
    /// dropped from it. Between the removal and the next publication it reads high by the
    /// removed bytes.
    own_in_progress_bytes: u64,
    /// Every byte this instance has ever recorded, never decremented. **This is the term
    /// deciders read**: differenced against the same figure as it stood when the base walk
    /// began, it is that instance's growth since the walk, and a monotone counter cannot be
    /// made to under-state growth by a removal anyone performs.
    own_added_ever_bytes: u64,
    /// This instance's publication count, for an operator reading two documents in sequence.
    seq: u64,
    written_at: u64,
    /// The `consolidation_interval` this instance publishes on, in seconds. A reader derives
    /// this document's lease from it ([`published_figure_lease_secs`]) rather than from its own
    /// configuration, because the cadence that matters is the writer's.
    publish_interval_secs: u64,
    /// The writing process's startup nonce (`cache::process_nonce`), which is how two processes
    /// holding the same `{host}:{pid}` are told apart
    /// ([`detect_hostname_collision_blocking`]).
    ///
    /// Optional, and the schema is deliberately **not** bumped for it: a binary that predates
    /// the field writes a document without it and ignores it on one it reads, and this binary
    /// reads `None` as "that writer does not publish a nonce", which is no evidence either way
    /// rather than a collision. So a rolling upgrade in either direction reads every document
    /// on the volume, and a schema bump would instead have put the whole fleet on the fallback
    /// walk for the length of the rollout.
    #[serde(default)]
    nonce: Option<String>,
}

/// This process's exact in-progress part bytes for one cache directory.
///
/// Exact because this process wrote every record in it: `records` holds the `compressed_size`
/// this process last wrote for each `part{N}.json` path, so a replacement subtracts the old size
/// and a removal subtracts the whole of it.
///
/// `records` is bounded by the publication-time prune, not by
/// [`MAX_TRACKED_MULTIPART_ENTRIES`]: the cap bounds **live** entries on the volume, and a
/// record removed by another proxy never reaches this process's removal hook, so without the
/// prune its key stayed in the map for the life of the process. The prune drops a record whose
/// upload **directory** is gone, which is the shape every removal path takes
/// (`remove_dir_all`), so the bound is the records this process wrote that are live, plus those
/// whose directory removal failed and left individual `part{N}.json` paths behind. The second
/// group is bounded by the incomplete-upload sweep's `incomplete_upload_ttl` rather than by the
/// cap, and costs this process one map entry and one `symlink_metadata` per publication each.
#[derive(Default)]
struct OwnInProgressFigure {
    records: std::collections::HashMap<std::path::PathBuf, u64>,
    bytes: u64,
    added_ever: u64,
    seq: u64,
    /// `(bytes, added_ever)` as last published, and when. `None` until the first publication.
    published: Option<(u64, u64, std::time::Instant)>,
}

/// Keyed by cache directory, as [`IN_PROGRESS_OBSERVATIONS`] is.
static OWN_IN_PROGRESS_FIGURES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, OwnInProgressFigure>>,
> = std::sync::LazyLock::new(Default::default);

fn own_in_progress_figures() -> std::sync::MutexGuard<
    'static,
    std::collections::HashMap<std::path::PathBuf, OwnInProgressFigure>,
> {
    OWN_IN_PROGRESS_FIGURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Instance identity, `hostname:pid`, matching `journal_consolidator::get_instance_id` and
/// `capacity_guard`'s so an instance's documents sort together.
///
/// A restart is therefore a new identity with an empty figure, which is the whole restart rule:
/// the predecessor's bytes are counted by the full walk every decider composes on top of, and
/// its abandoned document contributes no growth because its `own_added_ever_bytes` never moves
/// again.
///
/// Two rules leave that document out of the roster, and the fast one is the one that matters
/// after a restart. [`retire_dead_own_host_in_progress_figures`] unlinks it within a few of the
/// successor's consolidation intervals of its startup — two watches of
/// [`startup_retire_watch`], so about 30 seconds at the 5 s default — on the evidence that its
/// pid is dead on this machine and its `written_at` did not move while the successor watched;
/// that rests on the unique-hostname
/// prerequisite and is off once [`crate::cache::hostname_uniqueness_holds`] is false. Failing
/// that, [`in_progress_growth_since_blocking`] leaves it out once **both** its own publication
/// and its instance's last journal are older than `JOURNAL_ROSTER_WINDOW_SECS`, and
/// [`reap_retired_in_progress_figures_blocking`] unlinks it on the same rule from the next
/// publication of any proxy. Until one of the two fires, a decider walks the volume rather than
/// trusting a figure it cannot date.
pub(crate) fn own_in_progress_instance_id() -> String {
    format!(
        "{}:{}",
        gethostname::gethostname()
            .to_string_lossy()
            .replace(['/', '\\'], "_"),
        std::process::id()
    )
}

fn in_progress_figure_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The cache directory an `mpus_in_progress/{hash}` upload directory belongs to.
fn cache_dir_of_upload_dir(multipart_dir: &std::path::Path) -> Option<&std::path::Path> {
    let parent = multipart_dir.parent()?;
    if parent.file_name()? != "mpus_in_progress" {
        return None;
    }
    parent.parent()
}

/// This process wrote `record` with `bytes` of part data.
///
/// Called after the rename that makes the record durable, so the figure never counts bytes a
/// reader could not also find on the volume. A replacement subtracts the size this process last
/// wrote for the same path; `added_ever` does not, so a replaced part is counted twice in the
/// growth term, which over-states `P` for one walk interval. That is the safe direction.
pub(crate) fn note_part_record_written(
    cache_dir: &std::path::Path,
    record: &std::path::Path,
    bytes: u64,
) {
    let mut map = own_in_progress_figures();
    let own = map.entry(cache_dir.to_path_buf()).or_default();
    if let Some(previous) = own.records.insert(record.to_path_buf(), bytes) {
        own.bytes = own.bytes.saturating_sub(previous);
    }
    own.bytes = own.bytes.saturating_add(bytes);
    own.added_ever = own.added_ever.saturating_add(bytes);
}

/// This process removed `record`.
pub(crate) fn note_part_record_removed(cache_dir: &std::path::Path, record: &std::path::Path) {
    let mut map = own_in_progress_figures();
    let Some(own) = map.get_mut(cache_dir) else {
        return;
    };
    if let Some(bytes) = own.records.remove(record) {
        own.bytes = own.bytes.saturating_sub(bytes);
    }
}

/// `(bytes, added_ever)` for this process.
pub(crate) fn own_in_progress_figure(cache_dir: &std::path::Path) -> (u64, u64) {
    own_in_progress_figures()
        .get(cache_dir)
        .map(|own| (own.bytes, own.added_ever))
        .unwrap_or((0, 0))
}

/// Drop the records this process wrote whose upload directory is no longer on the volume, and
/// subtract their bytes. Returns the bytes dropped.
///
/// This is how a removal performed by **another** proxy, or by the incomplete-upload sweep,
/// reaches this process's own figure. Those paths remove a whole upload directory
/// (`remove_dir_all`) on whichever proxy handled the Complete, the Abort or the sweep, so the
/// writer's `note_part_record_removed` never fires; its records would otherwise stay in the map,
/// and in `own_in_progress_bytes`, for the life of the process.
///
/// One `symlink_metadata` per distinct upload directory this process still has records in, on the
/// consolidation cycle's blocking task. Nothing a decider waits on: `added_ever` is monotone and
/// untouched here, so pruning changes no decider's figure (see
/// [`compose_in_progress_part_bytes`]).
fn prune_own_in_progress_records_blocking(cache_dir: &std::path::Path) -> u64 {
    let upload_dirs: Vec<std::path::PathBuf> = {
        let map = own_in_progress_figures();
        let Some(own) = map.get(cache_dir) else {
            return 0;
        };
        let mut dirs: std::collections::BTreeSet<std::path::PathBuf> = Default::default();
        for record in own.records.keys() {
            if let Some(parent) = record.parent() {
                dirs.insert(parent.to_path_buf());
            }
        }
        dirs.into_iter().collect()
    };
    let gone: std::collections::BTreeSet<std::path::PathBuf> = upload_dirs
        .into_iter()
        .filter(|dir| {
            matches!(
                std::fs::symlink_metadata(dir),
                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound
            )
        })
        .collect();
    if gone.is_empty() {
        return 0;
    }
    let mut map = own_in_progress_figures();
    let Some(own) = map.get_mut(cache_dir) else {
        return 0;
    };
    let mut dropped: u64 = 0;
    own.records.retain(|record, bytes| {
        let keep = !record
            .parent()
            .is_some_and(|parent| gone.contains(&parent.to_path_buf()));
        if !keep {
            dropped = dropped.saturating_add(*bytes);
        }
        keep
    });
    own.bytes = own.bytes.saturating_sub(dropped);
    dropped
}

/// Publish this process's figure when it has moved since the last publication, or when the
/// document is due a refresh. Returns whether a document was written.
///
/// Called from the consolidation cycle, above its idle early return, with that cycle's
/// `publish_interval` (`consolidation_interval`): a figure that moved is published within one
/// interval and an unchanged one is refreshed once per interval, so a healthy document is always
/// well inside the [`published_figure_lease_secs`] a reader derives from the same interval.
/// Blocking: the prune's `symlink_metadata` calls, then a few hundred bytes, `fsync` and a
/// rename.
pub(crate) fn publish_own_in_progress_if_due_blocking(
    cache_dir: &std::path::Path,
    publish_interval: std::time::Duration,
) -> std::io::Result<bool> {
    use std::io::Write;

    // Above the due check, so the detector runs on every cycle rather than only on the cycles
    // that move the figure. One `read_dir` of a directory holding one small document per proxy,
    // which this function is about to make anyway.
    detect_hostname_collision_blocking(cache_dir, publish_interval);
    prune_own_in_progress_records_blocking(cache_dir);
    let publish_interval_secs = publish_interval.as_secs().max(1);
    let (bytes, added_ever, seq) = {
        let mut map = own_in_progress_figures();
        let own = map.entry(cache_dir.to_path_buf()).or_default();
        let due = match own.published {
            None => true,
            Some((published_bytes, published_added_ever, at)) => {
                published_bytes != own.bytes
                    || published_added_ever != own.added_ever
                    || at.elapsed().as_secs() >= publish_interval_secs
            }
        };
        if !due {
            return Ok(false);
        }
        own.seq = own.seq.saturating_add(1);
        (own.bytes, own.added_ever, own.seq)
    };
    let instance_id = own_in_progress_instance_id();
    // Any proxy may reap, and a publication is the cheapest place for it: this process is about
    // to write into the same directory anyway.
    reap_retired_in_progress_figures_blocking(cache_dir);
    let doc = InProgressFigureDoc {
        schema: IN_PROGRESS_FIGURE_SCHEMA,
        instance_id: instance_id.clone(),
        own_in_progress_bytes: bytes,
        own_added_ever_bytes: added_ever,
        seq,
        written_at: in_progress_figure_now_secs(),
        publish_interval_secs,
        nonce: Some(crate::cache::process_nonce().to_string()),
    };
    let dir = in_progress_figures_dir(cache_dir);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.{}", instance_id, IN_PROGRESS_FIGURE_EXT));
    let body = serde_json::to_string(&doc)
        .map_err(|e| std::io::Error::other(format!("serialize in-progress figure: {e}")))?;
    // An instance-suffixed temp name, so a peer's publication cannot truncate this one's temp,
    // and so a reader skipping anything whose extension is not `inprog` can never sum a
    // half-written document.
    let tmp = path.with_extension(format!(
        "{}.tmp.{}",
        IN_PROGRESS_FIGURE_EXT,
        std::process::id()
    ));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    own_in_progress_figures()
        .entry(cache_dir.to_path_buf())
        .or_default()
        .published = Some((bytes, added_ever, std::time::Instant::now()));
    Ok(true)
}

/// Unlink the published documents of instances that have gone.
///
/// A restart gives a proxy a new `{host}:{pid}`, so its predecessor's document stays on the
/// volume with a `written_at` that never moves again. Nothing removed one before this reaper,
/// and a document that is permanently past its lease is what made every decider fall back to
/// the walk on every decision after the first restart.
///
/// A document is removed when it is not this process's, its file is older than
/// [`JOURNAL_ROSTER_WINDOW_SECS`], and its instance has written no journal inside that window
/// either, so it is not a live writer whose publishing has merely stalled. Any proxy may do it
/// and the removal costs nothing in the unsafe direction: the full walk every decider composes
/// on counts that instance's part records whoever wrote them, and an instance with no document
/// and no journal is in no roster, so it asks for no growth term.
///
/// Dated by the file's own mtime rather than by `written_at`, so a document this binary cannot
/// parse is removed on the same rule instead of forcing a walk forever. Returns how many were
/// removed. Blocking: one listing of a directory holding one document per proxy, and the
/// `_journals/` listing only when something in it is old enough to go.
fn reap_retired_in_progress_figures_blocking(cache_dir: &std::path::Path) -> usize {
    let dir = in_progress_figures_dir(cache_dir);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return 0;
    };
    let now = in_progress_figure_now_secs();
    let own = own_in_progress_instance_id();
    let mut retired: Vec<(std::path::PathBuf, String)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path
            .extension()
            .is_some_and(|ext| ext == IN_PROGRESS_FIGURE_EXT)
        {
            continue;
        }
        let Some(instance) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        if instance == own {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|age| age.as_secs());
        let Some(modified) = modified else { continue };
        if now.saturating_sub(modified) <= JOURNAL_ROSTER_WINDOW_SECS {
            continue;
        }
        retired.push((path, instance));
    }
    if retired.is_empty() {
        return 0;
    }
    let Ok(roster) = journal_roster_blocking(cache_dir, now) else {
        return 0;
    };
    let mut reaped = 0;
    for (path, instance) in retired {
        if roster.contains(&instance) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                reaped += 1;
                info!(
                    "Removed the in-progress figure of {}, whose last publication and last \
                     journal are both older than {}s: that process has gone, and the part bytes \
                     it wrote stay counted by the walk.",
                    instance, JOURNAL_ROSTER_WINDOW_SECS
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!(
                "Could not remove the retired in-progress figure {}: {}. It is left out of the \
                 composition either way.",
                path.display(),
                e
            ),
        }
    }
    reaped
}

// ---------------------------------------------------------------------------------------------
// The unique-hostname prerequisite: detection, and the startup retire it guards
// ---------------------------------------------------------------------------------------------

/// `(host, pid)` of a `{host}:{pid}` instance id, with the pid in `kill`'s range.
fn split_instance_id(instance: &str) -> Option<(&str, u32)> {
    let mut parts = instance.splitn(3, ':');
    let host = parts.next()?;
    let pid = parts
        .next()?
        .parse::<u32>()
        .ok()
        .filter(|p| *p > 0 && *p <= i32::MAX as u32)?;
    Some((host, pid))
}

/// What a detector pass found, for the tests. Production reads only the latched flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostnameCollision {
    /// Another process is publishing under this process's exact `{host}:{pid}`, with a
    /// different nonce, and did so after this process started.
    ExactIdentity,
    /// A document under this hostname, whose pid reads dead here, moved its `written_at`
    /// anyway: a live peer this machine's `kill(pid, 0)` cannot see.
    LivePeerReadingDead,
}

/// One candidate's first observation: the `written_at` its document carried when its pid first
/// read dead, and when that reading was taken.
#[derive(Clone, Copy)]
struct DeadPidObservation {
    written_at: u64,
    observed_at_secs: u64,
}

/// Monotonic seconds since this process started watching, which is what the two-observation
/// branch measures its recheck against.
///
/// Seconds off a stored `Instant` rather than the `Instant` itself because tokio's paused clock
/// does not advance a `std::time::Instant`, so the branch could only be reached by a real wait
/// of [`PUBLISHED_FIGURE_LEASE_FLOOR_SECS`]. A test moves this instead
/// ([`advance_observation_clock_for_test`]).
fn observation_now_secs() -> u64 {
    static BASE: std::sync::LazyLock<std::time::Instant> =
        std::sync::LazyLock::new(std::time::Instant::now);
    let elapsed = BASE.elapsed().as_secs();
    #[cfg(test)]
    let elapsed = elapsed
        .saturating_add(OBSERVATION_CLOCK_SKEW_SECS.load(std::sync::atomic::Ordering::Relaxed));
    elapsed
}

#[cfg(test)]
static OBSERVATION_CLOCK_SKEW_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Move the observation clock forward by `secs`, for a test holding
/// [`crate::cache::hostname_flag_test_guard`]. Forward only: an observation taken after the move
/// records the moved reading, so no earlier test's advance can make a later one's first
/// observation look old.
#[cfg(test)]
pub(crate) fn advance_observation_clock_for_test(secs: u64) {
    OBSERVATION_CLOCK_SKEW_SECS.fetch_add(secs, std::sync::atomic::Ordering::Relaxed);
}

/// How long one publisher must be watched before its silence means anything: at least one of
/// **its own** publication intervals, doubled, and never under the lease floor.
///
/// The writer's cadence rather than the reader's, for the reason
/// [`published_figure_lease_secs`] derives the lease per document: a healthy publisher at
/// `consolidation_interval: 60s` moves `written_at` once a minute, so any shorter watch reads a
/// normal gap as silence. One law, used by the detector's recheck and by the startup retire's
/// watch, so the two cannot drift apart.
pub(crate) fn publisher_watch_secs(
    their_interval_secs: u64,
    my_interval: std::time::Duration,
) -> u64 {
    their_interval_secs
        .max(my_interval.as_secs())
        .saturating_mul(2)
        .max(PUBLISHED_FIGURE_LEASE_FLOOR_SECS)
}

/// Per cache directory, the dead-reading own-host documents this process is watching.
///
/// Bounded by the number of own-host instance ids with a document on the volume, which the
/// reaper and the startup retire both shrink. An id whose document is gone is dropped on the
/// pass that no longer sees it.
static DEAD_PID_OBSERVATIONS: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<
            std::path::PathBuf,
            std::collections::HashMap<String, DeadPidObservation>,
        >,
    >,
> = std::sync::LazyLock::new(Default::default);

/// Whether `pid` is dead on this machine: `ESRCH` and nothing else, the rule
/// `journal_consolidator::pid_may_be_alive` applies.
fn own_host_pid_reads_dead(pid: u32) -> bool {
    !crate::journal_consolidator::pid_may_be_alive(pid)
}

/// Detect a proxy sharing this machine's hostname on this cache volume.
///
/// Two conditions, both read off the `.inprog` documents, because those are the records this
/// prerequisite's shortcut acts on: the detector's input is exactly the retire's input, so there
/// is no second directory to keep in step and no record that could be stale in one and fresh in
/// the other. They are also the only per-process record refreshed on a cadence a reader can
/// derive from the document itself ([`published_figure_lease_secs`]), which both conditions need.
///
/// 1. **Exact identity.** A document at this process's own `{host}:{pid}` carrying a nonce that
///    is not this process's and a `written_at` at or after this process's start. Two containers
///    sharing a hostname and both running as pid 1 produce this, and they are writing the same
///    file: the nonce is what makes it visible at all.
/// 2. **A live peer reading dead.** A document whose host is this host and whose pid is not this
///    pid, whose pid `kill(pid, 0)` reports `ESRCH`, and whose `written_at` **moved** between two
///    observations at least one of its own publication intervals apart. A process that has
///    stopped cannot move its `written_at`, so movement means the pid test is answering about
///    some other machine's or namespace's process table. The two observations are what keep a
///    predecessor that exited between its last publication and this process's first look from
///    reading as a collision: its `written_at` is final, so it never moves.
///
/// A document with no nonce (an older binary) is no evidence for condition 1 and is still
/// subject to condition 2, which reads only `written_at`.
///
/// Returns what it found, for the tests. Production ignores the return and reads
/// [`crate::cache::hostname_uniqueness_holds`]. Blocking: one `read_dir` and one small read per
/// proxy, on the consolidation cycle's blocking task. An unreadable directory says nothing and
/// raises nothing.
pub(crate) fn detect_hostname_collision_blocking(
    cache_dir: &std::path::Path,
    publish_interval: std::time::Duration,
) -> Option<HostnameCollision> {
    let Ok(docs) = read_published_figures_blocking(cache_dir) else {
        return None;
    };
    let own = own_in_progress_instance_id();
    let (my_host, my_pid) = split_instance_id(&own)?;
    if let Some(doc) = docs.get(&own) {
        #[cfg(test)]
        let nonce_readable = !DETECTOR_IGNORES_THE_NONCE;
        #[cfg(not(test))]
        let nonce_readable = true;
        if let Some(nonce) = doc.nonce.as_deref().filter(|_| nonce_readable) {
            if nonce != crate::cache::process_nonce()
                && doc.written_at >= crate::cache::process_start_secs()
            {
                // Before the counter, so a reader of the counter cannot see the collision
                // without the write-cache admission gate already being shut.
                note_identity_shared(cache_dir);
                note_distinct_hostname_collision(
                    &own,
                    &doc.instance_id,
                    "is publishing under that same identity from another process",
                );
                return Some(HostnameCollision::ExactIdentity);
            }
        }
    }
    let mut seen: std::collections::HashMap<String, DeadPidObservation> = Default::default();
    let mut found = None;
    let mut observations = DEAD_PID_OBSERVATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous = observations.entry(cache_dir.to_path_buf()).or_default();
    for (instance, doc) in &docs {
        if *instance == own {
            continue;
        }
        let Some((host, pid)) = split_instance_id(instance) else {
            continue;
        };
        if host != my_host || pid == my_pid || !own_host_pid_reads_dead(pid) {
            continue;
        }
        let recheck_secs = publisher_watch_secs(doc.publish_interval_secs, publish_interval);
        match previous.get(instance) {
            Some(first)
                if observation_now_secs().saturating_sub(first.observed_at_secs)
                    >= recheck_secs =>
            {
                if doc.written_at > first.written_at {
                    note_distinct_hostname_collision(
                        &own,
                        instance,
                        "has kept publishing after this machine reported its process id dead, so \
                         it is running somewhere this machine cannot see",
                    );
                    found = Some(HostnameCollision::LivePeerReadingDead);
                }
                // Keep the first observation either way: a document that has not moved in two
                // intervals has not moved, and re-dating it would restart the wait forever.
                seen.insert(instance.clone(), *first);
            }
            Some(first) => {
                seen.insert(instance.clone(), *first);
            }
            None => {
                #[cfg(test)]
                if DETECTOR_DECIDES_ON_ONE_OBSERVATION {
                    note_distinct_hostname_collision(
                        &own,
                        instance,
                        "has kept publishing after this machine reported its process id dead, so \
                         it is running somewhere this machine cannot see",
                    );
                    found = Some(HostnameCollision::LivePeerReadingDead);
                }
                seen.insert(
                    instance.clone(),
                    DeadPidObservation {
                        written_at: doc.written_at,
                        observed_at_secs: observation_now_secs(),
                    },
                );
            }
        }
    }
    *previous = seen;
    found
}

/// Red-side lever (4c): the detector does not read the nonce, which is the tree before
/// this unit: two processes holding one `{host}:{pid}` are indistinguishable, so the document
/// one of them is overwriting reads as its own.
#[cfg(test)]
const DETECTOR_IGNORES_THE_NONCE: bool = false;

/// Red-side lever: condition 2 decides on **one** observation, which is the
/// branch the earlier review found untested. A predecessor that exited between its last publication
/// and this process's first look then reads as a live peer.
#[cfg(test)]
const DETECTOR_DECIDES_ON_ONE_OBSERVATION: bool = false;

/// Red-side lever: the startup retire watches for the flat lease floor instead of
/// the publisher's own cadence, which is the tree before this unit. At
/// `consolidation_interval: 60s` a healthy peer is quiet across that watch as a matter of
/// routine, so the watch distinguishes nothing.
#[cfg(test)]
const RETIRE_WATCH_IS_THE_FLAT_FLOOR: bool = false;

/// Peer identities this process has already counted a collision for.
///
/// The counter answers "how many proxies am I colliding with", not "how many times have I
/// looked": the detector runs once per consolidation cycle, so counting per pass made a
/// standing misconfiguration climb for ever (UNRESOLVED 3). Bounded by the number of
/// instance ids with a document on the volume.
static NOTED_COLLISION_PEERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::BTreeSet<String>>,
> = std::sync::LazyLock::new(Default::default);

/// Count and log a collision with `theirs` the first time this process sees that peer.
fn note_distinct_hostname_collision(mine: &str, theirs: &str, how: &str) {
    let first = NOTED_COLLISION_PEERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(theirs.to_string());
    if first {
        crate::cache::note_hostname_collision(mine, theirs, how);
    }
}

/// Forget which peers have been counted, for a test holding
/// [`crate::cache::hostname_flag_test_guard`].
#[cfg(test)]
pub(crate) fn reset_noted_collision_peers_for_test() {
    NOTED_COLLISION_PEERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    IDENTITY_SHARED_VOLUMES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

/// Whether another live process is publishing under this process's **exact** `{host}:{pid}`,
/// which is condition 1 of [`detect_hostname_collision_blocking`].
///
/// Narrower than [`crate::cache::hostname_uniqueness_holds`] and latched separately because the
/// hazard is different in kind. Condition 2's peer has a different pid, so it writes different
/// per-instance files and only the own-host reap inferences are wrong. Condition 1's peer holds
/// the same identity, so it shares this process's `{instance}.ledger`, `{instance}.ledger.mark`
/// and `capacity_reservations/{instance}.resv`, and the sequence allocator those files carry is
/// per process: both processes seed from one mark, reserve blocks over each other, and issue the
/// same `Ledger_Seq` to different ranges. A credit stamped with a number a `Fold_Cut` has
/// already passed is filtered out of the pending term while no fold has read its `.meta` — the
/// counted-by-neither outcome [`crate::write_ledger::WriteLedger::append`]'s refusal rule exists
/// to prevent — and, because the entry IS in the ledger file, the repair sweep's coverage test
/// reads the range as indexed and never repairs it.
///
/// So on detection this process stops admitting write-cache entries
/// ([`SignedPutHandler::admit_pending_cache_write`]) and refuses to append to the ledger. Both
/// leave uploads and reads working: a PUT or UploadPart is forwarded and served normally,
/// uncached, counted as `hostname_not_unique` in `skipped_puts_total`.
///
/// Keyed by cache directory, as [`DEAD_PID_OBSERVATIONS`] is, because the files the two
/// processes contend over are on one volume: the detection is an observation about that volume,
/// and it is the volume's own ledger and reservations that must stop being written.
static IDENTITY_SHARED_VOLUMES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::BTreeSet<std::path::PathBuf>>,
> = std::sync::LazyLock::new(Default::default);

/// Record that another live process holds this process's identity on `cache_dir`. Latched: a
/// peer that proved it once is still out there.
fn note_identity_shared(cache_dir: &std::path::Path) {
    IDENTITY_SHARED_VOLUMES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(cache_dir.to_path_buf());
}

/// Whether this process's exact `{host}:{pid}` is shared with another live process writing to
/// `cache_dir`.
pub(crate) fn identity_shared_with_a_live_peer(cache_dir: &std::path::Path) -> bool {
    IDENTITY_SHARED_VOLUMES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(cache_dir)
}

/// Latch the exact-identity condition for one cache directory, for a test holding
/// [`crate::cache::hostname_flag_test_guard`].
#[cfg(test)]
pub(crate) fn set_identity_shared_for_test(cache_dir: &std::path::Path) {
    note_identity_shared(cache_dir);
}

/// Red-side lever (4a): the startup retire does nothing, which is the tree before this
/// unit. Every decider then walks the volume for the hour `in_progress_growth_since_blocking`
/// needs before it will leave a restarted predecessor's document out of the roster.
#[cfg(test)]
const SKIP_STARTUP_RETIRE: bool = false;

/// Red-side lever (4b): the retire unlinks a candidate whose document moved while it was
/// watched, instead of raising the collision flag and stopping.
#[cfg(test)]
const RETIRE_IGNORES_THE_FLAG: bool = false;

/// How long [`retire_dead_own_host_in_progress_figures`] watches `candidates` before unlinking
/// them: the longest [`publisher_watch_secs`] of any of them.
///
/// Derived rather than flat, because a candidate's cadence is configurable from 1 s to 60 s and
/// the watch is a claim about **its** silence. The longest, not each one's own, so one pass's
/// single wait is long enough for every document in it.
pub(crate) fn startup_retire_watch(
    candidates: &[RetireCandidate],
    my_interval: std::time::Duration,
) -> std::time::Duration {
    #[cfg(test)]
    if RETIRE_WATCH_IS_THE_FLAT_FLOOR {
        return std::time::Duration::from_secs(PUBLISHED_FIGURE_LEASE_FLOOR_SECS);
    }
    let secs = candidates
        .iter()
        .map(|c| publisher_watch_secs(c.publish_interval_secs, my_interval))
        .max()
        .unwrap_or_else(|| publisher_watch_secs(0, my_interval));
    std::time::Duration::from_secs(secs)
}

/// The watch a pass with no candidate list of its own must use: the longest
/// [`publisher_watch_secs`] over the documents that could still raise a collision on a second
/// look. Used by `JournalConsolidator::initialize` to defer its own-host reap pass behind the
/// detector.
///
/// **Zero when no document could.** Condition 2 is raised by an own-host document whose pid
/// reads dead here and whose `written_at` moves anyway, so a volume holding no such document
/// has nothing for a second observation to say: waiting would delay this host's dead-process
/// cleanup for no evidence. Condition 1 needs no second look and has already had its pass. An
/// unreadable directory is treated as holding one such document, because the reason it could
/// not be read may be the peer writing in it.
pub(crate) fn published_cadence_watch_blocking(
    cache_dir: &std::path::Path,
    my_interval: std::time::Duration,
) -> std::time::Duration {
    let own = own_in_progress_instance_id();
    let Some((my_host, my_pid)) = split_instance_id(&own) else {
        return std::time::Duration::ZERO;
    };
    let longest = match read_published_figures_blocking(cache_dir) {
        Ok(docs) => docs
            .iter()
            .filter(|(instance, _)| {
                split_instance_id(instance).is_some_and(|(host, pid)| {
                    host == my_host && pid != my_pid && own_host_pid_reads_dead(pid)
                })
            })
            .map(|(_, doc)| publisher_watch_secs(doc.publish_interval_secs, my_interval))
            .max(),
        Err(_) => Some(publisher_watch_secs(0, my_interval)),
    };
    longest.map_or(std::time::Duration::ZERO, std::time::Duration::from_secs)
}

/// A document this host's startup retire may unlink, the `written_at` it carried when the watch
/// began, and the cadence its writer publishes on, which is what the watch is derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetireCandidate {
    pub instance: String,
    pub written_at: u64,
    pub publish_interval_secs: u64,
}

/// The own-host documents whose process has stopped, as of now.
///
/// A candidate's host is this host, its pid is not this pid, its pid reads dead
/// ([`own_host_pid_reads_dead`]), and its instance has written no journal inside
/// `JOURNAL_ROSTER_WINDOW_SECS`. The journal condition is the reaper's own and it costs the
/// restart case nothing: a predecessor's journal is removed within a cycle or two of the
/// restart once it holds nothing to apply (`journal_consolidator::reap_dead_own_host_journal`,
/// and the consolidator's own delete of a fully applied journal), so a predecessor leaves the
/// roster at once. What it excludes is a peer that is still writing journals, which is the one
/// own-host id whose `own_added_ever_bytes` could still move.
///
/// Empty while [`crate::cache::hostname_uniqueness_holds`] is false, which is the whole guard:
/// the inference "this hostname means this machine, so the pid test decides" is what makes a
/// dead pid evidence that the process is gone.
pub(crate) fn own_host_retire_candidates_blocking(
    cache_dir: &std::path::Path,
) -> Vec<RetireCandidate> {
    #[cfg(test)]
    if SKIP_STARTUP_RETIRE {
        return Vec::new();
    }
    if !crate::cache::hostname_uniqueness_holds() {
        return Vec::new();
    }
    let Ok(docs) = read_published_figures_blocking(cache_dir) else {
        return Vec::new();
    };
    let own = own_in_progress_instance_id();
    let Some((my_host, my_pid)) = split_instance_id(&own) else {
        return Vec::new();
    };
    let now = in_progress_figure_now_secs();
    let Ok(journals) = journal_roster_blocking(cache_dir, now) else {
        return Vec::new();
    };
    docs.iter()
        .filter(|(instance, _)| **instance != own)
        .filter(|(instance, _)| !journals.contains(*instance))
        .filter(|(instance, _)| {
            split_instance_id(instance).is_some_and(|(host, pid)| host == my_host && pid != my_pid)
        })
        .filter(|(instance, _)| {
            split_instance_id(instance).is_some_and(|(_, pid)| own_host_pid_reads_dead(pid))
        })
        .map(|(instance, doc)| RetireCandidate {
            instance: instance.clone(),
            written_at: doc.written_at,
            publish_interval_secs: doc.publish_interval_secs,
        })
        .collect()
}

/// Unlink the candidates whose `written_at` did not move while they were watched, and raise the
/// collision flag for any that did. Returns how many were unlinked.
///
/// A candidate whose document moved is a live peer that this machine's process table cannot
/// see, which is condition 2 of [`detect_hostname_collision_blocking`] reached from the retire's
/// own evidence rather than from the detector's cycle. It is **not** retired, and the flag it
/// raises turns this retire and every other own-host shortcut off for the rest of the process.
///
/// Retiring a document is the same thing the reaper does and costs nothing in the unsafe
/// direction for a process that has stopped: the full walk every decider composes on counts that
/// instance's part records whoever wrote them, and an instance with no document and no journal
/// is in no roster, so it asks for no growth term.
pub(crate) fn retire_quiet_own_host_figures_blocking(
    cache_dir: &std::path::Path,
    candidates: &[RetireCandidate],
) -> usize {
    if candidates.is_empty() || !crate::cache::hostname_uniqueness_holds() {
        return 0;
    }
    let Ok(docs) = read_published_figures_blocking(cache_dir) else {
        return 0;
    };
    let own = own_in_progress_instance_id();
    let dir = in_progress_figures_dir(cache_dir);
    let mut retired = 0;
    for candidate in candidates {
        let Some(doc) = docs.get(&candidate.instance) else {
            // Someone else unlinked it in between, which is the same outcome.
            continue;
        };
        let moved = doc.written_at > candidate.written_at;
        #[cfg(test)]
        let moved = moved && !RETIRE_IGNORES_THE_FLAG;
        if moved {
            note_distinct_hostname_collision(
                &own,
                &candidate.instance,
                "published again while this proxy watched it, after this machine reported its \
                 process id dead",
            );
            return retired;
        }
        let path = dir.join(format!("{}.{}", candidate.instance, IN_PROGRESS_FIGURE_EXT));
        match std::fs::remove_file(&path) {
            Ok(()) => {
                retired += 1;
                info!(
                    "Removed the in-progress figure of {}, a stopped process of this host: its \
                     process id is dead here, it has written no journal in {}s, and its figure \
                     did not move while this proxy watched it. The part bytes it wrote stay \
                     counted by the walk, and deciders stop walking the volume for it.",
                    candidate.instance, JOURNAL_ROSTER_WINDOW_SECS
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!(
                "Could not remove the stopped-process in-progress figure {}: {}. It is left for \
                 the age-based reaper.",
                path.display(),
                e
            ),
        }
    }
    retired
}

/// At startup, retire this host's stopped predecessors' published figures.
///
/// Without this, every restart leaves a document whose `written_at` never moves again, and no
/// decider on any proxy will trust the composition until that document is an hour old
/// ([`in_progress_growth_since_blocking`]) — so for an hour after every deploy, every deciding
/// reader on every proxy walks `mpus_in_progress/` itself, which is what the published figures
/// exist to avoid.
///
/// Two observations [`startup_retire_watch`] apart, so a document that is still being refreshed
/// by something this machine's `kill(pid, 0)` cannot see raises the flag instead of being
/// unlinked. Runs detached: nothing waits on it, and a client request never does.
pub(crate) async fn retire_dead_own_host_in_progress_figures(
    cache_dir: &std::path::Path,
    my_interval: std::time::Duration,
) -> usize {
    let dir = cache_dir.to_path_buf();
    let Ok(candidates) =
        tokio::task::spawn_blocking(move || own_host_retire_candidates_blocking(&dir)).await
    else {
        return 0;
    };
    if candidates.is_empty() {
        return 0;
    }
    tokio::time::sleep(startup_retire_watch(&candidates, my_interval)).await;
    let dir = cache_dir.to_path_buf();
    tokio::task::spawn_blocking(move || retire_quiet_own_host_figures_blocking(&dir, &candidates))
        .await
        .unwrap_or(0)
}

/// Every published figure on the volume, keyed by instance id.
///
/// `Err` when the directory or any document cannot be read or parsed, never a partial map: a
/// missing peer is indistinguishable from a peer with no parts, and treating it as the latter is
/// the under-count this whole mechanism exists to bound. A missing directory is an empty map,
/// which is a real observation (no instance has published yet).
fn read_published_figures_blocking(
    cache_dir: &std::path::Path,
) -> std::io::Result<std::collections::BTreeMap<String, InProgressFigureDoc>> {
    let dir = in_progress_figures_dir(cache_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(e) => return Err(e),
    };
    let mut docs = std::collections::BTreeMap::new();
    for entry in entries {
        let path = entry?.path();
        if !path
            .extension()
            .is_some_and(|ext| ext == IN_PROGRESS_FIGURE_EXT)
        {
            continue;
        }
        let content = std::fs::read_to_string(&path)?;
        let doc: InProgressFigureDoc = serde_json::from_str(&content).map_err(|e| {
            std::io::Error::other(format!(
                "unparseable in-progress figure {}: {e}",
                path.display()
            ))
        })?;
        if doc.schema != IN_PROGRESS_FIGURE_SCHEMA {
            return Err(std::io::Error::other(format!(
                "unknown in-progress figure schema {} in {}",
                doc.schema,
                path.display()
            )));
        }
        docs.insert(doc.instance_id.clone(), doc);
    }
    Ok(docs)
}

/// The instance id a journal file name belongs to: `{instance}.journal`, or
/// `{instance}:{nanos}.journal` when the primary was locked. The rule `eviction_worklist` uses.
fn journal_instance_of(file_name: &str) -> Option<&str> {
    let stem = file_name.strip_suffix(".journal")?;
    if stem.is_empty() {
        return None;
    }
    if let Some((head, tail)) = stem.rsplit_once(':') {
        if head.contains(':') && !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) {
            return Some(head);
        }
    }
    Some(stem)
}

/// Instance ids with a journal written inside [`JOURNAL_ROSTER_WINDOW_SECS`].
///
/// This is the "which instances exist" roster, modelled on
/// `eviction_worklist::active_journal_instances`. It is what makes a **missing** published figure
/// detectable: a peer that has never published has no document to be stale, so without a roster
/// its parts would simply be absent from the sum.
fn journal_roster_blocking(
    cache_dir: &std::path::Path,
    now: u64,
) -> std::io::Result<std::collections::BTreeSet<String>> {
    let dir = cache_dir.join("metadata").join("_journals");
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(e) => return Err(e),
    };
    let mut roster = std::collections::BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(instance) = journal_instance_of(name) else {
            continue;
        };
        let modified = match entry.metadata().and_then(|m| m.modified()) {
            Ok(m) => m,
            // Rotated away between the listing and the stat: not an unreadable journal.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let modified_secs = modified
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(modified_secs) <= JOURNAL_ROSTER_WINDOW_SECS {
            roster.insert(instance.to_string());
        }
    }
    Ok(roster)
}

/// Each instance's `own_added_ever_bytes` at one instant: the published figure for a peer, memory
/// for this process.
pub(crate) type AddedEverSnapshot = std::collections::BTreeMap<String, u64>;

/// The snapshot a walk is dated by, taken **before** the walk lists anything.
///
/// Taken first is what makes the composition safe: a part recorded before this snapshot is on the
/// volume before the walk starts, so the walk counts it; a part recorded after it raises some
/// instance's `own_added_ever_bytes` above the snapshot, so the growth term counts it. A part
/// recorded in between is counted by both, which over-states `P` and never under-states it.
///
/// `None` when the published documents cannot be read, which makes every decider composing on
/// this walk fall back to a walk of its own rather than to a smaller figure.
fn added_ever_snapshot_blocking(cache_dir: &std::path::Path) -> Option<AddedEverSnapshot> {
    let docs = read_published_figures_blocking(cache_dir).ok()?;
    let mut snapshot: AddedEverSnapshot = docs
        .into_iter()
        .map(|(id, doc)| (id, doc.own_added_ever_bytes))
        .collect();
    let (_, added_ever) = own_in_progress_figure(cache_dir);
    snapshot.insert(own_in_progress_instance_id(), added_ever);
    Some(snapshot)
}

/// Σ over every instance of its in-progress bytes recorded since `at_start` was taken.
///
/// `Err` when any instance in the roster has no usable published figure, which the caller turns
/// into a fresh walk. The three ways that happens, all of them a peer this process cannot account
/// for incrementally: no document at all (an older binary, or a proxy that has not published
/// yet), a document past the lease its publication interval earns
/// ([`published_figure_lease_secs`]) that is either younger than
/// [`JOURNAL_ROSTER_WINDOW_SECS`] or whose instance is still in the journal roster (a proxy
/// whose publishing has stalled), and an unreadable or unknown-schema document.
///
/// There is a fourth outcome, and it is not an `Err`: an instance whose own document is older
/// than [`JOURNAL_ROSTER_WINDOW_SECS`] and which has written no journal inside that same window
/// is a process that has gone, so it is left out of the roster. It asks for no walk and
/// contributes no growth term, and its part bytes are counted by the base walk.
fn in_progress_growth_since_blocking(
    cache_dir: &std::path::Path,
    at_start: &AddedEverSnapshot,
) -> std::io::Result<u64> {
    let now = in_progress_figure_now_secs();
    let docs = read_published_figures_blocking(cache_dir)?;
    let own = own_in_progress_instance_id();
    let journals = journal_roster_blocking(cache_dir, now)?;
    let mut roster = journals.clone();
    roster.extend(docs.keys().cloned());
    roster.insert(own.clone());
    let mut growth: u64 = 0;
    for instance in &roster {
        let added_ever = if *instance == own {
            // This process knows its own figure exactly and now, so its term has no lag at all.
            own_in_progress_figure(cache_dir).1
        } else {
            let Some(doc) = docs.get(instance) else {
                return Err(std::io::Error::other(format!(
                    "instance {instance} publishes no in-progress figure, so its parts can only \
                     be counted by a walk"
                )));
            };
            let age = now.saturating_sub(doc.written_at);
            let lease = published_figure_lease_secs(doc.publish_interval_secs);
            if age > lease {
                if age > JOURNAL_ROSTER_WINDOW_SECS && !journals.contains(instance) {
                    // A retired instance, which is what every restart leaves behind: its last
                    // publication is over `JOURNAL_ROSTER_WINDOW_SECS` old and it has written no
                    // journal inside that window either, so this `{host}:{pid}` is a process
                    // that has gone and its `own_added_ever_bytes` can never move again. It is
                    // left out of the roster rather than walked for, and its bytes are not lost
                    // by that: the base walk counts the records themselves whoever wrote them,
                    // and an instance with no growth term subtracts nothing. Reading it as a
                    // stalled live peer instead is what made every decider walk on every
                    // decision for the life of the volume after the first restart. The document
                    // is removed by [`reap_retired_in_progress_figures_blocking`].
                    //
                    // Both halves of this condition are the hour and not the 15s lease, which
                    // is the reaper's own threshold. A document between the lease and the hour
                    // falls
                    // back to the walk, because "a live writer always has a journal inside the
                    // window" has two counterexamples: a peer still writing `part{N}.json`
                    // records, which is not a journal write, whose publishing has stalled, and a
                    // reader whose clock runs over an hour fast. Under the lease alone either
                    // one loses its growth term and `P` reads below the volume, the one
                    // direction this figure may not move. It costs the restart case nothing: the
                    // predecessor's `written_at` and its last journal both date from just before
                    // the restart, so they cross the hour together, and that is also when the
                    // reaper unlinks the document.
                    debug!(
                        "The in-progress figure of {instance} is {age}s old and the instance has \
                         written no journal in {JOURNAL_ROSTER_WINDOW_SECS}s, so it is a retired \
                         process: left out of the composition, with its part bytes counted by \
                         the walk."
                    );
                    continue;
                }
                return Err(std::io::Error::other(format!(
                    "instance {instance}'s in-progress figure is {age}s old, past the {lease}s \
                     lease its {}s publication interval earns, so its parts can only be counted \
                     by a walk",
                    doc.publish_interval_secs
                )));
            }
            doc.own_added_ever_bytes
        };
        let was = at_start.get(instance).copied().unwrap_or(0);
        growth = growth.saturating_add(added_ever.saturating_sub(was));
    }
    Ok(growth)
}

/// `P` for a deciding reader: a full walk plus every instance's growth since that walk began.
///
/// The composition, exactly:
///
/// ```text
/// P = walk.bytes + Σ_instances max(0, added_ever_now(i) - added_ever_when_the_walk_began(i))
/// ```
///
/// with `walk` the newest kept walk of `mpus_in_progress/` younger than
/// [`PUBLISHED_WALK_BASE_MAX_AGE`], `added_ever` read from memory for this process and from
/// `{instance}.inprog` for every peer.
///
/// # What this does and does not cost
///
/// In the steady state it starts no walk: it reads one small document per proxy and the
/// `_journals/` listing. The once-a-minute walk the metrics collector already makes is the base,
/// so the deciders that used to walk once per 5 s now walk only on the fallback.
///
/// # The under-count bound
///
/// `P` can be short only of parts a **peer** recorded after its last publication, because this
/// process's own term is read from memory at decision time and every peer's term comes from a
/// document republished on any cycle that moved it. So the bound is one `consolidation_interval`
/// of each live peer's new parts, plus one case: an instance whose own document and whose last
/// journal are both older than [`JOURNAL_ROSTER_WINDOW_SECS`] is left out of the composition, so
/// parts it recorded after the base walk began are short of `P` until the next walk, by up to
/// [`PUBLISHED_WALK_BASE_MAX_AGE`] of them. Such an instance is a process that has gone, whose
/// counter cannot move again, so the dropped term is zero unless a reader's clock is over an hour
/// fast. Every other peer this process cannot account for incrementally — no document, a binary
/// that publishes nothing, a document past its lease but inside that hour, a new pid, one that
/// cannot be parsed — forces a fresh walk rather than a smaller figure. Bytes already on the
/// volume when the base walk began are in `walk.bytes` whoever wrote them, and a removal by
/// anyone only leaves the base high.
///
/// `None` is still the UNREADABLE sentinel and still propagates: a base walk that could not read
/// a record composes to `None`, never to a growth term on its own.
pub(crate) async fn compose_in_progress_part_bytes(
    cache_dir: &std::path::Path,
    base_max_age: std::time::Duration,
) -> InProgressComposition {
    let base = last_in_progress_observation(cache_dir)
        .filter(|observation| observation.began_at.elapsed() <= base_max_age)
        // The once-a-minute reconciliation found the composition below the walk, so some
        // instance's counter has missed an update and the growth term cannot be trusted until
        // the next collection replaces the verdict.
        .filter(|_| !reconciliation_forces_walk(cache_dir));
    if let Some(base) = base {
        if let Some(at_start) = base.added_ever_at_start.clone() {
            let dir = cache_dir.to_path_buf();
            let growth = tokio::task::spawn_blocking(move || {
                in_progress_growth_since_blocking(&dir, &at_start)
            })
            .await;
            match growth {
                Ok(Ok(growth)) => {
                    return InProgressComposition {
                        bytes: base.walk.bytes.map(|p| p.saturating_add(growth)),
                        base_bytes: base.walk.bytes,
                        growth_bytes: growth,
                        base_age: base.began_at.elapsed(),
                        fell_back: false,
                    };
                }
                Ok(Err(e)) => debug!(
                    "The published in-progress figures of {} are not a complete set ({}), so this \
                     reader walks instead.",
                    cache_dir.display(),
                    e
                ),
                Err(e) => warn!(
                    "Reading the published in-progress figures of {} did not complete ({}), so \
                     this reader walks instead.",
                    cache_dir.display(),
                    e
                ),
            }
        }
    }
    let observation = observe_in_progress(cache_dir, IN_PROGRESS_OBSERVATION_REUSE).await;
    InProgressComposition {
        bytes: observation.walk.bytes,
        base_bytes: observation.walk.bytes,
        growth_bytes: 0,
        base_age: observation.began_at.elapsed(),
        fell_back: true,
    }
}

/// What the last reconciliation found: the composition a decider would have made on the walk
/// before this one, against the walk itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct InProgressReconciliation {
    /// The newest walk's own figure: the true in-progress part bytes when it began.
    pub(crate) walk_bytes: Option<u64>,
    /// What [`compose_in_progress_part_bytes`] would have returned at the moment that walk
    /// began, composing on the walk before it.
    pub(crate) composed_bytes: Option<u64>,
    /// `composed_bytes - walk_bytes`. Zero or positive in health: the composition over-counts a
    /// removal the base walk has not seen yet. Negative means some instance's `added_ever` has
    /// missed an update, which is the under-count direction, and deciders take the walk until
    /// the next collection.
    pub(crate) drift_bytes: Option<i64>,
}

/// Reconcile the newest walk against the composition the walk before it supports, and arm or
/// clear the verdict deciders consult.
///
/// Both terms come from the two kept walks and the `added_ever` snapshots they were dated by, so
/// this reads nothing from the volume and has no clock in it: `composed = previous.walk.bytes +
/// Σ_i max(0, A_i(at the newest walk) - A_i(at the previous walk))`, exactly the expression a
/// decider would have evaluated at the instant the newest walk began, against that walk's own
/// figure for the same instant. Comparing published **current** figures against the walk instead
/// would be noise: a peer's figure lags its writes by up to one publication, so a healthy busy
/// fleet would read below the walk at most collections and every decider would be pushed onto
/// the fallback.
///
/// Called once per `metrics.collection_interval`, after the collection's own walk. A verdict of
/// `true` (composition below walk) stands until the next collection replaces it.
pub(crate) fn reconcile_in_progress_figures(
    cache_dir: &std::path::Path,
) -> InProgressReconciliation {
    let mut map = IN_PROGRESS_OBSERVATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(slot) = map.get_mut(cache_dir) else {
        return Default::default();
    };
    let (Some(last), Some(previous)) = (slot.last.as_ref(), slot.previous.as_ref()) else {
        // One walk so far: nothing to reconcile against, and no verdict either way.
        return InProgressReconciliation {
            walk_bytes: slot.last.as_ref().and_then(|last| last.walk.bytes),
            ..Default::default()
        };
    };
    let walk_bytes = last.walk.bytes;
    let composed = match (
        previous.walk.bytes,
        previous.added_ever_at_start.as_ref(),
        last.added_ever_at_start.as_ref(),
    ) {
        (Some(base), Some(was), Some(now)) => {
            let mut growth: u64 = 0;
            for (instance, added_ever) in now.iter() {
                let before = was.get(instance).copied().unwrap_or(0);
                growth = growth.saturating_add(added_ever.saturating_sub(before));
            }
            Some(base.saturating_add(growth))
        }
        // An unreadable walk or unreadable documents: no verdict. Those states already put a
        // decider on the walk through the composition's own `None` and fallback rules.
        _ => None,
    };
    let drift = match (composed, walk_bytes) {
        (Some(composed), Some(walk)) => Some(
            i64::try_from(composed).unwrap_or(i64::MAX) - i64::try_from(walk).unwrap_or(i64::MAX),
        ),
        _ => None,
    };
    slot.reconciliation = Some((
        drift.is_some_and(|drift| drift < 0),
        std::time::Instant::now(),
    ));
    InProgressReconciliation {
        walk_bytes,
        composed_bytes: composed,
        drift_bytes: drift,
    }
}

/// Whether the last reconciliation found the composition below the walk, and is recent enough to
/// still stand. Older than [`PUBLISHED_WALK_BASE_MAX_AGE`] it says nothing, and a base walk that
/// old is refused anyway.
fn reconciliation_forces_walk(cache_dir: &std::path::Path) -> bool {
    IN_PROGRESS_OBSERVATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(cache_dir)
        .and_then(|slot| slot.reconciliation)
        .is_some_and(|(below, at)| below && at.elapsed() <= PUBLISHED_WALK_BASE_MAX_AGE)
}

/// Test seam: a one-shot action run between a walk's `added_ever` snapshot and the walk itself,
/// which is the only window in which the two can be told apart.
#[cfg(test)]
type BetweenSnapshotAndWalkHook = Box<dyn FnOnce() + Send + 'static>;

#[cfg(test)]
type BetweenSnapshotAndWalkHooks =
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, BetweenSnapshotAndWalkHook>>;

#[cfg(test)]
static BETWEEN_SNAPSHOT_AND_WALK_HOOKS: std::sync::LazyLock<BetweenSnapshotAndWalkHooks> =
    std::sync::LazyLock::new(Default::default);

/// Register `hook` to run once, inside the next walk of `cache_dir`, after its snapshot and
/// before it lists anything.
#[cfg(test)]
pub(crate) fn set_between_snapshot_and_walk_hook_for_tests(
    cache_dir: &std::path::Path,
    hook: BetweenSnapshotAndWalkHook,
) {
    BETWEEN_SNAPSHOT_AND_WALK_HOOKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(cache_dir.to_path_buf(), hook);
}

#[cfg(test)]
fn run_between_snapshot_and_walk_hook_for_tests(cache_dir: &std::path::Path) {
    let hook = BETWEEN_SNAPSHOT_AND_WALK_HOOKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(cache_dir);
    if let Some(hook) = hook {
        hook();
    }
}

/// Test seam: make the next walk of `cache_dir` report a `began_at` that far in the past, once.
/// It is how a test builds an in-flight walk that is already too old for the reader that joins
/// it, which two walks microseconds apart cannot otherwise be.
#[cfg(test)]
static BACKDATE_NEXT_WALK_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, std::time::Duration>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) fn backdate_next_in_progress_walk_for_tests(
    cache_dir: &std::path::Path,
    by: std::time::Duration,
) {
    BACKDATE_NEXT_WALK_FOR_TEST
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(cache_dir.to_path_buf(), by);
}

#[cfg(test)]
fn take_walk_backdate_for_test(cache_dir: &std::path::Path) -> Option<std::time::Duration> {
    BACKDATE_NEXT_WALK_FOR_TEST
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(cache_dir)
}

/// Test seam: how many records this process's own figure is holding for `cache_dir`.
#[cfg(test)]
pub(crate) fn own_in_progress_records_len_for_test(cache_dir: &std::path::Path) -> usize {
    own_in_progress_figures()
        .get(cache_dir)
        .map(|own| own.records.len())
        .unwrap_or(0)
}

/// Test seam: write a peer instance's published figure, with a chosen `written_at` so a test can
/// make it stale and a chosen publication interval so a test can set the lease it earns. No
/// production path writes another instance's document.
#[cfg(test)]
pub(crate) fn publish_peer_in_progress_figure_for_test(
    cache_dir: &std::path::Path,
    instance_id: &str,
    bytes: u64,
    added_ever: u64,
    written_at: u64,
    publish_interval_secs: u64,
) {
    let dir = in_progress_figures_dir(cache_dir);
    std::fs::create_dir_all(&dir).unwrap();
    let doc = InProgressFigureDoc {
        schema: IN_PROGRESS_FIGURE_SCHEMA,
        instance_id: instance_id.to_string(),
        own_in_progress_bytes: bytes,
        own_added_ever_bytes: added_ever,
        seq: 1,
        written_at,
        publish_interval_secs,
        // A peer's nonce, distinct from this process's, so the exact-identity condition is
        // exercised whenever a fixture publishes under this process's own id.
        nonce: Some(format!("peer-{instance_id}")),
    };
    std::fs::write(
        dir.join(format!("{}.{}", instance_id, IN_PROGRESS_FIGURE_EXT)),
        serde_json::to_string(&doc).unwrap(),
    )
    .unwrap();
}

/// What [`compose_in_progress_part_bytes`] decided, and from what.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InProgressComposition {
    /// `P`, with [`in_progress_part_bytes`]'s UNREADABLE rules.
    pub(crate) bytes: Option<u64>,
    /// The base walk's own figure.
    pub(crate) base_bytes: Option<u64>,
    /// Σ growth since the base walk began.
    pub(crate) growth_bytes: u64,
    /// How old the base walk is.
    pub(crate) base_age: std::time::Duration,
    /// Whether the published figures were not a usable set, so this reader took a walk.
    pub(crate) fell_back: bool,
}
/// Test seam: date `cache_dir`'s kept walk `by` earlier. Two decider calls in a test are
/// microseconds apart, so without this nothing can tell a decider that reuses a walk younger
/// than [`IN_PROGRESS_OBSERVATION_REUSE`] from one that reuses a walk of any age.
#[cfg(test)]
pub(crate) fn age_last_in_progress_walk_for_tests(
    cache_dir: &std::path::Path,
    by: std::time::Duration,
) {
    let mut map = IN_PROGRESS_OBSERVATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(last) = map.get_mut(cache_dir).and_then(|slot| slot.last.as_mut()) {
        last.began_at = last.began_at.checked_sub(by).unwrap_or(last.began_at);
    }
}

/// Walks of `cache_dir` started in this test binary.
#[cfg(test)]
static IN_PROGRESS_WALKS_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, u64>>,
> = std::sync::LazyLock::new(Default::default);

/// Cache directories whose walk reports a failed listing, for tests.
#[cfg(test)]
static IN_PROGRESS_LISTING_FAILS_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
fn note_in_progress_walk_for_test(cache_dir: &std::path::Path) {
    *IN_PROGRESS_WALKS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(cache_dir.to_path_buf())
        .or_insert(0) += 1;
    RECORD_READ_THREADS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(cache_dir);
}

/// The threads that read part records in the latest walk of each cache directory.
#[cfg(test)]
static RECORD_READ_THREADS_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<
            std::path::PathBuf,
            std::collections::HashSet<std::thread::ThreadId>,
        >,
    >,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
fn note_record_read_thread_for_test(cache_dir: &std::path::Path) {
    RECORD_READ_THREADS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(cache_dir.to_path_buf())
        .or_default()
        .insert(std::thread::current().id());
}

/// How many threads read part records in the latest walk of `cache_dir`.
#[cfg(test)]
fn record_read_threads_for_tests(cache_dir: &std::path::Path) -> usize {
    RECORD_READ_THREADS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(cache_dir)
        .map_or(0, |threads| threads.len())
}

#[cfg(test)]
fn in_progress_listing_fails_for_test(cache_dir: &std::path::Path) -> bool {
    IN_PROGRESS_LISTING_FAILS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .contains(cache_dir)
}

/// Walks of `cache_dir`'s `mpus_in_progress/` started so far in this test binary.
#[cfg(test)]
pub(crate) fn in_progress_walks_for_tests(cache_dir: &std::path::Path) -> u64 {
    IN_PROGRESS_WALKS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(cache_dir)
        .copied()
        .unwrap_or(0)
}

/// Make every later walk of `cache_dir` report a failed listing (`fails`), or stop doing so.
#[cfg(test)]
pub(crate) fn set_in_progress_listing_fails_for_tests(cache_dir: &std::path::Path, fails: bool) {
    let mut set = IN_PROGRESS_LISTING_FAILS_FOR_TEST
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if fails {
        set.insert(cache_dir.to_path_buf());
    } else {
        set.remove(cache_dir);
    }
}

/// Drop `cache_dir`'s kept walk, so the next reader walks.
#[cfg(test)]
pub(crate) fn forget_in_progress_observation_for_test(cache_dir: &std::path::Path) {
    if let Some(slot) = IN_PROGRESS_OBSERVATIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get_mut(cache_dir)
    {
        slot.last = None;
    }
}

/// A `part{N}.json` read for its on-disk byte figure alone, with **field presence
/// preserved**.
///
/// Deliberately not [`crate::cache_types::CachedPartInfo`]. That type declares
/// `#[serde(default)] pub compressed_size: u64`, which is correct for its own purpose — a
/// struct literal needs a value — and wrong for this one, because it maps an absent field to
/// `0`, the same value a measured zero produces. `Option<u64>` here keeps the two apart, so
/// [`in_progress_part_bytes`] can report UNREADABLE for a record it cannot measure instead of
/// silently under-counting `P`.
///
/// Every other field of the record is ignored, which is what makes this safe to keep in step
/// with `CachedPartInfo`: serde ignores unknown fields by default, so a new field on that
/// type needs no change here.
///
/// Spec: cache-eviction-at-scale §§ 16.6.2, 16.6.6.
#[derive(serde::Deserialize)]
struct PartRecordSizeProbe {
    compressed_size: Option<u64>,
}

/// Test-only observation point, invoked from [`SignedPutHandler::finalize_multipart_upload`]
/// while `upload.lock` is **still held**, as the last statement before it is dropped.
///
/// It exists so the exactly-once boundary of § 16.6.2 can be asserted as an *ordering*
/// rather than as an end state. Every end-state assertion about the part records passes
/// whether they were deleted inside the lock or by the `remove_dir_all` after it, which
/// is the entire distinction the boundary rests on.
#[cfg(test)]
type PreUnlockObserver = Box<dyn Fn(&std::path::Path) + Send + Sync>;

#[cfg(test)]
pub(crate) static PRE_UNLOCK_OBSERVER: std::sync::Mutex<Option<PreUnlockObserver>> =
    std::sync::Mutex::new(None);

/// Test-only crash injection for [`SignedPutHandler::finalize_multipart_upload`]: cache keys
/// whose completion returns between the Write_Ledger append and the `.meta` publish, leaving
/// on disk exactly what a process that died there would leave. Keyed per cache key so
/// concurrently running tests cannot trip each other.
#[cfg(test)]
pub(crate) static CRASH_BETWEEN_LEDGER_AND_PUBLISH: std::sync::Mutex<Vec<String>> =
    std::sync::Mutex::new(Vec::new());

/// Test-only injection for [`SignedPutHandler::finalize_multipart_upload`]: files written,
/// for the named cache key, right after the completion's invalidation and before its part
/// renames. Stands for a peer republishing the key's `.meta` in that window, or an
/// invalidation that failed and left it in place. Keyed per cache key, consumed on use.
#[cfg(test)]
pub(crate) type RepublishedFiles = Vec<(std::path::PathBuf, Vec<u8>)>;

#[cfg(test)]
pub(crate) static REPUBLISH_AFTER_INVALIDATION: std::sync::Mutex<Vec<(String, RepublishedFiles)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn republish_after_invalidation(cache_key: &str) {
    let files = {
        let mut pending = REPUBLISH_AFTER_INVALIDATION
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match pending.iter().position(|(k, _)| k == cache_key) {
            Some(i) => pending.remove(i).1,
            None => return,
        }
    };
    for (path, bytes) in files {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("fixture: create republished parent dir");
        }
        std::fs::write(&path, bytes).expect("fixture: write republished file");
    }
}

/// Range files `finalize_multipart_upload` has moved into `ranges/` and not yet published under
/// a `.meta`. Dropped armed, it unlinks them, so every return between the first part rename and
/// the publish leaves no `.bin` that no `.meta` names. [`Self::disarm`] once the `.meta` is
/// published (or the files were otherwise accounted for).
#[derive(Default)]
struct UnpublishedRangeFiles {
    paths: Vec<PathBuf>,
}

impl UnpublishedRangeFiles {
    fn push(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    fn disarm(&mut self) {
        self.paths.clear();
    }

    fn remove_all(&mut self) {
        for moved in self.paths.drain(..) {
            if let Err(e) = std::fs::remove_file(&moved) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        "Failed to remove moved range file of an unpublished finalization: {:?}: {}",
                        moved, e
                    );
                }
            }
        }
    }
}

impl Drop for UnpublishedRangeFiles {
    fn drop(&mut self) {
        self.remove_all();
    }
}

/// Test-only failure points in `finalize_multipart_upload` after the first part rename. The
/// two part-level points fire only once a part has moved.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalizeFault {
    RangeDirCreate,
    PartRename,
    MetaDirCreate,
    MetaTmpWrite,
    MetaRename,
}

/// Armed `(cache_key, point)` pairs, consumed on use.
#[cfg(test)]
pub(crate) static FINALIZE_FAULTS: std::sync::Mutex<Vec<(String, FinalizeFault)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn finalize_fault(cache_key: &str, point: FinalizeFault) -> Result<()> {
    let mut armed = FINALIZE_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match armed
        .iter()
        .position(|(k, p)| k == cache_key && *p == point)
    {
        Some(i) => {
            armed.remove(i);
            Err(ProxyError::CacheError(format!(
                "injected finalize failure at {:?}",
                point
            )))
        }
        None => Ok(()),
    }
}

/// Test seam: `(cache_key, snapshot)` pairs put into the RAM metadata tier just before the
/// multipart `.meta` rename, the way a reader racing the publish re-seeds it. Consumed on use.
#[cfg(test)]
pub(crate) static RESEED_BEFORE_META_RENAME: std::sync::Mutex<
    Vec<(String, crate::cache_types::NewCacheMetadata)>,
> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
async fn reseed_before_meta_rename(
    cache_key: &str,
    cache_manager: Option<&Arc<crate::cache::CacheManager>>,
) {
    let snapshot = {
        let mut armed = RESEED_BEFORE_META_RENAME
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        armed
            .iter()
            .position(|(k, _)| k == cache_key)
            .map(|i| armed.remove(i).1)
    };
    if let (Some(snapshot), Some(cache_manager)) = (snapshot, cache_manager) {
        cache_manager
            .get_metadata_cache()
            .put(
                cache_key,
                snapshot,
                crate::metadata_cache::MetadataTtls {
                    get_ttl: std::time::Duration::from_secs(3600),
                    head_ttl: std::time::Duration::from_secs(3600),
                },
            )
            .await;
    }
}

#[cfg(test)]
fn crash_between_ledger_and_publish(cache_key: &str) -> bool {
    CRASH_BETWEEN_LEDGER_AND_PUBLISH
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .any(|k| k == cache_key)
}

/// Test-only: upload staging directories whose `part2.bin` `finalize_multipart_upload` removes
/// after its part-existence check and before its renames. Stands for a reaper (or anything else)
/// deleting the staging directory in that window. Keyed per directory, consumed on use.
#[cfg(test)]
pub(crate) static REMOVE_PART_BEFORE_RENAMES: std::sync::Mutex<Vec<PathBuf>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn remove_part_before_finalize_renames(multipart_dir: &std::path::Path) {
    let hit = {
        let mut pending = REMOVE_PART_BEFORE_RENAMES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending
            .iter()
            .position(|d| d == multipart_dir)
            .map(|i| pending.remove(i))
    };
    if hit.is_some() {
        let _ = std::fs::remove_file(multipart_dir.join("part2.bin"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path_safety::is_safe_path_component;
    use tempfile::TempDir;

    /// Assemble the tracker from disk the way production does — upload-level fields
    /// from `upload.meta` (synthesised if absent) plus the per-part `part{N}.json`
    /// records.
    ///
    /// Tests used to read `upload.meta` directly and trust its `parts` array. They
    /// cannot any more: parts are no longer written into that file, because rewriting
    /// it once per part was O(n²) and serialised every part of every concurrent upload
    /// on one cross-instance lock. This helper is the supported way to ask "what parts
    /// are recorded", and it goes through the same loader the product does so a test
    /// cannot pass against a layout the product does not read.
    fn tracker_from_disk(
        multipart_dir: &std::path::Path,
        upload_id: &str,
        cache_key: &str,
    ) -> crate::cache_types::MultipartUploadTracker {
        // The upload id is passed, not read off the directory name: the name is a
        // digest of it (`mpu_upload_dir`), and several tests assert on
        // `tracker.upload_id` for staging dirs created without a
        // `CreateMultipartUpload` to write `upload.meta`.
        SignedPutHandler::load_tracker_blocking(multipart_dir, upload_id, cache_key)
            .expect("tracker should load from per-part records")
    }

    fn create_test_handler(temp_dir: &TempDir) -> SignedPutHandler {
        let compression_handler = CompressionHandler::new(1024, true);
        SignedPutHandler::new(
            temp_dir.path().to_path_buf(),
            compression_handler,
            0,
            10 * 1024 * 1024, // 10MB capacity
            None,
            10 * 1024 * 1024, // 10 MiB max complete body
            5,                // write_cache_tee_channel_depth
        )
    }

    /// while a peer holds `upload.lock`, a waiting `finalize_multipart_upload` leaves
    /// the runtime worker free, and completes once the peer releases.
    ///
    /// One worker thread, so a completion that blocks it in `flock` stops every other task.
    ///
    /// # RED SIDE
    ///
    /// With `Self::lock_upload_file(&lock_file_path).await?` restored to the blocking
    /// `open` + `fs4::FileExt::lock` on the worker, the probe task does not run within 2 s.
    #[test]
    fn a_completion_waiting_on_upload_lock_leaves_the_runtime_worker_free() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/case65-upload-lock";
        let upload_id = "test-upload-case65";
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .unwrap();
        // Every wait below is a std channel timeout on this thread, not a runtime timer: a
        // blocked worker also stops the runtime's timer, so a runtime-side timeout would hang
        // rather than fail.
        let mut handler = create_test_handler(&temp_dir);
        rt.block_on(handler.cache_upload_part(cache_key, upload_id, 1, &[1u8; 1024], "etag1"))
            .unwrap();
        let lock_path = mpu_upload_dir(temp_dir.path(), upload_id).join("upload.lock");
        let peer = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        fs4::FileExt::lock(&peer).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        rt.spawn(async move {
            let requested = vec![RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            }];
            let result = handler
                .finalize_multipart_upload(
                    cache_key,
                    upload_id,
                    "final-etag",
                    &std::collections::HashMap::new(),
                    &requested,
                )
                .await;
            let _ = done_tx.send(result.is_ok());
        });
        // Let the completion reach the lock before probing.
        std::thread::sleep(std::time::Duration::from_millis(500));
        let (probe_tx, probe_rx) = std::sync::mpsc::channel();
        rt.spawn(async move {
            let _ = probe_tx.send(42u32);
        });
        let probe = probe_rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(peer);
        let completed = done_rx.recv_timeout(std::time::Duration::from_secs(10));
        rt.shutdown_background();
        assert!(
            matches!(probe, Ok(42)),
            "WORKER BLOCKED: a task on the 1-worker runtime did not run within 2 s while a \
             completion waited on upload.lock: {:?}",
            probe
        );
        assert!(
            matches!(completed, Ok(true)),
            "the completion did not finish within 10 s of the peer releasing: {:?}",
            completed
        );
    }

    /// Mock upstream: accept one connection, drain the request (headers + exactly
    /// `body_len` body bytes), then reply `200 OK`. Modeled on the identical helper
    /// in `signed_request_proxy.rs`'s own tests.
    async fn accept_and_ok(listener: tokio::net::TcpListener, body_len: usize) {
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
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await;
        let _ = sock.flush().await;
    }

    /// Drive `body` through a real loopback HTTP/1 connection to obtain a genuine
    /// `Request<hyper::body::Incoming>` — `Incoming` has no public constructor from
    /// bytes, so every test needing one goes through a real connection (same pattern
    /// as `signed_request_proxy.rs`'s inline tests).
    /// The returned `oneshot::Sender` is a **connection keep-alive guard and must be
    /// held until the caller has finished reading the returned body.** An `Incoming`
    /// is fed by the `serve_connection` future that produced it; if that future
    /// completes, the body stops yielding. Returning the request from the service
    /// and responding immediately therefore only works while the whole body already
    /// arrived in the first read — true for a few KiB, false for anything large. At
    /// 150 MiB it is a race that loses under parallel test load, and it loses by
    /// hanging in the I/O driver forever rather than failing. The service now parks
    /// on this channel instead, so the connection keeps draining the socket for as
    /// long as the caller needs. Dropping the sender releases the connection.
    async fn incoming_upload_part_request(
        uri_path_and_query: &str,
        body: Vec<u8>,
    ) -> (
        Request<hyper::body::Incoming>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        incoming_request_with_method("PUT", uri_path_and_query, body).await
    }

    /// As [`incoming_upload_part_request`], with the method chosen by the caller.
    ///
    /// `CreateMultipartUpload` is a `POST`, and while
    /// `handle_create_multipart_upload` does not itself branch on the method, it
    /// forwards the request verbatim — so a fixture that sent `PUT` would be
    /// exercising a request shape S3 never receives.
    async fn incoming_request_with_method(
        method: &str,
        uri_path_and_query: &str,
        body: Vec<u8>,
    ) -> (
        Request<hyper::body::Incoming>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        incoming_request_with_headers(method, uri_path_and_query, body, &[]).await
    }
    /// As [`incoming_request_with_method`], also sending `headers`.
    async fn incoming_request_with_headers(
        method: &str,
        uri_path_and_query: &str,
        body: Vec<u8>,
        headers: &[(&str, &str)],
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
        let req_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(req_tx)));
        let (done_tx, done_rx) = oneshot::channel::<()>();
        let done_rx = std::sync::Arc::new(std::sync::Mutex::new(Some(done_rx)));

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let req_tx = req_tx.clone();
            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                if let Some(tx) = req_tx.lock().unwrap().take() {
                    let _ = tx.send(req);
                }
                // Taken out of the mutex synchronously so no lock is held across the
                // await below.
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
        let uri_path_and_query = uri_path_and_query.to_string();
        let method = method.to_string();
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        tokio::spawn(async move {
            use hyper_util::client::legacy::Client;
            use hyper_util::rt::TokioExecutor;
            let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
            let uri = format!("http://{}{}", addr, uri_path_and_query);
            let mut builder = Request::builder()
                .method(method.as_str())
                .uri(&uri)
                .header("content-length", body_len.to_string());
            for (name, value) in &headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            let req = builder.body(Full::new(Bytes::from(body))).unwrap();
            let _ = client.request(req).await;
        });

        (req_rx.await.unwrap(), done_tx)
    }

    /// Requirement IMA 10.8: an `UploadPart` on a cache-capacity bypass streams
    /// verbatim to the upstream under S3's 5 GiB limit.
    ///
    /// `should_cache` returns `Bypass` whenever available cache capacity is
    /// exhausted (`current_cache_usage == max_cache_capacity` here). The bypass arm
    /// is forward-only and must use the same internal streamed cap as every other
    /// PUT and UploadPart path.
    #[tokio::test]
    async fn test_upload_part_bypass_arm_streams_to_upstream() {
        let temp_dir = TempDir::new().unwrap();
        let compression_handler = CompressionHandler::new(1024, true);
        // current_cache_usage == max_cache_capacity => available capacity is zero,
        // so should_cache(Some(_ > 0)) always returns Bypass regardless of size.
        let mut handler = SignedPutHandler::new(
            temp_dir.path().to_path_buf(),
            compression_handler,
            10 * 1024 * 1024, // current_cache_usage
            10 * 1024 * 1024, // max_cache_capacity (== current usage: forces Bypass)
            None,
            10 * 1024 * 1024,
            5,
        );

        // 16 MiB crosses several network frames without the 150 MiB allocation that
        // wedged the full suite under parallel load on 2026-08-17.
        let part_size = 16 * 1024 * 1024;
        // Mock upstream S3, sized to the part body so it can drain the request.
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_and_ok(upstream_listener, part_size));

        // `_conn_keepalive` must outlive the `handle_upload_part` call below — see the
        // helper's doc comment. Dropping it early closes the connection feeding the
        // request body and starves the upstream stream.
        let (req, _conn_keepalive) = incoming_upload_part_request(
            "/test-object?uploadId=test-upload-bypass&partNumber=1",
            vec![0xABu8; part_size],
        )
        .await;

        let transport = Arc::new(UpstreamTransport {
            ip: upstream_addr.ip(),
            port: upstream_addr.port(),
            tls: None,
            validated_endpoint: None,
        });

        // Bounded so a regression in the body-feeding path fails this test instead of
        // parking the whole suite in the I/O driver with no output. 0.33s is typical.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            handler.handle_upload_part(
                req,
                "test-bucket/test-object".to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport,
                "test-upload-bypass".to_string(),
                1,
            ),
        )
        .await
        .expect("handle_upload_part did not finish within 120s — the request body most likely stopped being fed (see incoming_upload_part_request's keep-alive guard)");

        // If the cap is enforced too small, handle_upload_part rejects the body before
        // ever connecting to the mock upstream, so the upstream task never completes.
        // Bound the wait rather than hanging forever on that failure mode.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        match &result {
            Ok(response) => {
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "UploadPart on a cache-capacity bypass must stream to the upstream \
                     and succeed under S3's 5 GiB limit"
                );
            }
            Err(ProxyError::RequestBodyTooLarge {
                content_length,
                max_bytes,
            }) => {
                panic!(
                    "UploadPart incorrectly rejected with 413 on the cache-capacity bypass: \
                     content_length={:?}, max_bytes={} (expected the streamed 5 GiB cap)",
                    content_length, max_bytes
                );
            }
            Err(e) => panic!("unexpected error: {}", e),
        }
    }

    #[test]
    fn test_should_cache_with_content_length_fits() {
        let temp_dir = TempDir::new().unwrap();
        let handler = create_test_handler(&temp_dir);

        let decision = handler.should_cache(Some(1024));
        assert_eq!(decision, CacheDecision::Cache);
    }

    #[test]
    fn test_should_cache_with_content_length_exceeds() {
        let temp_dir = TempDir::new().unwrap();
        let handler = create_test_handler(&temp_dir);

        let decision = handler.should_cache(Some(20 * 1024 * 1024)); // 20MB
        match decision {
            CacheDecision::Bypass(_) => {}
            _ => panic!("Expected Bypass decision"),
        }
    }

    #[test]
    fn test_should_cache_without_content_length() {
        let temp_dir = TempDir::new().unwrap();
        let handler = create_test_handler(&temp_dir);

        let decision = handler.should_cache(None);
        assert_eq!(decision, CacheDecision::StreamWithCapacityCheck);
    }

    #[test]
    fn test_parse_upload_part_query() {
        // Valid UploadPart query
        let query = "uploadId=test-upload-123&partNumber=1";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, Some(("test-upload-123".to_string(), 1)));

        // Valid with different order
        let query = "partNumber=5&uploadId=another-upload";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, Some(("another-upload".to_string(), 5)));

        // Missing partNumber
        let query = "uploadId=test-upload-123";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);

        // Missing uploadId
        let query = "partNumber=1";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);

        // Invalid partNumber
        let query = "uploadId=test-upload-123&partNumber=invalid";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);

        // Empty query
        let query = "";
        let result = SignedPutHandler::parse_upload_part_query(query);
        assert_eq!(result, None);
    }

    #[test]
    fn test_is_complete_multipart_upload() {
        // CompleteMultipartUpload query (has uploadId but no partNumber)
        let query = "uploadId=test-upload-123";
        assert!(SignedPutHandler::is_complete_multipart_upload(query));

        // UploadPart query (has both uploadId and partNumber)
        let query = "uploadId=test-upload-123&partNumber=1";
        assert!(!SignedPutHandler::is_complete_multipart_upload(query));

        // No uploadId
        let query = "partNumber=1";
        assert!(!SignedPutHandler::is_complete_multipart_upload(query));

        // Empty query
        let query = "";
        assert!(!SignedPutHandler::is_complete_multipart_upload(query));
    }

    #[test]
    fn multipart_predicates_read_parameter_names_not_substrings() {
        assert!(query_has_param("a=1&partNumber=2", "partNumber"));
        assert!(query_has_param("uploadId", "uploadId"));
        assert!(!query_has_param("x-id=partNumber", "partNumber"));
        assert!(!query_has_param("x=uploadId", "uploadId"));
        // A value spelling partNumber must not turn a completion into an upload part.
        assert!(SignedPutHandler::is_complete_multipart_upload(
            "uploadId=u1&x-id=partNumber"
        ));
        assert!(SignedPutHandler::is_abort_multipart_upload(
            "uploadId=u1&x-id=partNumber"
        ));
        // A value spelling uploadId must not make a request a completion.
        assert!(!SignedPutHandler::is_complete_multipart_upload(
            "x-id=uploadId"
        ));
    }
    #[test]
    fn test_is_abort_multipart_upload() {
        // AbortMultipartUpload query (has uploadId but no partNumber)
        let query = "uploadId=test-upload-123";
        assert!(SignedPutHandler::is_abort_multipart_upload(query));

        // UploadPart query (has both uploadId and partNumber)
        let query = "uploadId=test-upload-123&partNumber=1";
        assert!(!SignedPutHandler::is_abort_multipart_upload(query));

        // No uploadId
        let query = "partNumber=1";
        assert!(!SignedPutHandler::is_abort_multipart_upload(query));

        // Empty query
        let query = "";
        assert!(!SignedPutHandler::is_abort_multipart_upload(query));

        // Additional parameters with uploadId (should still be true)
        let query = "uploadId=test-upload-123&other=value";
        assert!(SignedPutHandler::is_abort_multipart_upload(query));
    }

    #[test]
    fn test_extract_upload_id() {
        // Valid uploadId
        let query = "uploadId=test-upload-123&partNumber=1";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, Some("test-upload-123".to_string()));

        // uploadId only
        let query = "uploadId=another-upload";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, Some("another-upload".to_string()));

        // No uploadId
        let query = "partNumber=1";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, None);

        // Empty query
        let query = "";
        let result = SignedPutHandler::extract_upload_id(query);
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cache_upload_part() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-123";
        let part_number = 1;
        let data = b"test data for part 1";
        let etag = "test-etag-1";

        // Cache the part
        let result = handler
            .cache_upload_part(cache_key, upload_id, part_number, data, etag)
            .await;
        assert!(result.is_ok());

        // Verify upload.meta exists with tracker info
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        let tracker = tracker_from_disk(&multipart_dir, upload_id, cache_key);

        assert_eq!(tracker.upload_id, upload_id);
        assert_eq!(tracker.cache_key, cache_key);
        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.parts[0].part_number, part_number);
        assert_eq!(tracker.parts[0].size, data.len() as u64);
        assert_eq!(tracker.parts[0].etag, etag);
        assert_eq!(tracker.total_size, data.len() as u64);
    }

    /// Test that concurrent UploadPart calls for the *same* part number on the
    /// *same* upload_id cannot leave the on-disk bytes out of sync with the
    /// tracker's ETag.
    ///
    /// This reproduces the pattern a misbehaving or racing client could produce:
    /// two UploadPart requests for part N overlap in time on a shared cache
    /// volume. Without the lock covering both the file write and the tracker
    /// update, interleaved renames and tracker writes can result in a tracker
    /// that references ETag_A while the on-disk bytes are from upload B.
    ///
    /// Uses two separate SignedPutHandler instances pointing at the same cache
    /// dir — the same shape as two proxy instances sharing an EFS volume, which
    /// is where this race would realistically surface. The buffered
    /// `cache_upload_part` helper drives the same `part{N}.lock` critical section
    /// that the production streaming part sink's `finalize` uses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cache_upload_part_concurrent_same_part_keeps_file_and_tracker_consistent() {
        let temp_dir = TempDir::new().unwrap();

        let cache_key = "test-bucket/concurrent-part-object";
        let upload_id = "test-upload-concurrent";
        let part_number = 1u32;

        // Two distinct payloads with distinct ETags — the pairing must hold.
        // Using sizes above the compression threshold (1024) so both paths
        // exercise the same compression code.
        let data_a = vec![b'A'; 4096];
        let etag_a = "\"etag-a-1111111111111111111111\"";
        let data_b = vec![b'B'; 4096];
        let etag_b = "\"etag-b-2222222222222222222222\"";

        // Drive many interleavings. Each iteration creates two fresh handlers
        // on the same cache dir and races them via tokio::join!.
        for iteration in 0..16 {
            // Fresh upload_id each iteration so prior iterations can't affect
            // the outcome via stale state.
            let iter_upload_id = format!("{}-iter-{}", upload_id, iteration);

            let temp_path = temp_dir.path().to_path_buf();
            let key = cache_key.to_string();
            let upload = iter_upload_id.clone();
            let data_a_clone = data_a.clone();
            let etag_a_s = etag_a.to_string();
            let data_b_clone = data_b.clone();
            let etag_b_s = etag_b.to_string();

            let handle_a = tokio::spawn(async move {
                let compression_handler = CompressionHandler::new(1024, true);
                let mut handler = SignedPutHandler::new(
                    temp_path,
                    compression_handler,
                    0,
                    10 * 1024 * 1024,
                    None,
                    10 * 1024 * 1024,
                    5,
                );
                handler
                    .cache_upload_part(&key, &upload, part_number, &data_a_clone, &etag_a_s)
                    .await
            });

            let temp_path_b = temp_dir.path().to_path_buf();
            let key_b = cache_key.to_string();
            let upload_b = iter_upload_id.clone();
            let handle_b = tokio::spawn(async move {
                let compression_handler = CompressionHandler::new(1024, true);
                let mut handler = SignedPutHandler::new(
                    temp_path_b,
                    compression_handler,
                    0,
                    10 * 1024 * 1024,
                    None,
                    10 * 1024 * 1024,
                    5,
                );
                handler
                    .cache_upload_part(&key_b, &upload_b, part_number, &data_b_clone, &etag_b_s)
                    .await
            });

            let (res_a, res_b) = tokio::join!(handle_a, handle_b);
            res_a.expect("task A panicked").expect("upload A failed");
            res_b.expect("task B panicked").expect("upload B failed");

            // Read the final tracker state.
            let multipart_dir = mpu_upload_dir(temp_dir.path(), &iter_upload_id);
            let tracker = tracker_from_disk(&multipart_dir, &iter_upload_id, cache_key);

            assert_eq!(
                tracker.parts.len(),
                1,
                "iteration {}: tracker should have exactly one entry for the part number",
                iteration
            );
            let tracked_part = &tracker.parts[0];
            assert_eq!(tracked_part.part_number, part_number);

            // Read the on-disk part file. It was LZ4-frame-compressed by the
            // winner's call; decompress and compare to the expected raw bytes
            // that correspond to the tracked ETag. This is the core invariant:
            // whatever ETag the tracker recorded, the on-disk bytes MUST be the
            // bytes that that ETag describes.
            let part_file = multipart_dir.join(format!("part{}.bin", part_number));
            let compressed_bytes =
                std::fs::read(&part_file).expect("part file exists after writes");
            let compression_handler = CompressionHandler::new(1024, true);
            let decompressed = compression_handler
                .decompress_data(&compressed_bytes)
                .expect("part file decompresses (frame checksum verifies)");

            let expected_bytes = if tracked_part.etag == etag_a {
                &data_a
            } else if tracked_part.etag == etag_b {
                &data_b
            } else {
                panic!(
                    "iteration {}: tracker has unexpected etag {:?}",
                    iteration, tracked_part.etag
                );
            };

            assert_eq!(
                decompressed.len(),
                expected_bytes.len(),
                "iteration {}: on-disk decompressed size must match the ETag recorded in the tracker",
                iteration
            );
            assert_eq!(
                &decompressed, expected_bytes,
                "iteration {}: on-disk bytes must match the payload for the tracker's ETag",
                iteration
            );
            assert_eq!(
                tracked_part.size,
                expected_bytes.len() as u64,
                "iteration {}: tracker-recorded size must match payload size",
                iteration
            );
        }
    }

    #[tokio::test]
    async fn test_cache_multiple_upload_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-456";

        // Cache multiple parts
        for part_num in 1..=3 {
            let data = format!("test data for part {}", part_num);
            let etag = format!("test-etag-{}", part_num);

            let result = handler
                .cache_upload_part(cache_key, upload_id, part_num, data.as_bytes(), &etag)
                .await;
            assert!(result.is_ok());
        }

        // Verify upload.meta exists with all parts
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        let tracker = tracker_from_disk(&multipart_dir, upload_id, cache_key);

        assert_eq!(tracker.upload_id, upload_id);
        assert_eq!(tracker.cache_key, cache_key);
        assert_eq!(tracker.parts.len(), 3);

        // Each part has data like "test data for part N" which is 20 bytes
        // Total: 60 bytes
        assert_eq!(tracker.total_size, 60);
    }

    #[tokio::test]
    async fn test_cleanup_multipart_upload() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-cleanup";

        // Cache multiple parts first
        for part_num in 1..=2 {
            let data = format!("test data for part {}", part_num);
            let etag = format!("test-etag-{}", part_num);

            let result = handler
                .cache_upload_part(cache_key, upload_id, part_num, data.as_bytes(), &etag)
                .await;
            assert!(result.is_ok());
        }

        // Verify parts exist before cleanup
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        let tracker = tracker_from_disk(&multipart_dir, upload_id, cache_key);

        let mut part_files_exist = 0;
        for part_info in &tracker.parts {
            let part_file = multipart_dir.join(format!("part{}.bin", part_info.part_number));
            if part_file.exists() {
                part_files_exist += 1;
            }
        }
        assert_eq!(part_files_exist, 2);

        // Cleanup the multipart upload
        let result = handler.cleanup_multipart_upload(upload_id).await;
        assert!(result.is_ok());

        // Verify multipart directory is gone (all parts removed with it)
        assert!(!multipart_dir.exists());
    }

    #[tokio::test]
    async fn test_finalize_multipart_upload_with_missing_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-missing-parts";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache only part 1, but not part 2
        let data1 = b"test data for part 1";
        let etag1 = "test-etag-1";
        let result = handler
            .cache_upload_part(cache_key, upload_id, 1, data1, etag1)
            .await;
        assert!(result.is_ok());

        // Record part 2 WITHOUT creating its data file, simulating a part that another
        // proxy instance recorded but whose bytes this instance cannot see. Written as
        // its own `part2.json` record, which is where part state now lives.
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        let part2_info = crate::cache_types::CachedPartInfo {
            part_number: 2,
            size: 20,
            // No `part2.bin` exists in this fixture, so there is no on-disk length to
            // record. 0 is the honest value and is what the product writes when the
            // stat fails.
            compressed_size: 0,
            etag: "test-etag-2".to_string(),
            compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
            file_identity: None,
        };
        SignedPutHandler::record_part_blocking(&multipart_dir, 2, &part2_info)
            .expect("record part 2 without its data file");

        // Verify part 1 file exists in upload dir but part 2 doesn't
        let part1_file = multipart_dir.join("part1.bin");
        assert!(part1_file.exists());

        let part2_file = multipart_dir.join("part2.bin");
        assert!(!part2_file.exists());

        // Both parts are in the tracker, so the part-record wait is satisfied and
        // the missing-part guard is reached on the strength of part2.bin being
        // absent from disk — which is the case this test is about. Previously this
        // passed `None`, which derived the requested set from the tracker itself;
        // that path is gone because it also disabled the guards.
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: etag1.to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "test-etag-2".to_string(),
            },
        ];

        // Attempt to finalize - should skip caching and clean up
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed (not fail the operation)
        assert!(result.is_ok());

        // Verify cleanup occurred
        assert!(!multipart_dir.exists()); // Multipart directory should be gone

        // Verify no object metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(!metadata_file.exists());
    }

    #[tokio::test]
    async fn test_finalize_multipart_upload_with_missing_directory() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "test-upload-no-directory";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Don't create any multipart directory or parts
        // This simulates CompleteMultipartUpload succeeding on S3 but no local state

        // Attempt to finalize - should skip caching gracefully. The requested part
        // list is irrelevant here: the missing directory is checked first.
        let result = handler
            .finalize_multipart_upload(cache_key, upload_id, etag, &response_headers, &[])
            .await;

        // Should succeed (not fail the operation)
        assert!(result.is_ok());

        // Verify no object metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(!metadata_file.exists());
    }

    // ============================================================================
    // Streamed-part → finalize contract (streaming-write-path Task 6.2)
    //
    // Task 6.1 converted `handle_upload_part` to stream the part body and stage it
    // via the streaming part path (`open_multipart_part_sink` →
    // `MultipartPartSink::write` → `finalize_and_record_cached_part`), recording the
    // part in the SAME `upload.meta` tracker schema (`MultipartUploadTracker` +
    // `CachedPartInfo`) and the SAME `mpus_in_progress/{upload_id}/part{N}.bin`
    // location the buffered `cache_upload_part` uses. These tests prove
    // `finalize_multipart_upload` reads streamed parts identically to buffered
    // parts: retain-on-success, and cleanup (no cache entry) on a missing requested
    // part or an ETag mismatch. The buffered-staging variants of these gates are
    // covered above; these lock in the streamed-staging path end-to-end.
    // ============================================================================

    /// Stage an `UploadPart` body through the **streaming** part path, exactly as
    /// `run_streaming_part_cache_write` does on S3 success: open the part sink,
    /// write the (decoded) object bytes, then `finalize_and_record_cached_part`
    /// under `part{N}.lock`. Writes `part{N}.bin` and the part's `part{N}.json`
    /// record in the same on-disk shape as the buffered `cache_upload_part`.
    async fn stage_streamed_part(
        cache_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        etag: &str,
    ) {
        let cache_mgr = crate::cache::CacheManager::new(
            cache_dir.to_path_buf(),
            false, // ram_cache_enabled
            0,     // max_ram_cache_size
            100,   // compression_threshold
            false, // compression_enabled
        );
        let mut sink = cache_mgr
            .open_multipart_part_sink(cache_key, upload_id, part_number, Some(data.len() as u64))
            .await
            .expect("open streaming part sink")
            .expect("the capacity guard admits with an unconfigured floor");
        sink.write(data)
            .expect("write part bytes to streaming sink");
        SignedPutHandler::finalize_and_record_cached_part(
            cache_dir,
            cache_key,
            upload_id,
            part_number,
            etag,
            sink,
            None,
        )
        .await
        .expect("finalize + record streamed part");
    }

    /// Streamed parts that satisfy every gate (S3 success + all requested parts
    /// cached + every ETag matches) are retained: the object `.meta` is created
    /// with the correct cumulative byte offsets and the in-progress dir is removed.
    #[tokio::test]
    async fn test_finalize_multipart_upload_streamed_parts_retained_on_success() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/streamed-contiguous";
        let upload_id = "streamed-upload-contiguous";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Stage three parts via the STREAMING path (not cache_upload_part).
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];
        stage_streamed_part(cache_dir, cache_key, upload_id, 1, &part1_data, "etag1").await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 2, &part2_data, "etag2").await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 3, &part3_data, "etag3").await;

        // The tracker written by the streaming path must be byte-schema-compatible
        // with what finalize reads.
        let multipart_dir = mpu_upload_dir(cache_dir, upload_id);
        let tracker = tracker_from_disk(&multipart_dir, upload_id, cache_key);
        assert_eq!(tracker.parts.len(), 3);
        assert_eq!(tracker.total_size, 1024 + 2048 + 512);

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        // Retain: object metadata created with correct cumulative ranges.
        let metadata_dir = cache_dir.join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            metadata_file.exists(),
            "object .meta should be created for fully-cached streamed parts"
        );
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert_eq!(metadata.object_metadata.parts_count, Some(3));
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(1024, 3071))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(3072, 3583))
        );
        assert_eq!(metadata.object_metadata.content_length, 1024 + 2048 + 512);

        // In-progress dir cleaned up after finalization.
        assert!(
            !multipart_dir.exists(),
            "in-progress dir should be removed after finalization"
        );
    }

    /// A requested part that was never streamed/cached locally fails the
    /// "every requested part cached" gate: finalize writes no cache entry.
    ///
    /// It also no longer DELETES the staging directory, and this test asserts
    /// that survival deliberately. Deleting it here is what produced the
    /// ENOENT/ESTALE error cascade: the parts being declared missing may still
    /// be mid-write, on this instance or another, and `remove_dir_all` pulls
    /// the directory (including `upload.lock`) out from under them. Staging is
    /// now left to the TTL sweep. The assertion is inverted from what it
    /// originally checked, which is the point — the old expectation encoded the
    /// behaviour that caused the cascade.
    ///
    /// Note this test necessarily takes `MULTIPART_COMPLETE_CACHE_WAIT` to run:
    /// part 2 never lands in the tracker, so the bounded wait runs to its
    /// deadline before the missing-part guard is reached. That is the wait doing
    /// its job, not a slow test.
    #[tokio::test]
    async fn test_finalize_multipart_upload_streamed_parts_cleanup_on_missing_part() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/streamed-missing";
        let upload_id = "streamed-upload-missing";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Only part 1 is streamed; part 2 is requested but never cached.
        let part1_data = vec![1u8; 1024];
        stage_streamed_part(cache_dir, cache_key, upload_id, 1, &part1_data, "etag1").await;

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
        ];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        // No object metadata: the missing part means nothing is cached.
        let metadata_dir = cache_dir.join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            !metadata_file.exists(),
            "no cache entry when a requested streamed part is missing"
        );
        // ...but the staging directory SURVIVES. A part this instance is
        // declaring missing may still be mid-write elsewhere, so deleting the
        // directory it is writing into is what produced the ENOENT/ESTALE
        // cascade. The TTL sweep reaps it instead.
        assert!(
            mpu_upload_dir(cache_dir, upload_id).exists(),
            "staging dir must be left for the TTL sweep, not deleted out from \
             under part tasks that may still be writing into it"
        );
    }

    // =====================================================================
    // Phase 2 — the truncation path (Requirement 5.1, 5.2)
    //
    // Both tests below were written AFTER the fix they cover, which is worth
    // recording rather than hiding. Their red side was recovered by temporarily
    // reverting the two Phase 2 changes. One caveat on that revert: the fix
    // removes the defective argument from the signature, so the reverted call
    // site necessarily differs by that argument and the reproduction is not
    // byte-identical to the original defect.
    // =====================================================================

    /// Requirement 5.1 — a `CompleteMultipartUpload` whose XML does not parse
    /// MUST NOT finalize the cache.
    ///
    /// Two halves, because the requirement spans a parse and a decision:
    ///
    /// (a) the parser reports `Err` for malformed part XML, which is what
    ///     drives the caller's skip branch; and
    /// (b) the requested part list is AUTHORITATIVE — finalizing against an
    ///     empty list writes nothing even though the tracker holds a complete,
    ///     healthy three-part upload.
    ///
    /// Half (b) is the one with teeth. The removed `None` fallback derived the
    /// requested set from the tracker itself, so on a parse failure the filter
    /// became a no-op and every guard downstream of it was vacuous — a
    /// partially recorded upload finalized as a short object. With the list
    /// authoritative, "no parts were requested" can only ever mean "cache
    /// nothing".
    #[tokio::test]
    async fn unparseable_complete_body_does_not_finalize_the_cache() {
        // (a) Malformed part XML is an Err, not a silently empty list. This is
        // the branch that must lead to skipping finalization.
        let malformed = br#"<CompleteMultipartUpload>
            <Part><PartNumber>not-a-number</PartNumber><ETag>"e1"</ETag></Part>
        </CompleteMultipartUpload>"#;
        assert!(
            parse_complete_mpu_request(malformed).is_err(),
            "malformed PartNumber must be reported as a parse error, since that \
             is what makes the caller skip cache finalization"
        );

        // (b) A complete, healthy tracker plus an EMPTY requested list must
        // still cache nothing.
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/unparseable-complete";
        let upload_id = "upload-unparseable-complete";
        let response_headers = std::collections::HashMap::new();

        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &vec![1u8; 1024],
            "etag1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            2,
            &vec![2u8; 2048],
            "etag2",
        )
        .await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 3, &vec![3u8; 512], "etag3").await;

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "\"abc123-3\"",
                &response_headers,
                &[], // what an unparseable body morally yields: nothing requested
            )
            .await
            .unwrap();

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        assert!(
            !metadata_file.exists(),
            "an empty requested-part list must cache nothing, even with a fully \
             populated tracker — otherwise the tracker's own contents are acting \
             as a fallback and every guard downstream of the filter is vacuous"
        );
    }

    /// Requirement 5.2 — a tracker holding a SUBSET of the requested parts MUST
    /// NOT produce object metadata whose `content_length` is the subset sum.
    ///
    /// This is the truncation signature itself. Offsets and `content_length` are
    /// derived purely by summing the tracker's part sizes, so a tracker holding
    /// two parts of a three-part upload yields entirely self-consistent metadata
    /// that describes a SHORTER object — and a later GET then serves the wrong
    /// length and the wrong bytes with a success status.
    ///
    /// The assertion is deliberately stronger than "no `.meta` exists": it also
    /// names the specific wrong value (the 3,072-byte two-part sum against a
    /// 3,584-byte object), so a future change that writes metadata here fails
    /// with a message saying what was truncated rather than just that a file
    /// appeared.
    ///
    /// The requested list is EMPTY rather than naming all three parts, and that
    /// choice is what gives the test a reachable red side. A non-empty list
    /// naming part 3 is caught by the pre-existing missing-parts guard even
    /// without this spec's changes, so it would pass either way. An empty list is
    /// the shape an unparseable Complete body produces, and it is where the
    /// removed "use all cached parts" fallback did its damage: it substituted the
    /// tracker's own two parts for the request's three and wrote metadata for a
    /// 3,072-byte object.
    #[tokio::test]
    async fn tracker_holding_a_subset_does_not_finalize_a_short_object() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/subset-truncation";
        let upload_id = "upload-subset-truncation";
        let response_headers = std::collections::HashMap::new();

        // Only parts 1 and 2 land locally; part 3 never does.
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &vec![1u8; 1024],
            "etag1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            2,
            &vec![2u8; 2048],
            "etag2",
        )
        .await;

        // No parts requested, and S3's ETag says the real object has three.
        handler
            .finalize_multipart_upload(cache_key, upload_id, "\"abc123-3\"", &response_headers, &[])
            .await
            .unwrap();

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();

        if metadata_file.exists() {
            let metadata: crate::cache_types::NewCacheMetadata =
                serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
            panic!(
                "a tracker holding 2 of 3 requested parts finalized anyway, \
                 producing metadata for a SHORTER object: content_length={} \
                 (the two-part subset sum) against a real object of 3584 bytes. \
                 A later GET would serve {} bytes with HTTP 200.",
                metadata.object_metadata.content_length, metadata.object_metadata.content_length
            );
        }
    }

    // =====================================================================
    // Phase 3 — the lifecycle race (Phase 1 task 3)
    // =====================================================================

    /// The race this spec exists for: `CompleteMultipartUpload` arrives while
    /// per-part cache tasks are still running, and the object must still be
    /// cached with the FULL part set.
    ///
    /// Shape, mirroring the measured production sequence rather than a
    /// convenient one: part 1 has landed (so the staging directory and tracker
    /// exist, as they do by Complete time in the real flow), parts 2 and 3 are
    /// still in flight on their own tasks, and Complete runs concurrently with
    /// them. Before the fix, Complete read a tracker holding only part 1,
    /// declared 2 and 3 "not cached locally", and cached nothing — that is the
    /// red side, recovered by neutralising `await_tracker_parts`.
    ///
    /// The staggered delays are load-bearing. Sequential staging lets each part
    /// task finish before the next request, so the lag Complete races against
    /// never builds and the test passes with or without the fix — the same trap
    /// recorded for the fleet probe, where sequential `s3api upload-part` calls
    /// could not reproduce the race either.
    #[tokio::test(flavor = "multi_thread")]
    async fn complete_arriving_before_part_tasks_finish_still_caches_every_part() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path().to_path_buf();

        let cache_key = "test-bucket/complete-races-parts";
        let upload_id = "upload-complete-races-parts";
        let response_headers = std::collections::HashMap::new();

        // Part 1 is already recorded, exactly as in the measured case where
        // Complete arrived 147 ms after the first part was recorded.
        stage_streamed_part(
            &cache_dir,
            cache_key,
            upload_id,
            1,
            &vec![1u8; 1024],
            "etag1",
        )
        .await;

        // Parts 2 and 3 are still in flight, landing after Complete has begun.
        let mut stragglers = Vec::new();
        for (part_number, size, etag, delay_ms) in [
            (2u32, 2048usize, "etag2", 150u64),
            (3u32, 512usize, "etag3", 400u64),
        ] {
            let cache_dir = cache_dir.clone();
            stragglers.push(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                stage_streamed_part(
                    &cache_dir,
                    cache_key,
                    upload_id,
                    part_number,
                    &vec![part_number as u8; size],
                    etag,
                )
                .await;
            }));
        }

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "\"abc123-3\"",
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        for straggler in stragglers {
            straggler.await.expect("straggler part task panicked");
        }

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        assert!(
            metadata_file.exists(),
            "Complete raced the still-running part tasks and cached NOTHING. \
             This is the defect: it read a tracker holding only part 1 and \
             declared parts 2 and 3 missing, when both landed milliseconds later."
        );

        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert_eq!(
            metadata.object_metadata.parts_count,
            Some(3),
            "the cached object must carry the full part set, not the subset that \
             happened to have landed when Complete looked"
        );
        assert_eq!(
            metadata.object_metadata.content_length,
            1024 + 2048 + 512,
            "cached content_length must be the whole object, not a subset sum"
        );
    }

    /// A streamed part whose tracker ETag disagrees with the requested ETag fails
    /// the "every ETag matches" gate: finalize cleans up and writes no cache entry.
    #[tokio::test]
    async fn test_finalize_multipart_upload_streamed_parts_cleanup_on_etag_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/streamed-etag-mismatch";
        let upload_id = "streamed-upload-etag-mismatch";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Stage part 1 with the ETag S3 returned for the streamed bytes.
        let part1_data = vec![1u8; 1024];
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &part1_data,
            "\"streamed-etag\"",
        )
        .await;

        // The completion request claims a DIFFERENT ETag for part 1.
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "\"different-etag\"".to_string(),
        }];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        // Cleanup: no object metadata, in-progress dir removed.
        let metadata_dir = cache_dir.join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            !metadata_file.exists(),
            "no cache entry on streamed-part ETag mismatch"
        );
        assert!(
            !mpu_upload_dir(cache_dir, upload_id).exists(),
            "in-progress dir should be cleaned up on ETag mismatch"
        );
    }

    // ============================================================================
    // Unit Tests for CompleteMultipartUpload XML Parsing
    // ============================================================================

    /// Test parsing valid CompleteMultipartUpload XML with multiple parts
    /// Requirements: 4.1, 4.2
    #[test]
    fn test_parse_complete_mpu_request_valid_xml() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
  </Part>
  <Part>
    <PartNumber>3</PartNumber>
    <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let parts = parse_complete_mpu_request(xml).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"a54357aff0632cce46d942af68356b38\"");
        assert_eq!(parts[1].part_number, 3);
        assert_eq!(parts[1].etag, "\"0c78aef83f66abc1fa1e8477f296d394\"");
    }

    /// Test parsing XML with single part
    /// Requirements: 4.1, 4.2
    #[test]
    fn test_parse_complete_mpu_request_single_part() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"abc123"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let parts = parse_complete_mpu_request(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"abc123\"");
    }

    /// Test parsing empty body returns empty list
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_empty_body() {
        let parts = parse_complete_mpu_request(b"").unwrap();
        assert!(parts.is_empty());
    }

    /// Test parsing XML with no Part elements returns empty list
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_no_parts() {
        let xml = b"<CompleteMultipartUpload></CompleteMultipartUpload>";
        let parts = parse_complete_mpu_request(xml).unwrap();
        assert!(parts.is_empty());
    }

    /// Test parsing malformed XML with missing PartNumber
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_missing_part_number() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <ETag>"abc123"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let result = parse_complete_mpu_request(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test parsing malformed XML with missing ETag
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_missing_etag() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
  </Part>
</CompleteMultipartUpload>"#;

        let result = parse_complete_mpu_request(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test parsing XML with invalid PartNumber (non-numeric)
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_invalid_part_number() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>abc</PartNumber>
    <ETag>"abc123"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let result = parse_complete_mpu_request(xml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test parsing invalid UTF-8 body
    /// Requirements: 4.3
    #[test]
    fn test_parse_complete_mpu_request_invalid_utf8() {
        let invalid_utf8 = vec![0xFF, 0xFE, 0x00, 0x01];
        let result = parse_complete_mpu_request(&invalid_utf8);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ProxyError::InvalidRequest(_)));
    }

    /// Test extract_xml_value helper with valid input
    /// Requirements: 4.2
    #[test]
    fn test_extract_xml_value_valid() {
        let xml = "<PartNumber>42</PartNumber><ETag>\"abc\"</ETag>";
        assert_eq!(extract_xml_value(xml, "PartNumber").unwrap(), "42");
        assert_eq!(extract_xml_value(xml, "ETag").unwrap(), "\"abc\"");
    }

    /// Test extract_xml_value with whitespace around value
    /// Requirements: 4.2
    #[test]
    fn test_extract_xml_value_with_whitespace() {
        let xml = "<PartNumber>  42  </PartNumber>";
        assert_eq!(extract_xml_value(xml, "PartNumber").unwrap(), "42");
    }

    /// Test extract_xml_value with missing tag
    /// Requirements: 4.2
    #[test]
    fn test_extract_xml_value_missing_tag() {
        let xml = "<PartNumber>42</PartNumber>";
        let result = extract_xml_value(xml, "ETag");
        assert!(result.is_err());
    }

    /// Test parsing XML with parts in non-sequential order
    /// Requirements: 4.1, 4.2
    #[test]
    fn test_parse_complete_mpu_request_non_sequential_parts() {
        let xml = br#"<CompleteMultipartUpload>
  <Part>
    <PartNumber>5</PartNumber>
    <ETag>"etag5"</ETag>
  </Part>
  <Part>
    <PartNumber>2</PartNumber>
    <ETag>"etag2"</ETag>
  </Part>
  <Part>
    <PartNumber>8</PartNumber>
    <ETag>"etag8"</ETag>
  </Part>
</CompleteMultipartUpload>"#;

        let parts = parse_complete_mpu_request(xml).unwrap();
        assert_eq!(parts.len(), 3);
        // Parts should be in the order they appear in the XML
        assert_eq!(parts[0].part_number, 5);
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[2].part_number, 8);
    }

    // ============================================================================
    // Unit Tests for ETag Normalization and Validation
    // ============================================================================

    /// One balanced pair of quotes on either side is tolerated.
    /// Requirements: 9.1
    #[test]
    fn test_part_etags_match_with_quotes() {
        assert!(part_etags_match("\"abc123\"", "\"abc123\""));
        assert!(part_etags_match("\"abc123\"", "abc123"));
        assert!(part_etags_match("abc123", "\"abc123\""));
        assert!(part_etags_match(
            "\"a54357aff0632cce46d942af68356b38\"",
            "a54357aff0632cce46d942af68356b38"
        ));
    }

    /// Unquoted ETags compare on their own bytes, and differing content is rejected.
    /// Requirements: 9.1
    #[test]
    fn test_part_etags_match_without_quotes() {
        assert!(part_etags_match("abc123", "abc123"));
        assert!(part_etags_match(
            "a54357aff0632cce46d942af68356b38",
            "a54357aff0632cce46d942af68356b38"
        ));
        assert!(!part_etags_match("abc123", "abc124"));
        assert!(!part_etags_match(
            "\"a54357aff0632cce46d942af68356b38\"",
            "\"b54357aff0632cce46d942af68356b38\""
        ));
    }

    /// The cases the retired `normalize_etag` got WRONG. It was `trim_matches('"')`, so
    /// every negative assertion below would have FAILED against it: it mapped `""`, `"`
    /// and the empty string all to empty (making them equal to one another) and stripped
    /// unbalanced quotes (making `"abc` equal to `abc`). Both shapes are reachable — see
    /// `part_etags_match`'s own doc comment — and equality here admits an unverified
    /// multipart finalization into the cache.
    /// Requirements: 9.1
    #[test]
    fn test_part_etags_match_refuses_empty_and_lopsided() {
        // Empty after trimming, on either side or both: never a match.
        assert!(!part_etags_match("", ""));
        assert!(!part_etags_match("\"\"", ""));
        assert!(!part_etags_match("", "\"\""));
        assert!(!part_etags_match("\"\"", "\"\""));
        assert!(!part_etags_match("\"", ""));
        assert!(!part_etags_match("\"", "\""));
        assert!(!part_etags_match("", "abc123"));
        assert!(!part_etags_match("abc123", ""));
        assert!(!part_etags_match("\"\"", "abc123"));
        // Lopsided quoting is a different validator, not the same one.
        assert!(!part_etags_match("\"abc", "abc"));
        assert!(!part_etags_match("abc", "abc\""));
        assert!(!part_etags_match("\"\"abc\"\"", "abc"));
        // A genuine match still matches, so this is not a blanket rejection.
        assert!(part_etags_match("\"abc\"", "abc"));
    }

    /// Test ETag validation skips cache finalization on mismatch
    /// Requirements: 9.1, 9.2, 9.3, 9.4
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-mismatch";
        let upload_id = "test-upload-etag-mismatch";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Create multipart directory and upload.meta
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        tokio::fs::create_dir_all(&multipart_dir).await.unwrap();

        // Cache a part with one ETag
        let part_data = vec![0u8; 1024];
        let cached_etag = "\"cached-etag-abc123\"";
        handler
            .cache_upload_part(cache_key, upload_id, 1, &part_data, cached_etag)
            .await
            .unwrap();

        // Create requested parts with a DIFFERENT ETag (mismatch)
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "\"different-etag-xyz789\"".to_string(),
        }];

        // Attempt to finalize - should skip caching due to ETag mismatch
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed (not fail the operation - Requirement 9.4)
        assert!(result.is_ok());

        // Verify cleanup occurred (Requirement 9.2)
        assert!(!multipart_dir.exists());

        // Verify no object metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(!metadata_file.exists());
    }

    /// Test ETag validation succeeds when ETags match (with quotes normalization)
    /// Requirements: 9.1, 9.2
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_match_with_quotes() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-match";
        let upload_id = "test-upload-etag-match";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Create multipart directory
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        tokio::fs::create_dir_all(&multipart_dir).await.unwrap();

        // Cache a part with quoted ETag
        let part_data = vec![0u8; 1024];
        let cached_etag = "\"abc123\"";
        handler
            .cache_upload_part(cache_key, upload_id, 1, &part_data, cached_etag)
            .await
            .unwrap();

        // Create requested parts with same ETag (also quoted - should match after normalization)
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "\"abc123\"".to_string(),
        }];

        // Attempt to finalize - should succeed since ETags match
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed
        assert!(result.is_ok());

        // Verify metadata was created (cache finalization succeeded)
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists());
    }

    /// Test ETag validation handles mixed quote formats
    /// Requirements: 9.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_match_mixed_quotes() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-mixed";
        let upload_id = "test-upload-etag-mixed";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Create multipart directory
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        tokio::fs::create_dir_all(&multipart_dir).await.unwrap();

        // Cache a part with quoted ETag
        let part_data = vec![0u8; 1024];
        let cached_etag = "\"abc123\"";
        handler
            .cache_upload_part(cache_key, upload_id, 1, &part_data, cached_etag)
            .await
            .unwrap();

        // Create requested parts with unquoted ETag (should still match after normalization)
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "abc123".to_string(),
        }];

        // Attempt to finalize - should succeed since normalized ETags match
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed
        assert!(result.is_ok());

        // Verify metadata was created (cache finalization succeeded)
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists());
    }

    /// Test unreferenced part cleanup during CompleteMultipartUpload
    /// When parts are cached but not included in the CompleteMultipartUpload request,
    /// they should be deleted from disk.
    /// Requirements: 6.1, 6.2, 6.3, 6.4
    #[tokio::test]
    async fn test_finalize_multipart_upload_deletes_unreferenced_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-unreferenced";
        let upload_id = "test-upload-unreferenced";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, and 3
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();

        // Verify all part files exist in the upload directory
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        let part1_file = multipart_dir.join("part1.bin");
        let part2_file = multipart_dir.join("part2.bin");
        let part3_file = multipart_dir.join("part3.bin");

        assert!(
            part1_file.exists(),
            "Part 1 file should exist before finalization"
        );
        assert!(
            part2_file.exists(),
            "Part 2 file should exist before finalization"
        );
        assert!(
            part3_file.exists(),
            "Part 3 file should exist before finalization"
        );

        // Complete with only parts 1 and 3 (skip part 2)
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify upload directory was cleaned up (part 2 deleted with it)
        assert!(
            !multipart_dir.exists(),
            "Upload directory should be removed after finalization"
        );

        // Verify parts 1 and 3 were moved to final byte offsets in ranges/
        // Part 1: 0-1023, Part 3: 1024-1535 (since part 2 is skipped)
        let ranges_dir = temp_dir.path().join("ranges");
        let final_part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1023.bin").unwrap();
        let final_part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_1024-1535.bin").unwrap();

        assert!(
            final_part1_file.exists(),
            "Part 1 should be renamed to final byte offset"
        );
        assert!(
            final_part3_file.exists(),
            "Part 3 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(2));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 2);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(1024, 1535))
        );
        // Part 2 should not be in part_ranges
        assert!(!metadata.object_metadata.part_ranges.contains_key(&2));
    }

    /// Producer audit (cache-eviction-at-scale task 29 part 3, see
    /// `CAPACITY-AUTHORITY-PATH-FORWARD.md` § "CORRECTION 2026-09-03"). Pins, for
    /// the multipart-completion `RangeSpec` builder in `finalize_multipart_upload`
    /// (this is producer 5 of 6 in the audit — the rename at `signed_put_handler.rs:2647`
    /// plus the `RangeSpec::new` construction at `signed_put_handler.rs:2688`), that
    /// the persisted `.bin` file's on-disk byte length equals the `compressed_size`
    /// recorded in the resulting `RangeSpec`, and that the bytes decode as a valid
    /// LZ4 frame.
    ///
    /// This producer is structurally different from the other five: its
    /// `compressed_size` is computed by reading `tokio::fs::metadata(&new_range_file_path)
    /// .await.map(|m| m.len())` — i.e. it already reads back `st_size` from disk
    /// rather than carrying forward a compression-time accumulator — so this test
    /// pins that read-back value is self-consistent (no truncation across the
    /// rename, no drift between the read and a later read) rather than proving an
    /// independent computation agrees with it.
    /// METALOCK-B3 — the completion's `.meta` publish holds the key's metadata slot lock.
    ///
    /// `upload.lock`, which this function already held, is keyed on `upload_id` and lives in a
    /// different namespace, so it excluded no `.meta` writer of this key: a consolidation, a
    /// HEAD-metadata merge, or a second Complete overwriting the same key could all publish
    /// underneath it. The completion reads the old document to enumerate the range files it
    /// supersedes and then publishes wholesale, so that window discarded a peer's `ranges` and
    /// orphaned the `.bin` files they named.
    ///
    /// The predicate is the SURVIVING DOCUMENT, not that a lock was taken: while a peer holds
    /// the slot the peer's document must still be the one on disk. `create_test_handler` wires
    /// no `CacheManager`, so `invalidate_cache_unified_for_operation` earlier in the function
    /// is skipped and cannot be what took the slot.
    #[tokio::test]
    async fn finalize_multipart_upload_does_not_publish_while_a_peer_holds_the_slot() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_key = "test-bucket/metalock-b3-mpu-locked";
        let upload_id = "test-upload-metalock-b3";

        handler
            .cache_upload_part(cache_key, upload_id, 1, &vec![1u8; 1024], "etag1")
            .await
            .unwrap();
        let requested_parts = vec![RequestedPart {
            part_number: 1,
            etag: "etag1".to_string(),
        }];
        // The positive control needs its OWN upload, on the SAME cache key. A completion
        // consumes its staging directory — the part files are renamed to their final
        // range-addressed paths — so re-running the same one would fail for a reason that has
        // nothing to do with the lock, and the control would prove nothing.
        let control_upload_id = "test-upload-metalock-b3-control";
        handler
            .cache_upload_part(cache_key, control_upload_id, 1, &vec![1u8; 1024], "etag1")
            .await
            .unwrap();

        // A peer's document, published under the peer's own guard before the completion runs.
        let metadata_file = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("metadata"),
            cache_key,
            ".meta",
        )
        .unwrap();
        std::fs::create_dir_all(metadata_file.parent().unwrap()).unwrap();
        std::fs::write(&metadata_file, b"{\"peer\":\"document\"}").unwrap();
        let peer_lock =
            crate::metadata_lock_manager::acquire_slot_lock_blocking(temp_dir.path(), cache_key)
                .expect("fixture: the peer must be able to take the key's slot");

        let contended = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "test-final-etag",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await;

        // OUTCOME FIRST: the peer's bytes, unchanged.
        let surviving = std::fs::read_to_string(&metadata_file).unwrap();
        assert_eq!(
            surviving, "{\"peer\":\"document\"}",
            "the completion published its `.meta` while a peer held the key's metadata slot \
             lock — the read-modify-write ran unlocked. call returned {contended:?}"
        );
        assert!(
            contended.is_err(),
            "the completion must fail rather than publish while a peer holds the slot. \
             got={contended:?}"
        );

        drop(peer_lock);

        // Positive control: with the slot free the same call publishes, so the arm above
        // cannot have passed by never reaching the write.
        handler
            .finalize_multipart_upload(
                cache_key,
                control_upload_id,
                "test-final-etag",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await
            .expect("positive control: once the slot is free the completion must publish");
        let published = std::fs::read_to_string(&metadata_file).unwrap();
        assert!(
            published.contains("\"cache_key\""),
            "positive control: the completion must have replaced the peer's document with a \
             real `NewCacheMetadata`. got={published}"
        );
    }

    /// V3-GH-PORT R4a — a reader that re-seeds this instance's RAM metadata tier with the
    /// superseded document between the pre-publish invalidation and the `.meta` rename must
    /// not leave it there. The seam puts the stale snapshot in at exactly that point.
    /// Mutation that must break this: drop the post-rename `invalidate_metadata_cache`.
    #[tokio::test]
    async fn a_snapshot_reseeded_during_the_multipart_publish_is_dropped() {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("r4a");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager.clone());
        let cache_key = "test-bucket/r4a-reseed.bin";
        let upload_id = "r4a-upload";
        handler
            .cache_upload_part(cache_key, upload_id, 1, &vec![7u8; 1024], "etag1")
            .await
            .unwrap();
        let stale = crate::cache_types::NewCacheMetadata {
            cache_key: cache_key.to_string(),
            object_metadata: crate::cache_types::ObjectMetadata {
                etag: "\"superseded\"".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        RESEED_BEFORE_META_RENAME
            .lock()
            .unwrap()
            .push((cache_key.to_string(), stale));

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "\"r4a-final\"",
                &HashMap::new(),
                &[RequestedPart {
                    part_number: 1,
                    etag: "etag1".to_string(),
                }],
            )
            .await
            .expect("the completion publishes");

        assert!(
            RESEED_BEFORE_META_RENAME
                .lock()
                .unwrap()
                .iter()
                .all(|(k, _)| k != cache_key),
            "fixture: the reseed seam was never reached"
        );
        let resident = cache_manager.get_metadata_cache().peek(cache_key).await;
        assert!(
            resident.is_none(),
            "the superseded snapshot re-seeded during the publish survived it: etag={:?}",
            resident.map(|m| m.object_metadata.etag)
        );
    }

    /// — a part file that vanishes after the existence check and before the renames
    /// declines the cache finalization: no `.meta` is published (so nothing names the missing
    /// range or credits its recorded bytes), the part already moved is not left orphaned in
    /// `ranges/`, and the staging directory is removed.
    #[tokio::test]
    async fn a_part_vanishing_before_the_renames_publishes_no_meta() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_key = "test-bucket/case66-vanishing-part";
        let upload_id = "test-upload-case66-vanish";

        // Parts are renamed in part-number order and the hook removes part 2, so part 1 has
        // already moved into `ranges/` when the missing part is found.
        for n in 1..=2u32 {
            handler
                .cache_upload_part(cache_key, upload_id, n, &vec![n as u8; 1024], "etag")
                .await
                .unwrap();
        }
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        assert!(
            multipart_dir.join("part2.bin").exists(),
            "fixture: part 2 must be staged"
        );
        REMOVE_PART_BEFORE_RENAMES
            .lock()
            .unwrap()
            .push(multipart_dir.clone());

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag".to_string(),
            },
        ];
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "\"final-etag-2\"",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await;
        assert!(result.is_ok(), "declining is not an error: {result:?}");

        let metadata_file = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("metadata"),
            cache_key,
            ".meta",
        )
        .unwrap();
        assert!(
            !metadata_file.exists(),
            "a Complete whose part vanished before the rename published a `.meta` naming the \
             missing range: {:?}",
            std::fs::read_to_string(&metadata_file).ok()
        );
        let ranges_parent = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("ranges"),
            cache_key,
            "_0-0.bin",
        )
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
        let leftover: Vec<_> = std::fs::read_dir(&ranges_parent)
            .map(|d| d.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        assert!(
            leftover.is_empty(),
            "a declined finalization left range files named by no `.meta`: {leftover:?}"
        );
        assert!(
            !multipart_dir.exists(),
            "the declined upload's staging directory must be removed"
        );
    }

    /// U2: a finalization that fails at `point`, after part 1 has moved into `ranges/`,
    /// returns an error, publishes no `.meta` and leaves no range file in `ranges/`.
    async fn assert_finalize_failure_leaves_no_orphan(point: FinalizeFault, tag: &str) {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_key = format!("test-bucket/case50-{tag}");
        let upload_id = format!("test-upload-case50-{tag}");
        for n in 1..=2u32 {
            handler
                .cache_upload_part(&cache_key, &upload_id, n, &vec![n as u8; 1024], "etag")
                .await
                .unwrap();
        }
        let requested_parts: Vec<RequestedPart> = (1..=2u32)
            .map(|part_number| RequestedPart {
                part_number,
                etag: "etag".to_string(),
            })
            .collect();
        FINALIZE_FAULTS
            .lock()
            .unwrap()
            .push((cache_key.clone(), point));
        let result = handler
            .finalize_multipart_upload(
                &cache_key,
                &upload_id,
                "\"final-etag-2\"",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await;
        assert!(
            FINALIZE_FAULTS
                .lock()
                .unwrap()
                .iter()
                .all(|(k, _)| k != &cache_key),
            "fixture: the {point:?} fault was never reached"
        );
        assert!(
            result.is_err(),
            "{point:?}: the injected failure must surface"
        );
        let metadata_file = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("metadata"),
            &cache_key,
            ".meta",
        )
        .unwrap();
        assert!(
            !metadata_file.exists(),
            "{point:?}: no `.meta` may be published"
        );
        let ranges_parent = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("ranges"),
            &cache_key,
            "_0-0.bin",
        )
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
        let leftover: Vec<_> = std::fs::read_dir(&ranges_parent)
            .map(|d| d.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        assert!(
            leftover.is_empty(),
            "{point:?}: a failed finalization left range files named by no `.meta`: {leftover:?}"
        );
    }

    #[tokio::test]
    async fn finalize_failing_at_range_dir_create_leaves_no_orphan() {
        assert_finalize_failure_leaves_no_orphan(FinalizeFault::RangeDirCreate, "dir").await;
    }

    #[tokio::test]
    async fn finalize_failing_at_part_rename_leaves_no_orphan() {
        assert_finalize_failure_leaves_no_orphan(FinalizeFault::PartRename, "rename").await;
    }

    #[tokio::test]
    async fn finalize_failing_at_meta_dir_create_leaves_no_orphan() {
        assert_finalize_failure_leaves_no_orphan(FinalizeFault::MetaDirCreate, "metadir").await;
    }

    #[tokio::test]
    async fn finalize_failing_at_meta_tmp_write_leaves_no_orphan() {
        assert_finalize_failure_leaves_no_orphan(FinalizeFault::MetaTmpWrite, "tmpwrite").await;
    }

    #[tokio::test]
    async fn finalize_failing_at_meta_rename_leaves_no_orphan() {
        assert_finalize_failure_leaves_no_orphan(FinalizeFault::MetaRename, "metarename").await;
    }

    /// MULTIPART-ETAG — a completed multipart upload is stored with the QUOTED ETag
    /// S3 returns, not the stripped form.
    ///
    /// The stored value is the predicate, read back from the published `.meta`, because it
    /// is what every later HEAD/GET/revalidation compares against. The client's response is
    /// rebuilt from S3's original body bytes and never carried the stripped value, so
    /// asserting on it would pass with the defect present.
    ///
    /// Drives the two steps `handle_complete_multipart_upload` performs on a successful S3
    /// response — `extract_etag_from_xml` on the body, then `finalize_multipart_upload` with
    /// its result — without the network forward that precedes them.
    #[tokio::test]
    async fn completed_multipart_upload_stores_the_quoted_etag() {
        for (label, xml) in [
            (
                "literal quotes",
                "<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
                 <ETag>\"3858f62230ac3c915f300c664312c11f-1\"</ETag>\
                 </CompleteMultipartUploadResult>",
            ),
            (
                "entity-encoded quotes",
                "<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
                 <ETag>&quot;3858f62230ac3c915f300c664312c11f-1&quot;</ETag>\
                 </CompleteMultipartUploadResult>",
            ),
        ] {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);
            let cache_key = "test-bucket/scn1-multipart-etag-quoted";
            let upload_id = "test-upload-scn1";

            handler
                .cache_upload_part(cache_key, upload_id, 1, &vec![7u8; 1024], "\"part1\"")
                .await
                .unwrap();
            let requested_parts = vec![RequestedPart {
                part_number: 1,
                etag: "\"part1\"".to_string(),
            }];

            let etag = SignedPutHandler::extract_etag_from_xml(xml);
            handler
                .finalize_multipart_upload(
                    cache_key,
                    upload_id,
                    &etag,
                    &std::collections::HashMap::new(),
                    &requested_parts,
                )
                .await
                .expect("finalization must succeed");

            let metadata_file = crate::disk_cache::get_sharded_path(
                &temp_dir.path().join("metadata"),
                cache_key,
                ".meta",
            )
            .unwrap();
            let raw = std::fs::read_to_string(&metadata_file).unwrap_or_else(|e| {
                panic!("{label}: finalization must publish a .meta at {metadata_file:?}: {e}")
            });
            let metadata: crate::cache_types::NewCacheMetadata =
                serde_json::from_str(&raw).unwrap();
            assert_eq!(
                metadata.object_metadata.etag, "\"3858f62230ac3c915f300c664312c11f-1\"",
                "{label}: the multipart entry must be stored in the quoted form S3 returns on \
                 GET/HEAD; an unquoted value compares unequal to every later validator and \
                 the entry is invalidated on its first HEAD"
            );
            assert_eq!(
                metadata.ranges.len(),
                1,
                "{label}: FIXTURE: the part-count guard must have accepted the quoted ETag, or \
                 nothing was cached and the etag assertion above read a document it should not"
            );
        }
    }

    /// The producer keeps its "no validator" outcome: an empty or quotes-only ETag in the
    /// XML is stored as empty, not as `""`.
    #[test]
    fn extract_etag_from_xml_empty_etag_stays_empty() {
        assert_eq!(
            SignedPutHandler::extract_etag_from_xml("<R><ETag>\"\"</ETag></R>"),
            ""
        );
        assert_eq!(SignedPutHandler::extract_etag_from_xml("<R></R>"), "");
        assert_eq!(
            SignedPutHandler::extract_etag_from_xml("<R><ETag>abc-2</ETag></R>"),
            "\"abc-2\"",
            "an unquoted XML value is normalised to the quoted form"
        );
    }

    /// a key whose lowercase form has a different byte length (`K` U+212A
    /// is 3 bytes and lowercases to 1; `İ` U+0130 is 2 bytes and lowercases to 3) must not
    /// shift the slice the ETag or UploadId is read from, or panic.
    #[test]
    fn extract_from_xml_is_not_shifted_by_case_folding_that_changes_length() {
        let shrinking = "\u{212A}".repeat(4);
        let growing = "\u{130}".repeat(4);
        for key in [shrinking.as_str(), growing.as_str()] {
            let complete = format!(
                "<CompleteMultipartUploadResult><Key>{}</Key><ETag>\"abc-3\"</ETag></CompleteMultipartUploadResult>",
                key
            );
            let initiate = format!(
                "<InitiateMultipartUploadResult><Key>{}</Key><UploadId>UP-1</UploadId></InitiateMultipartUploadResult>",
                key
            );
            let etag =
                std::panic::catch_unwind(|| SignedPutHandler::extract_etag_from_xml(&complete));
            let upload = std::panic::catch_unwind(|| {
                SignedPutHandler::extract_upload_id_from_xml(&initiate)
            });
            assert_eq!(
                (etag.ok(), upload.ok()),
                (
                    Some("\"abc-3\"".to_string()),
                    Some(Some("UP-1".to_string()))
                ),
                "key {:?}",
                key
            );
        }
        // The tag itself still matches in any ASCII case.
        assert_eq!(
            SignedPutHandler::extract_etag_from_xml("<r><etag>\"abc-2\"</ETAG></r>"),
            "\"abc-2\""
        );
    }

    #[tokio::test]
    async fn test_finalize_multipart_upload_st_size_equals_compressed_size() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/producer-audit-mpu-completion";
        let upload_id = "test-upload-producer-audit";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        let part1_data = vec![0x66u8; 4096];
        let part2_data = vec![0x99u8; 2048];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
        ];

        handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await
            .unwrap();

        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();

        assert_eq!(metadata.ranges.len(), 2);

        let ranges_dir = temp_dir.path().join("ranges");
        for range_spec in &metadata.ranges {
            let bin_path = ranges_dir.join(&range_spec.file_path);
            let on_disk = std::fs::read(&bin_path).unwrap();
            let st_size = std::fs::metadata(&bin_path).unwrap().len();

            assert_eq!(
                st_size, range_spec.compressed_size,
                "st_size(.bin) must equal RangeSpec.compressed_size for range {}-{} \
                 produced by the multipart-completion finalizer",
                range_spec.start, range_spec.end
            );

            let handler = crate::compression::CompressionHandler::new(10, true);
            handler.decompress_data(&on_disk).unwrap_or_else(|e| {
                panic!(
                    ".bin bytes for range {}-{} from finalize_multipart_upload must \
                     decode as an LZ4 frame: {}",
                    range_spec.start, range_spec.end, e
                )
            });
        }
    }

    /// (`MPU-CROSS-PROXY-PART-RECORDED-WITH-COMPRESSED-SIZE-ZERO`): a part written by
    /// another proxy is recorded, and credited, at the length its writer measured.
    ///
    /// # The shape
    ///
    /// The earlier fleet test: part 1 through proxy A, part 2 through proxy B, `CompleteMultipartUpload`
    /// through proxy C, all over one shared cache directory. Each role here is its own
    /// `CacheManager` (A and B through `stage_streamed_part`, which builds one per call; C
    /// wired with a journal consolidator, as production is), so the only thing the three share
    /// is the directory, which is the only thing three proxies share. Part records name no
    /// instance, and only C journals, so the process-wide instance id does not blur the roles.
    ///
    /// # What the emulation stands for, and why it is needed
    ///
    /// `finalize_incremental_part` renames `part{N}.bin` into place **before** its descriptor
    /// is closed (the close is deferred to a blocking task), and on NFS a client's written
    /// bytes reach the server at close, not at rename. So for a window after B's part record
    /// exists, a `stat` from C can see `part2.bin` shorter than B wrote it — logged
    /// `expected=0 bytes, actual=8388760 bytes` for exactly that range. A local filesystem has
    /// no such window, so the test builds it: `part2.bin` is truncated to 0 bytes (what C's
    /// client is shown) between B's finalize and C's Complete, and B's bytes are written back
    /// to the renamed range file afterwards (B's writeback landing). B's record was taken on
    /// B's own client, before the truncation, and is untouched.
    ///
    /// # RED SIDE
    ///
    /// Finalize sizing the range from its own `stat` records part 2 at 0 bytes, the credit is
    /// short by part 2, and `staged_compressed_size()` is short by the same amount.
    #[tokio::test]
    async fn a_part_finalized_on_another_proxy_is_recorded_at_its_writers_size() {
        use crate::cache::{CacheEvictionAlgorithm, CacheManager};
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path();
        for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
            std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
        }

        let cache_key = "test-bucket/scn2-cross-proxy-photo.jpg";
        let upload_id = "scn2-cross-proxy-upload";
        let part1 = vec![0x51u8; 64 * 1024];
        let part2 = vec![0x52u8; 48 * 1024];

        // Proxy A: part 1. Proxy B: part 2.
        stage_streamed_part(cache_dir, cache_key, upload_id, 1, &part1, "etag1").await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 2, &part2, "etag2").await;

        let multipart_dir = mpu_upload_dir(cache_dir, upload_id);
        let part2_bin = multipart_dir.join("part2.bin");
        let b_bytes = std::fs::read(&part2_bin).unwrap();
        let b_record: crate::cache_types::CachedPartInfo = serde_json::from_str(
            &std::fs::read_to_string(multipart_dir.join("part2.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            b_record.compressed_size,
            b_bytes.len() as u64,
            "PRECONDITION: B's record must carry the length B wrote"
        );

        // C's view of B's part: renamed into place, bytes not yet on the server.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&part2_bin)
            .unwrap();

        // Proxy C: the Complete, with accounting wired.
        let manager_c = Arc::new(CacheManager::new_with_eviction_algorithm(
            cache_dir.to_path_buf(),
            false,
            0,
            CacheEvictionAlgorithm::TinyLFU,
        ));
        let _ = manager_c.create_configured_disk_cache_manager();
        let consolidator = manager_c
            .get_journal_consolidator()
            .await
            .expect("fixture: C's consolidator must be wired");
        let total_before = consolidator.size_accumulator().current_delta();
        let staged_before = consolidator.size_accumulator().current_write_cache_delta();
        let mut handler_c = create_test_handler(&temp_dir);
        handler_c.set_cache_manager(manager_c.clone());
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
        ];
        handler_c
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "\"scn2finaletag-2\"",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await
            .unwrap();

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert_eq!(
            metadata.ranges.len(),
            2,
            "PRECONDITION: both parts completed"
        );

        // B's writeback lands.
        let part2_range = metadata
            .ranges
            .iter()
            .find(|r| r.start == part1.len() as u64)
            .expect("PRECONDITION: part 2's range");
        std::fs::write(
            cache_dir.join("ranges").join(&part2_range.file_path),
            &b_bytes,
        )
        .unwrap();

        let mut on_disk_sum = 0u64;
        let mut report = Vec::new();
        for r in &metadata.ranges {
            let len = std::fs::metadata(cache_dir.join("ranges").join(&r.file_path))
                .unwrap()
                .len();
            on_disk_sum += len;
            report.push(format!(
                "{}-{} compressed_size={} bin_len={}",
                r.start, r.end, r.compressed_size, len
            ));
        }
        let total_credit = consolidator.size_accumulator().current_delta() - total_before;
        let staged_credit =
            consolidator.size_accumulator().current_write_cache_delta() - staged_before;
        let staged_in_meta = metadata.staged_compressed_size();
        println!(
            "SCN2 cross-proxy part size: ranges=[{}] on_disk_sum={} total_credit={} \
             staged_credit={} staged_compressed_size={}",
            report.join(", "),
            on_disk_sum,
            total_credit,
            staged_credit,
            staged_in_meta
        );
        for r in &metadata.ranges {
            let len = std::fs::metadata(cache_dir.join("ranges").join(&r.file_path))
                .unwrap()
                .len();
            assert_eq!(
                r.compressed_size, len,
                "range {}-{} is recorded at {} bytes but its .bin holds {}: the completing \
                 proxy sized a part another proxy wrote from its own view of the file",
                r.start, r.end, r.compressed_size, len
            );
        }
        assert_eq!(
            total_credit, on_disk_sum as i64,
            "the total credit must equal the completed ranges' bytes"
        );
        assert_eq!(
            staged_credit, on_disk_sum as i64,
            "the staged credit must equal the completed ranges' bytes"
        );
        assert_eq!(
            staged_in_meta, on_disk_sum,
            "the fold's staged_compressed_size() must equal the completed ranges' bytes"
        );
    }

    /// Fixture for the tests: a two-part upload staged on `cache_dir` and a handler
    /// wired with a `CacheManager` and journal consolidator, as production is.
    async fn staged_completion(
        temp_dir: &TempDir,
        cache_key: &str,
        upload_id: &str,
    ) -> (
        SignedPutHandler,
        Arc<crate::journal_consolidator::JournalConsolidator>,
        Vec<RequestedPart>,
    ) {
        use crate::cache::{CacheEvictionAlgorithm, CacheManager};
        let cache_dir = temp_dir.path();
        for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
            std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
        }
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &[0x61u8; 32 * 1024],
            "etag1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            2,
            &[0x62u8; 16 * 1024],
            "etag2",
        )
        .await;
        let manager = Arc::new(CacheManager::new_with_eviction_algorithm(
            cache_dir.to_path_buf(),
            false,
            0,
            CacheEvictionAlgorithm::TinyLFU,
        ));
        let _ = manager.create_configured_disk_cache_manager();
        let consolidator = manager
            .get_journal_consolidator()
            .await
            .expect("fixture: the consolidator must be wired");
        let mut handler = create_test_handler(temp_dir);
        handler.set_cache_manager(manager);
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
        ];
        (handler, consolidator, requested_parts)
    }

    /// a multipart completion that replaces an object whose `.meta` is still on disk
    /// (a peer republished it after the completion's invalidation, or the invalidation
    /// failed) must keep the NEW object's range files and still remove the old object's.
    ///
    /// Range files are named `{key}_{start}-{end}.bin`, so a new range with the same extent
    /// as an old one has the same path. The old object here has extents `0-32767` (shared
    /// with the new object) and `32768-40959` (not shared). The predicate is the new `.meta`'s
    /// ranges each naming a file that exists and holds the new part's bytes, plus the old
    /// non-shared file being gone and the old `.meta` not outliving the completion.
    ///
    /// RED SIDE: with the part renames before the old-range deletion (the earlier order),
    /// the deletion removes the new `0-32767` file by the old `.meta`'s `file_path`, and the
    /// new `.meta` names a missing file.
    #[tokio::test]
    async fn a_replaced_multipart_object_does_not_delete_the_new_objects_shared_extent() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path();
        let cache_key = "test-bucket/case67-same-extent.bin";
        // The NEW upload: 32 KiB + 16 KiB, staged by the fixture.
        let (mut handler, _consolidator, new_parts) =
            staged_completion(&temp_dir, cache_key, "case67-new").await;

        // The OLD object: 32 KiB + 8 KiB, completed first, so its `.meta` is on disk.
        stage_streamed_part(
            cache_dir,
            cache_key,
            "case67-old",
            1,
            &[0x41u8; 32 * 1024],
            "o1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            "case67-old",
            2,
            &[0x42u8; 8 * 1024],
            "o2",
        )
        .await;
        let old_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "o1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "o2".to_string(),
            },
        ];
        handler
            .finalize_multipart_upload(
                cache_key,
                "case67-old",
                "\"scn3old-2\"",
                &std::collections::HashMap::new(),
                &old_parts,
            )
            .await
            .expect("fixture: the old completion must publish");

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        let ranges_dir = cache_dir.join("ranges");
        let old_meta_bytes = std::fs::read(&metadata_file).expect("fixture: old .meta exists");
        let old_meta: crate::cache_types::NewCacheMetadata =
            serde_json::from_slice(&old_meta_bytes).unwrap();
        assert_eq!(
            old_meta
                .ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>(),
            vec![(0, 32767), (32768, 40959)],
            "fixture: the old object's extents"
        );
        // What a peer's republish leaves: its `.meta` and the range files it names.
        let mut republished = vec![(metadata_file.clone(), old_meta_bytes)];
        for r in &old_meta.ranges {
            let p = ranges_dir.join(&r.file_path);
            republished.push((p.clone(), std::fs::read(&p).unwrap()));
        }
        let old_unshared = ranges_dir.join(&old_meta.ranges[1].file_path);

        // The new object's part bytes, read before the renames consume them.
        let new_dir = mpu_upload_dir(cache_dir, "case67-new");
        let new_part_bytes = [
            std::fs::read(new_dir.join("part1.bin")).unwrap(),
            std::fs::read(new_dir.join("part2.bin")).unwrap(),
        ];

        REPUBLISH_AFTER_INVALIDATION
            .lock()
            .unwrap()
            .push((cache_key.to_string(), republished));
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                "case67-new",
                "\"scn3new-2\"",
                &std::collections::HashMap::new(),
                &new_parts,
            )
            .await;
        REPUBLISH_AFTER_INVALIDATION
            .lock()
            .unwrap()
            .retain(|(k, _)| k != cache_key);
        result.expect("the new completion must publish");

        let new_meta: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert_eq!(
            new_meta.object_metadata.etag, "\"scn3new-2\"",
            "the new object's .meta must be the one published"
        );
        assert_eq!(new_meta.ranges.len(), 2);
        for (r, want) in new_meta.ranges.iter().zip(new_part_bytes.iter()) {
            let p = ranges_dir.join(&r.file_path);
            let got = std::fs::read(&p).unwrap_or_else(|e| {
                panic!(
                    "the new .meta names range {}-{} at {:?}, which is missing ({}): the old \
                     object's deletion removed the new object's file at the shared path",
                    r.start, r.end, p, e
                )
            });
            assert!(
                got == *want,
                "range {}-{} holds {} bytes that are not the new part's {} bytes",
                r.start,
                r.end,
                got.len(),
                want.len()
            );
        }
        assert!(
            !old_unshared.exists(),
            "the old object's non-shared range file {:?} must be removed at once",
            old_unshared
        );
    }

    /// a staging eviction that read the replaced object's `.meta` before a multipart
    /// completion replaced it must not delete the new object's files afterwards.
    ///
    /// The eviction is paused right after its `.meta` read (`PAUSE_EVICTION_AFTER_META_READ`)
    /// and resumed one second later, while the completion runs in between. The old object has
    /// extents `0-32767` (shared with the new object) and `32768-40959`; the new object has
    /// `0-32767` and `32768-49151`. The predicate: after both finish, the new `.meta` is
    /// published and every range it names exists with the new part's bytes, and the old
    /// non-shared file is gone.
    ///
    /// RED SIDE: with no slot lock in the eviction, the completion runs to the end inside the
    /// pause; the eviction then deletes `0-32767` by the old `.meta`'s `file_path` (now the new
    /// object's file) and removes the `.meta` at the key's path (now the new one), leaving the
    /// new object's `32768-49151` file named by nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_eviction_that_read_the_replaced_meta_does_not_delete_the_new_objects_files() {
        use crate::cache::CacheEvictionAlgorithm;
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path();
        let cache_key = "test-bucket/case68-evict-race.bin";
        let (mut handler, _consolidator, new_parts) =
            staged_completion(&temp_dir, cache_key, "case68-new").await;

        stage_streamed_part(
            cache_dir,
            cache_key,
            "case68-old",
            1,
            &[0x41u8; 32 * 1024],
            "o1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            "case68-old",
            2,
            &[0x42u8; 8 * 1024],
            "o2",
        )
        .await;
        let old_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "o1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "o2".to_string(),
            },
        ];
        handler
            .finalize_multipart_upload(
                cache_key,
                "case68-old",
                "\"scn4old-2\"",
                &std::collections::HashMap::new(),
                &old_parts,
            )
            .await
            .expect("fixture: the old completion must publish");

        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        let ranges_dir = cache_dir.join("ranges");
        let range_path = |start: u64, end: u64| {
            crate::disk_cache::get_sharded_path(
                &ranges_dir,
                cache_key,
                &format!("_{}-{}.bin", start, end),
            )
            .unwrap()
        };
        assert!(
            metadata_file.exists() && range_path(32768, 40959).exists(),
            "fixture: the old object is cached"
        );
        let new_dir = mpu_upload_dir(cache_dir, "case68-new");
        let new_part_bytes = [
            std::fs::read(new_dir.join("part1.bin")).unwrap(),
            std::fs::read(new_dir.join("part2.bin")).unwrap(),
        ];

        let wcm = Arc::new(crate::write_cache_manager::WriteCacheManager::new(
            cache_dir.to_path_buf(),
            1 << 30,
            10.0,
            std::time::Duration::from_secs(86400),
            std::time::Duration::from_secs(86400),
            CacheEvictionAlgorithm::TinyLFU,
            10 * 1024 * 1024,
        ));
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        crate::write_cache_manager::PAUSE_EVICTION_AFTER_META_READ
            .lock()
            .unwrap()
            .push((cache_key.to_string(), (reached_tx, resume_rx)));
        let evictor = {
            let wcm = Arc::clone(&wcm);
            let key = cache_key.to_string();
            tokio::spawn(async move { wcm.evict_write_cached_object(&key).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), reached_rx)
            .await
            .expect("the eviction must reach its pause")
            .unwrap();
        // Resume the eviction after one second, whether or not the completion has finished:
        // an eviction that excludes the completion holds it until then.
        let resumer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let _ = resume_tx.send(());
        });

        let completed = handler
            .finalize_multipart_upload(
                cache_key,
                "case68-new",
                "\"scn4new-2\"",
                &std::collections::HashMap::new(),
                &new_parts,
            )
            .await;
        resumer.await.unwrap();
        let evicted = evictor.await.unwrap();
        crate::write_cache_manager::PAUSE_EVICTION_AFTER_META_READ
            .lock()
            .unwrap()
            .retain(|(k, _)| k != cache_key);
        completed.expect("the new completion must publish");
        evicted.expect("the eviction of the old object must succeed");

        let left: Vec<_> = [(0, 32767), (32768, 40959), (32768, 49151)]
            .iter()
            .map(|(s, e)| range_path(*s, *e))
            .filter(|p| p.exists())
            .collect();
        let new_meta: crate::cache_types::NewCacheMetadata =
            match std::fs::read_to_string(&metadata_file) {
                Ok(s) => serde_json::from_str(&s).unwrap(),
                Err(e) => panic!(
                    "no .meta after the completion and the eviction both finished ({}); range \
                     files left on disk, named by nothing: {:?}. The eviction, acting on the \
                     replaced object's .meta, removed the new object's .meta and its \
                     shared-extent file",
                    e, left
                ),
            };
        assert_eq!(
            new_meta.object_metadata.etag, "\"scn4new-2\"",
            "the new object's .meta must be the one published"
        );
        assert_eq!(new_meta.ranges.len(), 2);
        for (r, want) in new_meta.ranges.iter().zip(new_part_bytes.iter()) {
            let p = ranges_dir.join(&r.file_path);
            let got = std::fs::read(&p).unwrap_or_else(|e| {
                panic!(
                    "the new .meta names range {}-{} at {:?}, which is missing ({}): the \
                     eviction deleted the new object's file at the shared path",
                    r.start, r.end, p, e
                )
            });
            assert!(
                got == *want,
                "range {}-{} holds {} bytes that are not the new part's {} bytes",
                r.start,
                r.end,
                got.len(),
                want.len()
            );
        }
        assert!(
            !range_path(32768, 40959).exists(),
            "the old object's non-shared range file must be removed"
        );
    }

    /// a completion whose assembled part count disagrees with S3's ETag suffix caches
    /// nothing and leaves nothing behind in `ranges/`, and the object it replaces is still
    /// removed at once.
    ///
    /// The predicate is the whole `ranges/` tree being empty after the declined completion
    /// (the old object's file and the new object's parts alike), with no `.meta` and no
    /// staging directory left.
    ///
    /// RED SIDE: with the part-count check after the part renames, the two renamed parts stay
    /// in `ranges/` named by no `.meta`, uncounted until orphan recovery reaches them.
    #[tokio::test]
    async fn a_part_count_mismatch_leaves_no_renamed_parts_in_ranges() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();
        let cache_key = "test-bucket/case68-part-count.bin";
        let headers = std::collections::HashMap::new();

        // The object this completion replaces, cached first.
        stage_streamed_part(
            cache_dir,
            cache_key,
            "case68-pc-old",
            1,
            &[0x4fu8; 1024],
            "o1",
        )
        .await;
        handler
            .finalize_multipart_upload(
                cache_key,
                "case68-pc-old",
                "\"scn4pcold-1\"",
                &headers,
                &[RequestedPart {
                    part_number: 1,
                    etag: "o1".to_string(),
                }],
            )
            .await
            .expect("fixture: the old completion must publish");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        assert!(metadata_file.exists(), "fixture: the old object is cached");

        // Two parts recorded and requested, while S3's ETag says the object has three.
        stage_streamed_part(cache_dir, cache_key, "case68-pc-new", 1, &[1u8; 1024], "e1").await;
        stage_streamed_part(cache_dir, cache_key, "case68-pc-new", 2, &[2u8; 2048], "e2").await;
        handler
            .finalize_multipart_upload(
                cache_key,
                "case68-pc-new",
                "\"scn4pcnew-3\"",
                &headers,
                &[
                    RequestedPart {
                        part_number: 1,
                        etag: "e1".to_string(),
                    },
                    RequestedPart {
                        part_number: 2,
                        etag: "e2".to_string(),
                    },
                ],
            )
            .await
            .expect("a declined completion is not an error");

        fn files_under(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.is_dir() {
                        files_under(&p, out);
                    } else {
                        out.push(p);
                    }
                }
            }
        }
        let mut left = Vec::new();
        files_under(&cache_dir.join("ranges"), &mut left);
        assert!(
            left.is_empty(),
            "a part-count mismatch left {} file(s) in ranges/ named by no .meta: {:?}",
            left.len(),
            left
        );
        assert!(
            !metadata_file.exists(),
            "the replaced object's .meta must be removed and no new one published"
        );
        assert!(
            !mpu_upload_dir(cache_dir, "case68-pc-new").exists(),
            "the declined upload's staging directory must be removed"
        );
    }

    /// (cache-eviction-at-scale § 16.6.12 L3): a process that dies between the
    /// completion's Write_Ledger append and its `.meta` publish must not leave a published
    /// staged `.meta` with no ledger entry.
    ///
    /// The predicate is the one staging eviction and the fold depend on: every staged range
    /// of a published staged `.meta` has an entry. The crash is injected at the one point
    /// between the two steps, and returning from there is what a dead process leaves on
    /// shared storage. With the append first, the crash leaves entries and no `.meta`, and
    /// the test also pins what a reader then sees: `MetadataAbsent`, retiring nothing while
    /// the entries are younger than the grace.
    ///
    /// RED SIDE: rename before the append (the earlier order) leaves the `.meta` published
    /// with zero entries.
    #[tokio::test]
    async fn a_crash_between_ledger_append_and_publish_leaves_no_unledgered_staged_meta() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/case69-crash-window.bin";
        let (mut handler, consolidator, requested_parts) =
            staged_completion(&temp_dir, cache_key, "case69-crash-upload").await;
        CRASH_BETWEEN_LEDGER_AND_PUBLISH
            .lock()
            .unwrap()
            .push(cache_key.to_string());
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                "case69-crash-upload",
                "\"scn5crash-2\"",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await;
        CRASH_BETWEEN_LEDGER_AND_PUBLISH
            .lock()
            .unwrap()
            .retain(|k| k != cache_key);
        result.expect("the injected crash returns Ok");

        let metadata_file = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("metadata"),
            cache_key,
            ".meta",
        )
        .unwrap();
        let entries: Vec<_> = consolidator
            .write_ledger()
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.cache_key == cache_key)
            .collect();

        if let Ok(content) = std::fs::read_to_string(&metadata_file) {
            let metadata: crate::cache_types::NewCacheMetadata =
                serde_json::from_str(&content).unwrap();
            let staged: Vec<_> = metadata
                .ranges
                .iter()
                .filter(|r| {
                    crate::cache_types::is_staged_range_spec(
                        r,
                        metadata.object_metadata.is_write_cached,
                    )
                })
                .collect();
            for r in &staged {
                assert!(
                    entries
                        .iter()
                        .any(|e| e.range_start == r.start && e.range_end == r.end),
                    "a crash between the ledger append and the publish left a published staged \
                     .meta whose range {}-{} has no Write_Ledger entry: staged ranges={}, \
                     ledger entries={}",
                    r.start,
                    r.end,
                    staged.len(),
                    entries.len()
                );
            }
        }

        // With the append first, the crash state is entries for an unpublished `.meta`.
        assert!(
            !metadata_file.exists(),
            "the crash must leave the .meta unpublished"
        );
        assert_eq!(entries.len(), 2, "both staged ranges must be ledgered");
        let candidate = crate::write_ledger::WriteLedger::group_by_key(entries).remove(0);
        let assessment =
            crate::write_ledger::assess_staged_candidate(temp_dir.path(), &candidate).await;
        assert_eq!(
            assessment.verdict,
            crate::write_ledger::StagedCandidateVerdict::MetadataAbsent,
            "a reader of the crash state must see MetadataAbsent, which eviction skips and \
             the fold counts as zero"
        );
        assert!(
            assessment.retirable.is_empty(),
            "fresh entries naming an absent .meta must not be retired inside the grace, or a \
             reader landing between a live completion's append and rename would strip its \
             entries"
        );
    }

    /// a completion whose Write_Ledger append fails is not cached at all, and credits
    /// nothing. The failure is real I/O: a regular file at `metadata/_write_ledger` makes the
    /// append's `create_dir_all` fail.
    ///
    /// The decision under test: neither a staged `.meta` with no entry (the L3 loss mode)
    /// nor a read-cached `.meta` (which would be served without a `Last-Modified` the
    /// write-cache path would otherwise learn). The range files the `.meta` would have
    /// named are removed, since nothing else would reference them.
    #[tokio::test]
    async fn a_failed_ledger_append_declines_the_completion() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/case69-failed-append.bin";
        let (mut handler, consolidator, requested_parts) =
            staged_completion(&temp_dir, cache_key, "case69-failed-upload").await;
        std::fs::write(
            temp_dir.path().join("metadata/_write_ledger"),
            b"not a directory",
        )
        .unwrap();
        let total_before = consolidator.size_accumulator().current_delta();
        let staged_before = consolidator.size_accumulator().current_write_cache_delta();
        let declines_before = super::mpu_completion_ledger_declines_total();
        handler
            .finalize_multipart_upload(
                cache_key,
                "case69-failed-upload",
                "\"scn5failed-2\"",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await
            .expect("a declined cache publish must not fail the completion");
        let metadata_file = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("metadata"),
            cache_key,
            ".meta",
        )
        .unwrap();
        assert!(
            !metadata_file.exists(),
            "a completion whose ledger append failed must not publish a .meta"
        );
        let range_dir = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("ranges"),
            cache_key,
            "_0-0.bin",
        )
        .unwrap();
        let leftover: Vec<_> = std::fs::read_dir(range_dir.parent().unwrap())
            .map(|d| d.filter_map(|e| e.ok()).map(|e| e.path()).collect())
            .unwrap_or_default();
        assert!(
            leftover.is_empty(),
            "the declined completion's range files must be removed: {leftover:?}"
        );
        assert_eq!(
            (
                consolidator.size_accumulator().current_delta() - total_before,
                consolidator.size_accumulator().current_write_cache_delta() - staged_before
            ),
            (0, 0),
            "a declined completion must credit neither channel"
        );
        // A floor, not an equality: the counter is process-wide and other tests run in
        // parallel, but only a declined completion increments it.
        assert!(
            super::mpu_completion_ledger_declines_total() > declines_before,
            "a declined completion must count in write_cache.mpu_completion_ledger_declines_total: \
             before={declines_before}, after={}",
            super::mpu_completion_ledger_declines_total()
        );
    }

    /// `cached_objects` counts one per `.meta` file. A completion publishes its `.meta`
    /// listing every part, and the `Add`s it journals fold onto that document, so
    /// consolidation never credits it; the publish must, exactly once.
    ///
    /// # RED SIDE
    ///
    /// Before the fix the count after the completion and a consolidation was `before`.
    #[tokio::test]
    async fn a_multipart_completion_credits_cached_objects_once() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/case15-mpu.bin";
        let (mut handler, consolidator, requested_parts) =
            staged_completion(&temp_dir, cache_key, "case15-upload").await;
        let count = || async {
            consolidator
                .load_size_state()
                .await
                .expect("fixture: size_state must load")
                .cached_objects
        };
        let before = count().await;
        handler
            .finalize_multipart_upload(
                cache_key,
                "case15-upload",
                "\"scn6mpu-2\"",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await
            .unwrap();
        assert!(
            crate::disk_cache::get_sharded_path(
                &temp_dir.path().join("metadata"),
                cache_key,
                ".meta"
            )
            .unwrap()
            .exists(),
            "PRECONDITION: the completion must publish its .meta"
        );
        consolidator.run_consolidation_cycle().await.unwrap();
        assert_eq!(
            count().await,
            before + 1,
            "UNCREDITED COMPLETION: the multipart object's .meta was not counted once"
        );
    }

    /// positive control: a completion whose appends land publishes a staged `.meta`
    /// with exactly one entry per range (the post-publish credit site must not append a
    /// second one) and credits the write tier by the staged bytes.
    #[tokio::test]
    async fn a_completion_appends_each_staged_range_once_before_publishing() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/case69-control.bin";
        let (mut handler, consolidator, requested_parts) =
            staged_completion(&temp_dir, cache_key, "case69-control-upload").await;
        let staged_before = consolidator.size_accumulator().current_write_cache_delta();
        handler
            .finalize_multipart_upload(
                cache_key,
                "case69-control-upload",
                "\"scn5control-2\"",
                &std::collections::HashMap::new(),
                &requested_parts,
            )
            .await
            .unwrap();
        let metadata_file = crate::disk_cache::get_sharded_path(
            &temp_dir.path().join("metadata"),
            cache_key,
            ".meta",
        )
        .unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert!(metadata.object_metadata.is_write_cached);
        let entries: Vec<_> = consolidator
            .write_ledger()
            .read_merged_oldest_first(0)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.cache_key == cache_key)
            .collect();
        let mut identities: Vec<_> = entries
            .iter()
            .map(|e| (e.range_start, e.range_end))
            .collect();
        identities.sort();
        let mut expected: Vec<_> = metadata.ranges.iter().map(|r| (r.start, r.end)).collect();
        expected.sort();
        assert_eq!(
            identities, expected,
            "exactly one ledger entry per published range"
        );
        assert_eq!(
            consolidator.size_accumulator().current_write_cache_delta() - staged_before,
            metadata.staged_compressed_size() as i64,
            "the write tier must be credited by the published staged bytes"
        );
    }

    /// Test part filtering with contiguous parts (all parts in sequence)
    /// Requirements: 5.1, 5.2, 5.3, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_skips_zero_length_part() {
        // Case A: the only part is empty. Case B: the final part is empty. S3 allows a
        // zero-length part only as the final or only part; neither has a byte range.
        for (case, sizes) in [("only", vec![0usize]), ("final", vec![1024usize, 0])] {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);
            let cache_key = format!("test-bucket/zero-length-{case}");
            let upload_id = format!("upload-zero-{case}");
            let mut requested_parts = Vec::new();
            for (idx, size) in sizes.iter().enumerate() {
                let n = (idx + 1) as u32;
                let etag = format!("etag{n}");
                handler
                    .cache_upload_part(&cache_key, &upload_id, n, &vec![7u8; *size], &etag)
                    .await
                    .unwrap();
                requested_parts.push(RequestedPart {
                    part_number: n,
                    etag,
                });
            }
            let result = handler
                .finalize_multipart_upload(
                    &cache_key,
                    &upload_id,
                    "final-etag",
                    &std::collections::HashMap::new(),
                    &requested_parts,
                )
                .await;
            assert!(result.is_ok(), "{case}: finalize must not fail the client");
            let metadata_file = crate::disk_cache::get_sharded_path(
                &temp_dir.path().join("metadata"),
                &cache_key,
                ".meta",
            )
            .unwrap();
            let published = std::fs::read_to_string(&metadata_file)
                .ok()
                .and_then(|s| serde_json::from_str::<crate::cache_types::NewCacheMetadata>(&s).ok())
                .map(|m| {
                    format!(
                        "content_length={} part_ranges={:?} ranges={:?}",
                        m.object_metadata.content_length,
                        m.object_metadata.part_ranges,
                        m.ranges
                            .iter()
                            .map(|r| (r.start, r.end))
                            .collect::<Vec<_>>()
                    )
                });
            assert!(
                published.is_none(),
                "{case}: a zero-length part must not be cached; published {published:?}"
            );
        }
    }
    #[tokio::test]
    async fn test_finalize_multipart_upload_contiguous_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-contiguous";
        let upload_id = "test-upload-contiguous";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, and 3 (contiguous)
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();

        // Complete with all parts in order (contiguous)
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify all parts were renamed to final byte offsets
        let ranges_dir = temp_dir.path().join("ranges");
        // Part 1: 0-1023 (1024 bytes)
        // Part 2: 1024-3071 (2048 bytes)
        // Part 3: 3072-3583 (512 bytes)
        let final_part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1023.bin").unwrap();
        let final_part2_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_1024-3071.bin").unwrap();
        let final_part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_3072-3583.bin").unwrap();

        assert!(
            final_part1_file.exists(),
            "Part 1 should be renamed to final byte offset"
        );
        assert!(
            final_part2_file.exists(),
            "Part 2 should be renamed to final byte offset"
        );
        assert!(
            final_part3_file.exists(),
            "Part 3 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges with cumulative offsets
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(3));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 3);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(1024, 3071))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(3072, 3583))
        );

        // Verify total content length
        assert_eq!(
            metadata.object_metadata.content_length,
            1024 + 2048 + 512 // 3584 bytes total
        );
    }

    /// Test cumulative offset calculation with variable-sized parts
    /// Verifies that byte ranges are calculated correctly when parts have different sizes
    /// Requirements: 5.3, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_variable_sized_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-variable-sizes";
        let upload_id = "test-upload-variable-sizes";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts with significantly different sizes
        let part1_data = vec![1u8; 5 * 1024 * 1024]; // 5 MB
        let part2_data = vec![2u8; 10 * 1024 * 1024]; // 10 MB
        let part3_data = vec![3u8; 7 * 1024 * 1024]; // 7 MB
        let part4_data = vec![4u8; 3 * 1024 * 1024]; // 3 MB

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 4, &part4_data, "etag4")
            .await
            .unwrap();

        // Complete with all parts
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
            RequestedPart {
                part_number: 4,
                etag: "etag4".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify metadata contains correct part_ranges with cumulative offsets
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        // Calculate expected byte ranges
        let part1_size: u64 = 5 * 1024 * 1024;
        let part2_size: u64 = 10 * 1024 * 1024;
        let part3_size: u64 = 7 * 1024 * 1024;
        let part4_size: u64 = 3 * 1024 * 1024;

        let part1_start: u64 = 0;
        let part1_end: u64 = part1_size - 1;
        let part2_start: u64 = part1_size;
        let part2_end: u64 = part1_size + part2_size - 1;
        let part3_start: u64 = part1_size + part2_size;
        let part3_end: u64 = part1_size + part2_size + part3_size - 1;
        let part4_start: u64 = part1_size + part2_size + part3_size;
        let part4_end: u64 = part1_size + part2_size + part3_size + part4_size - 1;

        assert_eq!(metadata.object_metadata.parts_count, Some(4));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 4);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(part1_start, part1_end)),
            "Part 1 range should be 0-{}",
            part1_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(part2_start, part2_end)),
            "Part 2 range should be {}-{}",
            part2_start,
            part2_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(part3_start, part3_end)),
            "Part 3 range should be {}-{}",
            part3_start,
            part3_end
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&4),
            Some(&(part4_start, part4_end)),
            "Part 4 range should be {}-{}",
            part4_start,
            part4_end
        );

        // Verify total content length
        let expected_total = part1_size + part2_size + part3_size + part4_size;
        assert_eq!(
            metadata.object_metadata.content_length, expected_total,
            "Total content length should be {} bytes",
            expected_total
        );
    }

    /// Test part filtering with non-contiguous parts (gaps in part numbers)
    /// Verifies that byte ranges are calculated correctly when parts are not sequential
    /// Requirements: 5.1, 5.2, 5.3, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_non_contiguous_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-non-contiguous";
        let upload_id = "test-upload-non-contiguous";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 3, 5 (non-contiguous - gaps at 2 and 4)
        let part1_data = vec![1u8; 1024];
        let part3_data = vec![3u8; 2048];
        let part5_data = vec![5u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 5, &part5_data, "etag5")
            .await
            .unwrap();

        // Complete with all cached parts (non-contiguous part numbers)
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 3,
                etag: "etag3".to_string(),
            },
            RequestedPart {
                part_number: 5,
                etag: "etag5".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify all parts were renamed to final byte offsets
        let ranges_dir = temp_dir.path().join("ranges");
        // Part 1: 0-1023 (1024 bytes)
        // Part 3: 1024-3071 (2048 bytes) - starts right after part 1
        // Part 5: 3072-3583 (512 bytes) - starts right after part 3
        let final_part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1023.bin").unwrap();
        let final_part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_1024-3071.bin").unwrap();
        let final_part5_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_3072-3583.bin").unwrap();

        assert!(
            final_part1_file.exists(),
            "Part 1 should be renamed to final byte offset"
        );
        assert!(
            final_part3_file.exists(),
            "Part 3 should be renamed to final byte offset"
        );
        assert!(
            final_part5_file.exists(),
            "Part 5 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges
        // Note: part_ranges uses the original part numbers as keys, not sequential indices
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(3));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 3);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&1),
            Some(&(0, 1023))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&3),
            Some(&(1024, 3071))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&5),
            Some(&(3072, 3583))
        );

        // Parts 2 and 4 should not exist in part_ranges
        assert!(!metadata.object_metadata.part_ranges.contains_key(&2));
        assert!(!metadata.object_metadata.part_ranges.contains_key(&4));

        // Verify total content length
        assert_eq!(
            metadata.object_metadata.content_length,
            1024 + 2048 + 512 // 3584 bytes total
        );
    }

    /// Test part filtering when subset of cached parts is requested
    /// Verifies that only requested parts are included and byte offsets are recalculated
    /// Requirements: 5.1, 5.2, 5.3, 5.4, 7.1
    #[tokio::test]
    async fn test_finalize_multipart_upload_subset_of_cached_parts() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-subset";
        let upload_id = "test-upload-subset";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, 3, 4, 5
        let part1_data = vec![1u8; 1000];
        let part2_data = vec![2u8; 2000];
        let part3_data = vec![3u8; 3000];
        let part4_data = vec![4u8; 4000];
        let part5_data = vec![5u8; 5000];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "etag1")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "etag2")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "etag3")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 4, &part4_data, "etag4")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 5, &part5_data, "etag5")
            .await
            .unwrap();

        // Complete with only parts 2 and 4 (subset)
        let requested_parts = vec![
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
            RequestedPart {
                part_number: 4,
                etag: "etag4".to_string(),
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        assert!(result.is_ok());

        // Verify metadata was created
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(metadata_file.exists(), "Metadata file should be created");

        // Verify unreferenced parts were deleted
        let ranges_dir = temp_dir.path().join("ranges");
        let part1_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_part1_0-999.bin")
                .unwrap();
        let part3_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_part3_0-2999.bin")
                .unwrap();
        let part5_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_part5_0-4999.bin")
                .unwrap();

        assert!(
            !part1_file.exists(),
            "Part 1 file should be deleted (unreferenced)"
        );
        assert!(
            !part3_file.exists(),
            "Part 3 file should be deleted (unreferenced)"
        );
        assert!(
            !part5_file.exists(),
            "Part 5 file should be deleted (unreferenced)"
        );

        // Verify requested parts were renamed to final byte offsets
        // Part 2: 0-1999 (2000 bytes) - starts at 0 since it's the first requested part
        // Part 4: 2000-5999 (4000 bytes) - starts right after part 2
        let final_part2_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_0-1999.bin").unwrap();
        let final_part4_file =
            crate::disk_cache::get_sharded_path(&ranges_dir, cache_key, "_2000-5999.bin").unwrap();

        assert!(
            final_part2_file.exists(),
            "Part 2 should be renamed to final byte offset"
        );
        assert!(
            final_part4_file.exists(),
            "Part 4 should be renamed to final byte offset"
        );

        // Verify metadata contains correct part_ranges
        let metadata_content = std::fs::read_to_string(&metadata_file).unwrap();
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&metadata_content).unwrap();

        assert_eq!(metadata.object_metadata.parts_count, Some(2));
        assert_eq!(metadata.object_metadata.part_ranges.len(), 2);
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&2),
            Some(&(0, 1999))
        );
        assert_eq!(
            metadata.object_metadata.part_ranges.get(&4),
            Some(&(2000, 5999))
        );

        // Parts 1, 3, 5 should not exist in part_ranges
        assert!(!metadata.object_metadata.part_ranges.contains_key(&1));
        assert!(!metadata.object_metadata.part_ranges.contains_key(&3));
        assert!(!metadata.object_metadata.part_ranges.contains_key(&5));

        // Verify total content length (only parts 2 and 4)
        assert_eq!(
            metadata.object_metadata.content_length,
            2000 + 4000 // 6000 bytes total
        );
    }

    /// Test ETag validation with multiple parts - one mismatch should skip all caching
    /// Requirements: 9.1, 9.2, 9.3, 9.4
    #[tokio::test]
    async fn test_finalize_multipart_upload_etag_mismatch_one_of_many() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object-etag-one-mismatch";
        let upload_id = "test-upload-etag-one-mismatch";
        let etag = "test-final-etag";
        let response_headers = std::collections::HashMap::new();

        // Cache parts 1, 2, 3
        let part1_data = vec![1u8; 1024];
        let part2_data = vec![2u8; 2048];
        let part3_data = vec![3u8; 512];

        handler
            .cache_upload_part(cache_key, upload_id, 1, &part1_data, "\"etag1\"")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 2, &part2_data, "\"etag2\"")
            .await
            .unwrap();
        handler
            .cache_upload_part(cache_key, upload_id, 3, &part3_data, "\"etag3\"")
            .await
            .unwrap();

        // Complete with parts where part 2 has mismatched ETag
        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "\"etag1\"".to_string(), // matches
            },
            RequestedPart {
                part_number: 2,
                etag: "\"wrong-etag\"".to_string(), // MISMATCH
            },
            RequestedPart {
                part_number: 3,
                etag: "\"etag3\"".to_string(), // matches
            },
        ];

        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                etag,
                &response_headers,
                &requested_parts,
            )
            .await;

        // Should succeed (operation not failed - Requirement 9.4)
        assert!(result.is_ok());

        // Verify no metadata was created (cache finalization skipped - Requirement 9.2)
        let metadata_dir = temp_dir.path().join("metadata");
        let metadata_file =
            crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta").unwrap();
        assert!(
            !metadata_file.exists(),
            "Metadata file should NOT be created due to ETag mismatch"
        );

        // Verify multipart directory was cleaned up
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        assert!(
            !multipart_dir.exists(),
            "Multipart directory should be cleaned up"
        );
    }

    // ============================================================================
    // Property-Based Tests for Multipart Upload
    // ============================================================================

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: write-through-cache-finalization, Property 5: Multipart part storage**
    /// *For any* successful UploadPart request, the part data SHALL be stored as a range file,
    /// and the tracking metadata SHALL contain the uploadId, partNumber, size, and ETag.
    /// **Validates: Requirements 2.1, 2.2, 2.5**
    #[quickcheck]
    fn prop_multipart_part_storage(part_number: u8, data_size: u8) -> TestResult {
        // Filter out invalid inputs
        let part_number = (part_number % 100) + 1; // 1-100
        let data_size = (data_size % 100) + 10; // 10-109 bytes

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);

            let cache_key = "test-bucket/test-object";
            let upload_id = "test-upload-prop";
            let data: Vec<u8> = (0..data_size).collect();
            let etag = format!("\"etag-{}\"", part_number);

            // Cache the part
            let result = handler
                .cache_upload_part(cache_key, upload_id, part_number as u32, &data, &etag)
                .await;

            if result.is_err() {
                return TestResult::failed();
            }

            // Part state lives in per-part records now, not in `upload.meta`.
            let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
            let tracker = tracker_from_disk(&multipart_dir, upload_id, cache_key);
            // Verify tracker contains correct info
            let part_found = tracker
                .parts
                .iter()
                .find(|p| p.part_number == part_number as u32);

            match part_found {
                Some(part) => {
                    // Verify part info matches
                    TestResult::from_bool(
                        tracker.upload_id == upload_id
                            && tracker.cache_key == cache_key
                            && part.size == data_size as u64
                            && part.etag == etag,
                    )
                }
                None => TestResult::failed(),
            }
        })
    }

    /// **Feature: write-through-cache-finalization, Property 7: Multipart completion creates linked metadata**
    /// *For any* successful CompleteMultipartUpload, the object metadata SHALL contain range entries
    /// for all cached parts with correct byte offsets, and the final ETag from S3.
    /// **Validates: Requirements 3.1, 3.3, 3.4**
    #[quickcheck]
    fn prop_multipart_completion_creates_linked_metadata(part_count: u8) -> TestResult {
        // Filter out invalid inputs
        let part_count = (part_count % 5) + 1; // 1-5 parts

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);

            let cache_key = "test-bucket/test-object";
            let upload_id = "test-upload-complete";
            // Multipart ETag format, whose `-N` suffix is S3's own statement of the
            // part count. It must agree with the number of parts actually cached:
            // finalization now cross-checks the two and declines rather than
            // caching an object S3 says has a different shape. This fixture used a
            // hardcoded `-5` for every part count, which that check correctly reads
            // as a truncated assembly.
            let final_etag = format!("\"abc123-{}\"", part_count);

            // First, cache multiple parts
            let mut requested_parts = Vec::new();
            for part_num in 1..=part_count {
                let data: Vec<u8> = (0..1024).map(|i| (i + part_num as usize) as u8).collect();
                let etag = format!("\"part-etag-{}\"", part_num);

                let result = handler
                    .cache_upload_part(cache_key, upload_id, part_num as u32, &data, &etag)
                    .await;

                if result.is_err() {
                    return TestResult::failed();
                }
                requested_parts.push(RequestedPart {
                    part_number: part_num as u32,
                    etag,
                });
            }

            // Now finalize the multipart upload
            let test_headers = std::collections::HashMap::new(); // Empty headers for test
            let result = handler
                .finalize_multipart_upload(
                    cache_key,
                    upload_id,
                    &final_etag,
                    &test_headers,
                    &requested_parts,
                )
                .await;

            if result.is_err() {
                return TestResult::failed();
            }

            // Verify the metadata file was created at the sharded path
            let metadata_dir = temp_dir.path().join("metadata");
            let metadata_file =
                match crate::disk_cache::get_sharded_path(&metadata_dir, cache_key, ".meta") {
                    Ok(path) => path,
                    Err(_) => return TestResult::failed(),
                };

            if !metadata_file.exists() {
                return TestResult::failed();
            }

            // Read and verify metadata
            let meta_content = match std::fs::read_to_string(&metadata_file) {
                Ok(c) => c,
                Err(_) => return TestResult::failed(),
            };

            let metadata: crate::cache_types::NewCacheMetadata =
                match serde_json::from_str(&meta_content) {
                    Ok(m) => m,
                    Err(_) => return TestResult::failed(),
                };

            // Verify metadata properties
            // 1. Range entries for all parts
            if metadata.ranges.len() != part_count as usize {
                return TestResult::failed();
            }

            // 2. Correct byte offsets (no gaps or overlaps)
            let mut expected_start: u64 = 0;
            for range in &metadata.ranges {
                if range.start != expected_start {
                    return TestResult::failed();
                }
                expected_start = range.end + 1;
            }

            // 3. Final ETag from S3
            if metadata.object_metadata.etag != final_etag {
                return TestResult::failed();
            }

            // 4. is_write_cached=true
            if !metadata.object_metadata.is_write_cached {
                return TestResult::failed();
            }

            // 5. write_cache_expires_at is set
            if metadata.object_metadata.write_cache_expires_at.is_none() {
                return TestResult::failed();
            }

            // 6. Multipart directory should be cleaned up
            let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
            if multipart_dir.exists() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

    /// **Feature: write-through-cache-finalization, Property 9: Abort upload cleanup**
    /// *For any* AbortMultipartUpload request, all cached parts and tracking metadata for that uploadId
    /// SHALL be immediately removed.
    /// **Validates: Requirements 4.5, 8.5**
    #[quickcheck]
    fn prop_abort_upload_cleanup(part_count: u8) -> TestResult {
        // Filter out invalid inputs
        let part_count = (part_count % 5) + 1; // 1-5 parts

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let mut handler = create_test_handler(&temp_dir);

            let cache_key = "test-bucket/test-object-abort";
            let upload_id = "test-upload-abort";

            // First, cache multiple parts
            for part_num in 1..=part_count {
                let data: Vec<u8> = (0..1024).map(|i| (i + part_num as usize) as u8).collect();
                let etag = format!("\"part-etag-{}\"", part_num);

                let result = handler
                    .cache_upload_part(cache_key, upload_id, part_num as u32, &data, &etag)
                    .await;

                if result.is_err() {
                    return TestResult::failed();
                }
            }

            // Verify parts were cached
            let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
            let tracker = tracker_from_disk(&multipart_dir, upload_id, cache_key);

            for part in &tracker.parts {
                let part_path = multipart_dir.join(format!("part{}.bin", part.part_number));

                // Verify part file exists before cleanup
                if !part_path.exists() {
                    return TestResult::failed();
                }
            }

            // Now cleanup the multipart upload (simulating AbortMultipartUpload)
            let result = handler.cleanup_multipart_upload(upload_id).await;

            if result.is_err() {
                return TestResult::failed();
            }

            // Verify tracking metadata and all parts are deleted (directory removed)
            if multipart_dir.exists() {
                return TestResult::failed();
            }

            TestResult::passed()
        })
    }

    /// **Property 2: Part Filtering Preserves Only Requested Parts**
    /// *For any* set of cached parts and requested parts, the filtered result contains exactly
    /// the intersection of cached and requested parts.
    /// - No parts outside the intersection are included
    /// - All parts in the intersection are included
    /// **Validates: Requirements 5.1, 5.2**
    #[quickcheck]
    fn prop_part_filtering_preserves_only_requested_parts(
        cached_parts_bitmap: u16,
        requested_parts_bitmap: u16,
    ) -> TestResult {
        // Use bitmaps to represent sets of part numbers (1-16)
        // Each bit position represents whether that part number is in the set
        // This gives us good coverage of various set combinations

        // Convert bitmaps to sets of part numbers (1-indexed)
        let cached_part_numbers: std::collections::HashSet<u32> = (0..16u32)
            .filter(|i| (cached_parts_bitmap >> i) & 1 == 1)
            .map(|i| i + 1) // Convert to 1-indexed part numbers
            .collect();

        let requested_part_numbers: std::collections::HashSet<u32> = (0..16u32)
            .filter(|i| (requested_parts_bitmap >> i) & 1 == 1)
            .map(|i| i + 1) // Convert to 1-indexed part numbers
            .collect();

        // Skip trivial cases where both sets are empty
        if cached_part_numbers.is_empty() && requested_part_numbers.is_empty() {
            return TestResult::discard();
        }

        // Create mock CachedPartInfo for each cached part
        let cached_parts: Vec<crate::cache_types::CachedPartInfo> = cached_part_numbers
            .iter()
            .map(|&part_num| {
                crate::cache_types::CachedPartInfo::new_uncompressed(
                    part_num,
                    1024, // arbitrary size
                    format!("\"etag-{}\"", part_num),
                )
            })
            .collect();

        // Create RequestedPart for each requested part
        let requested_parts: Vec<RequestedPart> = requested_part_numbers
            .iter()
            .map(|&part_num| RequestedPart {
                part_number: part_num,
                etag: format!("\"etag-{}\"", part_num),
            })
            .collect();

        // Apply the same filtering logic as finalize_multipart_upload
        // (Requirements 5.1, 5.2, 5.3)
        let requested_set: std::collections::HashSet<u32> =
            requested_parts.iter().map(|p| p.part_number).collect();

        let filtered_parts: Vec<&crate::cache_types::CachedPartInfo> = cached_parts
            .iter()
            .filter(|p| requested_set.contains(&p.part_number))
            .collect();

        // Calculate expected intersection
        let expected_intersection: std::collections::HashSet<u32> = cached_part_numbers
            .intersection(&requested_part_numbers)
            .copied()
            .collect();

        // Extract actual filtered part numbers
        let actual_filtered: std::collections::HashSet<u32> =
            filtered_parts.iter().map(|p| p.part_number).collect();

        // Property 1: Filtered result equals the intersection
        if actual_filtered != expected_intersection {
            return TestResult::failed();
        }

        // Property 2: No parts outside the intersection are included
        for part in &filtered_parts {
            if !expected_intersection.contains(&part.part_number) {
                return TestResult::failed();
            }
        }

        // Property 3: All parts in the intersection are included
        for &part_num in &expected_intersection {
            if !actual_filtered.contains(&part_num) {
                return TestResult::failed();
            }
        }

        // Property 4: Count matches
        if filtered_parts.len() != expected_intersection.len() {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    /// **Property 4: Part Ranges Build Correctly from Sizes**
    /// *For any* ordered list of part sizes, cumulative offsets produce contiguous non-overlapping
    /// ranges where each range length equals the part size.
    /// - No gaps between ranges
    /// - No overlaps between ranges
    /// - Each range length equals the part size
    /// **Validates: Requirement 7.1**
    #[quickcheck]
    fn prop_part_ranges_build_correctly_from_sizes(part_sizes: Vec<u32>) -> TestResult {
        // Filter out zero-sized parts (S3 requires minimum 5MB parts, but for testing
        // we just need non-zero sizes to verify the algorithm)
        let part_sizes: Vec<u64> = part_sizes
            .into_iter()
            .filter(|&s| s > 0)
            .map(|s| s as u64)
            .collect();

        // Skip trivial cases with no parts
        if part_sizes.is_empty() {
            return TestResult::discard();
        }

        // Limit to reasonable number of parts to keep tests fast
        if part_sizes.len() > 100 {
            return TestResult::discard();
        }

        // Build part_ranges using the same algorithm as finalize_multipart_upload
        // (Requirements 5.3, 7.1)
        let byte_offsets: Vec<(u32, u64, u64)> = {
            let mut offsets = Vec::with_capacity(part_sizes.len());
            let mut current_offset: u64 = 0;
            for (idx, &size) in part_sizes.iter().enumerate() {
                let part_number = (idx + 1) as u32; // 1-indexed part numbers
                let start = current_offset;
                let end = current_offset + size - 1;
                offsets.push((part_number, start, end));
                current_offset += size;
            }
            offsets
        };

        // Build part_ranges HashMap (Requirements 7.1, 7.2)
        let part_ranges: std::collections::HashMap<u32, (u64, u64)> = byte_offsets
            .iter()
            .map(|(part_number, start, end)| (*part_number, (*start, *end)))
            .collect();

        // Property 1: Each range length equals the part size
        for (idx, &size) in part_sizes.iter().enumerate() {
            let part_number = (idx + 1) as u32;
            if let Some(&(start, end)) = part_ranges.get(&part_number) {
                let range_length = end - start + 1;
                if range_length != size {
                    return TestResult::failed();
                }
            } else {
                // Part should exist in the map
                return TestResult::failed();
            }
        }

        // Property 2: Ranges are contiguous (no gaps)
        // First range should start at 0
        if let Some(&(start, _)) = part_ranges.get(&1) {
            if start != 0 {
                return TestResult::failed();
            }
        }

        // Each subsequent range should start immediately after the previous one ends
        for idx in 1..part_sizes.len() {
            let prev_part_number = idx as u32;
            let curr_part_number = (idx + 1) as u32;

            if let (Some(&(_, prev_end)), Some(&(curr_start, _))) = (
                part_ranges.get(&prev_part_number),
                part_ranges.get(&curr_part_number),
            ) {
                // Current start should be exactly prev_end + 1 (no gap)
                if curr_start != prev_end + 1 {
                    return TestResult::failed();
                }
            }
        }

        // Property 3: Ranges are non-overlapping
        // Since we verified contiguity above (each starts at prev_end + 1),
        // and each range has positive length, they cannot overlap.
        // But let's verify explicitly by checking no range contains another's start
        let mut sorted_ranges: Vec<(u64, u64)> = part_ranges.values().copied().collect();
        sorted_ranges.sort_by_key(|&(start, _)| start);

        for i in 0..sorted_ranges.len() {
            for j in (i + 1)..sorted_ranges.len() {
                let (_, end_i) = sorted_ranges[i];
                let (start_j, _) = sorted_ranges[j];

                // Range j should start after range i ends (no overlap)
                if start_j <= end_i {
                    return TestResult::failed();
                }
            }
        }

        // Property 4: Total coverage equals sum of all part sizes
        let total_size: u64 = part_sizes.iter().sum();
        if let Some(&(_, last_end)) = part_ranges.get(&(part_sizes.len() as u32)) {
            // Last byte should be at total_size - 1 (0-indexed)
            if last_end != total_size - 1 {
                return TestResult::failed();
            }
        }

        // Property 5: Number of ranges equals number of parts
        if part_ranges.len() != part_sizes.len() {
            return TestResult::failed();
        }

        TestResult::passed()
    }

    /// **Property 5: ETag Validation Rejects Mismatches**
    /// *For any* pair of distinct ETags, comparing them causes cache finalization to be skipped;
    /// for any identical pair (with or without surrounding quotes), finalization proceeds.
    /// - Distinct ETags (after normalization) should not match
    /// - Identical ETags with various quote combinations should match
    /// - Quote normalization correctly strips surrounding quotes
    /// **Validates: Requirements 9.1, 9.2**
    #[quickcheck]
    fn prop_etag_validation_rejects_mismatches(
        etag_base: String,
        other_etag_base: String,
        cached_has_quotes: bool,
        request_has_quotes: bool,
    ) -> TestResult {
        // Filter out empty strings and strings containing quotes (to avoid nested quotes)
        if etag_base.is_empty() || etag_base.contains('"') {
            return TestResult::discard();
        }
        if other_etag_base.is_empty() || other_etag_base.contains('"') {
            return TestResult::discard();
        }

        // Limit string length to keep tests fast
        if etag_base.len() > 64 || other_etag_base.len() > 64 {
            return TestResult::discard();
        }

        // Test 1: Identical ETags with various quote combinations should match
        let cached_etag = if cached_has_quotes {
            format!("\"{}\"", etag_base)
        } else {
            etag_base.clone()
        };

        let request_etag = if request_has_quotes {
            format!("\"{}\"", etag_base)
        } else {
            etag_base.clone()
        };

        // A validator that is nothing but quotes (or empty, or whitespace-only once the
        // XML extractor has trimmed it) carries no opaque content and is outside this
        // property: `part_etags_match` refuses it on BOTH sides by design, which is what
        // `test_part_etags_match_refuses_empty_and_lopsided` asserts. Discard rather than
        // fail, so the generator cannot turn that deliberate refusal into a property
        // violation.
        if etag_base.trim().trim_matches('"').is_empty()
            || other_etag_base.trim().trim_matches('"').is_empty()
        {
            return TestResult::discard();
        }

        // Property 1: Identical base ETags match whatever their quoting.
        if !part_etags_match(&request_etag, &cached_etag) {
            return TestResult::failed();
        }

        // Property 2: each side also matches the bare base it was built from.
        if !part_etags_match(&cached_etag, &etag_base)
            || !part_etags_match(&request_etag, &etag_base)
        {
            return TestResult::failed();
        }

        // Test 2: Distinct ETags should not match (when bases are different)
        if etag_base != other_etag_base {
            let other_cached_etag = if cached_has_quotes {
                format!("\"{}\"", other_etag_base)
            } else {
                other_etag_base.clone()
            };

            // Property 3: Distinct ETags must not match.
            if part_etags_match(&request_etag, &other_cached_etag) {
                return TestResult::failed();
            }

            // Property 4: the distinct ETag still matches its own base.
            if !part_etags_match(&other_cached_etag, &other_etag_base) {
                return TestResult::failed();
            }
        }

        // Test 3: Verify the ETag validation logic used in finalize_multipart_upload
        // This simulates the actual comparison done during CompleteMultipartUpload
        let etags_match = part_etags_match(&request_etag, &cached_etag);

        // Property 5: Same base ETags should always match regardless of quotes
        if !etags_match {
            return TestResult::failed();
        }

        // Test 4: Verify distinct ETags are rejected
        if etag_base != other_etag_base {
            let distinct_request_etag = if request_has_quotes {
                format!("\"{}\"", other_etag_base)
            } else {
                other_etag_base.clone()
            };

            let distinct_match = part_etags_match(&distinct_request_etag, &cached_etag);

            // Property 6: Distinct ETags should never match
            if distinct_match {
                return TestResult::failed();
            }
        }

        TestResult::passed()
    }

    // =========================================================================
    // Streaming write-cache task tests (Task 4.1)
    //
    // Exercise `run_streaming_cache_write` directly (the spawned wrapper just
    // discards its outcome) over a real `WriteCacheRangeSink` backed by a temp
    // disk cache. They cover: non-chunked write+commit, aws-chunked decode+commit,
    // decoded-length mismatch → discard, and S3-failure → discard.
    // =========================================================================

    /// Build a configured-enough disk cache manager for sink-backed streaming
    /// cache tests (mirrors `cache::write_cache_range_sink_tests::make_disk_cache`).
    async fn make_streaming_disk_cache(
        temp_dir: &TempDir,
        batch_size: usize,
    ) -> crate::disk_cache::DiskCacheManager {
        let dc = crate::disk_cache::DiskCacheManager::new(
            temp_dir.path().to_path_buf(),
            true, // compression_enabled
            1024, // compression_threshold
            false,
            batch_size,
        );
        dc.initialize().await.unwrap();
        dc
    }

    async fn open_streaming_sink(
        dc: crate::disk_cache::DiskCacheManager,
        cache_key: &str,
        content_length: u64,
    ) -> crate::cache::WriteCacheRangeSink {
        crate::cache::WriteCacheRangeSink::open(
            dc,
            cache_key,
            content_length,
            true,
            Some(crate::write_cache_manager::WriteReservation::noop()),
        )
        .await
        .unwrap()
        .expect("the capacity guard admits with an unconfigured floor")
    }

    /// Push `frames` through `forward_tx` with the forward loop's exact channel
    /// discipline (`signed_request_proxy::forward_signed_request_streaming`):
    /// `try_send`, an awaited `send` on `Full`, and dropping the tee on `Closed`.
    /// Returns whether the tee was dropped.
    async fn drive_forward_discipline(
        forward_tx: tokio::sync::mpsc::Sender<Bytes>,
        frames: usize,
    ) -> bool {
        let mut tee = Some(forward_tx);
        for i in 0..frames {
            let frame = Bytes::from(vec![(i % 256) as u8; 1024]);
            if let Some(sender) = tee.as_ref() {
                let gone = match sender.try_send(frame) {
                    Ok(()) => false,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(f)) => {
                        sender.send(f).await.is_err()
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => true,
                };
                if gone {
                    tee = None;
                }
            }
        }
        tee.is_none()
    }

    /// / PUT-TEE-STALL-CAN-FAIL-CLIENT-UPLOAD. A cache writer that stops
    /// accepting frames (blocked on a shared-storage write) must not stop the
    /// forward loop past the stall bound: the forward must drop the tee and finish,
    /// the cache side must be told not to commit, and the abandonment is counted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_cache_writer_is_abandoned_within_the_bound() {
        let bound = std::time::Duration::from_millis(200);
        let before = super::tee_abandoned_total_put();
        let super::BoundedCacheTee {
            forward_tx,
            cache_rx,
            s3_result_tx,
            s3_result_rx,
        } = super::bounded_cache_tee(2, bound, "scn7-stalled");
        // The cache writer is "blocked": `cache_rx` is held and never read.
        let start = std::time::Instant::now();
        let dropped = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            drive_forward_discipline(forward_tx, 64),
        )
        .await
        .expect(
            "forward loop stalled on a blocked cache writer for 3 s with a 200 ms \
             stall bound: the cache side is coupled to the client body read",
        );
        assert!(dropped, "the forward loop must have dropped the tee");
        assert!(
            start.elapsed() < std::time::Duration::from_secs(2),
            "forward took {:?}, beyond bound + slack",
            start.elapsed()
        );
        s3_result_tx.send(Ok(ok_response_info())).unwrap();
        let delivered = tokio::time::timeout(std::time::Duration::from_secs(3), s3_result_rx)
            .await
            .expect("cache side must be released within 3 s");
        assert!(
            matches!(delivered, Ok(Err(_))),
            "an abandoned tee must hand the cache task a non-success so it discards \
             the partial entry, got {:?}",
            delivered.map(|r| r.map(|i| i.status))
        );
        assert!(
            super::tee_abandoned_total_put() > before,
            "tee_abandoned_total_put did not move: before={before}"
        );
        drop(cache_rx);
    }

    /// Count `.tmp` files anywhere under `dir`.
    fn count_tmp_files(dir: &std::path::Path) -> usize {
        let mut n = 0;
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    n += count_tmp_files(&p);
                } else if p.extension().is_some_and(|x| x == "tmp") {
                    n += 1;
                }
            }
        }
        n
    }

    /// / PUT-CACHE-SETUP-WAITS-UNBOUNDED. Opening the write-cache sink happens
    /// before the proxy reads a byte of the client's body, so a setup blocked on
    /// shared storage holds the client's body unread. The setup must give up within
    /// `WRITE_CACHE_SETUP_BOUND` and hand back no tee (the body then streams to S3
    /// uncached), count the abandonment, and leave no `.tmp` once the late open
    /// finally completes. Exercised for both the single-part PUT and `UploadPart`
    /// setup, which are the only two sink opens before a body read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_write_cache_setup_is_abandoned_within_the_bound() {
        const STALL: std::time::Duration = std::time::Duration::from_secs(10);
        let limit = super::WRITE_CACHE_SETUP_BOUND + std::time::Duration::from_millis(2500);
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("scn8");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager);
        let put_key = "test-bucket/scn8-stalled-put";
        let part_key = "test-bucket/scn8-stalled-part";
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .extend([(put_key.to_string(), STALL), (part_key.to_string(), STALL)]);
        let before = super::setup_abandoned_total_put();

        let start = std::time::Instant::now();
        let (tee, result) = handler
            .setup_put_cache_tee(put_key, &HashMap::new(), false, Some(4096))
            .await;
        let put_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        let (part_tee, part_result) = handler
            .setup_upload_part_cache_tee(part_key, "scn8-upload", 1, false, Some(4096), None)
            .await;
        let part_elapsed = start.elapsed();

        assert!(
            put_elapsed < limit,
            "PUT cache setup held the client body unread for {put_elapsed:?} on a stalled \
             cache volume; the bound is {:?}",
            super::WRITE_CACHE_SETUP_BOUND
        );
        assert!(
            part_elapsed < limit,
            "UploadPart cache setup held the client body unread for {part_elapsed:?} on a \
             stalled cache volume; the bound is {:?}",
            super::WRITE_CACHE_SETUP_BOUND
        );
        // PUT and UploadPart setups run beside the forward, so each tee is handed
        // over at once and the abandonment is decided in the background at the bound.
        assert!(
            tee.is_some() && result.is_some(),
            "a PUT's tee must be handed to the forward without waiting for setup"
        );
        assert!(
            part_tee.is_some() && part_result.is_some(),
            "an UploadPart's tee must be handed to the forward without waiting for setup"
        );
        drop((tee, result, part_tee, part_result));
        tokio::time::sleep(super::WRITE_CACHE_SETUP_BOUND + std::time::Duration::from_millis(500))
            .await;
        assert!(
            super::setup_abandoned_total_put() >= before + 2,
            "setup_abandoned_total_put did not count both abandonments: before={before}, \
             now={}",
            super::setup_abandoned_total_put()
        );

        // The late opens complete on their blocking threads; their sinks must be
        // dropped and their `.tmp` files removed.
        tokio::time::sleep(STALL + std::time::Duration::from_secs(2)).await;
        assert_eq!(
            count_tmp_files(&cache_root),
            0,
            "a sink opened after its setup was abandoned left a .tmp behind"
        );
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .retain(|(k, _)| k != put_key && k != part_key);
    }

    /// A setup abandoned at `WRITE_CACHE_SETUP_BOUND` keeps running
    /// on its blocking thread and may still open a `.tmp` and a batch buffer, so its
    /// pending-store permit must stay held until that setup finishes, not drop at the
    /// timeout. Both setups (PUT and `UploadPart`) are stalled past the bound; the permits
    /// must stay taken after the bound and return only once the stalls end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_abandoned_setup_holds_its_pending_store_permit_until_it_finishes() {
        const STALL: std::time::Duration = std::time::Duration::from_secs(10);
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("case70-abandoned-permit");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager);
        let permits = Arc::new(tokio::sync::Semaphore::new(2));
        handler.set_pending_cache_writes(permits.clone());
        let put_key = "test-bucket/case70-abandoned-permit-put";
        let part_key = "test-bucket/case70-abandoned-permit-part";
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .extend([(put_key.to_string(), STALL), (part_key.to_string(), STALL)]);

        // Record every observation and assert only at the end, so nothing panics while a
        // stalled setup still occupies a blocking thread.
        let observed = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            let start = std::time::Instant::now();
            let (tee, result) = handler
                .setup_put_cache_tee(put_key, &HashMap::new(), false, Some(4096))
                .await;
            let (part_tee, part_result) = handler
                .setup_upload_part_cache_tee(part_key, "case70-upload", 1, false, Some(4096), None)
                .await;
            let taken_at_admission = permits.available_permits();
            drop((tee, result, part_tee, part_result));

            // Past the bound both setups are abandoned, but still stalled on their threads.
            let mut after_bound = Vec::new();
            for at in [6u64, 7, 8] {
                let target = std::time::Duration::from_secs(at);
                if let Some(wait) = target.checked_sub(start.elapsed()) {
                    tokio::time::sleep(wait).await;
                }
                after_bound.push((start.elapsed(), permits.available_permits()));
            }

            // Once the stalls end, both permits come back.
            let deadline = STALL + std::time::Duration::from_secs(5);
            while permits.available_permits() < 2 && start.elapsed() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            (
                taken_at_admission,
                after_bound,
                permits.available_permits(),
                start.elapsed(),
            )
        })
        .await;
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .retain(|(k, _)| k != put_key && k != part_key);

        let (taken_at_admission, after_bound, at_end, elapsed) =
            observed.expect("the permit-lifetime observation did not finish within 60s");
        assert_eq!(
            taken_at_admission, 0,
            "fixture precondition: both setups must hold a permit at admission"
        );
        // Fixture precondition: both setups were abandoned at the bound. Without it the
        // permit samples below could pass on setups that never reached the bound.
        let abandoned = super::SETUP_ABANDONED_KEYS.lock().unwrap().clone();
        for key in [put_key, part_key] {
            assert!(
                abandoned.iter().any(|k| k == key),
                "fixture precondition: the setup for {key} was not abandoned at the {:?} \
                 bound, so this test did not exercise an abandoned setup",
                super::WRITE_CACHE_SETUP_BOUND
            );
        }
        assert!(
            after_bound.iter().all(|(_, available)| *available == 0),
            "a setup abandoned at the {:?} bound released its pending-store permit while it \
             was still running (samples of (elapsed, permits available of 2): {:?})",
            super::WRITE_CACHE_SETUP_BOUND,
            after_bound
        );
        assert_eq!(
            at_end, 2,
            "both permits must return once the stalled setups finish (elapsed {elapsed:?})"
        );
    }

    /// (review FU1): a part finalize abandoned at its 30-second timeout keeps
    /// running on its blocking thread, still owning the sink, its batch buffer, its fd and
    /// the part lock, so its pending-store permit must stay held until that thread ends,
    /// not drop when the timeout fires. The gate is `part1.lock` held from another open
    /// file description, which parks the finalize's `flock` past the timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_abandoned_part_finalize_holds_its_pending_store_permit_until_it_ends() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("case71-abandoned-finalize");
        let cache_key = "test-bucket/case71-abandoned-finalize";
        let upload_id = "case71-upload";
        let cache_mgr = crate::cache::CacheManager::new(
            cache_dir.clone(),
            false, // ram_cache_enabled
            0,     // max_ram_cache_size
            100,   // compression_threshold
            false, // compression_enabled
        );
        let mut sink = cache_mgr
            .open_multipart_part_sink(cache_key, upload_id, 1, Some(4096))
            .await
            .expect("open streaming part sink")
            .expect("the capacity guard admits with an unconfigured floor");
        sink.write(&[0x5Au8; 4096]).expect("write part bytes");

        let lock_path = SignedPutHandler::part_lock_path(&mpu_upload_dir(&cache_dir, upload_id), 1);
        let gate = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .expect("open part1.lock for the gate");
        fs4::FileExt::lock(&gate).expect("take part1.lock for the gate");

        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = permits
            .clone()
            .try_acquire_owned()
            .expect("fixture: one permit available");

        // Record every observation and assert only after the gate is released, so nothing
        // panics while the finalize still parks a blocking thread on the lock.
        let observed = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            let start = std::time::Instant::now();
            let result = SignedPutHandler::finalize_and_record_cached_part(
                &cache_dir,
                cache_key,
                upload_id,
                1,
                "etag1",
                sink,
                Some(permit),
            )
            .await;
            let returned_after = start.elapsed();
            let timed_out = matches!(&result, Err(e) if format!("{e:?}").contains("Timed out"));
            let mut after_timeout = Vec::new();
            for _ in 0..3 {
                after_timeout.push(permits.available_permits());
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
            (timed_out, returned_after, after_timeout)
        })
        .await;

        drop(gate);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while permits.available_permits() < 1 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let at_end = permits.available_permits();

        let (timed_out, returned_after, after_timeout) =
            observed.expect("the permit-lifetime observation did not finish within 60s");
        assert!(
            timed_out,
            "fixture precondition: the finalize must be abandoned at its 30s timeout \
             (returned after {returned_after:?})"
        );
        assert!(
            after_timeout.iter().all(|available| *available == 0),
            "a part finalize abandoned at its 30s timeout released its pending-store permit \
             while its blocking step was still running (permits available of 1, sampled \
             after the timeout: {after_timeout:?})"
        );
        assert_eq!(
            at_end, 1,
            "the permit must return once the abandoned finalize's blocking step ends"
        );
    }

    /// Control for `stalled_write_cache_setup_is_abandoned_within_the_bound`: an unstalled
    /// setup opens its tee and does not count an abandonment for its own key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn healthy_write_cache_setup_opens_its_tee() {
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("scn8-healthy");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager);
        let (tee, result) = handler
            .setup_put_cache_tee(
                "test-bucket/scn8-healthy-put",
                &HashMap::new(),
                false,
                Some(4096),
            )
            .await;
        assert!(
            tee.is_some() && result.is_some(),
            "healthy PUT setup must open a tee"
        );
        let (tee, result) = handler
            .setup_upload_part_cache_tee(
                "test-bucket/scn8-healthy-part",
                "scn8-healthy-upload",
                1,
                false,
                Some(4096),
                None,
            )
            .await;
        assert!(
            tee.is_some() && result.is_some(),
            "healthy part setup must open a tee"
        );
    }

    /// / PUT-CACHE-SETUP-TAKES-SECONDS-ON-EFS. The client's body read must not wait on
    /// cache bookkeeping: with the sink open slowed to 2 s, the PUT's tee must be handed to
    /// the forward at once, the body's frames must be accepted while setup is still running,
    /// and the object must still end write-through cached once the upstream succeeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_put_cache_setup_does_not_delay_the_body_and_still_caches() {
        const SLOW: std::time::Duration = std::time::Duration::from_secs(2);
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("scn9-slow");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager.clone());
        let key = "test-bucket/scn9-slow-setup-put";
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .push((key.to_string(), SLOW));
        let body = vec![0x5au8; 4096];
        let start = std::time::Instant::now();
        let (tee, result) = handler
            .setup_put_cache_tee(key, &HashMap::new(), false, Some(body.len() as u64))
            .await;
        let (tee, result) = (
            tee.expect("a slow setup must still hand the forward a tee"),
            result.expect("a slow setup must still hand the forward an S3-result sender"),
        );
        tee.send(Bytes::from(body.clone())).await.unwrap();
        drop(tee);
        let body_accepted = start.elapsed();
        assert!(
            body_accepted < std::time::Duration::from_millis(100),
            "the PUT body waited {body_accepted:?} on a {SLOW:?} cache setup before it could \
             be forwarded"
        );
        result.send(Ok(ok_response_info())).unwrap();
        let meta = cache_manager.get_new_metadata_file_path(key);
        let deadline = std::time::Instant::now() + SLOW + std::time::Duration::from_secs(8);
        while !meta.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .retain(|(k, _)| k != key);
        assert!(
            meta.exists(),
            "a PUT whose setup finished after its body arrived was not write-through cached"
        );
    }
    /// When setup is still running once the tee's buffer is full, the tee is dropped
    /// and the forward continues at full speed, uncached. It must never wait for setup.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_put_cache_setup_with_a_full_tee_drops_the_tee_without_stalling() {
        const SLOW: std::time::Duration = std::time::Duration::from_secs(2);
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("scn9-full");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            2,
        );
        handler.set_cache_manager(cache_manager.clone());
        let key = "test-bucket/scn9-full-tee-put";
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .push((key.to_string(), SLOW));
        let frames = 16usize;
        let before = super::setup_abandoned_total_put();
        let start = std::time::Instant::now();
        let (tee, result) = handler
            .setup_put_cache_tee(key, &HashMap::new(), false, Some((frames * 1024) as u64))
            .await;
        let tee = tee.expect("a slow setup must still hand the forward a tee");
        let mut closed = false;
        for _ in 0..frames {
            if tee.send(Bytes::from(vec![1u8; 1024])).await.is_err() {
                closed = true;
                break;
            }
        }
        drop(tee);
        let elapsed = start.elapsed();
        // The forward may wait up to WRITE_CACHE_SETUP_WAIT_BOUND for setup, no longer.
        let limit = super::WRITE_CACHE_SETUP_WAIT_BOUND + std::time::Duration::from_millis(500);
        assert!(
            elapsed < limit,
            "the forward waited {elapsed:?} for a cache setup to drain a full tee (limit \
             {limit:?})"
        );
        assert!(
            closed,
            "a full tee with setup still pending must be dropped"
        );
        if let Some(result) = result {
            let _ = result.send(Ok(ok_response_info()));
        }
        tokio::time::sleep(SLOW + std::time::Duration::from_secs(2)).await;
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .retain(|(k, _)| k != key);
        assert!(
            !cache_manager.get_new_metadata_file_path(key).exists(),
            "a PUT whose tee was dropped must not be cached"
        );
        assert!(
            super::setup_abandoned_total_put() > before,
            "a tee dropped for a pending setup must count in setup_abandoned_total_put"
        );
        assert_eq!(
            count_tmp_files(&cache_root),
            0,
            "a dropped tee left a .tmp behind"
        );
    }

    /// / TEE-DROPS-CACHING-BEFORE-A-HEALTHY-SETUP-FINISHES. A healthy setup takes tens of
    /// milliseconds on shared storage, while a fast client fills the tee's
    /// `depth + 2`-frame buffer in about a millisecond. A body of 64 frames into depth 5,
    /// behind a 50 ms setup, must wait briefly for setup rather than drop the tee, and the
    /// object must end write-through cached.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fast_body_behind_a_healthy_put_setup_is_still_cached() {
        const SETUP: std::time::Duration = std::time::Duration::from_millis(50);
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("scna-put");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            5,
        );
        handler.set_cache_manager(cache_manager.clone());
        let key = "test-bucket/scna-fast-body-put";
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .push((key.to_string(), SETUP));
        let frames = 64usize;
        let start = std::time::Instant::now();
        let (tee, result) = handler
            .setup_put_cache_tee(key, &HashMap::new(), false, Some((frames * 1024) as u64))
            .await;
        let tee = tee.expect("a PUT must get a tee");
        let result = result.expect("a PUT must get an S3-result sender");
        let mut sent = 0usize;
        for _ in 0..frames {
            if tee.send(Bytes::from(vec![7u8; 1024])).await.is_err() {
                break;
            }
            sent += 1;
        }
        drop(tee);
        let elapsed = start.elapsed();
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .retain(|(k, _)| k != key);
        assert_eq!(
            sent, frames,
            "the tee was dropped after {sent} of {frames} frames behind a {SETUP:?} setup"
        );
        let limit = super::WRITE_CACHE_SETUP_WAIT_BOUND + std::time::Duration::from_millis(500);
        assert!(
            elapsed < limit,
            "the forward took {elapsed:?} behind a {SETUP:?} setup (limit {limit:?})"
        );
        result.send(Ok(ok_response_info())).unwrap();
        let meta = cache_manager.get_new_metadata_file_path(key);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
        while !meta.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            meta.exists(),
            "a PUT behind a healthy {SETUP:?} setup was not write-through cached"
        );
    }

    /// An `UploadPart`'s setup must run beside the forward as the PUT's does. With a
    /// 300 ms part-sink open, the tee must be handed over at once and a 64-frame body into
    /// depth 5 must flow without the tee being dropped, and the part must be staged once the
    /// upstream succeeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upload_part_setup_runs_beside_the_body_and_still_stages_the_part() {
        const SETUP: std::time::Duration = std::time::Duration::from_millis(300);
        let temp_dir = TempDir::new().unwrap();
        let cache_root = temp_dir.path().join("scna-part");
        let cache_manager =
            initialized_accounting_cache_manager(cache_root.clone(), 1024 * 1024 * 1024).await;
        let mut handler = SignedPutHandler::new(
            cache_root.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            5,
        );
        handler.set_cache_manager(cache_manager.clone());
        let key = "test-bucket/scna-part";
        let upload_id = "scna-upload";
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .push((key.to_string(), SETUP));
        let frames = 64usize;
        let start = std::time::Instant::now();
        let (tee, result) = handler
            .setup_upload_part_cache_tee(
                key,
                upload_id,
                1,
                false,
                Some((frames * 1024) as u64),
                None,
            )
            .await;
        let handed_over = start.elapsed();
        let tee = tee.expect("an UploadPart must get a tee");
        let result = result.expect("an UploadPart must get an S3-result sender");
        let mut sent = 0usize;
        for _ in 0..frames {
            if tee.send(Bytes::from(vec![9u8; 1024])).await.is_err() {
                break;
            }
            sent += 1;
        }
        drop(tee);
        crate::cache::WRITE_CACHE_SETUP_STALL
            .lock()
            .unwrap()
            .retain(|(k, _)| k != key);
        assert!(
            handed_over < std::time::Duration::from_millis(100),
            "the UploadPart body waited {handed_over:?} on a {SETUP:?} part-sink open before \
             it could be forwarded"
        );
        assert_eq!(
            sent, frames,
            "the part tee was dropped after {sent} of {frames} frames behind a {SETUP:?} setup"
        );
        let mut response = ok_response_info();
        response
            .headers
            .insert("etag", "\"scna-part-etag\"".parse().unwrap());
        result.send(Ok(response)).unwrap();
        let part_bin = super::mpu_upload_dir(&cache_root, upload_id).join("part1.bin");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
        while !part_bin.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            part_bin.exists(),
            "an UploadPart behind a {SETUP:?} setup was not staged at {part_bin:?}"
        );
    }

    /// Control for the test above: a healthy cache writer receives every frame in
    /// order and the S3 result passes through unchanged, with no abandonment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn healthy_cache_writer_receives_every_frame_and_the_s3_result() {
        let super::BoundedCacheTee {
            forward_tx,
            mut cache_rx,
            s3_result_tx,
            s3_result_rx,
        } = super::bounded_cache_tee(2, std::time::Duration::from_secs(5), "scn7-healthy");
        let reader = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(f) = cache_rx.recv().await {
                got.push(f[0]);
            }
            got
        });
        let dropped = drive_forward_discipline(forward_tx, 300).await;
        assert!(
            !dropped,
            "a healthy writer must never cause the tee to drop"
        );
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), reader)
            .await
            .unwrap()
            .unwrap();
        let want: Vec<u8> = (0..300usize).map(|i| (i % 256) as u8).collect();
        assert_eq!(got, want, "frames must arrive complete and in order");
        s3_result_tx.send(Ok(ok_response_info())).unwrap();
        let delivered = tokio::time::timeout(std::time::Duration::from_secs(5), s3_result_rx)
            .await
            .unwrap();
        assert!(
            matches!(delivered, Ok(Ok(ref i)) if i.status == StatusCode::OK),
            "the S3 result must pass through unchanged"
        );
    }

    fn ok_response_info() -> ResponseInfo {
        let mut headers = HeaderMap::new();
        headers.insert("etag", "\"stream-etag\"".parse().unwrap());
        ResponseInfo {
            status: StatusCode::OK,
            headers,
            write_guard: WriteGuard::Unguarded,
        }
    }

    /// Build a minimal single-chunk aws-chunked body for `payload`.
    fn aws_chunked_single_chunk(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("{:x};chunk-signature=0\r\n", payload.len()).as_bytes());
        out.extend_from_slice(payload);
        out.extend_from_slice(b"\r\n0;chunk-signature=0\r\n\r\n");
        out
    }

    /// Non-chunked body: frames are object bytes; S3 success → Committed + `.bin`.
    #[tokio::test]
    async fn streaming_cache_non_chunked_commits() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-nonchunked";
        let object: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, (object.len() as u64) - 1);
        let sink = open_streaming_sink(dc, cache_key, object.len() as u64).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        // Feed the object in two frames, then close the channel and deliver success.
        tee_tx
            .send(Bytes::copy_from_slice(&object[..3000]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&object[3000..]))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);
        assert!(
            final_path.exists(),
            "committed .bin must exist: {:?}",
            final_path
        );
    }

    /// Regression for Requirement 12 (cache writes must not block the async runtime),
    /// acceptance criterion 12.4: the streamed write path, run under a
    /// worker-constrained runtime with concurrent large writes, must keep the runtime
    /// responsive rather than wedging it.
    ///
    /// Why this is deterministic and not a timing race:
    ///
    /// The original incident was that the drain loop's blocking `tee_rx.blocking_recv()`
    /// + blocking `sink.write` ran *inline on the async worker thread*. On the 2-worker
    /// default runtime, two concurrent large PUTs pinned both workers in synchronous
    /// writeback and starved everything else (the `/health` task, the SIGTERM handler).
    /// The fix moves that blocking work onto a `spawn_blocking` thread.
    ///
    /// This test reproduces the structural precondition rather than measuring latency:
    /// each write's body frames are produced by a *separate spawned async task*, and
    /// each write's S3 result is delivered by that same async task — i.e. completing a
    /// write *requires async tasks to be scheduled on a worker while the drain is
    /// running*. We launch more concurrent writes (`N = 4`) than worker threads (2).
    ///
    /// - With the fix: the four drains run on blocking threads, so the two workers stay
    ///   free to poll the four feeder tasks. Frames flow through the bounded
    ///   (depth-2) channels, the feeders deliver the S3 results, and all four writes
    ///   commit. The join completes.
    /// - With the bug (blocking work back on the workers): the first two drains scheduled
    ///   pin both workers inside `blocking_recv`; their feeder tasks can never be polled,
    ///   so no frame is ever sent, `blocking_recv` blocks forever, and the runtime
    ///   deadlocks. The join never completes.
    ///
    /// So correct code completes in milliseconds and a regression *cannot* complete at
    /// all. The `timeout` is only a safety net to turn that deadlock into a loud failure
    /// instead of a hung CI job; it is not a latency assertion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_cache_writes_do_not_block_async_workers() {
        // More concurrent writes than worker threads, so under the bug the drains
        // exhaust the worker pool and starve the feeders.
        const N: usize = 4;
        // Enough frames through a small bounded channel that the feeder must be
        // repeatedly re-polled (awaiting on a full channel) while the drain consumes —
        // exercising the worker/blocking-thread hand-off, not a single shot.
        const FRAMES: usize = 32;
        const FRAME_LEN: usize = 4096;
        let object_len = (FRAMES * FRAME_LEN) as u64;

        let temp_dir = TempDir::new().unwrap();
        let mut run_handles = Vec::with_capacity(N);

        for i in 0..N {
            let cache_key = format!("test-bucket/stream-noblock-{i}");
            // Distinct disk cache per sink (open takes the manager by value); all share
            // the temp dir with distinct keys.
            let dc = make_streaming_disk_cache(&temp_dir, FRAME_LEN).await;
            let sink = open_streaming_sink(dc, &cache_key, object_len).await;

            // Bounded, deliberately shallow so the feeder backpressures on `send`.
            let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(2);
            let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

            // Feeder: an independent async task. It can only make progress if a worker
            // thread is free to poll it — which is the whole property under test.
            tokio::spawn(async move {
                let frame = Bytes::from(vec![(i % 256) as u8; FRAME_LEN]);
                for _ in 0..FRAMES {
                    if tee_tx.send(frame.clone()).await.is_err() {
                        return;
                    }
                }
                drop(tee_tx);
                let _ = s3_tx.send(Ok(ok_response_info()));
            });

            // Runner: the real streamed write-cache path, spawned as its own task.
            run_handles.push(tokio::spawn(SignedPutHandler::run_streaming_cache_write(
                cache_key,
                sink,
                tee_rx,
                s3_rx,
                false,
                None,
                std::time::Duration::from_secs(3600),
                HashMap::new(),
                None,
                None,
                None,
            )));
        }

        // Safety net only: correct code finishes in milliseconds; a regression deadlocks
        // and would otherwise hang CI forever.
        let joined = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut outcomes = Vec::with_capacity(N);
            for h in run_handles {
                outcomes.push(h.await.unwrap());
            }
            outcomes
        })
        .await
        .expect(
            "streamed write path wedged the worker-constrained runtime: concurrent cache \
             writes blocked the async workers (Requirement 12.4 regression)",
        );

        assert_eq!(joined.len(), N);
        for outcome in joined {
            assert_eq!(
                outcome,
                StreamingCacheOutcome::Committed,
                "every concurrent streamed write must commit while the runtime stays responsive"
            );
        }
    }

    /// aws-chunked body: decoded incrementally, S3 success → Committed + `.bin`.
    #[tokio::test]
    async fn streaming_cache_aws_chunked_commits() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-chunked";
        let payload = b"hello streaming world!";
        let encoded = aws_chunked_single_chunk(payload);

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, (payload.len() as u64) - 1);
        let sink = open_streaming_sink(dc, cache_key, payload.len() as u64).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        // Split the encoded body across two frames to exercise incremental push.
        let split = encoded.len() / 2;
        tee_tx
            .send(Bytes::copy_from_slice(&encoded[..split]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&encoded[split..]))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            true,
            Some(payload.len() as u64),
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);
        assert!(
            final_path.exists(),
            "committed .bin must exist: {:?}",
            final_path
        );
    }

    /// chunked-request-framing (GitHub issue #19) R6/R10.5: a request carrying
    /// `x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER` WITHOUT
    /// `Content-Encoding: aws-chunked` must still be classified as aws-chunked,
    /// or the cache tee stages the raw aws-chunked framed bytes (chunk headers,
    /// CRLFs, the trailer section) as object data instead of decoding them —
    /// silent cache corruption rather than a visible error. This asserts on the
    /// CACHED CONTENT itself, per R10.5, since that is the actual harm: before
    /// the R6 fix, `aws_chunked_decoder::is_aws_chunked` returns `false` for
    /// this header set (detected today only by the `content-encoding` arm,
    /// confirmed absent from this request), so the caller passes
    /// `is_aws_chunked = false` into `run_streaming_cache_write`, which then
    /// writes the raw encoded frame straight to the sink as if it were plain
    /// object bytes. After the fix, the sentinel alone is enough to classify
    /// the request correctly and the cache holds the clean decoded payload.
    #[tokio::test]
    async fn streaming_cache_unsigned_payload_trailer_sentinel_alone_is_not_corrupted() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-sentinel-only";
        let payload = b"hello streaming world, sentinel only, no content-encoding!";
        let encoded = aws_chunked_single_chunk(payload);

        // The exact header shape pyarrow and the current AWS CLI CRT client
        // send (chunked-request-framing spec Phase 0.2/0.3 measurement):
        // `x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER` present,
        // `content-encoding` deliberately ABSENT — the sentinel is meant to be
        // sufficient on its own.
        let mut request_headers = HashMap::new();
        request_headers.insert(
            "x-amz-content-sha256".to_string(),
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_string(),
        );

        // This is the actual production call path: the caller derives
        // `is_aws_chunked` from the request headers via the classifier under
        // test, not from a hardcoded bool — so a classifier regression here
        // reproduces the real defect rather than a synthetic one.
        let is_aws_chunked_flag = aws_chunked_decoder::is_aws_chunked(&request_headers);
        assert!(
            is_aws_chunked_flag,
            "R6: the unsigned-payload-trailer sentinel alone must classify as \
             aws-chunked — if this assertion fails, the classifier fix regressed"
        );

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, (payload.len() as u64) - 1);
        let sink = open_streaming_sink(dc, cache_key, payload.len() as u64).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::copy_from_slice(&encoded)).await.unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            is_aws_chunked_flag,
            Some(payload.len() as u64),
            std::time::Duration::from_secs(3600),
            request_headers,
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);
        assert!(
            final_path.exists(),
            "committed .bin must exist: {:?}",
            final_path
        );

        // The headline assertion (R10.5): the CACHED CONTENT must be the clean
        // decoded payload, not the raw aws-chunked framed bytes (which would
        // contain the hex chunk-size header, CRLFs, and the trailer section
        // interleaved with the real data).
        let compressed_bytes =
            std::fs::read(&final_path).expect("committed range file must be readable");
        let compression_handler = CompressionHandler::new(1024, true);
        let cached_bytes = compression_handler
            .decompress_data(&compressed_bytes)
            .expect("committed range file must decompress (frame checksum verifies)");
        assert_eq!(
            cached_bytes,
            payload,
            "R6/R10.5: cached object content must be the decoded payload, not \
             the raw aws-chunked wire bytes — got {:?}",
            String::from_utf8_lossy(&cached_bytes)
        );
        assert!(
            !cached_bytes
                .windows(b"chunk-signature".len())
                .any(|w| w == b"chunk-signature"),
            "cached content must not contain literal aws-chunked framing bytes"
        );
    }

    /// Decoded length disagrees with the expected (x-amz-decoded-content-length):
    /// the sink is discarded, caching is skipped, and no `.bin` is published.
    #[tokio::test]
    async fn streaming_cache_decoded_length_mismatch_discards() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-mismatch";
        let payload = b"hello streaming world!"; // decodes to 22 bytes
        let encoded = aws_chunked_single_chunk(payload);
        let claimed_len: u64 = 9999; // deliberately wrong

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, claimed_len - 1);
        let sink = open_streaming_sink(dc, cache_key, claimed_len).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::copy_from_slice(&encoded)).await.unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            true,
            Some(claimed_len),
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(
            outcome,
            StreamingCacheOutcome::Skipped("decoded_length_mismatch")
        );
        assert!(
            !final_path.exists(),
            "mismatch must not publish a .bin: {:?}",
            final_path
        );
    }

    /// S3 failure: the sink is discarded, caching is skipped, no `.bin` published.
    #[tokio::test]
    async fn streaming_cache_s3_failure_discards() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-s3-failure";
        let object: Vec<u8> = vec![0x42u8; 4000];

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        let final_path = dc.get_new_range_file_path(cache_key, 0, (object.len() as u64) - 1);
        let sink = open_streaming_sink(dc, cache_key, object.len() as u64).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::copy_from_slice(&object)).await.unwrap();
        drop(tee_tx);
        // Deliver an S3 error result.
        s3_tx
            .send(Err(ProxyError::HttpError(
                "simulated upstream 500".to_string(),
            )))
            .unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Skipped("s3_error"));
        assert!(
            !final_path.exists(),
            "S3 failure must not publish a .bin: {:?}",
            final_path
        );
    }

    /// Cache-failure isolation (Task 4.2 / Req 7.1, 7.2): an aws-chunked decode
    /// error mid-drain must discard the sink, **close the tee receiver** so the
    /// forward loop drops the tee and keeps streaming verbatim, and skip caching —
    /// never publishing a `.bin` and never touching the upload. This is the Phase-1
    /// early-return path the 4.1 commit/mismatch/s3-failure tests do not exercise
    /// (those decode successfully or fail only after the channel is drained).
    #[tokio::test]
    async fn streaming_cache_decode_error_closes_tee() {
        let temp_dir = TempDir::new().unwrap();
        let cache_key = "test-bucket/stream-decode-error";
        // A complete, malformed chunk header line (non-hex size) → `push` errors on
        // the first frame, before any object bytes are written.
        let malformed = b"zzz;chunk-signature=0\r\n";

        let dc = make_streaming_disk_cache(&temp_dir, 4096).await;
        // The claimed length is irrelevant here; the decode error fires first.
        let final_path = dc.get_new_range_file_path(cache_key, 0, 99);
        let sink = open_streaming_sink(dc, cache_key, 100).await;

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        // Hold `s3_tx` without sending: the decode error returns in Phase 1, before
        // the S3 result is ever awaited.
        let (_s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::from_static(malformed)).await.unwrap();
        // Deliberately keep `tee_tx` alive so we can observe the receiver being
        // closed by the cache task (a real forward loop would see `Closed` on its
        // next send and drop the tee, continuing to stream verbatim).

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            true, // aws-chunked
            Some(100),
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            None,
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Skipped("decode_error"));
        assert!(
            tee_tx.is_closed(),
            "decode error must close the tee receiver so the forward loop drops the \
             tee and keeps forwarding verbatim"
        );
        assert!(
            !final_path.exists(),
            "decode error must not publish a .bin: {:?}",
            final_path
        );
    }

    /// Read-after-write parity — an immediate GET after a PUT must hit: a streamed PUT
    /// committed through `run_streaming_cache_write` against a real `CacheManager`
    /// must write the `.meta` **immediately**, so an immediate post-PUT GET is a
    /// cache hit. This guards the parity fix that finalizes the range and stores the
    /// `.meta` synchronously (via `CacheManager::store_streamed_write_cache_metadata`)
    /// rather than using the journal-only `WriteCacheRangeSink::commit`, which would
    /// defer the `.meta` until consolidation and make that GET a miss.
    #[tokio::test]
    async fn streaming_cache_writes_meta_immediately_for_read_after_write_hit() {
        use crate::cache::CacheManager;
        use crate::cache_types::NewCacheMetadata;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false, // ram_cache_enabled — disabled; .meta on disk is what a GET reads
            0,     // RAM cache size
            1024,  // compression_threshold
            true,  // compression_enabled
        ));
        // Wire the journal consolidator into the manager (required before
        // `initialize`, and what `create_configured_disk_cache_manager` does for the
        // real proxy startup path).
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-read-after-write";
        let object: Vec<u8> = (0..6000u32).map(|i| (i % 97) as u8).collect();

        // Open the sink the same way the single-PUT handler does (Task 5.1).
        let sink = cache_manager
            .open_write_cache_sink(cache_key, object.len() as u64)
            .await
            .expect("open_write_cache_sink should not error")
            .expect("capacity should be available for the sink");

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();

        tee_tx.send(Bytes::copy_from_slice(&object)).await.unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_cache_write(
            cache_key.to_string(),
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            std::time::Duration::from_secs(3600),
            HashMap::new(),
            None,
            Some(cache_manager.clone()),
            None,
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);

        // The `.meta` must exist immediately — this is what makes a post-PUT GET hit.
        let meta_path = cache_manager.get_new_metadata_file_path(cache_key);
        assert!(
            meta_path.exists(),
            "streamed PUT must write .meta immediately for read-after-write parity: {:?}",
            meta_path
        );

        let metadata: NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert!(
            metadata.object_metadata.is_write_cached,
            "streamed PUT .meta must be marked write-cached"
        );
        assert_eq!(
            metadata.object_metadata.content_length,
            object.len() as u64,
            "cached object length must match the streamed object"
        );
        assert_eq!(
            metadata.ranges.len(),
            1,
            "streamed PUT must produce exactly one range (0..len-1)"
        );
        assert_eq!(metadata.ranges[0].start, 0);
        assert_eq!(metadata.ranges[0].end, object.len() as u64 - 1);
    }

    // =========================================================================
    // Streaming part-cache task tests (Task 6.1)
    //
    // Exercise `run_streaming_part_cache_write` directly (the spawned wrapper just
    // discards its outcome) over a real `MultipartPartSink` backed by a temp cache.
    // They cover: non-chunked stage+record + LZ4 round-trip, aws-chunked decode +
    // decoded-length validation, and S3-failure → discard.
    // =========================================================================

    /// A streamed non-chunked `UploadPart` commits: the part `.bin` is published in
    /// the upload's in-progress dir, the `upload.meta` tracker records the part
    /// (number, decoded size, S3 ETag), and the staged bytes round-trip through the
    /// LZ4 frame decoder — the concatenated-frame format the GET-side range loader
    /// reads after the part is linked into the object.
    #[tokio::test]
    async fn streaming_part_cache_non_chunked_records_part_and_round_trips() {
        use crate::cache::CacheManager;
        use std::io::Read;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false,
            0,
            1024,
            true,
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-part-object";
        let upload_id = "upload-stream-1";
        let part_number = 2u32;
        let object: Vec<u8> = (0..7000u32).map(|i| (i % 131) as u8).collect();

        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number, None)
            .await
            .expect("open_multipart_part_sink should succeed")
            .expect("the capacity guard admits with an unconfigured floor");

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        tee_tx
            .send(Bytes::copy_from_slice(&object[..4000]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&object[4000..]))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            temp_dir.path().to_path_buf(),
            None,
            None, // capacity_budget: Cache decision, already size-gated
            None, // pending_permit
        )
        .await;
        assert_eq!(outcome, StreamingCacheOutcome::Committed);

        let upload_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        let part_path = upload_dir.join(format!("part{}.bin", part_number));
        assert!(
            part_path.exists(),
            "part file must be published: {:?}",
            part_path
        );

        let tracker = tracker_from_disk(&upload_dir, upload_id, cache_key);
        assert_eq!(tracker.parts.len(), 1);
        assert_eq!(tracker.parts[0].part_number, part_number);
        assert_eq!(
            tracker.parts[0].size,
            object.len() as u64,
            "tracker must record the decoded part size"
        );
        assert!(part_etags_match(&tracker.parts[0].etag, "stream-etag"));

        // The staged bytes round-trip through the LZ4 frame decoder (concatenated
        // frames), proving the part file is readable by the GET-side range loader.
        let compressed = std::fs::read(&part_path).unwrap();
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(
            decoded, object,
            "decompressed part bytes must equal the streamed object"
        );
    }

    /// A streamed aws-chunked `UploadPart` decodes incrementally: the cached part is
    /// the decoded payload (not the chunked wire bytes), the decoded-length check
    /// against `x-amz-decoded-content-length` passes, and the tracker records the
    /// decoded size.
    #[tokio::test]
    async fn streaming_part_cache_aws_chunked_decodes_and_records() {
        use crate::cache::CacheManager;
        use std::io::Read;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false,
            0,
            1024,
            true,
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-part-chunked";
        let upload_id = "upload-stream-chunked";
        let part_number = 3u32;
        let payload: Vec<u8> = (0..3000u32).map(|i| (i % 199) as u8).collect();
        let chunked = aws_chunked_single_chunk(&payload);

        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number, None)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        tee_tx.send(Bytes::copy_from_slice(&chunked)).await.unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            true,
            Some(payload.len() as u64),
            temp_dir.path().to_path_buf(),
            None,
            None, // capacity_budget: Cache decision, already size-gated
            None, // pending_permit
        )
        .await;
        assert_eq!(outcome, StreamingCacheOutcome::Committed);

        let upload_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        let tracker = tracker_from_disk(&upload_dir, upload_id, cache_key);
        assert_eq!(
            tracker.parts[0].size,
            payload.len() as u64,
            "tracker must record the DECODED payload length, not the chunked wire length"
        );

        let part_path = upload_dir.join(format!("part{}.bin", part_number));
        let compressed = std::fs::read(&part_path).unwrap();
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(
            decoded, payload,
            "cached part must be the decoded payload, not the aws-chunked wire bytes"
        );
    }

    /// On S3 failure the streamed part is discarded (Req 7.1): no `part{N}.bin` is
    /// published and no `upload.meta` tracker is written — the per-part correctness
    /// gate (commit only on S3 success) holds on the streamed path.
    #[tokio::test]
    async fn streaming_part_cache_s3_error_discards() {
        use crate::cache::CacheManager;

        let temp_dir = TempDir::new().unwrap();
        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false,
            0,
            1024,
            true,
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let cache_key = "test-bucket/stream-part-err";
        let upload_id = "upload-stream-err";
        let part_number = 1u32;

        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number, None)
            .await
            .unwrap()
            .expect("the capacity guard admits with an unconfigured floor");

        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        tee_tx
            .send(Bytes::from_static(b"some part bytes"))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx
            .send(Err(ProxyError::HttpError("boom".to_string())))
            .unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            temp_dir.path().to_path_buf(),
            None,
            None, // capacity_budget: Cache decision, already size-gated
            None, // pending_permit
        )
        .await;
        assert_eq!(outcome, StreamingCacheOutcome::Skipped("s3_error"));

        let upload_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        assert!(
            !upload_dir.join(format!("part{}.bin", part_number)).exists(),
            "no part file should be published on S3 error"
        );
        assert!(
            !upload_dir.join("upload.meta").exists(),
            "no tracker should be written on S3 error"
        );
    }

    // =========================================================================
    // Bounding the `StreamWithCapacityCheck` arm of `UploadPart` (R16.8)
    //
    // `check_cache_capacity` returns `StreamWithCapacityCheck` when the request
    // carries no usable Content-Length. That decision used to share an arm with
    // `CacheDecision::Cache`, so such a part staged with no capacity consideration
    // of any kind: the part sink is not pre-sized, does not reserve write-cache
    // capacity, and `open_multipart_part_sink` runs no `disk_safety_refusal`.
    //
    // Neither `None` case is reachable through the AWS CLI, which always sets a
    // valid Content-Length, so these fixtures build the request state directly.
    // =========================================================================

    /// Both ways a part's Content-Length can go missing land on the unbounded
    /// decision: header absent, and header present but unparseable. `parse::<u64>()`
    /// with `.ok()` discards the error, so a malformed value is indistinguishable
    /// from an absent one downstream — which is why the bound has to cover both.
    #[test]
    fn part_without_usable_content_length_takes_the_streaming_decision() {
        let temp_dir = TempDir::new().unwrap();
        let handler = create_test_handler(&temp_dir);

        // Sub-case 1: header absent.
        let absent: HashMap<String, String> = HashMap::new();
        assert_eq!(part_content_length(&absent), None, "absent header");
        assert_eq!(
            handler.should_cache(part_content_length(&absent)),
            CacheDecision::StreamWithCapacityCheck
        );

        // Sub-case 2: header present but unparseable. Each of these is a distinct
        // way to defeat `parse::<u64>()`.
        for bad in [
            "",
            "not-a-number",
            "-1",
            "1.5",
            "12abc",
            "99999999999999999999",
        ] {
            let mut headers: HashMap<String, String> = HashMap::new();
            headers.insert("content-length".to_string(), bad.to_string());
            assert_eq!(
                part_content_length(&headers),
                None,
                "unparseable Content-Length {bad:?} must not yield a length"
            );
            assert_eq!(
                handler.should_cache(part_content_length(&headers)),
                CacheDecision::StreamWithCapacityCheck,
                "unparseable Content-Length {bad:?} must take the streaming decision"
            );
        }

        // Control: a parseable value that fits does NOT take this decision, so the
        // two sub-cases above are attributable to the parse failing rather than to
        // every part taking the same route.
        let mut good: HashMap<String, String> = HashMap::new();
        good.insert("content-length".to_string(), "1024".to_string());
        assert_eq!(part_content_length(&good), Some(1024));
        assert_eq!(
            handler.should_cache(Some(1024)),
            CacheDecision::Cache,
            "a part that fits must be gated by the Content-Length arithmetic, not the budget"
        );
    }

    /// Fixture for the three budget cases below. Stages `body` through the real
    /// streaming part path with the supplied budget and returns the outcome plus the
    /// upload directory, so the caller can assert on published files too.
    async fn stage_part_with_budget(
        temp_dir: &TempDir,
        cache_key: &str,
        upload_id: &str,
        body: &[u8],
        capacity_budget: Option<StreamingCapacityBudget>,
    ) -> (StreamingCacheOutcome, PathBuf) {
        use crate::cache::CacheManager;

        let cache_manager = Arc::new(CacheManager::new(
            temp_dir.path().to_path_buf(),
            false,
            0,
            1024,
            true,
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();

        let part_number = 1u32;
        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number, None)
            .await
            .expect("open_multipart_part_sink should succeed")
            .expect("the capacity guard admits with an unconfigured floor");

        // Two frames, so a budget breach can be observed mid-part rather than only
        // at the end.
        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        let split = body.len() / 2;
        tee_tx
            .send(Bytes::copy_from_slice(&body[..split]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&body[split..]))
            .await
            .unwrap();
        drop(tee_tx);
        // S3 succeeds in every case, so an S3-side skip cannot be mistaken for the
        // capacity verdict.
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            temp_dir.path().to_path_buf(),
            None,
            capacity_budget,
            None, // pending_permit
        )
        .await;

        let upload_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        (outcome, upload_dir)
    }

    /// A part with no usable Content-Length is refused when staging it would take
    /// the cache past its maximum, and the refusal is `check_streaming_capacity`'s.
    ///
    /// **Validates: Requirements 16.8, 16.13, 16.15**
    #[tokio::test]
    async fn no_content_length_part_is_refused_when_over_budget() {
        const BODY_LEN: usize = 8000;
        const MAX_CAPACITY: u64 = 10_000;
        const CURRENT_USAGE: u64 = 9_000;

        // Preconditions, asserted rather than assumed. The predicate under test is
        // `check_streaming_capacity`'s `current_usage + bytes_written > max_capacity`
        // in `src/capacity_manager.rs`; these assertions exist so that a later change
        // to any neighbouring constant cannot make the arm vacuous by refusing the
        // fixture for a different reason.
        assert!(
            CURRENT_USAGE + BODY_LEN as u64 > MAX_CAPACITY,
            "fixture must breach the predicate under test"
        );
        assert!(
            BODY_LEN as u64 <= MAX_CAPACITY,
            "fixture must fit in an empty cache, so the refusal is attributable to \
             accumulated usage rather than to the part exceeding the whole cache"
        );
        // Guards `WriteCacheManager::try_reserve`'s object-size refusal
        // (`src/write_cache_manager.rs`, the `size > self.max_object_size` branch).
        // That gate is not on the part path today; if it is ever added, this
        // assertion is what stops the test passing for its reason instead of ours.
        assert!(
            BODY_LEN as u64 <= crate::config::CacheConfig::default().write_cache_max_object_size,
            "fixture must be under the write-cache per-object cap"
        );

        let temp_dir = TempDir::new().unwrap();
        let body: Vec<u8> = (0..BODY_LEN).map(|i| (i % 251) as u8).collect();

        let (outcome, upload_dir) = stage_part_with_budget(
            &temp_dir,
            "test-bucket/part-over-budget",
            "upload-over-budget",
            &body,
            Some(StreamingCapacityBudget {
                current_usage: CURRENT_USAGE,
                max_capacity: MAX_CAPACITY,
            }),
        )
        .await;

        assert_eq!(
            outcome,
            StreamingCacheOutcome::Skipped("capacity_exceeded"),
            "an over-budget part with no Content-Length must be refused by \
             check_streaming_capacity"
        );
        assert!(
            !upload_dir.join("part1.bin").exists(),
            "a refused part must not be published"
        );
        assert!(
            !upload_dir.join("upload.meta").exists(),
            "a refused part must not be recorded in the tracker"
        );
    }

    /// The same fixture with room to spare commits, so the budget is a bound rather
    /// than a blanket refusal.
    ///
    /// **Validates: Requirements 16.8**
    #[tokio::test]
    async fn no_content_length_part_within_budget_still_commits() {
        const BODY_LEN: usize = 8000;
        const MAX_CAPACITY: u64 = 10_000_000;
        const CURRENT_USAGE: u64 = 9_000;

        assert!(
            CURRENT_USAGE + BODY_LEN as u64 <= MAX_CAPACITY,
            "fixture must satisfy the predicate under test"
        );

        let temp_dir = TempDir::new().unwrap();
        let body: Vec<u8> = (0..BODY_LEN).map(|i| (i % 251) as u8).collect();

        let (outcome, upload_dir) = stage_part_with_budget(
            &temp_dir,
            "test-bucket/part-within-budget",
            "upload-within-budget",
            &body,
            Some(StreamingCapacityBudget {
                current_usage: CURRENT_USAGE,
                max_capacity: MAX_CAPACITY,
            }),
        )
        .await;

        assert_eq!(outcome, StreamingCacheOutcome::Committed);
        assert!(upload_dir.join("part1.bin").exists());
    }

    /// The red side, kept as a control: with **no** budget the identical
    /// over-capacity fixture commits. That is what the combined arm did for every
    /// no-Content-Length part, and it is what makes the refusal above attributable
    /// to the budget alone — the only input that differs between the two tests.
    ///
    /// **Validates: Requirements 16.15**
    #[tokio::test]
    async fn part_without_a_budget_stages_regardless_of_capacity() {
        let temp_dir = TempDir::new().unwrap();
        // Same 8000 bytes that the over-budget test refuses.
        let body: Vec<u8> = (0..8000).map(|i| (i % 251) as u8).collect();

        let (outcome, upload_dir) = stage_part_with_budget(
            &temp_dir,
            "test-bucket/part-no-budget",
            "upload-no-budget",
            &body,
            None,
        )
        .await;

        assert_eq!(
            outcome,
            StreamingCacheOutcome::Committed,
            "with no budget the part stages regardless of capacity — the behaviour \
             R16.8 replaces for the no-Content-Length case"
        );
        assert!(upload_dir.join("part1.bin").exists());
    }

    /// Build deterministic incompressible data with the same operation as
    /// `head -c N /dev/zero | openssl enc -aes-256-ctr`. Zero-filled bytes would
    /// compress away and would not exercise the disk volume that multipart staging
    /// actually consumes; `/dev/urandom` would add an unnecessary fixture bottleneck.
    fn openssl_ctr_fixture(bytes: usize) -> Vec<u8> {
        use std::process::Command;

        let command = format!(
            "head -c {bytes} /dev/zero | openssl enc -aes-256-ctr \\
             -K 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \\
             -iv 000102030405060708090a0b0c0d0e0f"
        );
        let output = Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
            .expect("openssl must be available for the incompressible fixture");
        assert!(
            output.status.success(),
            "openssl fixture generation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            output.stdout.len(),
            bytes,
            "AES-CTR must preserve the fixture length"
        );
        output.stdout
    }

    async fn admission_current_cache_usage(cache_manager: &crate::cache::CacheManager) -> u64 {
        cache_manager
            .get_cache_size_stats()
            .await
            .expect("admission snapshot must be readable")
            .sizes
            .expect("get_cache_size_stats must populate CacheSizes")
            .total_cache_size
    }

    async fn skipped_put_count(metrics: &Arc<RwLock<MetricsManager>>, reason: &str) -> u64 {
        metrics
            .read()
            .await
            .collect_metrics()
            .await
            .signed_put
            .expect("signed PUT metrics must be present")
            .skipped_puts_total
            .get(reason)
            .copied()
            .unwrap_or(0)
    }

    async fn initialized_accounting_cache_manager(
        cache_dir: std::path::PathBuf,
        max_cache_size: u64,
    ) -> Arc<crate::cache::CacheManager> {
        // The floor `new_with_shared_storage` itself defaults to, passed explicitly so
        // this helper's behaviour is unchanged by the `_with_floor` variant below.
        initialized_accounting_cache_manager_with_floor(
            cache_dir,
            max_cache_size,
            crate::cache::DISK_SAFETY_FREE_SPACE_FLOOR_BYTES,
        )
        .await
    }

    /// As above, but with `cache.disk_safety_free_space_floor` set explicitly.
    ///
    /// The floor is the *only* input to component 2 of `disk_safety_refusal` that a test
    /// can move — free space itself is a property of the host filesystem — so it is how
    /// R16.14's fixture constrains **free space** rather than configured size. `u64::MAX`
    /// makes the component unsatisfiable on any volume; `0` disables it entirely
    /// (`src/cache.rs`, `disk_safety_refusal_inner`), which is what the control arm needs.
    ///
    /// Set before the `Arc`, because the setter takes `&mut self`.
    async fn initialized_accounting_cache_manager_with_floor(
        cache_dir: std::path::PathBuf,
        max_cache_size: u64,
        free_space_floor: u64,
    ) -> Arc<crate::cache::CacheManager> {
        let mut cache_manager = crate::cache::CacheManager::new_with_shared_storage(
            cache_dir,
            false,
            0,
            max_cache_size,
            crate::cache::CacheEvictionAlgorithm::default(),
            1,
            true,
            std::time::Duration::from_secs(315_360_000),
            std::time::Duration::from_secs(3_600),
            std::time::Duration::from_secs(3_600),
            false,
            crate::config::SharedStorageConfig::default(),
            10.0,
            true,
            std::time::Duration::from_secs(86_400),
            crate::config::MetadataCacheConfig::default(),
            95,
            80,
            true,
            std::time::Duration::from_secs(60),
            1_048_576,
            false,
            std::time::Duration::from_secs(10),
            64,
            std::time::Duration::from_secs(5),
        );
        cache_manager.set_disk_safety_free_space_floor(free_space_floor);
        let cache_manager = Arc::new(cache_manager);
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();
        cache_manager
    }

    async fn stage_accounting_part(
        cache_manager: &Arc<crate::cache::CacheManager>,
        cache_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
        metrics: Arc<RwLock<MetricsManager>>,
    ) {
        let sink = cache_manager
            .open_multipart_part_sink(cache_key, upload_id, part_number, None)
            .await
            .expect("multipart part sink must open")
            .expect("the capacity guard admits with an unconfigured floor");
        let (tee_tx, tee_rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let (s3_tx, s3_rx) = tokio::sync::oneshot::channel::<Result<ResponseInfo>>();
        let split = body.len() / 2;
        tee_tx
            .send(Bytes::copy_from_slice(&body[..split]))
            .await
            .unwrap();
        tee_tx
            .send(Bytes::copy_from_slice(&body[split..]))
            .await
            .unwrap();
        drop(tee_tx);
        s3_tx.send(Ok(ok_response_info())).unwrap();

        let outcome = SignedPutHandler::run_streaming_part_cache_write(
            cache_key.to_string(),
            upload_id.to_string(),
            part_number,
            sink,
            tee_rx,
            s3_rx,
            false,
            None,
            cache_dir.to_path_buf(),
            Some(metrics),
            None,
            None, // pending_permit
        )
        .await;
        assert_eq!(
            outcome,
            StreamingCacheOutcome::Committed,
            "the real multipart staging path must publish the part before measuring admission"
        );
    }

    /// R16.9's red side: parts written below `mpus_in_progress/` are invisible to
    /// the `current_cache_usage` snapshot `HttpProxy` passes to
    /// `check_cache_capacity`. The accounting fix belongs to Phase 3, so this test
    /// remains ignored until that phase makes the assertion pass.
    ///
    /// The single-PUT positive controls prove the two skipped-PUT label observers
    /// work on their only producers. The multipart body then commits through
    /// `open_multipart_part_sink` under the same metrics handle and neither label
    /// changes, establishing that those labels cannot report multipart staging.
    ///
    /// **Validates: Requirements 16.2, 16.5, 16.9, 16.12**
    #[tokio::test]
    #[ignore = "R16.9 red-side regression: multipart staging is not yet accounted"]
    async fn multipart_staging_moves_disk_bytes_without_moving_admission_usage() {
        const PARTS: u32 = 3;
        const PART_BYTES: usize = 1024 * 1024;
        const MAX_CACHE_CAPACITY: u64 = 16 * 1024 * 1024;
        const MAX_OBJECT_SIZE: u64 = 256 * 1024 * 1024;

        let temp_dir = TempDir::new().unwrap();
        let metrics = Arc::new(RwLock::new(MetricsManager::new()));

        // Prove each counter can move on its real single-PUT producer before using
        // it as a negative observation for multipart staging. No body is sent: the
        // decision happens while `setup_put_cache_tee` opens its sink.
        let object_manager = initialized_accounting_cache_manager(
            temp_dir.path().join("object-too-large"),
            3 * MAX_OBJECT_SIZE,
        )
        .await;
        let mut object_put = SignedPutHandler::new(
            temp_dir.path().to_path_buf(),
            (*object_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        object_put.set_cache_manager(object_manager);
        object_put.set_metrics_manager(metrics.clone());
        let (tee, result) = object_put
            .setup_put_cache_tee(
                "test-bucket/object-too-large",
                &HashMap::new(),
                false,
                Some(MAX_OBJECT_SIZE + 1),
            )
            .await;
        assert!(tee.is_none() && result.is_none());
        assert_eq!(skipped_put_count(&metrics, "object_too_large").await, 1);

        let disk_manager =
            initialized_accounting_cache_manager(temp_dir.path().join("disk-safety"), 1).await;
        let mut disk_put = SignedPutHandler::new(
            temp_dir.path().to_path_buf(),
            (*disk_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        disk_put.set_cache_manager(disk_manager);
        disk_put.set_metrics_manager(metrics.clone());
        let (tee, result) = disk_put
            .setup_put_cache_tee("test-bucket/disk-safety", &HashMap::new(), false, Some(2))
            .await;
        assert!(tee.is_none() && result.is_none());
        assert_eq!(skipped_put_count(&metrics, "disk_safety").await, 1);

        let cache_manager = initialized_accounting_cache_manager(
            temp_dir.path().join("multipart-staging"),
            MAX_CACHE_CAPACITY,
        )
        .await;
        let body = openssl_ctr_fixture(PART_BYTES);
        let before = admission_current_cache_usage(&cache_manager).await;
        let disk_safety_before = skipped_put_count(&metrics, "disk_safety").await;
        let object_too_large_before = skipped_put_count(&metrics, "object_too_large").await;

        for part_number in 1..=PARTS {
            let current_cache_usage = admission_current_cache_usage(&cache_manager).await;
            assert!(
                matches!(
                    check_cache_capacity(
                        Some(PART_BYTES as u64),
                        current_cache_usage,
                        MAX_CACHE_CAPACITY,
                    ),
                    CacheDecision::Cache
                ),
                "part {part_number} must reach multipart staging through the same admission \
                 arithmetic HttpProxy uses"
            );
            stage_accounting_part(
                &cache_manager,
                temp_dir.path(),
                "test-bucket/multipart-accounting",
                "multipart-accounting-upload",
                part_number,
                &body,
                metrics.clone(),
            )
            .await;
        }

        assert_eq!(
            skipped_put_count(&metrics, "disk_safety").await,
            disk_safety_before,
            "disk_safety has no producer on the multipart part path"
        );
        assert_eq!(
            skipped_put_count(&metrics, "object_too_large").await,
            object_too_large_before,
            "object_too_large has no producer on the multipart part path"
        );

        let after = admission_current_cache_usage(&cache_manager).await;
        let staged_bytes = PARTS as u64 * PART_BYTES as u64;
        assert!(
            after >= before + staged_bytes,
            "R16.9: current_cache_usage (the value check_cache_capacity reads) must include \
             the {staged_bytes} bytes staged below mpus_in_progress; before={before}, after={after}"
        );
    }

    // =========================================================================
    // R16.14 — free space, not configured size, at `CreateMultipartUpload`
    //
    // R16.10's guard refuses write-through caching of a whole multipart upload,
    // once, at `CreateMultipartUpload`, because staged parts land under
    // `mpus_in_progress/` where eviction cannot reach them. R16.11 records the
    // refusal on `skipped_puts_total["disk_safety"]`. Both ship; R16.14's fixture
    // did not exist until this pair.
    //
    // WHAT MAKES THIS DIFFERENT FROM THE TWO NEAREST TESTS, neither of which
    // covers it:
    //
    //   * `multipart_staging_moves_disk_bytes_without_moving_admission_usage`
    //     above constrains `max_cache_size` to 1 — that is *configured size*,
    //     component 1 — and then asserts `disk_safety` **cannot** move on the
    //     multipart PART path. That is the opposite observation on a different
    //     component of a different path.
    //   * Task 36's A4 fixtures force declines at the seven *publication*
    //     boundaries. A refusal to create the upload tracker at all is not a
    //     publication boundary.
    //
    // WHY THE PRECONDITION ASSERTIONS ARE LOAD-BEARING. `disk_safety_refusal(0)`
    // can return `Some` for either of two reasons — component 1 (`total + 0 >
    // max_cache_size`) or component 2 (`available < 0 + floor`) — and the
    // `self.cache_manager: None` arm refuses nothing at all. Asserting "it
    // refused" therefore asserts none of the three. The two arms below differ in
    // exactly one input, the floor, and the control proves the other two gates
    // are open, so a refusal in the subject arm is attributable to free space.
    // (`pre-push-checklist.md` § "Assert the predicate the code evaluates", and
    // § `capacity-bypass-check.sh` on a rejection with more than one cause.)
    // =========================================================================

    /// Mock upstream for `CreateMultipartUpload`: accept one connection, read to the
    /// end of the request headers, then reply `200 OK` with a real
    /// `InitiateMultipartUploadResult` carrying `upload_id`.
    ///
    /// `accept_and_ok` cannot be reused: it replies with an empty body, so
    /// `extract_upload_id_from_xml` would return `None` and no tracker would be
    /// written — which is the subject arm's expected outcome, and would make the
    /// control arm pass for the wrong reason.
    async fn accept_and_reply_initiate_multipart(
        listener: tokio::net::TcpListener,
        upload_id: String,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 8192];
        loop {
            let n = sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <InitiateMultipartUploadResult>\
             <Bucket>test-bucket</Bucket><Key>r16-14-object</Key>\
             <UploadId>{}</UploadId></InitiateMultipartUploadResult>",
            upload_id
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
            xml.len(),
            xml
        );
        let _ = sock.write_all(response.as_bytes()).await;
        let _ = sock.flush().await;
    }

    /// Drive one `CreateMultipartUpload` against a mock S3 through a handler whose
    /// cache manager carries `free_space_floor`. Returns the response status and the
    /// upload's in-progress directory.
    async fn create_multipart_with_free_space_floor(
        cache_dir: std::path::PathBuf,
        max_cache_size: u64,
        free_space_floor: u64,
        upload_id: &str,
        metrics: Arc<RwLock<MetricsManager>>,
    ) -> (
        StatusCode,
        std::path::PathBuf,
        Arc<crate::cache::CacheManager>,
    ) {
        let cache_manager = initialized_accounting_cache_manager_with_floor(
            cache_dir.clone(),
            max_cache_size,
            free_space_floor,
        )
        .await;

        let mut handler = SignedPutHandler::new(
            cache_dir.clone(),
            (*cache_manager.get_compression_handler()).clone(),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager.clone());
        handler.set_metrics_manager(metrics);

        // PRECONDITION: the handler holds a cache manager. The `None` arm of the
        // guard refuses nothing, so a forgotten `set_cache_manager` would make the
        // control arm pass for a reason that has nothing to do with free space —
        // and would make R16.11 satisfied-in-source and dead at runtime.
        assert!(
            handler.cache_manager.is_some(),
            "the guard's None arm refuses nothing; a handler without a cache manager \
             cannot evidence either side of R16.14"
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_and_reply_initiate_multipart(
            listener,
            upload_id.to_string(),
        ));

        let (req, _conn_keepalive) =
            incoming_request_with_method("POST", "/r16-14-object?uploads", Vec::new()).await;

        let transport = Arc::new(UpstreamTransport {
            ip: upstream_addr.ip(),
            port: upstream_addr.port(),
            tls: None,
            validated_endpoint: None,
        });

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handler.handle_create_multipart_upload(
                req,
                "test-bucket/r16-14-object".to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport,
            ),
        )
        .await
        .expect("handle_create_multipart_upload did not finish within 60s")
        .expect("CreateMultipartUpload must reach the mock upstream and return its response");

        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        let upload_dir = mpu_upload_dir(&cache_dir, upload_id);
        (response.status(), upload_dir, cache_manager)
    }

    /// R16.14: with free space below the floor, `CreateMultipartUpload` declines to
    /// write-through cache the upload — no tracker, and the refusal is recorded on
    /// `skipped_puts_total["disk_safety"]` — while the upload itself still reaches S3.
    ///
    /// The control arm is the same fixture with component 2 disabled, which is what
    /// makes the negative assertion attributable: without it, "no tracker" is
    /// produced equally by a missing cache manager, an unparseable upstream response,
    /// or a refusal for configured size.
    ///
    /// **Validates: Requirements 16.10, 16.11, 16.14**
    #[tokio::test]
    async fn create_multipart_upload_refuses_caching_when_free_space_is_below_the_floor() {
        // Generous, so component 1 (`total + incoming > max_cache_size`) cannot fire:
        // the cache is freshly created, so `total` is 0 and `incoming` is 0 at this
        // call site by construction (the object size is unknown here).
        const MAX_CACHE_SIZE: u64 = 10 * 1024 * 1024 * 1024;

        let temp_dir = TempDir::new().unwrap();
        let metrics = Arc::new(RwLock::new(MetricsManager::new()));

        // ---- Control arm: floor 0 disables component 2 entirely. ----
        let control_before =
            skipped_put_count(&metrics, crate::cache::DISK_SAFETY_SKIP_REASON).await;
        let (control_status, control_upload_dir, control_manager) =
            create_multipart_with_free_space_floor(
                temp_dir.path().join("control"),
                MAX_CACHE_SIZE,
                0,
                "r16-14-control-upload",
                metrics.clone(),
            )
            .await;

        // PRECONDITION, asserted rather than assumed: with component 2 disabled this
        // fixture is inside every other gate the guard applies. If a later change to
        // `max_cache_size` accounting, or to what `disk_safety_refusal` reads, made
        // component 1 fire here, this assertion fails naming the cause instead of the
        // subject arm silently passing for the wrong component.
        assert!(
            control_manager.disk_safety_refusal(0).await.is_none(),
            "fixture precondition: with the free-space floor disabled the upload must be \
             admitted, so the subject arm's refusal is attributable to free space alone"
        );
        assert_eq!(
            admission_current_cache_usage(&control_manager).await,
            0,
            "fixture precondition: a fresh cache must report zero usage, so component 1 \
             has the full {MAX_CACHE_SIZE} bytes of headroom"
        );

        assert_eq!(
            control_status,
            StatusCode::OK,
            "control: the mock upstream's CreateMultipartUpload response must reach the client"
        );
        assert!(
            control_upload_dir.join("upload.meta").exists(),
            "control: an admitted CreateMultipartUpload must write the upload tracker — \
             without this the subject arm's missing tracker evidences nothing"
        );
        assert_eq!(
            skipped_put_count(&metrics, crate::cache::DISK_SAFETY_SKIP_REASON).await,
            control_before,
            "control: an admitted upload must not record a disk_safety skip"
        );

        // ---- Subject arm: the floor is unsatisfiable on any volume. ----
        // This constrains FREE SPACE, not object size: `disk_safety_refusal_inner`
        // component 2 compares `fs4::available_space` against `incoming + floor`, and
        // `u64::MAX` exceeds any real volume's free bytes. `max_cache_size` is
        // identical to the control's, so configured size is held constant.
        let subject_before =
            skipped_put_count(&metrics, crate::cache::DISK_SAFETY_SKIP_REASON).await;
        let (subject_status, subject_upload_dir, _subject_manager) =
            create_multipart_with_free_space_floor(
                temp_dir.path().join("subject"),
                MAX_CACHE_SIZE,
                u64::MAX,
                "r16-14-subject-upload",
                metrics.clone(),
            )
            .await;

        // Assertion 1 of 2: no local tracker for the upload.
        assert!(
            !subject_upload_dir.join("upload.meta").exists(),
            "R16.14: a CreateMultipartUpload refused for free space must write no upload \
             tracker, or parts will stage into a cache that has no room for them"
        );
        assert!(
            !subject_upload_dir.exists(),
            "R16.14: the refusal must happen before the in-progress directory is created"
        );

        // Assertion 2 of 2: the refusal is observable, and observable as *this* cause.
        assert_eq!(
            skipped_put_count(&metrics, crate::cache::DISK_SAFETY_SKIP_REASON).await,
            subject_before + 1,
            "R16.11/R16.14: the refusal must increment skipped_puts_total[\"disk_safety\"], \
             so \"refused for space\" is distinguishable from \"refused for something else\""
        );

        // R16.10's stated contract: the upload itself is unaffected.
        assert_eq!(
            subject_status,
            StatusCode::OK,
            "R16.10: only write-through caching is declined — the CreateMultipartUpload \
             still reaches S3 and its response still reaches the client"
        );
    }

    // --- uploadId validation tests (Security: path traversal prevention) ---

    #[tokio::test]
    async fn test_cleanup_multipart_upload_rejects_traversal() {
        // Ensure cleanup_multipart_upload with a traversal uploadId does NOT
        // perform remove_dir_all outside the cache directory.
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        // Create a sentinel file outside the mpus_in_progress tree
        let sentinel = temp_dir.path().join("sentinel.txt");
        std::fs::write(&sentinel, "must survive").unwrap();

        // Even though cleanup doesn't validate (the handler does), confirm
        // that a traversal uploadId pointing at the sentinel's parent doesn't
        // delete the sentinel when the mpus_in_progress subdir doesn't exist.
        // This tests the defense-in-depth: the directory simply doesn't exist.
        let result = handler.cleanup_multipart_upload("../../sentinel.txt").await;
        assert!(result.is_ok());
        assert!(
            sentinel.exists(),
            "sentinel file must survive cleanup with traversal uploadId"
        );
    }

    #[tokio::test]
    async fn test_upload_id_validation_rejects_malicious_ids() {
        // Verify is_safe_path_component correctly rejects all dangerous patterns
        // that could be used as uploadId values for path traversal.

        // Traversal attempts
        assert!(!is_safe_path_component("../../etc/passwd"));
        assert!(!is_safe_path_component("../x"));
        assert!(!is_safe_path_component(".."));
        assert!(!is_safe_path_component("foo..bar"));

        // Path separators
        assert!(!is_safe_path_component("a/b"));
        assert!(!is_safe_path_component("a\\b"));

        // Control characters
        assert!(!is_safe_path_component("upload\x00id"));
        assert!(!is_safe_path_component("upload\nid"));

        // Empty
        assert!(!is_safe_path_component(""));

        // Valid S3-like uploadIds still pass
        assert!(is_safe_path_component(
            "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
        ));
        assert!(is_safe_path_component(
            "2Hoj0CxQnbMljdfMrU3bYHPJFSRPCmLzSHBfSIz4k"
        ));
        assert!(is_safe_path_component("normal-upload-id-123"));
    }

    #[tokio::test]
    async fn test_cache_upload_part_with_safe_upload_id_works() {
        // Normal uploadId still writes cache data (regression guard)
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);

        let cache_key = "test-bucket/test-object";
        let upload_id = "safe-upload-id-ABC123";
        let part_number = 1;
        let data = b"test data for safe uploadId";
        let etag = "test-etag-safe";

        let result = handler
            .cache_upload_part(cache_key, upload_id, part_number, data, etag)
            .await;
        assert!(result.is_ok());

        // Verify the upload directory was created in the correct location
        let multipart_dir = mpu_upload_dir(temp_dir.path(), upload_id);
        assert!(
            multipart_dir.exists(),
            "upload directory should be created for safe uploadId"
        );
    }

    #[tokio::test]
    async fn test_malicious_upload_id_no_directory_created() {
        // A malicious uploadId must NOT create any directory in the cache.
        // (Handler validation rejects before cache_upload_part is called, but
        // if the guard were bypassed, the path would be unsafe.)
        let temp_dir = TempDir::new().unwrap();

        // Create a sentinel directory outside mpus_in_progress
        let outside_dir = temp_dir.path().join("important_data");
        std::fs::create_dir_all(&outside_dir).unwrap();
        let sentinel = outside_dir.join("file.txt");
        std::fs::write(&sentinel, "critical data").unwrap();

        // The handler would reject "../../important_data" via is_safe_path_component
        // but verify the cleanup path also doesn't escape.
        let mut handler = create_test_handler(&temp_dir);
        let result = handler
            .cleanup_multipart_upload("../../important_data")
            .await;
        assert!(result.is_ok());

        // The sentinel must survive
        assert!(
            sentinel.exists(),
            "sentinel file outside cache must survive malicious uploadId cleanup"
        );
        assert!(
            outside_dir.exists(),
            "directory outside cache must survive malicious uploadId cleanup"
        );
    }

    // ========================================================================
    // An S3-issued uploadId containing `..` is write-through cached
    // (MPU-UPLOADID-WITH-DOTDOT-DECLINED-FROM-WRITE-THROUGH)
    // ========================================================================

    /// The shape S3 actually issues: 128 characters of `[A-Za-z0-9._-]` with a `..` in
    /// the middle. 4 of 40 real uploadIds created against a us-west-2 test bucket
    /// on 2026-09-25 contained `..`; none contained `/` or `\`.
    const DOTDOT_UPLOAD_ID: &str = "m0TmAnJ8zEI3xQ7vR2_kLp9WcHs4Nd6Yb1Gf5Ut8Ze3Ja0Mo7Ri2Xs9Kv4Pw6Dl1Cq8En5Bh3..Ty0Fg7Iu2Ok9Sn4Va6Lm1Wr8Hx3Qe5Zc0Jp7Gt2Yd9Mb4Ki6Uf1Aw";

    /// Mock upstream: accept one connection, drain the request headers plus exactly
    /// `body_len` body bytes, then write `response` verbatim.
    async fn accept_drain_and_reply(
        listener: tokio::net::TcpListener,
        body_len: usize,
        response: Vec<u8>,
    ) {
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
        let _ = sock.write_all(&response).await;
        let _ = sock.flush().await;
    }

    /// The narrowed uploadId guard admits every character class S3 issues, `..`
    /// included, and refuses only what would corrupt a log line or the tracker.
    #[test]
    fn cacheable_upload_id_admits_real_ids_and_refuses_control_and_empty() {
        assert!(is_cacheable_upload_id(DOTDOT_UPLOAD_ID));
        assert!(is_cacheable_upload_id(
            "2Hoj0CxQnbMljdfMrU3bYHPJFSRPCmLzSHBfSIz4k"
        ));
        assert!(
            is_cacheable_upload_id("a/b..c\\d"),
            "path bytes no longer matter"
        );
        assert!(!is_cacheable_upload_id(""));
        assert!(!is_cacheable_upload_id("upload\nid"), "log-line injection");
        assert!(!is_cacheable_upload_id("upload\x00id"));
        assert!(
            !is_cacheable_upload_id("upload id"),
            "space splits a log field"
        );
        assert!(!is_cacheable_upload_id("uploadé"));
        // The reject branch logs the refused id; a byte slice through a multi-byte
        // character would panic there.
        assert_eq!(truncate_upload_id("ééééééééééééé"), "éééééééééééé...");
        // Distinct ids never share a directory, and the name is a 64-hex digest.
        let a = mpu_upload_dir(std::path::Path::new("/c"), "x..y");
        let b = mpu_upload_dir(std::path::Path::new("/c"), "x..z");
        assert_ne!(a, b);
        assert_eq!(
            a.parent().unwrap(),
            std::path::Path::new("/c/mpus_in_progress")
        );
        assert_eq!(a.file_name().unwrap().len(), 64);
    }

    fn transport_for(addr: std::net::SocketAddr) -> Arc<UpstreamTransport> {
        Arc::new(UpstreamTransport {
            ip: addr.ip(),
            port: addr.port(),
            tls: None,
            validated_endpoint: None,
        })
    }

    /// Defect red side: `UploadPart` + `CompleteMultipartUpload` with a real-shape
    /// uploadId containing `..`, driven through the handler entry points (so through
    /// their uploadId guards), must produce a served cache entry.
    ///
    /// Before the fix both guards called `is_safe_path_component`, which rejects any `..`
    /// substring, so the part was forwarded without staging, Complete was forwarded
    /// without finalizing, and no `.meta` was written — the fleet signature
    /// `UploadPart: rejected unsafe uploadId=m0TmAnJ8zEI3...`.
    ///
    /// Predicate: the published `.meta` exists, describes the whole object, and its range
    /// loads back byte-exact. Bytes returned to the client prove nothing here — the
    /// declined path returns S3's response too.
    #[tokio::test]
    async fn upload_id_containing_dotdot_is_write_through_cached() {
        assert_eq!(DOTDOT_UPLOAD_ID.len(), 128, "fixture must be S3's length");
        assert!(
            DOTDOT_UPLOAD_ID.contains(".."),
            "fixture must carry the `..`"
        );
        assert!(
            !is_safe_path_component(DOTDOT_UPLOAD_ID),
            "fixture precondition: the path-component validator rejects this id, which is \
             exactly why it must not be the gate on caching"
        );

        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let cache_manager = Arc::new(crate::cache::CacheManager::new(
            cache_dir.clone(),
            false, // ram_cache_enabled
            0,     // max_ram_cache_size
            100,   // compression_threshold
            false, // compression_enabled
        ));
        let mut handler = SignedPutHandler::new(
            cache_dir.clone(),
            CompressionHandler::new(1024, false),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager.clone());

        let cache_key = "test-bucket/scnb-dotdot-object";
        let host = "test-bucket.s3.amazonaws.com".to_string();
        let part_body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let part_etag = "\"scnb-part-etag-1\"";

        // ---- UploadPart ----
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_drain_and_reply(
            listener,
            part_body.len(),
            format!("HTTP/1.1 200 OK\r\nETag: {part_etag}\r\nContent-Length: 0\r\n\r\n")
                .into_bytes(),
        ));
        let (req, _keepalive_part) = incoming_upload_part_request(
            &format!(
                "/scnb-dotdot-object?uploadId={}&partNumber=1",
                DOTDOT_UPLOAD_ID
            ),
            part_body.clone(),
        )
        .await;
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handler.handle_upload_part(
                req,
                cache_key.to_string(),
                host.clone(),
                transport_for(addr),
                DOTDOT_UPLOAD_ID.to_string(),
                1,
            ),
        )
        .await
        .expect("UploadPart did not finish within 60s")
        .expect("UploadPart must reach the mock upstream");
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        // The CreateMultipartUpload record, without which Complete does not cache.
        handler
            .create_multipart_upload_tracker_with_content_type(
                cache_key,
                DOTDOT_UPLOAD_ID,
                None,
                &HashMap::from([("content-type".to_string(), DEFAULT_CONTENT_TYPE.to_string())]),
            )
            .await
            .unwrap();

        // ---- CompleteMultipartUpload ----
        let complete_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
             <ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
        )
        .into_bytes();
        let result_xml = "<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket>\
                          <Key>scnb-dotdot-object</Key>\
                          <ETag>\"3858f62230ac3c915f300c664312c11f-1\"</ETag>\
                          </CompleteMultipartUploadResult>";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_drain_and_reply(
            listener,
            complete_body.len(),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                result_xml.len(),
                result_xml
            )
            .into_bytes(),
        ));
        let (req, _keepalive_complete) = incoming_request_with_method(
            "POST",
            &format!("/scnb-dotdot-object?uploadId={}", DOTDOT_UPLOAD_ID),
            complete_body,
        )
        .await;
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handler.handle_complete_multipart_upload(
                req,
                cache_key.to_string(),
                host,
                transport_for(addr),
                DOTDOT_UPLOAD_ID.to_string(),
            ),
        )
        .await
        .expect("CompleteMultipartUpload did not finish within 60s")
        .expect("CompleteMultipartUpload must reach the mock upstream");
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        // ---- The predicate: a served cache entry ----
        let metadata_file =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        assert!(
            metadata_file.exists(),
            "SCNb: a multipart upload whose uploadId contains `..` must be write-through \
             cached; no .meta at {:?}",
            metadata_file
        );
        let metadata: crate::cache_types::NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_file).unwrap()).unwrap();
        assert_eq!(metadata.object_metadata.parts_count, Some(1));
        assert_eq!(
            metadata.object_metadata.content_length,
            part_body.len() as u64
        );
        assert_eq!(metadata.ranges.len(), 1, "one part, one range");
        let disk = crate::disk_cache::DiskCacheManager::new(
            cache_dir.clone(),
            true,
            1024,
            false,
            1_048_576,
        );
        let served = disk
            .load_range_data(&metadata.ranges[0])
            .await
            .expect("the cached range must load");
        assert_eq!(
            served, part_body,
            "the cached range must serve the part's bytes"
        );

        // The staging directory lived under its digest, never under the raw id.
        assert!(
            !cache_dir
                .join("mpus_in_progress")
                .join(DOTDOT_UPLOAD_ID)
                .exists(),
            "no directory may be named after the raw uploadId"
        );
    }

    // ========================================================================
    // In-progress MPU part bytes (`P`) — cache-eviction-at-scale § 16.6.2
    //
    // Three properties: the recorded figure is the COMPRESSED on-disk length and
    // not the uncompressed S3-facing one; the sum spans uploads and parts; and the
    // part records are gone BEFORE `upload.lock` is released, which is an ordering
    // property that no end-state assertion can distinguish.
    // ========================================================================

    /// `compressed_size` on `part{N}.json` is the on-disk length of `part{N}.bin`,
    /// which is a DIFFERENT figure from `CachedPartInfo::size`.
    ///
    /// The payload is deliberately highly compressible. On incompressible data the two
    /// figures coincide and the test would pass against code that wrote either one, so
    /// the fixture asserts the gap exists before asserting which figure was recorded.
    ///
    /// A failure implicates the `std::fs::metadata(&part_file_path)` stat and the
    /// `.with_compressed_size(compressed_size)` call in
    /// `finalize_and_record_cached_part` (`src/signed_put_handler.rs`) — specifically,
    /// a `compressed_size` equal to `size` means the stat was replaced by the tracked
    /// uncompressed figure, which is the substitution § 16.6.2 forbids.
    #[tokio::test]
    async fn part_record_compressed_size_is_the_on_disk_length_not_the_uncompressed_size() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path();
        let cache_key = "test-bucket/compressible-part.bin";
        let upload_id = "upload-compressed-size";

        // 4 MiB of one repeated byte. LZ4 reduces this by orders of magnitude.
        let data = vec![0xABu8; 4 * 1024 * 1024];

        // Compression ENABLED, unlike `stage_streamed_part`'s manager — without it the
        // on-disk length equals the uncompressed length and the test is vacuous.
        let cache_mgr = crate::cache::CacheManager::new(
            cache_dir.to_path_buf(),
            false, // ram_cache_enabled
            0,     // max_ram_cache_size
            100,   // compression_threshold
            true,  // compression_enabled
        );
        let mut sink = cache_mgr
            .open_multipart_part_sink(cache_key, upload_id, 1, None)
            .await
            .expect("open streaming part sink")
            .expect("the capacity guard admits with an unconfigured floor");
        sink.write(&data).expect("write part bytes");
        SignedPutHandler::finalize_and_record_cached_part(
            cache_dir, cache_key, upload_id, 1, "etag1", sink, None,
        )
        .await
        .expect("finalize + record part");

        let multipart_dir = mpu_upload_dir(cache_dir, upload_id);
        let record: crate::cache_types::CachedPartInfo = serde_json::from_str(
            &std::fs::read_to_string(multipart_dir.join("part1.json")).unwrap(),
        )
        .expect("part record must parse");
        let on_disk = std::fs::metadata(multipart_dir.join("part1.bin"))
            .expect("part1.bin must exist after finalize")
            .len();

        // Fixture precondition: the two figures must actually differ, or nothing below
        // discriminates. Asserted before the verdict so a fixture that stopped
        // compressing fails naming itself rather than looking like a product pass.
        assert!(
            on_disk < data.len() as u64,
            "fixture: payload did not compress (on_disk={}, uncompressed={}); this test \
             cannot distinguish the two figures unless they differ",
            on_disk,
            data.len()
        );

        // The S3-facing figure stays uncompressed — this is the input the object's byte
        // offsets and `total_size` are built from, so it is asserted rather than assumed.
        assert_eq!(
            record.size,
            data.len() as u64,
            "CachedPartInfo::size must remain the UNCOMPRESSED length"
        );

        assert_eq!(
            record.compressed_size, on_disk,
            "compressed_size must be the stat-ed on-disk length of part1.bin"
        );
        assert_ne!(
            record.compressed_size, record.size,
            "compressed_size must not be sourced from CachedPartInfo::size"
        );
    }

    /// `in_progress_part_bytes` sums `compressed_size` across multiple uploads and
    /// multiple parts within an upload, and ignores everything that is not a part
    /// record.
    ///
    /// A failure implicates the free function `in_progress_part_bytes`
    /// (`src/signed_put_handler.rs`): a short total means a directory or record was
    /// skipped; a total larger than expected means the `part{N}.json` name filter is
    /// matching something else.
    #[tokio::test]
    async fn in_progress_part_bytes_sums_across_uploads_and_parts() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path();

        // upload-a: two parts; upload-b: three parts. Distinct values so a
        // double-count or an omission cannot land on the right total by chance.
        let fixture: [(&str, &[(u32, u64)]); 2] = [
            ("upload-a", &[(1, 1_000), (2, 20_000)]),
            ("upload-b", &[(1, 300_000), (2, 4_000_000), (7, 50_000_000)]),
        ];
        let mut expected = 0u64;

        for (upload_id, parts) in fixture {
            let multipart_dir = mpu_upload_dir(cache_dir, upload_id);
            std::fs::create_dir_all(&multipart_dir).unwrap();
            for (part_number, compressed) in parts {
                let info = crate::cache_types::CachedPartInfo::new(
                    *part_number,
                    // Uncompressed size deliberately different from the compressed one,
                    // so a sum that read the wrong field gives the wrong answer.
                    compressed * 3,
                    format!("etag-{}-{}", upload_id, part_number),
                    crate::compression::CompressionAlgorithm::Lz4,
                )
                .with_compressed_size(*compressed);
                SignedPutHandler::record_part_blocking(&multipart_dir, *part_number, &info)
                    .unwrap();
                expected += compressed;
            }
            // Decoys that must not contribute: the upload-level tracker, a lock file,
            // and a leftover temp record.
            std::fs::write(multipart_dir.join("upload.meta"), b"{}").unwrap();
            std::fs::write(multipart_dir.join("part1.lock"), b"").unwrap();
            std::fs::write(multipart_dir.join("part9.json.tmp"), b"{}").unwrap();
        }

        assert_eq!(
            in_progress_part_bytes(cache_dir),
            Some(expected),
            "must sum compressed_size over every part record of every in-flight upload"
        );

        // An absent tree is 0, not a panic — the common case on a proxy that has never
        // seen a multipart upload.
        let empty = TempDir::new().unwrap();
        assert_eq!(in_progress_part_bytes(empty.path()), Some(0));
    }

    /// A part record written **before `compressed_size` existed** reads as UNREADABLE, not
    /// as zero bytes.
    ///
    /// # The two figures that normally agree, made to diverge
    ///
    /// "Absent" and "measured zero" produce the identical `u64` through
    /// `CachedPartInfo`'s `#[serde(default)]`, which is why this defect was invisible: every
    /// assertion about the *total* holds equally well under either reading, because both add
    /// nothing. The state where they part company is a record with **no field at all**
    /// sitting beside a record with a real measurement — so this fixture writes exactly
    /// that, and asserts on the sum's *readability* rather than on its value, because the
    /// value under the defect is a perfectly plausible number.
    ///
    /// # A failure implicates the `Ok(Ok(None))` arm of `in_progress_part_bytes`
    ///
    /// and, behind it, `PartRecordSizeProbe::compressed_size` being an `Option`. That arm is
    /// the branch the code takes on field absence; the assertion reads the function's
    /// return value, which is the value the fold's Complete_Install check branches on
    /// (`FoldRetainReason::InProgressPartsUnreadable`) — not a derived gauge downstream of
    /// it.
    ///
    /// # Direction-safety is the point, and it is asserted as an inequality
    ///
    /// The measured arm proves the sum is not merely always-`None`, and the legacy arm
    /// proves the sum is not the under-count. Together they rule out both degenerate
    /// implementations, which a single-arm test would not.
    #[tokio::test]
    async fn a_part_record_predating_compressed_size_reads_as_unreadable_not_zero() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path();
        let upload_dir = cache_dir.join("mpus_in_progress").join("upload-mixed");
        std::fs::create_dir_all(&upload_dir).unwrap();

        // Part 1: written by THIS build, with a real measurement.
        const MEASURED: u64 = 8_388_608;
        let info = crate::cache_types::CachedPartInfo::new(
            1,
            MEASURED * 2,
            "\"etag-1\"".to_string(),
            crate::compression::CompressionAlgorithm::Lz4,
        )
        .with_compressed_size(MEASURED);
        SignedPutHandler::record_part_blocking(&upload_dir, 1, &info).unwrap();

        // Precondition: with only the measured record present, the sum is readable and is
        // that record's figure. Without this the assertion below cannot distinguish "the
        // legacy record was detected" from "this function always returns None".
        assert_eq!(
            in_progress_part_bytes(cache_dir),
            Some(MEASURED),
            "fixture: a record carrying a real compressed_size must be summed, or the \
             UNREADABLE assertion below proves nothing"
        );

        // Part 2: the shape a PRE-UPGRADE binary wrote — every other field present,
        // `compressed_size` simply absent. Written as literal JSON rather than through
        // `CachedPartInfo`, because that type cannot express the absence this test is about.
        std::fs::write(
            upload_dir.join("part2.json"),
            r#"{"part_number":2,"size":16777216,"etag":"\"etag-2\"","compression_algorithm":"Lz4"}"#,
        )
        .unwrap();

        // The record parses cleanly — that is exactly why it was dangerous — so this is not
        // the unparseable-record arm.
        assert!(
            serde_json::from_str::<crate::cache_types::CachedPartInfo>(
                &std::fs::read_to_string(upload_dir.join("part2.json")).unwrap()
            )
            .is_ok(),
            "fixture: the legacy record must PARSE, or this test is exercising the \
             malformed-record arm instead of the missing-field one"
        );

        assert_eq!(
            in_progress_part_bytes(cache_dir),
            None,
            "a part record with no compressed_size field must make P UNREADABLE. Reading it \
             as 0 under-counts P, and an under-counted staged figure makes the eviction \
             trigger fire LATE — the direction that fills the volume."
        );

        // And the under-count is named explicitly, so a regression that reinstates it fails
        // with a message about the number rather than about an Option.
        assert_ne!(
            in_progress_part_bytes(cache_dir),
            Some(MEASURED),
            "P must not silently equal the measured part alone: that is the under-count, \
             and it is a plausible-looking figure rather than an obvious fault"
        );
    }

    /// Every `part{N}.json` for the upload is gone BEFORE `upload.lock` is released.
    ///
    /// This asserts the ORDERING, which is the whole content of the change. An
    /// end-state assertion ("no part records after finalize returns") passes whether
    /// the records were unlinked inside the lock or swept afterwards by
    /// `remove_dir_all`, so it cannot fail against the defect. The probe therefore runs
    /// on the far side of the deletion and the near side of the `drop(lock_file)`.
    ///
    /// A failure implicates the position of the `delete_part_records_blocking` call in
    /// `finalize_multipart_upload` (`src/signed_put_handler.rs`): if that call moved
    /// back below `drop(lock_file)`, or was removed in favour of the trailing
    /// `remove_dir_all`, the probe observes the records still present and this test
    /// fails deterministically rather than racily.
    #[tokio::test]
    async fn part_records_are_deleted_before_upload_lock_is_released() {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();

        let cache_key = "test-bucket/ordering-boundary";
        let upload_id = "upload-ordering-boundary";
        let response_headers = std::collections::HashMap::new();

        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            1,
            &vec![1u8; 4096],
            "etag1",
        )
        .await;
        stage_streamed_part(
            cache_dir,
            cache_key,
            upload_id,
            2,
            &vec![2u8; 8192],
            "etag2",
        )
        .await;

        let multipart_dir = mpu_upload_dir(cache_dir, upload_id);
        let mut before: Vec<u32> = SignedPutHandler::recorded_part_numbers(&multipart_dir)
            .into_iter()
            .collect();
        before.sort_unstable();
        assert_eq!(
            before,
            vec![1, 2],
            "fixture: both part records must exist before finalize, or the probe below \
             observes an empty set for the wrong reason"
        );

        let observed: std::sync::Arc<std::sync::Mutex<Option<Vec<u32>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let sink = observed.clone();
        // The directory is named by the uploadId's digest, not the uploadId.
        let wanted = mpu_upload_dir(std::path::Path::new("/"), upload_id)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .to_string();
        *PRE_UNLOCK_OBSERVER
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(Box::new(move |dir: &std::path::Path| {
            // Filter by upload id: this static is process-wide and other tests in
            // this module also call `finalize_multipart_upload`.
            if dir.file_name().and_then(|n| n.to_str()) == Some(wanted.as_str()) {
                let mut present: Vec<u32> = SignedPutHandler::recorded_part_numbers(dir)
                    .into_iter()
                    .collect();
                present.sort_unstable();
                *sink.lock().unwrap_or_else(|p| p.into_inner()) = Some(present);
            }
        }));

        let requested_parts = vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
        ];
        let result = handler
            .finalize_multipart_upload(
                cache_key,
                upload_id,
                "final-etag",
                &response_headers,
                &requested_parts,
            )
            .await;

        // Clear the hook before asserting, so a failing assertion cannot leave it set
        // for a concurrently running test.
        *PRE_UNLOCK_OBSERVER
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
        result.expect("finalize must succeed");

        let snapshot = observed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .expect(
                "the pre-unlock probe never fired: finalize did not reach the observation \
                 point, so this test proved nothing about ordering",
            );
        assert!(
            snapshot.is_empty(),
            "part records {:?} still existed while upload.lock was held — the deletion \
             is happening outside the lock, which is the window § 16.6.2 closes",
            snapshot
        );
    }

    // ========================================================================
    // Response-path invalidation of the prior cached version
    //
    // The client's 200 for a write-through PUT must not leave the key's previous
    // cached version servable. Before the fix, invalidation ran only inside the
    // detached cache task, so a PUT that S3 accepted but the proxy did not cache (a
    // capacity bypass, a declined or abandoned setup, a full backlog) or whose task
    // was lost to a restart left the old bytes served until their TTL.
    // ========================================================================

    /// A 200 for a single PUT, with `etag`.
    fn put_ok_reply(etag: &str) -> Vec<u8> {
        format!("HTTP/1.1 200 OK\r\nETag: {etag}\r\nContent-Length: 0\r\n\r\n").into_bytes()
    }

    /// The bytes a GET of `cache_key` would be served from the disk cache, or `None`
    /// when the GET would miss (no `.meta`, or a range that does not load).
    async fn served_from_cache(cache_dir: &std::path::Path, cache_key: &str) -> Option<Vec<u8>> {
        let meta_path =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
                .unwrap();
        let text = std::fs::read_to_string(&meta_path).ok()?;
        let metadata: crate::cache_types::NewCacheMetadata = serde_json::from_str(&text).ok()?;
        let disk = crate::disk_cache::DiskCacheManager::new(
            cache_dir.to_path_buf(),
            true,
            1024,
            false,
            1_048_576,
        );
        let mut served = Vec::new();
        for range in &metadata.ranges {
            served.extend(disk.load_range_data(range).await.ok()?);
        }
        Some(served)
    }

    /// Drive one single-part PUT through `handler` against a mock S3 that accepts it.
    async fn put_through_handler(
        handler: &mut SignedPutHandler,
        cache_key: &str,
        path: &str,
        body: Vec<u8>,
        etag: &str,
    ) -> StatusCode {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_drain_and_reply(
            listener,
            body.len(),
            put_ok_reply(etag),
        ));
        let (req, _keepalive) = incoming_request_with_method("PUT", path, body).await;
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handler.handle_signed_put(
                req,
                cache_key.to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport_for(addr),
            ),
        )
        .await
        .expect("PUT did not finish within 60s")
        .expect("PUT must reach the mock upstream");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;
        resp.status()
    }

    /// A cache manager over `cache_dir`, initialized the way proxy startup does, with
    /// compression off so `served_from_cache` reads the range files directly.
    async fn initialized_put_cache_manager(
        cache_dir: &std::path::Path,
    ) -> Arc<crate::cache::CacheManager> {
        let cache_manager = Arc::new(crate::cache::CacheManager::new(
            cache_dir.to_path_buf(),
            false, // ram_cache_enabled: the `.meta` on disk is what a GET reads
            0,
            100,
            false, // compression_enabled
        ));
        let _ = cache_manager.create_configured_disk_cache_manager();
        cache_manager.initialize().await.unwrap();
        cache_manager
    }

    fn write_through_handler(
        cache_dir: &std::path::Path,
        cache_manager: &Arc<crate::cache::CacheManager>,
        max_cache_capacity: u64,
    ) -> SignedPutHandler {
        let mut handler = SignedPutHandler::new(
            cache_dir.to_path_buf(),
            CompressionHandler::new(1024, false),
            0,
            max_cache_capacity,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager.clone());
        handler
    }

    /// PUT v1 through the write-through path and wait until a GET is served v1's bytes
    /// from the cache (the cache task is detached, so the `.meta` lands after the 200).
    async fn cache_v1(
        cache_dir: &std::path::Path,
        cache_manager: &Arc<crate::cache::CacheManager>,
        cache_key: &str,
        path: &str,
        v1: &[u8],
    ) {
        let mut caching = write_through_handler(cache_dir, cache_manager, u64::MAX);
        let status =
            put_through_handler(&mut caching, cache_key, path, v1.to_vec(), "\"v1\"").await;
        assert_eq!(status, StatusCode::OK);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if served_from_cache(cache_dir, cache_key).await.as_deref() == Some(v1) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "fixture precondition: v1 was never served from the cache within 30s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// The predicate shared by the overwrite tests: after S3 acknowledged v2, a GET must
    /// miss the cache or be served v2. Being served v1 is the defect.
    async fn assert_not_serving_prior_version(
        cache_dir: &std::path::Path,
        cache_key: &str,
        v1: &[u8],
        v2: &[u8],
        how: &str,
    ) {
        match served_from_cache(cache_dir, cache_key).await {
            None => {}
            Some(bytes) if bytes == v2 => {}
            Some(bytes) => panic!(
                "after S3 acknowledged v2 ({}), a GET is served {} bytes from the cache \
                 (v1 is {} bytes, v2 is {} bytes, served == v1: {}): the prior version was \
                 not invalidated before the PUT response returned",
                how,
                bytes.len(),
                v1.len(),
                v2.len(),
                bytes == v1
            ),
        }
    }

    /// Red side: v2 is a capacity bypass (`CacheDecision::Bypass`), which forwards the
    /// PUT verbatim and spawns no cache task, so nothing ever invalidated v1.
    #[tokio::test]
    async fn an_acknowledged_overwrite_on_the_capacity_bypass_stops_serving_the_prior_version() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let cache_manager = initialized_put_cache_manager(&cache_dir).await;
        let cache_key = "test-bucket/case72-overwrite-bypass";
        let path = "/case72-overwrite-bypass";
        let v1: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let v2: Vec<u8> = (0..7000u32).map(|i| (i % 13) as u8 + 100).collect();

        cache_v1(&cache_dir, &cache_manager, cache_key, path, &v1).await;

        let mut bypassing = write_through_handler(&cache_dir, &cache_manager, 1);
        assert!(
            matches!(
                bypassing.should_cache(Some(v2.len() as u64)),
                CacheDecision::Bypass(_)
            ),
            "fixture precondition: v2 must take the capacity bypass"
        );
        let status =
            put_through_handler(&mut bypassing, cache_key, path, v2.clone(), "\"v2\"").await;
        assert_eq!(status, StatusCode::OK, "the client's PUT succeeded on S3");

        assert_not_serving_prior_version(&cache_dir, cache_key, &v1, &v2, "capacity bypass").await;
    }

    /// Red side: S3 completes a multipart upload over a cached key, but this proxy
    /// staged none of its parts, so `finalize_multipart_upload` skips before its own
    /// invalidation. Before the fix v1 stayed served.
    #[tokio::test]
    async fn an_acknowledged_multipart_overwrite_that_skips_finalization_stops_serving_the_prior_version(
    ) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let cache_manager = initialized_put_cache_manager(&cache_dir).await;
        let cache_key = "test-bucket/case72-overwrite-mpu";
        let path = "/case72-overwrite-mpu";
        let v1: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();

        cache_v1(&cache_dir, &cache_manager, cache_key, path, &v1).await;

        let upload_id = "case72-unstaged-upload";
        assert!(
            !mpu_upload_dir(&cache_dir, upload_id).exists(),
            "fixture precondition: no staged parts for this upload"
        );
        let complete_body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
             <ETag>\"p1\"</ETag></Part></CompleteMultipartUpload>"
            .to_vec();
        let result_xml = "<CompleteMultipartUploadResult><ETag>\"v2-1\"</ETag>\
                          </CompleteMultipartUploadResult>";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_drain_and_reply(
            listener,
            complete_body.len(),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                result_xml.len(),
                result_xml
            )
            .into_bytes(),
        ));
        let (req, _keepalive) = incoming_request_with_method(
            "POST",
            &format!("{path}?uploadId={upload_id}"),
            complete_body,
        )
        .await;
        let mut handler = write_through_handler(&cache_dir, &cache_manager, u64::MAX);
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handler.handle_complete_multipart_upload(
                req,
                cache_key.to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport_for(addr),
                upload_id.to_string(),
            ),
        )
        .await
        .expect("CompleteMultipartUpload did not finish within 60s")
        .expect("CompleteMultipartUpload must reach the mock upstream");
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        assert_not_serving_prior_version(
            &cache_dir,
            cache_key,
            &v1,
            &[],
            "CompleteMultipartUpload, finalization skipped",
        )
        .await;
    }

    /// The overwrite predicate again, with v2 skipped because the pending-store bound is
    /// full: the skip the bound adds must not widen the stale-read exposure. This is the
    /// test that reaches `stream_put_to_upstream`'s response-path invalidation (the site
    /// every cached-path skip relies on): without that call it is served v1.
    #[tokio::test]
    async fn an_acknowledged_overwrite_skipped_for_a_full_backlog_stops_serving_the_prior_version()
    {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let cache_manager = initialized_put_cache_manager(&cache_dir).await;
        let cache_key = "test-bucket/case72-overwrite-backlog";
        let path = "/case72-overwrite-backlog";
        let v1: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let v2: Vec<u8> = (0..7000u32).map(|i| (i % 13) as u8 + 100).collect();

        cache_v1(&cache_dir, &cache_manager, cache_key, path, &v1).await;

        let mut handler = write_through_handler(&cache_dir, &cache_manager, u64::MAX);
        let metrics = Arc::new(RwLock::new(MetricsManager::new()));
        handler.set_metrics_manager(metrics.clone());
        handler.set_pending_cache_writes(Arc::new(tokio::sync::Semaphore::new(0)));
        let status = put_through_handler(&mut handler, cache_key, path, v2.clone(), "\"v2\"").await;
        assert_eq!(status, StatusCode::OK, "the client's PUT succeeded on S3");
        assert_eq!(
            skipped_put_count(&metrics, BACKLOG_FULL_SKIP_REASON).await,
            1,
            "fixture precondition: v2 must be skipped for a full backlog"
        );

        assert_not_serving_prior_version(&cache_dir, cache_key, &v1, &v2, "backlog full").await;
    }

    /// The pending-store bound: with `bound` stores held inside their publish, further PUTs
    /// spawn no store, are counted `backlog_full`, and still return S3's 200. Releasing the
    /// held stores commits them and returns every permit.
    #[tokio::test]
    async fn pending_write_cache_stores_are_bounded_and_the_excess_forwards_uncached() {
        const BOUND: usize = 4;
        const EXCESS: usize = 4;
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let cache_manager = initialized_put_cache_manager(&cache_dir).await;
        let metrics = Arc::new(RwLock::new(MetricsManager::new()));
        let permits = Arc::new(tokio::sync::Semaphore::new(BOUND));
        let body: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();

        let new_handler = || {
            let mut h = write_through_handler(&cache_dir, &cache_manager, u64::MAX);
            h.set_metrics_manager(metrics.clone());
            h.set_pending_cache_writes(permits.clone());
            h
        };

        // BOUND stores, each held at the gate right after its `.bin` rename, permit in hand.
        // A parked store occupies a blocking thread the test runtime's drop waits on, so no
        // assertion may panic while a gate is held: every check below is recorded first and
        // asserted only after the gates are released.
        let mut gates = Vec::new();
        let mut gate_reached = true;
        for i in 0..BOUND {
            let key = format!("test-bucket/case72-bound-held-{i}");
            let gate = crate::disk_cache::range_rename_pause::arm(&key);
            let mut h = new_handler();
            let status = put_through_handler(
                &mut h,
                &key,
                &format!("/case72-bound-held-{i}"),
                body.clone(),
                "\"h\"",
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            gate_reached &=
                tokio::time::timeout(std::time::Duration::from_secs(30), gate.reached.notified())
                    .await
                    .is_ok();
            gates.push((key, gate));
        }
        let held_permits = BOUND - permits.available_permits();

        // The excess: forwarded, acknowledged, not cached.
        let mut excess_ok = 0usize;
        for i in 0..EXCESS {
            let key = format!("test-bucket/case72-bound-excess-{i}");
            let mut h = new_handler();
            let status = put_through_handler(
                &mut h,
                &key,
                &format!("/case72-bound-excess-{i}"),
                body.clone(),
                "\"x\"",
            )
            .await;
            excess_ok += usize::from(status == StatusCode::OK);
        }
        // Give any store an excess PUT wrongly spawned time to publish, so its absence below
        // is not merely "not yet".
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let excess_cached = (0..EXCESS)
            .filter(|i| {
                crate::disk_cache::get_sharded_path(
                    &cache_dir.join("metadata"),
                    &format!("test-bucket/case72-bound-excess-{i}"),
                    ".meta",
                )
                .unwrap()
                .exists()
            })
            .count();
        let backlog_full = skipped_put_count(&metrics, BACKLOG_FULL_SKIP_REASON).await;

        // Release the held stores before asserting, so a failure cannot leave them parked.
        for (_, gate) in &gates {
            gate.resume.notify_one();
        }
        assert!(gate_reached, "fixture: a held store never reached its gate");
        assert_eq!(
            (held_permits, excess_ok, excess_cached, backlog_full),
            (BOUND, EXCESS, 0, EXCESS as u64),
            "with {BOUND} stores pending, each of {EXCESS} further PUTs must return S3's 200, \
             be forwarded uncached and be counted backlog_full; (permits held by the pending \
             stores, excess PUTs answered 200, excess PUTs cached, backlog_full count)"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let held_served = futures::future::join_all(
                gates
                    .iter()
                    .map(|(key, _)| served_from_cache(&cache_dir, key)),
            )
            .await;
            if held_served.iter().all(|s| s.as_deref() == Some(&body[..]))
                && permits.available_permits() == BOUND
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "released stores did not all commit and return their permits within 30s: \
                 available={}",
                permits.available_permits()
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    // ========================================================================
    // an older acknowledged write's detached store never publishes over a newer one
    // ========================================================================

    async fn cached_put_count(metrics: &Arc<RwLock<MetricsManager>>) -> u64 {
        metrics
            .read()
            .await
            .collect_metrics()
            .await
            .signed_put
            .expect("signed PUT metrics must be present")
            .cached_puts_total
    }

    /// PUT v2 and PUT v3 of one key are both acknowledged; v2's store is held until v3's
    /// store has published, then released. A GET must then miss or be served v3. With
    /// `two_proxies`, v3 goes through a second `CacheManager` over the same cache
    /// directory, as a second proxy sharing the volume would.
    async fn older_store_after_newer_publish(two_proxies: bool) {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let proxy_a = initialized_put_cache_manager(&cache_dir).await;
        let proxy_b = if two_proxies {
            initialized_put_cache_manager(&cache_dir).await
        } else {
            proxy_a.clone()
        };
        let tag = if two_proxies { "two" } else { "one" };
        let cache_key = format!("test-bucket/case19-reorder-{tag}");
        let path = format!("/case19-reorder-{tag}");
        let v1: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let v2: Vec<u8> = (0..7000u32).map(|i| (i % 13) as u8 + 100).collect();
        let v3: Vec<u8> = (0..9000u32).map(|i| (i % 7) as u8 + 200).collect();

        cache_v1(&cache_dir, &proxy_a, &cache_key, &path, &v1).await;

        // v2: acknowledged, its store held after S3 answered and before it touches the cache.
        let gate = pre_store_pause::arm(&cache_key);
        let metrics_v2 = Arc::new(RwLock::new(MetricsManager::new()));
        let mut h2 = write_through_handler(&cache_dir, &proxy_a, u64::MAX);
        h2.set_metrics_manager(metrics_v2.clone());
        let status_v2 = put_through_handler(&mut h2, &cache_key, &path, v2.clone(), "\"v2\"").await;
        let reached =
            tokio::time::timeout(std::time::Duration::from_secs(30), gate.reached.notified())
                .await
                .is_ok();

        // v3: acknowledged after v2's 200, and its store runs to completion.
        let mut h3 = write_through_handler(&cache_dir, &proxy_b, u64::MAX);
        let status_v3 = put_through_handler(&mut h3, &cache_key, &path, v3.clone(), "\"v3\"").await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut v3_published = false;
        while std::time::Instant::now() < deadline {
            if served_from_cache(&cache_dir, &cache_key).await.as_deref() == Some(&v3[..]) {
                v3_published = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Release v2's store and wait for it to end, published or declined.
        gate.resume.notify_one();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut v2_store_ended = false;
        while std::time::Instant::now() < deadline {
            if cached_put_count(&metrics_v2).await
                + skipped_put_count(&metrics_v2, SUPERSEDED_SKIP_REASON).await
                >= 1
            {
                v2_store_ended = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert!(reached, "fixture: v2's store never reached its pause");
        assert_eq!(
            (status_v2, status_v3),
            (StatusCode::OK, StatusCode::OK),
            "both PUTs were acknowledged"
        );
        assert!(v3_published, "fixture: v3 was never served from the cache");
        assert!(v2_store_ended, "fixture: v2's store never ended");
        match served_from_cache(&cache_dir, &cache_key).await {
            None => {}
            Some(bytes) if bytes == v3 => {}
            Some(bytes) => panic!(
                "after PUT v3 was acknowledged and published ({}), v2's older store ran and a \
                 GET is served {} bytes from the cache (v2 is {} bytes, v3 is {} bytes, served \
                 == v2: {}): an older write replaced a newer one",
                if two_proxies {
                    "on a second proxy"
                } else {
                    "on the same proxy"
                },
                bytes.len(),
                v2.len(),
                v3.len(),
                bytes == v2
            ),
        }
    }

    /// red side 1: both stores on one proxy.
    #[tokio::test]
    async fn an_older_acknowledged_puts_store_does_not_replace_a_newer_published_put() {
        older_store_after_newer_publish(false).await;
    }

    /// red side 2: the newer PUT and its store on a second proxy sharing the volume.
    #[tokio::test]
    async fn an_older_acknowledged_puts_store_does_not_replace_a_newer_put_from_another_proxy() {
        older_store_after_newer_publish(true).await;
    }

    // ========================================================================
    // a newer write whose response-path stamp failed still orders older stores
    // ========================================================================

    /// PUT v2 is acknowledged and its store held. PUT v3 is acknowledged, but its
    /// response-path invalidation exhausts its slot-lock retries, so the marker still holds
    /// v2's token. v2's store is released and publishes; then the background stamp retry is
    /// released. A GET must then miss or be served v3, never v2.
    #[tokio::test]
    async fn an_older_store_does_not_outlive_a_newer_put_whose_stamp_failed() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let proxy = initialized_put_cache_manager(&cache_dir).await;
        let cache_key = "test-bucket/case73-stamp-failed".to_string();
        let path = "/case73-stamp-failed";
        let v1: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let v2: Vec<u8> = (0..7000u32).map(|i| (i % 13) as u8 + 100).collect();
        let v3: Vec<u8> = (0..9000u32).map(|i| (i % 7) as u8 + 200).collect();

        cache_v1(&cache_dir, &proxy, &cache_key, path, &v1).await;

        // v2: acknowledged and stamped, its store held before it touches the cache.
        let gate = pre_store_pause::arm(&cache_key);
        let metrics_v2 = Arc::new(RwLock::new(MetricsManager::new()));
        let mut h2 = write_through_handler(&cache_dir, &proxy, u64::MAX);
        h2.set_metrics_manager(metrics_v2.clone());
        let status_v2 = put_through_handler(&mut h2, &cache_key, path, v2.clone(), "\"v2\"").await;
        let reached =
            tokio::time::timeout(std::time::Duration::from_secs(30), gate.reached.notified())
                .await
                .is_ok();

        // v3: S3 accepts it, but its response-path stamp gives up on the slot lock.
        crate::metadata_lock_manager::forced_retry_exhaustion::arm(&cache_key, 1);
        let retry = crate::cache::write_stamp_retry_gate::arm(&cache_key);
        let mut h3 = write_through_handler(&cache_dir, &proxy, u64::MAX);
        let status_v3 = put_through_handler(&mut h3, &cache_key, path, v3.clone(), "\"v3\"").await;
        let retry_reached =
            tokio::time::timeout(std::time::Duration::from_secs(5), retry.reached.notified())
                .await
                .is_ok();

        // Release v2's store and wait for it to end, published or declined.
        gate.resume.notify_one();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut v2_store_ended = false;
        while std::time::Instant::now() < deadline {
            if cached_put_count(&metrics_v2).await
                + skipped_put_count(&metrics_v2, SUPERSEDED_SKIP_REASON).await
                >= 1
            {
                v2_store_ended = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Release the stamp retry, if there is one, and wait for its stamp to land.
        retry.resume.notify_one();
        let retry_stamped = retry_reached
            && tokio::time::timeout(std::time::Duration::from_secs(30), retry.stamped.notified())
                .await
                .is_ok();

        assert!(reached, "fixture: v2's store never reached its pause");
        assert_eq!(
            (status_v2, status_v3),
            (StatusCode::OK, StatusCode::OK),
            "both PUTs were acknowledged"
        );
        assert!(v2_store_ended, "fixture: v2's store never ended");
        match served_from_cache(&cache_dir, &cache_key).await {
            None => {}
            Some(bytes) if bytes == v3 => {}
            Some(bytes) => panic!(
                "after PUT v3 was acknowledged with its stamp failed, v2's older store ran and a \
                 GET is served {} bytes from the cache (v2 is {} bytes, v3 is {} bytes, served \
                 == v2: {}; stamp retry reached: {}, landed: {}): an older write outlived a \
                 newer one",
                bytes.len(),
                v2.len(),
                v3.len(),
                bytes == v2,
                retry_reached,
                retry_stamped
            ),
        }
        assert!(
            retry_stamped,
            "the background stamp retry must land (reached: {retry_reached})"
        );
    }

    /// the buffered empty-object store declines once a newer PUT is acknowledged.
    /// PUT v2 (zero bytes) is acknowledged and its store held; PUT v3 is acknowledged and
    /// published; v2's store is released. A GET must miss or be served v3.
    #[tokio::test]
    async fn an_older_empty_puts_store_does_not_replace_a_newer_published_put() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let proxy = initialized_put_cache_manager(&cache_dir).await;
        let cache_key = "test-bucket/case73-empty-reorder".to_string();
        let path = "/case73-empty-reorder";
        let v1: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let v3: Vec<u8> = (0..9000u32).map(|i| (i % 7) as u8 + 200).collect();

        cache_v1(&cache_dir, &proxy, &cache_key, path, &v1).await;

        let gate = pre_store_pause::arm(&cache_key);
        let metrics_v2 = Arc::new(RwLock::new(MetricsManager::new()));
        let mut h2 = write_through_handler(&cache_dir, &proxy, u64::MAX);
        h2.set_metrics_manager(metrics_v2.clone());
        let status_v2 = put_through_handler(&mut h2, &cache_key, path, Vec::new(), "\"v2\"").await;
        let reached =
            tokio::time::timeout(std::time::Duration::from_secs(30), gate.reached.notified())
                .await
                .is_ok();

        let mut h3 = write_through_handler(&cache_dir, &proxy, u64::MAX);
        let status_v3 = put_through_handler(&mut h3, &cache_key, path, v3.clone(), "\"v3\"").await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut v3_published = false;
        while std::time::Instant::now() < deadline {
            if served_from_cache(&cache_dir, &cache_key).await.as_deref() == Some(&v3[..]) {
                v3_published = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        gate.resume.notify_one();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut v2_store_ended = false;
        while std::time::Instant::now() < deadline {
            if cached_put_count(&metrics_v2).await
                + skipped_put_count(&metrics_v2, SUPERSEDED_SKIP_REASON).await
                >= 1
            {
                v2_store_ended = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert!(
            reached,
            "fixture: the empty v2's store never reached its pause"
        );
        assert_eq!(
            (status_v2, status_v3),
            (StatusCode::OK, StatusCode::OK),
            "both PUTs were acknowledged"
        );
        assert!(v3_published, "fixture: v3 was never served from the cache");
        assert!(v2_store_ended, "fixture: the empty v2's store never ended");
        match served_from_cache(&cache_dir, &cache_key).await {
            None => {}
            Some(bytes) if bytes == v3 => {}
            Some(bytes) => panic!(
                "after PUT v3 was acknowledged and published, the empty v2's older store ran and \
                 a GET is served {} bytes from the cache (v3 is {} bytes): an older empty write \
                 replaced a newer one",
                bytes.len(),
                v3.len()
            ),
        }
    }

    /// Stage two streamed parts of `upload_id` and return the Complete's part list.
    async fn staged_two_part_upload(
        cache_dir: &std::path::Path,
        cache_key: &str,
        upload_id: &str,
    ) -> Vec<RequestedPart> {
        stage_streamed_part(cache_dir, cache_key, upload_id, 1, &[1u8; 1024], "etag1").await;
        stage_streamed_part(cache_dir, cache_key, upload_id, 2, &[2u8; 512], "etag2").await;
        vec![
            RequestedPart {
                part_number: 1,
                etag: "etag1".to_string(),
            },
            RequestedPart {
                part_number: 2,
                etag: "etag2".to_string(),
            },
        ]
    }

    /// a CompleteMultipartUpload whose guard no longer holds (`superseded`), or
    /// whose response-path stamp failed (`Unavailable`), publishes no `.meta`.
    async fn guarded_multipart_completion_declines(guard: WriteGuard, tag: &str) {
        let temp_dir = TempDir::new().unwrap();
        let mut handler = create_test_handler(&temp_dir);
        let cache_dir = temp_dir.path();
        let cache_key = format!("test-bucket/case73-mpu-{tag}");
        let upload_id = format!("case73-upload-{tag}");
        let parts = staged_two_part_upload(cache_dir, &cache_key, &upload_id).await;

        handler
            .finalize_multipart_upload_guarded(
                &cache_key,
                &upload_id,
                "final-etag",
                &std::collections::HashMap::new(),
                &parts,
                guard,
                None,
            )
            .await
            .unwrap();

        let meta =
            crate::disk_cache::get_sharded_path(&cache_dir.join("metadata"), &cache_key, ".meta")
                .unwrap();
        assert!(
            !meta.exists(),
            "a {tag} CompleteMultipartUpload published a .meta: an unordered completion was cached"
        );
    }

    #[tokio::test]
    async fn a_superseded_multipart_completion_publishes_nothing() {
        // A token the key's marker does not hold: a newer write of the key stamped after it.
        guarded_multipart_completion_declines(
            WriteGuard::Token(crate::cache::WriteToken::fresh()),
            "superseded",
        )
        .await;
    }

    #[tokio::test]
    async fn a_multipart_completion_whose_stamp_failed_publishes_nothing() {
        guarded_multipart_completion_declines(WriteGuard::Unavailable, "unavailable").await;
    }

    /// CompleteMultipartUpload C1 was acknowledged and stamped; PUT W2 of the same
    /// key was acknowledged after it on this proxy, but W2's stamp failed and is pending. C1's
    /// finalize must not publish while that stamp is pending.
    #[tokio::test]
    async fn a_completion_acknowledged_before_a_pending_write_publishes_nothing() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let proxy = initialized_put_cache_manager(&cache_dir).await;
        let mut handler = write_through_handler(&cache_dir, &proxy, u64::MAX);
        let cache_key = "test-bucket/case29-mpu-pending".to_string();
        let upload_id = "case29-upload-pending";
        let parts = staged_two_part_upload(&cache_dir, &cache_key, upload_id).await;
        let c1 = proxy
            .invalidate_for_write(&cache_key)
            .await
            .expect("fixture: C1's stamp lands");
        crate::metadata_lock_manager::forced_retry_exhaustion::arm(&cache_key, 1);
        let gate = crate::cache::write_stamp_retry_gate::arm(&cache_key);
        let w2 = proxy.invalidate_for_write(&cache_key).await;
        let retry = proxy.spawn_write_stamp_retry(&cache_key);
        let finalized = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            handler.finalize_multipart_upload_guarded(
                &cache_key,
                upload_id,
                "c1-etag",
                &std::collections::HashMap::new(),
                &parts,
                WriteGuard::Token(c1),
                None,
            ),
        )
        .await;
        let served_c1 = served_from_cache(&cache_dir, &cache_key).await.is_some();
        gate.resume.notify_one();
        let stamped =
            tokio::time::timeout(std::time::Duration::from_secs(30), gate.stamped.notified())
                .await
                .is_ok();
        assert!(
            w2.is_err() && retry == crate::cache::WriteStampRetry::Started,
            "fixture: W2's stamp fails and goes to the retry ({retry:?})"
        );
        assert!(
            matches!(finalized, Ok(Ok(()))),
            "fixture: the finalize returns: {finalized:?}"
        );
        assert!(
            !served_c1,
            "a CompleteMultipartUpload acknowledged before a newer write of the key published \
             while that write's stamp was pending on this proxy: a GET is served C1's object"
        );
        assert!(stamped, "fixture: W2's held stamp lands once released");
    }
    /// fix 2. PUT v2 is acknowledged and its store held. A DELETE of the key is
    /// acknowledged through the response-path invalidation the http_proxy write arms use, but
    /// its stamp exhausts its slot-lock retries, so the marker still holds v2's token. v2's
    /// store is released and publishes; then the background stamp retry is released. A GET
    /// must then miss.
    #[tokio::test]
    async fn an_older_store_does_not_outlive_a_delete_whose_stamp_failed() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let proxy = initialized_put_cache_manager(&cache_dir).await;
        let cache_key = "test-bucket/case22-delete-stamp-failed".to_string();
        let path = "/case22-delete-stamp-failed";
        let v1: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let v2: Vec<u8> = (0..7000u32).map(|i| (i % 13) as u8 + 100).collect();
        cache_v1(&cache_dir, &proxy, &cache_key, path, &v1).await;
        // v2: acknowledged and stamped, its store held before it touches the cache.
        let gate = pre_store_pause::arm(&cache_key);
        let metrics_v2 = Arc::new(RwLock::new(MetricsManager::new()));
        let mut h2 = write_through_handler(&cache_dir, &proxy, u64::MAX);
        h2.set_metrics_manager(metrics_v2.clone());
        let status_v2 = put_through_handler(&mut h2, &cache_key, path, v2.clone(), "\"v2\"").await;
        let reached =
            tokio::time::timeout(std::time::Duration::from_secs(30), gate.reached.notified())
                .await
                .is_ok();
        // DELETE: S3 accepts it, but its response-path stamp gives up on the slot lock.
        crate::metadata_lock_manager::forced_retry_exhaustion::arm(&cache_key, 1);
        let retry = crate::cache::write_stamp_retry_gate::arm(&cache_key);
        let deleted = proxy
            .invalidate_acknowledged_write(&cache_key, "DELETE")
            .await;
        let retry_reached =
            tokio::time::timeout(std::time::Duration::from_secs(5), retry.reached.notified())
                .await
                .is_ok();
        // Release v2's store and wait for it to end, published or declined.
        gate.resume.notify_one();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut v2_store_ended = false;
        while std::time::Instant::now() < deadline {
            if cached_put_count(&metrics_v2).await
                + skipped_put_count(&metrics_v2, SUPERSEDED_SKIP_REASON).await
                >= 1
            {
                v2_store_ended = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // Release the stamp retry, if there is one, and wait for its stamp to land.
        retry.resume.notify_one();
        let retry_stamped = retry_reached
            && tokio::time::timeout(std::time::Duration::from_secs(30), retry.stamped.notified())
                .await
                .is_ok();
        assert!(reached, "fixture: v2's store never reached its pause");
        assert_eq!(
            status_v2,
            StatusCode::OK,
            "fixture: PUT v2 was acknowledged"
        );
        assert!(
            deleted.is_err(),
            "fixture: the DELETE's first stamp must fail"
        );
        assert!(v2_store_ended, "fixture: v2's store never ended");
        if let Some(bytes) = served_from_cache(&cache_dir, &cache_key).await {
            panic!(
                "after a DELETE was acknowledged with its stamp failed, v2's older store ran and \
                 a GET is served {} bytes from the cache (v2 is {} bytes, served == v2: {}; stamp \
                 retry reached: {}, landed: {}): an older write outlived a delete",
                bytes.len(),
                v2.len(),
                bytes == v2,
                retry_reached,
                retry_stamped
            );
        }
        assert!(
            retry_stamped,
            "the background stamp retry must land (reached: {retry_reached})"
        );
    }

    // =========================================================================
    // the CreateMultipartUpload tracker write and the detached Complete task are
    // bounded, and both entry points share one Create predicate.
    // =========================================================================

    /// Red side (fix 1): CreateMultipartUpload waited, unbounded, for its `upload.meta`
    /// write on the shared volume before answering. The stall is a FIFO at the tracker path:
    /// opening it for write blocks until a reader opens it, as a hung NFS or EFS write does.
    #[tokio::test]
    async fn create_is_answered_within_the_bound_when_the_tracker_write_stalls() {
        use std::os::unix::fs::OpenOptionsExt;
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let upload_id = "case74-stalled-tracker-upload";
        let upload_dir = mpu_upload_dir(&cache_dir, upload_id);
        std::fs::create_dir_all(&upload_dir).unwrap();
        let fifo = upload_dir.join("upload.meta");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(made.success(), "fixture: mkfifo {:?}", fifo);

        let mut handler = SignedPutHandler::new(
            cache_dir.clone(),
            CompressionHandler::new(1024, false),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        handler.set_pending_cache_writes(permits.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_and_reply_initiate_multipart(
            listener,
            upload_id.to_string(),
        ));
        let (req, _keepalive) =
            incoming_request_with_method("POST", "/case74-object?uploads", Vec::new()).await;
        let started = std::time::Instant::now();
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handler.handle_create_multipart_upload(
                req,
                "test-bucket/case74-object".to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport_for(addr),
            ),
        )
        .await;
        let elapsed = started.elapsed();
        let answer = match answered {
            Ok(Ok(resp)) => {
                let status = resp.status();
                let body = resp.into_body().collect().await.unwrap().to_bytes();
                Some((status, String::from_utf8_lossy(&body).contains(upload_id)))
            }
            Ok(Err(e)) => panic!("CreateMultipartUpload failed: {e}"),
            Err(_) => None,
        };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        // Release the parked write before asserting: a blocking open parked on the FIFO holds
        // a blocking thread the test runtime's drop waits on. A non-blocking read-write open
        // never parks, lets the writer's open complete, and the poll below gives up rather
        // than hang if no writer ever arrives.
        let drained = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&fifo)
                .unwrap();
            let mut got = Vec::new();
            let mut buf = [0u8; 8192];
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                match f.read(&mut buf) {
                    Ok(n) if n > 0 => {
                        got.extend_from_slice(&buf[..n]);
                        if got.ends_with(b"}") {
                            break;
                        }
                    }
                    _ => std::thread::sleep(std::time::Duration::from_millis(20)),
                }
            }
            got
        })
        .await
        .unwrap();
        let mut permit_returned = false;
        for _ in 0..100 {
            if permits.available_permits() == 1 {
                permit_returned = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let bound = WRITE_CACHE_SETUP_WAIT_BOUND + std::time::Duration::from_millis(500);
        assert!(
            answer.is_some() && elapsed < bound,
            "a CreateMultipartUpload must be answered within {:?} when its tracker write \
             stalls, not wait for the volume: answered: {:?}, after {:?}",
            bound,
            answer,
            elapsed
        );
        assert_eq!(
            answer,
            Some((StatusCode::OK, true)),
            "the client gets S3's answer, UploadId included"
        );
        assert!(
            String::from_utf8_lossy(&drained).contains(upload_id),
            "the tracker write completes in the background once the volume answers: {:?}",
            String::from_utf8_lossy(&drained)
        );
        assert!(
            permit_returned,
            "the tracker write's pending-store permit must be returned when it ends"
        );
        assert!(
            !upload_dir.exists(),
            "a tracker that lands after the Create was answered must be withdrawn, so the upload \
             is not cached without it and no upload directory is left behind: {:?}",
            std::fs::read_dir(&upload_dir)
                .map(|d| d.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
        );
    }

    /// Red side (fix 2): the detached Complete finalization took no pending-store permit,
    /// so with every permit held it still ran and published the object. With the fix it is
    /// declined (`backlog_full`) and the client's answer is S3's, unchanged; with a permit
    /// free the same upload finalizes and the permit comes back when the task ends.
    #[tokio::test]
    async fn a_detached_complete_finalization_is_bounded_by_the_pending_store_permits() {
        let metrics = Arc::new(RwLock::new(MetricsManager::new()));
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let mut outcomes = Vec::new();
        for (arm, hold_bound) in [("bound full", true), ("permit free", false)] {
            let temp_dir = TempDir::new().unwrap();
            let cache_dir = temp_dir.path().to_path_buf();
            let cache_manager = initialized_put_cache_manager(&cache_dir).await;
            let mut handler = write_through_handler(&cache_dir, &cache_manager, u64::MAX);
            handler.set_metrics_manager(metrics.clone());
            handler.set_pending_cache_writes(permits.clone());
            let cache_key = format!("test-bucket/case74-bounded-{hold_bound}");
            let path = format!("/case74-bounded-{hold_bound}");
            let upload_id = format!("case74-bounded-upload-{hold_bound}");
            let host = "test-bucket.s3.amazonaws.com".to_string();
            let part_body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
            let part_etag = "\"case74-part-etag-1\"";

            // Stage part 1. Its store takes the one permit and returns it when it ends.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_drain_and_reply(
                listener,
                part_body.len(),
                format!("HTTP/1.1 200 OK\r\nETag: {part_etag}\r\nContent-Length: 0\r\n\r\n")
                    .into_bytes(),
            ));
            let (req, _keepalive_part) = incoming_upload_part_request(
                &format!("{path}?uploadId={upload_id}&partNumber=1"),
                part_body.clone(),
            )
            .await;
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_upload_part(
                    req,
                    cache_key.clone(),
                    host.clone(),
                    transport_for(addr),
                    upload_id.clone(),
                    1,
                ),
            )
            .await
            .expect("UploadPart did not finish within 60s")
            .expect("UploadPart must reach the mock upstream");
            assert_eq!(resp.status(), StatusCode::OK);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;
            let mut part_store_ended = false;
            for _ in 0..200 {
                if permits.available_permits() == 1 {
                    part_store_ended = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(
                part_store_ended,
                "fixture: the part store never returned its permit"
            );

            // The bound: every pending-store permit held by other stores.
            let held = if hold_bound {
                Some(permits.clone().acquire_owned().await.unwrap())
            } else {
                None
            };
            let skipped_before = skipped_put_count(&metrics, BACKLOG_FULL_SKIP_REASON).await;

            // The CreateMultipartUpload record, without which Complete does not cache.
            handler
                .create_multipart_upload_tracker_with_content_type(
                    &cache_key,
                    &upload_id,
                    None,
                    &HashMap::from([(
                        "content-type".to_string(),
                        DEFAULT_CONTENT_TYPE.to_string(),
                    )]),
                )
                .await
                .unwrap();

            let complete_body = format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
                 <ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
            )
            .into_bytes();
            let result_xml = "<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket>\
                              <Key>case74-bounded</Key>\
                              <ETag>\"3858f62230ac3c915f300c664312c11f-1\"</ETag>\
                              </CompleteMultipartUploadResult>";
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_drain_and_reply(
                listener,
                complete_body.len(),
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                    result_xml.len(),
                    result_xml
                )
                .into_bytes(),
            ));
            let (req, _keepalive_complete) = incoming_request_with_method(
                "POST",
                &format!("{path}?uploadId={upload_id}"),
                complete_body,
            )
            .await;
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_complete_multipart_upload(
                    req,
                    cache_key.clone(),
                    host,
                    transport_for(addr),
                    upload_id.clone(),
                ),
            )
            .await
            .expect("CompleteMultipartUpload did not finish within 60s")
            .expect("CompleteMultipartUpload must reach the mock upstream");
            let status = resp.status();
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

            // A finalization that runs publishes within a few seconds; wait that long in both
            // arms so "not published" is not merely "not yet".
            let meta = crate::disk_cache::get_sharded_path(
                &cache_dir.join("metadata"),
                &cache_key,
                ".meta",
            )
            .unwrap();
            let mut published = false;
            for _ in 0..60 {
                if meta.exists() {
                    published = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let skipped =
                skipped_put_count(&metrics, BACKLOG_FULL_SKIP_REASON).await - skipped_before;
            drop(held);
            let mut returned = false;
            for _ in 0..200 {
                if permits.available_permits() == 1 {
                    returned = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            outcomes.push((
                arm,
                status,
                body == Bytes::from_static(result_xml.as_bytes()),
                published,
                skipped,
                returned,
            ));
        }
        assert_eq!(
            outcomes,
            vec![
                ("bound full", StatusCode::OK, true, false, 1, true),
                ("permit free", StatusCode::OK, true, true, 0, true),
            ],
            "(arm, status, client body is S3's, object published, backlog_full skips, every \
             permit returned): a detached Complete finalization must take a pending-store \
             permit, and be declined when none is free"
        );
    }

    /// the router (`HttpProxy::handle_other_request`) and the handler's dispatch
    /// share this one CreateMultipartUpload predicate. It reads parameter names, so `uploadId`
    /// as a value does not hide a Create and a name merely starting with `uploads` does not
    /// make one.
    #[test]
    fn the_create_multipart_upload_predicate_reads_parameter_names() {
        let cases = [
            ("uploads", true),
            ("uploads=", true),
            ("uploads&x-id=CreateMultipartUpload", true),
            ("x-id=CreateMultipartUpload&uploads", true),
            ("uploads&tag=uploadId", true),
            ("uploads&uploadId=u1", false),
            ("uploadId=u1&partNumber=1", false),
            ("prefix=uploads", false),
            ("a=1&uploadsx", false),
            ("", false),
        ];
        let wrong: Vec<(&str, bool)> = cases
            .iter()
            .filter(|(q, want)| SignedPutHandler::is_create_multipart_upload(q) != *want)
            .copied()
            .collect();
        assert!(
            wrong.is_empty(),
            "the Create predicate disagrees with S3's parameter names on (query, expected): \
             {wrong:?}"
        );
    }

    // =========================================================================
    // an upload whose CreateMultipartUpload recorded no tracker is not cached without
    // its representation headers, and the Create's disk-safety check is inside the bound.
    // =========================================================================

    /// Red side (fix 1): with the pending-store bound full at CreateMultipartUpload no
    /// tracker was written, yet the upload was still finalized, into a cache entry missing the
    /// Create's `Content-Type` and `x-amz-meta-*`, which a cache hit then served. The control
    /// arm is the same upload with a permit free at the Create, which publishes with them, so
    /// "not published" in the subject arm is not merely a fixture that cannot publish.
    #[tokio::test]
    async fn an_upload_whose_create_wrote_no_tracker_is_not_cached_without_its_headers() {
        let metrics = Arc::new(RwLock::new(MetricsManager::new()));
        let host = "test-bucket.s3.amazonaws.com".to_string();
        let mut outcomes = Vec::new();
        for (arm, full_at_create) in [
            ("bound full at Create", true),
            ("permit free at Create", false),
        ] {
            let temp_dir = TempDir::new().unwrap();
            let cache_dir = temp_dir.path().to_path_buf();
            let cache_manager = initialized_put_cache_manager(&cache_dir).await;
            let mut handler = write_through_handler(&cache_dir, &cache_manager, u64::MAX);
            handler.set_metrics_manager(metrics.clone());
            let permits = Arc::new(tokio::sync::Semaphore::new(1));
            handler.set_pending_cache_writes(permits.clone());
            let cache_key = format!("test-bucket/case75-create-{full_at_create}");
            let path = format!("/case75-create-{full_at_create}");
            let upload_id = format!("case75-create-upload-{full_at_create}");
            let await_permit = |permits: Arc<tokio::sync::Semaphore>| async move {
                for _ in 0..200 {
                    if permits.available_permits() == 1 {
                        return true;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                false
            };

            // ---- CreateMultipartUpload, with the bound full in the subject arm ----
            let held = if full_at_create {
                Some(permits.clone().acquire_owned().await.unwrap())
            } else {
                None
            };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_and_reply_initiate_multipart(
                listener,
                upload_id.clone(),
            ));
            let (req, _keepalive_create) = incoming_request_with_headers(
                "POST",
                &format!("{path}?uploads"),
                Vec::new(),
                &[("content-type", "text/csv"), ("x-amz-meta-a", "b")],
            )
            .await;
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_create_multipart_upload(
                    req,
                    cache_key.clone(),
                    host.clone(),
                    transport_for(addr),
                ),
            )
            .await
            .expect("CreateMultipartUpload did not finish within 60s")
            .expect("CreateMultipartUpload must reach the mock upstream");
            let create_status = resp.status();
            let create_body = resp.into_body().collect().await.unwrap().to_bytes();
            let create_ok = create_status == StatusCode::OK
                && String::from_utf8_lossy(&create_body).contains(&upload_id);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;
            drop(held);
            assert!(
                await_permit(permits.clone()).await,
                "fixture: the Create's tracker task never returned its permit"
            );

            // ---- UploadPart 1, with a permit free ----
            let part_body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
            let part_etag = "\"case75-part-etag-1\"";
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_drain_and_reply(
                listener,
                part_body.len(),
                format!("HTTP/1.1 200 OK\r\nETag: {part_etag}\r\nContent-Length: 0\r\n\r\n")
                    .into_bytes(),
            ));
            let (req, _keepalive_part) = incoming_upload_part_request(
                &format!("{path}?uploadId={upload_id}&partNumber=1"),
                part_body.clone(),
            )
            .await;
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_upload_part(
                    req,
                    cache_key.clone(),
                    host.clone(),
                    transport_for(addr),
                    upload_id.clone(),
                    1,
                ),
            )
            .await
            .expect("UploadPart did not finish within 60s")
            .expect("UploadPart must reach the mock upstream");
            assert_eq!(resp.status(), StatusCode::OK);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;
            assert!(
                await_permit(permits.clone()).await,
                "fixture: the part store never returned its permit"
            );
            assert!(
                mpu_upload_dir(&cache_dir, &upload_id).exists(),
                "fixture: the part staged (parts stage without a tracker)"
            );

            // ---- CompleteMultipartUpload ----
            let complete_body = format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
                 <ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
            )
            .into_bytes();
            let result_xml = "<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket>\
                              <Key>case75-create</Key>\
                              <ETag>\"3858f62230ac3c915f300c664312c11f-1\"</ETag>\
                              </CompleteMultipartUploadResult>";
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_drain_and_reply(
                listener,
                complete_body.len(),
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                    result_xml.len(),
                    result_xml
                )
                .into_bytes(),
            ));
            let (req, _keepalive_complete) = incoming_request_with_method(
                "POST",
                &format!("{path}?uploadId={upload_id}"),
                complete_body,
            )
            .await;
            let resp = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_complete_multipart_upload(
                    req,
                    cache_key.clone(),
                    host.clone(),
                    transport_for(addr),
                    upload_id.clone(),
                ),
            )
            .await
            .expect("CompleteMultipartUpload did not finish within 60s")
            .expect("CompleteMultipartUpload must reach the mock upstream");
            let complete_ok = resp.status() == StatusCode::OK
                && resp.into_body().collect().await.unwrap().to_bytes()
                    == Bytes::from_static(result_xml.as_bytes());
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

            // A finalization that runs publishes within a few seconds; wait that long in both
            // arms so "not published" is not merely "not yet".
            let meta = crate::disk_cache::get_sharded_path(
                &cache_dir.join("metadata"),
                &cache_key,
                ".meta",
            )
            .unwrap();
            for _ in 0..60 {
                if meta.exists() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(
                await_permit(permits.clone()).await,
                "fixture: the Complete task never returned its permit"
            );
            // What a cache hit serves: the published entry's response headers.
            let published = std::fs::read_to_string(&meta).ok().map(|text| {
                let stored: crate::cache_types::NewCacheMetadata =
                    serde_json::from_str(&text).unwrap();
                let headers = stored.object_metadata.response_headers;
                (
                    headers.get("content-type").cloned(),
                    headers.get("x-amz-meta-a").cloned(),
                )
            });
            let staged_left = mpu_upload_dir(&cache_dir, &upload_id).exists();
            outcomes.push((arm, create_ok, complete_ok, published, staged_left));
        }
        let some = |s: &str| Some(s.to_string());
        assert_eq!(
            outcomes,
            vec![
                ("bound full at Create", true, true, None, false),
                (
                    "permit free at Create",
                    true,
                    true,
                    Some((some("text/csv"), some("b"))),
                    false
                ),
            ],
            "(arm, Create answered with S3's UploadId, Complete answered with S3's body, the \
             published entry's (content-type, x-amz-meta-a), staged parts left): an upload whose \
             CreateMultipartUpload wrote no tracker must not be cached without the Create's \
             representation headers"
        );
    }

    /// Red side (fix 2): CreateMultipartUpload awaited `disk_safety_refusal(0)` in front
    /// of the forward, and that reads `size_state.json` from the shared volume. The stall is a
    /// FIFO at that path: a read of it blocks until a writer opens it, as a hung NFS or EFS
    /// read does. The answer must come within the bound, and a Create answered before its
    /// cache setup finished leaves no tracker, so the upload is not cached.
    #[tokio::test]
    async fn create_is_answered_within_the_bound_when_the_disk_safety_check_stalls() {
        use std::os::unix::fs::OpenOptionsExt;
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        // Generous, so component 1 runs (it needs `max_cache_size > 0`) and admits once it
        // reads; floor 0 disables component 2.
        let cache_manager = initialized_accounting_cache_manager_with_floor(
            cache_dir.clone(),
            10 * 1024 * 1024 * 1024,
            0,
        )
        .await;
        assert!(
            cache_manager.get_journal_consolidator().await.is_some(),
            "fixture: component 1 reads size_state.json through the journal consolidator"
        );
        let size_state = cache_dir.join("size_tracking").join("size_state.json");
        std::fs::create_dir_all(size_state.parent().unwrap()).unwrap();
        let original = std::fs::read(&size_state).ok();
        let _ = std::fs::remove_file(&size_state);
        let made = std::process::Command::new("mkfifo")
            .arg(&size_state)
            .status()
            .unwrap();
        assert!(made.success(), "fixture: mkfifo {:?}", size_state);

        let mut handler = SignedPutHandler::new(
            cache_dir.clone(),
            CompressionHandler::new(1024, false),
            0,
            u64::MAX,
            None,
            10 * 1024 * 1024,
            4,
        );
        handler.set_cache_manager(cache_manager.clone());
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        handler.set_pending_cache_writes(permits.clone());

        let upload_id = "case75-stalled-disk-safety-upload";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(accept_and_reply_initiate_multipart(
            listener,
            upload_id.to_string(),
        ));
        let (req, _keepalive) =
            incoming_request_with_method("POST", "/case75-object?uploads", Vec::new()).await;
        let started = std::time::Instant::now();
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handler.handle_create_multipart_upload(
                req,
                "test-bucket/case75-object".to_string(),
                "test-bucket.s3.amazonaws.com".to_string(),
                transport_for(addr),
            ),
        )
        .await;
        let elapsed = started.elapsed();
        let answer = match answered {
            Ok(Ok(resp)) => {
                let status = resp.status();
                let body = resp.into_body().collect().await.unwrap().to_bytes();
                Some((status, String::from_utf8_lossy(&body).contains(upload_id)))
            }
            Ok(Err(e)) => panic!("CreateMultipartUpload failed: {e}"),
            Err(_) => None,
        };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;

        // Release every parked read before asserting: a blocking read parked on the FIFO holds
        // a blocking thread the test runtime's drop waits on. A non-blocking read-write open
        // never parks and wakes any reader parked in `open`; the original document is renamed
        // over the FIFO while that writer is held, so a later reader opens a regular file.
        let fifo = size_state.clone();
        tokio::task::spawn_blocking(move || {
            let held = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&fifo)
                .unwrap();
            let restore = fifo.with_extension("case75-restore");
            std::fs::write(&restore, original.unwrap_or_default()).unwrap();
            std::fs::rename(&restore, &fifo).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(200));
            drop(held);
        })
        .await
        .unwrap();
        let mut permit_returned = false;
        for _ in 0..100 {
            if permits.available_permits() == 1 {
                permit_returned = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let upload_dir = mpu_upload_dir(&cache_dir, upload_id);

        let bound = WRITE_CACHE_SETUP_WAIT_BOUND + std::time::Duration::from_millis(500);
        assert!(
            answer.is_some() && elapsed < bound,
            "a CreateMultipartUpload must be answered within {:?} when its disk-safety check \
             stalls on the volume: answered: {:?}, after {:?}",
            bound,
            answer,
            elapsed
        );
        assert_eq!(
            answer,
            Some((StatusCode::OK, true)),
            "the client gets S3's answer, UploadId included"
        );
        assert!(
            permit_returned,
            "the Create's cache-setup permit must be returned once the volume answers"
        );
        assert!(
            !upload_dir.exists(),
            "a Create answered before its cache setup finished must leave no tracker or upload \
             directory, so the upload is not cached: {:?}",
            std::fs::read_dir(&upload_dir)
                .map(|d| d.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
        );
    }

    /// the fleet-wide cap on in-progress multipart entries
    /// ([`MAX_TRACKED_MULTIPART_ENTRIES`]). Each test owns its cache directory, so the budget's
    /// per-directory state and the cap seam cannot couple parallel tests.
    mod tracked_multipart_cap {
        use super::*;

        const HOST: &str = "test-bucket.s3.amazonaws.com";

        /// Accept one connection, read its headers and `body_len` body bytes, reply `response`,
        /// and return the body bytes the stub received.
        async fn accept_capture_and_reply(
            listener: tokio::net::TcpListener,
            body_len: usize,
            response: Vec<u8>,
        ) -> Vec<u8> {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 65536];
            let header_end = loop {
                let n = sock.read(&mut tmp).await.unwrap();
                if n == 0 {
                    return Vec::new();
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            while buf.len() - header_end < body_len {
                let n = sock.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            let _ = sock.write_all(&response).await;
            let _ = sock.flush().await;
            buf[header_end..].to_vec()
        }

        async fn fixture(
            cache_dir: &std::path::Path,
            cap: Option<u64>,
        ) -> (
            Arc<crate::cache::CacheManager>,
            SignedPutHandler,
            Arc<RwLock<MetricsManager>>,
        ) {
            if let Some(cap) = cap {
                tracked_multipart_budget::set_cap_for_test(cache_dir, cap);
            }
            let cache_manager = initialized_put_cache_manager(cache_dir).await;
            let mut handler = write_through_handler(cache_dir, &cache_manager, u64::MAX);
            let metrics = Arc::new(RwLock::new(MetricsManager::new()));
            handler.set_metrics_manager(metrics.clone());
            (cache_manager, handler, metrics)
        }

        async fn create(
            handler: &SignedPutHandler,
            cache_key: &str,
            path: &str,
            upload_id: &str,
        ) -> StatusCode {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_and_reply_initiate_multipart(
                listener,
                upload_id.to_string(),
            ));
            let (req, _keepalive) =
                incoming_request_with_method("POST", &format!("{path}?uploads"), Vec::new()).await;
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_create_multipart_upload(
                    req,
                    cache_key.to_string(),
                    HOST.to_string(),
                    transport_for(addr),
                ),
            )
            .await
            .expect("CreateMultipartUpload did not finish within 60s")
            .expect("CreateMultipartUpload must reach the stub S3");
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upstream).await;
            response.status()
        }

        /// One UploadPart: the client's status and ETag, and the body the stub S3 received.
        #[allow(clippy::too_many_arguments)]
        async fn upload_part(
            handler: &mut SignedPutHandler,
            cache_key: &str,
            path: &str,
            upload_id: &str,
            part_number: u32,
            body: &[u8],
            etag: &str,
        ) -> (StatusCode, Option<String>, Vec<u8>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_capture_and_reply(
                listener,
                body.len(),
                format!("HTTP/1.1 200 OK\r\nETag: {etag}\r\nContent-Length: 0\r\n\r\n")
                    .into_bytes(),
            ));
            let (req, _keepalive) = incoming_upload_part_request(
                &format!("{path}?uploadId={upload_id}&partNumber={part_number}"),
                body.to_vec(),
            )
            .await;
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_upload_part(
                    req,
                    cache_key.to_string(),
                    HOST.to_string(),
                    transport_for(addr),
                    upload_id.to_string(),
                    part_number,
                ),
            )
            .await
            .expect("UploadPart did not finish within 60s")
            .expect("UploadPart must reach the stub S3");
            let received = tokio::time::timeout(std::time::Duration::from_secs(5), upstream)
                .await
                .expect("the stub S3 never finished reading the part")
                .unwrap();
            let etag = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            (response.status(), etag, received)
        }

        /// One CompleteMultipartUpload: the client's status, and whether the stub received the
        /// client's body and the client received the stub's answer, both unchanged.
        async fn complete(
            handler: &mut SignedPutHandler,
            cache_key: &str,
            path: &str,
            upload_id: &str,
            parts: &[(u32, &str)],
        ) -> (StatusCode, bool) {
            let complete_body = format!(
                "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
                parts
                    .iter()
                    .map(|(n, e)| format!(
                        "<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>"
                    ))
                    .collect::<String>()
            )
            .into_bytes();
            let result_xml = format!(
                "<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>{}</Key>\
                 <ETag>\"3858f62230ac3c915f300c664312c11f-{}\"</ETag>\
                 </CompleteMultipartUploadResult>",
                path.trim_start_matches('/'),
                parts.len()
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let upstream = tokio::spawn(accept_capture_and_reply(
                listener,
                complete_body.len(),
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                    result_xml.len(),
                    result_xml
                )
                .into_bytes(),
            ));
            let (req, _keepalive) = incoming_request_with_method(
                "POST",
                &format!("{path}?uploadId={upload_id}"),
                complete_body.clone(),
            )
            .await;
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                handler.handle_complete_multipart_upload(
                    req,
                    cache_key.to_string(),
                    HOST.to_string(),
                    transport_for(addr),
                    upload_id.to_string(),
                ),
            )
            .await
            .expect("CompleteMultipartUpload did not finish within 60s")
            .expect("CompleteMultipartUpload must reach the stub S3");
            let status = response.status();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let received = tokio::time::timeout(std::time::Duration::from_secs(5), upstream)
                .await
                .expect("the stub S3 never finished reading the Complete")
                .unwrap();
            (
                status,
                received == complete_body && body == result_xml.into_bytes(),
            )
        }

        async fn wait_until(what: impl Fn() -> bool, seconds: u64) -> bool {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
            loop {
                if what() {
                    return true;
                }
                if std::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }

        /// Whether a GET of `cache_key` is served from the cache within `seconds`. A detached
        /// finalization publishes within a few seconds, so waiting this long keeps "not served"
        /// from meaning only "not yet".
        async fn served_within(
            cache_dir: &std::path::Path,
            cache_key: &str,
            seconds: u64,
        ) -> Option<Vec<u8>> {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
            loop {
                if let Some(bytes) = served_from_cache(cache_dir, cache_key).await {
                    return Some(bytes);
                }
                if std::time::Instant::now() >= deadline {
                    return None;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        fn part_body(seed: u32) -> Vec<u8> {
            (0..4096u32).map(|i| ((i + seed) % 251) as u8).collect()
        }

        /// Red side (a): past the cap, CreateMultipartUpload writes no tracker, UploadPart stages
        /// nothing, both reach S3 unchanged, Complete returns S3's answer, and nothing is cached.
        #[tokio::test]
        async fn an_upload_past_the_cap_is_forwarded_uncached_and_tracks_nothing() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            // Room for one tracked upload with one recorded part.
            let (_cache_manager, mut handler, metrics) = fixture(dir, Some(2)).await;

            let (first_key, first_path, first_id) = (
                "test-bucket/case76-a-first",
                "/case76-a-first",
                "case76-a-first-upload",
            );
            assert_eq!(
                create(&handler, first_key, first_path, first_id).await,
                StatusCode::OK
            );
            let (status, _, _) = upload_part(
                &mut handler,
                first_key,
                first_path,
                first_id,
                1,
                &part_body(1),
                "\"case76-a-first-p1\"",
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert!(
                mpu_upload_dir(dir, first_id).join("upload.meta").exists(),
                "fixture precondition: the first upload is tracked, so it holds the budget"
            );

            let (key, path, id) = (
                "test-bucket/case76-a-past",
                "/case76-a-past",
                "case76-a-past-upload",
            );
            let skipped_before =
                skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await;
            let create_status = create(&handler, key, path, id).await;
            let tracker_after_create = mpu_upload_dir(dir, id).join("upload.meta").exists();
            let body = part_body(2);
            let (part_status, part_etag, received) = upload_part(
                &mut handler,
                key,
                path,
                id,
                1,
                &body,
                "\"case76-a-past-p1\"",
            )
            .await;
            let directory_after_part = mpu_upload_dir(dir, id).exists();
            let (complete_status, complete_unchanged) =
                complete(&mut handler, key, path, id, &[(1, "\"case76-a-past-p1\"")]).await;
            let cached = served_within(dir, key, 3).await.is_some();
            let skipped = skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await
                - skipped_before;
            assert_eq!(
                (
                    create_status,
                    tracker_after_create,
                    part_status,
                    part_etag.as_deref(),
                    received == body,
                    directory_after_part,
                    complete_status,
                    complete_unchanged,
                    cached,
                    skipped,
                ),
                (
                    StatusCode::OK,
                    false,
                    StatusCode::OK,
                    Some("\"case76-a-past-p1\""),
                    true,
                    false,
                    StatusCode::OK,
                    true,
                    false,
                    2,
                ),
                "(Create status, tracker written, UploadPart status, part ETag, S3 received the \
                 part's bytes, upload directory exists after the part, Complete status, Complete \
                 unchanged both ways, object cached, multipart_tracking_full skips): an upload past \
                 MAX_TRACKED_MULTIPART_ENTRIES must create nothing under mpus_in_progress/, reach \
                 S3 unchanged and not be cached"
            );
        }

        /// Control (b): below the cap an upload is tracked and write-cached as before.
        #[tokio::test]
        async fn an_upload_below_the_cap_is_tracked_and_write_cached() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let (_cache_manager, mut handler, metrics) = fixture(dir, None).await;
            let (key, path, id) = ("test-bucket/case76-b", "/case76-b", "case76-b-upload");
            let create_status = create(&handler, key, path, id).await;
            let tracked = mpu_upload_dir(dir, id).join("upload.meta").exists();
            let (p1, p2) = (part_body(3), part_body(4));
            let (s1, _, _) =
                upload_part(&mut handler, key, path, id, 1, &p1, "\"case76-b-p1\"").await;
            let (s2, _, _) =
                upload_part(&mut handler, key, path, id, 2, &p2, "\"case76-b-p2\"").await;
            let (complete_status, complete_unchanged) = complete(
                &mut handler,
                key,
                path,
                id,
                &[(1, "\"case76-b-p1\""), (2, "\"case76-b-p2\"")],
            )
            .await;
            let served = served_within(dir, key, 10).await;
            let expected: Vec<u8> = p1.iter().chain(p2.iter()).copied().collect();
            let skipped = skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await;
            assert_eq!(
                (
                    create_status,
                    tracked,
                    s1,
                    s2,
                    complete_status,
                    complete_unchanged,
                    served.as_deref() == Some(expected.as_slice()),
                    skipped,
                ),
                (
                    StatusCode::OK,
                    true,
                    StatusCode::OK,
                    StatusCode::OK,
                    StatusCode::OK,
                    true,
                    true,
                    0,
                ),
                "(Create status, tracked, part statuses, Complete status, Complete unchanged, \
                 served the uploaded bytes from the cache, multipart_tracking_full skips): below \
                 MAX_TRACKED_MULTIPART_ENTRIES an upload is write-cached"
            );
        }

        /// Red side (c): the figure is read from the shared directory, so another proxy, or this
        /// one after a restart, with no in-process state of its own, still refuses at the cap.
        #[tokio::test]
        async fn the_cap_is_read_from_the_shared_volume_so_another_proxy_and_a_restart_see_it() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let (_manager_a, mut proxy_a, _metrics_a) = fixture(dir, Some(2)).await;
            let (key_a, path_a, id_a) =
                ("test-bucket/case76-c-a", "/case76-c-a", "case76-c-a-upload");
            assert_eq!(create(&proxy_a, key_a, path_a, id_a).await, StatusCode::OK);
            let (status, _, _) = upload_part(
                &mut proxy_a,
                key_a,
                path_a,
                id_a,
                1,
                &part_body(5),
                "\"case76-c-p1\"",
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            let record = mpu_upload_dir(dir, id_a).join("part1.json");
            assert!(
                wait_until(|| record.exists(), 10).await,
                "fixture precondition: proxy A's part record never landed"
            );

            // Proxy B: its own CacheManager on the same directory, and no in-process budget
            // state for it, as another proxy or a restarted one starts.
            tracked_multipart_budget::forget_for_test(dir);
            let (_manager_b, proxy_b, metrics_b) = fixture(dir, None).await;
            let on_disk = tracked_multipart_budget::count_entries(dir);
            let (key_b, path_b, id_b) =
                ("test-bucket/case76-c-b", "/case76-c-b", "case76-c-b-upload");
            let status_b = create(&proxy_b, key_b, path_b, id_b).await;
            let tracked_b = mpu_upload_dir(dir, id_b).exists();
            let skipped_b =
                skipped_put_count(&metrics_b, MULTIPART_TRACKING_FULL_SKIP_REASON).await;
            assert_eq!(
                (on_disk, status_b, tracked_b, skipped_b),
                (Some(2), StatusCode::OK, false, 1),
                "(entries on the shared volume, proxy B's Create status, B tracked, B's \
                 multipart_tracking_full skips): a proxy with no in-process state must take the \
                 cap's figure from the shared directory"
            );
        }

        /// Red side (d): an upload whose part crosses the cap stops being cached. Its directory,
        /// recorded parts included, is removed; its Complete reaches S3 and returns S3's answer;
        /// no partial entry is published, and the key's prior cached version is not served.
        #[tokio::test]
        async fn an_upload_crossing_the_cap_mid_way_completes_and_leaves_no_partial_entry() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            // Room for the tracker and two recorded parts.
            let (cache_manager, mut handler, metrics) = fixture(dir, Some(3)).await;
            let (key, path, id) = ("test-bucket/case76-d", "/case76-d", "case76-d-upload");
            cache_v1(dir, &cache_manager, key, path, b"case76-d prior version").await;

            assert_eq!(create(&handler, key, path, id).await, StatusCode::OK);
            for n in 1..=2u32 {
                let (status, _, _) = upload_part(
                    &mut handler,
                    key,
                    path,
                    id,
                    n,
                    &part_body(10 + n),
                    &format!("\"case76-d-p{n}\""),
                )
                .await;
                assert_eq!(status, StatusCode::OK);
            }
            let upload_dir = mpu_upload_dir(dir, id);
            assert!(
                wait_until(
                    || upload_dir.join("part1.json").exists()
                        && upload_dir.join("part2.json").exists(),
                    10
                )
                .await,
                "fixture precondition: parts 1 and 2 were recorded, so part 3 crosses the cap"
            );

            let skipped_before =
                skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await;
            let body3 = part_body(13);
            let (s3, etag3, received3) =
                upload_part(&mut handler, key, path, id, 3, &body3, "\"case76-d-p3\"").await;
            let directory_removed = wait_until(|| !upload_dir.exists(), 5).await;
            let (complete_status, complete_unchanged) = complete(
                &mut handler,
                key,
                path,
                id,
                &[
                    (1, "\"case76-d-p1\""),
                    (2, "\"case76-d-p2\""),
                    (3, "\"case76-d-p3\""),
                ],
            )
            .await;
            let served = served_within(dir, key, 3).await;
            let skipped = skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await
                - skipped_before;
            assert_eq!(
                (
                    s3,
                    etag3.as_deref(),
                    received3 == body3,
                    directory_removed,
                    complete_status,
                    complete_unchanged,
                    served.map(|b| b.len()),
                    skipped,
                ),
                (
                    StatusCode::OK,
                    Some("\"case76-d-p3\""),
                    true,
                    true,
                    StatusCode::OK,
                    true,
                    None,
                    1,
                ),
                "(part 3 status, part 3 ETag, S3 received part 3's bytes, upload directory \
                 removed, Complete status, Complete unchanged, bytes served from the cache, \
                 multipart_tracking_full skips): an upload crossing the cap mid-way must stop \
                 being cached, release its entries, and complete at S3 untouched"
            );
        }

        /// Red side (e): admission holds the shared directory at the cap, so the walk over it is
        /// bounded by the cap. 60 admissions against a cap of 20, each writing an upload
        /// directory as a tracker write does.
        #[tokio::test]
        async fn admission_holds_the_shared_directory_at_the_cap() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            tracked_multipart_budget::set_cap_for_test(dir, 20);
            let mut admitted = 0u64;
            for i in 0..60 {
                if let Ok(slot) = tracked_multipart_budget::admit(dir, 1).await {
                    std::fs::create_dir_all(mpu_upload_dir(dir, &format!("case76-e-{i}"))).unwrap();
                    drop(slot);
                    admitted += 1;
                }
            }
            assert_eq!(
                (admitted, tracked_multipart_budget::count_entries(dir)),
                (20, Some(20)),
                "(admitted, entries on disk): 60 admissions against a cap of 20 must leave at \
                 most 20 entries for the walk"
            );
        }

        /// The budget's figure is unknown when the walk's listing fails. That refusal is
        /// `multipart_tracking_unknown`, not `multipart_tracking_full`, and it keeps the parts
        /// the upload already staged: a failed listing says nothing about the volume being full.
        #[tokio::test]
        async fn a_census_listing_error_is_unknown_and_keeps_the_staged_parts() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let (_cache_manager, mut handler, metrics) = fixture(dir, None).await;
            let (key, path, id) = ("test-bucket/case48-d", "/case48-d", "case48-d-upload");
            assert_eq!(create(&handler, key, path, id).await, StatusCode::OK);
            let (s1, _, _) = upload_part(
                &mut handler,
                key,
                path,
                id,
                1,
                &part_body(21),
                "\"case48-d-p1\"",
            )
            .await;
            assert_eq!(s1, StatusCode::OK);
            let record = mpu_upload_dir(dir, id).join("part1.json");
            assert!(
                wait_until(|| record.exists(), 10).await,
                "fixture precondition: part 1 was staged and recorded"
            );

            // The next walk's listing fails, and this process has no count yet.
            tracked_multipart_budget::forget_for_test(dir);
            set_in_progress_listing_fails_for_tests(dir, true);
            let full_before =
                skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await;
            let unknown_before =
                skipped_put_count(&metrics, MULTIPART_TRACKING_UNKNOWN_SKIP_REASON).await;
            let body2 = part_body(22);
            let (s2, etag2, received2) =
                upload_part(&mut handler, key, path, id, 2, &body2, "\"case48-d-p2\"").await;
            let part1_removed = wait_until(|| !record.exists(), 2).await;
            set_in_progress_listing_fails_for_tests(dir, false);
            let full = skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await
                - full_before;
            let unknown = skipped_put_count(&metrics, MULTIPART_TRACKING_UNKNOWN_SKIP_REASON).await
                - unknown_before;
            assert_eq!(
                (
                    s2,
                    etag2.as_deref(),
                    received2 == body2,
                    part1_removed,
                    unknown,
                    full
                ),
                (StatusCode::OK, Some("\"case48-d-p2\""), true, false, 1, 0),
                "(part 2 status, part 2 ETag, S3 received part 2's bytes, staged part 1 removed, \
                 multipart_tracking_unknown skips, multipart_tracking_full skips): a count whose \
                 listing failed must refuse as unknown and keep the parts already staged"
            );
        }

        /// A part for an upload with no directory creates the directory and its record, two walk
        /// entries, so it is charged two. Room for two entries, one taken by a tracked upload: a
        /// part of an untracked upload must not fit.
        #[tokio::test]
        async fn a_part_that_creates_its_upload_directory_is_charged_both_entries() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let (_cache_manager, mut handler, metrics) = fixture(dir, Some(2)).await;
            let (key_a, path_a, id_a) = (
                "test-bucket/case48-f1-a",
                "/case48-f1-a",
                "case48-f1-a-upload",
            );
            assert_eq!(create(&handler, key_a, path_a, id_a).await, StatusCode::OK);
            assert!(
                wait_until(
                    || mpu_upload_dir(dir, id_a).join("upload.meta").exists(),
                    10
                )
                .await,
                "fixture precondition: upload A is tracked, holding one entry"
            );
            let (key_b, path_b, id_b) = (
                "test-bucket/case48-f1-b",
                "/case48-f1-b",
                "case48-f1-b-upload",
            );
            let body = part_body(31);
            let (status, _, received) = upload_part(
                &mut handler,
                key_b,
                path_b,
                id_b,
                1,
                &body,
                "\"case48-f1-p1\"",
            )
            .await;
            let record_b = mpu_upload_dir(dir, id_b).join("part1.json");
            let staged_b = wait_until(|| record_b.exists(), 3).await;
            let full = skipped_put_count(&metrics, MULTIPART_TRACKING_FULL_SKIP_REASON).await;
            assert_eq!(
                (
                    status,
                    received == body,
                    staged_b,
                    tracked_multipart_budget::count_entries(dir),
                    full
                ),
                (StatusCode::OK, true, false, Some(1), 1),
                "(part status, S3 received the part, part of untracked upload B staged, entries on \
                 disk, multipart_tracking_full skips): a part that would create its upload \
                 directory must be charged both entries, or the volume holds more than the cap"
            );
        }

        /// Measurement for the cap's level, not a gate: builds 20,000 upload directories with one
        /// part record each and times one `in_progress_part_bytes` walk and one census count.
        /// Run with `cargo test --release --lib walk_cost -- --ignored --nocapture`.
        #[test]
        #[ignore = "timing measurement: builds 20,000 upload directories"]
        fn walk_cost_per_entry() {
            const UPLOADS: u64 = 20_000;
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            for i in 0..UPLOADS {
                let upload = mpu_upload_dir(dir, &format!("case76-walk-{i}"));
                std::fs::create_dir_all(&upload).unwrap();
                std::fs::write(upload.join("upload.meta"), b"{}").unwrap();
                std::fs::write(upload.join("part1.json"), br#"{"compressed_size":4096}"#).unwrap();
            }
            let entries = UPLOADS * 2;
            let started = std::time::Instant::now();
            let bytes = in_progress_part_bytes(dir);
            let walk = started.elapsed();
            let started = std::time::Instant::now();
            let counted = tracked_multipart_budget::count_entries(dir);
            let census = started.elapsed();
            assert_eq!(bytes, Some(UPLOADS * 4096));
            assert_eq!(counted, Some(entries));
            println!(
                "walk: {entries} entries, in_progress_part_bytes {walk:?} ({:.2} us/entry), \
                 count_entries {census:?} ({:.2} us/entry)",
                walk.as_secs_f64() * 1e6 / entries as f64,
                census.as_secs_f64() * 1e6 / entries as f64
            );
        }
    }

    /// the walk of `mpus_in_progress/` spread over threads.
    mod incremental_walk {
        use super::*;

        fn record(dir: &std::path::Path, upload: &str, n: u32, size: u64) -> std::path::PathBuf {
            let upload = mpu_upload_dir(dir, upload);
            std::fs::create_dir_all(&upload).unwrap();
            let info = crate::cache_types::CachedPartInfo::new(
                n,
                size * 2,
                format!("\"case77-{n}\""),
                crate::compression::CompressionAlgorithm::Lz4,
            )
            .with_compressed_size(size);
            // The production writer: temporary file, fsync, rename over the record.
            SignedPutHandler::record_part_blocking(&upload, n, &info).unwrap();
            upload.join(format!("part{n}.json"))
        }

        /// (a) A walk after N new parts opens the records of the N upload directories that
        /// changed, not every record. Those directories' other records come with them: a
        /// directory whose own `stat` moved cannot have its remembered sizes reused (see
        /// [`UploadDirFingerprint`]), so the 5 new parts cost 5 x 11 reads, not 200.
        #[test]
        fn a_walk_after_new_parts_opens_only_the_new_records() {
            let temp = tempfile::TempDir::new().unwrap();
            let dir = temp.path();
            let mut expected = 0u64;
            for u in 0..20u32 {
                for n in 1..=10u32 {
                    let size = 1_000 + u64::from(u * 100 + n);
                    record(dir, &format!("case77-a-{u}"), n, size);
                    expected += size;
                }
            }
            let first = walk_in_progress(dir).bytes;
            let opens_first = part_record_opens_for_test(dir);
            for u in 0..5u32 {
                let size = 9_000 + u64::from(u);
                record(dir, &format!("case77-a-{u}"), 11, size);
                expected += size;
            }
            let second = walk_in_progress(dir).bytes;
            let opens_second = part_record_opens_for_test(dir) - opens_first;
            assert_eq!(
                (first.is_some(), opens_first, second, opens_second),
                (true, 200, Some(expected), 55),
                "(first walk readable, records the first walk opened, P after 5 new parts, \
                 records the second walk opened): a walk must open only the records of the \
                 upload directories that changed since the last one"
            );
        }

        /// (e) The schedule [`PartRecordFingerprint`] alone cannot see: one record replaced
        /// twice between two walks, the second replacement given back the inode number, change
        /// time and length of the first. Its remembered size must not be reused, because its
        /// upload directory's own `stat` moved.
        #[test]
        fn a_record_replaced_twice_does_not_reuse_its_remembered_size() {
            let temp = tempfile::TempDir::new().unwrap();
            let dir = temp.path();
            record(dir, "case78-e", 1, 1_000);
            let part2 = record(dir, "case78-e", 2, 3_000);
            let before = walk_in_progress(dir).bytes;
            // Long enough that the two renames below move the upload directory's timestamps
            // even on a file system whose directory clock is coarse.
            std::thread::sleep(std::time::Duration::from_millis(20));
            record(dir, "case78-e", 2, 4_000);
            record(dir, "case78-e", 2, 5_000);
            // Same JSON length throughout, so only the inode and change time are in question;
            // the seam sets them to the replaced record's own, which is the collision.
            forge_remembered_part_record_for_test(dir, &part2, 3_000);
            let after = walk_in_progress(dir).bytes;
            assert_eq!(
                (before, after),
                (Some(4_000), Some(6_000)),
                "(P before, P after part 2 was replaced twice with its own stat remembered): a \
                 remembered size from an upload directory whose stat has moved must be read again"
            );
        }

        /// (b) A record replaced the way the writer replaces it is read again.
        #[test]
        fn a_replaced_record_is_read_again() {
            let temp = tempfile::TempDir::new().unwrap();
            let dir = temp.path();
            record(dir, "case77-b", 1, 1_000);
            record(dir, "case77-b", 2, 2_000);
            let before = walk_in_progress(dir).bytes;
            // A retried UploadPart for part 2 with other bytes; same length of JSON on purpose.
            record(dir, "case77-b", 2, 3_000);
            let after = walk_in_progress(dir).bytes;
            assert_eq!(
                (before, after),
                (Some(3_000), Some(4_000)),
                "(P before, P after part 2 was replaced): a replaced record must be read again"
            );
        }

        /// (c) A record that becomes unreadable makes P unknown even though an earlier walk
        /// read it.
        #[test]
        fn a_record_unreadable_after_a_read_stays_unknown() {
            let temp = tempfile::TempDir::new().unwrap();
            let dir = temp.path();
            record(dir, "case77-c", 1, 1_000);
            let path = record(dir, "case77-c", 2, 2_000);
            let before = walk_in_progress(dir).bytes;
            // Replaced by rename, as the writer would, with content that does not parse.
            let tmp = path.with_extension("json.tmp");
            std::fs::write(&tmp, b"not json").unwrap();
            std::fs::rename(&tmp, &path).unwrap();
            let after = walk_in_progress(dir).bytes;
            let again = walk_in_progress(dir).bytes;
            assert_eq!(
                (before, after, again),
                (Some(3_000), None, None),
                "(P before, P after part 2 became unparseable, P on the walk after): an \
                 unreadable record must stay UNREADABLE, never its remembered size"
            );
        }

        /// A removed upload is forgotten: P drops by its bytes.
        #[test]
        fn a_removed_upload_leaves_the_sum() {
            let temp = tempfile::TempDir::new().unwrap();
            let dir = temp.path();
            record(dir, "case77-r1", 1, 1_000);
            record(dir, "case77-r2", 1, 2_000);
            let before = walk_in_progress(dir).bytes;
            std::fs::remove_dir_all(mpu_upload_dir(dir, "case77-r2")).unwrap();
            let after = walk_in_progress(dir).bytes;
            assert_eq!((before, after), (Some(3_000), Some(1_000)));
        }
    }

    /// the deciding readers compose the last full walk with every proxy's published
    /// in-progress growth, instead of walking themselves.
    mod published_in_progress_figure {
        use super::*;

        /// A part record written by THIS process, through the production writer, so this
        /// process's own figure owns its bytes.
        fn own_record(dir: &std::path::Path, upload: &str, n: u32, size: u64) {
            let upload = mpu_upload_dir(dir, upload);
            std::fs::create_dir_all(&upload).unwrap();
            let info = crate::cache_types::CachedPartInfo::new(
                n,
                size * 2,
                format!("\"case79-{n}\""),
                crate::compression::CompressionAlgorithm::Lz4,
            )
            .with_compressed_size(size);
            SignedPutHandler::record_part_blocking(&upload, n, &info).unwrap();
        }

        /// A part record on the volume that this process did NOT write: another proxy's. Written
        /// as the file a walk reads, with no call into this process's own figure.
        fn peer_record(dir: &std::path::Path, upload: &str, n: u32, size: u64) {
            let upload = mpu_upload_dir(dir, upload);
            std::fs::create_dir_all(&upload).unwrap();
            std::fs::write(
                upload.join(format!("part{n}.json")),
                format!(r#"{{"compressed_size":{size}}}"#),
            )
            .unwrap();
        }

        /// A peer with a journal is in the roster, so a missing or stale figure of its is
        /// detectable.
        fn peer_journal(dir: &std::path::Path, instance_id: &str) {
            let journals = dir.join("metadata").join("_journals");
            std::fs::create_dir_all(&journals).unwrap();
            std::fs::write(journals.join(format!("{instance_id}.journal")), b"").unwrap();
        }

        fn now() -> u64 {
            in_progress_figure_now_secs()
        }

        /// The default `consolidation_interval`, which is what a proxy publishes on.
        const CYCLE_SECS: u64 = 5;
        const CYCLE: std::time::Duration = std::time::Duration::from_secs(CYCLE_SECS);

        /// (a) In the steady state a deciding caller starts no walk at all: it composes on the
        /// walk the metrics collector already made, however many decisions it makes. The kept
        /// walk is aged past `IN_PROGRESS_OBSERVATION_REUSE` before every decision, which is what
        /// used to force a walk of its own.
        #[tokio::test]
        async fn a_decider_starts_no_walk_in_the_steady_state() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            own_record(dir, "case79-a", 1, 7_000);
            assert!(publish_own_in_progress_if_due_blocking(dir, CYCLE).unwrap());
            // The collector's walk: the base every decision composes on.
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            let mut figures = Vec::new();
            for _ in 0..5 {
                age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
                figures.push(
                    compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE)
                        .await
                        .bytes,
                );
            }
            let walks_by_deciders = in_progress_walks_for_tests(dir) - walks_after_base;
            assert_eq!(
                (
                    base.walk.bytes,
                    walks_after_base,
                    walks_by_deciders,
                    figures.clone()
                ),
                (Some(7_000), 1, 0, vec![Some(7_000); 5]),
                "(P in the base walk, walks by the collection, walks by 5 decisions, each \
                 decision's P): a decider composes on the published figures and starts no walk"
            );
        }

        /// (b) A live peer's new parts are in the decider's figure within one publication, from
        /// the peer's document, with no walk.
        #[tokio::test]
        async fn a_live_peers_new_parts_appear_within_one_cycle() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let peer = "case79-b-peer:11";
            peer_journal(dir, peer);
            peer_record(dir, "case79-b-1", 1, 4_000);
            publish_peer_in_progress_figure_for_test(dir, peer, 4_000, 4_000, now(), CYCLE_SECS);
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            // The peer records one more part and republishes on its next cycle. This process
            // never reads the new record: only the peer's document.
            peer_record(dir, "case79-b-1", 2, 1_500);
            publish_peer_in_progress_figure_for_test(dir, peer, 5_500, 5_500, now(), CYCLE_SECS);
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            assert_eq!(
                (
                    base.walk.bytes,
                    composed.bytes,
                    composed.growth_bytes,
                    composed.fell_back,
                    in_progress_walks_for_tests(dir) - walks_after_base
                ),
                (Some(4_000), Some(5_500), 1_500, false, 0),
                "(P in the base walk, composed P, growth, fell back, walks): a live peer's new \
                 parts are in the decider's figure within one publication and cost no walk"
            );
        }

        /// (c) A dead peer's bytes stay counted. Its parts landed AFTER the base walk, so the
        /// growth term is the only place they could come from and its document is stale, which
        /// must send this reader to a fresh walk rather than drop the instance. The figure never
        /// reads below the true walk.
        #[tokio::test]
        async fn a_dead_peers_bytes_stay_counted_and_never_read_below_the_walk() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let peer = "case79-c-peer:22";
            peer_journal(dir, peer);
            // The base walk is taken before the peer writes anything.
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            peer_record(dir, "case79-c-1", 1, 2_000);
            // Published, then the proxy died: the document is past its lease and will never move
            // again.
            publish_peer_in_progress_figure_for_test(
                dir,
                peer,
                2_000,
                2_000,
                now() - published_figure_lease_secs(CYCLE_SECS) - 540,
                CYCLE_SECS,
            );
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            let walks_by_decision = in_progress_walks_for_tests(dir) - walks_after_base;
            let true_walk = walk_in_progress(dir).bytes;
            assert_eq!(
                (
                    base.walk.bytes,
                    composed.bytes,
                    composed.fell_back,
                    walks_by_decision,
                    composed.bytes >= true_walk,
                    true_walk
                ),
                (Some(0), Some(2_000), true, 1, true, Some(2_000)),
                "(P in the base walk, composed P, fell back, walks by the decision, composed \
                 P is at least the true walk, the true walk): a dead peer's figure is past its \
                 lease, so the decider walks and keeps its bytes rather than dropping them"
            );
        }

        /// A pid of this machine that has certainly stopped: spawned and reaped, so
        /// `kill(pid, 0)` reports `ESRCH` for it.
        fn dead_pid() -> u32 {
            let mut child = std::process::Command::new("true").spawn().unwrap();
            let pid = child.id();
            child.wait().unwrap();
            pid
        }

        /// This machine's hostname, as every instance identity on the volume spells it.
        fn my_host() -> String {
            own_in_progress_instance_id()
                .rsplit_once(':')
                .unwrap()
                .0
                .to_string()
        }

        /// (4a). THE RESTART CASE. A predecessor of this host left a document behind, its
        /// pid is dead, and its journal has already gone (which is what happens within a cycle
        /// or two of a restart: a journal holding nothing to apply is removed). The startup
        /// retire unlinks the document, and the decider composes without a walk.
        ///
        /// The predecessor deliberately has **no** journal. With one, `JOURNAL_ROSTER_WINDOW_SECS`
        /// keeps it in the roster and the decider walks on both trees, so the assertion would
        /// pass against a retire that does nothing.
        #[tokio::test]
        async fn a_stopped_predecessor_of_this_host_is_retired_at_startup() {
            let _guard = crate::cache::hostname_flag_test_guard().await;
            crate::cache::set_hostname_not_unique_for_test(false);
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let predecessor = format!("{}:{}", my_host(), dead_pid());
            // Its part records are on the volume and counted by the walk, as they are after any
            // restart.
            peer_record(dir, "case80-a-1", 1, 4_400);
            publish_peer_in_progress_figure_for_test(
                dir,
                &predecessor,
                4_400,
                4_400,
                now() - published_figure_lease_secs(CYCLE_SECS) - 60,
                CYCLE_SECS,
            );
            // The base walk, taken after the predecessor stopped, so its bytes are in it.
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            let candidates = own_host_retire_candidates_blocking(dir);
            let retired = retire_quiet_own_host_figures_blocking(dir, &candidates);
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE),
            )
            .await
            .expect("compose_in_progress_part_bytes must not hang");
            let walks_by_decision = in_progress_walks_for_tests(dir) - walks_after_base;
            crate::cache::set_hostname_not_unique_for_test(false);
            assert_eq!(
                (
                    candidates.len(),
                    retired,
                    base.walk.bytes,
                    composed.bytes,
                    composed.fell_back,
                    walks_by_decision,
                    crate::cache::hostname_uniqueness_holds()
                ),
                (1, 1, Some(4_400), Some(4_400), false, 0, true),
                "(candidates, retired, P in the base walk, composed P, fell back, walks by the \
                 decision, the prerequisite still holds): a stopped predecessor of this host must \
                 be retired at startup, so the decider composes on the base walk instead of \
                 walking the volume itself for the next hour"
            );
        }

        /// (4b). A live peer sharing this hostname, whose pid reads dead here because it is
        /// in another pid namespace, keeps publishing. The retire must leave its document alone
        /// and raise the prerequisite flag, not unlink the figure of a proxy that is still
        /// recording parts.
        #[tokio::test]
        async fn a_live_same_hostname_peer_is_not_retired_and_raises_the_flag() {
            let _guard = crate::cache::hostname_flag_test_guard().await;
            crate::cache::set_hostname_not_unique_for_test(false);
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let peer = format!("{}:{}", my_host(), dead_pid());
            let path =
                in_progress_figures_dir(dir).join(format!("{peer}.{IN_PROGRESS_FIGURE_EXT}"));
            publish_peer_in_progress_figure_for_test(
                dir,
                &peer,
                1_000,
                1_000,
                now() - 30,
                CYCLE_SECS,
            );
            let candidates = own_host_retire_candidates_blocking(dir);
            // It publishes again while the retire watches it: the one thing a stopped process
            // cannot do.
            publish_peer_in_progress_figure_for_test(dir, &peer, 2_000, 2_000, now(), CYCLE_SECS);
            let retired = retire_quiet_own_host_figures_blocking(dir, &candidates);
            let still_there = path.exists();
            let flagged = !crate::cache::hostname_uniqueness_holds();
            // A second pass must now find nothing at all, because the flag is up.
            let after_flag = own_host_retire_candidates_blocking(dir).len();
            crate::cache::set_hostname_not_unique_for_test(false);
            assert_eq!(
                (candidates.len(), retired, still_there, flagged, after_flag),
                (1, 0, true, true, 0),
                "(candidates, retired, the document survives, the prerequisite flag is raised, \
                 candidates on the next pass): a document that moved while it was watched \
                 belongs to a live peer this machine cannot see, so it must be kept and the \
                 own-host shortcuts must turn off"
            );
        }

        /// (4c). Two processes holding this process's exact `{host}:{pid}` - two containers
        /// sharing a hostname, both pid 1 - write the same document. The nonce is what makes
        /// that visible, and it is read only for a document refreshed after this process
        /// started, so a predecessor that happened to reuse the pid is not a collision.
        #[test]
        fn an_exact_identity_collision_with_two_nonces_raises_the_flag() {
            let _guard = crate::cache::hostname_flag_test_guard_blocking();
            crate::cache::set_hostname_not_unique_for_test(false);
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let own = own_in_progress_instance_id();
            // First, the same identity published BEFORE this process started: a predecessor
            // that reused the pid, which must not be read as a collision.
            publish_peer_in_progress_figure_for_test(
                dir,
                &own,
                1_000,
                1_000,
                crate::cache::process_start_secs().saturating_sub(600),
                CYCLE_SECS,
            );
            let before_start = detect_hostname_collision_blocking(dir, CYCLE);
            let held_after_old_doc = crate::cache::hostname_uniqueness_holds();
            // Now the same identity, with a different nonce, refreshed after this process
            // started: another process is live under this exact identity.
            publish_peer_in_progress_figure_for_test(dir, &own, 2_000, 2_000, now(), CYCLE_SECS);
            let detected = detect_hostname_collision_blocking(dir, CYCLE);
            let flagged = !crate::cache::hostname_uniqueness_holds();
            crate::cache::set_hostname_not_unique_for_test(false);
            assert_eq!(
                (before_start, held_after_old_doc, detected, flagged),
                (None, true, Some(HostnameCollision::ExactIdentity), true),
                "(detection on a document older than this process, the prerequisite still held \
                 then, detection on one refreshed since, the flag is raised): a foreign nonce \
                 under this process's own identity is a collision only when it was written after \
                 this process started"
            );
        }

        /// The startup retire watches a candidate for as long as **its** writer
        /// publishes on, not for a flat 15 s.
        ///
        /// At `consolidation_interval: 60s`, which `docs/SHARED_STORAGE.md` recommends reaching
        /// for under lock contention, a healthy peer moves `written_at` once a minute, so a 15 s
        /// watch finds it quiet as a matter of routine and the movement check distinguishes
        /// nothing. The watch is read through the production entry point as the virtual time it
        /// spends waiting, on a paused clock: exact and raceless, where a background publisher
        /// racing a `spawn_blocking` would be neither.
        #[tokio::test(start_paused = true)]
        async fn the_retire_watch_comes_from_the_publishers_own_cadence() {
            let _guard = crate::cache::hostname_flag_test_guard().await;
            crate::cache::set_hostname_not_unique_for_test(false);
            reset_noted_collision_peers_for_test();
            /// A cadence at the documented upper end, where the flat floor is shorter than one
            /// of the publisher's own intervals.
            const SLOW_SECS: u64 = 60;
            let slow = std::time::Duration::from_secs(SLOW_SECS);
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let predecessor = format!("{}:{}", my_host(), dead_pid());
            publish_peer_in_progress_figure_for_test(
                dir,
                &predecessor,
                1_000,
                1_000,
                now() - published_figure_lease_secs(SLOW_SECS) - 60,
                SLOW_SECS,
            );
            let candidates = own_host_retire_candidates_blocking(dir);
            let derived = startup_retire_watch(&candidates, slow);
            // The deferred startup reap pass derives its own watch from the same law, and waits
            // not at all when no document on the volume could raise a second-look collision.
            let deferred = published_cadence_watch_blocking(dir, slow);
            let nothing_to_watch =
                published_cadence_watch_blocking(TempDir::new().unwrap().path(), slow);
            let started = tokio::time::Instant::now();
            let retired = tokio::time::timeout(
                std::time::Duration::from_secs(3_600),
                retire_dead_own_host_in_progress_figures(dir, slow),
            )
            .await
            .expect("the startup retire must not hang");
            let watched = started.elapsed();
            crate::cache::set_hostname_not_unique_for_test(false);
            reset_noted_collision_peers_for_test();
            assert_eq!(
                (
                    candidates.len(),
                    derived.as_secs(),
                    watched.as_secs() >= SLOW_SECS,
                    retired,
                    deferred.as_secs(),
                    nothing_to_watch.as_secs()
                ),
                (1, 2 * SLOW_SECS, true, 1, 2 * SLOW_SECS, 0),
                "(candidates, the derived watch in seconds, the watch covered one of the \
                 publisher's own intervals, retired, the deferred reap pass's watch, its watch \
                 on a volume with nothing to re-observe): a candidate that publishes once every \
                 60s is quiet across any shorter watch as a matter of routine, so the watch must \
                 be derived from its cadence rather than from the lease floor"
            );
        }

        /// Condition 2 raises the prerequisite only on a **second** observation
        /// whose `written_at` moved.
        ///
        /// Three passes on one fixture, which is what separates the two states the branch
        /// exists to tell apart: a predecessor that exited between its last publication and
        /// this process's first look writes once and never again, and a live peer in a pid
        /// namespace this machine cannot see keeps publishing. The observation clock is moved
        /// instead of slept through ([`advance_observation_clock_for_test`]).
        #[test]
        fn condition_two_raises_only_on_a_second_observation_that_moved() {
            let _guard = crate::cache::hostname_flag_test_guard_blocking();
            crate::cache::set_hostname_not_unique_for_test(false);
            reset_noted_collision_peers_for_test();
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let peer = format!("{}:{}", my_host(), dead_pid());
            // A stopped predecessor's last write: one observation of a document whose pid reads
            // dead here, which on its own is not evidence of anything.
            publish_peer_in_progress_figure_for_test(dir, &peer, 1_000, 1_000, now(), CYCLE_SECS);
            let first = detect_hostname_collision_blocking(dir, CYCLE);
            let after_first = !crate::cache::hostname_uniqueness_holds();
            // A second observation past the recheck this document's own cadence derives, with
            // the document unchanged: silence is what a stopped process looks like.
            advance_observation_clock_for_test(publisher_watch_secs(CYCLE_SECS, CYCLE) + 1);
            let quiet = detect_hostname_collision_blocking(dir, CYCLE);
            let after_quiet = !crate::cache::hostname_uniqueness_holds();
            // Then it publishes again, which is the one thing a stopped process cannot do. The
            // first observation is kept across the quiet pass, so this pass is still past the
            // recheck without moving the clock again.
            publish_peer_in_progress_figure_for_test(
                dir,
                &peer,
                2_000,
                2_000,
                now() + 1,
                CYCLE_SECS,
            );
            let moved = detect_hostname_collision_blocking(dir, CYCLE);
            let after_moved = !crate::cache::hostname_uniqueness_holds();
            crate::cache::set_hostname_not_unique_for_test(false);
            reset_noted_collision_peers_for_test();
            assert_eq!(
                (first, after_first, quiet, after_quiet, moved, after_moved),
                (
                    None,
                    false,
                    None,
                    false,
                    Some(HostnameCollision::LivePeerReadingDead),
                    true
                ),
                "(first pass, flagged after it, second pass with the document unchanged, \
                 flagged after it, third pass after it published again, flagged after it): a \
                 dead-reading pid is a collision only once the document it owns has moved \
                 between two observations, or a predecessor's final write would read as a live \
                 peer"
            );
        }

        /// (f) An instance in the roster that publishes nothing at all - an older binary, or a
        /// proxy that has not published yet - forces the walk fallback, and its parts are counted
        /// by that walk.
        #[tokio::test]
        async fn an_instance_with_no_published_figure_forces_the_walk() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let peer = "case79-f-peer:33";
            peer_journal(dir, peer);
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            peer_record(dir, "case79-f-1", 1, 2_500);
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            assert_eq!(
                (
                    base.walk.bytes,
                    composed.bytes,
                    composed.fell_back,
                    in_progress_walks_for_tests(dir) - walks_after_base
                ),
                (Some(0), Some(2_500), true, 1),
                "(P in the base walk, composed P, fell back, walks by the decision): an instance \
                 that publishes no figure must force a walk, not be read as no parts"
            );
        }

        /// The lease a document earns is three of the interval its WRITER
        /// publishes on, so at the 5 s default a document 20 s old is past it and the decider
        /// walks. A fixed sixty-second lease trusted that same document, which is how a peer
        /// whose cycle ran long hid new parts for twelve cycles.
        #[tokio::test]
        async fn the_lease_is_three_of_the_publishers_own_cycles() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let peer = "case79-lease-peer:44";
            peer_journal(dir, peer);
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            // The peer recorded a part and then stopped publishing 20 s ago: inside the old
            // fixed 60 s lease, outside the 15 s its 5 s cycle earns.
            peer_record(dir, "case79-lease-1", 1, 3_300);
            publish_peer_in_progress_figure_for_test(dir, peer, 0, 0, now() - 20, CYCLE_SECS);
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            let walks_by_decision = in_progress_walks_for_tests(dir) - walks_after_base;
            let true_walk = walk_in_progress(dir).bytes;
            assert_eq!(
                (
                    published_figure_lease_secs(CYCLE_SECS),
                    base.walk.bytes,
                    composed.bytes,
                    composed.fell_back,
                    walks_by_decision,
                    true_walk
                ),
                (15, Some(0), Some(3_300), true, 1, Some(3_300)),
                "(the lease a 5s cycle earns, P in the base walk, composed P, fell back, walks \
                 by the decision, the true walk): a document 20s old is past the lease its own \
                 publication interval earns, so the decider walks and sees the peer's part"
            );
        }

        /// A removal performed by ANOTHER proxy - a Complete, an Abort
        /// or the sweep, each a `remove_dir_all` of the upload directory wherever it landed -
        /// never reaches this process's removal hook. The publication-time prune is what brings
        /// its own figure back down and bounds `records`, and it cannot drive the figure below
        /// the truth because it only drops records whose directory is gone.
        #[tokio::test]
        async fn a_peers_removal_brings_the_writers_figure_down_and_bounds_its_records() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            own_record(dir, "case79-e-keep", 1, 1_100);
            own_record(dir, "case79-e-gone", 1, 2_200);
            own_record(dir, "case79-e-gone", 2, 3_300);
            assert!(publish_own_in_progress_if_due_blocking(dir, CYCLE).unwrap());
            let before = own_in_progress_figure(dir);
            let records_before = own_in_progress_records_len_for_test(dir);
            // Another proxy completes that upload: the whole directory goes, on a proxy that is
            // not this one, so no hook of this process fires.
            std::fs::remove_dir_all(mpu_upload_dir(dir, "case79-e-gone")).unwrap();
            // Whether this publication wrote a document is not the claim: the prune's effect on
            // the figure and on `records` is, and asserting the return would make a weaker
            // failure stand in for it.
            publish_own_in_progress_if_due_blocking(dir, CYCLE).unwrap();
            let after = own_in_progress_figure(dir);
            let records_after = own_in_progress_records_len_for_test(dir);
            let true_walk = walk_in_progress(dir).bytes;
            assert_eq!(
                (
                    before,
                    records_before,
                    after,
                    records_after,
                    after.0 >= true_walk.unwrap_or(0),
                    true_walk
                ),
                ((6_600, 6_600), 3, (1_100, 6_600), 1, true, Some(1_100)),
                "((bytes, added_ever) before, records before, after, records after, the figure \
                 is still at least the truth, the true walk): a peer's removal reaches the \
                 writer's own figure through the prune, and never takes it below the truth"
            );
        }

        /// The carry-over: the fallback must not act on a walk that was
        /// already in flight when it arrived and began arbitrarily long before: it walks again
        /// rather than accept one older than the bound it asked for.
        #[tokio::test]
        async fn the_fallback_never_acts_on_a_walk_older_than_its_bound() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            own_record(dir, "case79-join", 1, 900);
            // The next walk reports a `began_at` 10 s in the past, which is what a walk already
            // in flight when this reader arrives looks like.
            backdate_next_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let walks_before = in_progress_walks_for_tests(dir);
            let observation = observe_in_progress(dir, IN_PROGRESS_OBSERVATION_REUSE).await;
            assert_eq!(
                (
                    observation.walk.bytes,
                    observation.began_at.elapsed() <= IN_PROGRESS_OBSERVATION_REUSE,
                    in_progress_walks_for_tests(dir) - walks_before
                ),
                (Some(900), true, 2),
                "(P, the returned walk is inside the bound asked for, walks): a walk that began \
                 before the bound must not be returned to a reader that asked for a fresh one"
            );
        }

        /// One decision waits on one walk. A walk this caller started is
        /// accepted even though it took longer than the bound asked for, because it began after
        /// the call: no walk can return anything newer, and the rule it replaces paid a second
        /// walk of the same length on every call once a walk crossed the bound. A walk that was
        /// already in flight before the call is still refused on its age
        /// (`the_fallback_never_acts_on_a_walk_older_than_its_bound`).
        #[tokio::test]
        async fn a_decision_waits_on_at_most_one_walk_it_started_itself() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            own_record(dir, "case81-slow", 1, 1_300);
            // A walk that takes longer than the bound this reader asks for, which is the regime
            // the finding is about.
            set_between_snapshot_and_walk_hook_for_tests(
                dir,
                Box::new(|| std::thread::sleep(std::time::Duration::from_millis(50))),
            );
            let bound = std::time::Duration::from_millis(10);
            let walks_before = in_progress_walks_for_tests(dir);
            let observation = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                observe_in_progress(dir, bound),
            )
            .await
            .expect("observe_in_progress must not hang");
            assert_eq!(
                (
                    observation.walk.bytes,
                    in_progress_walks_for_tests(dir) - walks_before,
                    observation.began_at.elapsed() > bound
                ),
                (Some(1_300), 1, true),
                "(P, walks this one decision waited on, the accepted walk is older than the bound \
                 it asked for): a walk that began after the call is accepted whatever it cost, so \
                 one decision pays one walk"
            );
        }

        /// (d) A missed update - a part record on the volume that no instance's `added_ever`
        /// counted - is caught by the once-a-minute reconciliation, which compares the newest
        /// walk against the figure the walk before it supports. Once it reads below, every
        /// decider takes the walk until the next collection replaces the verdict.
        #[tokio::test]
        async fn a_missed_update_is_caught_by_the_reconciliation_and_the_decider_walks() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            // Collection one.
            observe_in_progress(dir, std::time::Duration::ZERO).await;
            // A record written without its counter moving: the missed update.
            peer_record(dir, "case79-d-1", 1, 1_500);
            // Collection two, then the reconciliation it carries.
            observe_in_progress(dir, std::time::Duration::ZERO).await;
            let reconciliation = reconcile_in_progress_figures(dir);
            // More unaccounted bytes arrive after that collection's walk, so composing on it
            // would under-count; the verdict is what stops a decider doing so.
            peer_record(dir, "case79-d-1", 2, 700);
            // In production the collection's walk is a `metrics.collection_interval` old by the
            // time a decision lands, so the fallback walks rather than reuse it. Two calls in a
            // test are microseconds apart, so the age is applied here. The base stays well
            // inside `PUBLISHED_WALK_BASE_MAX_AGE`, which is what the lever composes on.
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            let true_walk = walk_in_progress(dir).bytes;
            assert_eq!(
                (
                    reconciliation.walk_bytes,
                    reconciliation.composed_bytes,
                    reconciliation.drift_bytes,
                    composed.fell_back,
                    composed.bytes,
                    true_walk
                ),
                (
                    Some(1_500),
                    Some(0),
                    Some(-1_500),
                    true,
                    Some(2_200),
                    Some(2_200)
                ),
                "(the walk, the figure the previous walk supports, drift, fell back, composed P, \
                 the true walk): a counter that missed an update makes the reconciliation read \
                 below the walk, and the decider then walks rather than compose"
            );
        }

        /// The load-bearing step of the under-count proof, from the review: the `added_ever`
        /// snapshot is taken BEFORE the walk lists anything. A record that lands inside that
        /// window is then counted by both terms, which over-states `P`; taken after the walk, it
        /// would be counted by neither and `P` would read below the truth.
        #[tokio::test]
        async fn the_added_ever_snapshot_is_taken_before_the_walk_lists_anything() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let hook_dir = dir.to_path_buf();
            set_between_snapshot_and_walk_hook_for_tests(
                dir,
                Box::new(move || own_record(&hook_dir, "case79-order", 1, 1_000)),
            );
            observe_in_progress(dir, std::time::Duration::ZERO).await;
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            let true_walk = walk_in_progress(dir).bytes;
            assert_eq!(
                (
                    composed.base_bytes,
                    composed.growth_bytes,
                    composed.bytes,
                    composed.bytes >= true_walk,
                    true_walk
                ),
                (Some(1_000), 1_000, Some(2_000), true, Some(1_000)),
                "(the base walk, growth, composed P, P is at least the truth, the true walk): a \
                 part record written inside the snapshot-to-walk window is counted by both \
                 terms, never by neither"
            );
        }

        /// Three staleness bounds stack on a deciding read of the staged
        /// authority figure, and the total is stated rather than left to be re-derived: the base
        /// walk's age, a peer's publication lag inside the lease its cycle earns, and the
        /// authority memo's own TTL. Pinned so that moving any one of them moves the stated
        /// total.
        #[test]
        fn the_total_staleness_of_a_composed_decision_is_the_sum_of_its_three_bounds() {
            assert_eq!(
                (
                    PUBLISHED_WALK_BASE_MAX_AGE.as_secs(),
                    published_figure_lease_secs(CYCLE_SECS),
                    crate::staged_fold::AUTHORITY_SNAPSHOT_TTL_SECS,
                    total_composed_staleness_bound_secs(CYCLE_SECS)
                ),
                (180, 15, 5, 200),
                "(base walk bound, a peer's lease at the 5s default, the authority memo TTL, the \
                 stated total): the three stack, and 200s is the oldest input a decision can act \
                 on"
            );
        }

        /// The fold's install guard keeps the freshness it had before these
        /// documents existed: it composes on a base no older than `INSTALL_GUARD_BASE_MAX_AGE`,
        /// so a base past that sends it to a walk, where a decider on the hot path still
        /// composes on the same base and walks not at all.
        #[tokio::test]
        async fn the_install_guard_composes_on_a_shorter_base_bound_than_a_decider() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            own_record(dir, "case79-guard", 1, 2_100);
            assert!(publish_own_in_progress_if_due_blocking(dir, CYCLE).unwrap());
            observe_in_progress(dir, std::time::Duration::ZERO).await;
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(30));
            let walks_before = in_progress_walks_for_tests(dir);
            let decider = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            let walks_by_decider = in_progress_walks_for_tests(dir) - walks_before;
            let guard = compose_in_progress_part_bytes(dir, INSTALL_GUARD_BASE_MAX_AGE).await;
            assert_eq!(
                (
                    decider.fell_back,
                    walks_by_decider,
                    guard.fell_back,
                    in_progress_walks_for_tests(dir) - walks_before - walks_by_decider,
                    guard.bytes
                ),
                (false, 0, true, 1, Some(2_100)),
                "(the decider fell back, its walks, the guard fell back, its walks, the guard's \
                 P): a 30s old base is inside the decider's bound and past the install guard's"
            );
        }

        /// Date an instance's published document `by` earlier on the filesystem. The reaper reads
        /// the file's own mtime, so a test that needs a retired document has to move that and not
        /// only `written_at`.
        fn backdate_figure_file(dir: &std::path::Path, instance: &str, by: std::time::Duration) {
            let path = in_progress_figures_dir(dir)
                .join(format!("{}.{}", instance, IN_PROGRESS_FIGURE_EXT));
            let file = std::fs::File::options().write(true).open(&path).unwrap();
            let when = std::time::SystemTime::now() - by;
            file.set_times(std::fs::FileTimes::new().set_modified(when))
                .unwrap();
        }

        /// A retired `{host}:{pid}` document is what every proxy restart leaves
        /// behind, and it is past its lease forever. It must not put the decider on the walk: the
        /// instance has written no journal inside the roster window, so it is left out of the
        /// composition and its part bytes stay counted by the base walk, beside a live peer whose
        /// document is read as usual.
        #[tokio::test]
        async fn a_retired_instances_document_does_not_force_the_decider_to_walk() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let live = "case81-live-peer:55";
            let retired = "case81-retired-peer:1001";
            peer_journal(dir, live);
            // The retired process's part is on the volume, so the base walk counts it; its
            // document is a day old and will never be written again.
            peer_record(dir, "case81-retired-1", 1, 6_000);
            publish_peer_in_progress_figure_for_test(
                dir,
                retired,
                6_000,
                6_000,
                now() - 86_400,
                CYCLE_SECS,
            );
            peer_record(dir, "case81-live-1", 1, 2_000);
            publish_peer_in_progress_figure_for_test(dir, live, 2_000, 2_000, now(), CYCLE_SECS);
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            // Read before this test's own confirming walk, which moves the same counter.
            let walks_by_decision = in_progress_walks_for_tests(dir) - walks_after_base;
            let true_walk = walk_in_progress(dir).bytes;
            assert_eq!(
                (
                    base.walk.bytes,
                    composed.bytes,
                    composed.fell_back,
                    walks_by_decision,
                    composed.bytes >= true_walk,
                    true_walk
                ),
                (Some(8_000), Some(8_000), false, 0, true, Some(8_000)),
                "(P in the base walk, composed P, fell back, walks by the decision, composed P is \
                 at least the true walk, the true walk): a retired instance's document is left \
                 out of the composition, not read as a peer whose parts need a walk"
            );
        }

        /// A publication unlinks a retired instance's document,
        /// so the directory does not grow one document per restart and the roster exclusion has
        /// nothing left to exclude. A document whose instance still has a journal inside the
        /// roster window is left alone, because a live writer that has stopped publishing is the
        /// case the lease exists to catch.
        #[test]
        fn a_publication_reaps_a_retired_document_and_keeps_a_stalled_live_ones() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let retired = "case81-reap-retired:1002";
            let stalled = "case81-reap-stalled:1003";
            peer_journal(dir, stalled);
            for instance in [retired, stalled] {
                publish_peer_in_progress_figure_for_test(
                    dir,
                    instance,
                    1_000,
                    1_000,
                    now() - 86_400,
                    CYCLE_SECS,
                );
                backdate_figure_file(dir, instance, std::time::Duration::from_secs(86_400));
            }
            own_record(dir, "case81-reap-own", 1, 500);
            assert!(publish_own_in_progress_if_due_blocking(dir, CYCLE).unwrap());
            let figures = in_progress_figures_dir(dir);
            let exists = |instance: &str| {
                figures
                    .join(format!("{}.{}", instance, IN_PROGRESS_FIGURE_EXT))
                    .exists()
            };
            assert_eq!(
                (
                    exists(retired),
                    exists(stalled),
                    exists(&own_in_progress_instance_id())
                ),
                (false, true, true),
                "(the retired instance's document, the stalled live instance's, this process's): \
                 a publication removes a document whose instance has written no journal inside \
                 the roster window, and leaves every other"
            );
        }
        /// A peer whose document is past its lease but younger than the
        /// roster window, which has written no journal of its own and is still recording parts,
        /// must not be dropped from the composition: the growth term the exclusion would discard
        /// is real, and `P` would read below the volume by every part that peer recorded after
        /// the base walk. The decider falls back to a walk instead, which is the safe direction
        /// and the reaper's own threshold.
        #[tokio::test]
        async fn a_live_peer_inside_the_roster_window_keeps_the_figure_above_the_walk() {
            let temp = TempDir::new().unwrap();
            let dir = temp.path();
            let stalled = "case82-stalled-live-peer:77";
            // Published 60s ago: past the 15s lease its 5s cycle earns, far inside the hour. It
            // has written no journal, which is the premise the lease-only exclusion rested on,
            // and it is still writing part records.
            peer_record(dir, "case82-stalled-1", 1, 3_000);
            publish_peer_in_progress_figure_for_test(
                dir,
                stalled,
                3_000,
                3_000,
                now() - 60,
                CYCLE_SECS,
            );
            let base = observe_in_progress(dir, std::time::Duration::ZERO).await;
            let walks_after_base = in_progress_walks_for_tests(dir);
            // A part the peer recorded after the base walk, which only its growth term or a
            // fresh walk can count.
            peer_record(dir, "case82-stalled-2", 1, 4_000);
            age_last_in_progress_walk_for_tests(dir, std::time::Duration::from_secs(10));
            let composed = compose_in_progress_part_bytes(dir, PUBLISHED_WALK_BASE_MAX_AGE).await;
            // Read before this test's own confirming walk, which moves the same counter.
            let walks_by_decision = in_progress_walks_for_tests(dir) - walks_after_base;
            let true_walk = walk_in_progress(dir).bytes;
            assert_eq!(
                (
                    base.walk.bytes,
                    composed.bytes,
                    composed.fell_back,
                    walks_by_decision,
                    composed.bytes >= true_walk,
                    true_walk
                ),
                (Some(3_000), Some(7_000), true, 1, true, Some(7_000)),
                "(P in the base walk, composed P, fell back, walks by the decision, composed P \
                 is at least the true walk, the true walk): a peer inside the roster window \
                 whose publication has stalled sends the decider to a walk, so the parts it \
                 recorded after the base walk are still in the figure"
            );
        }
    }
    mod spread_walk {
        use super::*;

        /// `uploads` upload directories of `parts` part records each, sizes all distinct; returns
        /// the exact `P` and entry count.
        fn build(dir: &std::path::Path, uploads: u64, parts: u64) -> (u64, u64) {
            let mut bytes = 0u64;
            for u in 0..uploads {
                let upload = mpu_upload_dir(dir, &format!("case48-walk-{u}"));
                std::fs::create_dir_all(&upload).unwrap();
                std::fs::write(upload.join("upload.meta"), b"{}").unwrap();
                for n in 1..=parts {
                    let size = 1_000 + u * 7_919 + n * 13;
                    std::fs::write(
                        upload.join(format!("part{n}.json")),
                        format!(r#"{{"compressed_size":{size}}}"#),
                    )
                    .unwrap();
                    bytes += size;
                }
            }
            (bytes, uploads * (1 + parts))
        }

        /// The spread walk returns exactly what the serial walk returns, on both shapes: many
        /// uploads of one part, and few uploads of many parts. The second shape is the one where
        /// spreading only across uploads would gain nothing, so it also asserts the part records
        /// were read on more than one thread.
        #[test]
        fn the_spread_walk_equals_the_serial_walk_on_both_shapes() {
            for (uploads, parts) in [(400u64, 1u64), (3, 300)] {
                let temp = TempDir::new().unwrap();
                let dir = temp.path();
                let (bytes, entries) = build(dir, uploads, parts);
                let serial = walk_in_progress_spread(dir, 1);
                let serial_threads = record_read_threads_for_tests(dir);
                let spread = walk_in_progress_spread(dir, IN_PROGRESS_WALK_THREADS);
                let spread_threads = record_read_threads_for_tests(dir);
                assert_eq!(
                    (
                        serial,
                        spread,
                        serial_threads,
                        spread_threads > 1,
                        in_progress_part_bytes(dir)
                    ),
                    (
                        InProgressWalk {
                            bytes: Some(bytes),
                            entries: Some(entries)
                        },
                        InProgressWalk {
                            bytes: Some(bytes),
                            entries: Some(entries)
                        },
                        1,
                        true,
                        Some(bytes)
                    ),
                    "(serial walk, spread walk, threads reading records serially, more than one \
                     thread read records when spread, in_progress_part_bytes) for {uploads} \
                     uploads of {parts} parts"
                );
            }
        }

        /// One record the walk cannot measure makes the spread walk's `P` UNREADABLE, never the
        /// sum of the others, whether the record is unparseable or lacks `compressed_size`. The
        /// entry count, which comes from listings alone, is still known.
        #[test]
        fn one_unmeasurable_record_makes_the_spread_walk_unreadable_not_a_partial_sum() {
            for bad in [&b"not json"[..], &br#"{"etag":"x"}"#[..]] {
                let temp = TempDir::new().unwrap();
                let dir = temp.path();
                let (_, entries) = build(dir, 4, 50);
                std::fs::write(
                    mpu_upload_dir(dir, "case48-walk-2").join("part25.json"),
                    bad,
                )
                .unwrap();
                let spread = walk_in_progress_spread(dir, IN_PROGRESS_WALK_THREADS);
                let serial = walk_in_progress_spread(dir, 1);
                assert_eq!(
                    (spread, serial.bytes),
                    (
                        InProgressWalk {
                            bytes: None,
                            entries: Some(entries)
                        },
                        None
                    ),
                    "(spread walk, serial walk's P) with part25.json of upload 2 written as {:?}: \
                     P must be UNREADABLE, not a partial sum",
                    String::from_utf8_lossy(bad)
                );
            }
        }
    }
}
#[cfg(test)]
mod explicit_unlock_tests {
    use super::*;
    use crate::journal_consolidator::explicit_unlock_tests::{child_holding, peer_can_lock};
    /// dropping the guard `lock_upload_file` returns frees `upload.lock` while a child
    /// holds the descriptor.
    ///
    /// # RED SIDE
    ///
    /// With the `unlock_lock_file` call in `Drop for UnlockOnDrop` removed, the final assertion
    /// fails.
    #[tokio::test]
    async fn a_dropped_upload_lock_frees_the_flock_while_a_child_holds_the_descriptor() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("upload.lock");
        let guard = SignedPutHandler::lock_upload_file(&path).await.unwrap();
        let mut child = child_holding(&guard);
        assert!(!peer_can_lock(&path), "fixture: upload.lock must be held");
        drop(guard);
        let freed = peer_can_lock(&path);
        let _ = child.kill();
        let _ = child.wait();
        assert!(
            freed,
            "dropping the upload.lock guard must unlock despite the child's copy of the descriptor"
        );
    }
}
