//! Download Bandwidth QoS — token bucket + DRR fair scheduler.
//!
//! Limits the aggregate rate at which cache-miss origin downloads consume S3
//! egress, and divides that budget fairly across **fairness classes** resolved
//! per request (caller identity or bucket).
//!
//! ## Architecture (three layers, cheapest first)
//!
//! **Layer 0 — disabled bypass:** `instance_ceiling_bps == 0` ⇒ every
//! `try_consume` returns `true` immediately with only one relaxed atomic load.
//!
//! **Layer 1 — lock-free fast path:** a global token bucket is an `AtomicU64`.
//! `try_consume(n)` is a CAS loop that subtracts `n` only when the bucket holds
//! at least `n` tokens.  No task hop, no lock, no `await`.
//!
//! **Layer 2 — DRR scheduler (under contention only):** a background task owns
//! the refill timer and a Deficit Round-Robin queue entered only when
//! `try_consume` fails.  Blocked streams enqueue a `{key, bytes, oneshot_tx}`;
//! the DRR task wakes them as tokens become available, one class at a time.
//!
//! ## Fairness key
//!
//! `resolve_fairness_key` extracts `app/<value>` from the `user-agent` header
//! (when `caller_id.enabled`), validates it, and returns a `FairnessKey::Caller`
//! or falls back to `FairnessKey::Bucket` / `FairnessKey::Default`.
//! Caller- and bucket-derived keys occupy **separate namespaces** (the enum
//! variant itself) so a caller value equal to a bucket name never collides.
//!
//! ## Quantum leasing
//!
//! [`ThrottleStream`](crate::throttle_stream) draws from a local lease balance
//! rather than calling the limiter per frame.  It refills the balance in
//! increments of up to [`LEASE_QUANTUM`] bytes, keeping limiter-interaction
//! frequency proportional to `throughput / quantum` and independent of frame
//! size or request count.

use crate::config::{CallerIdConfig, DownloadBandwidthConfig};
use crate::logging::sanitize_log_field;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

// ─── Internal tuning constants (not operator-configurable) ────────────────────

/// Maximum bytes acquired per lease interaction.
///
/// For large streams this amortises the per-lease cost to
/// `throughput / LEASE_QUANTUM` interactions per second (independent of frame
/// size).  The clamp-to-available rule in [`ThrottleStream`] makes this a
/// ceiling: a stream is never parked because a full quantum is unavailable.
///
/// Value: 1 MiB ≈ chunk/8 for 8 MiB range GETs — enough amortisation with
/// smooth sub-quantum interleaving under contention.
pub const LEASE_QUANTUM: u64 = 1 << 20; // 1 MiB

/// Token-bucket burst window for smoothing.
///
/// The bucket capacity is `ceiling_bps × BURST_WINDOW`, so the bucket can hold
/// at most 100 ms worth of tokens.  This bounds overshoot at low ceilings and
/// keeps TTFB low (tokens are almost always available for the first frame).
pub const BURST_WINDOW: Duration = Duration::from_millis(100);

// ─── Fairness key ─────────────────────────────────────────────────────────────

/// Per-request fairness class identity.
///
/// The enum variant provides the namespace: a `Caller("x")` and a `Bucket("x")`
/// with the same string value are distinct classes and never collide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FairnessKey {
    /// Caller identity extracted from the User-Agent `app/<value>` token.
    Caller(String),
    /// S3 bucket name (fallback when caller identity is absent or invalid).
    Bucket(String),
    /// No resolvable identity (no bucket and no valid caller).
    Default,
}

impl FairnessKey {
    /// Metric-label rendering: `ua:<value>`, `bkt:<name>`, or `default`.
    pub fn label(&self) -> String {
        match self {
            FairnessKey::Caller(v) => format!("ua:{}", v),
            FairnessKey::Bucket(b) => format!("bkt:{}", b),
            FairnessKey::Default => "default".to_string(),
        }
    }
}

// ─── Key resolver ─────────────────────────────────────────────────────────────

/// Resolve the per-request fairness key from request headers and config.
///
/// Resolution order:
/// 1. If `cfg.caller_id.enabled`, extract `app/<value>` from `user-agent`.
///    Validate with `max_len` and the optional compiled regex.
///    On success → `Caller(sanitize_log_field(value))`.
/// 2. Otherwise (disabled / absent / invalid) → `Bucket(resolved_bucket)` if
///    a bucket was resolved, else `Default`.
pub fn resolve_fairness_key(
    headers: &std::collections::HashMap<String, String>,
    resolved_bucket: Option<&str>,
    cfg: &CallerIdConfig,
    compiled_regex: Option<&regex::Regex>,
) -> FairnessKey {
    if cfg.enabled {
        // Extract `app/<value>` from the user-agent header.
        let ua = headers
            .get("user-agent")
            .or_else(|| headers.get("User-Agent"))
            .map(|s| s.as_str())
            .unwrap_or("");

        if let Some(token) = extract_app_token(ua) {
            // Validate length
            if token.len() <= cfg.max_len {
                // Validate against optional regex
                let regex_ok = compiled_regex.is_none_or(|re| re.is_match(token));
                if regex_ok {
                    return FairnessKey::Caller(sanitize_log_field(token));
                }
            }
            // Present but invalid — fall through to bucket
            debug!(
                "UA app-id present but failed validation (len={}/max={}, regex_ok={}), \
                 falling back to bucket key",
                token.len(),
                cfg.max_len,
                compiled_regex.is_none_or(|re| re.is_match(token)),
            );
        }
    }

    // No valid caller — use bucket or default.
    match resolved_bucket {
        Some(b) if !b.is_empty() => FairnessKey::Bucket(b.to_string()),
        _ => FairnessKey::Default,
    }
}

/// Extract the `<value>` from a User-Agent `app/<value>` token.
///
/// The token is the characters after `app/` up to the first ASCII space (or
/// end-of-string).  Returns `None` when no `app/` token is present.
fn extract_app_token(ua: &str) -> Option<&str> {
    // Find "app/" in the UA string
    let start = ua.find("app/")?;
    let after_app = &ua[start + 4..];
    // Token ends at first space or end-of-string
    let end = after_app.find(' ').unwrap_or(after_app.len());
    let token = &after_app[..end];
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

// ─── DRR scheduler internals ─────────────────────────────────────────────────

/// Request sent from a blocked `ThrottleStream` to the DRR background task.
struct DrrRequest {
    /// Fairness class this request belongs to.
    key: FairnessKey,
    /// Number of bytes this request wants to acquire.
    bytes: u64,
    /// Send the grant signal when tokens are available.
    grant_tx: oneshot::Sender<()>,
}

/// Per-class state in the DRR scheduler.
struct ClassState {
    /// Accumulated deficit for this class (bytes the class is owed).
    deficit: u64,
    /// Pending requests queued for this class, in FIFO order.
    queue: VecDeque<DrrRequest>,
}

impl ClassState {
    fn new() -> Self {
        Self {
            deficit: 0,
            queue: VecDeque::new(),
        }
    }
}

// ─── BandwidthLimiter ────────────────────────────────────────────────────────

/// Shared bandwidth limiter: token bucket + DRR fair scheduler.
///
/// Cloned cheaply (`Arc` internals).  Pass to every [`ThrottleStream`].
#[derive(Clone)]
pub struct BandwidthLimiter {
    /// Layer 0: 0 = disabled. Written by fleet task, read by every poll_next.
    ceiling_bps: Arc<AtomicU64>,
    /// Layer 1: available token count (bytes). Refilled by the background task.
    pub(crate) available_tokens: Arc<AtomicU64>,
    /// Layer 2: channel to send blocked acquire requests to the DRR task.
    drr_tx: mpsc::UnboundedSender<DrrRequest>,
    /// Compiled validation regex, shared across all requests.
    compiled_regex: Option<Arc<regex::Regex>>,
    /// Snapshot of caller_id config (enabled flag + max_len).
    caller_id_cfg: CallerIdConfig,
    /// Counter for fail-open events (limiter faulted / DRR stuck).
    failopen_total: Arc<AtomicU64>,
    /// Per-class cumulative byte tracker for observability (top-K).
    pub(crate) byte_tracker: Arc<TopKTracker>,
}

impl BandwidthLimiter {
    /// Create a new limiter from config and optionally spawn the background DRR task.
    ///
    /// Returns `(limiter, task_handle)`. When `max_bytes_per_sec == 0` (disabled),
    /// returns a disabled limiter and `None` — no background task is spawned, so
    /// this is safe to call outside a Tokio runtime. When enabled, the returned
    /// `JoinHandle` should be detached (dropped) to let the task run for the
    /// process lifetime.
    pub fn new(cfg: &DownloadBandwidthConfig) -> (Self, Option<tokio::task::JoinHandle<()>>) {
        let ceiling_bps = cfg.max_bytes_per_sec;

        // When disabled skip all dynamic allocation and task spawning so this
        // can be called safely outside a Tokio runtime (e.g. integration tests).
        if ceiling_bps == 0 {
            let (drr_tx, _drr_rx) = mpsc::unbounded_channel::<DrrRequest>();
            let limiter = Self {
                ceiling_bps: Arc::new(AtomicU64::new(0)),
                available_tokens: Arc::new(AtomicU64::new(0)),
                drr_tx,
                compiled_regex: cfg
                    .caller_id
                    .validation_regex
                    .as_deref()
                    .and_then(|p| regex::Regex::new(p).ok())
                    .map(Arc::new),
                caller_id_cfg: cfg.caller_id.clone(),
                failopen_total: Arc::new(AtomicU64::new(0)),
                byte_tracker: Arc::new(TopKTracker::new(cfg.max_tracked_classes.max(1))),
            };
            return (limiter, None);
        }

        let ceiling_arc = Arc::new(AtomicU64::new(ceiling_bps));
        let tokens_arc = Arc::new(AtomicU64::new(burst_capacity(ceiling_bps)));
        let (drr_tx, drr_rx) = mpsc::unbounded_channel::<DrrRequest>();
        let failopen = Arc::new(AtomicU64::new(0));

        let compiled_regex = cfg
            .caller_id
            .validation_regex
            .as_deref()
            .and_then(|p| regex::Regex::new(p).ok())
            .map(Arc::new);

        let limiter = Self {
            ceiling_bps: Arc::clone(&ceiling_arc),
            available_tokens: Arc::clone(&tokens_arc),
            drr_tx,
            compiled_regex,
            caller_id_cfg: cfg.caller_id.clone(),
            failopen_total: Arc::clone(&failopen),
            byte_tracker: Arc::new(TopKTracker::new(cfg.max_tracked_classes.max(1))),
        };

        let handle = tokio::spawn(drr_task(ceiling_arc, tokens_arc, drr_rx, failopen));

        (limiter, Some(handle))
    }

    /// Create a disabled limiter (ceiling = 0, no background task).
    pub fn disabled() -> Self {
        let (drr_tx, _drr_rx) = mpsc::unbounded_channel::<DrrRequest>();
        Self {
            ceiling_bps: Arc::new(AtomicU64::new(0)),
            available_tokens: Arc::new(AtomicU64::new(0)),
            drr_tx,
            compiled_regex: None,
            caller_id_cfg: CallerIdConfig::default(),
            failopen_total: Arc::new(AtomicU64::new(0)),
            byte_tracker: Arc::new(TopKTracker::new(1)),
        }
    }

    /// Returns `true` when the feature is disabled (ceiling = 0).
    #[inline]
    pub fn is_disabled(&self) -> bool {
        self.ceiling_bps.load(Ordering::Relaxed) == 0
    }

    /// Update the instance ceiling (called by the fleet cold-path task).
    pub fn set_ceiling_bps(&self, bps: u64) {
        self.ceiling_bps.store(bps, Ordering::Relaxed);
    }

    /// Current instance ceiling in bytes/s.
    pub fn ceiling_bps(&self) -> u64 {
        self.ceiling_bps.load(Ordering::Relaxed)
    }

    /// Cumulative fail-open events.
    pub fn failopen_total(&self) -> u64 {
        self.failopen_total.load(Ordering::Relaxed)
    }

    /// Resolve the per-request fairness key from headers.
    pub fn resolve_key(
        &self,
        headers: &std::collections::HashMap<String, String>,
        resolved_bucket: Option<&str>,
    ) -> FairnessKey {
        resolve_fairness_key(
            headers,
            resolved_bucket,
            &self.caller_id_cfg,
            self.compiled_regex.as_deref(),
        )
    }

    /// Layer-1 fast-path acquire: atomically subtract `n` tokens if available.
    ///
    /// Returns `Some(granted)` on success where `granted` is clamped to
    /// `[min_bytes, min(desired, available)]`, or `None` if fewer than
    /// `min_bytes` tokens are available.
    #[inline]
    pub fn try_acquire(&self, min_bytes: u64, desired: u64) -> Option<u64> {
        if self.ceiling_bps.load(Ordering::Relaxed) == 0 {
            // Layer 0 bypass — disabled.
            return Some(desired);
        }
        let mut current = self.available_tokens.load(Ordering::Relaxed);
        loop {
            if current < min_bytes {
                return None;
            }
            let granted = current.min(desired);
            match self.available_tokens.compare_exchange_weak(
                current,
                current - granted,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(granted),
                Err(actual) => current = actual,
            }
        }
    }

    /// Refund unused tokens back to the bucket (called on stream end/drop).
    pub fn refund(&self, bytes: u64) {
        if bytes == 0 || self.is_disabled() {
            return;
        }
        let ceiling = self.ceiling_bps.load(Ordering::Relaxed);
        let cap = burst_capacity(ceiling);
        // Saturating add, then clamp to burst capacity.
        let mut current = self.available_tokens.load(Ordering::Relaxed);
        loop {
            let new_val = current.saturating_add(bytes).min(cap);
            match self.available_tokens.compare_exchange_weak(
                current,
                new_val,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record bytes consumed by a specific fairness class (for observability).
    ///
    /// Called from [`ThrottleStream`] after each frame is delivered.  Uses the
    /// top-K tracker so cardinality is bounded.
    #[inline]
    pub fn record_bytes(&self, key: &FairnessKey, bytes: u64) {
        if bytes > 0 {
            self.byte_tracker.record(&key.label(), bytes);
        }
    }

    /// Point-in-time snapshot of bandwidth limiter state for observability.
    pub fn snapshot(&self) -> BandwidthLimiterSnapshot {
        let (class_bytes, residual_bytes) = self.byte_tracker.snapshot();
        BandwidthLimiterSnapshot {
            enabled: !self.is_disabled(),
            instance_ceiling_bps: self.ceiling_bps.load(Ordering::Relaxed),
            failopen_total: self.failopen_total.load(Ordering::Relaxed),
            class_bytes: class_bytes.into_iter().collect(),
            residual_bytes,
        }
    }

    /// Layer-2 slow path: enqueue a blocked acquire request and return a future
    /// that resolves when the DRR scheduler grants the budget.
    ///
    /// On channel send failure (DRR task has exited) this returns a receiver
    /// whose sender is immediately dropped, so the caller will see it resolve
    /// with `Err(RecvError)` and can fail open.
    pub fn enqueue_blocked(&self, key: FairnessKey, bytes: u64) -> oneshot::Receiver<()> {
        let (grant_tx, grant_rx) = oneshot::channel();
        let req = DrrRequest {
            key,
            bytes,
            grant_tx,
        };
        if let Err(e) = self.drr_tx.send(req) {
            // DRR task has exited — drop the sender so receiver resolves Err
            warn!("DRR task unavailable, failing open: {}", e);
            self.failopen_total.fetch_add(1, Ordering::Relaxed);
            // grant_tx dropped here, grant_rx will resolve to Err immediately
        }
        grant_rx
    }
}

/// Point-in-time snapshot of bandwidth limiter state for observability.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BandwidthLimiterSnapshot {
    /// `false` when `max_bytes_per_sec = 0` (feature disabled).
    pub enabled: bool,
    /// Current per-instance ceiling in bytes/s.
    pub instance_ceiling_bps: u64,
    /// Cumulative fail-open events (limiter fault / DRR timeout).
    pub failopen_total: u64,
    /// Per-class cumulative bytes (label → bytes).  Bounded by `max_tracked_classes`.
    pub class_bytes: std::collections::HashMap<String, u64>,
    /// Bytes attributed to classes outside the top-K.
    pub residual_bytes: u64,
}

// ─── Heavy-hitter top-K (Space-Saving style, for metric cardinality) ─────────

/// Bounded heavy-hitter tracker using a Space-Saving-style algorithm.
///
/// Tracks the top-K fairness classes by cumulative bytes.  Classes outside the
/// top-K are folded into a single `residual` label.  This bounds per-class
/// metric cardinality while still counting every byte (Req 5.3).
///
/// ## Algorithm
/// - If `label` is already tracked, increment its count.
/// - If the table has room (`len < cap`), add it with `bytes`.
/// - Otherwise, find the class with the minimum count (`min_class`), replace it
///   with `label` and set its count to `min_count + bytes` (Space-Saving
///   over-count — bounds error by `min_count`).
///
/// Classes evicted to residual continue to accumulate via the `residual` counter.
pub struct TopKTracker {
    cap: usize,
    /// Per-class byte counts for retained top-K classes.
    classes: dashmap::DashMap<String, u64>,
    /// Bytes attributed to evicted / overflow classes.
    residual: AtomicU64,
}

impl TopKTracker {
    /// Create a new tracker with capacity `cap`.
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            classes: dashmap::DashMap::new(),
            residual: AtomicU64::new(0),
        }
    }

    /// Record `bytes` for `label`.
    pub fn record(&self, label: &str, bytes: u64) {
        // Fast path: label already tracked.
        if let Some(mut entry) = self.classes.get_mut(label) {
            *entry += bytes;
            return;
        }
        // Slow path: new label.
        if self.classes.len() < self.cap {
            self.classes
                .entry(label.to_string())
                .and_modify(|v| *v += bytes)
                .or_insert(bytes);
        } else {
            // Find minimum-count class (Space-Saving eviction).
            let min_entry = self
                .classes
                .iter()
                .min_by_key(|e| *e.value())
                .map(|e| (e.key().clone(), *e.value()));
            if let Some((min_key, min_count)) = min_entry {
                // Add evicted class bytes to residual.
                self.residual.fetch_add(min_count, Ordering::Relaxed);
                self.classes.remove(&min_key);
                self.classes.insert(label.to_string(), min_count + bytes);
            } else {
                // Table empty despite cap > 0 — shouldn't happen, but safe.
                self.residual.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// Return a snapshot of top-K entries plus the residual count.
    pub fn snapshot(&self) -> (Vec<(String, u64)>, u64) {
        let entries: Vec<(String, u64)> = self
            .classes
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();
        let residual = self.residual.load(Ordering::Relaxed);
        (entries, residual)
    }

    /// Current number of tracked classes (≤ cap).
    pub fn len(&self) -> usize {
        self.classes.len()
    }

    /// Whether there are no tracked classes.
    pub fn is_empty(&self) -> bool {
        self.classes.is_empty()
    }

    /// Residual byte count.
    pub fn residual(&self) -> u64 {
        self.residual.load(Ordering::Relaxed)
    }
}

// ─── Global instance (initialized once at proxy startup) ─────────────────────

/// Process-wide limiter instance, initialized once by `init_global_limiter`.
static GLOBAL_LIMITER: OnceLock<Arc<BandwidthLimiter>> = OnceLock::new();

/// Initialize the global limiter from config and spawn the background DRR task.
///
/// Must be called once before any requests are served.  Subsequent calls on the
/// same process are silently ignored (the existing limiter is reused).  The DRR
/// task is detached — it runs for the lifetime of the process/runtime.
pub fn init_global_limiter(cfg: &DownloadBandwidthConfig) {
    if GLOBAL_LIMITER.get().is_some() {
        return; // Already initialized (e.g. in a test that creates multiple proxies)
    }
    let (limiter, drr_handle) = BandwidthLimiter::new(cfg);
    let arc = Arc::new(limiter);
    // Ignore error if another call raced us here.
    let _ = GLOBAL_LIMITER.set(arc);
    // Drop the handle — the DRR task is already running and detaches cleanly.
    drop(drr_handle);
}

/// Initialize the global limiter and, if shared storage is in use, spawn the
/// fleet cold-path heartbeat + live-count task.
///
/// `cache_dir` is the proxy's `cache.cache_dir` value.  When `None` or when the
/// ceiling is `0` (feature disabled), no fleet I/O task is started.
pub fn init_global_limiter_with_fleet(
    cfg: &DownloadBandwidthConfig,
    cache_dir: Option<std::path::PathBuf>,
) {
    init_global_limiter(cfg);
    if cfg.max_bytes_per_sec > 0 {
        if let Some(dir) = cache_dir {
            let aggregate_bps = cfg.max_bytes_per_sec;
            let fleet_cfg = cfg.fleet.clone();
            tokio::spawn(fleet_bandwidth_task(dir, aggregate_bps, fleet_cfg));
        }
    }
}

/// Return the global `BandwidthLimiter`.
///
/// Returns a **disabled** limiter if `init_global_limiter` has not been called
/// (safe default for tests and library users that do not need QoS).
pub fn global_limiter() -> Arc<BandwidthLimiter> {
    GLOBAL_LIMITER
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(BandwidthLimiter::disabled()))
}

// ─── Helper: burst capacity ───────────────────────────────────────────────────

/// Maximum token-bucket capacity: `ceiling_bps × BURST_WINDOW`.
fn burst_capacity(ceiling_bps: u64) -> u64 {
    let secs = BURST_WINDOW.as_secs_f64();
    (ceiling_bps as f64 * secs) as u64
}

/// Compute the local per-instance ceiling from the aggregate and live count.
///
/// Isolated as a pure function so Phase-B demand-weighted reconciliation can
/// replace just this function without touching enforcement.
pub fn compute_local_ceiling(aggregate_bps: u64, instance_count: u32) -> u64 {
    if aggregate_bps == 0 {
        // Configured-disabled — preserve the "off" semantics (Layer 0 bypass).
        return 0;
    }
    let n = instance_count.max(1) as u64;
    // Floor to ≥1: integer division of a non-zero aggregate by a large instance
    // count can truncate to 0, which Layer 0 interprets as "disabled" and would
    // silently stop throttling (fail-open to unlimited). A non-zero aggregate
    // must always yield a non-zero per-instance ceiling.
    (aggregate_bps / n).max(1)
}

// ─── Fleet cold-path task (cap / N heartbeat + live-count) ───────────────────

/// Periodic fleet-sharing task.
///
/// Every `refresh_interval` (floor: 10 s):
/// 1. Touches `cache_dir/qos/heartbeats/{instance_id}.qos` as a heartbeat.
/// 2. Reads the `qos/heartbeats/` directory and counts `.qos` files with mtime
///    within `instance_staleness` → live instance count `N`.  Clearly-dead
///    heartbeats (mtime past the cleanup grace) are removed in the same pass.
/// 3. Sets `global_limiter().ceiling_bps = aggregate / max(N, 1)` (floored to ≥1).
///
/// Heartbeats live in a dedicated `qos/heartbeats/` subtree — never under
/// `metadata/` — so they are isolated from the cache-metadata sweeps
/// (consolidator, eviction, `is_cache_entry_active`, dead-journal cleanup) that
/// walk `metadata/_journals/`, and so a cache-metadata reset never wipes live
/// liveness state.
///
/// Failure handling: keeps last-known ceiling; logs loudly.  Never sets the
/// ceiling to unlimited (only to `aggregate / fallback_instance_count`).
async fn fleet_bandwidth_task(
    cache_dir: std::path::PathBuf,
    aggregate_bps: u64,
    cfg: crate::config::FleetSharingConfig,
) {
    let instance_id = format!(
        "{}:{}",
        gethostname::gethostname().to_string_lossy(),
        std::process::id(),
    );
    let heartbeat_dir = cache_dir.join("qos").join("heartbeats");
    let heartbeat_file = heartbeat_dir.join(format!("{}.qos", instance_id));

    let refresh = std::cmp::max(cfg.refresh_interval, std::time::Duration::from_secs(10));
    let mut interval = tokio::time::interval(refresh);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut last_n = cfg.fallback_instance_count;

    loop {
        interval.tick().await;

        // Step 1: touch the heartbeat file.
        // Write a non-empty byte so that O_TRUNC on EFS/NFS actually updates mtime
        // (writing 0 bytes is a no-op on many NFS implementations and doesn't change mtime).
        if let Some(parent) = heartbeat_file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = tokio::fs::write(&heartbeat_file, b"\n").await {
            warn!(
                "[BANDWIDTH_FLEET] Failed to write heartbeat file {}: {}",
                heartbeat_file.display(),
                e
            );
        }

        // Step 2: count live instances by reading the qos/heartbeats/ directory.
        let staleness = cfg.instance_staleness;
        let n_result = tokio::task::spawn_blocking({
            let heartbeat_dir = heartbeat_dir.clone();
            move || count_live_instances(&heartbeat_dir, staleness)
        })
        .await;

        let n = match n_result {
            Ok(Ok(count)) => {
                last_n = count;
                count
            }
            Ok(Err(e)) => {
                warn!(
                    "[BANDWIDTH_FLEET] Failed to count live instances (using fallback N={}): {}",
                    last_n, e
                );
                last_n
            }
            Err(e) => {
                warn!(
                    "[BANDWIDTH_FLEET] Live-count task panicked (using fallback N={}): {}",
                    last_n, e
                );
                last_n
            }
        };

        // Fail safe toward more throttling: N=0 treated as fallback.
        let safe_n = std::cmp::max(n, cfg.fallback_instance_count);
        let new_ceiling = compute_local_ceiling(aggregate_bps, safe_n);
        global_limiter().set_ceiling_bps(new_ceiling);

        debug!(
            "[BANDWIDTH_FLEET] live_instances={}, fallback={}, per_instance_ceiling={} bps",
            n, cfg.fallback_instance_count, new_ceiling
        );
    }
}

/// Count `.qos` heartbeat files in `heartbeat_dir` whose mtime is within
/// `staleness` of now (= live instances).
///
/// Only files with a `.qos` extension are considered — defence-in-depth even
/// though the directory is dedicated to heartbeats, so a stray temp/dotfile
/// never inflates the count. Clearly-dead heartbeats — mtime older than the
/// cleanup grace (`max(staleness × 10, 10 min)`) — are removed best-effort in
/// the same pass, so heartbeats from dead PIDs (the instance_id embeds the PID,
/// which changes on every restart) do not accumulate on shared storage.
///
/// Returns at least 1 (this instance always counts itself).
fn count_live_instances(
    heartbeat_dir: &std::path::Path,
    staleness: std::time::Duration,
) -> std::io::Result<u32> {
    let now = std::time::SystemTime::now();
    let cutoff = now
        .checked_sub(staleness)
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    // Files older than this grace are dead litter (e.g. a since-restarted PID),
    // not merely a slow instance — safe to delete. Generous floor avoids
    // deleting a temporarily-stalled peer.
    let cleanup_grace = std::cmp::max(
        staleness.saturating_mul(10),
        std::time::Duration::from_secs(600),
    );
    let cleanup_cutoff = now
        .checked_sub(cleanup_grace)
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

    let mut count = 0u32;
    for entry in std::fs::read_dir(heartbeat_dir)? {
        let entry = entry?;
        let path = entry.path();
        // Only .qos heartbeat files count; ignore anything else.
        if path.extension().and_then(|e| e.to_str()) != Some("qos") {
            continue;
        }
        let meta = entry.metadata()?;
        if !meta.is_file() {
            continue;
        }
        if let Ok(mtime) = meta.modified() {
            if mtime >= cutoff {
                count += 1;
            } else if mtime < cleanup_cutoff {
                // Dead heartbeat — remove best-effort (ignore races/permission errors).
                let _ = std::fs::remove_file(&path);
            }
            // Between cutoff and cleanup_cutoff: not counted, not deleted
            // (could be a temporarily slow but still-live instance).
        }
    }
    Ok(count.max(1)) // Always at least 1 (this instance)
}

// ─── DRR background task ──────────────────────────────────────────────────────

/// The DRR background task.
///
/// Owns:
/// - A refill timer (fires at `BURST_WINDOW` frequency)
/// - The `DrrRequest` receiver channel
/// - Per-class deficit counters and request queues
///
/// Work is conserving: when only one class is active it can consume the full
/// ceiling; idle classes accumulate no deficit.
async fn drr_task(
    ceiling_bps: Arc<AtomicU64>,
    available_tokens: Arc<AtomicU64>,
    mut drr_rx: mpsc::UnboundedReceiver<DrrRequest>,
    failopen_total: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(BURST_WINDOW);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Per-class DRR state.  Only classes with pending requests are present.
    let mut classes: HashMap<FairnessKey, ClassState> = HashMap::new();
    // Round-robin order (keys of currently-active classes).
    let mut rr_order: VecDeque<FairnessKey> = VecDeque::new();

    loop {
        tokio::select! {
            // Refill tick — add tokens and serve pending waiters.
            _ = interval.tick() => {
                let ceiling = ceiling_bps.load(Ordering::Relaxed);
                if ceiling == 0 {
                    // Disabled — drain any queued requests immediately (fail-open).
                    drain_all(&mut classes, &mut rr_order, &failopen_total);
                    continue;
                }
                let cap = burst_capacity(ceiling);
                let to_add = (ceiling as f64 * BURST_WINDOW.as_secs_f64()) as u64;
                // Refill, capped at burst capacity (unless DRR waiters need more).
                // When there are pending DRR requests, allow accumulation beyond the
                // burst cap so that very low ceilings (burst < frame_size) can still
                // serve frames — otherwise the DRR can never accumulate enough tokens.
                let has_pending = classes.values().any(|c| !c.queue.is_empty());
                let mut cur = available_tokens.load(Ordering::Relaxed);
                loop {
                    let new_val = if has_pending {
                        cur.saturating_add(to_add) // accumulate beyond cap until served
                    } else {
                        cur.saturating_add(to_add).min(cap) // normal smoothing cap
                    };
                    match available_tokens.compare_exchange_weak(
                        cur, new_val, Ordering::AcqRel, Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(actual) => cur = actual,
                    }
                }
                // Walk DRR round-robin and wake waiters as tokens allow.
                serve_waiters(&mut classes, &mut rr_order, &available_tokens, ceiling);
            }

            // New blocked request — add to class queue and try to serve immediately.
            Some(req) = drr_rx.recv() => {
                let ceiling = ceiling_bps.load(Ordering::Relaxed);
                if ceiling == 0 {
                    // Disabled — grant immediately.
                    let _ = req.grant_tx.send(());
                    continue;
                }
                let key = req.key.clone();
                let entry = classes.entry(req.key.clone()).or_insert_with(ClassState::new);
                if entry.queue.is_empty() {
                    // First waiter for this class — add to round-robin order.
                    rr_order.push_back(key);
                }
                entry.queue.push_back(req);
                // Attempt an immediate serve (tokens may be available right now).
                serve_waiters(&mut classes, &mut rr_order, &available_tokens, ceiling);
            }

            else => break,
        }
    }
    debug!("DRR task exiting");
}

/// Walk the DRR round-robin once and wake as many waiters as tokens allow.
fn serve_waiters(
    classes: &mut HashMap<FairnessKey, ClassState>,
    rr_order: &mut VecDeque<FairnessKey>,
    available_tokens: &Arc<AtomicU64>,
    ceiling: u64,
) {
    // One pass through active classes in round-robin order.
    let n = rr_order.len();
    let mut i = 0;
    while i < n {
        let key = match rr_order.front() {
            Some(k) => k.clone(),
            None => break,
        };
        rr_order.pop_front();

        let class = match classes.get_mut(&key) {
            Some(c) if !c.queue.is_empty() => c,
            _ => {
                // Class is empty — remove it.
                classes.remove(&key);
                i += 1;
                continue;
            }
        };

        // Add quantum to this class's deficit.
        class.deficit = class.deficit.saturating_add(LEASE_QUANTUM);

        // Serve as many head waiters as deficit and tokens allow.
        while let Some(req) = class.queue.front() {
            let needed = req.bytes;
            if class.deficit < needed {
                break; // Not enough deficit for this waiter yet.
            }
            // Attempt to atomically take `needed` tokens.
            let mut cur = available_tokens.load(Ordering::Relaxed);
            let got = loop {
                if cur < needed {
                    // Insufficient tokens — stop serving this class for now.
                    break 0u64;
                }
                match available_tokens.compare_exchange_weak(
                    cur,
                    cur - needed,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break needed,
                    Err(actual) => cur = actual,
                }
            };

            if got == 0 {
                break; // No tokens available for any class right now.
            }

            // Tokens granted — wake the waiter.
            class.deficit -= needed;
            let req = class.queue.pop_front().unwrap();
            let _ = req.grant_tx.send(());
        }

        if class.queue.is_empty() {
            // Work-conserving: clear deficit for idle class so it starts fresh.
            class.deficit = 0;
            classes.remove(&key);
        } else {
            // Still has waiters — put back in rotation.
            rr_order.push_back(key);
        }

        i += 1;
        // After serving, re-apply the burst cap so idle accumulation is bounded.
        // (Only clamp if no more pending requests to avoid capping what we need.)
        if classes.is_empty() {
            let cap = burst_capacity(ceiling);
            let cur = available_tokens.load(Ordering::Relaxed);
            if cur > cap {
                available_tokens.store(cap, Ordering::Relaxed);
            }
        }
    }
}

/// Fail-open: drain all pending requests, granting them without consuming tokens.
fn drain_all(
    classes: &mut HashMap<FairnessKey, ClassState>,
    rr_order: &mut VecDeque<FairnessKey>,
    failopen_total: &Arc<AtomicU64>,
) {
    let count: usize = classes.values().map(|c| c.queue.len()).sum();
    if count > 0 {
        warn!(
            "BandwidthLimiter disabled with {} pending DRR requests; granting all (fail-open)",
            count
        );
        failopen_total.fetch_add(count as u64, Ordering::Relaxed);
    }
    for (_, class) in classes.drain() {
        for req in class.queue {
            let _ = req.grant_tx.send(());
        }
    }
    rr_order.clear();
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CallerIdConfig, DownloadBandwidthConfig};
    use std::collections::HashMap;

    // ── FairnessKey + resolver ────────────────────────────────────────────────

    fn make_headers(ua: &str) -> HashMap<String, String> {
        let mut h = HashMap::new();
        h.insert("user-agent".to_string(), ua.to_string());
        h
    }

    fn make_cfg(enabled: bool, regex: Option<&str>, max_len: usize) -> CallerIdConfig {
        CallerIdConfig {
            enabled,
            validation_regex: regex.map(|s| s.to_string()),
            max_len,
        }
    }

    #[test]
    fn resolve_present_valid_returns_caller() {
        let cfg = make_cfg(true, None, 64);
        let headers = make_headers("aws-cli/2.0 Python/3.9 app/myapp");
        let key = resolve_fairness_key(&headers, Some("my-bucket"), &cfg, None);
        assert_eq!(key, FairnessKey::Caller("myapp".to_string()));
    }

    #[test]
    fn resolve_caller_ignores_bucket_when_caller_valid() {
        let cfg = make_cfg(true, None, 64);
        let headers = make_headers("app/caller123");
        let key = resolve_fairness_key(&headers, Some("some-bucket"), &cfg, None);
        assert_eq!(key, FairnessKey::Caller("caller123".to_string()));
        // Bucket is NOT in the key — caller wins
        assert_ne!(key, FairnessKey::Bucket("some-bucket".to_string()));
    }

    #[test]
    fn resolve_absent_ua_falls_back_to_bucket() {
        let cfg = make_cfg(true, None, 64);
        let headers = make_headers("aws-cli/2.0 Python/3.9"); // no app/
        let key = resolve_fairness_key(&headers, Some("my-bucket"), &cfg, None);
        assert_eq!(key, FairnessKey::Bucket("my-bucket".to_string()));
    }

    #[test]
    fn resolve_invalid_ua_falls_back_to_bucket() {
        let cfg = make_cfg(true, Some("^[a-z]+$"), 64);
        let re = regex::Regex::new("^[a-z]+$").unwrap();
        let headers = make_headers("app/INVALID123"); // doesn't match regex
        let key = resolve_fairness_key(&headers, Some("my-bucket"), &cfg, Some(&re));
        assert_eq!(key, FairnessKey::Bucket("my-bucket".to_string()));
    }

    #[test]
    fn resolve_oversize_ua_falls_back_to_bucket() {
        let cfg = make_cfg(true, None, 4); // max_len = 4
        let headers = make_headers("app/toolongvalue");
        let key = resolve_fairness_key(&headers, Some("my-bucket"), &cfg, None);
        assert_eq!(key, FairnessKey::Bucket("my-bucket".to_string()));
    }

    #[test]
    fn resolve_caller_disabled_always_uses_bucket() {
        let cfg = make_cfg(false, None, 64);
        let headers = make_headers("app/myapp");
        let key = resolve_fairness_key(&headers, Some("my-bucket"), &cfg, None);
        assert_eq!(key, FairnessKey::Bucket("my-bucket".to_string()));
    }

    #[test]
    fn resolve_no_bucket_no_caller_returns_default() {
        let cfg = make_cfg(true, None, 64);
        let headers = make_headers("no-app-token/here");
        let key = resolve_fairness_key(&headers, None, &cfg, None);
        assert_eq!(key, FairnessKey::Default);
    }

    #[test]
    fn caller_and_bucket_same_value_do_not_collide() {
        // "acme" as a caller and "acme" as a bucket must be distinct classes.
        let caller = FairnessKey::Caller("acme".to_string());
        let bucket = FairnessKey::Bucket("acme".to_string());
        assert_ne!(caller, bucket);
        // They must hash differently when used as map keys.
        let mut map: HashMap<FairnessKey, u32> = HashMap::new();
        map.insert(caller, 1);
        map.insert(bucket, 2);
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn extract_app_token_finds_value_in_middle_of_ua() {
        assert_eq!(
            extract_app_token("aws-cli/2.0 app/myapp Linux/5.4"),
            Some("myapp")
        );
        assert_eq!(extract_app_token("app/only"), Some("only"));
        assert_eq!(extract_app_token("aws-cli/2.0 Python/3.9"), None);
        assert_eq!(extract_app_token(""), None);
    }

    // ── TopKTracker ───────────────────────────────────────────────────────────

    #[test]
    fn top_k_tracks_within_cap() {
        let tracker = TopKTracker::new(3);
        tracker.record("a", 100);
        tracker.record("b", 200);
        tracker.record("c", 300);
        assert_eq!(tracker.len(), 3);
        assert_eq!(tracker.residual(), 0);
    }

    #[test]
    fn top_k_evicts_and_folds_to_residual() {
        let tracker = TopKTracker::new(2);
        tracker.record("a", 1000);
        tracker.record("b", 2000);
        // Third class triggers eviction.
        tracker.record("c", 3000);
        // At most cap + 1 distinct series should exist (including residual).
        assert!(tracker.len() <= 2, "tracker.len()={}", tracker.len());
        // Total bytes must be conserved (top-K bytes + residual).
        let (entries, residual) = tracker.snapshot();
        let total: u64 = entries.iter().map(|(_, b)| b).sum::<u64>() + residual;
        // Space-Saving over-counts evicted bytes, so total >= actual bytes.
        assert!(total >= 6000, "bytes not conserved: total={}", total);
    }

    #[test]
    fn top_k_many_classes_bounded_by_cap() {
        let cap = 10usize;
        let tracker = TopKTracker::new(cap);
        for i in 0..100 {
            tracker.record(&format!("class-{}", i), (i + 1) as u64 * 100);
        }
        assert!(tracker.len() <= cap, "too many classes: {}", tracker.len());
        // All bytes must be accounted for (sum ≤ total with Space-Saving over-count).
        let (entries, residual) = tracker.snapshot();
        let tracked: u64 = entries.iter().map(|(_, b)| b).sum();
        let actual_total: u64 = (1..=100).map(|i| i * 100).sum::<u64>();
        // tracked + residual >= actual_total (Space-Saving may over-count)
        assert!(
            tracked + residual >= actual_total,
            "bytes lost: tracked={}, residual={}, actual={}",
            tracked,
            residual,
            actual_total
        );
    }

    // ── compute_local_ceiling ─────────────────────────────────────────────────

    // ── Fleet / compute_local_ceiling ─────────────────────────────────────────

    #[test]
    fn fleet_count_live_instances_counts_fresh_files() {
        let dir = tempfile::tempdir().unwrap();
        // Create two fresh .qos heartbeat files.
        std::fs::write(dir.path().join("a.qos"), b"\n").unwrap();
        std::fs::write(dir.path().join("b.qos"), b"\n").unwrap();
        let count = count_live_instances(dir.path(), std::time::Duration::from_secs(30)).unwrap();
        assert!(count >= 2, "should count at least 2 fresh files");
    }

    #[test]
    fn fleet_count_excludes_stale_files() {
        let dir = tempfile::tempdir().unwrap();
        // Create a file and backdate its mtime by 60s (past 30s staleness, but
        // within the 10-min cleanup grace so it is kept, just not counted).
        let stale = dir.path().join("stale.qos");
        std::fs::write(&stale, b"\n").unwrap();
        let past = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(60))
            .unwrap();
        filetime::set_file_mtime(&stale, filetime::FileTime::from_system_time(past)).unwrap();

        // Fresh file.
        std::fs::write(dir.path().join("fresh.qos"), b"\n").unwrap();

        let count = count_live_instances(dir.path(), std::time::Duration::from_secs(30)).unwrap();
        // Only the fresh file should be counted.
        assert_eq!(count, 1, "stale file should not be counted");
        assert!(
            stale.exists(),
            "within-grace stale file must not be deleted"
        );
    }

    #[test]
    fn fleet_count_ignores_non_qos_files() {
        let dir = tempfile::tempdir().unwrap();
        // Journal-system artifacts that share neither dir nor extension must not
        // inflate the count even if they somehow land here.
        std::fs::write(dir.path().join("inst.journal"), b"x").unwrap();
        std::fs::write(dir.path().join("inst.journal.lock"), b"x").unwrap();
        std::fs::write(dir.path().join(".DS_Store"), b"x").unwrap();
        // No .qos files at all → count floors to 1 (this instance).
        let count = count_live_instances(dir.path(), std::time::Duration::from_secs(30)).unwrap();
        assert_eq!(count, 1, "non-.qos files must be ignored");
    }

    #[test]
    fn fleet_count_deletes_dead_heartbeats() {
        let dir = tempfile::tempdir().unwrap();
        // Backdate well past the cleanup grace (10 min) → dead litter, removed.
        let dead = dir.path().join("dead.qos");
        std::fs::write(&dead, b"\n").unwrap();
        let long_ago = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(3600))
            .unwrap();
        filetime::set_file_mtime(&dead, filetime::FileTime::from_system_time(long_ago)).unwrap();

        let _ = count_live_instances(dir.path(), std::time::Duration::from_secs(30)).unwrap();
        assert!(
            !dead.exists(),
            "dead heartbeat past cleanup grace must be removed"
        );
    }

    #[test]
    fn fleet_count_nonexistent_dir_returns_io_error() {
        let result = count_live_instances(
            std::path::Path::new("/nonexistent/dir"),
            std::time::Duration::from_secs(30),
        );
        assert!(result.is_err());
    }

    #[test]
    fn compute_ceiling_divides_evenly() {
        assert_eq!(compute_local_ceiling(100_000_000, 4), 25_000_000);
        assert_eq!(compute_local_ceiling(100_000_000, 1), 100_000_000);
    }

    #[test]
    fn compute_ceiling_rounds_down() {
        assert_eq!(compute_local_ceiling(100, 3), 33); // 100/3 = 33 (truncated)
    }

    #[test]
    fn compute_ceiling_zero_instance_count_treated_as_one() {
        assert_eq!(compute_local_ceiling(100_000, 0), 100_000);
    }

    #[test]
    fn compute_ceiling_floors_nonzero_aggregate_to_one() {
        // Non-zero aggregate divided by a large N must never truncate to 0,
        // which Layer 0 would treat as disabled (fail-open to unlimited).
        assert_eq!(compute_local_ceiling(100, 200), 1);
        assert_eq!(compute_local_ceiling(1, 1000), 1);
    }

    #[test]
    fn compute_ceiling_zero_aggregate_stays_disabled() {
        // Configured-off (0) must stay 0 regardless of instance count.
        assert_eq!(compute_local_ceiling(0, 1), 0);
        assert_eq!(compute_local_ceiling(0, 8), 0);
    }

    // ── BandwidthLimiter token bucket ─────────────────────────────────────────

    #[test]
    fn disabled_limiter_try_acquire_always_succeeds() {
        let lim = BandwidthLimiter::disabled();
        assert!(lim.is_disabled());
        assert_eq!(lim.try_acquire(100, 1024), Some(1024));
    }

    #[tokio::test]
    async fn enabled_limiter_try_acquire_under_cap() {
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: 10 * 1024 * 1024, // 10 MiB/s
            ..Default::default()
        };
        let (lim, _handle) = BandwidthLimiter::new(&cfg);
        // The burst capacity is 10 MiB/s * 100ms = 1 MiB initially.
        // A small acquire should succeed immediately.
        let result = lim.try_acquire(1024, 1024);
        assert!(result.is_some(), "small acquire under cap should succeed");
    }

    #[tokio::test]
    async fn enabled_limiter_refund_restores_tokens() {
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: 1024 * 1024, // 1 MiB/s, burst = 100 KiB
            ..Default::default()
        };
        let (lim, _handle) = BandwidthLimiter::new(&cfg);
        let initial = lim.available_tokens.load(Ordering::Relaxed);
        // Take some tokens.
        let took = lim.try_acquire(1024, 1024).unwrap();
        let after_take = lim.available_tokens.load(Ordering::Relaxed);
        assert_eq!(after_take, initial - took);
        // Refund them.
        lim.refund(took);
        let after_refund = lim.available_tokens.load(Ordering::Relaxed);
        assert_eq!(after_refund, initial);
    }

    #[tokio::test]
    async fn drr_grants_single_waiter_on_refill() {
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: 10 * 1024 * 1024, // 10 MiB/s
            ..Default::default()
        };
        let (lim, _handle) = BandwidthLimiter::new(&cfg);

        // Drain the token bucket completely.
        let _cap = burst_capacity(cfg.max_bytes_per_sec);
        lim.available_tokens.store(0, Ordering::Relaxed);

        // Enqueue a small request.
        let key = FairnessKey::Bucket("test".to_string());
        let grant_rx = lim.enqueue_blocked(key, 512);

        // After BURST_WINDOW the DRR task should refill and grant the request.
        let result = tokio::time::timeout(BURST_WINDOW * 5, grant_rx).await;
        assert!(
            result.is_ok(),
            "DRR should grant the request within a few BURST_WINDOWs"
        );
    }

    #[tokio::test]
    async fn two_competing_classes_get_fair_shares() {
        let ceiling = 10 * 1024 * 1024u64; // 10 MiB/s
        let cfg = DownloadBandwidthConfig {
            max_bytes_per_sec: ceiling,
            ..Default::default()
        };
        let (lim, _handle) = BandwidthLimiter::new(&cfg);

        // Drain the bucket.
        lim.available_tokens.store(0, Ordering::Relaxed);

        let key_a = FairnessKey::Bucket("bucket-a".to_string());
        let key_b = FairnessKey::Bucket("bucket-b".to_string());

        // Both classes request exactly one LEASE_QUANTUM.
        let rx_a = lim.enqueue_blocked(key_a, LEASE_QUANTUM);
        let rx_b = lim.enqueue_blocked(key_b, LEASE_QUANTUM);

        // Both should eventually be granted.
        let timeout = BURST_WINDOW * 10;
        let r_a = tokio::time::timeout(timeout, rx_a).await;
        let r_b = tokio::time::timeout(timeout, rx_b).await;
        assert!(r_a.is_ok(), "class A should be granted within timeout");
        assert!(r_b.is_ok(), "class B should be granted within timeout");
    }
}
