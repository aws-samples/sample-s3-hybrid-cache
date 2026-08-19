//! Connection Pool Module
//!
//! Manages IP distribution, DNS resolution, and health tracking for S3 endpoints.
//! Actual TCP connection pooling is handled by hyper's built-in pool.

use crate::{ProxyError, Result};
use dashmap::DashMap;
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::TokioResolver;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// EndpointOverrides — shared exact + suffix hostname-to-IP resolution
// ---------------------------------------------------------------------------

/// Parsed endpoint overrides supporting both exact hostname matches and
/// suffix (wildcard) patterns.
///
/// Config keys starting with `*.` are treated as suffix patterns — the `*`
/// is stripped and the remaining suffix (including the leading dot) is
/// matched against the end of the hostname.  All other keys are exact
/// matches.  At resolve time exact wins first, then the longest matching
/// suffix.
#[derive(Debug, Clone)]
pub struct EndpointOverrides {
    exact: HashMap<String, Vec<IpAddr>>,
    /// Sorted longest-suffix-first for most-specific-wins semantics.
    suffixes: Vec<(String, Vec<IpAddr>)>,
}

impl EndpointOverrides {
    /// Parse from the raw config map.
    pub fn from_config(raw: &HashMap<String, Vec<String>>) -> Self {
        let mut exact = HashMap::new();
        let mut suffixes: Vec<(String, Vec<IpAddr>)> = Vec::new();

        for (hostname, ip_strings) in raw {
            let mut ips = Vec::new();
            for ip_str in ip_strings {
                match ip_str.parse::<IpAddr>() {
                    Ok(ip) => ips.push(ip),
                    Err(e) => {
                        warn!(
                            "Invalid IP address '{}' in endpoint_overrides for '{}': {}",
                            ip_str, hostname, e
                        );
                    }
                }
            }
            if ips.is_empty() {
                continue;
            }
            if let Some(suffix) = hostname.strip_prefix('*') {
                info!("Endpoint suffix override: *{} -> {:?}", suffix, ips);
                suffixes.push((suffix.to_string(), ips));
            } else {
                info!("Endpoint override: {} -> {:?}", hostname, ips);
                exact.insert(hostname.clone(), ips);
            }
        }
        suffixes.sort_by_key(|b| std::cmp::Reverse(b.0.len()));

        Self { exact, suffixes }
    }

    /// Resolve a hostname against overrides.  Returns `None` if no match.
    pub fn resolve(&self, endpoint: &str) -> Option<&Vec<IpAddr>> {
        if let Some(ips) = self.exact.get(endpoint) {
            return Some(ips);
        }
        for (suffix, ips) in &self.suffixes {
            if endpoint.ends_with(suffix.as_str()) {
                return Some(ips);
            }
        }
        None
    }

    /// Whether any overrides (exact or suffix) are configured.
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.suffixes.is_empty()
    }

    /// Number of exact entries (for log messages).
    pub fn exact_count(&self) -> usize {
        self.exact.len()
    }

    /// Number of suffix entries (for log messages).
    pub fn suffix_count(&self) -> usize {
        self.suffixes.len()
    }

    /// Iterate over all exact overrides (for distributor seeding / health reporting).
    pub fn exact_iter(&self) -> impl Iterator<Item = (&String, &Vec<IpAddr>)> {
        self.exact.iter()
    }
}

// ---------------------------------------------------------------------------
// IpDistributor — lock-free round-robin IP selection
// ---------------------------------------------------------------------------

/// Distributes requests across S3 IP addresses using round-robin selection.
///
/// Maintains a set of IP addresses and an atomic counter for lock-free
/// round-robin distribution. Used by `ConnectionPoolManager` to select
/// target IPs for per-IP connection pool separation.
#[derive(Debug)]
pub struct IpDistributor {
    ips: Vec<IpAddr>,
    counter: AtomicUsize,
}

impl IpDistributor {
    /// Create a new IpDistributor with the given set of IP addresses.
    pub fn new(ips: Vec<IpAddr>) -> Self {
        Self {
            ips,
            counter: AtomicUsize::new(0),
        }
    }

    /// Select the next IP address using round-robin distribution.
    ///
    /// Returns `None` if the IP set is empty. Uses `fetch_add` with
    /// `Relaxed` ordering for lock-free atomic increment.
    pub fn select_ip(&self) -> Option<IpAddr> {
        if self.ips.is_empty() {
            return None;
        }
        let index = self.counter.fetch_add(1, Ordering::Relaxed) % self.ips.len();
        Some(self.ips[index])
    }

    /// Replace the IP set with new IPs from a DNS refresh.
    ///
    /// Logs additions and removals at info level. Resets the round-robin
    /// counter to avoid modulo bias when the set size changes.
    pub fn update_ips(&mut self, new_ips: Vec<IpAddr>, reason: &str) {
        let added: Vec<&IpAddr> = new_ips.iter().filter(|ip| !self.ips.contains(ip)).collect();
        let removed: Vec<&IpAddr> = self.ips.iter().filter(|ip| !new_ips.contains(ip)).collect();

        for ip in &added {
            info!(ip = %ip, reason = %reason, "IP added to distributor");
        }
        for ip in &removed {
            info!(ip = %ip, reason = %reason, "IP removed from distributor");
        }

        self.ips = new_ips;
        self.counter.store(0, Ordering::Relaxed);
    }

    /// Remove a specific IP address from the selection set.
    ///
    /// Only removes the IP from the distributor's selection Vec so new
    /// requests are no longer routed to it. Existing connections to the removed
    /// IP remain in hyper's internal pool and complete naturally.
    pub fn remove_ip(&mut self, ip: IpAddr, reason: &str) {
        if let Some(pos) = self.ips.iter().position(|&x| x == ip) {
            self.ips.remove(pos);
            info!(ip = %ip, reason = %reason, "IP removed from distributor");
            self.counter.store(0, Ordering::Relaxed);
        }
    }

    /// Re-admit a single IP into the selection set.
    ///
    /// Returns `true` if the IP was added, `false` if it was already present
    /// (making the call idempotent, so a repeated recovery probe cannot produce
    /// a duplicate entry that would skew round-robin weighting toward that IP).
    ///
    /// Used by the recovery-probe path to restore an IP that was excluded by
    /// [`remove_ip`](Self::remove_ip) once a probe confirms it is reachable
    /// again, without waiting for the next DNS refresh.
    pub fn add_ip(&mut self, ip: IpAddr, reason: &str) -> bool {
        if self.ips.contains(&ip) {
            return false;
        }
        self.ips.push(ip);
        info!(ip = %ip, reason = %reason, "IP added to distributor");
        self.counter.store(0, Ordering::Relaxed);
        true
    }

    /// Return the number of IPs currently in the selection set.
    pub fn ip_count(&self) -> usize {
        self.ips.len()
    }

    /// Return a snapshot of the current IP set for health check reporting.
    pub fn get_ips(&self) -> Vec<IpAddr> {
        self.ips.clone()
    }

    /// Select the next IP address using round-robin distribution, skipping excluded IPs.
    ///
    /// Returns `None` if the IP set is empty or every IP is in the exclusion list.
    /// Advances the internal counter by 1 regardless of how many IPs are skipped,
    /// so subsequent calls continue round-robin from a fresh position.
    pub fn select_ip_excluding(&self, exclude: &[IpAddr]) -> Option<IpAddr> {
        if self.ips.is_empty() {
            return None;
        }
        let len = self.ips.len();
        let start = self.counter.fetch_add(1, Ordering::Relaxed) % len;
        // Scan up to `len` positions to find a non-excluded IP
        for offset in 0..len {
            let candidate = self.ips[(start + offset) % len];
            if !exclude.contains(&candidate) {
                return Some(candidate);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// IpHealthTracker — lock-free per-IP failure tracking with recovery
// ---------------------------------------------------------------------------

/// Per-IP health state tracking unhealthy status and cooldown for recovery probing.
#[derive(Debug, Clone)]
pub struct IpHealthEntry {
    /// Timestamp when the IP was marked unhealthy (threshold reached).
    pub unhealthy_at: Instant,
    /// Current cooldown duration before the IP becomes a probe candidate.
    pub cooldown: Duration,
}

/// Tracks consecutive failures per IP for automatic exclusion from round-robin.
///
/// Uses `DashMap` for lock-free concurrent access. When an IP's consecutive
/// failure count reaches the threshold, the caller should remove it from the
/// `IpDistributor`. Successes reset the counter. DNS refresh restores excluded IPs.
///
/// Recovery: once an IP has been marked unhealthy and its cooldown elapses,
/// it becomes a "probe candidate". A successful probe clears the unhealthy state;
/// a failed probe doubles the cooldown (capped at `max_cooldown`).
pub struct IpHealthTracker {
    failures: DashMap<IpAddr, u32>,
    /// Tracks unhealthy IPs with their cooldown state for recovery probing.
    unhealthy: DashMap<IpAddr, IpHealthEntry>,
    threshold: u32,
    /// Initial cooldown before an unhealthy IP becomes a probe candidate.
    initial_cooldown: Duration,
    /// Maximum cooldown duration (caps exponential backoff).
    max_cooldown: Duration,
}

impl IpHealthTracker {
    pub fn new(threshold: u32) -> Self {
        Self {
            failures: DashMap::new(),
            unhealthy: DashMap::new(),
            threshold,
            initial_cooldown: Duration::from_secs(5),
            max_cooldown: Duration::from_secs(300),
        }
    }

    /// Create a new tracker with configurable cooldown parameters.
    pub fn new_with_cooldown(
        threshold: u32,
        initial_cooldown: Duration,
        max_cooldown: Duration,
    ) -> Self {
        Self {
            failures: DashMap::new(),
            unhealthy: DashMap::new(),
            threshold,
            initial_cooldown,
            max_cooldown,
        }
    }

    /// Record a successful request. Resets failure count and clears unhealthy state for the IP.
    pub fn record_success(&self, ip: &IpAddr) {
        self.failures.remove(ip);
        self.unhealthy.remove(ip);
    }

    /// Record a failed request. Returns `true` if the threshold is reached
    /// and the IP should be excluded from the distributor.
    ///
    /// When the threshold is reached, the IP is also recorded as unhealthy
    /// with the initial cooldown for recovery probing.
    pub fn record_failure(&self, ip: &IpAddr) -> bool {
        self.record_failure_at(ip, Instant::now())
    }

    /// [`record_failure`](Self::record_failure) with an explicit clock reading.
    ///
    /// The public wrapper passes `Instant::now()`. Tests pass a synthetic `now`
    /// so cooldown behaviour is asserted deterministically instead of by sleeping
    /// (a `thread::sleep`-based assertion with a millisecond budget flakes on a
    /// loaded CI runner, where a single scheduler preemption between two adjacent
    /// statements can exceed the budget).
    pub(crate) fn record_failure_at(&self, ip: &IpAddr, now: Instant) -> bool {
        let mut count = self.failures.entry(*ip).or_insert(0);
        *count += 1;
        let threshold_reached = *count >= self.threshold;
        if threshold_reached {
            // Mark as unhealthy with initial cooldown if not already tracked
            self.unhealthy.entry(*ip).or_insert(IpHealthEntry {
                unhealthy_at: now,
                cooldown: self.initial_cooldown,
            });
        }
        threshold_reached
    }

    /// Record a failed probe attempt on an unhealthy IP.
    /// Doubles the cooldown (capped at max_cooldown) and resets the unhealthy_at timestamp.
    pub fn record_probe_failure(&self, ip: &IpAddr) {
        self.record_probe_failure_at(ip, Instant::now())
    }

    /// [`record_probe_failure`](Self::record_probe_failure) with an explicit clock reading.
    pub(crate) fn record_probe_failure_at(&self, ip: &IpAddr, now: Instant) {
        if let Some(mut entry) = self.unhealthy.get_mut(ip) {
            let new_cooldown = entry.cooldown.saturating_mul(2);
            entry.cooldown = if new_cooldown > self.max_cooldown {
                self.max_cooldown
            } else {
                new_cooldown
            };
            entry.unhealthy_at = now;
        }
    }

    /// Record a successful probe on a formerly unhealthy IP.
    /// Clears the unhealthy state and failure count.
    pub fn record_probe_success(&self, ip: &IpAddr) {
        self.failures.remove(ip);
        self.unhealthy.remove(ip);
    }

    /// Returns IPs whose cooldown has elapsed and are eligible for probing.
    /// These IPs are still considered unhealthy until a probe succeeds.
    pub fn get_probe_candidates(&self) -> Vec<IpAddr> {
        self.get_probe_candidates_at(Instant::now())
    }

    /// [`get_probe_candidates`](Self::get_probe_candidates) with an explicit clock reading.
    pub(crate) fn get_probe_candidates_at(&self, now: Instant) -> Vec<IpAddr> {
        self.unhealthy
            .iter()
            .filter_map(|entry| {
                let elapsed = now.duration_since(entry.value().unhealthy_at);
                if elapsed >= entry.value().cooldown {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if a specific IP is a probe candidate (cooldown elapsed).
    pub fn is_probe_candidate(&self, ip: &IpAddr) -> bool {
        self.is_probe_candidate_at(ip, Instant::now())
    }

    /// [`is_probe_candidate`](Self::is_probe_candidate) with an explicit clock reading.
    pub(crate) fn is_probe_candidate_at(&self, ip: &IpAddr, now: Instant) -> bool {
        if let Some(entry) = self.unhealthy.get(ip) {
            let elapsed = now.duration_since(entry.unhealthy_at);
            elapsed >= entry.cooldown
        } else {
            false
        }
    }

    /// Check if an IP is currently marked as unhealthy.
    pub fn is_unhealthy(&self, ip: &IpAddr) -> bool {
        self.unhealthy.contains_key(ip)
    }

    /// Drop all tracked state for a single IP.
    ///
    /// Used when an IP has left DNS entirely: there is nothing left to re-admit,
    /// so retaining its failure count and cooldown would leak map entries and
    /// could mis-attribute a future reappearance of the same address.
    pub fn forget(&self, ip: &IpAddr) {
        self.failures.remove(ip);
        self.unhealthy.remove(ip);
    }

    /// Snapshot of every IP currently marked unhealthy.
    ///
    /// Unlike [`get_probe_candidates`](Self::get_probe_candidates) this ignores
    /// cooldown state, so callers can reconcile the tracked set against DNS.
    pub fn tracked_unhealthy_ips(&self) -> Vec<IpAddr> {
        self.unhealthy.iter().map(|e| *e.key()).collect()
    }

    // NOTE: a blanket `clear()` used to exist and was called on every DNS refresh,
    // which wiped every failure count and cooldown each `pool_check_interval`
    // (default 10s) — so the backoff in `record_probe_failure` could never
    // accumulate and an unreachable IP was re-admitted every 10s. Recovery is now
    // driven by probing, and per-IP state is dropped through `forget`, so nothing
    // needs to reset the whole tracker. Reintroduce a blanket reset only with a
    // caller that genuinely needs one.
}

// ---------------------------------------------------------------------------
// ConnectionPoolManager — DNS resolution + IP distribution
// ---------------------------------------------------------------------------

/// Manages DNS resolution and IP distribution for S3 endpoints.
///
/// Actual TCP connection pooling is handled by hyper's built-in pool.
/// This struct provides:
/// - DNS resolution with configurable servers (bypasses /etc/hosts)
/// - Static endpoint overrides for PrivateLink deployments
/// - Per-endpoint IpDistributor for round-robin IP selection
/// - Hostname lookup for TLS SNI when URI authority is rewritten to IP
pub struct ConnectionPoolManager {
    resolver: TokioResolver,
    default_dns_refresh_interval: Duration,
    dns_refresh_count: u64,
    /// Parsed endpoint overrides (exact + suffix) for PrivateLink etc.
    overrides: EndpointOverrides,
    /// Per-endpoint IP distributors for round-robin request distribution
    pub ip_distributors: HashMap<String, IpDistributor>,
    /// Resolved IPs per endpoint (for DNS refresh tracking)
    resolved_ips: HashMap<String, Vec<IpAddr>>,
    /// Last DNS refresh time per endpoint
    last_dns_refresh: HashMap<String, std::time::SystemTime>,
    /// Maximum number of registered endpoints (prevents unbounded growth)
    max_registered_endpoints: usize,
}

impl ConnectionPoolManager {
    /// Create a new connection pool manager with external DNS servers
    pub fn new() -> Result<Self> {
        use hickory_resolver::config::{ResolveHosts, CLOUDFLARE, GOOGLE};
        let mut config = ResolverConfig::default();
        for ns in GOOGLE.udp_and_tcp() {
            config.add_name_server(ns);
        }
        for ns in CLOUDFLARE.udp_and_tcp() {
            config.add_name_server(ns);
        }

        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = ResolveHosts::Never;

        let resolver = TokioResolver::builder_with_config(config, TokioRuntimeProvider::default())
            .with_options(opts)
            .build()
            .map_err(|e| {
                ProxyError::ConnectionError(format!("Failed to build DNS resolver: {}", e))
            })?;
        info!("DNS resolver initialized with external servers (bypassing /etc/hosts)");

        Ok(Self {
            resolver,
            default_dns_refresh_interval: Duration::from_secs(60),
            dns_refresh_count: 0,
            overrides: EndpointOverrides::from_config(&HashMap::new()),
            ip_distributors: HashMap::new(),
            resolved_ips: HashMap::new(),
            last_dns_refresh: HashMap::new(),
            max_registered_endpoints: 10_000,
        })
    }

    /// Create a new connection pool manager with configuration
    pub fn new_with_config(config: crate::config::ConnectionPoolConfig) -> Result<Self> {
        use hickory_resolver::config::{NameServerConfig, ResolveHosts, CLOUDFLARE, GOOGLE};
        let mut resolver_config = ResolverConfig::default();

        if config.dns_servers.is_empty() {
            for ns in GOOGLE.udp_and_tcp() {
                resolver_config.add_name_server(ns);
            }
            for ns in CLOUDFLARE.udp_and_tcp() {
                resolver_config.add_name_server(ns);
            }
            info!("DNS resolver initialized with default servers: Google DNS + Cloudflare DNS (bypassing /etc/hosts)");
        } else {
            for dns_server in &config.dns_servers {
                match dns_server.parse::<std::net::IpAddr>() {
                    Ok(ip) => {
                        let ns_config = NameServerConfig::udp_and_tcp(ip);
                        resolver_config.add_name_server(ns_config);
                    }
                    Err(e) => {
                        warn!("Invalid DNS server address '{}': {}", dns_server, e);
                    }
                }
            }
            info!(
                "DNS resolver initialized with custom servers: {:?} (bypassing /etc/hosts)",
                config.dns_servers
            );
        }

        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = ResolveHosts::Never;

        let resolver =
            TokioResolver::builder_with_config(resolver_config, TokioRuntimeProvider::default())
                .with_options(opts)
                .build()
                .map_err(|e| {
                    ProxyError::ConnectionError(format!("Failed to build DNS resolver: {}", e))
                })?;

        // Parse endpoint overrides (exact + suffix patterns)
        let overrides = EndpointOverrides::from_config(&config.endpoint_overrides);

        // Eagerly initialize distributors for exact endpoint overrides
        let mut ip_distributors = HashMap::new();
        for (hostname, ips) in overrides.exact_iter() {
            info!(
                endpoint = %hostname,
                ip_count = ips.len(),
                "Initializing IP distributor from endpoint overrides"
            );
            ip_distributors.insert(hostname.clone(), IpDistributor::new(ips.clone()));
        }
        // Note: suffix overrides create distributors lazily on first match
        // because the actual hostname isn't known until request time.

        Ok(Self {
            resolver,
            default_dns_refresh_interval: config.dns_refresh_interval,
            dns_refresh_count: 0,
            overrides,
            ip_distributors,
            resolved_ips: HashMap::new(),
            last_dns_refresh: HashMap::new(),
            max_registered_endpoints: config.max_registered_endpoints,
        })
    }

    /// Look up an endpoint override — exact match first, then longest-suffix match.
    /// Returns the IPs if found, None otherwise.
    pub fn resolve_override(&self, endpoint: &str) -> Option<&Vec<IpAddr>> {
        self.overrides.resolve(endpoint)
    }

    /// Whether any endpoint overrides (exact or suffix) are configured.
    pub fn has_overrides(&self) -> bool {
        !self.overrides.is_empty()
    }

    /// Resolve endpoint to IP addresses using the configured DNS resolver
    /// (bypasses /etc/hosts). Checks endpoint overrides first, then falls back to DNS.
    pub async fn resolve_endpoint(&self, endpoint: &str) -> Result<Vec<IpAddr>> {
        // Check overrides (exact then suffix)
        if let Some(ips) = self.overrides.resolve(endpoint) {
            info!("Using endpoint override for {}: {:?}", endpoint, ips);
            return Ok(ips.clone());
        }

        debug!("Resolving DNS for endpoint: {}", endpoint);

        let response = self.resolver.lookup_ip(endpoint).await.map_err(|e| {
            ProxyError::ConnectionError(format!("DNS resolution failed for {}: {}", endpoint, e))
        })?;

        let ip_addresses: Vec<IpAddr> = response.iter().collect();

        if ip_addresses.is_empty() {
            return Err(ProxyError::ConnectionError(format!(
                "No IP addresses found for endpoint: {}",
                endpoint
            )));
        }

        info!(
            "Resolved {} to {} IP addresses: {:?}",
            endpoint,
            ip_addresses.len(),
            ip_addresses
        );
        Ok(ip_addresses)
    }

    /// Register an endpoint for DNS-based IP distribution.
    ///
    /// Performs an immediate DNS resolution and seeds the IP distributor.
    /// Subsequent calls to `refresh_dns` will keep the distributor up to date.
    /// No-op if the endpoint is already registered or covered by `endpoint_overrides`.
    pub async fn register_endpoint(&mut self, endpoint: &str) {
        // Skip if already registered or covered by a static override (exact or suffix)
        if self.resolved_ips.contains_key(endpoint) || self.resolve_override(endpoint).is_some() {
            return;
        }
        // Cap: prevent unbounded growth of registered endpoints
        if self.resolved_ips.len() >= self.max_registered_endpoints {
            warn!(
                endpoint = %endpoint,
                cap = self.max_registered_endpoints,
                "Endpoint registration cap reached; ignoring new endpoint"
            );
            return;
        }
        info!(endpoint = %endpoint, "Registering endpoint for DNS-based IP distribution");
        // `None`: this is a first-time registration (the early return above skips
        // endpoints already known), so no IP of this endpoint can be under a
        // health exclusion yet and there is nothing to hold back.
        if let Err(e) = self.refresh_endpoint_dns(endpoint, None).await {
            warn!(
                "Initial DNS resolution failed for endpoint {}: {}",
                endpoint, e
            );
        }
    }

    /// Refresh DNS for all registered endpoints whose refresh interval has elapsed.
    ///
    /// When `health_tracker` is `Some`, IPs the tracker still considers unhealthy
    /// are held out of the rebuilt distributor so a refresh cannot silently undo a
    /// health exclusion. Pass `None` to rebuild from the raw DNS result (the
    /// pre-recovery behaviour, retained for tests that exercise DNS alone).
    pub async fn refresh_dns(&mut self, health_tracker: Option<&IpHealthTracker>) -> Result<()> {
        let now = std::time::SystemTime::now();
        let endpoints_to_refresh: Vec<String> = self
            .resolved_ips
            .keys()
            .filter(|endpoint| {
                self.last_dns_refresh
                    .get(*endpoint)
                    .map(|last| {
                        now.duration_since(*last).unwrap_or(Duration::ZERO)
                            >= self.default_dns_refresh_interval
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        for endpoint in endpoints_to_refresh {
            if let Err(e) = self.refresh_endpoint_dns(&endpoint, health_tracker).await {
                warn!("Failed to refresh DNS for endpoint {}: {}", endpoint, e);
            }
        }

        Ok(())
    }

    /// Refresh DNS for a specific endpoint and update the IP distributor.
    ///
    /// `resolved_ips` always records the complete DNS result, but the distributor
    /// is rebuilt from the result minus any IP `health_tracker` still considers
    /// unhealthy. Recovery from an exclusion is the recovery probe's job (a probe
    /// success clears the unhealthy state, after which the IP is re-admitted
    /// immediately and by every later refresh); a DNS refresh must not short-circuit
    /// the cooldown, or the exponential backoff can never accumulate.
    ///
    /// Tracker entries for IPs that have vanished from DNS entirely are dropped,
    /// so the unhealthy map cannot grow without bound as S3 rotates addresses.
    pub async fn refresh_endpoint_dns(
        &mut self,
        endpoint: &str,
        health_tracker: Option<&IpHealthTracker>,
    ) -> Result<()> {
        debug!("Refreshing DNS for endpoint: {}", endpoint);

        let new_ip_addresses = self.resolve_endpoint(endpoint).await?;

        self.resolved_ips
            .insert(endpoint.to_string(), new_ip_addresses.clone());
        self.last_dns_refresh
            .insert(endpoint.to_string(), std::time::SystemTime::now());

        self.dns_refresh_count += 1;
        debug!("DNS refresh count: {}", self.dns_refresh_count);

        // Forget tracked IPs that no endpoint resolves to any more, then hold back
        // the ones still under a health exclusion. Pruning is computed against the
        // union of every endpoint's resolved set, not just this endpoint's, so a
        // refresh of one endpoint cannot discard another endpoint's exclusions.
        let distributor_ips = match health_tracker {
            Some(tracker) => {
                let all_resolved: std::collections::HashSet<IpAddr> =
                    self.resolved_ips.values().flatten().copied().collect();
                for ip in tracker.tracked_unhealthy_ips() {
                    if !all_resolved.contains(&ip) {
                        debug!(ip = %ip, "Forgetting health state for IP no longer present in DNS");
                        tracker.forget(&ip);
                    }
                }
                let (healthy, excluded): (Vec<IpAddr>, Vec<IpAddr>) = new_ip_addresses
                    .iter()
                    .partition(|ip| !tracker.is_unhealthy(ip));
                if !excluded.is_empty() {
                    info!(
                        endpoint = %endpoint,
                        excluded = ?excluded,
                        "Holding unhealthy IPs out of distributor on DNS refresh (awaiting recovery probe)"
                    );
                }
                healthy
            }
            None => new_ip_addresses.clone(),
        };

        // Rebuild the distributor with the healthy subset of resolved IPs
        if let Some(distributor) = self.ip_distributors.get_mut(endpoint) {
            info!(
                endpoint = %endpoint,
                ip_count = distributor_ips.len(),
                "Updating IP distributor on DNS refresh"
            );
            distributor.update_ips(distributor_ips, "DNS refresh");
        } else {
            info!(
                endpoint = %endpoint,
                ip_count = distributor_ips.len(),
                "Creating IP distributor on DNS refresh"
            );
            self.ip_distributors
                .insert(endpoint.to_string(), IpDistributor::new(distributor_ips));
        }

        Ok(())
    }

    /// Get a distributed IP address for the given endpoint using round-robin selection.
    ///
    /// Returns `None` if no distributor exists or the IP set is empty, triggering
    /// fallback to hostname-based resolution.
    pub fn get_distributed_ip(&self, endpoint: &str) -> Option<IpAddr> {
        self.ip_distributors
            .get(endpoint)
            .and_then(|d| d.select_ip())
    }

    /// Get up to `n` distinct healthy IPs for the given endpoint.
    ///
    /// Uses `select_ip` for the first pick, then `select_ip_excluding` for each
    /// subsequent pick to ensure distinctness. Returns an empty `Vec` when the
    /// endpoint has no distributor (DNS not yet resolved).
    ///
    /// A result shorter than `n` is **not** an error — the caller handles:
    /// - 2 IPs → pin original and hedge to distinct IPs
    /// - 1 IP → pin both arms to the same IP (separate connections)
    /// - 0 IPs → run both arms unpinned
    pub fn get_distinct_distributed_ips(&self, host: &str, n: usize) -> Vec<IpAddr> {
        let distributor = match self.ip_distributors.get(host) {
            Some(d) => d,
            None => return Vec::new(),
        };

        let mut result = Vec::with_capacity(n);

        // First pick: normal round-robin
        if let Some(first) = distributor.select_ip() {
            result.push(first);
        } else {
            return result;
        }

        // Subsequent picks: exclude already-selected IPs
        for _ in 1..n {
            if let Some(ip) = distributor.select_ip_excluding(&result) {
                result.push(ip);
            } else {
                break;
            }
        }

        result
    }

    /// Look up the endpoint hostname that owns a given IP address.
    ///
    /// Searches all IP distributors to find which endpoint the IP belongs to.
    /// Used by `CustomHttpsConnector` to determine the original hostname for TLS SNI
    /// when the URI authority has been rewritten to an IP address.
    pub fn get_hostname_for_ip(&self, ip: &IpAddr) -> Option<String> {
        for (endpoint, distributor) in &self.ip_distributors {
            if distributor.get_ips().contains(ip) {
                return Some(endpoint.clone());
            }
        }
        // Also check exact endpoint_overrides for IPs not yet in a distributor
        for (endpoint, ips) in self.overrides.exact_iter() {
            if ips.contains(ip) {
                return Some(endpoint.clone());
            }
        }
        None
    }

    /// Look up the endpoint that owns an IP, including IPs currently excluded
    /// from their distributor for health reasons.
    ///
    /// [`get_hostname_for_ip`](Self::get_hostname_for_ip) searches only the live
    /// distributor sets, so it returns `None` for an IP that `remove_ip` has
    /// already excluded — precisely the IP a recovery probe needs to resolve.
    /// This lookup additionally consults `resolved_ips`, which retains the full
    /// DNS result per endpoint and is not touched by `remove_ip`.
    ///
    /// Returns `None` when the IP has disappeared from DNS entirely, which the
    /// probe path treats as "stop tracking this IP" rather than "probe it".
    pub fn get_endpoint_for_ip(&self, ip: &IpAddr) -> Option<String> {
        if let Some(endpoint) = self.get_hostname_for_ip(ip) {
            return Some(endpoint);
        }
        for (endpoint, ips) in &self.resolved_ips {
            if ips.contains(ip) {
                return Some(endpoint.clone());
            }
        }
        None
    }

    /// Get mutable reference to a distributor for IP exclusion.
    pub fn get_distributor_mut(&mut self, endpoint: &str) -> Option<&mut IpDistributor> {
        self.ip_distributors.get_mut(endpoint)
    }

    /// Get per-IP connection distribution statistics for all endpoints with active distributors.
    pub fn get_ip_distribution_stats(&self) -> IpDistributionStats {
        let mut endpoints = Vec::new();

        for (endpoint, distributor) in &self.ip_distributors {
            let distributor_ips = distributor.get_ips();
            let ip_stats: Vec<IpConnectionStats> = distributor_ips
                .iter()
                .map(|ip| IpConnectionStats {
                    ip: ip.to_string(),
                    active_connections: 0,
                    idle_connections: 0,
                })
                .collect();

            endpoints.push(EndpointIpDistributionStats {
                endpoint: endpoint.clone(),
                total_distributor_ips: distributor_ips.len(),
                ips: ip_stats,
            });
        }

        IpDistributionStats { endpoints }
    }

    /// Get DNS refresh count
    pub fn get_dns_refresh_count(&self) -> u64 {
        self.dns_refresh_count
    }
}

// ---------------------------------------------------------------------------
// Observability types
// ---------------------------------------------------------------------------

/// Per-IP connection count statistics for observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpConnectionStats {
    pub ip: String,
    pub active_connections: usize,
    pub idle_connections: usize,
}

/// IP distribution statistics for a single endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointIpDistributionStats {
    pub endpoint: String,
    pub total_distributor_ips: usize,
    pub ips: Vec<IpConnectionStats>,
}

/// Aggregated IP distribution statistics across all endpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpDistributionStats {
    pub endpoints: Vec<EndpointIpDistributionStats>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_ips(count: u8) -> Vec<IpAddr> {
        (1..=count)
            .map(|i| IpAddr::V4(Ipv4Addr::new(10, 0, 0, i)))
            .collect()
    }

    // --- IpDistributor tests ---

    #[test]
    fn test_select_ip_returns_none_when_empty() {
        let distributor = IpDistributor::new(vec![]);
        assert!(distributor.select_ip().is_none());
    }

    #[test]
    fn test_round_robin_cycles_through_all_ips_in_order() {
        let ips = test_ips(3);
        let distributor = IpDistributor::new(ips.clone());

        assert_eq!(distributor.select_ip(), Some(ips[0]));
        assert_eq!(distributor.select_ip(), Some(ips[1]));
        assert_eq!(distributor.select_ip(), Some(ips[2]));
        assert_eq!(distributor.select_ip(), Some(ips[0]));
    }

    #[test]
    fn test_update_ips_replaces_set_and_resets_counter() {
        let mut distributor = IpDistributor::new(test_ips(3));
        distributor.select_ip();
        distributor.select_ip();

        let new_ips = vec![
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)),
        ];
        distributor.update_ips(new_ips.clone(), "DNS refresh");

        assert_eq!(distributor.select_ip(), Some(new_ips[0]));
        assert_eq!(distributor.select_ip(), Some(new_ips[1]));
        assert_eq!(distributor.ip_count(), 2);
    }

    #[test]
    fn test_remove_ip_excludes_from_selection() {
        let ips = test_ips(3);
        let mut distributor = IpDistributor::new(ips.clone());

        distributor.remove_ip(ips[1], "health exclusion");
        assert_eq!(distributor.ip_count(), 2);

        let selected: Vec<IpAddr> = (0..4).filter_map(|_| distributor.select_ip()).collect();
        assert_eq!(selected, vec![ips[0], ips[2], ips[0], ips[2]]);
    }

    #[test]
    fn test_remove_ip_nonexistent_is_noop() {
        let ips = test_ips(2);
        let mut distributor = IpDistributor::new(ips.clone());
        let nonexistent = IpAddr::V4(Ipv4Addr::new(99, 99, 99, 99));
        distributor.remove_ip(nonexistent, "health exclusion");
        assert_eq!(distributor.ip_count(), 2);
    }

    #[test]
    fn test_remove_all_ips_then_select_returns_none() {
        let ips = test_ips(2);
        let mut distributor = IpDistributor::new(ips.clone());
        distributor.remove_ip(ips[0], "health exclusion");
        distributor.remove_ip(ips[1], "health exclusion");
        assert!(distributor.select_ip().is_none());
    }

    #[test]
    fn test_single_ip_always_selected() {
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let distributor = IpDistributor::new(vec![ip]);
        for _ in 0..5 {
            assert_eq!(distributor.select_ip(), Some(ip));
        }
    }

    // --- IpHealthTracker tests ---

    #[test]
    fn test_health_tracker_success_resets_count() {
        let tracker = IpHealthTracker::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_success(&ip);
        // After success, count is reset — next failure starts from 1
        assert!(!tracker.record_failure(&ip));
        assert!(!tracker.record_failure(&ip));
        assert!(tracker.record_failure(&ip)); // 3rd failure hits threshold
    }

    #[test]
    fn test_health_tracker_threshold_triggers() {
        let tracker = IpHealthTracker::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        assert!(!tracker.record_failure(&ip)); // 1
        assert!(!tracker.record_failure(&ip)); // 2
        assert!(tracker.record_failure(&ip)); // 3 — threshold reached
    }

    #[test]
    fn test_health_tracker_independent_per_ip() {
        let tracker = IpHealthTracker::new(3);
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

        // Failures are tracked independently per IP
        tracker.record_failure(&ip1);
        tracker.record_failure(&ip2);
        // ip1 has 1 failure, ip2 has 1 failure — neither at threshold (3)
        assert!(!tracker.record_failure(&ip1)); // ip1 now at 2
        assert!(!tracker.record_failure(&ip2)); // ip2 now at 2
        assert!(tracker.record_failure(&ip1)); // ip1 now at 3 — threshold
        assert!(tracker.record_failure(&ip2)); // ip2 now at 3 — threshold
    }

    #[test]
    fn test_health_tracker_marks_unhealthy_at_threshold() {
        let tracker = IpHealthTracker::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        assert!(!tracker.is_unhealthy(&ip));
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        assert!(!tracker.is_unhealthy(&ip));
        tracker.record_failure(&ip); // threshold reached
        assert!(tracker.is_unhealthy(&ip));
    }

    #[test]
    fn test_health_tracker_probe_candidate_after_cooldown() {
        // Synthetic clock: production-realistic 5s cooldown, advanced explicitly
        // rather than slept through, so the assertion cannot flake under load.
        let tracker =
            IpHealthTracker::new_with_cooldown(3, Duration::from_secs(5), Duration::from_secs(300));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let t0 = Instant::now();

        // Hit threshold
        tracker.record_failure_at(&ip, t0);
        tracker.record_failure_at(&ip, t0);
        tracker.record_failure_at(&ip, t0);
        assert!(tracker.is_unhealthy(&ip));

        // Cooldown has elapsed
        let after_cooldown = t0 + Duration::from_secs(5);
        assert!(tracker.is_probe_candidate_at(&ip, after_cooldown));
        let candidates = tracker.get_probe_candidates_at(after_cooldown);
        assert!(candidates.contains(&ip));
    }

    #[test]
    fn test_health_tracker_not_probe_candidate_before_cooldown() {
        // Use a long cooldown so it doesn't elapse
        let tracker = IpHealthTracker::new_with_cooldown(
            3,
            Duration::from_secs(300),
            Duration::from_secs(300),
        );
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        assert!(tracker.is_unhealthy(&ip));
        assert!(!tracker.is_probe_candidate(&ip));
        assert!(tracker.get_probe_candidates().is_empty());
    }

    #[test]
    fn test_health_tracker_probe_success_clears_unhealthy() {
        let tracker = IpHealthTracker::new_with_cooldown(
            3,
            Duration::from_millis(1),
            Duration::from_secs(300),
        );
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        // Hit threshold
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        assert!(tracker.is_unhealthy(&ip));

        // Probe success clears unhealthy state
        tracker.record_probe_success(&ip);
        assert!(!tracker.is_unhealthy(&ip));
        assert!(!tracker.is_probe_candidate(&ip));
    }

    #[test]
    fn test_health_tracker_probe_failure_doubles_cooldown() {
        // Synthetic clock throughout. The previous sleep-based version budgeted
        // 10ms/20ms windows and asserted `!is_probe_candidate` on the statement
        // immediately after `record_probe_failure`, so a >=20ms scheduler stall
        // between two adjacent statements failed it on the shared CI runner.
        let tracker =
            IpHealthTracker::new_with_cooldown(3, Duration::from_secs(5), Duration::from_secs(300));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let t0 = Instant::now();

        // Hit threshold — initial cooldown is 5s
        tracker.record_failure_at(&ip, t0);
        tracker.record_failure_at(&ip, t0);
        tracker.record_failure_at(&ip, t0);

        // Initial cooldown elapsed — eligible for probing
        let t_probe = t0 + Duration::from_secs(5);
        assert!(tracker.is_probe_candidate_at(&ip, t_probe));

        // Probe fails — cooldown doubles to 10s and unhealthy_at re-anchors to t_probe
        tracker.record_probe_failure_at(&ip, t_probe);
        assert_eq!(
            tracker.unhealthy.get(&ip).unwrap().cooldown,
            Duration::from_secs(10),
            "probe failure should double the 5s cooldown"
        );

        // Immediately after the failed probe: not a candidate, cooldown re-anchored
        assert!(!tracker.is_probe_candidate_at(&ip, t_probe));

        // Partway through the doubled cooldown: still not a candidate
        assert!(!tracker.is_probe_candidate_at(&ip, t_probe + Duration::from_secs(9)));

        // Full doubled cooldown elapsed: candidate again
        assert!(tracker.is_probe_candidate_at(&ip, t_probe + Duration::from_secs(10)));
    }

    // -----------------------------------------------------------------------
    // Recovery probe: re-admission, endpoint resolution, and DNS-refresh
    // interaction. Before these, the probe API had no production callers and a
    // DNS refresh (every pool_check_interval, default 10s) both re-admitted
    // excluded IPs and cleared all cooldown state, so backoff never accumulated.
    // -----------------------------------------------------------------------

    #[test]
    fn test_add_ip_readmits_and_is_idempotent() {
        let ip_a = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip_b = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let mut dist = IpDistributor::new(vec![ip_a, ip_b]);

        dist.remove_ip(ip_b, "test exclusion");
        assert_eq!(dist.ip_count(), 1);

        assert!(dist.add_ip(ip_b, "recovery probe succeeded"));
        assert_eq!(dist.ip_count(), 2);
        assert!(dist.get_ips().contains(&ip_b));

        // Idempotent: a repeat probe must not duplicate the entry, which would
        // skew round-robin weighting toward the re-admitted IP.
        assert!(!dist.add_ip(ip_b, "recovery probe succeeded"));
        assert_eq!(dist.ip_count(), 2);
    }

    #[test]
    fn test_get_endpoint_for_ip_resolves_health_removed_dns_ip() {
        // The capability the probe path depends on. `get_hostname_for_ip` searches
        // the live distributor sets plus the *exact endpoint_overrides* map, so for
        // a DNS-derived IP it goes blind the moment `remove_ip` excludes it —
        // exactly the IP a probe needs to resolve. Seed the DNS-derived state
        // directly (no network) rather than via endpoint_overrides, because an
        // override-seeded IP stays findable through `exact_iter` and would make
        // this assertion pass for the wrong reason.
        let endpoint = "s3.us-west-2.amazonaws.com";
        let kept = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 100));
        let excluded = IpAddr::V4(Ipv4Addr::new(10, 0, 2, 100));

        let mut manager =
            ConnectionPoolManager::new_with_config(crate::config::ConnectionPoolConfig::default())
                .unwrap();
        manager
            .resolved_ips
            .insert(endpoint.to_string(), vec![kept, excluded]);
        manager.ip_distributors.insert(
            endpoint.to_string(),
            IpDistributor::new(vec![kept, excluded]),
        );

        // While in rotation, both lookups agree
        assert_eq!(
            manager.get_hostname_for_ip(&excluded).as_deref(),
            Some(endpoint)
        );
        assert_eq!(
            manager.get_endpoint_for_ip(&excluded).as_deref(),
            Some(endpoint)
        );

        manager
            .get_distributor_mut(endpoint)
            .unwrap()
            .remove_ip(excluded, "test exclusion");

        // Once excluded, they diverge — which is the whole reason the new lookup exists
        assert_eq!(
            manager.get_hostname_for_ip(&excluded),
            None,
            "live-distributor lookup cannot see a health-excluded DNS IP"
        );
        assert_eq!(
            manager.get_endpoint_for_ip(&excluded).as_deref(),
            Some(endpoint),
            "probe lookup must still resolve an excluded IP via resolved_ips"
        );
    }

    #[test]
    fn test_get_endpoint_for_ip_returns_none_for_unknown_ip() {
        // Drives the probe path's "IP left DNS entirely" branch, which drops the
        // tracked health state instead of probing an address nothing resolves to.
        let manager =
            ConnectionPoolManager::new_with_config(crate::config::ConnectionPoolConfig::default())
                .unwrap();
        assert_eq!(
            manager.get_endpoint_for_ip(&IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9))),
            None
        );
    }

    #[test]
    fn test_dns_refresh_holds_back_unhealthy_ip() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            "s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.1.100".to_string(), "10.0.2.100".to_string()],
        );
        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let tracker = IpHealthTracker::new(3);
        let unhealthy = IpAddr::V4(Ipv4Addr::new(10, 0, 2, 100));
        let healthy = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 100));

        tracker.record_failure(&unhealthy);
        tracker.record_failure(&unhealthy);
        assert!(tracker.record_failure(&unhealthy));

        futures::executor::block_on(
            manager.refresh_endpoint_dns("s3.us-west-2.amazonaws.com", Some(&tracker)),
        )
        .unwrap();

        let ips = manager
            .get_distributor_mut("s3.us-west-2.amazonaws.com")
            .unwrap()
            .get_ips();
        assert!(ips.contains(&healthy), "healthy IP stays in rotation");
        assert!(
            !ips.contains(&unhealthy),
            "a DNS refresh must not undo a health exclusion — that is what let the \
             cooldown reset every 10s and prevented backoff from accumulating"
        );
        // The exclusion state itself survives the refresh
        assert!(tracker.is_unhealthy(&unhealthy));
        // And the IP is still resolvable for probing
        assert_eq!(
            manager.get_endpoint_for_ip(&unhealthy).as_deref(),
            Some("s3.us-west-2.amazonaws.com")
        );
    }

    #[test]
    fn test_dns_refresh_readmits_after_probe_success_clears_state() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            "s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.1.100".to_string(), "10.0.2.100".to_string()],
        );
        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let tracker = IpHealthTracker::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 2, 100));

        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        futures::executor::block_on(
            manager.refresh_endpoint_dns("s3.us-west-2.amazonaws.com", Some(&tracker)),
        )
        .unwrap();
        assert!(!manager
            .get_distributor_mut("s3.us-west-2.amazonaws.com")
            .unwrap()
            .get_ips()
            .contains(&ip));

        // A successful probe clears the exclusion; the next refresh is then free
        // to restore the IP (the probe path also re-admits it immediately).
        tracker.record_probe_success(&ip);
        futures::executor::block_on(
            manager.refresh_endpoint_dns("s3.us-west-2.amazonaws.com", Some(&tracker)),
        )
        .unwrap();
        assert!(manager
            .get_distributor_mut("s3.us-west-2.amazonaws.com")
            .unwrap()
            .get_ips()
            .contains(&ip));
    }

    #[test]
    fn test_dns_refresh_forgets_ip_absent_from_dns() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            "s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.1.100".to_string()],
        );
        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let tracker = IpHealthTracker::new(3);
        // An IP that no endpoint resolves to — e.g. S3 rotated it away while it
        // was excluded. Left tracked, it would leak a map entry forever.
        let departed = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));

        tracker.record_failure(&departed);
        tracker.record_failure(&departed);
        tracker.record_failure(&departed);
        assert!(tracker.is_unhealthy(&departed));

        futures::executor::block_on(
            manager.refresh_endpoint_dns("s3.us-west-2.amazonaws.com", Some(&tracker)),
        )
        .unwrap();

        assert!(
            !tracker.is_unhealthy(&departed),
            "health state for an IP no longer in DNS should be dropped"
        );
        assert!(tracker.tracked_unhealthy_ips().is_empty());
    }

    #[test]
    fn test_dns_refresh_of_one_endpoint_preserves_another_endpoints_exclusion() {
        // Pruning is computed against the union of all resolved sets, so
        // refreshing endpoint A must not discard endpoint B's exclusions.
        let mut overrides = std::collections::HashMap::new();
        overrides.insert("a.example.com".to_string(), vec!["10.0.1.1".to_string()]);
        overrides.insert("b.example.com".to_string(), vec!["10.0.2.1".to_string()]);
        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let tracker = IpHealthTracker::new(3);
        let b_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 2, 1));

        // Register both endpoints so resolved_ips knows about b.example.com
        futures::executor::block_on(manager.refresh_endpoint_dns("a.example.com", None)).unwrap();
        futures::executor::block_on(manager.refresh_endpoint_dns("b.example.com", None)).unwrap();

        tracker.record_failure(&b_ip);
        tracker.record_failure(&b_ip);
        tracker.record_failure(&b_ip);

        futures::executor::block_on(manager.refresh_endpoint_dns("a.example.com", Some(&tracker)))
            .unwrap();

        assert!(
            tracker.is_unhealthy(&b_ip),
            "refreshing a.example.com must not prune b.example.com's exclusion"
        );
    }

    #[test]
    fn test_health_tracker_forget_drops_all_state() {
        let tracker = IpHealthTracker::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        assert!(tracker.is_unhealthy(&ip));
        assert_eq!(tracker.tracked_unhealthy_ips(), vec![ip]);

        tracker.forget(&ip);
        assert!(!tracker.is_unhealthy(&ip));
        assert!(tracker.tracked_unhealthy_ips().is_empty());
        // Failure count dropped too, so the IP starts from zero if it returns
        assert!(!tracker.record_failure(&ip));
    }

    #[test]
    fn test_tracked_unhealthy_ips_ignores_cooldown_unlike_probe_candidates() {
        let tracker = IpHealthTracker::new_with_cooldown(
            3,
            Duration::from_secs(300),
            Duration::from_secs(300),
        );
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);

        assert_eq!(tracker.tracked_unhealthy_ips(), vec![ip]);
        assert!(
            tracker.get_probe_candidates().is_empty(),
            "cooldown has not elapsed, so it is tracked but not yet probeable"
        );
    }

    #[test]
    fn test_health_tracker_probe_failure_caps_at_max_cooldown() {
        let tracker = IpHealthTracker::new_with_cooldown(
            3,
            Duration::from_secs(200),
            Duration::from_secs(300), // max 5 min
        );
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        // Hit threshold
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);

        // Probe failure: 200s * 2 = 400s, but capped at 300s
        tracker.record_probe_failure(&ip);

        // Verify cooldown is capped
        let entry = tracker.unhealthy.get(&ip).unwrap();
        assert_eq!(entry.cooldown, Duration::from_secs(300));
    }

    #[test]
    fn test_health_tracker_success_clears_unhealthy_state() {
        let tracker = IpHealthTracker::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        // Hit threshold
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        assert!(tracker.is_unhealthy(&ip));

        // Regular success also clears unhealthy state
        tracker.record_success(&ip);
        assert!(!tracker.is_unhealthy(&ip));
    }

    // --- ConnectionPoolManager tests ---

    #[test]
    fn test_endpoint_overrides_used_for_distribution() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            "s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.1.100".to_string(), "10.0.2.100".to_string()],
        );

        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };

        let manager = ConnectionPoolManager::new_with_config(config).unwrap();

        let expected_ips = [
            IpAddr::V4(Ipv4Addr::new(10, 0, 1, 100)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 2, 100)),
        ];

        // Eagerly initialized — no lazy init needed
        let ip1 = manager.get_distributed_ip("s3.us-west-2.amazonaws.com");
        let ip2 = manager.get_distributed_ip("s3.us-west-2.amazonaws.com");
        let ip3 = manager.get_distributed_ip("s3.us-west-2.amazonaws.com");

        assert_eq!(ip1, Some(expected_ips[0]));
        assert_eq!(ip2, Some(expected_ips[1]));
        assert_eq!(ip3, Some(expected_ips[0]));
    }

    #[test]
    fn test_get_distributed_ip_returns_none_no_distributor() {
        let config = crate::config::ConnectionPoolConfig::default();
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();
        assert_eq!(
            manager.get_distributed_ip("s3.eu-west-1.amazonaws.com"),
            None
        );
    }

    #[test]
    fn test_startup_before_dns_resolution_falls_back_to_hostname() {
        let config = crate::config::ConnectionPoolConfig {
            ip_distribution_enabled: true,
            ..crate::config::ConnectionPoolConfig::default()
        };
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();

        assert_eq!(
            manager.get_distributed_ip("s3.eu-west-1.amazonaws.com"),
            None
        );
        assert_eq!(
            manager.get_distributed_ip("s3.us-east-1.amazonaws.com"),
            None
        );
    }

    #[test]
    fn test_get_hostname_for_ip_returns_endpoint_from_distributor() {
        let config = crate::config::ConnectionPoolConfig::default();
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 224)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 225)),
        ];
        let endpoint = "s3.eu-west-1.amazonaws.com";
        manager
            .ip_distributors
            .insert(endpoint.to_string(), IpDistributor::new(ips.clone()));

        assert_eq!(
            manager.get_hostname_for_ip(&ips[0]),
            Some(endpoint.to_string())
        );
        assert_eq!(
            manager.get_hostname_for_ip(&ips[1]),
            Some(endpoint.to_string())
        );
    }

    #[test]
    fn test_get_hostname_for_ip_returns_endpoint_from_overrides() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            "s3.us-east-1.amazonaws.com".to_string(),
            vec!["10.0.1.50".to_string(), "10.0.1.51".to_string()],
        );

        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };

        let manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 50));
        assert_eq!(
            manager.get_hostname_for_ip(&ip),
            Some("s3.us-east-1.amazonaws.com".to_string())
        );
    }

    #[test]
    fn test_get_hostname_for_ip_returns_none_when_not_found() {
        let config = crate::config::ConnectionPoolConfig::default();
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let unknown_ip = IpAddr::V4(Ipv4Addr::new(99, 99, 99, 99));
        assert_eq!(manager.get_hostname_for_ip(&unknown_ip), None);
    }

    #[test]
    fn test_get_ip_distribution_stats_returns_correct_per_ip_counts() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            "s3.eu-west-1.amazonaws.com".to_string(),
            vec![
                "10.0.0.1".to_string(),
                "10.0.0.2".to_string(),
                "10.0.0.3".to_string(),
            ],
        );

        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };

        let manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let stats = manager.get_ip_distribution_stats();

        assert_eq!(stats.endpoints.len(), 1);
        let ep = &stats.endpoints[0];
        assert_eq!(ep.endpoint, "s3.eu-west-1.amazonaws.com");
        assert_eq!(ep.total_distributor_ips, 3);
        assert_eq!(ep.ips.len(), 3);
    }

    #[test]
    fn test_get_ip_distribution_stats_empty_when_no_distributors() {
        let config = crate::config::ConnectionPoolConfig::default();
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();
        let stats = manager.get_ip_distribution_stats();
        assert!(stats.endpoints.is_empty());
    }

    #[test]
    fn test_get_distributed_ip_returns_none_when_distributor_has_zero_ips() {
        let config = crate::config::ConnectionPoolConfig {
            ip_distribution_enabled: true,
            ..crate::config::ConnectionPoolConfig::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 1)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 2)),
        ];
        let endpoint = "s3.eu-west-1.amazonaws.com";
        manager
            .ip_distributors
            .insert(endpoint.to_string(), IpDistributor::new(ips.clone()));

        assert!(manager.get_distributed_ip(endpoint).is_some());

        let distributor = manager.ip_distributors.get_mut(endpoint).unwrap();
        distributor.remove_ip(ips[0], "health exclusion");
        distributor.remove_ip(ips[1], "health exclusion");

        assert_eq!(manager.get_distributed_ip(endpoint), None);
    }

    // --- EndpointOverrides tests ---

    #[test]
    fn test_endpoint_overrides_exact_match() {
        let mut raw = HashMap::new();
        raw.insert(
            "s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.1.100".to_string()],
        );
        let eo = EndpointOverrides::from_config(&raw);
        assert!(eo.resolve("s3.us-west-2.amazonaws.com").is_some());
        assert!(eo.resolve("other.s3.us-west-2.amazonaws.com").is_none());
    }

    #[test]
    fn test_endpoint_overrides_suffix_match() {
        let mut raw = HashMap::new();
        raw.insert(
            "*.s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.1.100".to_string()],
        );
        let eo = EndpointOverrides::from_config(&raw);
        // Suffix should match any subdomain
        assert!(eo.resolve("mybucket.s3.us-west-2.amazonaws.com").is_some());
        assert!(eo.resolve("other.s3.us-west-2.amazonaws.com").is_some());
        // Bare apex should also match (ends with ".s3.us-west-2.amazonaws.com")
        // But "s3.us-west-2.amazonaws.com" itself does NOT end with ".s3.us-west-2..."
        assert!(eo.resolve("s3.us-west-2.amazonaws.com").is_none());
    }

    #[test]
    fn test_endpoint_overrides_exact_wins_over_suffix() {
        let mut raw = HashMap::new();
        raw.insert(
            "*.s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.1.100".to_string()],
        );
        raw.insert(
            "special.s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.2.200".to_string()],
        );
        let eo = EndpointOverrides::from_config(&raw);
        // Exact match should return the exact IP
        let exact_ips = eo.resolve("special.s3.us-west-2.amazonaws.com").unwrap();
        assert_eq!(exact_ips[0], IpAddr::V4(Ipv4Addr::new(10, 0, 2, 200)));
        // Other subdomains hit the suffix
        let suffix_ips = eo.resolve("other.s3.us-west-2.amazonaws.com").unwrap();
        assert_eq!(suffix_ips[0], IpAddr::V4(Ipv4Addr::new(10, 0, 1, 100)));
    }

    #[test]
    fn test_endpoint_overrides_longest_suffix_wins() {
        let mut raw = HashMap::new();
        raw.insert("*.amazonaws.com".to_string(), vec!["10.0.0.1".to_string()]);
        raw.insert(
            "*.accesspoint.s3-global.amazonaws.com".to_string(),
            vec!["10.0.0.2".to_string()],
        );
        let eo = EndpointOverrides::from_config(&raw);
        // MRAP hostname should match the more specific suffix
        let ips = eo
            .resolve("myalias.mrap.accesspoint.s3-global.amazonaws.com")
            .unwrap();
        assert_eq!(ips[0], IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        // Regular S3 hostname matches the shorter suffix
        let ips2 = eo.resolve("mybucket.s3.us-west-2.amazonaws.com").unwrap();
        assert_eq!(ips2[0], IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    }

    #[test]
    fn test_endpoint_overrides_is_empty() {
        let eo = EndpointOverrides::from_config(&HashMap::new());
        assert!(eo.is_empty());

        let mut raw = HashMap::new();
        raw.insert(
            "*.s3.us-west-2.amazonaws.com".to_string(),
            vec!["10.0.0.1".to_string()],
        );
        let eo2 = EndpointOverrides::from_config(&raw);
        assert!(!eo2.is_empty());
    }

    #[tokio::test]
    async fn test_register_endpoint_stops_at_cap() {
        let config = crate::config::ConnectionPoolConfig {
            max_registered_endpoints: 2,
            ..crate::config::ConnectionPoolConfig::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        // Manually insert endpoints to simulate successful registration without DNS
        manager
            .resolved_ips
            .insert("endpoint-a.example.com".to_string(), vec![]);
        manager
            .resolved_ips
            .insert("endpoint-b.example.com".to_string(), vec![]);

        // Cap is 2 — a third endpoint should be rejected
        manager.register_endpoint("endpoint-c.example.com").await;
        assert!(
            !manager.resolved_ips.contains_key("endpoint-c.example.com"),
            "third endpoint should be rejected when cap is reached"
        );

        // Existing endpoints are unaffected
        assert!(manager.resolved_ips.contains_key("endpoint-a.example.com"));
        assert!(manager.resolved_ips.contains_key("endpoint-b.example.com"));
    }

    #[tokio::test]
    async fn test_register_endpoint_dedup_before_cap() {
        let config = crate::config::ConnectionPoolConfig {
            max_registered_endpoints: 2,
            ..crate::config::ConnectionPoolConfig::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        // Pre-register one endpoint
        manager
            .resolved_ips
            .insert("endpoint-a.example.com".to_string(), vec![]);

        // Re-registering the same endpoint is a no-op (dedup), does not count toward cap
        manager.register_endpoint("endpoint-a.example.com").await;
        assert_eq!(manager.resolved_ips.len(), 1);
    }

    // --- IpDistributor::select_ip_excluding tests ---

    #[test]
    fn test_select_ip_excluding_skips_excluded_ips() {
        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
        ];
        let distributor = IpDistributor::new(ips.clone());

        let exclude = vec![ips[0]];
        // Call multiple times — should never return the excluded IP
        for _ in 0..10 {
            let picked = distributor.select_ip_excluding(&exclude);
            assert!(picked.is_some());
            assert_ne!(picked.unwrap(), ips[0]);
        }
    }

    #[test]
    fn test_select_ip_excluding_returns_none_when_all_excluded() {
        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        ];
        let distributor = IpDistributor::new(ips.clone());

        let result = distributor.select_ip_excluding(&ips);
        assert_eq!(result, None);
    }

    #[test]
    fn test_select_ip_excluding_returns_none_on_empty_distributor() {
        let distributor = IpDistributor::new(vec![]);
        let exclude = vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))];
        assert_eq!(distributor.select_ip_excluding(&exclude), None);
    }

    #[test]
    fn test_select_ip_excluding_with_empty_exclude_list() {
        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        ];
        let distributor = IpDistributor::new(ips.clone());

        // With no exclusions, behaves like select_ip
        let result = distributor.select_ip_excluding(&[]);
        assert!(result.is_some());
        assert!(ips.contains(&result.unwrap()));
    }

    // --- ConnectionPoolManager::get_distinct_distributed_ips tests ---

    #[test]
    fn test_get_distinct_distributed_ips_returns_2_for_3_ip_endpoint() {
        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
        ];
        let config = crate::config::ConnectionPoolConfig::default();
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        manager.ip_distributors.insert(
            "s3.us-west-2.amazonaws.com".to_string(),
            IpDistributor::new(ips.clone()),
        );

        let result = manager.get_distinct_distributed_ips("s3.us-west-2.amazonaws.com", 2);
        assert_eq!(result.len(), 2);
        assert_ne!(result[0], result[1], "IPs must be distinct");
    }

    #[test]
    fn test_get_distinct_distributed_ips_returns_1_for_1_ip_endpoint() {
        let ips = vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))];
        let config = crate::config::ConnectionPoolConfig::default();
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        manager.ip_distributors.insert(
            "s3.eu-west-1.amazonaws.com".to_string(),
            IpDistributor::new(ips.clone()),
        );

        let result = manager.get_distinct_distributed_ips("s3.eu-west-1.amazonaws.com", 2);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ips[0]);
    }

    #[test]
    fn test_get_distinct_distributed_ips_returns_empty_for_unknown_endpoint() {
        let config = crate::config::ConnectionPoolConfig::default();
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();

        let result = manager.get_distinct_distributed_ips("unknown.example.com", 2);
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_distinct_distributed_ips_all_distinct() {
        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4)),
        ];
        let config = crate::config::ConnectionPoolConfig::default();
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();
        manager.ip_distributors.insert(
            "s3.us-west-2.amazonaws.com".to_string(),
            IpDistributor::new(ips),
        );

        let result = manager.get_distinct_distributed_ips("s3.us-west-2.amazonaws.com", 3);
        assert_eq!(result.len(), 3);
        // All elements must be distinct
        let mut unique = result.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3);
    }
}
