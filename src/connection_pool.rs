//! Connection Pool Module
//!
//! Manages IP distribution, DNS resolution, and health tracking for S3 endpoints.
//! Actual TCP connection pooling is handled by hyper's built-in pool.

use crate::{ProxyError, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, info, warn};
use trust_dns_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
use trust_dns_resolver::TokioAsyncResolver;

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
        suffixes.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

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

    /// Return the number of IPs currently in the selection set.
    pub fn ip_count(&self) -> usize {
        self.ips.len()
    }

    /// Return a snapshot of the current IP set for health check reporting.
    pub fn get_ips(&self) -> Vec<IpAddr> {
        self.ips.clone()
    }
}

// ---------------------------------------------------------------------------
// IpHealthTracker — lock-free per-IP failure tracking
// ---------------------------------------------------------------------------

/// Tracks consecutive failures per IP for automatic exclusion from round-robin.
///
/// Uses `DashMap` for lock-free concurrent access. When an IP's consecutive
/// failure count reaches the threshold, the caller should remove it from the
/// `IpDistributor`. Successes reset the counter. DNS refresh restores excluded IPs.
pub struct IpHealthTracker {
    failures: DashMap<IpAddr, u32>,
    threshold: u32,
}

impl IpHealthTracker {
    pub fn new(threshold: u32) -> Self {
        Self {
            failures: DashMap::new(),
            threshold,
        }
    }

    /// Record a successful request. Resets failure count for the IP.
    pub fn record_success(&self, ip: &IpAddr) {
        self.failures.remove(ip);
    }

    /// Record a failed request. Returns `true` if the threshold is reached
    /// and the IP should be excluded from the distributor.
    pub fn record_failure(&self, ip: &IpAddr) -> bool {
        let mut count = self.failures.entry(*ip).or_insert(0);
        *count += 1;
        *count >= self.threshold
    }

    /// Clear all failure counts (e.g., on DNS refresh when IPs are restored).
    pub fn clear(&self) {
        self.failures.clear();
    }
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
    resolver: TokioAsyncResolver,
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
}

impl ConnectionPoolManager {
    /// Create a new connection pool manager with external DNS servers
    pub fn new() -> Result<Self> {
        let mut config = ResolverConfig::new();
        config.add_name_server(NameServerConfigGroup::google().into_inner()[0].clone());
        config.add_name_server(NameServerConfigGroup::google().into_inner()[1].clone());
        config.add_name_server(NameServerConfigGroup::cloudflare().into_inner()[0].clone());
        config.add_name_server(NameServerConfigGroup::cloudflare().into_inner()[1].clone());

        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = false;

        let resolver = TokioAsyncResolver::tokio(config, opts);
        info!("DNS resolver initialized with external servers (bypassing /etc/hosts)");

        Ok(Self {
            resolver,
            default_dns_refresh_interval: Duration::from_secs(60),
            dns_refresh_count: 0,
            overrides: EndpointOverrides::from_config(&HashMap::new()),
            ip_distributors: HashMap::new(),
            resolved_ips: HashMap::new(),
            last_dns_refresh: HashMap::new(),
        })
    }

    /// Create a new connection pool manager with configuration
    pub fn new_with_config(config: crate::config::ConnectionPoolConfig) -> Result<Self> {
        let mut resolver_config = ResolverConfig::new();

        if config.dns_servers.is_empty() {
            resolver_config
                .add_name_server(NameServerConfigGroup::google().into_inner()[0].clone());
            resolver_config
                .add_name_server(NameServerConfigGroup::google().into_inner()[1].clone());
            resolver_config
                .add_name_server(NameServerConfigGroup::cloudflare().into_inner()[0].clone());
            resolver_config
                .add_name_server(NameServerConfigGroup::cloudflare().into_inner()[1].clone());
            info!("DNS resolver initialized with default servers: Google DNS + Cloudflare DNS (bypassing /etc/hosts)");
        } else {
            use trust_dns_resolver::config::{NameServerConfig, Protocol};
            for dns_server in &config.dns_servers {
                match dns_server.parse::<std::net::IpAddr>() {
                    Ok(ip) => {
                        let socket_addr = std::net::SocketAddr::new(ip, 53);
                        let ns_config = NameServerConfig {
                            socket_addr,
                            protocol: Protocol::Udp,
                            tls_dns_name: None,
                            trust_negative_responses: true,
                            bind_addr: None,
                        };
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
        opts.use_hosts_file = false;

        let resolver = TokioAsyncResolver::tokio(resolver_config, opts);

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

    /// Resolve endpoint to IP addresses
    async fn resolve_endpoint(&self, endpoint: &str) -> Result<Vec<IpAddr>> {
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
        info!(endpoint = %endpoint, "Registering endpoint for DNS-based IP distribution");
        if let Err(e) = self.refresh_endpoint_dns(endpoint).await {
            warn!("Initial DNS resolution failed for endpoint {}: {}", endpoint, e);
        }
    }

    /// Refresh DNS for all registered endpoints whose refresh interval has elapsed.
    pub async fn refresh_dns(&mut self) -> Result<()> {
        let now = std::time::SystemTime::now();
        let endpoints_to_refresh: Vec<String> = self
            .resolved_ips
            .keys()
            .filter(|endpoint| {
                self.last_dns_refresh
                    .get(*endpoint)
                    .map(|last| {
                        now.duration_since(*last)
                            .unwrap_or(Duration::ZERO)
                            >= self.default_dns_refresh_interval
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        for endpoint in endpoints_to_refresh {
            if let Err(e) = self.refresh_endpoint_dns(&endpoint).await {
                warn!("Failed to refresh DNS for endpoint {}: {}", endpoint, e);
            }
        }

        Ok(())
    }

    /// Refresh DNS for a specific endpoint and update the IP distributor.
    ///
    /// Also clears health tracker failure counts for any IPs that are restored
    /// by the refresh, so previously-excluded IPs get a clean slate.
    pub async fn refresh_endpoint_dns(&mut self, endpoint: &str) -> Result<()> {
        debug!("Refreshing DNS for endpoint: {}", endpoint);

        let new_ip_addresses = self.resolve_endpoint(endpoint).await?;

        self.resolved_ips
            .insert(endpoint.to_string(), new_ip_addresses.clone());
        self.last_dns_refresh
            .insert(endpoint.to_string(), std::time::SystemTime::now());

        self.dns_refresh_count += 1;
        debug!("DNS refresh count: {}", self.dns_refresh_count);

        // Rebuild the distributor with all resolved IPs (restores previously excluded IPs)
        if let Some(distributor) = self.ip_distributors.get_mut(endpoint) {
            info!(
                endpoint = %endpoint,
                ip_count = new_ip_addresses.len(),
                "Updating IP distributor on DNS refresh"
            );
            distributor.update_ips(new_ip_addresses, "DNS refresh");
        } else {
            info!(
                endpoint = %endpoint,
                ip_count = new_ip_addresses.len(),
                "Creating IP distributor on DNS refresh"
            );
            self.ip_distributors
                .insert(endpoint.to_string(), IpDistributor::new(new_ip_addresses));
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
        assert!(tracker.record_failure(&ip));  // 3 — threshold reached
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
        assert!(tracker.record_failure(&ip1));  // ip1 now at 3 — threshold
        assert!(tracker.record_failure(&ip2));  // ip2 now at 3 — threshold
    }

    #[test]
    fn test_health_tracker_clear_resets_all() {
        let tracker = IpHealthTracker::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        tracker.record_failure(&ip);
        tracker.record_failure(&ip);
        tracker.clear();
        // After clear, count starts from 0
        assert!(!tracker.record_failure(&ip)); // 1
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

        let expected_ips = vec![
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
        assert_eq!(manager.get_distributed_ip("s3.eu-west-1.amazonaws.com"), None);
    }

    #[test]
    fn test_startup_before_dns_resolution_falls_back_to_hostname() {
        let config = crate::config::ConnectionPoolConfig {
            ip_distribution_enabled: true,
            ..crate::config::ConnectionPoolConfig::default()
        };
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();

        assert_eq!(manager.get_distributed_ip("s3.eu-west-1.amazonaws.com"), None);
        assert_eq!(manager.get_distributed_ip("s3.us-east-1.amazonaws.com"), None);
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

        assert_eq!(manager.get_hostname_for_ip(&ips[0]), Some(endpoint.to_string()));
        assert_eq!(manager.get_hostname_for_ip(&ips[1]), Some(endpoint.to_string()));
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
            vec!["10.0.0.1".to_string(), "10.0.0.2".to_string(), "10.0.0.3".to_string()],
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
        raw.insert(
            "*.amazonaws.com".to_string(),
            vec!["10.0.0.1".to_string()],
        );
        raw.insert(
            "*.accesspoint.s3-global.amazonaws.com".to_string(),
            vec!["10.0.0.2".to_string()],
        );
        let eo = EndpointOverrides::from_config(&raw);
        // MRAP hostname should match the more specific suffix
        let ips = eo.resolve("myalias.mrap.accesspoint.s3-global.amazonaws.com").unwrap();
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
        raw.insert("*.s3.us-west-2.amazonaws.com".to_string(), vec!["10.0.0.1".to_string()]);
        let eo2 = EndpointOverrides::from_config(&raw);
        assert!(!eo2.is_empty());
    }
}
