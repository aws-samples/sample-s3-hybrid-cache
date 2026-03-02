//! Connection Pool Module
//!
//! Manages persistent HTTP connections to S3 endpoints with intelligent load balancing,
//! DNS resolution, health monitoring, and performance optimization.

use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tracing::{debug, info, warn};
use trust_dns_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
use trust_dns_resolver::TokioAsyncResolver;

/// Load balancing strategy for connection selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    LeastConnections,
    PerformanceBased,
    RequestSizeBased,
}

/// IP address resolution result with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpAddressInfo {
    pub ip_address: IpAddr,
    pub resolved_at: SystemTime,
    pub ttl: Option<Duration>,
    pub is_preferred: bool,
}

/// DNS resolution cache entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsResolutionCache {
    pub endpoint: String,
    pub ip_addresses: Vec<IpAddressInfo>,
    pub last_resolved: SystemTime,
    pub next_refresh: SystemTime,
}

/// Connection selection criteria
#[derive(Debug, Clone)]
pub struct ConnectionSelectionCriteria {
    pub request_size: Option<u64>,
    pub priority: ConnectionPriority,
    pub load_balancing_strategy: LoadBalancingStrategy,
}

/// Connection priority for request routing
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionPriority {
    LowLatency,     // For small requests < 1MB
    HighThroughput, // For large requests > 1MB
    Balanced,       // Default balanced approach
}

/// Performance metrics for connection optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub latency_samples: Vec<Duration>,
    pub throughput_samples: Vec<f64>, // bytes per second
    pub last_updated: SystemTime,
    pub sample_count: u64,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            latency_samples: Vec::new(),
            throughput_samples: Vec::new(),
            last_updated: SystemTime::now(),
            sample_count: 0,
        }
    }
}

/// Connection pool for managing HTTP connections to S3 endpoints
#[derive(Debug)]
pub struct ConnectionPool {
    pub endpoint: String,
    pub ip_addresses: Vec<IpAddr>,
    pub connections: HashMap<IpAddr, Vec<Connection>>,
    pub health_metrics: HashMap<IpAddr, HealthMetrics>,
    pub last_dns_refresh: SystemTime,
    pub max_connections_per_ip: usize,
    pub dns_refresh_interval: Duration,
    pub dns_cache: Option<DnsResolutionCache>,
    pub load_balancing_strategy: LoadBalancingStrategy,
}

/// Individual HTTP connection
#[derive(Debug, Clone)]
pub struct Connection {
    pub id: String,
    pub ip_address: IpAddr,
    pub created_at: SystemTime,
    pub last_used: SystemTime,
    pub request_count: u64,
    pub is_active: bool,
}

/// Health metrics for connection monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMetrics {
    pub ip_address: IpAddr,
    pub average_latency: Duration,
    pub success_rate: f32,
    pub active_connections: usize,
    pub total_requests: u64,
    pub failed_requests: u64,
    pub last_failure: Option<SystemTime>,
    pub consecutive_failures: u32,
    pub performance_metrics: PerformanceMetrics,
    pub is_excluded: bool,
    pub exclusion_expires_at: Option<SystemTime>,
}

impl Default for HealthMetrics {
    fn default() -> Self {
        Self {
            ip_address: IpAddr::from([0, 0, 0, 0]),
            average_latency: Duration::from_millis(0),
            success_rate: 1.0,
            active_connections: 0,
            total_requests: 0,
            failed_requests: 0,
            last_failure: None,
            consecutive_failures: 0,
            performance_metrics: PerformanceMetrics::default(),
            is_excluded: false,
            exclusion_expires_at: None,
        }
    }
}

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
    /// This only removes the IP from the distributor's selection Vec so new
    /// requests are no longer routed to it. Existing connections to the removed
    /// IP remain in hyper's internal pool and in-flight requests on those
    /// connections complete naturally — hyper manages connection lifecycle
    /// independently of the distributor. No explicit abort logic is needed.
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

/// Connection pool manager
pub struct ConnectionPoolManager {
    pools: HashMap<String, ConnectionPool>,
    resolver: TokioAsyncResolver,
    default_dns_refresh_interval: Duration,
    default_max_connections_per_ip: usize,
    default_load_balancing_strategy: LoadBalancingStrategy,
    dns_refresh_count: u64,
    /// Static hostname-to-IP mappings that bypass DNS resolution (for PrivateLink etc.)
    endpoint_overrides: HashMap<String, Vec<IpAddr>>,
    /// Per-endpoint IP distributors for round-robin request distribution
    ip_distributors: HashMap<String, IpDistributor>,
}

impl ConnectionPool {
}

impl ConnectionPoolManager {
    /// Create a new connection pool manager with external DNS servers
    ///
    /// This bypasses /etc/hosts and uses external DNS servers (Google DNS and Cloudflare DNS)
    /// to ensure the proxy can resolve S3 endpoints to real AWS IPs even when clients
    /// have configured their hosts file to point S3 domains to the proxy itself.
    pub fn new() -> Result<Self> {
        // Use external DNS servers to bypass /etc/hosts
        // This is critical for proxy operation: clients point S3 to the proxy via hosts file,
        // but the proxy must resolve S3 to real AWS IPs
        let mut config = ResolverConfig::new();

        // Add Google DNS (8.8.8.8, 8.8.4.4)
        config.add_name_server(NameServerConfigGroup::google().into_inner()[0].clone());
        config.add_name_server(NameServerConfigGroup::google().into_inner()[1].clone());

        // Add Cloudflare DNS (1.1.1.1, 1.0.0.1) as backup
        config.add_name_server(NameServerConfigGroup::cloudflare().into_inner()[0].clone());
        config.add_name_server(NameServerConfigGroup::cloudflare().into_inner()[1].clone());

        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = false; // Critical: bypass /etc/hosts

        let resolver = TokioAsyncResolver::tokio(config, opts);

        info!("DNS resolver initialized with external servers (bypassing /etc/hosts)");

        Ok(Self {
            pools: HashMap::new(),
            resolver,
            default_dns_refresh_interval: Duration::from_secs(60), // 1 minute as per requirement 16.2
            default_max_connections_per_ip: 10,
            default_load_balancing_strategy: LoadBalancingStrategy::PerformanceBased,
            dns_refresh_count: 0,
            endpoint_overrides: HashMap::new(),
            ip_distributors: HashMap::new(),
        })
    }

    /// Create a new connection pool manager with configuration
    pub fn new_with_config(config: crate::config::ConnectionPoolConfig) -> Result<Self> {
        // Use external DNS servers to bypass /etc/hosts
        let mut resolver_config = ResolverConfig::new();

        if config.dns_servers.is_empty() {
            // Default: Add Google DNS (8.8.8.8, 8.8.4.4) and Cloudflare DNS (1.1.1.1, 1.0.0.1)
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
            // Use custom DNS servers from configuration
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
        opts.use_hosts_file = false; // Critical: bypass /etc/hosts

        let resolver = TokioAsyncResolver::tokio(resolver_config, opts);

        // Parse endpoint overrides
        let mut endpoint_overrides = HashMap::new();
        for (hostname, ip_strings) in &config.endpoint_overrides {
            let mut ips = Vec::new();
            for ip_str in ip_strings {
                match ip_str.parse::<IpAddr>() {
                    Ok(ip) => ips.push(ip),
                    Err(e) => {
                        warn!("Invalid IP address '{}' in endpoint_overrides for '{}': {}", ip_str, hostname, e);
                    }
                }
            }
            if !ips.is_empty() {
                info!("Endpoint override: {} -> {:?}", hostname, ips);
                endpoint_overrides.insert(hostname.clone(), ips);
            }
        }

        Ok(Self {
            pools: HashMap::new(),
            resolver,
            default_dns_refresh_interval: config.dns_refresh_interval,
            default_max_connections_per_ip: config.max_connections_per_ip,
            default_load_balancing_strategy: LoadBalancingStrategy::PerformanceBased,
            dns_refresh_count: 0,
            endpoint_overrides,
            ip_distributors: HashMap::new(),
        })
    }

    /// Get or create a connection pool for an endpoint
    pub async fn get_pool(&mut self, endpoint: &str) -> Result<&mut ConnectionPool> {
        if !self.pools.contains_key(endpoint) {
            let pool = self.create_pool(endpoint).await?;
            self.pools.insert(endpoint.to_string(), pool);
        }

        Ok(self.pools.get_mut(endpoint).unwrap())
    }

    /// Create a new connection pool for an endpoint
    async fn create_pool(&self, endpoint: &str) -> Result<ConnectionPool> {
        info!("Creating connection pool for endpoint: {}", endpoint);

        let ip_addresses = self.resolve_endpoint(endpoint).await?;
        let mut health_metrics = HashMap::new();
        let mut connections = HashMap::new();

        // Initialize connection pools and health metrics for each IP address (Requirement 16.1)
        for ip in &ip_addresses {
            let mut metrics = HealthMetrics::default();
            metrics.ip_address = *ip;
            health_metrics.insert(*ip, metrics);
            connections.insert(*ip, Vec::new());
        }

        // Create DNS cache entry
        let dns_cache = DnsResolutionCache {
            endpoint: endpoint.to_string(),
            ip_addresses: ip_addresses
                .iter()
                .map(|ip| IpAddressInfo {
                    ip_address: *ip,
                    resolved_at: SystemTime::now(),
                    ttl: Some(self.default_dns_refresh_interval),
                    is_preferred: true,
                })
                .collect(),
            last_resolved: SystemTime::now(),
            next_refresh: SystemTime::now() + self.default_dns_refresh_interval,
        };

        Ok(ConnectionPool {
            endpoint: endpoint.to_string(),
            ip_addresses,
            connections,
            health_metrics,
            last_dns_refresh: SystemTime::now(),
            max_connections_per_ip: self.default_max_connections_per_ip,
            dns_refresh_interval: self.default_dns_refresh_interval,
            dns_cache: Some(dns_cache),
            load_balancing_strategy: self.default_load_balancing_strategy.clone(),
        })
    }

    /// Resolve endpoint to IP addresses
    async fn resolve_endpoint(&self, endpoint: &str) -> Result<Vec<IpAddr>> {
        // Check endpoint overrides first (for PrivateLink etc.)
        if let Some(ips) = self.endpoint_overrides.get(endpoint) {
            info!(
                "Using endpoint override for {}: {:?}",
                endpoint, ips
            );
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

    /// Get optimal connection for a request (Requirement 16.3)
    pub async fn get_connection(
        &mut self,
        endpoint: &str,
        request_size: Option<u64>,
    ) -> Result<Connection> {
        // Determine connection priority based on request size (Requirements 16.5, 16.6)
        let priority = match request_size {
            Some(size) if size < 1_048_576 => ConnectionPriority::LowLatency, // < 1MB
            Some(_) => ConnectionPriority::HighThroughput,                    // >= 1MB
            None => ConnectionPriority::Balanced,
        };

        // Get pool and extract load balancing strategy
        let load_balancing_strategy = {
            let pool = self.get_pool(endpoint).await?;
            pool.load_balancing_strategy.clone()
        };

        let criteria = ConnectionSelectionCriteria {
            request_size,
            priority,
            load_balancing_strategy,
        };

        self.select_optimal_connection_for_endpoint(endpoint, &criteria)
            .await
    }

    /// Select optimal connection based on criteria for a specific endpoint
    async fn select_optimal_connection_for_endpoint(
        &mut self,
        endpoint: &str,
        criteria: &ConnectionSelectionCriteria,
    ) -> Result<Connection> {
        // Ensure pool exists
        self.get_pool(endpoint).await?;

        // Extract the data we need from the pool
        let (_available_ips, selected_ip) = {
            let pool = self.pools.get(endpoint).unwrap();

            // Filter out excluded IP addresses (Requirement 16.7)
            let available_ips: Vec<IpAddr> = pool
                .ip_addresses
                .iter()
                .filter(|ip| {
                    if let Some(metrics) = pool.health_metrics.get(ip) {
                        !metrics.is_excluded
                            || metrics
                                .exclusion_expires_at
                                .map_or(true, |expires| SystemTime::now() > expires)
                    } else {
                        true
                    }
                })
                .copied()
                .collect();

            if available_ips.is_empty() {
                return Err(ProxyError::ConnectionError(
                    "No available IP addresses for connection".to_string(),
                ));
            }

            // Select best IP based on performance metrics and request characteristics
            let selected_ip = self.select_best_ip(&available_ips, pool, criteria)?;
            (available_ips, selected_ip)
        };

        // Get or create connection for the selected IP
        let pool = self.pools.get_mut(endpoint).unwrap();
        Self::get_or_create_connection(pool, selected_ip)
    }

    /// Select best IP address based on performance metrics (Requirement 16.3)
    fn select_best_ip(
        &self,
        available_ips: &[IpAddr],
        pool: &ConnectionPool,
        criteria: &ConnectionSelectionCriteria,
    ) -> Result<IpAddr> {
        let mut best_ip = available_ips[0];
        let mut best_score = f64::MIN;

        for &ip in available_ips {
            let score = self.calculate_ip_score(ip, pool, criteria);
            if score > best_score {
                best_score = score;
                best_ip = ip;
            }
        }

        debug!(
            "Selected IP {} with score {} for request",
            best_ip, best_score
        );
        Ok(best_ip)
    }

    /// Calculate score for IP address selection
    fn calculate_ip_score(
        &self,
        ip: IpAddr,
        pool: &ConnectionPool,
        criteria: &ConnectionSelectionCriteria,
    ) -> f64 {
        let metrics = pool.health_metrics.get(&ip).unwrap();

        // Base score from success rate (0.0 to 1.0)
        let mut score = metrics.success_rate as f64;

        // Adjust for latency (lower is better)
        let latency_penalty = metrics.average_latency.as_millis() as f64 / 1000.0;
        score -= latency_penalty * 0.1;

        // Adjust for connection load
        let connection_count = pool.connections.get(&ip).map_or(0, |conns| conns.len());
        let load_penalty = connection_count as f64 / pool.max_connections_per_ip as f64;
        score -= load_penalty * 0.2;

        // Adjust based on request priority
        match criteria.priority {
            ConnectionPriority::LowLatency => {
                // Prefer IPs with lower latency
                score += (1000.0 - metrics.average_latency.as_millis() as f64) / 1000.0 * 0.3;
            }
            ConnectionPriority::HighThroughput => {
                // Prefer IPs with higher throughput and more available connections
                if let Some(avg_throughput) = metrics.performance_metrics.throughput_samples.last()
                {
                    score += avg_throughput / 1_000_000.0 * 0.3; // Normalize to MB/s
                }
            }
            ConnectionPriority::Balanced => {
                // Balanced approach - no additional adjustments
            }
        }

        // Penalty for consecutive failures
        score -= metrics.consecutive_failures as f64 * 0.1;

        score.max(0.0)
    }

    /// Get or create connection for IP address
    fn get_or_create_connection(pool: &mut ConnectionPool, ip: IpAddr) -> Result<Connection> {
        let connections = pool.connections.get_mut(&ip).unwrap();

        // Try to reuse an existing idle connection
        if let Some(connection) = connections.iter_mut().find(|c| !c.is_active) {
            connection.is_active = true;
            connection.last_used = SystemTime::now();
            debug!(
                "Reusing existing connection {} for IP {}",
                connection.id, ip
            );
            return Ok(connection.clone());
        }

        // Create new connection if under limit
        if connections.len() < pool.max_connections_per_ip {
            let connection = Connection {
                id: uuid::Uuid::new_v4().to_string(),
                ip_address: ip,
                created_at: SystemTime::now(),
                last_used: SystemTime::now(),
                request_count: 0,
                is_active: true,
            };

            connections.push(connection.clone());
            debug!("Created new connection {} for IP {}", connection.id, ip);
            return Ok(connection);
        }

        // If at limit, close least recently used connection and create new one (Requirement 16.9)
        if let Some(lru_index) = connections
            .iter()
            .enumerate()
            .min_by_key(|(_, c)| c.last_used)
            .map(|(i, _)| i)
        {
            let old_connection = connections.remove(lru_index);
            debug!("Closed LRU connection {} for IP {}", old_connection.id, ip);
        }

        let connection = Connection {
            id: uuid::Uuid::new_v4().to_string(),
            ip_address: ip,
            created_at: SystemTime::now(),
            last_used: SystemTime::now(),
            request_count: 0,
            is_active: true,
        };

        connections.push(connection.clone());
        debug!(
            "Created new connection {} for IP {} (replaced LRU)",
            connection.id, ip
        );
        Ok(connection)
    }

    /// Refresh DNS for all endpoints (Requirement 16.2)
    pub async fn refresh_dns(&mut self) -> Result<()> {
        let now = SystemTime::now();
        let endpoints_to_refresh: Vec<String> = self
            .pools
            .iter()
            .filter(|(_, pool)| {
                now.duration_since(pool.last_dns_refresh)
                    .unwrap_or(Duration::ZERO)
                    >= pool.dns_refresh_interval
            })
            .map(|(endpoint, _)| endpoint.clone())
            .collect();

        for endpoint in endpoints_to_refresh {
            if let Err(e) = self.refresh_endpoint_dns(&endpoint).await {
                warn!("Failed to refresh DNS for endpoint {}: {}", endpoint, e);
            }
        }

        Ok(())
    }

    /// Refresh DNS for a specific endpoint
    async fn refresh_endpoint_dns(&mut self, endpoint: &str) -> Result<()> {
        debug!("Refreshing DNS for endpoint: {}", endpoint);

        let new_ip_addresses = self.resolve_endpoint(endpoint).await?;

        if let Some(pool) = self.pools.get_mut(endpoint) {
            let old_ips = pool.ip_addresses.clone();
            pool.ip_addresses = new_ip_addresses.clone();
            pool.last_dns_refresh = SystemTime::now();

            // Increment DNS refresh counter
            self.dns_refresh_count += 1;
            debug!("DNS refresh count: {}", self.dns_refresh_count);

            // Update DNS cache
            if let Some(ref mut dns_cache) = pool.dns_cache {
                dns_cache.ip_addresses = new_ip_addresses
                    .iter()
                    .map(|ip| IpAddressInfo {
                        ip_address: *ip,
                        resolved_at: SystemTime::now(),
                        ttl: Some(pool.dns_refresh_interval),
                        is_preferred: true,
                    })
                    .collect();
                dns_cache.last_resolved = SystemTime::now();
                dns_cache.next_refresh = SystemTime::now() + pool.dns_refresh_interval;
            }

            // Initialize health metrics for new IP addresses (Requirement 16.8)
            for &ip in &new_ip_addresses {
                if !pool.health_metrics.contains_key(&ip) {
                    let mut metrics = HealthMetrics::default();
                    metrics.ip_address = ip;
                    pool.health_metrics.insert(ip, metrics);
                    pool.connections.insert(ip, Vec::new());
                    info!(
                        "Added new IP address {} to pool for endpoint {}",
                        ip, endpoint
                    );
                }
            }

            // Remove connections for IP addresses that are no longer resolved
            let removed_ips: Vec<IpAddr> = old_ips
                .iter()
                .filter(|ip| !new_ip_addresses.contains(ip))
                .copied()
                .collect();

            for ip in removed_ips {
                pool.connections.remove(&ip);
                pool.health_metrics.remove(&ip);
                info!(
                    "Removed IP address {} from pool for endpoint {}",
                    ip, endpoint
                );
            }
        }

        // Update the IP distributor with the current set of healthy IPs (Requirements 3.1, 3.2)
        // Filter out excluded IPs so the distributor only selects healthy ones
        let healthy_ips: Vec<IpAddr> = if let Some(pool) = self.pools.get(endpoint) {
            new_ip_addresses
                .iter()
                .filter(|ip| {
                    pool.health_metrics
                        .get(ip)
                        .map(|m| !m.is_excluded)
                        .unwrap_or(true)
                })
                .copied()
                .collect()
        } else {
            new_ip_addresses.clone()
        };

        if let Some(distributor) = self.ip_distributors.get_mut(endpoint) {
            info!(
                endpoint = %endpoint,
                ip_count = healthy_ips.len(),
                "Updating IP distributor on DNS refresh"
            );
            distributor.update_ips(healthy_ips, "DNS refresh");
        } else {
            info!(
                endpoint = %endpoint,
                ip_count = healthy_ips.len(),
                "Creating IP distributor on DNS refresh"
            );
            self.ip_distributors
                .insert(endpoint.to_string(), IpDistributor::new(healthy_ips));
        }

        Ok(())
    }

    /// Monitor connection health for all pools (Requirement 16.3)
    pub async fn monitor_connection_health(&mut self) -> Result<()> {
        let now = SystemTime::now();

        // Collect IP exclusion/re-enable events to update distributors after the loop
        let mut excluded_ips: Vec<(String, IpAddr)> = Vec::new();
        let mut reenabled_ips: Vec<(String, IpAddr)> = Vec::new();

        for (endpoint, pool) in self.pools.iter_mut() {
            for (ip, metrics) in pool.health_metrics.iter_mut() {
                // Check if IP should be un-excluded
                if metrics.is_excluded {
                    if let Some(expires_at) = metrics.exclusion_expires_at {
                        if now > expires_at {
                            metrics.is_excluded = false;
                            metrics.exclusion_expires_at = None;
                            metrics.consecutive_failures = 0;
                            info!(
                                ip = %ip,
                                endpoint = %endpoint,
                                reason = "exclusion expiry",
                                "Re-enabled IP address for endpoint"
                            );
                            reenabled_ips.push((endpoint.clone(), *ip));
                        }
                    }
                }

                // Mark IP as excluded if consecutive failures exceed threshold (Requirement 3.4)
                if !metrics.is_excluded && metrics.consecutive_failures >= 3 {
                    metrics.is_excluded = true;
                    metrics.exclusion_expires_at =
                        Some(now + Duration::from_secs(30));
                    info!(
                        ip = %ip,
                        endpoint = %endpoint,
                        reason = "health exclusion",
                        consecutive_failures = metrics.consecutive_failures,
                        "Excluded IP address from distributor"
                    );
                    excluded_ips.push((endpoint.clone(), *ip));
                }

                // Update active connection count
                if let Some(connections) = pool.connections.get(ip) {
                    metrics.active_connections = connections.iter().filter(|c| c.is_active).count();
                }

                // Calculate success rate
                if metrics.total_requests > 0 {
                    metrics.success_rate =
                        1.0 - (metrics.failed_requests as f32 / metrics.total_requests as f32);
                }

                // Update average latency from samples
                if !metrics.performance_metrics.latency_samples.is_empty() {
                    let total_latency: Duration =
                        metrics.performance_metrics.latency_samples.iter().sum();
                    metrics.average_latency =
                        total_latency / metrics.performance_metrics.latency_samples.len() as u32;
                }
            }
        }

        // Update IP distributors for excluded IPs (Requirement 3.4)
        for (endpoint, ip) in excluded_ips {
            if let Some(distributor) = self.ip_distributors.get_mut(&endpoint) {
                distributor.remove_ip(ip, "health exclusion");
            }
        }

        // Note: Re-enabled IPs will be re-added on the next DNS refresh via update_ips.
        // We don't add them back here because the distributor's update_ips in
        // refresh_endpoint_dns already filters out excluded IPs.
        // Log re-enabled IPs for observability (Requirement 6.3)
        for (endpoint, ip) in reenabled_ips {
            debug!(
                ip = %ip,
                endpoint = %endpoint,
                "Re-enabled IP will be added back to distributor on next DNS refresh"
            );
        }

        Ok(())
    }
    /// Release a connection back to the pool
    pub fn release_connection(&mut self, endpoint: &str, connection_id: &str) -> Result<()> {
        if let Some(pool) = self.pools.get_mut(endpoint) {
            for connections in pool.connections.values_mut() {
                if let Some(connection) = connections.iter_mut().find(|c| c.id == connection_id) {
                    connection.is_active = false;
                    connection.last_used = SystemTime::now();
                    debug!("Released connection {} back to pool", connection_id);
                    return Ok(());
                }
            }
        }

        debug!("Connection {} not found for release", connection_id);
        Ok(())
    }

    /// Intelligent load balancing for multiple connections (Requirement 16.6)
    pub async fn get_multiple_connections(
        &mut self,
        endpoint: &str,
        request_size: Option<u64>,
        connection_count: usize,
    ) -> Result<Vec<Connection>> {
        // For large requests, distribute across multiple IP addresses for maximum throughput
        if request_size.unwrap_or(0) > 1_048_576 && connection_count > 1 {
            // Ensure pool exists
            self.get_pool(endpoint).await?;

            // Extract available IPs
            let available_ips: Vec<IpAddr> = {
                let pool = self.pools.get(endpoint).unwrap();
                pool.ip_addresses
                    .iter()
                    .filter(|ip| {
                        if let Some(metrics) = pool.health_metrics.get(ip) {
                            !metrics.is_excluded
                                || metrics
                                    .exclusion_expires_at
                                    .map_or(true, |expires| SystemTime::now() > expires)
                        } else {
                            true
                        }
                    })
                    .copied()
                    .collect()
            };

            if available_ips.is_empty() {
                return Err(ProxyError::ConnectionError(
                    "No available IP addresses for connections".to_string(),
                ));
            }

            // Distribute connections across available IPs
            let mut connections = Vec::new();
            let connections_per_ip =
                (connection_count + available_ips.len() - 1) / available_ips.len();
            let mut remaining_connections = connection_count;

            for &ip in &available_ips {
                if remaining_connections == 0 {
                    break;
                }

                let ip_connections = remaining_connections.min(connections_per_ip);
                for _ in 0..ip_connections {
                    let pool = self.pools.get_mut(endpoint).unwrap();
                    let connection = Self::get_or_create_connection(pool, ip)?;
                    connections.push(connection);
                    remaining_connections -= 1;
                }
            }

            info!(
                "Distributed {} connections across {} IP addresses",
                connections.len(),
                available_ips.len()
            );
            return Ok(connections);
        }

        // For small requests or single connection, use standard selection
        let connection = self.get_connection(endpoint, request_size).await?;
        Ok(vec![connection])
    }
    /// Get health metrics for all connections
    pub async fn get_health_metrics(&self) -> Result<Vec<HealthMetrics>> {
        let mut all_metrics = Vec::new();

        for (_endpoint, pool) in &self.pools {
            for (_ip, metrics) in &pool.health_metrics {
                all_metrics.push(metrics.clone());
            }
        }

        Ok(all_metrics)
    }

    /// Get connection statistics for monitoring
    pub fn get_connection_statistics(&self, endpoint: &str) -> Option<ConnectionStatistics> {
        self.pools.get(endpoint).map(|pool| {
            let mut total_connections = 0;
            let mut active_connections = 0;
            let mut excluded_ips = 0;

            for (ip, connections) in &pool.connections {
                total_connections += connections.len();
                active_connections += connections.iter().filter(|c| c.is_active).count();

                if let Some(metrics) = pool.health_metrics.get(ip) {
                    if metrics.is_excluded {
                        excluded_ips += 1;
                    }
                }
            }

            let idle_connections = total_connections - active_connections;

            ConnectionStatistics {
                endpoint: endpoint.to_string(),
                total_ip_addresses: pool.ip_addresses.len(),
                excluded_ip_addresses: excluded_ips,
                total_connections,
                active_connections,
                idle_connections,
                last_dns_refresh: pool.last_dns_refresh,
                next_dns_refresh: pool.last_dns_refresh + pool.dns_refresh_interval,
            }
        })
    }

    /// Get per-IP connection distribution statistics for all endpoints with active distributors.
    ///
    /// Returns per-IP active/idle connection counts for each endpoint that has an
    /// IpDistributor, enabling operators to verify load distribution (Requirement 6.1).
    pub fn get_ip_distribution_stats(&self) -> IpDistributionStats {
        let mut endpoints = Vec::new();

        for (endpoint, distributor) in &self.ip_distributors {
            let distributor_ips = distributor.get_ips();
            let mut ip_stats = Vec::new();

            for ip in &distributor_ips {
                let (active, idle) = self
                    .pools
                    .get(endpoint)
                    .and_then(|pool| pool.connections.get(ip))
                    .map(|connections| {
                        let active = connections.iter().filter(|c| c.is_active).count();
                        let idle = connections.len() - active;
                        (active, idle)
                    })
                    .unwrap_or((0, 0));

                ip_stats.push(IpConnectionStats {
                    ip: ip.to_string(),
                    active_connections: active,
                    idle_connections: idle,
                });
            }

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

    /// Get a distributed IP address for the given endpoint using round-robin selection.
    ///
    /// For endpoints with `endpoint_overrides`, lazily initializes an `IpDistributor`
    /// from the static IPs on first call (Requirement 4.1).
    /// For DNS-resolved endpoints, the distributor is populated by `refresh_endpoint_dns`
    /// (Requirement 4.2).
    /// Returns `None` if no distributor exists or the IP set is empty, triggering
    /// fallback to hostname-based resolution (Requirement 7.1).
    pub fn get_distributed_ip(&mut self, endpoint: &str) -> Option<IpAddr> {
        // Lazily initialize distributor from endpoint_overrides if not yet created (Req 4.1)
        if !self.ip_distributors.contains_key(endpoint) {
            if let Some(override_ips) = self.endpoint_overrides.get(endpoint) {
                if !override_ips.is_empty() {
                    info!(
                        endpoint = %endpoint,
                        ip_count = override_ips.len(),
                        "Initializing IP distributor from endpoint overrides"
                    );
                    self.ip_distributors
                        .insert(endpoint.to_string(), IpDistributor::new(override_ips.clone()));
                }
            }
        }

        // Delegate to the distributor's round-robin selection (Req 1.1, 4.2, 4.3)
        self.ip_distributors
            .get(endpoint)
            .and_then(|d| d.select_ip())
    }

    /// Look up the endpoint hostname that owns a given IP address.
    ///
    /// Searches all IP distributors to find which endpoint the IP belongs to.
    /// Used by `CustomHttpsConnector` to determine the original hostname for TLS SNI
    /// when the URI authority has been rewritten to an IP address (Requirement 2.1).
    pub fn get_hostname_for_ip(&self, ip: &IpAddr) -> Option<String> {
        for (endpoint, distributor) in &self.ip_distributors {
            if distributor.get_ips().contains(ip) {
                return Some(endpoint.clone());
            }
        }
        // Also check endpoint_overrides for IPs not yet in a distributor
        for (endpoint, ips) in &self.endpoint_overrides {
            if ips.contains(ip) {
                return Some(endpoint.clone());
            }
        }
        None
    }

    /// Cleanup idle connections to free resources
    pub fn cleanup_idle_connections(&mut self, max_idle_time: Duration) -> Result<()> {
        let now = SystemTime::now();
        let mut cleaned_count = 0;

        for (_endpoint, pool) in self.pools.iter_mut() {
            for (ip, connections) in pool.connections.iter_mut() {
                connections.retain(|connection| {
                    let is_idle = !connection.is_active
                        && now
                            .duration_since(connection.last_used)
                            .unwrap_or(Duration::ZERO)
                            > max_idle_time;

                    if is_idle {
                        cleaned_count += 1;
                        debug!("Cleaned up idle connection {} for IP {}", connection.id, ip);
                    }

                    !is_idle
                });
            }
        }

        if cleaned_count > 0 {
            info!("Cleaned up {} idle connections", cleaned_count);
        }

        Ok(())
    }

    /// Get connections that have exceeded max lifetime
    /// Requirement 4.1, 4.4: Identify connections exceeding max lifetime
    pub fn get_expired_connections(&self, max_lifetime: Duration) -> Vec<(String, String)> {
        let now = SystemTime::now();
        let mut expired = Vec::new();

        for (endpoint, pool) in self.pools.iter() {
            for (_, connections) in pool.connections.iter() {
                for connection in connections {
                    if let Ok(age) = now.duration_since(connection.created_at) {
                        if age > max_lifetime {
                            expired.push((endpoint.clone(), connection.id.clone()));
                        }
                    }
                }
            }
        }

        expired
    }

    /// Close all connections gracefully (for graceful shutdown)
    pub async fn close_all_connections(&mut self) -> Result<()> {
        info!("Closing all connection pools gracefully");

        for (endpoint, pool) in self.pools.iter_mut() {
            info!("Closing connections for endpoint: {}", endpoint);

            // Close all connections in this pool
            for (ip, connections) in pool.connections.iter_mut() {
                info!("Closing {} connections for IP: {}", connections.len(), ip);
                connections.clear(); // This will drop the connections
            }
        }

        info!("All connection pools closed gracefully");
        Ok(())
    }

    /// Force close all connections (for emergency shutdown)
    pub async fn force_close_all_connections(&mut self) -> Result<()> {
        warn!("Force closing all connection pools");

        // Clear all pools immediately
        self.pools.clear();

        warn!("All connection pools force closed");
        Ok(())
    }
}

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

/// Connection statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStatistics {
    pub endpoint: String,
    pub total_ip_addresses: usize,
    pub excluded_ip_addresses: usize,
    pub total_connections: usize,
    pub active_connections: usize,
    pub idle_connections: usize,
    pub last_dns_refresh: SystemTime,
    pub next_dns_refresh: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_ips(count: u8) -> Vec<IpAddr> {
        (1..=count)
            .map(|i| IpAddr::V4(Ipv4Addr::new(10, 0, 0, i)))
            .collect()
    }

    #[test]
    fn test_select_ip_returns_none_when_empty() {
        let distributor = IpDistributor::new(vec![]);
        assert!(distributor.select_ip().is_none());
    }

    #[test]
    fn test_round_robin_cycles_through_all_ips_in_order() {
        let ips = test_ips(3);
        let distributor = IpDistributor::new(ips.clone());

        // First cycle
        assert_eq!(distributor.select_ip(), Some(ips[0]));
        assert_eq!(distributor.select_ip(), Some(ips[1]));
        assert_eq!(distributor.select_ip(), Some(ips[2]));

        // Second cycle wraps around
        assert_eq!(distributor.select_ip(), Some(ips[0]));
        assert_eq!(distributor.select_ip(), Some(ips[1]));
        assert_eq!(distributor.select_ip(), Some(ips[2]));
    }

    #[test]
    fn test_update_ips_replaces_set_and_resets_counter() {
        let mut distributor = IpDistributor::new(test_ips(3));

        // Advance counter past first IP
        distributor.select_ip();
        distributor.select_ip();

        // Replace with new IPs
        let new_ips = vec![
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)),
        ];
        distributor.update_ips(new_ips.clone(), "DNS refresh");

        // Counter reset: first selection returns the first new IP
        assert_eq!(distributor.select_ip(), Some(new_ips[0]));
        assert_eq!(distributor.select_ip(), Some(new_ips[1]));
        assert_eq!(distributor.ip_count(), 2);
        assert_eq!(distributor.get_ips(), new_ips);
    }

    #[test]
    fn test_remove_ip_excludes_from_selection() {
        let ips = test_ips(3);
        let mut distributor = IpDistributor::new(ips.clone());

        distributor.remove_ip(ips[1], "health exclusion"); // remove 10.0.0.2

        assert_eq!(distributor.ip_count(), 2);

        // Only 10.0.0.1 and 10.0.0.3 remain
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
        assert_eq!(distributor.get_ips(), ips);
    }

    #[test]
    fn test_remove_all_ips_then_select_returns_none() {
        let ips = test_ips(2);
        let mut distributor = IpDistributor::new(ips.clone());

        distributor.remove_ip(ips[0], "health exclusion");
        distributor.remove_ip(ips[1], "health exclusion");

        assert_eq!(distributor.ip_count(), 0);
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

    // --- DNS lifecycle integration tests (Task 4.3) ---

    #[test]
    fn test_update_ips_after_dns_refresh_selects_new_ips() {
        // Simulate DNS refresh: start with old IPs, update to new ones,
        // verify new IPs are selected and old ones are not.
        let old_ips = test_ips(2); // 10.0.0.1, 10.0.0.2
        let mut distributor = IpDistributor::new(old_ips.clone());

        // Verify old IPs work
        assert_eq!(distributor.select_ip(), Some(old_ips[0]));

        // DNS refresh returns new IPs
        let new_ips = vec![
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 1)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 2)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 3)),
        ];
        distributor.update_ips(new_ips.clone(), "DNS refresh");

        // All selections should come from new IPs only
        let selected: Vec<IpAddr> = (0..6).filter_map(|_| distributor.select_ip()).collect();
        assert_eq!(selected, vec![
            new_ips[0], new_ips[1], new_ips[2],
            new_ips[0], new_ips[1], new_ips[2],
        ]);

        // Old IPs must not appear
        for ip in &old_ips {
            assert!(!selected.contains(ip));
        }
    }

    #[test]
    fn test_removed_ip_no_longer_selected() {
        // Simulate health exclusion: remove an IP, verify it never appears in selection.
        let ips = test_ips(4); // 10.0.0.1 .. 10.0.0.4
        let mut distributor = IpDistributor::new(ips.clone());

        // Remove 10.0.0.2 and 10.0.0.4
        distributor.remove_ip(ips[1], "health exclusion");
        distributor.remove_ip(ips[3], "health exclusion");

        // Select many times, verify removed IPs never appear
        let selected: Vec<IpAddr> = (0..20).filter_map(|_| distributor.select_ip()).collect();
        assert!(!selected.contains(&ips[1]));
        assert!(!selected.contains(&ips[3]));
        // Only 10.0.0.1 and 10.0.0.3 should be selected
        for ip in &selected {
            assert!(ip == &ips[0] || ip == &ips[2]);
        }
    }

    #[test]
    fn test_endpoint_overrides_used_for_distribution() {
        // When endpoint_overrides are configured, get_distributed_ip should
        // lazily initialize a distributor from those static IPs.
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

        let expected_ips = vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 1, 100)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 2, 100)),
        ];

        // First call lazily initializes the distributor from overrides
        let ip1 = manager.get_distributed_ip("s3.us-west-2.amazonaws.com");
        let ip2 = manager.get_distributed_ip("s3.us-west-2.amazonaws.com");
        let ip3 = manager.get_distributed_ip("s3.us-west-2.amazonaws.com");

        assert_eq!(ip1, Some(expected_ips[0]));
        assert_eq!(ip2, Some(expected_ips[1]));
        // Round-robin wraps
        assert_eq!(ip3, Some(expected_ips[0]));
    }

    #[test]
    fn test_get_distributed_ip_returns_none_no_distributor() {
        // When no distributor exists and no overrides exist, get_distributed_ip returns None.
        // This covers the startup scenario before DNS resolution completes (Requirement 7.1, 7.3).
        let config = crate::config::ConnectionPoolConfig::default();
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        assert_eq!(manager.get_distributed_ip("s3.eu-west-1.amazonaws.com"), None);
    }

    #[test]
    fn test_startup_before_dns_resolution_falls_back_to_hostname() {
        // Validates: Requirements 7.1, 7.3
        // At startup, no DNS refresh has occurred, so ip_distributors is empty.
        // get_distributed_ip must return None for any endpoint, which triggers
        // the fallback path in try_forward_request to use the original hostname URI.
        let config = crate::config::ConnectionPoolConfig {
            ip_distribution_enabled: true,
            ..crate::config::ConnectionPoolConfig::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        // ip_distributors is empty at startup — no DNS refresh has happened
        assert!(manager.ip_distributors.is_empty());

        // Multiple different S3 endpoints all return None before DNS resolves
        assert_eq!(manager.get_distributed_ip("s3.eu-west-1.amazonaws.com"), None);
        assert_eq!(manager.get_distributed_ip("s3.us-east-1.amazonaws.com"), None);
        assert_eq!(manager.get_distributed_ip("s3.ap-southeast-1.amazonaws.com"), None);

        // Distributors remain empty (no lazy initialization without endpoint_overrides)
        assert!(manager.ip_distributors.is_empty());
    }

    // --- TLS SNI preservation tests (Task 8.2, Requirement 2.1) ---

    #[test]
    fn test_get_hostname_for_ip_returns_endpoint_from_distributor() {
        // When an IP is present in an IpDistributor, get_hostname_for_ip should
        // return the endpoint hostname associated with that distributor.
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
        // When an IP is in endpoint_overrides but not yet in a distributor,
        // get_hostname_for_ip should still find it via the overrides fallback.
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
        // When an IP is not in any distributor or endpoint_overrides,
        // get_hostname_for_ip should return None.
        let config = crate::config::ConnectionPoolConfig::default();
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();

        let unknown_ip = IpAddr::V4(Ipv4Addr::new(99, 99, 99, 99));
        assert_eq!(manager.get_hostname_for_ip(&unknown_ip), None);
    }

    #[test]
    fn test_ip_address_detection_for_tls_sni() {
        // The CustomHttpsConnector uses IpAddr::from_str to detect whether a URI
        // host is an IP address (from IpDistributor rewriting) or a hostname.
        // This test verifies that detection works correctly.
        use std::net::IpAddr;

        // IP addresses parse successfully — triggers the IP-based TLS SNI path
        assert!("52.92.17.224".parse::<IpAddr>().is_ok());
        assert!("10.0.1.100".parse::<IpAddr>().is_ok());
        assert!("2600:1fa0:4a42::".parse::<IpAddr>().is_ok()); // IPv6

        // Hostnames fail to parse — uses hostname directly for TLS SNI
        assert!("s3.amazonaws.com".parse::<IpAddr>().is_err());
        assert!("s3.eu-west-1.amazonaws.com".parse::<IpAddr>().is_err());
        assert!("my-bucket.s3.amazonaws.com".parse::<IpAddr>().is_err());
    }

    // --- Observability tests (Task 10.3, Requirements 6.1, 6.2, 6.3) ---

    #[test]
    fn test_get_ip_distribution_stats_returns_correct_per_ip_counts() {
        // Create a ConnectionPoolManager with endpoint_overrides so we can
        // lazily initialize a distributor via get_distributed_ip.
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            "s3.eu-west-1.amazonaws.com".to_string(),
            vec!["10.0.0.1".to_string(), "10.0.0.2".to_string(), "10.0.0.3".to_string()],
        );

        let config = crate::config::ConnectionPoolConfig {
            endpoint_overrides: overrides,
            ..Default::default()
        };

        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        // Trigger lazy initialization of the distributor
        let _ = manager.get_distributed_ip("s3.eu-west-1.amazonaws.com");

        let stats = manager.get_ip_distribution_stats();

        // Should have one endpoint entry
        assert_eq!(stats.endpoints.len(), 1);
        let ep = &stats.endpoints[0];
        assert_eq!(ep.endpoint, "s3.eu-west-1.amazonaws.com");
        assert_eq!(ep.total_distributor_ips, 3);
        assert_eq!(ep.ips.len(), 3);

        // Verify all three IPs are present with zero connections (no pool created)
        let ip_strings: Vec<&str> = ep.ips.iter().map(|s| s.ip.as_str()).collect();
        assert!(ip_strings.contains(&"10.0.0.1"));
        assert!(ip_strings.contains(&"10.0.0.2"));
        assert!(ip_strings.contains(&"10.0.0.3"));

        for ip_stat in &ep.ips {
            assert_eq!(ip_stat.active_connections, 0);
            assert_eq!(ip_stat.idle_connections, 0);
        }
    }

    #[test]
    fn test_get_ip_distribution_stats_empty_when_no_distributors() {
        // A default ConnectionPoolManager has no distributors, so stats should be empty.
        let config = crate::config::ConnectionPoolConfig::default();
        let manager = ConnectionPoolManager::new_with_config(config).unwrap();

        let stats = manager.get_ip_distribution_stats();
        assert!(stats.endpoints.is_empty());
    }

    #[test]
    fn test_update_ips_and_remove_ip_with_reason_parameter() {
        // Verify that update_ips and remove_ip accept the reason parameter
        // and the distributor state is correct after the operations.
        let initial_ips = test_ips(3); // 10.0.0.1, 10.0.0.2, 10.0.0.3
        let mut distributor = IpDistributor::new(initial_ips.clone());
        assert_eq!(distributor.ip_count(), 3);

        // update_ips with a "DNS refresh" reason — replaces the set
        let new_ips = vec![
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 1)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 2)),
        ];
        distributor.update_ips(new_ips.clone(), "DNS refresh");
        assert_eq!(distributor.ip_count(), 2);
        assert_eq!(distributor.get_ips(), new_ips);

        // remove_ip with a "health exclusion" reason
        distributor.remove_ip(new_ips[0], "health exclusion");
        assert_eq!(distributor.ip_count(), 1);
        assert_eq!(distributor.get_ips(), vec![new_ips[1]]);

        // After removal, only the remaining IP is selected
        assert_eq!(distributor.select_ip(), Some(new_ips[1]));

        // update_ips with "exclusion expiry" reason — can restore IPs
        let restored_ips = vec![
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 1)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 2)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 3)),
        ];
        distributor.update_ips(restored_ips.clone(), "exclusion expiry");
        assert_eq!(distributor.ip_count(), 3);
        assert_eq!(distributor.get_ips(), restored_ips);
    }

    // --- In-flight request handling during IP removal (Task 11.2, Requirement 3.3) ---

    #[test]
    fn test_remove_ip_is_non_destructive_to_existing_state() {
        // Verifies that remove_ip only affects the distributor's selection set.
        // The IpDistributor has no reference to hyper's connection pool, so
        // removing an IP cannot abort in-flight requests. Hyper manages
        // connection lifecycle independently: existing connections to a removed
        // IP remain in hyper's pool and in-flight requests complete naturally.
        //
        // This test confirms the architectural guarantee by verifying:
        // 1. remove_ip only modifies the Vec<IpAddr> selection set
        // 2. Remaining IPs continue to be selected normally
        // 3. The removed IP is simply absent from future selections
        // 4. No panic, no side effects beyond the selection set change
        let ips = test_ips(4); // 10.0.0.1 .. 10.0.0.4

        let mut distributor = IpDistributor::new(ips.clone());

        // Select a few IPs before removal to advance the counter
        let pre_removal_ip = distributor.select_ip();
        assert!(pre_removal_ip.is_some());

        // Remove an IP — this only removes from the Vec, nothing else
        let removed_ip = ips[2]; // 10.0.0.3
        distributor.remove_ip(removed_ip, "DNS refresh");

        // The distributor still functions for remaining IPs
        assert_eq!(distributor.ip_count(), 3);
        let remaining = distributor.get_ips();
        assert!(!remaining.contains(&removed_ip));
        assert!(remaining.contains(&ips[0]));
        assert!(remaining.contains(&ips[1]));
        assert!(remaining.contains(&ips[3]));

        // Subsequent selections only return remaining IPs
        let selected: Vec<IpAddr> = (0..12).filter_map(|_| distributor.select_ip()).collect();
        assert!(!selected.contains(&removed_ip));
        for ip in &selected {
            assert!(remaining.contains(ip));
        }

        // Distribution across remaining IPs is still even (round-robin)
        let count_per_ip: std::collections::HashMap<IpAddr, usize> =
            selected.iter().fold(std::collections::HashMap::new(), |mut acc, &ip| {
                *acc.entry(ip).or_insert(0) += 1;
                acc
            });
        // 12 selections across 3 IPs = 4 each
        for &ip in &remaining {
            assert_eq!(count_per_ip.get(&ip).copied().unwrap_or(0), 4);
        }
    }

    // --- Graceful degradation tests (Task 11.3, Requirements 7.1, 7.2, 7.3) ---

    #[test]
    fn test_get_distributed_ip_returns_none_when_distributor_has_zero_ips() {
        // Validates: Requirement 7.1
        // Edge case: a distributor was created (e.g., after DNS refresh) but all IPs
        // were subsequently removed (e.g., all marked unhealthy). The distributor
        // exists in the map but has an empty IP set, so get_distributed_ip must
        // return None to trigger the hostname fallback path.
        let config = crate::config::ConnectionPoolConfig {
            ip_distribution_enabled: true,
            ..crate::config::ConnectionPoolConfig::default()
        };
        let mut manager = ConnectionPoolManager::new_with_config(config).unwrap();

        // Manually insert a distributor with IPs, then remove them all
        let ips = vec![
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 1)),
            IpAddr::V4(Ipv4Addr::new(52, 92, 17, 2)),
        ];
        let endpoint = "s3.eu-west-1.amazonaws.com";
        manager
            .ip_distributors
            .insert(endpoint.to_string(), IpDistributor::new(ips.clone()));

        // Verify distributor works before removal
        assert!(manager.get_distributed_ip(endpoint).is_some());

        // Remove all IPs (simulating all IPs marked unhealthy)
        let distributor = manager.ip_distributors.get_mut(endpoint).unwrap();
        distributor.remove_ip(ips[0], "health exclusion");
        distributor.remove_ip(ips[1], "health exclusion");
        assert_eq!(distributor.ip_count(), 0);

        // get_distributed_ip must return None, triggering hostname fallback
        assert_eq!(manager.get_distributed_ip(endpoint), None);
    }



}
