//! Connection Pool Module
//!
//! Manages persistent HTTP connections to S3 endpoints with intelligent load balancing,
//! DNS resolution, health monitoring, and performance optimization.

use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
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
}

impl ConnectionPool {
    /// Get the best performing IP addresses for connection selection
    pub fn get_best_performing_ips(&self, limit: usize) -> Vec<IpAddr> {
        let mut ip_scores: Vec<(IpAddr, f64)> = self
            .health_metrics
            .iter()
            .filter(|(_, metrics)| !metrics.is_excluded)
            .map(|(&ip, metrics)| {
                let score = metrics.success_rate as f64
                    - (metrics.average_latency.as_millis() as f64 / 1000.0) * 0.1
                    - (metrics.consecutive_failures as f64 * 0.2);
                (ip, score)
            })
            .collect();

        ip_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ip_scores
            .into_iter()
            .take(limit)
            .map(|(ip, _)| ip)
            .collect()
    }

    /// Check if DNS refresh is needed
    pub fn needs_dns_refresh(&self) -> bool {
        SystemTime::now()
            .duration_since(self.last_dns_refresh)
            .unwrap_or(Duration::ZERO)
            >= self.dns_refresh_interval
    }

    /// Get connection count for a specific IP
    pub fn get_connection_count(&self, ip: IpAddr) -> usize {
        self.connections.get(&ip).map_or(0, |conns| conns.len())
    }

    /// Get active connection count for a specific IP
    pub fn get_active_connection_count(&self, ip: IpAddr) -> usize {
        self.connections
            .get(&ip)
            .map_or(0, |conns| conns.iter().filter(|c| c.is_active).count())
    }
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

        Ok(())
    }

    /// Monitor connection health for all pools (Requirement 16.3)
    pub async fn monitor_connection_health(&mut self) -> Result<()> {
        let now = SystemTime::now();

        for (endpoint, pool) in self.pools.iter_mut() {
            for (ip, metrics) in pool.health_metrics.iter_mut() {
                // Check if IP should be un-excluded
                if metrics.is_excluded {
                    if let Some(expires_at) = metrics.exclusion_expires_at {
                        if now > expires_at {
                            metrics.is_excluded = false;
                            metrics.exclusion_expires_at = None;
                            metrics.consecutive_failures = 0;
                            info!("Re-enabled IP address {} for endpoint {}", ip, endpoint);
                        }
                    }
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

        Ok(())
    }

    /// Record request metrics for performance tracking
    pub fn record_request_metrics(
        &mut self,
        endpoint: &str,
        ip: IpAddr,
        latency: Duration,
        throughput: Option<f64>,
        success: bool,
    ) -> Result<()> {
        if let Some(pool) = self.pools.get_mut(endpoint) {
            if let Some(metrics) = pool.health_metrics.get_mut(&ip) {
                metrics.total_requests += 1;

                if success {
                    metrics.consecutive_failures = 0;
                } else {
                    metrics.failed_requests += 1;
                    metrics.consecutive_failures += 1;
                    metrics.last_failure = Some(SystemTime::now());

                    // Exclude IP if too many consecutive failures (Requirement 16.7)
                    if metrics.consecutive_failures >= 3 {
                        metrics.is_excluded = true;
                        metrics.exclusion_expires_at =
                            Some(SystemTime::now() + Duration::from_secs(30)); // 30 seconds
                        warn!(
                            "Excluded IP address {} for 30 seconds due to {} consecutive failures",
                            ip, metrics.consecutive_failures
                        );
                    }
                }

                // Update performance metrics
                metrics.performance_metrics.latency_samples.push(latency);
                if let Some(tp) = throughput {
                    metrics.performance_metrics.throughput_samples.push(tp);
                }
                metrics.performance_metrics.sample_count += 1;
                metrics.performance_metrics.last_updated = SystemTime::now();

                // Keep only recent samples (last 100)
                if metrics.performance_metrics.latency_samples.len() > 100 {
                    metrics.performance_metrics.latency_samples.remove(0);
                }
                if metrics.performance_metrics.throughput_samples.len() > 100 {
                    metrics.performance_metrics.throughput_samples.remove(0);
                }
            }
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

    /// Track IP address performance for intelligent selection (Requirement 16.4)
    pub fn track_ip_performance(
        &mut self,
        endpoint: &str,
        ip: IpAddr,
        performance_score: f64,
    ) -> Result<()> {
        if let Some(pool) = self.pools.get_mut(endpoint) {
            if let Some(metrics) = pool.health_metrics.get_mut(&ip) {
                // Update performance tracking
                let current_time = SystemTime::now();
                metrics.performance_metrics.last_updated = current_time;

                // De-prioritize consistently poor performing IPs (Requirement 16.4)
                if performance_score < 0.3 {
                    metrics.consecutive_failures += 1;
                    if metrics.consecutive_failures >= 5 {
                        metrics.is_excluded = true;
                        metrics.exclusion_expires_at =
                            Some(current_time + Duration::from_secs(600)); // 10 minutes
                        warn!("De-prioritized IP address {} due to poor performance", ip);
                    }
                } else {
                    metrics.consecutive_failures = 0;
                }
            }
        }
        Ok(())
    }

    /// Balance load across connections based on current utilization
    pub fn balance_load(&mut self, endpoint: &str) -> Result<()> {
        if let Some(pool) = self.pools.get_mut(endpoint) {
            // Calculate load distribution across IPs
            let mut ip_loads: Vec<(IpAddr, f64)> = Vec::new();

            for (&ip, connections) in &pool.connections {
                let active_count = connections.iter().filter(|c| c.is_active).count();
                let load = active_count as f64 / pool.max_connections_per_ip as f64;
                ip_loads.push((ip, load));
            }

            // Sort by load (ascending)
            ip_loads.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

            // Log load distribution for monitoring
            for (ip, load) in &ip_loads {
                debug!("IP {} load: {:.2}%", ip, load * 100.0);
            }

            // Update preferred status based on load
            for (ip, load) in ip_loads {
                if let Some(dns_cache) = &mut pool.dns_cache {
                    if let Some(ip_info) = dns_cache
                        .ip_addresses
                        .iter_mut()
                        .find(|info| info.ip_address == ip)
                    {
                        ip_info.is_preferred = load < 0.8; // Mark as preferred if under 80% load
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle connection failures and implement exclusion logic (Requirement 16.7)
    pub fn handle_connection_failure(
        &mut self,
        endpoint: &str,
        ip: IpAddr,
        error_type: &str,
    ) -> Result<()> {
        if let Some(pool) = self.pools.get_mut(endpoint) {
            if let Some(metrics) = pool.health_metrics.get_mut(&ip) {
                metrics.failed_requests += 1;
                metrics.consecutive_failures += 1;
                metrics.last_failure = Some(SystemTime::now());

                // Implement progressive exclusion based on failure type and count
                let exclusion_duration = match error_type {
                    "timeout" => Duration::from_secs(60 * metrics.consecutive_failures as u64), // Progressive timeout
                    "connection_refused" => Duration::from_secs(60), // 1 minute for connection refused
                    "dns_failure" => Duration::from_secs(60),        // 1 minute for DNS issues
                    _ => Duration::from_secs(30),                    // 30 seconds for other errors
                };

                // Exclude IP if failures exceed threshold
                if metrics.consecutive_failures >= 3 {
                    metrics.is_excluded = true;
                    metrics.exclusion_expires_at = Some(SystemTime::now() + exclusion_duration);

                    // Close all connections to the failed IP
                    if let Some(connections) = pool.connections.get_mut(&ip) {
                        connections.clear();
                    }

                    warn!(
                        "Excluded IP address {} for {} seconds due to {} consecutive {} failures",
                        ip,
                        exclusion_duration.as_secs(),
                        metrics.consecutive_failures,
                        error_type
                    );
                }
            }
        }
        Ok(())
    }

    /// Periodically retry excluded IP addresses (Requirement 16.7)
    pub async fn retry_excluded_ips(&mut self) -> Result<()> {
        let now = SystemTime::now();

        for (endpoint, pool) in self.pools.iter_mut() {
            let excluded_ips: Vec<IpAddr> = pool
                .health_metrics
                .iter()
                .filter(|(_, metrics)| {
                    metrics.is_excluded
                        && metrics
                            .exclusion_expires_at
                            .map_or(true, |expires| now > expires)
                })
                .map(|(&ip, _)| ip)
                .collect();

            for ip in excluded_ips {
                if let Some(metrics) = pool.health_metrics.get_mut(&ip) {
                    metrics.is_excluded = false;
                    metrics.exclusion_expires_at = None;
                    metrics.consecutive_failures = 0;
                    info!(
                        "Retrying previously excluded IP address {} for endpoint {}",
                        ip, endpoint
                    );
                }
            }
        }

        Ok(())
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

    /// Get connection statistics for all endpoints
    pub fn get_all_connection_statistics(&self) -> Vec<ConnectionStatistics> {
        self.pools
            .keys()
            .filter_map(|endpoint| self.get_connection_statistics(endpoint))
            .collect()
    }

    /// Get DNS refresh count
    pub fn get_dns_refresh_count(&self) -> u64 {
        self.dns_refresh_count
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
