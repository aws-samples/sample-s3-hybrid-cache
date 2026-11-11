//! TCP Proxy Module
//!
//! Provides transparent TCP tunneling for HTTPS connections (default mode).
//! This module handles Layer 3/4 transparent TCP tunneling without TLS termination,
//! preserving end-to-end TLS encryption and certificate validation.

use crate::connection_pool::EndpointOverrides;
use crate::destination_policy::{DestinationPolicy, OwnAddresses};
use crate::{ProxyError, Result};
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::TokioResolver;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// The longest a tunnel spends connecting upstream, over all its addresses and rounds.
/// Thirty seconds is the passthrough's worst case before it tried every address.
pub(crate) const TUNNEL_CONNECT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// TCP Proxy handler for transparent HTTPS passthrough
pub struct TcpProxy {
    listen_addr: SocketAddr,
    resolver: TokioResolver,
    /// Parsed endpoint overrides (exact + suffix) for PrivateLink etc.
    overrides: EndpointOverrides,
    /// Destination policy for CONNECT/SNI restriction
    destination_policy: Arc<DestinationPolicy>,
    /// Addresses a tunnel is never relayed to, because they lead back to a proxy.
    own_addresses: Arc<OwnAddresses>,
}

impl TcpProxy {
    /// Create a new TCP proxy instance with an external DNS resolver that uses the
    /// configured address-family strategy for upstream lookups.
    pub fn new_with_dns_ip_strategy(
        listen_addr: SocketAddr,
        overrides: EndpointOverrides,
        destination_policy: Arc<DestinationPolicy>,
        dns_ip_strategy: crate::config::DnsIpStrategy,
        own_addresses: Arc<OwnAddresses>,
    ) -> Self {
        use hickory_resolver::config::{CLOUDFLARE, GOOGLE};
        // Use external DNS servers to bypass /etc/hosts (same as connection pool)
        let mut config = ResolverConfig::default();
        for ns in GOOGLE.udp_and_tcp() {
            config.add_name_server(ns);
        }
        for ns in CLOUDFLARE.udp_and_tcp() {
            config.add_name_server(ns);
        }

        let resolver = crate::connection_pool::build_resolver(config, dns_ip_strategy)
            .expect("Failed to build DNS resolver");

        if !overrides.is_empty() {
            info!(
                "TCP proxy initialized with {} exact + {} suffix endpoint override(s)",
                overrides.exact_count(),
                overrides.suffix_count()
            );
        }

        Self {
            listen_addr,
            resolver,
            overrides,
            destination_policy,
            own_addresses,
        }
    }

    /// Start the TCP proxy server
    pub async fn start(&self, mut shutdown_signal: crate::shutdown::ShutdownSignal) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let mut accept_errors = crate::accept_policy::AcceptErrorLog::new();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((client_stream, client_addr)) => {
                            debug!("TCP connection from {}", client_addr);

                            // Set socket options for better performance and reliability
                            if let Err(e) = Self::configure_socket(&client_stream) {
                                warn!(
                                    "Failed to configure client socket for {}: {}",
                                    client_addr, e
                                );
                            }

                            let resolver = self.resolver.clone();
                            let overrides = self.overrides.clone();
                            let policy = self.destination_policy.clone();
                            let own_addresses = Arc::clone(&self.own_addresses);
                            tokio::spawn(async move {
                                if let Err(e) =
                                    Self::handle_connection(client_stream, client_addr, resolver, overrides, policy, own_addresses).await
                                {
                                    // Check if this is a client-initiated cancellation or connection error
                                    let err_str = e.to_string();
                                    if err_str.contains("connection closed")
                                        || err_str.contains("broken pipe")
                                        || err_str.contains("reset by peer")
                                    {
                                        debug!("Client disconnected from {}: {}", client_addr, e);
                                    } else {
                                        error!("TCP proxy error for {}: {}", client_addr, e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            accept_errors.on_accept_error("TCP proxy", &e).await;
                        }
                    }
                }
                _ = shutdown_signal.wait_for_shutdown() => {
                    info!("TCP proxy received shutdown signal, stopping accept loop");
                    break;
                }
            }
        }

        info!("TCP proxy stopped");
        Ok(())
    }

    /// Handle a single TCP connection
    async fn handle_connection(
        client_stream: TcpStream,
        client_addr: SocketAddr,
        resolver: TokioResolver,
        overrides: EndpointOverrides,
        policy: Arc<DestinationPolicy>,
        own_addresses: Arc<OwnAddresses>,
    ) -> Result<()> {
        // Set up connection cleanup on error
        let cleanup_connection = || {
            debug!("Cleaning up connection for client {}", client_addr);
            // Connection will be automatically closed when client_stream is dropped
        };

        // Read the initial data to extract the SNI (Server Name Indication) from TLS handshake
        // This allows us to determine the target S3 endpoint
        let mut buffer = [0u8; 4096];
        let bytes_read = match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client_stream.peek(&mut buffer),
        )
        .await
        {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                error!("Failed to peek client data from {}: {}", client_addr, e);
                cleanup_connection();
                return Err(ProxyError::ConnectionError(format!(
                    "Failed to peek client data: {}",
                    e
                )));
            }
            Err(_) => {
                error!("Timeout waiting for client data from {}", client_addr);
                cleanup_connection();
                return Err(ProxyError::TimeoutError(
                    "Timeout waiting for client data".to_string(),
                ));
            }
        };

        if bytes_read == 0 {
            warn!("No data received from client {}", client_addr);
            cleanup_connection();
            return Ok(());
        }

        // Extract SNI from TLS handshake to determine target endpoint
        let target_host = match Self::extract_sni_from_tls_handshake(&buffer[..bytes_read]) {
            Some(host) => host,
            None => {
                error!(
                    "Failed to extract SNI from TLS handshake for {}",
                    client_addr
                );
                cleanup_connection();
                return Err(ProxyError::ConnectionError(
                    "Failed to extract SNI from TLS handshake".to_string(),
                ));
            }
        };

        debug!(
            "Extracted target host: {} for client {}",
            target_host, client_addr
        );

        // Establish TCP tunnel to the target S3 endpoint with error handling
        match Self::establish_tcp_tunnel(
            client_stream,
            client_addr,
            &target_host,
            resolver,
            overrides,
            policy,
            &own_addresses,
            443,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                error!(
                    "TCP tunnel failed for {} -> {}: {}",
                    client_addr, target_host, e
                );
                cleanup_connection();
                Err(e)
            }
        }
    }

    /// Extract Server Name Indication (SNI) from TLS handshake
    fn extract_sni_from_tls_handshake(data: &[u8]) -> Option<String> {
        // Basic TLS handshake parsing to extract SNI
        // TLS record format: [type(1)] [version(2)] [length(2)] [handshake_data...]
        if data.len() < 5 {
            return None;
        }

        // Check if this is a TLS handshake record (type = 0x16)
        if data[0] != 0x16 {
            return None;
        }

        // Skip TLS record header (5 bytes) to get to handshake message
        let handshake_data = &data[5..];
        if handshake_data.len() < 4 {
            return None;
        }

        // Check if this is a ClientHello message (type = 0x01)
        if handshake_data[0] != 0x01 {
            return None;
        }

        // Parse ClientHello to find SNI extension
        // This is a simplified parser - in production, you might want to use a proper TLS library
        Self::parse_client_hello_for_sni(handshake_data)
    }

    /// Parse ClientHello message to extract SNI
    fn parse_client_hello_for_sni(data: &[u8]) -> Option<String> {
        // ClientHello structure:
        // - Handshake type (1 byte) = 0x01
        // - Length (3 bytes)
        // - Version (2 bytes)
        // - Random (32 bytes)
        // - Session ID length (1 byte) + Session ID
        // - Cipher suites length (2 bytes) + Cipher suites
        // - Compression methods length (1 byte) + Compression methods
        // - Extensions length (2 bytes) + Extensions

        if data.len() < 38 {
            return None;
        }

        let mut offset = 4; // Skip handshake type and length
        offset += 2; // Skip version
        offset += 32; // Skip random

        // Skip session ID
        if offset >= data.len() {
            return None;
        }
        let session_id_len = data[offset] as usize;
        offset += 1 + session_id_len;

        // Skip cipher suites
        if offset + 2 > data.len() {
            return None;
        }
        let cipher_suites_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2 + cipher_suites_len;

        // Skip compression methods
        if offset >= data.len() {
            return None;
        }
        let compression_methods_len = data[offset] as usize;
        offset += 1 + compression_methods_len;

        // Parse extensions
        if offset + 2 > data.len() {
            return None;
        }
        let extensions_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        let extensions_end = offset + extensions_len;
        if extensions_end > data.len() {
            return None;
        }

        // Look for SNI extension (type = 0x0000)
        while offset + 4 <= extensions_end {
            let ext_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
            let ext_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
            offset += 4;

            if ext_type == 0x0000 && offset + ext_len <= extensions_end {
                // Found SNI extension, parse server name list
                return Self::parse_sni_extension(&data[offset..offset + ext_len]);
            }

            offset += ext_len;
        }

        None
    }

    /// Parse SNI extension to extract server name
    fn parse_sni_extension(data: &[u8]) -> Option<String> {
        // SNI extension format:
        // - Server name list length (2 bytes)
        // - Server name type (1 byte) = 0x00 for hostname
        // - Server name length (2 bytes)
        // - Server name (variable length)

        if data.len() < 5 {
            return None;
        }

        let list_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        if list_len + 2 > data.len() {
            return None;
        }

        let name_type = data[2];
        if name_type != 0x00 {
            return None; // Only hostname type supported
        }

        let name_len = u16::from_be_bytes([data[3], data[4]]) as usize;
        if 5 + name_len > data.len() {
            return None;
        }

        let hostname = String::from_utf8(data[5..5 + name_len].to_vec()).ok()?;
        // A control character or a quote can never be part of a resolvable host name, and
        // the name is printed in many log lines, so a CR/LF here could forge log entries.
        // Treat it as no SNI: the connection is closed, as it would be at DNS resolution.
        if hostname.chars().any(|c| c.is_control() || c == '"') {
            return None;
        }
        Some(hostname)
    }

    /// Establish TCP tunnel to target endpoint
    // Every argument is per-connection state handed over by the accept loop, which is the
    // only caller outside tests; a struct to carry them would be a type with one user.
    #[allow(clippy::too_many_arguments)]
    async fn establish_tcp_tunnel(
        mut client_stream: TcpStream,
        client_addr: SocketAddr,
        target_host: &str,
        resolver: TokioResolver,
        overrides: EndpointOverrides,
        policy: Arc<DestinationPolicy>,
        own_addresses: &OwnAddresses,
        target_port: u16,
    ) -> Result<()> {
        debug!(
            "Resolving {} using external DNS (bypassing /etc/hosts)",
            target_host
        );
        // Determine connect IPs: endpoint overrides take priority, otherwise
        // policy.check() resolves + validates in one step (no second DNS lookup).
        let ip_addresses: Vec<IpAddr> = if let Some(ips) = overrides.resolve(target_host) {
            // Override IPs still need policy validation (prohibited-range check)
            match policy.classify_ips(target_host, ips) {
                Ok(allowed) => {
                    // Per connection, so `debug!`: every HTTPS passthrough connection to an
                    // overridden host passes through here.
                    debug!("Using endpoint override for {}: {:?}", target_host, allowed);
                    allowed
                }
                Err(reason) => {
                    warn!(
                        hostname = %target_host,
                        port = target_port,
                        reason = %reason,
                        "SNI passthrough endpoint-override IPs rejected by policy"
                    );
                    let _ = client_stream.shutdown().await;
                    return Ok(());
                }
            }
        } else {
            // policy.check() resolves DNS and validates IPs atomically —
            // reuse the returned IPs to avoid a TOCTOU re-resolution.
            match policy.check(target_host, target_port, &resolver).await {
                Ok(allowed_ips) => allowed_ips,
                Err(reason) => {
                    warn!(
                        hostname = %target_host,
                        port = target_port,
                        reason = %reason,
                        "SNI passthrough destination rejected by policy"
                    );
                    let _ = client_stream.shutdown().await;
                    return Ok(());
                }
            }
        };

        // Never relay to an address that leads back to a proxy, carved-out override
        // addresses included: the tunnel would arrive at a passthrough again.
        let ip_addresses = match own_addresses.filter_tunnel_targets(ip_addresses) {
            Ok(rest) => rest,
            Err(refused) => {
                own_addresses.warn_refused("SNI passthrough", target_host, target_port, &refused);
                let _ = client_stream.shutdown().await;
                return Ok(());
            }
        };

        if ip_addresses.is_empty() {
            error!("No IP addresses found for {}", target_host);
            drop(client_stream);
            return Err(ProxyError::ConnectionError(format!(
                "No IP addresses found for {}",
                target_host
            )));
        }

        debug!(
            "Resolved {} to {} IP addresses: {:?}",
            target_host,
            ip_addresses.len(),
            ip_addresses
        );

        debug!(
            "Establishing tunnel from {} to {} ({:?} port {})",
            client_addr, target_host, ip_addresses, target_port
        );
        // Every address the policy allowed, in resolver order, until one connects.
        let server_stream = match Self::connect_first_reachable(
            &ip_addresses,
            target_port,
            3,
            std::time::Duration::from_secs(10),
            TUNNEL_CONNECT_DEADLINE,
        )
        .await
        {
            Ok((stream, _ip)) => stream,
            Err(e) => {
                error!(
                    "Failed to connect to target {} ({:?}) for client {} after retries: {}",
                    target_host, ip_addresses, client_addr, e
                );
                // Close client connection gracefully as per requirement 14.6
                drop(client_stream);
                return Err(ProxyError::ConnectionError(format!(
                    "Failed to connect to target {}: {}",
                    target_host, e
                )));
            }
        };

        debug!(
            "TCP tunnel established: {} <-> {}:443",
            client_addr, target_host
        );

        // Start bidirectional forwarding with error handling
        let target_addr_str = format!("{}:{}", target_host, target_port);
        let start_time = std::time::Instant::now();
        match Self::forward_tcp_traffic(client_stream, server_stream, client_addr, &target_addr_str)
            .await
        {
            Ok((tx_bytes, rx_bytes)) => {
                let duration = start_time.elapsed();
                info!(
                    "TCP tunnel completed: {} <-> {} | duration: {:.1}s | rx: {}, tx: {}",
                    Self::format_addr(client_addr),
                    target_addr_str,
                    duration.as_secs_f64(),
                    Self::format_bytes(rx_bytes),
                    Self::format_bytes(tx_bytes)
                );
                Ok(())
            }
            Err(e) => {
                let duration = start_time.elapsed();
                error!(
                    "TCP tunnel error for {} <-> {} after {:.1}s: {}",
                    Self::format_addr(client_addr),
                    target_addr_str,
                    duration.as_secs_f64(),
                    e
                );
                Err(e)
            }
        }
    }

    /// Connect to the first of `addresses` that accepts, trying them in order.
    ///
    /// Each round tries every address once, each attempt bounded by `attempt_timeout`.
    /// Up to `rounds` rounds run, with a backoff of 100 ms after the first and 200 ms
    /// after the second, doubling for each later round. The whole call stops at
    /// `deadline`, so the time spent no longer grows with the number of addresses.
    /// Callers pass only the addresses the destination policy allowed, so no other
    /// address is dialled. Returns the stream and the address it reached.
    pub(crate) async fn connect_first_reachable(
        addresses: &[IpAddr],
        port: u16,
        rounds: u32,
        attempt_timeout: std::time::Duration,
        deadline: std::time::Duration,
    ) -> Result<(TcpStream, IpAddr)> {
        let started = tokio::time::Instant::now();
        let mut last_error: Option<String> = None;
        'rounds: for round in 1..=rounds {
            for &ip in addresses {
                let remaining = deadline.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    last_error = Some(format!(
                        "connect deadline of {:?} reached (last: {})",
                        deadline,
                        last_error.as_deref().unwrap_or("none")
                    ));
                    break 'rounds;
                }
                let addr = SocketAddr::new(ip, port);
                match tokio::time::timeout(attempt_timeout.min(remaining), TcpStream::connect(addr))
                    .await
                {
                    Ok(Ok(stream)) => {
                        if last_error.is_some() {
                            debug!("Connected to {} after earlier failures", addr);
                        }
                        return Ok((stream, ip));
                    }
                    Ok(Err(e)) => {
                        warn!("Connection attempt {} to {} failed: {}", round, addr, e);
                        last_error = Some(format!("{}: {}", addr, e));
                    }
                    Err(_) => {
                        warn!("Connection attempt {} to {} timed out", round, addr);
                        last_error = Some(format!("{}: connection timeout", addr));
                    }
                }
            }
            if round < rounds {
                let delay = std::time::Duration::from_millis(100u64 << (round - 1).min(10));
                tokio::time::sleep(delay.min(deadline.saturating_sub(started.elapsed()))).await;
            }
        }
        Err(ProxyError::ConnectionError(format!(
            "Failed to connect to any of {} address(es) on port {} after {} round(s): {}",
            addresses.len(),
            port,
            rounds,
            last_error.unwrap_or_else(|| "no addresses".to_string())
        )))
    }
    /// Forward TCP traffic bidirectionally between client and server
    /// Returns (tx_bytes, rx_bytes) where tx is client->server and rx is server->client
    async fn forward_tcp_traffic(
        client_stream: TcpStream,
        server_stream: TcpStream,
        client_addr: SocketAddr,
        target_addr: &str,
    ) -> Result<(u64, u64)> {
        let (mut client_read, mut client_write) = client_stream.into_split();
        let (mut server_read, mut server_write) = server_stream.into_split();

        // Forward data from client to server
        let client_to_server = async {
            let mut buffer = [0u8; 8192];
            let mut total_bytes = 0u64;

            loop {
                match client_read.read(&mut buffer).await {
                    Ok(0) => {
                        debug!(
                            "Client {} closed connection (sent {} bytes total)",
                            client_addr, total_bytes
                        );
                        // Gracefully shutdown server write half
                        let _ = server_write.shutdown().await;
                        break;
                    }
                    Ok(n) => {
                        match server_write.write_all(&buffer[..n]).await {
                            Ok(()) => {
                                total_bytes += n as u64;
                                debug!(
                                    "Forwarded {} bytes from client {} to server {} (total: {})",
                                    n, client_addr, target_addr, total_bytes
                                );
                            }
                            Err(e) => {
                                // Connection reset/broken pipe is normal when connections close abruptly
                                if e.kind() == std::io::ErrorKind::ConnectionReset
                                    || e.kind() == std::io::ErrorKind::BrokenPipe
                                {
                                    debug!("Connection to server {} closed while writing from client {}: {}", target_addr, client_addr, e);
                                } else {
                                    error!(
                                        "Failed to write to server {} from client {}: {}",
                                        target_addr, client_addr, e
                                    );
                                }
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        // Connection reset by peer is normal when clients close connections abruptly
                        if e.kind() == std::io::ErrorKind::ConnectionReset {
                            debug!("Client {} closed connection: {}", client_addr, e);
                        } else {
                            error!("Failed to read from client {}: {}", client_addr, e);
                        }
                        break;
                    }
                }
            }

            total_bytes
        };

        // Forward data from server to client
        let server_to_client = async {
            let mut buffer = [0u8; 8192];
            let mut total_bytes = 0u64;

            loop {
                match server_read.read(&mut buffer).await {
                    Ok(0) => {
                        debug!(
                            "Server {} closed connection (sent {} bytes total)",
                            target_addr, total_bytes
                        );
                        // Gracefully shutdown client write half
                        let _ = client_write.shutdown().await;
                        break;
                    }
                    Ok(n) => match client_write.write_all(&buffer[..n]).await {
                        Ok(()) => {
                            total_bytes += n as u64;
                            debug!(
                                "Forwarded {} bytes from server {} to client {} (total: {})",
                                n, target_addr, client_addr, total_bytes
                            );
                        }
                        Err(e) => {
                            error!(
                                "Failed to write to client {} from server {}: {}",
                                client_addr, target_addr, e
                            );
                            break;
                        }
                    },
                    Err(e) => {
                        // Connection reset by peer is normal when servers close connections abruptly
                        if e.kind() == std::io::ErrorKind::ConnectionReset {
                            debug!("Server {} closed connection: {}", target_addr, e);
                        } else {
                            error!("Failed to read from server {}: {}", target_addr, e);
                        }
                        break;
                    }
                }
            }

            total_bytes
        };

        // Run both forwarding tasks concurrently and wait for both to complete
        let (tx_bytes, rx_bytes) = tokio::join!(client_to_server, server_to_client);

        debug!(
            "Both forwarding directions completed for {} <-> {}: tx={}, rx={}",
            client_addr, target_addr, tx_bytes, rx_bytes
        );

        Ok((tx_bytes, rx_bytes))
    }

    /// Format socket address, simplifying IPv6-mapped IPv4 addresses
    fn format_addr(addr: SocketAddr) -> String {
        match addr {
            SocketAddr::V6(v6) if v6.ip().to_ipv4_mapped().is_some() => {
                // Convert ::ffff:127.0.0.1 to 127.0.0.1
                format!("{}:{}", v6.ip().to_ipv4_mapped().unwrap(), v6.port())
            }
            _ => addr.to_string(),
        }
    }

    /// Format bytes in human-readable format
    fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;

        if bytes >= GB {
            format!("{:.2}GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2}MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.1}KB", bytes as f64 / KB as f64)
        } else {
            format!("{}B", bytes)
        }
    }

    /// Configure socket options for better performance and reliability
    fn configure_socket(stream: &TcpStream) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = stream.as_raw_fd();

        // Enable TCP keepalive to detect dead connections
        unsafe {
            let keepalive: libc::c_int = 1;
            if libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &keepalive as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            ) != 0
            {
                return Err(ProxyError::ConnectionError(
                    "Failed to set SO_KEEPALIVE".to_string(),
                ));
            }

            // Set TCP keepalive parameters (macOS uses TCP_KEEPALIVE instead of TCP_KEEPIDLE)
            #[cfg(target_os = "macos")]
            {
                let keepalive_time: libc::c_int = 60; // Start keepalive after 60 seconds of inactivity
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPALIVE,
                    &keepalive_time as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                ) != 0
                {
                    return Err(ProxyError::ConnectionError(
                        "Failed to set TCP_KEEPALIVE".to_string(),
                    ));
                }
            }

            #[cfg(target_os = "linux")]
            {
                let keepidle: libc::c_int = 60; // Start keepalive after 60 seconds of inactivity
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPIDLE,
                    &keepidle as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                ) != 0
                {
                    return Err(ProxyError::ConnectionError(
                        "Failed to set TCP_KEEPIDLE".to_string(),
                    ));
                }

                let keepintvl: libc::c_int = 10; // Send keepalive probes every 10 seconds
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPINTVL,
                    &keepintvl as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                ) != 0
                {
                    return Err(ProxyError::ConnectionError(
                        "Failed to set TCP_KEEPINTVL".to_string(),
                    ));
                }

                let keepcnt: libc::c_int = 3; // Close connection after 3 failed probes
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPCNT,
                    &keepcnt as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                ) != 0
                {
                    return Err(ProxyError::ConnectionError(
                        "Failed to set TCP_KEEPCNT".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal but well-formed TLS ClientHello record carrying a
    /// single SNI hostname extension. Used to exercise the SNI parser without
    /// a real TLS stack.
    fn build_client_hello_with_sni(hostname: &str) -> Vec<u8> {
        let host_bytes = hostname.as_bytes();

        // SNI extension body: server name list len (2) + type (1) + name len (2) + name
        let mut sni_ext_body = Vec::new();
        let list_len = 1 + 2 + host_bytes.len();
        sni_ext_body.extend_from_slice(&(list_len as u16).to_be_bytes());
        sni_ext_body.push(0x00); // hostname type
        sni_ext_body.extend_from_slice(&(host_bytes.len() as u16).to_be_bytes());
        sni_ext_body.extend_from_slice(host_bytes);

        // Extension: type 0x0000 (server_name) + length (2) + body
        let mut extension = Vec::new();
        extension.extend_from_slice(&0x0000u16.to_be_bytes());
        extension.extend_from_slice(&(sni_ext_body.len() as u16).to_be_bytes());
        extension.extend_from_slice(&sni_ext_body);

        // Extensions block: total length (2) + extensions
        let mut extensions = Vec::new();
        extensions.extend_from_slice(&(extension.len() as u16).to_be_bytes());
        extensions.extend_from_slice(&extension);

        // ClientHello body
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]); // legacy_version TLS 1.2
        body.extend_from_slice(&[0u8; 32]); // random
        body.push(0x00); // session id length = 0
        body.extend_from_slice(&0x0002u16.to_be_bytes()); // cipher suites length
        body.extend_from_slice(&[0x00, 0x2f]); // one cipher suite
        body.push(0x01); // compression methods length
        body.push(0x00); // null compression
        body.extend_from_slice(&extensions);

        // Handshake message: type (1) + length (3) + body
        let mut handshake = Vec::new();
        handshake.push(0x01); // ClientHello
        let body_len = body.len();
        handshake.push(((body_len >> 16) & 0xff) as u8);
        handshake.push(((body_len >> 8) & 0xff) as u8);
        handshake.push((body_len & 0xff) as u8);
        handshake.extend_from_slice(&body);

        // TLS record: type (1) + version (2) + length (2) + handshake
        let mut record = Vec::new();
        record.push(0x16); // handshake content type
        record.extend_from_slice(&[0x03, 0x01]); // record version
        record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
        record.extend_from_slice(&handshake);

        record
    }

    #[test]
    fn test_extract_sni_valid_client_hello() {
        let record = build_client_hello_with_sni("my-bucket.s3.us-west-2.amazonaws.com");
        let sni = TcpProxy::extract_sni_from_tls_handshake(&record);
        assert_eq!(sni.as_deref(), Some("my-bucket.s3.us-west-2.amazonaws.com"));
    }

    #[test]
    fn test_extract_sni_short_hostname() {
        let record = build_client_hello_with_sni("s3.amazonaws.com");
        let sni = TcpProxy::extract_sni_from_tls_handshake(&record);
        assert_eq!(sni.as_deref(), Some("s3.amazonaws.com"));
    }

    #[test]
    fn test_extract_sni_rejects_non_handshake_record() {
        // Content type 0x17 = application_data, not handshake (0x16)
        let data = [0x17u8, 0x03, 0x03, 0x00, 0x05, 1, 2, 3, 4, 5];
        assert_eq!(TcpProxy::extract_sni_from_tls_handshake(&data), None);
    }

    #[test]
    fn test_extract_sni_rejects_control_characters_and_quotes() {
        // A name with CR/LF would otherwise reach every log line that prints the host.
        for name in [
            "evil.example\r\n2026-10-19T00:00:00Z  INFO forged line",
            "evil.example\u{85}next",
            "evil\".example",
        ] {
            let record = build_client_hello_with_sni(name);
            assert_eq!(
                TcpProxy::extract_sni_from_tls_handshake(&record),
                None,
                "SNI {name:?} must be rejected"
            );
        }
        let record = build_client_hello_with_sni("s3.us-west-2.amazonaws.com");
        assert_eq!(
            TcpProxy::extract_sni_from_tls_handshake(&record).as_deref(),
            Some("s3.us-west-2.amazonaws.com")
        );
    }
    #[test]
    fn test_extract_sni_rejects_too_short() {
        assert_eq!(
            TcpProxy::extract_sni_from_tls_handshake(&[0x16, 0x03]),
            None
        );
        assert_eq!(TcpProxy::extract_sni_from_tls_handshake(&[]), None);
    }

    #[test]
    fn test_extract_sni_rejects_non_client_hello() {
        // Valid TLS record header but handshake type 0x02 (ServerHello)
        let mut data = vec![0x16, 0x03, 0x01, 0x00, 0x04];
        data.extend_from_slice(&[0x02, 0x00, 0x00, 0x00]); // ServerHello, len 0
        assert_eq!(TcpProxy::extract_sni_from_tls_handshake(&data), None);
    }

    #[test]
    fn test_extract_sni_truncated_handshake_body() {
        // Well-formed header claiming a ClientHello, but the body is truncated
        // before the random/extensions, so parsing must bail out with None.
        let mut data = vec![0x16, 0x03, 0x01, 0x00, 0x06];
        data.extend_from_slice(&[0x01, 0x00, 0x00, 0x02, 0x03, 0x03]); // ClientHello, 2-byte body
        assert_eq!(TcpProxy::extract_sni_from_tls_handshake(&data), None);
    }

    #[test]
    fn test_parse_sni_extension_rejects_non_hostname_type() {
        // list_len covers type+len+name; name type 0x01 is not hostname (0x00)
        let body = [0x00, 0x04, 0x01, 0x00, 0x01, b'x'];
        assert_eq!(TcpProxy::parse_sni_extension(&body), None);
    }

    #[test]
    fn test_parse_sni_extension_rejects_truncated() {
        assert_eq!(TcpProxy::parse_sni_extension(&[0x00, 0x04]), None);
    }

    #[test]
    fn test_format_bytes_units() {
        assert_eq!(TcpProxy::format_bytes(0), "0B");
        assert_eq!(TcpProxy::format_bytes(512), "512B");
        assert_eq!(TcpProxy::format_bytes(1024), "1.0KB");
        assert_eq!(TcpProxy::format_bytes(1536), "1.5KB");
        assert_eq!(TcpProxy::format_bytes(1024 * 1024), "1.00MB");
        assert_eq!(TcpProxy::format_bytes(1024 * 1024 * 1024), "1.00GB");
    }

    #[test]
    fn test_format_bytes_boundary() {
        // Just below 1 KB stays in bytes
        assert_eq!(TcpProxy::format_bytes(1023), "1023B");
        // Just below 1 MB stays in KB
        assert_eq!(TcpProxy::format_bytes(1024 * 1024 - 1), "1024.0KB");
    }

    #[test]
    fn test_format_addr_ipv4() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        assert_eq!(TcpProxy::format_addr(addr), "127.0.0.1:8080");
    }

    #[test]
    fn test_format_addr_ipv4_mapped_ipv6() {
        // ::ffff:127.0.0.1 should be simplified to the IPv4 form
        let addr: SocketAddr = "[::ffff:127.0.0.1]:443".parse().unwrap();
        assert_eq!(TcpProxy::format_addr(addr), "127.0.0.1:443");
    }

    #[test]
    fn test_format_addr_native_ipv6() {
        let addr: SocketAddr = "[2606:4700:4700::1111]:443".parse().unwrap();
        assert_eq!(TcpProxy::format_addr(addr), "[2606:4700:4700::1111]:443");
    }

    /// Verify that the DNS TOCTOU fix works correctly:
    /// - policy.check() resolves DNS and returns validated IPs in one step
    /// - The IPs returned by policy.check/classify_ips ARE the connect set
    /// - No second DNS resolution is needed
    #[test]
    fn test_policy_validated_ips_used_directly_no_reresolution() {
        use crate::destination_policy::DestinationPolicy;
        use std::collections::HashSet;

        // Policy with no allowlist, port 443
        let policy = DestinationPolicy::new(443, None, HashSet::new());

        // Simulate what policy.check() returns after DNS resolution:
        // a mix of public IPs (allowed) and one prohibited IP (filtered out).
        let resolved_ips: Vec<IpAddr> = vec![
            "52.94.236.248".parse().unwrap(), // public (AWS)
            "127.0.0.1".parse().unwrap(),     // prohibited (loopback)
            "8.8.8.8".parse().unwrap(),       // public (Google DNS)
        ];

        let result = policy.classify_ips("s3.us-west-2.amazonaws.com", &resolved_ips);
        let allowed_ips = result.expect("classify_ips should succeed with public IPs present");

        // Key assertion: the returned IPs are exactly the safe-to-connect set.
        // The prohibited 127.0.0.1 must NOT be in the result.
        assert_eq!(allowed_ips.len(), 2);
        assert!(allowed_ips.contains(&"52.94.236.248".parse::<IpAddr>().unwrap()));
        assert!(allowed_ips.contains(&"8.8.8.8".parse::<IpAddr>().unwrap()));
        assert!(!allowed_ips.contains(&"127.0.0.1".parse::<IpAddr>().unwrap()));

        // This proves: if establish_tcp_tunnel uses the return value of
        // policy.check() directly (as the fix does), the connect candidate set
        // is exactly the policy-validated set — no re-resolution occurs.
    }

    /// Verify that endpoint override IPs are validated through the policy
    /// before use — prohibited override IPs without a carve-out are rejected.
    #[test]
    fn test_override_ips_validated_against_policy() {
        use crate::destination_policy::DestinationPolicy;
        use std::collections::HashSet;

        // Policy with no endpoint_override_ips carve-out
        let policy = DestinationPolicy::new(443, None, HashSet::new());

        // A loopback IP (prohibited) as an override should be rejected
        let prohibited_ips: Vec<IpAddr> = vec!["127.0.0.1".parse().unwrap()];
        let result = policy.classify_ips("example.com", &prohibited_ips);
        assert!(
            result.is_err(),
            "Prohibited override IPs without carve-out must be rejected"
        );

        // A public IP as an override should be allowed
        let public_ips: Vec<IpAddr> = vec!["8.8.8.8".parse().unwrap()];
        let result = policy.classify_ips("example.com", &public_ips);
        let allowed = result.expect("Public override IPs should be allowed");
        assert_eq!(allowed, public_ips);
    }

    /// Verify that the endpoint override carve-out allows prohibited IPs
    /// that are explicitly listed in endpoint_override_ips.
    #[test]
    fn test_override_ips_carveout_allows_prohibited() {
        use crate::destination_policy::DestinationPolicy;
        use std::collections::HashSet;

        // Include 10.0.0.1 in the carve-out set (e.g., PrivateLink ENI)
        let mut carveout: HashSet<IpAddr> = HashSet::new();
        carveout.insert("10.0.0.1".parse().unwrap());

        let policy = DestinationPolicy::new(443, None, carveout);

        // 10.0.0.1 is in a prohibited range but has the carve-out
        let override_ips: Vec<IpAddr> = vec!["10.0.0.1".parse().unwrap()];
        let result = policy.classify_ips("vpce-endpoint.amazonaws.com", &override_ips);
        let allowed = result.expect("Carved-out override IP should be allowed");
        assert_eq!(allowed, override_ips);
    }
}

#[cfg(test)]
mod address_order_tests {
    use super::*;
    use crate::destination_policy::DestinationPolicy;
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;

    const HOST: &str = "passthrough-order.test";

    /// Start one passthrough tunnel for `HOST`, whose endpoint override lists
    /// `addresses`, under a policy whose carve-out holds `carved_out`, with the
    /// upstream on `port`. Returns the client end and the tunnel task.
    async fn start_passthrough(
        addresses: &[&str],
        carved_out: &[&str],
        port: u16,
        own: OwnAddresses,
    ) -> (TcpStream, tokio::task::JoinHandle<Result<()>>) {
        let overrides = EndpointOverrides::from_config(&HashMap::from([(
            HOST.to_string(),
            addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        )]));
        let carve_out: HashSet<IpAddr> = carved_out.iter().map(|a| a.parse().unwrap()).collect();
        let policy = Arc::new(DestinationPolicy::new(port, None, carve_out));
        let resolver = crate::connection_pool::build_resolver(
            ResolverConfig::default(),
            crate::config::DnsIpStrategy::Ipv6AndIpv4,
        )
        .unwrap();
        let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(front.local_addr().unwrap())
            .await
            .unwrap();
        let (accepted, peer) = front.accept().await.unwrap();
        let tunnel = tokio::spawn(async move {
            TcpProxy::establish_tcp_tunnel(
                accepted, peer, HOST, resolver, overrides, policy, &own, port,
            )
            .await
        });
        (client, tunnel)
    }

    /// The passthrough moves on to the next allowed address when the first refuses.
    /// The first is `::1`, where nothing listens on this port; on a host with no IPv6
    /// loopback that connect fails at once as well. The second is 127.0.0.1, where
    /// the upstream listens, and the client's bytes must arrive through it.
    #[tokio::test]
    async fn the_passthrough_connects_through_the_second_address_when_the_first_refuses() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let (mut client, tunnel) = start_passthrough(
            &["::1", "127.0.0.1"],
            &["::1", "127.0.0.1"],
            port,
            OwnAddresses::listed_only(&[]),
        )
        .await;
        let (mut server, _) = tokio::time::timeout(Duration::from_secs(5), upstream.accept())
            .await
            .expect("the tunnel never reached the second address")
            .unwrap();
        server.write_all(b"pong").await.unwrap();
        let mut buf = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut buf))
            .await
            .expect("no byte came back through the tunnel")
            .unwrap();
        assert_eq!(&buf, b"pong");
        drop(client);
        drop(server);
        let _ = tokio::time::timeout(Duration::from_secs(5), tunnel).await;
    }

    /// An address the destination policy refused is never dialled, even when every
    /// allowed address fails. 127.0.0.1 is loopback and outside the carve-out here,
    /// so the tunnel must fail without its listener seeing a connection.
    #[tokio::test]
    async fn the_passthrough_never_dials_an_address_the_policy_refused() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let (_client, tunnel) = start_passthrough(
            &["::1", "127.0.0.1"],
            &["::1"],
            port,
            OwnAddresses::listed_only(&[]),
        )
        .await;
        let outcome = tokio::time::timeout(Duration::from_secs(10), tunnel)
            .await
            .expect("the tunnel must give up")
            .unwrap();
        assert!(
            outcome.is_err(),
            "the only allowed address refuses, so the tunnel must fail"
        );
        let reached = tokio::time::timeout(Duration::from_millis(500), upstream.accept()).await;
        assert!(
            reached.is_err(),
            "127.0.0.1, which the policy refused, must never be dialled"
        );
    }

    /// The passthrough refuses a destination that is one of this host's interface
    /// addresses. 127.0.0.1 is on the loopback interface of every host, so the
    /// production interface read makes it this proxy's own without root. The endpoint
    /// override carves it out of the destination policy, so only the loop check can
    /// stop the tunnel; the listener on it stands in for the proxy's own port.
    #[tokio::test]
    async fn the_passthrough_refuses_an_interface_address_of_this_host() {
        let own = OwnAddresses::new(&[]);
        assert!(
            own.contains("127.0.0.1".parse().unwrap()),
            "precondition: the interface read lists 127.0.0.1"
        );
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let (_client, tunnel) = start_passthrough(&["127.0.0.1"], &["127.0.0.1"], port, own).await;
        let reached = tokio::time::timeout(Duration::from_millis(500), upstream.accept()).await;
        assert!(
            reached.is_err(),
            "127.0.0.1 is this host's own address and must never be dialled"
        );
        let outcome = tokio::time::timeout(Duration::from_secs(5), tunnel)
            .await
            .expect("a refused tunnel must end")
            .unwrap();
        assert!(
            outcome.is_ok(),
            "a refused tunnel closes quietly: {outcome:?}"
        );
    }

    /// The passthrough refuses an address listed in `tunnel_refused_addresses`.
    /// 127.0.0.2 answers on Linux's loopback without being an interface address, so
    /// only the list makes it the proxy's own.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_passthrough_refuses_a_listed_address() {
        let listed: IpAddr = "127.0.0.2".parse().unwrap();
        let upstream = TcpListener::bind("127.0.0.2:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let (_client, tunnel) = start_passthrough(
            &["127.0.0.2"],
            &["127.0.0.2"],
            port,
            OwnAddresses::new(&[listed]),
        )
        .await;
        let reached = tokio::time::timeout(Duration::from_millis(500), upstream.accept()).await;
        assert!(
            reached.is_err(),
            "127.0.0.2 is listed and must never be dialled"
        );
        let outcome = tokio::time::timeout(Duration::from_secs(5), tunnel)
            .await
            .expect("a refused tunnel must end")
            .unwrap();
        assert!(
            outcome.is_ok(),
            "a refused tunnel closes quietly: {outcome:?}"
        );
    }

    /// A name that resolves to a listed address and to another one is refused as a
    /// whole: the other address's listener sees no connection.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_passthrough_refuses_a_name_with_one_own_address_among_others() {
        let listed: IpAddr = "127.0.0.2".parse().unwrap();
        let other = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = other.local_addr().unwrap().port();
        let (_client, tunnel) = start_passthrough(
            &["127.0.0.2", "127.0.0.1"],
            &["127.0.0.2", "127.0.0.1"],
            port,
            OwnAddresses::listed_only(&[listed]),
        )
        .await;
        let reached = tokio::time::timeout(Duration::from_millis(500), other.accept()).await;
        assert!(
            reached.is_err(),
            "one own address refuses the tunnel, so the other must never be dialled"
        );
        let outcome = tokio::time::timeout(Duration::from_secs(5), tunnel)
            .await
            .expect("a refused tunnel must end")
            .unwrap();
        assert!(
            outcome.is_ok(),
            "a refused tunnel closes quietly: {outcome:?}"
        );
    }

    /// Under the production read every 127/8 address is own, since the passthrough's
    /// dual-stack listener answers on all of them.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_passthrough_refuses_any_loopback_address() {
        let own = OwnAddresses::new(&[]);
        let upstream = TcpListener::bind("127.0.0.2:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let (_client, tunnel) = start_passthrough(&["127.0.0.2"], &["127.0.0.2"], port, own).await;
        let reached = tokio::time::timeout(Duration::from_millis(500), upstream.accept()).await;
        assert!(
            reached.is_err(),
            "127.0.0.2 is loopback and must never be dialled"
        );
        let outcome = tokio::time::timeout(Duration::from_secs(5), tunnel)
            .await
            .expect("a refused tunnel must end")
            .unwrap();
        assert!(
            outcome.is_ok(),
            "a refused tunnel closes quietly: {outcome:?}"
        );
    }

    /// The control for the refusals: an address that is not own is still relayed. No
    /// foreign address answers on a test host, so the interface read is switched off
    /// and 127.0.0.2 stands in for it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_passthrough_relays_an_address_that_is_not_its_own() {
        let own = OwnAddresses::listed_only(&[]);
        assert!(
            !own.contains("127.0.0.2".parse().unwrap()),
            "precondition: 127.0.0.2 is neither listed nor on an interface"
        );
        let upstream = TcpListener::bind("127.0.0.2:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let (mut client, tunnel) =
            start_passthrough(&["127.0.0.2"], &["127.0.0.2"], port, own).await;
        let (mut server, _) = tokio::time::timeout(Duration::from_secs(5), upstream.accept())
            .await
            .expect("an unrelated address must be relayed")
            .unwrap();
        server.write_all(b"pong").await.unwrap();
        let mut buf = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut buf))
            .await
            .expect("no byte came back through the tunnel")
            .unwrap();
        assert_eq!(&buf, b"pong");
        drop(client);
        drop(server);
        let _ = tokio::time::timeout(Duration::from_secs(5), tunnel).await;
    }

    /// The production interface read does not refuse an address that is not this host's.
    /// No foreign address answers on a test host without a network, so the dial itself
    /// cannot be completed. A refusal ends the tunnel at once and quietly (`Ok`), where a
    /// relay dials the documentation-range address: the tunnel is still connecting after
    /// half a second, or its dial has failed with an error. A `new()` that refused every
    /// address ends the tunnel with `Ok` and fails the dial assertion first; the checks on
    /// the interface read itself run after it, so they never hide the dial's verdict.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_production_interface_read_dials_an_address_that_is_not_this_hosts() {
        let own = OwnAddresses::new(&[]);
        let foreign: IpAddr = "192.0.2.44".parse().unwrap();
        let own_by_read = own.contains(foreign);
        let filtered = own.filter_tunnel_targets(vec![foreign]);
        let (client, mut tunnel) =
            start_passthrough(&["192.0.2.44"], &["192.0.2.44"], 4431, own).await;
        match tokio::time::timeout(Duration::from_millis(500), &mut tunnel).await {
            Err(_elapsed) => tunnel.abort(),
            Ok(joined) => {
                let outcome = joined.unwrap();
                assert!(
                    outcome.is_err(),
                    "a tunnel to an address that is not this host's must dial it; ending with \
                     Ok is the quiet close a refusal gives: {outcome:?} (the interface read \
                     calls the address own: {own_by_read}, the target filter returned \
                     {filtered:?})"
                );
            }
        }
        drop(client);
        assert!(
            !own_by_read,
            "precondition: 192.0.2.44 is not on an interface of this host"
        );
        assert_eq!(
            filtered,
            Ok(vec![foreign]),
            "the list is passed through unchanged"
        );
    }
    /// A tunnel's connect stops at its deadline however many addresses it has. Every
    /// address is a listener whose accept queue is full, so the kernel drops each SYN
    /// and every attempt runs to its 10 s bound. On a paused clock, eight such
    /// addresses over the passthrough's three rounds take 240.3 s without the deadline.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_tunnel_connect_stops_at_its_deadline_whatever_the_address_count() {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let full = socket.listen(0).unwrap();
        let addr = full.local_addr().unwrap();
        let mut held = Vec::new();
        let mut filled = false;
        for _ in 0..16 {
            match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(stream) => held.push(stream),
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    filled = true;
                    break;
                }
                Err(e) => panic!("precondition: the probe was refused, not left hanging: {e}"),
            }
        }
        assert!(filled, "precondition: the accept queue never filled");
        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let result = TcpProxy::connect_first_reachable(
            &[addr.ip(); 8],
            addr.port(),
            3,
            Duration::from_secs(10),
            TUNNEL_CONNECT_DEADLINE,
        )
        .await;
        let elapsed = started.elapsed();
        assert!(result.is_err(), "nothing can accept");
        assert!(
            elapsed <= TUNNEL_CONNECT_DEADLINE + Duration::from_millis(100),
            "the connect took {elapsed:?}, past its {TUNNEL_CONNECT_DEADLINE:?} deadline"
        );
        drop(held);
    }
}
