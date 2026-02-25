//! TCP Proxy Module
//!
//! Provides transparent TCP tunneling for HTTPS connections (default mode).
//! This module handles Layer 3/4 transparent TCP tunneling without TLS termination,
//! preserving end-to-end TLS encryption and certificate validation.

use crate::{ProxyError, Result};
use std::net::{IpAddr, SocketAddr};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};
use trust_dns_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
use trust_dns_resolver::TokioAsyncResolver;

/// TCP Proxy handler for transparent HTTPS passthrough
pub struct TcpProxy {
    listen_addr: SocketAddr,
    resolver: TokioAsyncResolver,
    /// Static hostname-to-IP mappings that bypass DNS resolution (for PrivateLink etc.)
    endpoint_overrides: HashMap<String, Vec<IpAddr>>,
}

impl TcpProxy {
    /// Create a new TCP proxy instance with external DNS resolver
    pub fn new(listen_addr: SocketAddr, endpoint_overrides: HashMap<String, Vec<IpAddr>>) -> Self {
        // Use external DNS servers to bypass /etc/hosts (same as connection pool)
        let mut config = ResolverConfig::new();
        config.add_name_server(NameServerConfigGroup::google().into_inner()[0].clone());
        config.add_name_server(NameServerConfigGroup::google().into_inner()[1].clone());
        config.add_name_server(NameServerConfigGroup::cloudflare().into_inner()[0].clone());
        config.add_name_server(NameServerConfigGroup::cloudflare().into_inner()[1].clone());

        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = false; // Critical: bypass /etc/hosts

        let resolver = TokioAsyncResolver::tokio(config, opts);

        if !endpoint_overrides.is_empty() {
            info!("TCP proxy initialized with {} endpoint override(s)", endpoint_overrides.len());
        }

        Self {
            listen_addr,
            resolver,
            endpoint_overrides,
        }
    }

    /// Start the TCP proxy server
    pub async fn start(&self, mut shutdown_signal: crate::shutdown::ShutdownSignal) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;

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
                            let endpoint_overrides = self.endpoint_overrides.clone();
                            tokio::spawn(async move {
                                if let Err(e) =
                                    Self::handle_connection(client_stream, client_addr, resolver, endpoint_overrides).await
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
                            error!("Failed to accept TCP connection: {}", e);
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
        resolver: TokioAsyncResolver,
        endpoint_overrides: HashMap<String, Vec<IpAddr>>,
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
        match Self::establish_tcp_tunnel(client_stream, client_addr, &target_host, resolver, endpoint_overrides).await {
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

        let hostname = &data[5..5 + name_len];
        String::from_utf8(hostname.to_vec()).ok()
    }

    /// Establish TCP tunnel to target endpoint
    async fn establish_tcp_tunnel(
        client_stream: TcpStream,
        client_addr: SocketAddr,
        target_host: &str,
        resolver: TokioAsyncResolver,
        endpoint_overrides: HashMap<String, Vec<IpAddr>>,
    ) -> Result<()> {
        debug!(
            "Resolving {} using external DNS (bypassing /etc/hosts)",
            target_host
        );

        // Check endpoint overrides first (for PrivateLink etc.)
        let ip_addresses: Vec<IpAddr> = if let Some(ips) = endpoint_overrides.get(target_host) {
            info!(
                "Using endpoint override for {}: {:?}",
                target_host, ips
            );
            ips.clone()
        } else {
            // Resolve hostname using external DNS to bypass /etc/hosts
            match resolver.lookup_ip(target_host).await {
                Ok(lookup) => lookup.iter().collect(),
                Err(e) => {
                    error!("DNS resolution failed for {}: {}", target_host, e);
                    drop(client_stream);
                    return Err(ProxyError::ConnectionError(format!(
                        "DNS resolution failed for {}: {}",
                        target_host, e
                    )));
                }
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

        // Try to connect to the first IP address (could be enhanced with load balancing)
        let target_ip = ip_addresses[0];
        let target_addr = SocketAddr::new(target_ip, 443);

        debug!(
            "Establishing tunnel from {} to {} ({}:443)",
            client_addr, target_host, target_ip
        );

        // Attempt to connect to target with timeout and retry logic
        let server_stream = match Self::connect_with_retry_to_ip(target_addr, 3).await {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "Failed to connect to target {}:{} for client {} after retries: {}",
                    target_host, target_ip, client_addr, e
                );
                // Close client connection gracefully as per requirement 14.6
                drop(client_stream);
                return Err(ProxyError::ConnectionError(format!(
                    "Failed to connect to target {}:{}: {}",
                    target_host, target_ip, e
                )));
            }
        };

        debug!(
            "TCP tunnel established: {} <-> {}:443",
            client_addr, target_host
        );

        // Start bidirectional forwarding with error handling
        let target_addr_str = format!("{}:443", target_host);
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

    /// Connect to target IP with retry logic for better reliability
    async fn connect_with_retry_to_ip(
        target_addr: SocketAddr,
        max_retries: u32,
    ) -> Result<TcpStream> {
        let mut last_error = None;

        for attempt in 1..=max_retries {
            match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                TcpStream::connect(target_addr),
            )
            .await
            {
                Ok(Ok(stream)) => {
                    if attempt > 1 {
                        debug!("Connected to {} on attempt {}", target_addr, attempt);
                    }
                    return Ok(stream);
                }
                Ok(Err(e)) => {
                    warn!(
                        "Connection attempt {} to {} failed: {}",
                        attempt, target_addr, e
                    );
                    last_error = Some(e);
                }
                Err(_) => {
                    warn!(
                        "Connection attempt {} to {} timed out",
                        attempt, target_addr
                    );
                    last_error = Some(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Connection timeout",
                    ));
                }
            }

            if attempt < max_retries {
                // Exponential backoff: 100ms, 200ms, 400ms
                let delay = std::time::Duration::from_millis(100 * (1 << (attempt - 1)));
                tokio::time::sleep(delay).await;
            }
        }

        Err(ProxyError::ConnectionError(format!(
            "Failed to connect to {} after {} attempts: {}",
            target_addr,
            max_retries,
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "Unknown error".to_string())
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
