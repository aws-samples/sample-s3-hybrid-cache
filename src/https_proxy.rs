//! HTTPS Proxy Module
//!
//! Handles HTTPS connections using TCP passthrough mode:
//! - TCP passthrough: Direct TCP tunneling without TLS termination

use crate::{config::Config, tcp_proxy::TcpProxy, Result};
use std::net::{IpAddr, SocketAddr};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// HTTPS Proxy server using TCP passthrough
pub struct HttpsProxy {
    listen_addr: SocketAddr,
    endpoint_overrides: HashMap<String, Vec<IpAddr>>,
}

impl HttpsProxy {
    /// Create a new HTTPS proxy instance
    pub fn new(listen_addr: SocketAddr, config: Arc<Config>) -> Self {
        // Parse endpoint overrides from config
        let mut endpoint_overrides = HashMap::new();
        for (hostname, ip_strings) in &config.connection_pool.endpoint_overrides {
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
                endpoint_overrides.insert(hostname.clone(), ips);
            }
        }

        Self {
            listen_addr,
            endpoint_overrides,
        }
    }

    /// Start the HTTPS proxy server in TCP passthrough mode
    pub async fn start(&self, shutdown_signal: crate::shutdown::ShutdownSignal) -> Result<()> {
        info!(
            "Starting HTTPS proxy in TCP passthrough mode on {}",
            self.listen_addr
        );
        let tcp_proxy = TcpProxy::new(self.listen_addr, self.endpoint_overrides.clone());
        tcp_proxy.start(shutdown_signal).await
    }
}
