//! HTTPS Proxy Module
//!
//! Handles HTTPS connections using TCP passthrough mode:
//! - TCP passthrough: Direct TCP tunneling without TLS termination

use crate::connection_pool::EndpointOverrides;
use crate::destination_policy::DestinationPolicy;
use crate::{config::Config, tcp_proxy::TcpProxy, Result};
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tracing::info;

/// HTTPS Proxy server using TCP passthrough
pub struct HttpsProxy {
    listen_addr: SocketAddr,
    overrides: EndpointOverrides,
    destination_policy: Arc<DestinationPolicy>,
}

impl HttpsProxy {
    /// Create a new HTTPS proxy instance
    pub fn new(listen_addr: SocketAddr, config: Arc<Config>) -> Self {
        let overrides = EndpointOverrides::from_config(&config.connection_pool.endpoint_overrides);

        // Build endpoint_override_ips from the config for the destination policy
        let mut endpoint_override_ips: HashSet<IpAddr> = HashSet::new();
        for ip_strings in config.connection_pool.endpoint_overrides.values() {
            for ip_str in ip_strings {
                if let Ok(ip) = ip_str.parse::<IpAddr>() {
                    endpoint_override_ips.insert(ip);
                }
            }
        }

        // Get connect_allowlist from TLS config if present
        let connect_allowlist = config
            .server
            .tls
            .as_ref()
            .and_then(|tls| tls.connect_allowlist.clone());

        let destination_policy = Arc::new(DestinationPolicy::new(
            443,
            connect_allowlist,
            endpoint_override_ips,
        ));

        Self {
            listen_addr,
            overrides,
            destination_policy,
        }
    }

    /// Start the HTTPS proxy server in TCP passthrough mode
    pub async fn start(&self, shutdown_signal: crate::shutdown::ShutdownSignal) -> Result<()> {
        info!(
            "Starting HTTPS proxy in TCP passthrough mode on {}",
            self.listen_addr
        );
        let tcp_proxy = TcpProxy::new(
            self.listen_addr,
            self.overrides.clone(),
            self.destination_policy.clone(),
        );
        tcp_proxy.start(shutdown_signal).await
    }
}
