//! HTTPS Proxy Module
//!
//! Handles HTTPS connections using TCP passthrough mode:
//! - TCP passthrough: Direct TCP tunneling without TLS termination

use crate::connection_pool::EndpointOverrides;
use crate::{config::Config, tcp_proxy::TcpProxy, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

/// HTTPS Proxy server using TCP passthrough
pub struct HttpsProxy {
    listen_addr: SocketAddr,
    overrides: EndpointOverrides,
}

impl HttpsProxy {
    /// Create a new HTTPS proxy instance
    pub fn new(listen_addr: SocketAddr, config: Arc<Config>) -> Self {
        let overrides = EndpointOverrides::from_config(&config.connection_pool.endpoint_overrides);

        Self {
            listen_addr,
            overrides,
        }
    }

    /// Start the HTTPS proxy server in TCP passthrough mode
    pub async fn start(&self, shutdown_signal: crate::shutdown::ShutdownSignal) -> Result<()> {
        info!(
            "Starting HTTPS proxy in TCP passthrough mode on {}",
            self.listen_addr
        );
        let tcp_proxy = TcpProxy::new(self.listen_addr, self.overrides.clone());
        tcp_proxy.start(shutdown_signal).await
    }
}
