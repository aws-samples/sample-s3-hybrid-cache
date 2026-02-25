//! Custom HTTPS Connector for Hyper Connection Pooling
//!
//! This module provides a custom connector that integrates Hyper's connection pooling
//! with the existing ConnectionPoolManager for IP selection and load balancing.

use crate::connection_pool::ConnectionPoolManager;
use crate::{ProxyError, Result};
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use rustls::pki_types::ServerName;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};
use tower::Service;
use tracing::{debug, warn};

/// Wrapper type for TLS streams that implements Connection trait
pub struct HttpsStream(TlsStream<TcpStream>);

impl HttpsStream {
    fn new(stream: TlsStream<TcpStream>) -> Self {
        Self(stream)
    }
}

impl Read for HttpsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let mut tokio_buf = tokio::io::ReadBuf::uninit(unsafe { buf.as_mut() });
        match Pin::new(&mut self.0).poll_read(cx, &mut tokio_buf) {
            Poll::Ready(Ok(())) => {
                let filled = tokio_buf.filled().len();
                unsafe {
                    buf.advance(filled);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Write for HttpsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Connection for HttpsStream {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// Custom HTTPS connector that integrates with ConnectionPoolManager
///
/// This connector implements the tower::Service trait required by Hyper's client.
/// It consults the ConnectionPoolManager for IP selection (load balancing) and
/// establishes TLS connections using tokio-rustls.
pub struct CustomHttpsConnector {
    pool_manager: Arc<tokio::sync::Mutex<ConnectionPoolManager>>,
    tls_connector: TlsConnector,
    metrics_manager:
        Arc<tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>>,
}

impl CustomHttpsConnector {
    /// Create a new CustomHttpsConnector
    pub fn new(
        pool_manager: Arc<tokio::sync::Mutex<ConnectionPoolManager>>,
        tls_connector: TlsConnector,
    ) -> Self {
        Self {
            pool_manager,
            tls_connector,
            metrics_manager: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Set the shared metrics manager reference for tracking connection metrics
    pub fn set_metrics_manager_ref(
        &mut self,
        metrics_manager: Arc<
            tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>,
        >,
    ) {
        self.metrics_manager = metrics_manager;
    }
}

impl Service<Uri> for CustomHttpsConnector {
    type Response = HttpsStream;
    type Error = ProxyError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // Always ready to create new connections
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let pool_manager = Arc::clone(&self.pool_manager);
        let tls_connector = self.tls_connector.clone();
        let metrics_manager = self.metrics_manager.clone();

        Box::pin(async move {
            // Extract hostname from URI
            let hostname = uri
                .host()
                .ok_or_else(|| ProxyError::ConfigError("No host in URI".to_string()))?;

            debug!(
                "[HTTPS_CONNECTOR] Establishing connection to hostname: {}",
                hostname
            );

            // Get IP address from ConnectionPoolManager (handles load balancing)
            let connection = {
                let mut pm = pool_manager.lock().await;
                pm.get_connection(hostname, None).await?
            };

            let ip = connection.ip_address;
            debug!(
                "[HTTPS_CONNECTOR] Selected IP {} for hostname {}",
                ip, hostname
            );

            // Establish TCP connection to selected IP
            let tcp = TcpStream::connect((ip, 443)).await.map_err(|e| {
                warn!(
                    "[HTTPS_CONNECTOR] TCP connection failed to {}:{}: {}",
                    ip, 443, e
                );
                ProxyError::ConnectionError(format!("Failed to connect to {}:{}: {}", ip, 443, e))
            })?;

            // Set TCP_NODELAY to disable Nagle's algorithm for lower latency
            if let Err(e) = tcp.set_nodelay(true) {
                warn!(
                    "[HTTPS_CONNECTOR] Failed to set TCP_NODELAY for {}:{}: {}",
                    ip, 443, e
                );
            }

            debug!("[HTTPS_CONNECTOR] TCP connection established to {}:443", ip);

            // Perform TLS handshake
            let server_name = ServerName::try_from(hostname.to_string()).map_err(|e| {
                ProxyError::TlsError(format!("Invalid server name '{}': {}", hostname, e))
            })?;

            let tls = tls_connector.connect(server_name, tcp).await.map_err(|e| {
                warn!(
                    "[HTTPS_CONNECTOR] TLS handshake failed to {} ({}): {}",
                    hostname, ip, e
                );
                ProxyError::TlsError(format!(
                    "TLS handshake failed to {} ({}): {}",
                    hostname, ip, e
                ))
            })?;

            debug!(
                "[HTTPS_CONNECTOR] TLS connection established to {} ({}) - conn_id: {}",
                hostname, ip, connection.id
            );

            // Record connection creation in metrics
            let mm = metrics_manager.read().await;
            if let Some(ref metrics) = *mm {
                metrics
                    .read()
                    .await
                    .record_connection_created(hostname)
                    .await;
                debug!(
                    "[HTTPS_CONNECTOR] Recorded connection creation for endpoint: {}",
                    hostname
                );
            }

            Ok(HttpsStream::new(tls))
        })
    }
}

impl Clone for CustomHttpsConnector {
    fn clone(&self) -> Self {
        Self {
            pool_manager: Arc::clone(&self.pool_manager),
            tls_connector: self.tls_connector.clone(),
            metrics_manager: self.metrics_manager.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectionPoolConfig;
    use std::time::Duration;

    #[tokio::test]
    async fn test_connector_creation() {
        // Install default crypto provider for Rustls
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Create a test ConnectionPoolManager
        let config = ConnectionPoolConfig {
            max_connections_per_ip: 10,
            dns_refresh_interval: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            keepalive_enabled: true,
            max_idle_per_host: 1,
            max_lifetime: Duration::from_secs(300),
            pool_check_interval: Duration::from_secs(10),
            dns_servers: Vec::new(),
            endpoint_overrides: std::collections::HashMap::new(),
        };

        let pool_manager = Arc::new(tokio::sync::Mutex::new(
            ConnectionPoolManager::new_with_config(config).unwrap(),
        ));

        // Create TLS connector
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let tls_connector = TlsConnector::from(Arc::new(tls_config));

        // Create connector
        let connector = CustomHttpsConnector::new(pool_manager, tls_connector);

        // Verify connector can be cloned
        let _connector2 = connector.clone();
    }
}
