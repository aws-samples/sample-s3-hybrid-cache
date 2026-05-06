//! Custom HTTPS Connector for Hyper Connection Pooling
//!
//! This module provides a custom connector that integrates Hyper's connection pooling
//! with the existing ConnectionPoolManager for IP selection and load balancing.
//! Applies TCP socket options (keepalive, receive buffer) via socket2 before TLS handshake.

use crate::config::ConnectionPoolConfig;
use crate::connection_pool::{ConnectionPoolManager, IpHealthTracker};
use crate::{ProxyError, Result};
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use rustls::pki_types::ServerName;
use socket2::{Socket, TcpKeepalive};
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
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

/// Custom HTTPS connector that integrates with ConnectionPoolManager.
///
/// Applies TCP keepalive and receive buffer options via socket2 on new connections.
/// Records connection failures in IpHealthTracker for automatic IP exclusion.
pub struct CustomHttpsConnector {
    pool_manager: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    /// Default TLS connector (TLS 1.2 + 1.3)
    tls_default: TlsConnector,
    /// TLS 1.2-only connector for VPC interface endpoints (PrivateLink).
    /// None when no endpoint_overrides are configured.
    tls_privatelink: Option<TlsConnector>,
    config: ConnectionPoolConfig,
    health_tracker: Arc<IpHealthTracker>,
    metrics_manager:
        Arc<tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>>,
}

impl CustomHttpsConnector {
    pub fn new(
        pool_manager: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
        tls_default: TlsConnector,
        tls_privatelink: Option<TlsConnector>,
        config: ConnectionPoolConfig,
        health_tracker: Arc<IpHealthTracker>,
    ) -> Self {
        Self {
            pool_manager,
            tls_default,
            tls_privatelink,
            config,
            health_tracker,
            metrics_manager: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    pub fn set_metrics_manager_ref(
        &mut self,
        metrics_manager: Arc<
            tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>,
        >,
    ) {
        self.metrics_manager = metrics_manager;
    }
}

/// Apply TCP socket options (keepalive + receive buffer) via socket2.
/// Converts TcpStream → socket2::Socket → apply options → convert back.
/// Errors are logged as warnings and do not fail the connection.
fn apply_socket_options(
    tcp: TcpStream,
    config: &ConnectionPoolConfig,
) -> std::result::Result<TcpStream, ProxyError> {
    let std_stream = tcp.into_std().map_err(|e| {
        ProxyError::ConnectionError(format!("Failed to convert TcpStream to std: {}", e))
    })?;

    let socket = Socket::from(std_stream);

    // TCP keepalive
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(config.keepalive_idle_secs))
        .with_interval(Duration::from_secs(config.keepalive_interval_secs))
        .with_retries(config.keepalive_retries);
    if let Err(e) = socket.set_tcp_keepalive(&keepalive) {
        warn!("Failed to set TCP keepalive: {}", e);
    }

    // Receive buffer
    if let Some(size) = config.tcp_recv_buffer_size {
        if let Err(e) = socket.set_recv_buffer_size(size) {
            warn!("Failed to set SO_RCVBUF to {}: {}", size, e);
        }
    }

    let std_stream: std::net::TcpStream = socket.into();
    let tcp = TcpStream::from_std(std_stream).map_err(|e| {
        ProxyError::ConnectionError(format!(
            "Failed to convert std TcpStream back to tokio: {}",
            e
        ))
    })?;

    tcp.set_nodelay(true).ok();

    Ok(tcp)
}

impl Service<Uri> for CustomHttpsConnector {
    type Response = HttpsStream;
    type Error = ProxyError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let pool_manager = Arc::clone(&self.pool_manager);
        let tls_default = self.tls_default.clone();
        let tls_privatelink = self.tls_privatelink.clone();
        let metrics_manager = self.metrics_manager.clone();
        let config = self.config.clone();
        let health_tracker = Arc::clone(&self.health_tracker);

        Box::pin(async move {
            let uri_host = uri
                .host()
                .ok_or_else(|| ProxyError::ConfigError("No host in URI".to_string()))?;

            // Determine connect IP and TLS hostname
            let (connect_ip, tls_hostname) = if let Ok(ip) = uri_host.parse::<IpAddr>() {
                // URI host is an IP — IpDistributor already selected it.
                // Look up the original S3 hostname for TLS SNI.
                let hostname = {
                    let pm = pool_manager.read().await;
                    pm.get_hostname_for_ip(&ip)
                };

                match hostname {
                    Some(h) => {
                        debug!(
                            "[HTTPS_CONNECTOR] URI host is IP {}, using hostname '{}' for TLS SNI",
                            ip, h
                        );
                        (ip, h)
                    }
                    None => {
                        warn!(
                            "[HTTPS_CONNECTOR] No hostname mapping found for IP {}, TLS SNI may fail",
                            ip
                        );
                        (ip, uri_host.to_string())
                    }
                }
            } else {
                // URI host is a regular hostname — resolve via the pool manager's
                // configured DNS resolver (Google/Cloudflare/custom), which bypasses
                // /etc/hosts. This matches the HTTP proxy's resolution behaviour.
                debug!(
                    "[HTTPS_CONNECTOR] Resolving hostname via configured DNS: {}",
                    uri_host
                );
                let ips = {
                    let pm = pool_manager.read().await;
                    pm.resolve_endpoint(uri_host).await?
                };
                let ip = *ips.first().ok_or_else(|| {
                    ProxyError::ConnectionError(format!("No addresses found for {}", uri_host))
                })?;
                (ip, uri_host.to_string())
            };

            // Establish TCP connection
            let tcp = TcpStream::connect((connect_ip, 443)).await.map_err(|e| {
                warn!(
                    "[HTTPS_CONNECTOR] TCP connection failed to {}:{}: {}",
                    connect_ip, 443, e
                );
                // Record failure for health tracking
                if health_tracker.record_failure(&connect_ip) {
                    warn!(ip = %connect_ip, "IP failure threshold reached on TCP connect, excluding");
                    let pm = pool_manager.clone();
                    let host = tls_hostname.clone();
                    tokio::spawn(async move {
                        let mut pm = pm.write().await;
                        if let Some(dist) = pm.get_distributor_mut(&host) {
                            dist.remove_ip(connect_ip, "TCP connect failure");
                        }
                    });
                }
                ProxyError::ConnectionError(format!(
                    "Failed to connect to {}:{}: {}",
                    connect_ip, 443, e
                ))
            })?;

            // Apply TCP socket options (keepalive + receive buffer) via socket2
            let tcp = apply_socket_options(tcp, &config)?;

            debug!(
                "[HTTPS_CONNECTOR] TCP connection established to {}:443",
                connect_ip
            );

            // Select TLS connector: use TLS 1.2-only for PrivateLink destinations,
            // default (1.2+1.3) for everything else.
            let tls_connector = if let Some(ref pl) = tls_privatelink {
                let pm = pool_manager.read().await;
                if pm.resolve_override(&tls_hostname).is_some() {
                    debug!(
                        "[HTTPS_CONNECTOR] Using TLS 1.2 connector for PrivateLink destination: {}",
                        tls_hostname
                    );
                    pl.clone()
                } else {
                    tls_default.clone()
                }
            } else {
                tls_default.clone()
            };

            // TLS handshake
            let server_name = ServerName::try_from(tls_hostname.clone()).map_err(|e| {
                ProxyError::TlsError(format!("Invalid server name '{}': {}", tls_hostname, e))
            })?;

            let tls = tls_connector.connect(server_name, tcp).await.map_err(|e| {
                warn!(
                    "[HTTPS_CONNECTOR] TLS handshake failed to {} ({}): {}",
                    tls_hostname, connect_ip, e
                );
                // Record failure for health tracking
                if health_tracker.record_failure(&connect_ip) {
                    warn!(ip = %connect_ip, "IP failure threshold reached on TLS handshake, excluding");
                    let pm = pool_manager.clone();
                    let host = tls_hostname.clone();
                    tokio::spawn(async move {
                        let mut pm = pm.write().await;
                        if let Some(dist) = pm.get_distributor_mut(&host) {
                            dist.remove_ip(connect_ip, "TLS handshake failure");
                        }
                    });
                }
                ProxyError::TlsError(format!(
                    "TLS handshake failed to {} ({}): {}",
                    tls_hostname, connect_ip, e
                ))
            })?;

            debug!(
                "[HTTPS_CONNECTOR] TLS connection established to {} ({})",
                tls_hostname, connect_ip
            );

            // Record connection creation in metrics
            let mm = metrics_manager.read().await;
            if let Some(ref metrics) = *mm {
                metrics
                    .read()
                    .await
                    .record_connection_created(&tls_hostname)
                    .await;
            }

            Ok(HttpsStream::new(tls))
        })
    }
}

impl Clone for CustomHttpsConnector {
    fn clone(&self) -> Self {
        Self {
            pool_manager: Arc::clone(&self.pool_manager),
            tls_default: self.tls_default.clone(),
            tls_privatelink: self.tls_privatelink.clone(),
            config: self.config.clone(),
            health_tracker: Arc::clone(&self.health_tracker),
            metrics_manager: self.metrics_manager.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectionPoolConfig;

    #[tokio::test]
    async fn test_connector_creation() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let config = ConnectionPoolConfig::default();

        let pool_manager = Arc::new(tokio::sync::RwLock::new(
            ConnectionPoolManager::new_with_config(config.clone()).unwrap(),
        ));

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let tls_connector = TlsConnector::from(Arc::new(tls_config));
        let health_tracker = Arc::new(IpHealthTracker::new(3));

        let connector =
            CustomHttpsConnector::new(pool_manager, tls_connector, None, config, health_tracker);

        let _connector2 = connector.clone();
    }
}
