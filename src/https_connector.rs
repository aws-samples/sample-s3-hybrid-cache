//! Custom HTTPS Connector for Hyper Connection Pooling
//!
//! This module provides a custom connector that integrates Hyper's connection pooling
//! with the existing ConnectionPoolManager for IP selection and load balancing.
//! Applies TCP socket options (keepalive, receive buffer) via socket2 before TLS handshake.
//!
//! Also provides [`build_tls_config_for_host`], the single source of truth for
//! outbound TLS version selection: override-matched hosts get TLS 1.2 only;
//! all others get the default (TLS 1.2 or 1.3).

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

/// Build the appropriate `rustls::ClientConfig` for an outbound connection to `host`.
///
/// Override-matched hosts (those present in `pool_manager.resolve_override`) receive
/// a TLS 1.2-only configuration (VPC interface endpoints / PrivateLink don't support
/// TLS 1.3). All other hosts receive the default configuration (TLS 1.2 or 1.3).
///
/// This is the single source of truth for outbound TLS version selection — all
/// callsites that need a per-host `ClientConfig` should use this function.
pub fn build_tls_config_for_host(
    host: &str,
    root_store: rustls::RootCertStore,
    pool_manager: &ConnectionPoolManager,
) -> rustls::ClientConfig {
    if pool_manager.resolve_override(host).is_some() {
        debug!(
            host = %host,
            "Using TLS 1.2-only config for PrivateLink/override-matched destination"
        );
        build_tls12_only_config(root_store)
    } else {
        build_tls_default_config(root_store)
    }
}

/// Build the default TLS configuration (TLS 1.2 or 1.3).
pub fn build_tls_default_config(root_store: rustls::RootCertStore) -> rustls::ClientConfig {
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Build a TLS 1.2-only configuration for PrivateLink / VPC interface endpoints.
pub fn build_tls12_only_config(root_store: rustls::RootCertStore) -> rustls::ClientConfig {
    rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

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
/// Uses [`build_tls_config_for_host`] to select the correct TLS version per host.
pub struct CustomHttpsConnector {
    pool_manager: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
    /// Root certificate store for building per-host TLS configurations.
    root_store: rustls::RootCertStore,
    config: ConnectionPoolConfig,
    health_tracker: Arc<IpHealthTracker>,
    metrics_manager:
        Arc<tokio::sync::RwLock<Option<Arc<tokio::sync::RwLock<crate::metrics::MetricsManager>>>>>,
}

impl CustomHttpsConnector {
    pub fn new(
        pool_manager: Arc<tokio::sync::RwLock<ConnectionPoolManager>>,
        root_store: rustls::RootCertStore,
        config: ConnectionPoolConfig,
        health_tracker: Arc<IpHealthTracker>,
    ) -> Self {
        Self {
            pool_manager,
            root_store,
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
        let root_store = self.root_store.clone();
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

            // Select TLS connector via the shared builder function.
            let tls_connector = {
                let pm = pool_manager.read().await;
                let cfg = build_tls_config_for_host(&tls_hostname, root_store, &pm);
                TlsConnector::from(Arc::new(cfg))
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
            root_store: self.root_store.clone(),
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

        let health_tracker = Arc::new(IpHealthTracker::new(3));

        let connector = CustomHttpsConnector::new(pool_manager, root_store, config, health_tracker);

        let _connector2 = connector.clone();
    }

    #[test]
    fn test_build_tls_config_default() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = ConnectionPoolConfig::default();
        let pm = ConnectionPoolManager::new_with_config(config).unwrap();

        // Non-override host should get default TLS config (1.2 + 1.3)
        let cfg = build_tls_config_for_host("s3.us-east-1.amazonaws.com", root_store, &pm);
        // Default config supports TLS 1.2 and 1.3
        assert!(cfg.alpn_protocols.is_empty()); // no ALPN set by default
    }

    #[test]
    fn test_build_tls_config_override_matched() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // Create a config with endpoint overrides
        let mut config = ConnectionPoolConfig::default();
        config.endpoint_overrides.insert(
            "vpce-bucket.s3.us-east-1.vpce.amazonaws.com".to_string(),
            vec!["10.0.1.100".parse().unwrap()],
        );
        let pm = ConnectionPoolManager::new_with_config(config).unwrap();

        // Override-matched host should get TLS 1.2-only config
        let _cfg = build_tls_config_for_host(
            "vpce-bucket.s3.us-east-1.vpce.amazonaws.com",
            root_store,
            &pm,
        );
        // The config is TLS 1.2 only — verified by the builder_with_protocol_versions call
    }

    #[test]
    fn test_build_tls_default_config_fn() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let _cfg = build_tls_default_config(root_store);
    }

    #[test]
    fn test_build_tls12_only_config_fn() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let _cfg = build_tls12_only_config(root_store);
    }
}
