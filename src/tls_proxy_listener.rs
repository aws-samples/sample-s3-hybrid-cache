//! TLS Proxy Listener Module
//!
//! Accepts TLS connections using the proxy's own certificate, terminates TLS,
//! and passes the decrypted HTTP stream to the existing HTTP proxy request
//! handler for caching and forwarding. This enables encrypted client-to-proxy
//! traffic while maintaining full caching capability.

use crate::{
    cache::CacheManager, config::Config, http_proxy::HttpProxy, inflight_tracker::InFlightTracker,
    logging::LoggerManager, range_handler::RangeHandler, s3_client::S3ClientApi,
    shutdown::ShutdownSignal, ProxyError, Result,
};
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

/// TLS proxy listener that terminates TLS and delegates to the HTTP proxy pipeline.
///
/// Shares the same `Arc` references as `HttpProxy` so that concurrency limits,
/// metrics, logging, and caching are unified across all listener types.
pub struct TlsProxyListener {
    listen_addr: SocketAddr,
    tls_acceptor: TlsAcceptor,
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
    range_handler: Arc<RangeHandler>,
    request_semaphore: Arc<Semaphore>,
    active_connections: Arc<AtomicUsize>,
    metrics_manager: Option<Arc<RwLock<crate::metrics::MetricsManager>>>,
    logger_manager: Option<Arc<Mutex<LoggerManager>>>,
    inflight_tracker: Arc<InFlightTracker>,
    proxy_referer: Option<String>,
}

impl TlsProxyListener {
    /// Create a new TLS proxy listener.
    ///
    /// Loads the TLS certificate and private key from the given PEM file paths,
    /// builds a `TlsAcceptor`, and stores shared references for request handling.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        listen_addr: SocketAddr,
        cert_path: &str,
        key_path: &str,
        config: Arc<Config>,
        cache_manager: Arc<CacheManager>,
        s3_client: Arc<dyn S3ClientApi + Send + Sync>,
        range_handler: Arc<RangeHandler>,
        request_semaphore: Arc<Semaphore>,
        active_connections: Arc<AtomicUsize>,
        metrics_manager: Option<Arc<RwLock<crate::metrics::MetricsManager>>>,
        logger_manager: Option<Arc<Mutex<LoggerManager>>>,
        inflight_tracker: Arc<InFlightTracker>,
        proxy_referer: Option<String>,
    ) -> Result<Self> {
        let server_config = load_tls_config(cert_path, key_path)?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

        Ok(Self {
            listen_addr,
            tls_acceptor,
            config,
            cache_manager,
            s3_client,
            range_handler,
            request_semaphore,
            active_connections,
            metrics_manager,
            logger_manager,
            inflight_tracker,
            proxy_referer,
        })
    }

    /// Start the TLS proxy listener accept loop.
    ///
    /// Binds to `listen_addr`, accepts TCP connections, performs TLS handshakes,
    /// and delegates decrypted HTTP streams to the HTTP proxy request handler.
    /// Follows the same pattern as `HttpProxy::start()` including shutdown
    /// handling and connection draining.
    pub async fn start(&self, mut shutdown_signal: ShutdownSignal) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!("TLS proxy listener on {}", self.listen_addr);

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            debug!("TLS connection from {}", addr);

                            // Set TCP_NODELAY to disable Nagle's algorithm for lower latency
                            if let Err(e) = stream.set_nodelay(true) {
                                warn!("Failed to set TCP_NODELAY for {}: {}", addr, e);
                            }

                            let tls_acceptor = self.tls_acceptor.clone();
                            let config = Arc::clone(&self.config);
                            let cache_manager = Arc::clone(&self.cache_manager);
                            let s3_client = Arc::clone(&self.s3_client);
                            let range_handler = Arc::clone(&self.range_handler);
                            let request_semaphore = Arc::clone(&self.request_semaphore);
                            let active_connections = Arc::clone(&self.active_connections);
                            let metrics_manager = self.metrics_manager.clone();
                            let logger_manager = self.logger_manager.clone();
                            let inflight_tracker = Arc::clone(&self.inflight_tracker);
                            let proxy_referer = self.proxy_referer.clone();

                            tokio::spawn(async move {
                                // Perform TLS handshake
                                let tls_stream = match tls_acceptor.accept(stream).await {
                                    Ok(tls_stream) => tls_stream,
                                    Err(e) => {
                                        warn!("TLS handshake failed for {}: {}", addr, e);
                                        return;
                                    }
                                };

                                let io = TokioIo::new(tls_stream);

                                // Track active connection
                                active_connections.fetch_add(1, Ordering::Relaxed);

                                let service = service_fn(move |req: Request<Incoming>| {
                                    let config = Arc::clone(&config);
                                    let cache_manager = Arc::clone(&cache_manager);
                                    let s3_client = Arc::clone(&s3_client);
                                    let range_handler = Arc::clone(&range_handler);
                                    let request_semaphore = Arc::clone(&request_semaphore);
                                    let metrics_manager = metrics_manager.clone();
                                    let logger_manager = logger_manager.clone();
                                    let inflight_tracker = Arc::clone(&inflight_tracker);
                                    let proxy_referer = proxy_referer.clone();

                                    async move {
                                        // Intercept CONNECT requests for TCP passthrough
                                        // (e.g., when HTTPS_PROXY is set instead of HTTP_PROXY)
                                        if req.method() == Method::CONNECT {
                                            return handle_connect(req, addr).await;
                                        }

                                        HttpProxy::handle_request(
                                            req,
                                            addr,
                                            config,
                                            cache_manager,
                                            s3_client,
                                            range_handler,
                                            request_semaphore,
                                            metrics_manager,
                                            logger_manager,
                                            inflight_tracker,
                                            proxy_referer,
                                        )
                                        .await
                                    }
                                });

                                if let Err(err) = http1::Builder::new()
                                    .serve_connection(io, service)
                                    .with_upgrades()
                                    .await
                                {
                                    let err_str = err.to_string();
                                    if err_str.contains("connection closed")
                                        || err_str.contains("broken pipe")
                                        || err_str.contains("reset by peer")
                                        || err_str.contains("error writing")
                                        || err_str.contains("write error")
                                        || err.is_canceled()
                                    {
                                        debug!("TLS client disconnected from {}: {}", addr, err);
                                    } else {
                                        error!("Error serving TLS connection from {}: {}", addr, err);
                                    }
                                }

                                // Decrement active connection count
                                active_connections.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept TLS connection: {}", e);
                        }
                    }
                }
                _ = shutdown_signal.wait_for_shutdown() => {
                    info!("TLS proxy listener received shutdown signal, stopping accept loop");
                    break;
                }
            }
        }

        // Drain period: wait for in-flight connections to complete
        let drain_timeout = Duration::from_secs(5);
        let drain_start = std::time::Instant::now();
        let active = self.active_connections.load(Ordering::Relaxed);
        if active > 0 {
            info!(
                "TLS proxy listener draining {} active connections (timeout: {:?})",
                active, drain_timeout
            );
            while self.active_connections.load(Ordering::Relaxed) > 0
                && drain_start.elapsed() < drain_timeout
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let remaining = self.active_connections.load(Ordering::Relaxed);
            if remaining > 0 {
                warn!(
                    "TLS proxy listener shutdown with {} connections still active",
                    remaining
                );
            } else {
                info!("TLS proxy listener all connections drained");
            }
        }

        info!("TLS proxy listener stopped");
        Ok(())
    }
}

/// Handle an HTTP CONNECT request by establishing a TCP tunnel to the target.
///
/// When a client sets `HTTPS_PROXY`, the SDK sends `CONNECT host:443` asking
/// the proxy to create an end-to-end encrypted tunnel. The proxy cannot cache
/// traffic inside the tunnel (it's opaque bytes), but we handle it gracefully
/// as a passthrough — the same behavior as the HTTPS listener on port 443.
///
/// Returns a `200 Connection Established` response. Hyper's upgrade mechanism
/// then hands us the raw stream for bidirectional forwarding.
async fn handle_connect(
    req: Request<Incoming>,
    client_addr: SocketAddr,
) -> std::result::Result<
    Response<http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
    std::convert::Infallible,
> {
    use http_body_util::BodyExt;

    // Extract target host:port from the CONNECT URI (e.g., "s3.us-west-2.amazonaws.com:443")
    let target = req
        .uri()
        .authority()
        .map(|a| a.to_string())
        .unwrap_or_default();

    if target.is_empty() {
        warn!("CONNECT request from {} with empty target", client_addr);
        let resp = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(
                Full::new(Bytes::from("Bad Request: missing CONNECT target\n"))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap();
        return Ok(resp);
    }

    info!(
        "CONNECT tunnel request from {} to {} (HTTPS_PROXY passthrough, no caching)",
        client_addr, target
    );

    // Spawn the tunnel task that runs after the upgrade completes
    let target_clone = target.clone();
    tokio::spawn(async move {
        // Wait for hyper to hand us the upgraded (raw) connection
        match hyper::upgrade::on(req).await {
            Ok(upgraded) => {
                let mut client_io = TokioIo::new(upgraded);

                // Parse host and port from target
                let (host, port) = if let Some(colon) = target_clone.rfind(':') {
                    let h = &target_clone[..colon];
                    let p = target_clone[colon + 1..].parse::<u16>().unwrap_or(443);
                    (h.to_string(), p)
                } else {
                    (target_clone.clone(), 443u16)
                };

                // Connect to the target
                let target_addr = format!("{}:{}", host, port);
                match TcpStream::connect(&target_addr).await {
                    Ok(mut server_stream) => {
                        debug!(
                            "CONNECT tunnel established: {} <-> {}",
                            client_addr, target_addr
                        );

                        // Bidirectional copy
                        match tokio::io::copy_bidirectional(&mut client_io, &mut server_stream)
                            .await
                        {
                            Ok((tx, rx)) => {
                                debug!(
                                    "CONNECT tunnel closed: {} <-> {} | tx={} rx={}",
                                    client_addr, target_addr, tx, rx
                                );
                            }
                            Err(e) => {
                                debug!(
                                    "CONNECT tunnel error: {} <-> {} | {}",
                                    client_addr, target_addr, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!("CONNECT tunnel failed to connect to {}: {}", target_addr, e);
                        // Try to send an error back to the client
                        let _ = AsyncWriteExt::shutdown(&mut client_io).await;
                    }
                }
            }
            Err(e) => {
                warn!("CONNECT upgrade failed for {}: {}", client_addr, e);
            }
        }
    });

    // Return 200 to tell the client the tunnel is established
    let resp = Response::builder()
        .status(StatusCode::OK)
        .body(
            Full::new(Bytes::new())
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap();
    Ok(resp)
}

/// Load TLS certificate chain and private key from PEM files and build a
/// `rustls::ServerConfig`.
///
/// Returns `ProxyError::TlsError` with the file path in the message on any
/// I/O or parsing failure, enabling operators to diagnose misconfiguration at
/// startup rather than at runtime.
fn load_tls_config(cert_path: &str, key_path: &str) -> Result<rustls::ServerConfig> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| ProxyError::TlsError(format!("Cannot read cert file '{}': {}", cert_path, e)))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            ProxyError::TlsError(format!("Invalid certificate in '{}': {}", cert_path, e))
        })?;

    let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key_path).map_err(|e| {
        // from_pem_file returns Error::NoItemsFound when the file contains no
        // recognised private key block — map that to the same message as before.
        let msg = format!("{}", e);
        if msg.contains("NoItemsFound") || msg.contains("no items") {
            ProxyError::TlsError(format!("No private key found in '{}'", key_path))
        } else {
            ProxyError::TlsError(format!("Invalid private key in '{}': {}", key_path, e))
        }
    })?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| ProxyError::TlsError(format!("Certificate/key mismatch: {}", e)))?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// Generate a file path from alphanumeric + '/' characters that is guaranteed
    /// not to exist on disk. We prefix with a non-existent root directory to
    /// ensure no accidental collision with real files.
    fn make_nonexistent_path(seed: Vec<u8>) -> String {
        let chars: String = seed
            .iter()
            .map(|b| {
                let idx = (*b as usize) % 37; // 26 letters + 10 digits + '/'
                if idx < 26 {
                    (b'a' + idx as u8) as char
                } else if idx < 36 {
                    (b'0' + (idx - 26) as u8) as char
                } else {
                    '/'
                }
            })
            .collect();
        // Prefix with a directory that cannot exist to avoid real file collisions
        format!("/nonexistent_qc_test_dir/{}", chars)
    }

    /// **Feature: tls-proxy-listener, Property 8: Certificate loading errors include file paths**
    ///
    /// *For any* non-existent file path provided as `cert_path`, the error message
    /// returned by `load_tls_config` SHALL contain the cert file path string.
    /// Since the cert file is opened first, a non-existent cert path always triggers
    /// the cert error path.
    ///
    /// **Validates: Requirements 9.2, 9.3**
    #[quickcheck]
    fn prop_cert_loading_error_contains_file_path(seed: Vec<u8>) -> TestResult {
        if seed.is_empty() {
            return TestResult::discard();
        }

        let cert_path = make_nonexistent_path(seed.clone());
        let key_path = make_nonexistent_path(seed.iter().map(|b| b.wrapping_add(1)).collect());

        // Both paths are non-existent, so load_tls_config must fail on the cert path first
        let result = load_tls_config(&cert_path, &key_path);
        match result {
            Ok(_) => TestResult::failed(), // Should never succeed with non-existent files
            Err(e) => {
                let error_msg = e.to_string();
                TestResult::from_bool(error_msg.contains(&cert_path))
            }
        }
    }
}
