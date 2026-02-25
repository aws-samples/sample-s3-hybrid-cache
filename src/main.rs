use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use s3_proxy::{
    background_recovery::{BackgroundRecoveryConfig, BackgroundRecoverySystem},
    config::Config,
    dashboard::DashboardServer,
    health::HealthManager,
    http_proxy::HttpProxy,
    https_proxy::HttpsProxy,
    logging::LoggerManager,
    metrics::MetricsManager,
    orphaned_range_recovery::OrphanedRangeRecovery,
    permissions::PermissionValidator,
    shutdown::{ShutdownCoordinator, ShutdownSignal},
    Result,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Start health check HTTP server
async fn start_health_server(
    addr: SocketAddr,
    health_manager: Arc<RwLock<HealthManager>>,
    mut shutdown_signal: ShutdownSignal,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        s3_proxy::ProxyError::IoError(format!("Failed to bind health server: {}", e))
    })?;

    info!("Health check server listening on {}", addr);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _) = accept_result.map_err(|e| {
                    s3_proxy::ProxyError::IoError(format!("Failed to accept connection: {}", e))
                })?;

                let io = TokioIo::new(stream);
                let health_manager_clone = health_manager.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let health_manager = health_manager_clone.clone();
                        async move { health_manager.read().await.handle_health_request(req).await }
                    });

                    if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                        error!("Error serving health check connection: {}", e);
                    }
                });
            }
            _ = shutdown_signal.wait_for_shutdown() => {
                info!("Health check server received shutdown signal");
                break;
            }
        }
    }

    Ok(())
}

/// Start metrics HTTP server
async fn start_metrics_server(
    addr: SocketAddr,
    metrics_manager: Arc<RwLock<MetricsManager>>,
    mut shutdown_signal: ShutdownSignal,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        s3_proxy::ProxyError::IoError(format!("Failed to bind metrics server: {}", e))
    })?;

    info!("Metrics server listening on {}", addr);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _) = accept_result.map_err(|e| {
                    s3_proxy::ProxyError::IoError(format!("Failed to accept connection: {}", e))
                })?;

                let io = TokioIo::new(stream);
                let metrics_manager_clone = metrics_manager.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let metrics_manager = metrics_manager_clone.clone();
                        async move {
                            metrics_manager
                                .read()
                                .await
                                .handle_metrics_request(req)
                                .await
                        }
                    });

                    if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                        error!("Error serving metrics connection: {}", e);
                    }
                });
            }
            _ = shutdown_signal.wait_for_shutdown() => {
                info!("Metrics server received shutdown signal");
                break;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration
    let config = Config::load()?;

    // Validate directory permissions before initializing any components (Requirement 21)
    PermissionValidator::validate_all(
        &config.cache.cache_dir,
        &config.logging.access_log_dir,
        &config.logging.app_log_dir,
        config.cache.write_cache_enabled,
    )?;

    // Initialize logging
    let logging_config = s3_proxy::logging::LoggingConfig {
        access_log_dir: config.logging.access_log_dir.clone(),
        app_log_dir: config.logging.app_log_dir.clone(),
        access_log_enabled: config.logging.access_log_enabled,
        access_log_mode: match config.logging.access_log_mode {
            s3_proxy::config::AccessLogMode::All => s3_proxy::logging::AccessLogMode::All,
            s3_proxy::config::AccessLogMode::CachedOnly => {
                s3_proxy::logging::AccessLogMode::CachedOnly
            }
        },
        hostname: gethostname::gethostname().to_string_lossy().to_string(),
        log_level: config.logging.log_level.clone(),
        // Task 4.3: Pass config values for buffered access logging
        access_log_flush_interval: config.logging.access_log_flush_interval,
        access_log_buffer_size: config.logging.access_log_buffer_size,
    };

    let mut logger = LoggerManager::new(logging_config);
    logger.initialize()?;

    info!(
        "Starting S3 Proxy server v{} (built: {})",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_TIMESTAMP")
    );
    info!("HTTP port: {}", config.server.http_port);

    // Log cache configuration
    info!(
        "Cache eviction algorithm: {:?}",
        config.cache.eviction_algorithm
    );
    info!(
        "Disk cache: dir={}, max_size={}MB",
        config.cache.cache_dir.display(),
        config.cache.max_cache_size / 1024 / 1024
    );
    if config.cache.ram_cache_enabled {
        info!(
            "RAM cache: enabled, max_size={}MB",
            config.cache.max_ram_cache_size / 1024 / 1024
        );
        info!("RAM-disk coherency: flush_interval={:?}, flush_threshold={}, verification_interval={:?}",
            config.cache.ram_cache_flush_interval,
            config.cache.ram_cache_flush_threshold,
            config.cache.ram_cache_verification_interval);
    } else {
        info!("RAM cache: disabled");
    }

    // Log write cache status
    if config.cache.write_cache_enabled {
        info!(
            "Write cache: enabled, max_object_size={}MB, put_ttl={:?}",
            config.cache.write_cache_max_object_size / 1024 / 1024,
            config.cache.put_ttl
        );
    } else {
        info!("Write cache: disabled");
    }

    // Initialize shutdown coordinator
    let mut shutdown_coordinator = ShutdownCoordinator::new(Duration::from_secs(30));

    // Create HTTP proxy (always enabled on port 80)
    // Bind to [::] for IPv6 dual-stack (accepts both IPv4 and IPv6)
    let http_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], config.server.http_port));
    let mut http_proxy = HttpProxy::new(http_addr, std::sync::Arc::new(config.clone()))?;

    // Set logger manager on HTTP proxy for access logging
    let logger_manager = Arc::new(tokio::sync::Mutex::new(logger));
    http_proxy.set_logger_manager(logger_manager.clone());

    // Task 5.1: Set logger manager on shutdown coordinator for flushing access log buffer
    shutdown_coordinator.set_logger_manager(logger_manager.clone());

    // Create HTTPS proxy based on mode
    // Bind to [::] for IPv6 dual-stack (accepts both IPv4 and IPv6)
    let https_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], config.server.https_port));
    let https_proxy = HttpsProxy::new(https_addr, std::sync::Arc::new(config.clone()));

    // Get references to shared components from HTTP proxy for health/metrics monitoring
    // Note: We use the HTTP proxy's components since both proxies create their own instances
    let cache_manager_ref = http_proxy.get_cache_manager();
    let connection_pool_ref = http_proxy.get_connection_pool();
    let compression_handler_ref = http_proxy.get_compression_handler();

    // Wire cache manager and connection pool into shutdown coordinator
    shutdown_coordinator.set_cache_manager(cache_manager_ref.clone());
    shutdown_coordinator.set_connection_pool(connection_pool_ref.clone());

    // Initialize health manager with component references
    let mut health_manager = HealthManager::new();
    health_manager.set_cache_manager(cache_manager_ref.clone());
    health_manager.set_connection_pool(connection_pool_ref.clone());
    health_manager.set_compression_handler(compression_handler_ref.clone());
    let health_manager = Arc::new(RwLock::new(health_manager));

    // Initialize metrics manager with component references
    let mut metrics_manager = MetricsManager::new();
    metrics_manager.set_cache_manager(cache_manager_ref.clone());
    metrics_manager.set_connection_pool(connection_pool_ref.clone());
    metrics_manager.set_compression_handler(compression_handler_ref.clone());

    // Initialize OTLP if enabled
    if let Err(e) = metrics_manager
        .initialize_otlp(config.metrics.otlp.clone())
        .await
    {
        error!("Failed to initialize OTLP metrics exporter: {}", e);
    }

    let metrics_manager = Arc::new(RwLock::new(metrics_manager));

    // Set metrics manager reference on cache manager for eviction coordination metrics
    cache_manager_ref
        .set_metrics_manager(metrics_manager.clone())
        .await;

    // Set cache size tracker on metrics manager for cache_size metrics
    if let Some(size_tracker) = cache_manager_ref.get_size_tracker().await {
        metrics_manager
            .write()
            .await
            .set_cache_size_tracker(size_tracker);
    }

    // Set metrics manager reference on HTTP proxy for signed PUT caching metrics
    http_proxy.set_metrics_manager(metrics_manager.clone());

    // Start background cache hit update buffer flush task (for journal-based cache-hit updates)
    let cache_hit_update_buffer = cache_manager_ref.get_cache_hit_update_buffer().await;
    if let Some(buffer) = cache_hit_update_buffer {
        info!("Starting cache hit update buffer flush background task (interval: 5s)");
        let mut cache_hit_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());
        let _cache_hit_flush_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Flush buffered cache-hit updates to journal
                        match buffer.flush().await {
                            Ok(result) => {
                                if result.entries_flushed > 0 {
                                    debug!(
                                        "Cache hit update buffer flushed: entries={}, duration={}ms",
                                        result.entries_flushed, result.duration_ms
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Cache hit update buffer flush failed: {}", e);
                            }
                        }
                    }
                    _ = cache_hit_shutdown.wait_for_shutdown() => {
                        info!("Cache hit update buffer flush task received shutdown signal");
                        // Final flush before stopping
                        match buffer.flush().await {
                            Ok(result) => {
                                if result.entries_flushed > 0 {
                                    info!("Cache hit update buffer final flush: {} entries", result.entries_flushed);
                                }
                            }
                            Err(e) => {
                                warn!("Cache hit update buffer final flush failed: {}", e);
                            }
                        }
                        break;
                    }
                }
            }
        });
    } else {
        debug!("Cache hit update buffer flush disabled (shared storage mode not enabled)");
    }

    // Start background journal consolidation task (for atomic metadata writes and size tracking)
    let journal_consolidator = cache_manager_ref.get_journal_consolidator().await;
    let consolidation_interval = config.cache.shared_storage.consolidation_interval;
    if let Some(consolidator) = journal_consolidator {
        // Set journal consolidator on shutdown coordinator for final consolidation on shutdown
        shutdown_coordinator.set_journal_consolidator(consolidator.clone());

        info!(
            "Starting journal consolidation background task (interval: {}s)",
            consolidation_interval.as_secs()
        );
        let mut consolidation_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());
        let _journal_consolidation_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(consolidation_interval);
            // Use Delay behavior: if a cycle takes longer than the interval,
            // skip missed ticks rather than bursting to catch up
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Run consolidation cycle - handles discovery, consolidation, size tracking,
                        // eviction triggering, and cleanup all in one call
                        match consolidator.run_consolidation_cycle().await {
                            Ok(result) => {
                                if result.entries_consolidated > 0 || result.eviction_triggered {
                                    info!(
                                        "Consolidation cycle: entries={}, size_delta={:+}, evicted={}, current_size={}",
                                        result.entries_consolidated,
                                        result.size_delta,
                                        result.bytes_evicted,
                                        result.current_size
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Consolidation cycle failed: {}", e);
                            }
                        }
                    }
                    _ = consolidation_shutdown.wait_for_shutdown() => {
                        info!("Journal consolidation background task received shutdown signal");
                        break;
                    }
                }
            }
        });
    } else {
        debug!("Journal consolidation disabled (shared storage mode not enabled)");
    }

    // Start background orphan recovery system
    // This recovers orphaned .bin files that aren't tracked in metadata due to journal consolidation lag
    if config.cache.shared_storage.orphan_recovery_enabled {
        let hybrid_writer = cache_manager_ref.get_hybrid_metadata_writer().await;
        if let Some(writer) = hybrid_writer {
            info!(
                "Starting background orphan recovery system (interval: {}s, max_per_cycle: {})",
                config
                    .cache
                    .shared_storage
                    .orphan_recovery_interval
                    .as_secs(),
                config.cache.shared_storage.orphan_max_per_cycle
            );

            let orphaned_recovery = Arc::new(OrphanedRangeRecovery::new(
                config.cache.cache_dir.clone(),
                writer,
            ));

            let recovery_config =
                BackgroundRecoveryConfig::from_shared_storage_config(&config.cache.shared_storage);

            let mut background_recovery =
                BackgroundRecoverySystem::new(orphaned_recovery, recovery_config);

            // Start the background recovery system
            if let Err(e) = background_recovery.start().await {
                error!("Failed to start background orphan recovery system: {}", e);
            } else {
                info!("Background orphan recovery system started successfully");
            }
        } else {
            warn!("Cannot start orphan recovery: HybridMetadataWriter not available");
        }
    } else {
        debug!(
            "Background orphan recovery disabled (orphan_recovery_enabled={})",
            config.cache.shared_storage.orphan_recovery_enabled
        );
    }

    // Start health and metrics servers if enabled
    let _health_task = if config.health.enabled {
        info!(
            "Starting health check server on port {}",
            config.health.port
        );
        // Bind to [::] for IPv6 dual-stack (accepts both IPv4 and IPv6)
        let health_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], config.health.port));
        let health_manager_clone = health_manager.clone();
        let health_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());

        Some(tokio::spawn(async move {
            if let Err(e) = start_health_server(health_addr, health_manager_clone, health_shutdown).await {
                error!("Health check server failed: {}", e);
            }
        }))
    } else {
        None
    };

    let _metrics_task = if config.metrics.enabled {
        info!("Starting metrics server on port {}", config.metrics.port);
        // Bind to [::] for IPv6 dual-stack (accepts both IPv4 and IPv6)
        let metrics_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], config.metrics.port));
        let metrics_manager_clone = metrics_manager.clone();
        let metrics_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());

        Some(tokio::spawn(async move {
            if let Err(e) = start_metrics_server(metrics_addr, metrics_manager_clone, metrics_shutdown).await {
                error!("Metrics server failed: {}", e);
            }
        }))
    } else {
        None
    };

    // Subscribe shutdown signals for HTTP and HTTPS proxies before coordinator is moved
    let http_shutdown_rx = shutdown_coordinator.subscribe();
    let https_shutdown_rx = shutdown_coordinator.subscribe();

    // Start dashboard server if enabled
    let _dashboard_task = if config.dashboard.enabled {
        info!(
            "Starting dashboard server on port {}",
            config.dashboard.port
        );

        // Create dashboard server with configuration
        let dashboard_config = Arc::new(config.dashboard.clone());
        let mut dashboard_server =
            DashboardServer::new(dashboard_config, config.logging.app_log_dir.clone());

        // Set component references
        dashboard_server
            .set_cache_manager(cache_manager_ref.clone())
            .await;
        dashboard_server
            .set_metrics_manager(metrics_manager.clone())
            .await;
        dashboard_server.set_logger_manager(logger_manager.clone());

        // Get shutdown signal for dashboard
        let dashboard_shutdown = shutdown_coordinator.subscribe();
        let dashboard_shutdown_signal = ShutdownSignal::new(dashboard_shutdown);

        Some(tokio::spawn(async move {
            if let Err(e) = dashboard_server.start(dashboard_shutdown_signal).await {
                error!("Dashboard server failed: {}", e);
            }
        }))
    } else {
        None
    };

    // Start shutdown listener
    let shutdown_task = tokio::spawn(async move {
        if let Err(e) = shutdown_coordinator.listen_for_shutdown().await {
            error!("Shutdown coordinator failed: {}", e);
        }
    });

    // Start both HTTP and HTTPS proxies with shutdown signals
    let http_shutdown = ShutdownSignal::new(http_shutdown_rx);
    let _http_task = tokio::spawn(async move {
        if let Err(e) = http_proxy.start(http_shutdown).await {
            error!("HTTP proxy failed: {}", e);
        }
    });

    let https_shutdown = ShutdownSignal::new(https_shutdown_rx);
    let _https_task = tokio::spawn(async move {
        if let Err(e) = https_proxy.start(https_shutdown).await {
            error!("HTTPS proxy failed: {}", e);
        }
    });

    // Wait for shutdown to complete — all components receive the broadcast signal
    // and stop their accept loops. The shutdown_task runs the coordinator's teardown
    // (flush logs, final consolidation, release locks, close pools).
    shutdown_task.await.ok();
    info!("Shutdown coordinator completed, waiting for server tasks");

    // Give server tasks a moment to finish after receiving their shutdown signals
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("S3 Proxy shutdown complete");
    Ok(())
}
