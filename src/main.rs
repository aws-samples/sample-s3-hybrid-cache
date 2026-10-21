use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use s3_proxy::{
    accept_policy::AcceptErrorLog,
    config::{Config, ServerMode},
    dashboard::DashboardServer,
    health::HealthManager,
    http_proxy::HttpProxy,
    https_proxy::HttpsProxy,
    logging::LoggerManager,
    metrics::MetricsManager,
    permissions::PermissionValidator,
    shutdown::{ShutdownCoordinator, ShutdownSignal},
    tls_proxy_listener::TlsProxyListener,
    upstream_overrides::UpstreamOverrides,
    Result,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Return the HTTP response for a request that does not match a dedicated listener's route.
///
/// Health and metrics run on separate ports, but each port still accepts arbitrary HTTP
/// requests. Restricting dispatch here makes the configured `endpoint` meaningful and
/// prevents a scrape of an accidental path from being treated as valid service data.
fn endpoint_rejection<B>(request: &Request<B>, endpoint: &str) -> Option<Response<String>> {
    if request.uri().path() != endpoint {
        return Some(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(String::new())
                .expect("static 404 response must be valid"),
        );
    }

    if request.method() != Method::GET {
        return Some(
            Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header("Allow", "GET")
                .body(String::new())
                .expect("static 405 response must be valid"),
        );
    }

    None
}

/// Accept loop shared by the health and metrics listeners.
///
/// An accept error never ends the loop: it is logged (rate-limited) and, unless it concerns only
/// the connection being accepted, followed by a pause. The policy lives in
/// `s3_proxy::accept_policy`, shared with the dashboard, HTTP, TCP passthrough and TLS listeners.
///
/// `accept` yields the next connection, `handle` takes ownership of it (and is expected to
/// spawn), and the loop ends when `shutdown` resolves. Taking the accept source as a closure
/// lets a test feed the loop an error followed by a real connection.
async fn run_accept_loop<S, A, AF, H, SF>(
    listener_name: &'static str,
    mut accept: A,
    mut handle: H,
    shutdown: SF,
) -> Result<()>
where
    A: FnMut() -> AF,
    AF: std::future::Future<Output = std::io::Result<S>>,
    H: FnMut(S),
    SF: std::future::Future,
{
    tokio::pin!(shutdown);
    let mut accept_errors = AcceptErrorLog::new();
    loop {
        tokio::select! {
            accept_result = accept() => {
                match accept_result {
                    Ok(stream) => handle(stream),
                    Err(e) => accept_errors.on_accept_error(listener_name, &e).await,
                }
            }
            _ = &mut shutdown => {
                info!("{} received shutdown signal", listener_name);
                break;
            }
        }
    }
    Ok(())
}

/// Start health check HTTP server
async fn start_health_server(
    addr: SocketAddr,
    endpoint: String,
    health_manager: Arc<RwLock<HealthManager>>,
    mut shutdown_signal: ShutdownSignal,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        s3_proxy::ProxyError::IoError(format!("Failed to bind health server: {}", e))
    })?;

    info!("Health check server listening on {}", addr);

    let listener = &listener;
    run_accept_loop(
        "Health check server",
        move || async move { listener.accept().await.map(|(stream, _)| stream) },
        |stream| {
            let io = TokioIo::new(stream);
            let health_manager_clone = health_manager.clone();
            let endpoint = endpoint.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let health_manager = health_manager_clone.clone();
                    let endpoint = endpoint.clone();
                    async move {
                        if let Some(response) = endpoint_rejection(&req, &endpoint) {
                            return Ok(response);
                        }
                        health_manager.read().await.handle_health_request(req).await
                    }
                });

                if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                    error!("Error serving health check connection: {}", e);
                }
            });
        },
        shutdown_signal.wait_for_shutdown(),
    )
    .await
}

/// Start metrics HTTP server
async fn start_metrics_server(
    addr: SocketAddr,
    endpoint: String,
    metrics_manager: Arc<RwLock<MetricsManager>>,
    mut shutdown_signal: ShutdownSignal,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        s3_proxy::ProxyError::IoError(format!("Failed to bind metrics server: {}", e))
    })?;

    info!("Metrics server listening on {}", addr);

    let listener = &listener;
    run_accept_loop(
        "Metrics server",
        move || async move { listener.accept().await.map(|(stream, _)| stream) },
        |stream| {
            let io = TokioIo::new(stream);
            let metrics_manager_clone = metrics_manager.clone();
            let endpoint = endpoint.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let metrics_manager = metrics_manager_clone.clone();
                    let endpoint = endpoint.clone();
                    async move {
                        if let Some(response) = endpoint_rejection(&req, &endpoint) {
                            return Ok(response);
                        }
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
        },
        shutdown_signal.wait_for_shutdown(),
    )
    .await
}

/// Set process umask to 0o077 so all files are created with owner-only permissions (Req 14).
/// On non-Unix platforms, logs a warning since umask is not available.
#[cfg(unix)]
fn set_process_umask() {
    // SAFETY: libc::umask is a simple syscall that sets the process file creation mask.
    // It has no unsafe memory implications and always succeeds on Unix.
    unsafe {
        libc::umask(0o077);
    }
}

#[cfg(not(unix))]
fn set_process_umask() {
    // Cannot set umask on non-Unix platforms; warn at startup.
    // Note: tracing is not yet initialized at this point, so use eprintln as a fallback.
    eprintln!("WARN: umask not available on this platform; cache files may have permissive modes");
}

/// The name of the ledger-retirement subcommand. `bench-metadata` lives in
/// `s3_proxy::bench_metadata`.
const RETIRE_INSTANCE: &str = "retire-instance";

/// `s3-proxy --config <path> retire-instance <instance-id>`.
///
/// A separate `clap` command rather than a subcommand of the proxy's own CLI, so the proxy's
/// startup parse is untouched and this command can never fall through into starting a proxy.
fn retire_instance_cli() -> clap::Command {
    clap::Command::new("s3-proxy")
        .about("Retire a dead write-ledger instance id from staged-authority activation")
        .arg(
            clap::Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .required(true)
                .help("The proxy's configuration file; only cache.cache_dir is read"),
        )
        .subcommand_required(true)
        .subcommand(
            clap::Command::new(RETIRE_INSTANCE)
                .about(
                    "Stop a write-ledger instance id with no current ready record from holding \
                     staged-authority activation back. Refused if any ledger file named for it \
                     was modified within the last hour.",
                )
                .arg(
                    clap::Arg::new("instance-id")
                        .required(true)
                        .value_name("INSTANCE_ID")
                        .help("{hostname}:{pid}, as write_cache.staged_authority_activation_blocking_instance names it"),
                ),
        )
}

/// `None` when the command line does not ask for `retire-instance`, so normal startup proceeds.
/// Otherwise the parse: `(config path, instance id)`, or clap's error.
fn parse_retire_instance(
    args: &[std::ffi::OsString],
) -> Option<std::result::Result<(String, String), clap::Error>> {
    if !args.iter().skip(1).any(|a| a == RETIRE_INSTANCE) {
        return None;
    }
    Some(retire_instance_cli().try_get_matches_from(args).map(|m| {
        let config = m.get_one::<String>("config").cloned().unwrap_or_default();
        let id = m
            .subcommand_matches(RETIRE_INSTANCE)
            .and_then(|s| s.get_one::<String>("instance-id").cloned())
            .unwrap_or_default();
        (config, id)
    }))
}

/// Run `retire-instance` and return the process exit code: 0 retired, 1 refused, 2 usage.
fn run_retire_instance(parsed: std::result::Result<(String, String), clap::Error>) -> i32 {
    let (config_path, instance_id) = match parsed {
        Ok(p) => p,
        Err(e) => {
            let _ = e.print();
            return 2;
        }
    };
    let cache_dir = match std::fs::read_to_string(&config_path)
        .map_err(|e| e.to_string())
        .and_then(|c| serde_yaml_ng::from_str::<Config>(&c).map_err(|e| e.to_string()))
    {
        Ok(mut config) => {
            // `CACHE_DIR` overrides the file at startup, so it must here too.
            config.apply_env_overrides();
            config.cache.cache_dir
        }
        Err(e) => {
            eprintln!("Could not load the configuration file {config_path}: {e}");
            return 2;
        }
    };
    match s3_proxy::staged_authority::retire_instance(&cache_dir, &instance_id) {
        Ok(marker) => {
            println!(
                "Retired {id}: wrote {path}. It no longer counts against staged-authority \
                 activation while no write-ledger file named for it is modified after the newest \
                 modification recorded ({ns} ns since the epoch). Any later write under that id \
                 voids the marker.",
                id = marker.instance_id,
                path = s3_proxy::staged_authority::retired_instances_dir(&cache_dir)
                    .join(format!("{}.retired", marker.instance_id))
                    .display(),
                ns = marker.ledger_last_modified_ns,
            );
            0
        }
        Err(refusal) => {
            eprintln!("Refused to retire {instance_id}: {refusal}. Nothing was written.");
            1
        }
    }
}

/// Bound on the runtime's blocking-pool wait at process exit.
///
/// `Drop for BlockingPool` waits for every *running* blocking task with no timeout, and
/// several of this crate's blocking tasks wait on file locks that live on the **shared**
/// cache volume — so the holder can be a peer instance, or an NFS client that died holding
/// the lease. Without a bound, process exit is outside this process's control.
///
/// Five seconds, chosen against two ceilings. Below: every blocking task whose work matters
/// is awaited inside `async_main` before it returns (the graceful path's log flush, final
/// journal consolidation and lock release are all `await`ed by `ShutdownCoordinator`), and
/// what can still be outstanding here is local file I/O that completes in milliseconds, so
/// 5 s is three orders of magnitude of headroom. Above: the graceful path is itself bounded
/// at 30 s, plus a 500 ms drain, giving a 35.5 s worst-case exit — well inside systemd's
/// 90 s `TimeoutStopSec` default, so the process *exits* rather than being `SIGKILL`ed.
const SHUTDOWN_BLOCKING_TIMEOUT: Duration = Duration::from_secs(5);

/// Entry point.
///
/// Deliberately not `#[tokio::main]`: that macro drops the runtime implicitly, and that drop
/// is an untimed wait on the blocking pool (see `SHUTDOWN_BLOCKING_TIMEOUT`). This builds
/// the same runtime the macro would — multi-threaded, no explicit `worker_threads` so
/// tokio's default applies, `enable_all()`, and the same panic message if the build fails —
/// and ends with a bounded shutdown instead.
fn main() -> Result<()> {
    // The one administrative command, handled before any runtime, logging or listener exists
    // so it cannot start a proxy by accident. Design § 16.6.6 B9.
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    if let Some(parsed) = parse_retire_instance(&args) {
        set_process_umask();
        std::process::exit(run_retire_instance(parsed));
    }
    // `bench-metadata` likewise exits before any proxy state exists. It is read-only against
    // the cache volume; see `s3_proxy::bench_metadata`.
    if s3_proxy::bench_metadata::is_requested(&args) {
        std::process::exit(s3_proxy::bench_metadata::main_entry(&args));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed building the Runtime");
    let result = runtime.block_on(async_main());
    runtime.shutdown_timeout(SHUTDOWN_BLOCKING_TIMEOUT);
    result
}

async fn async_main() -> Result<()> {
    // Set restrictive umask before any file creation (Req 14)
    set_process_umask();

    // Load configuration, then validate directory permissions before initializing any
    // components (Requirement 21). Logging cannot start until the configuration names its
    // directory and level, so the events both steps emit (the removed, ignored and remapped
    // field warnings among them) are held here and replayed once it has.
    let (loaded, startup_events) = s3_proxy::logging::collect_startup_events(|| {
        let config = Config::load()?;
        PermissionValidator::validate_all(
            &config.cache.cache_dir,
            &config.logging.access_log_dir,
            &config.logging.app_log_dir,
            config.cache.write_cache_enabled,
        )?;
        Ok::<Config, s3_proxy::ProxyError>(config)
    });
    let config = loaded?;

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
        access_log_retention_days: config.logging.access_log_retention_days,
        app_log_retention_days: config.logging.app_log_retention_days,
        log_cleanup_interval: config.logging.log_cleanup_interval,
        access_log_file_rotation_interval: config.logging.access_log_file_rotation_interval,
    };

    let mut logger = LoggerManager::new(logging_config);
    logger.initialize()?;
    startup_events.replay();

    info!(
        "Starting Hybrid Cache for Amazon S3 server v{} (built: {})",
        env!("CARGO_PKG_VERSION"),
        env!("BUILD_TIMESTAMP")
    );
    // Log mode-specific port information
    match config.server.mode {
        ServerMode::Standard => {
            info!("Server mode: standard");
            info!("HTTP port: {}", config.server.http_port);
            info!("HTTPS port: {}", config.server.https_port);
        }
        ServerMode::ProxyOnly => {
            info!("Server mode: proxy_only");
            info!("HTTP forward proxy port: {}", config.server.proxy_port);
        }
    }

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
    // Advisory: an NFS cache volume must be mounted with lookupcache=pos.
    s3_proxy::permissions::warn_if_nfs_caches_negative_lookups(&config.cache.cache_dir);
    // One-time breaking-change migration warning: the per-bucket `_settings.json`
    // mechanism has been replaced by `cache_dir/cache_rules.json`. Scan once at
    // startup and warn (without reading/honouring the files) if any are present.
    // (Requirements 7.1, 7.4)
    s3_proxy::bucket_settings::warn_if_legacy_settings_present(&config.cache.cache_dir);
    if config.cache.ram_cache_enabled {
        info!(
            "RAM cache: enabled, max_size={}MB",
            config.cache.max_ram_cache_size / 1024 / 1024
        );
        // `ram_cache_flush_interval` governs no flush. Its two consumers are the
        // cross-instance access-activity window (2x it) and the eviction activity
        // oracle's refresh cadence (half it). Reporting it under a "flush" label
        // alongside a field nothing read is how tuning a dead knob came to look
        // like it had taken effect, so this line names the two real effects. The
        // field's own name stays as it is: renaming a live config key to fix a log
        // line would be a breaking change for a cosmetic gain.
        info!(
            "RAM access-activity window: {:?}; eviction activity oracle refresh: {:?} \
             (from cache.ram_cache_flush_interval={:?})",
            config.cache.ram_cache_flush_interval * 2,
            config.cache.ram_cache_flush_interval / 2,
            config.cache.ram_cache_flush_interval
        );
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
        // Derive the first Staged_Range_Budget verdict now, off the request path, so the
        // first write-through PUTs after a start do not wait on it.
        s3_proxy::cache::kick_staged_range_budget_verdict(&config.cache.cache_dir);
    } else {
        info!("Write cache: disabled");
    }

    // Warn per protection-waiving upstream override (Requirement 1.6, 1.7).
    // Startup-only diagnostic build of the matcher: cheap, runs once, and is
    // separate from the connector/S3Client matcher used on the hot path. Only
    // Plaintext / TlsUnvalidated entries are surfaced; TlsValidated waives
    // nothing and is intentionally silent.
    for entry in UpstreamOverrides::from_config(&config.connection_pool.upstream_overrides)
        .protection_waiving_entries()
    {
        warn!(
            endpoint = %entry.endpoint,
            "Upstream override waives a security protection: {}",
            entry.waived_protection()
        );
    }

    // Initialize shutdown coordinator
    let mut shutdown_coordinator = ShutdownCoordinator::new(Duration::from_secs(30));

    // Create HTTP proxy and optionally HTTPS proxy based on server mode
    let config_arc = std::sync::Arc::new(config.clone());
    let (mut http_proxy, https_proxy) = match config.server.mode {
        ServerMode::Standard => {
            // Standard mode: HTTP on http_port, HTTPS on https_port
            let http_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], config.server.http_port));
            let http_proxy = HttpProxy::new(http_addr, config_arc.clone())?;

            let https_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], config.server.https_port));
            let https_proxy = HttpsProxy::new(https_addr, config_arc.clone());

            (http_proxy, Some(https_proxy))
        }
        ServerMode::ProxyOnly => {
            // Proxy-only mode: HTTP forward proxy on proxy_port, no HTTPS passthrough
            let proxy_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], config.server.proxy_port));
            let http_proxy = HttpProxy::new(proxy_addr, config_arc.clone())?;

            info!(
                "Proxy-only mode: HTTP direct listener (port {}) not started",
                config.server.http_port
            );
            info!(
                "Proxy-only mode: HTTPS passthrough listener (port {}) not started",
                config.server.https_port
            );

            (http_proxy, None)
        }
    };

    // Set logger manager on HTTP proxy for access logging
    let logger_manager = Arc::new(tokio::sync::Mutex::new(logger));
    http_proxy.set_logger_manager(logger_manager.clone());

    // Task 5.1: Set logger manager on shutdown coordinator for flushing access log buffer
    shutdown_coordinator.set_logger_manager(logger_manager.clone());

    // Get references to shared components from HTTP proxy for health/metrics monitoring
    // Note: We use the HTTP proxy's components since both proxies create their own instances
    let cache_manager_ref = http_proxy.get_cache_manager();
    let connection_pool_ref = http_proxy.get_connection_pool();
    let compression_handler_ref = http_proxy.get_compression_handler();
    let s3_client_ref = http_proxy.get_s3_client();

    // Wire cache manager and connection pool into shutdown coordinator
    shutdown_coordinator.set_cache_manager(cache_manager_ref.clone());
    shutdown_coordinator.set_connection_pool(connection_pool_ref.clone());
    // The node-local pending-stamp log, read and re-queued before any listener accepts:
    // from here on this proxy declines stores and fills of every key it holds a pending stamp
    // for. Its retries start once `HttpProxy::start` has initialized the cache manager, so their
    // invalidations reach the size accounting.
    cache_manager_ref.open_stamp_backlog(&config.logging.local_state_dir);
    {
        let cache_manager = cache_manager_ref.clone();
        let mut ready = http_proxy.ready_receiver();
        tokio::spawn(async move {
            if ready.wait_for(|ready| *ready).await.is_ok() {
                cache_manager.start_stamp_backlog_retries();
            }
        });
    }

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
    // Wire per-bucket accounting knobs from config.
    // Spec: per-bucket-metrics. Requirements: 5.1, 7.1
    metrics_manager.set_per_bucket_config(
        config.metrics.per_bucket.max_series,
        config.metrics.per_bucket.bucket_prefixes.clone(),
    );
    // The background collector's period, and the base of the slow-collection threshold. Already
    // clamped by `MetricsConfig::validate_and_clamp` during config load.
    metrics_manager.set_collection_interval(config.metrics.collection_interval);

    // Initialize OTLP if enabled
    if let Err(e) = metrics_manager
        .initialize_otlp(config.metrics.otlp.clone())
        .await
    {
        error!("Failed to initialize OTLP metrics exporter: {}", e);
    }

    // Enable per-bucket OTLP observable counters when the flag is set.
    // Must be called after initialize_otlp and before the Arc wrapping.
    // Spec: per-bucket-metrics. Requirements: 4.1, 4.3
    if config.metrics.otlp.per_bucket_enabled {
        metrics_manager.enable_per_bucket_otlp().await;
    }

    let metrics_manager = Arc::new(RwLock::new(metrics_manager));

    // Set metrics manager reference on cache manager for eviction coordination metrics
    cache_manager_ref
        .set_metrics_manager(metrics_manager.clone())
        .await;

    // NOTE: there is deliberately no cache-size-tracker wiring step here.
    // `MetricsManager` reads the tracker through the cache manager at collection
    // time instead. A wiring step at this point could only ever observe `None`:
    // the tracker is created by `CacheManager::initialize()`, which runs inside
    // the `HttpProxy::start()` task spawned further down this function, so it
    // has not been built yet. That is what made `cache_size` report `null` for
    // the life of every process from v1.1.x until 2.8.3. Do not reintroduce it.

    // Set metrics manager reference on HTTP proxy for signed PUT caching metrics
    http_proxy.set_metrics_manager(metrics_manager.clone());

    // Wire active connections counter into metrics manager for concurrent request tracking
    metrics_manager.write().await.set_active_connections(
        http_proxy.get_active_connections(),
        config.server.max_concurrent_requests,
    );

    // Wire the request-concurrency semaphore into metrics for permit observability
    // (permits_held, permits_available). Requirement: TCA 5.1, 5.2
    metrics_manager
        .write()
        .await
        .set_request_semaphore(http_proxy.get_request_semaphore());

    // Wire the in-flight buffered-byte ledger and RAM cache ceiling into metrics
    // for observability (inflight_memory.*). Requirement: IMA 8.1-8.7
    {
        let mut mm = metrics_manager.write().await;
        mm.set_inflight_ledger(http_proxy.get_inflight_ledger());
        mm.set_ram_cache_max_bytes(config.cache.max_ram_cache_size);
    }

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
                            Ok(_) => {}
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
        // The lock watchdog runs on its own OS thread, so it still fires when a stuck cycle pins
        // the tokio worker this task runs on. Stopped when the task leaves its loop at shutdown.
        let lock_watchdog_threshold = consolidator.lock_watchdog_threshold();
        let lock_watchdog = consolidator.start_lock_watchdog(lock_watchdog_threshold);
        info!(
            "Started consolidation lock watchdog (threshold: {}s)",
            lock_watchdog_threshold.as_secs()
        );
        let mut consolidation_ready = http_proxy.ready_receiver();
        let _journal_consolidation_task = tokio::spawn(async move {
            // The first tick fires immediately, so wait for `CacheManager::initialize` first:
            // it runs `consolidator.initialize()` (dead-instance journal and temp reaping) and
            // gives the consolidator its cache-manager and size-tracker refs, without which a
            // cycle cannot trigger eviction.
            let ready = tokio::select! {
                r = consolidation_ready.wait_for(|ready| *ready) => r.is_ok(),
                _ = consolidation_shutdown.wait_for_shutdown() => false,
            };
            if !ready {
                info!(
                    "Journal consolidation background task not started: cache initialization did not complete or shutdown came first"
                );
                let _ = tokio::task::spawn_blocking(move || lock_watchdog.stop()).await;
                return;
            }
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
                                    // No bytes-evicted figure: eviction is a detached task
                                    // (v1.1.35) and has only just been spawned. The figure is
                                    // logged by that task ("Background eviction completed").
                                    // `write_cache_delta` is last so the existing fields keep
                                    // their positions; a write-cache-only cycle that triggers
                                    // eviction otherwise reads `size_delta=+0` with no figure.
                                    info!(
                                        "Consolidation cycle: entries={}, size_delta={:+}, eviction_triggered={}, total_cache_size={}, write_cache_delta={:+}",
                                        result.entries_consolidated,
                                        result.size_delta,
                                        result.eviction_triggered,
                                        result.current_size,
                                        result.write_cache_delta
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
            // Joins within one poll interval (at most 1 s), so off the async worker.
            let _ = tokio::task::spawn_blocking(move || lock_watchdog.stop()).await;
        });
    } else {
        debug!("Journal consolidation disabled (shared storage mode not enabled)");
    }

    // Start background DNS refresh task for IP distribution
    // Periodically re-resolves S3 endpoints so the round-robin distributor stays current
    // as S3 rotates its IP addresses. Ticks every pool_check_interval (default: 10s); on
    // each tick `refresh_dns` re-resolves only the endpoints whose dns_refresh_interval
    // (default: 60s) has elapsed since their last resolution (connection_pool.rs).
    if config.connection_pool.ip_distribution_enabled {
        let dns_refresh_interval = config.connection_pool.pool_check_interval;
        let dns_refresh_client = s3_client_ref.clone();
        let mut dns_refresh_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());

        info!(
            "Starting DNS refresh background task (interval: {}s)",
            dns_refresh_interval.as_secs()
        );

        let _dns_refresh_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(dns_refresh_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = dns_refresh_client.refresh_dns().await {
                            warn!("DNS refresh failed: {}", e);
                        }
                        // Recovery probing shares this tick deliberately, and runs
                        // after the refresh: the refresh holds health-excluded IPs
                        // out of the distributor, and the probe is the only thing
                        // that puts them back. Eligibility is governed by
                        // health_probe_initial_cooldown / health_probe_max_cooldown,
                        // so this interval only bounds how promptly an elapsed
                        // cooldown is noticed. Sharing the task also keeps the
                        // shutdown subscriber count unchanged.
                        let readmitted = dns_refresh_client.probe_unhealthy_ips().await;
                        if readmitted > 0 {
                            info!("Recovery probe re-admitted {} IP(s) to their distributors", readmitted);
                        }
                    }
                    _ = dns_refresh_shutdown.wait_for_shutdown() => {
                        info!("DNS refresh background task received shutdown signal");
                        break;
                    }
                }
            }
        });
    } else {
        debug!("DNS refresh disabled (ip_distribution_enabled=false)");
    }

    // Start background log cleanup task
    let cleanup_logger = logger_manager.clone();
    let cleanup_interval = config.logging.log_cleanup_interval;
    let access_retention = config.logging.access_log_retention_days;
    let app_retention = config.logging.app_log_retention_days;
    // Captured as owned strings so the cleanup task can name the directories it
    // swept without holding a borrow on `config`.
    let access_log_dir_display = config.logging.access_log_dir.display().to_string();
    let app_log_dir_display = config.logging.app_log_dir.display().to_string();
    let mut cleanup_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());

    info!(
        "Starting log cleanup background task (interval: {}s, access_retention: {}d, app_retention: {}d)",
        cleanup_interval.as_secs(), access_retention, app_retention
    );

    let _log_cleanup_task = tokio::spawn(async move {
        // Run cleanup once immediately at startup
        {
            let logger = cleanup_logger.lock().await;
            match logger.rotate_logs(access_retention, app_retention) {
                // Name the directories actually walked, and split deletions by
                // tier. A non-zero adopted count is the only signal that an
                // instance which used to write here has stopped cleaning up
                // after itself. Reporting the paths is also what stops this line
                // reading as a successful sweep when no sweep happened, which is
                // how a gated access-log sweep looked identical to an empty one.
                Ok(result) => info!(
                    "Startup log cleanup: access={} {} deleted ({} own, {} adopted), app={} {} deleted ({} own, {} adopted), {} errors",
                    access_log_dir_display,
                    result.access_files_deleted,
                    result.access_files_deleted - result.access_files_adopted,
                    result.access_files_adopted,
                    app_log_dir_display,
                    result.app_files_deleted,
                    result.app_files_deleted - result.app_files_adopted,
                    result.app_files_adopted,
                    result.errors
                ),
                Err(e) => warn!("Startup log cleanup failed: {}", e),
            }
        }

        let mut interval = tokio::time::interval(cleanup_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let logger = cleanup_logger.lock().await;
                    match logger.rotate_logs(access_retention, app_retention) {
                        Ok(result) => {
                            if result.access_files_deleted > 0 || result.app_files_deleted > 0 || result.errors > 0 {
                                info!("Log cleanup: access={} {} deleted ({} own, {} adopted), app={} {} deleted ({} own, {} adopted), {} errors",
                                    access_log_dir_display,
                                    result.access_files_deleted,
                                    result.access_files_deleted - result.access_files_adopted,
                                    result.access_files_adopted,
                                    app_log_dir_display,
                                    result.app_files_deleted,
                                    result.app_files_deleted - result.app_files_adopted,
                                    result.app_files_adopted,
                                    result.errors);
                            }
                        }
                        Err(e) => warn!("Log cleanup failed: {}", e),
                    }
                }
                _ = cleanup_shutdown.wait_for_shutdown() => {
                    info!("Log cleanup task received shutdown signal");
                    break;
                }
            }
        }
    });

    // Start the background metrics collector.
    //
    // One full collection per `metrics.collection_interval`, starting now. `/metrics` requests and
    // dashboard refreshes serve its latest result for the shared-volume figures and read every
    // in-process figure live, and it is the only thing that records the OTLP gauges — so without
    // it, OTLP exported nothing unless something happened to request /metrics.
    //
    // Gated on there being a reader: the /metrics listener, the OTLP exporter, or the dashboard.
    // With none of the three, nothing can observe a collection, so none is paid for. OTLP is a
    // reader in its own right, deliberately: `metrics.enabled: false` with `otlp.enabled: true` is
    // the configuration in which a scrape-driven export could never have run at all.
    //
    // Bound at THIS scope, like `_health_task` below, so the handle lives as long as the process.
    // Spawned after the `metrics_manager.write()` wiring calls above, so they cannot queue behind
    // the first collection's read guard. The one exception is `set_hedge_metrics`, which
    // `HttpProxy::set_metrics_manager` issues from a task of its own: it may wait for one
    // collection, once, and new readers queue behind it for that long (tokio's RwLock is fair).
    // See `spawn_background_collector` for that bound.
    let _metrics_collector_task =
        if config.metrics.enabled || config.metrics.otlp.enabled || config.dashboard.enabled {
            info!(
                "Starting background metrics collector (interval: {}s)",
                config.metrics.collection_interval.as_secs()
            );
            Some(s3_proxy::metrics::spawn_background_collector(
                metrics_manager.clone(),
                config.metrics.collection_interval,
                ShutdownSignal::new(shutdown_coordinator.subscribe()),
            ))
        } else {
            debug!(
                "Background metrics collector not started: /metrics, OTLP export and the \
                 dashboard are all disabled"
            );
            None
        };

    // Start health and metrics servers if enabled
    let _health_task = if config.health.enabled {
        info!(
            "Starting health check server on {}:{}",
            config.health.bind_address, config.health.port
        );
        // `Config::load` has already required an IP address here; a value that still
        // fails to parse stops startup rather than widening the bind.
        let health_bind: std::net::IpAddr = config.health.bind_address.parse().map_err(|_| {
            s3_proxy::ProxyError::ConfigError(format!(
                "health.bind_address must be an IP address, got {:?}",
                config.health.bind_address
            ))
        })?;
        let health_addr = SocketAddr::from((health_bind, config.health.port));
        let health_endpoint = config.health.endpoint.clone();
        let health_manager_clone = health_manager.clone();
        let health_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());

        Some(tokio::spawn(async move {
            if let Err(e) = start_health_server(
                health_addr,
                health_endpoint,
                health_manager_clone,
                health_shutdown,
            )
            .await
            {
                error!("Health check server failed: {}", e);
            }
        }))
    } else {
        None
    };

    let _metrics_task = if config.metrics.enabled {
        info!(
            "Starting metrics server on {}:{}",
            config.metrics.bind_address, config.metrics.port
        );
        // `Config::load` has already required an IP address here; a value that still
        // fails to parse stops startup rather than widening the bind.
        let metrics_bind: std::net::IpAddr = config.metrics.bind_address.parse().map_err(|_| {
            s3_proxy::ProxyError::ConfigError(format!(
                "metrics.bind_address must be an IP address, got {:?}",
                config.metrics.bind_address
            ))
        })?;
        let metrics_addr = SocketAddr::from((metrics_bind, config.metrics.port));
        let metrics_endpoint = config.metrics.endpoint.clone();
        let metrics_manager_clone = metrics_manager.clone();
        let metrics_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());

        Some(tokio::spawn(async move {
            if let Err(e) = start_metrics_server(
                metrics_addr,
                metrics_endpoint,
                metrics_manager_clone,
                metrics_shutdown,
            )
            .await
            {
                error!("Metrics server failed: {}", e);
            }
        }))
    } else {
        None
    };

    // Subscribe shutdown signals for HTTP proxy (and HTTPS if in standard mode)
    let http_shutdown_rx = shutdown_coordinator.subscribe();
    let https_shutdown_rx = if https_proxy.is_some() {
        Some(shutdown_coordinator.subscribe())
    } else {
        None
    };

    // Start TLS proxy listener if enabled
    let _tls_task = if let Some(tls_config) = &config.server.tls {
        if tls_config.enabled {
            let tls_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], tls_config.tls_proxy_port));
            match TlsProxyListener::new(
                tls_addr,
                &tls_config.cert_path,
                &tls_config.key_path,
                http_proxy.get_config(),
                http_proxy.get_cache_manager(),
                http_proxy.get_s3_client(),
                http_proxy.get_range_handler(),
                http_proxy.get_request_semaphore(),
                http_proxy.get_active_connections(),
                http_proxy.get_metrics_manager(),
                http_proxy.get_logger_manager(),
                http_proxy.get_inflight_tracker(),
                http_proxy.get_proxy_referer(),
            ) {
                Ok(tls_listener) => {
                    info!("TLS proxy port: {}", tls_config.tls_proxy_port);
                    let tls_shutdown = ShutdownSignal::new(shutdown_coordinator.subscribe());
                    let mut tls_ready = http_proxy.ready_receiver();
                    Some(tokio::spawn(async move {
                        // Bind now so early clients queue in the backlog, then accept only
                        // once the HTTP proxy has finished initializing the shared cache.
                        let listener = match tls_listener.bind().await {
                            Ok(listener) => listener,
                            Err(e) => {
                                error!("TLS proxy listener failed to bind: {}", e);
                                return;
                            }
                        };
                        if tls_ready.wait_for(|ready| *ready).await.is_err() {
                            error!(
                                "TLS proxy listener not started: cache initialization did not complete"
                            );
                            return;
                        }
                        if let Err(e) = tls_listener.serve(listener, tls_shutdown).await {
                            error!("TLS proxy listener failed: {}", e);
                        }
                    }))
                }
                Err(e) => {
                    error!(
                        "Failed to start TLS proxy listener: {}. HTTP/HTTPS proxies will continue.",
                        e
                    );
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

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
        dashboard_server
            .set_active_connections(
                http_proxy.get_active_connections(),
                config.server.max_concurrent_requests,
            )
            .await;

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

    // Start HTTP proxy with shutdown signal
    let http_shutdown = ShutdownSignal::new(http_shutdown_rx);
    let _http_task = tokio::spawn(async move {
        if let Err(e) = http_proxy.start(http_shutdown).await {
            error!("HTTP proxy failed: {}", e);
        }
    });

    // Start HTTPS proxy only in standard mode
    let _https_task = if let Some(https_proxy) = https_proxy {
        let https_shutdown = ShutdownSignal::new(
            https_shutdown_rx.expect("HTTPS shutdown receiver must exist in standard mode"),
        );
        Some(tokio::spawn(async move {
            if let Err(e) = https_proxy.start(https_shutdown).await {
                error!("HTTPS proxy failed: {}", e);
            }
        }))
    } else {
        None
    };

    // Wait for shutdown to complete — all components receive the broadcast signal
    // and stop their accept loops. The shutdown_task runs the coordinator's teardown
    // (flush logs, final consolidation, release locks, close pools).
    shutdown_task.await.ok();
    info!("Shutdown coordinator completed, waiting for server tasks");

    // Give server tasks a moment to finish after receiving their shutdown signals
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("Hybrid Cache for Amazon S3 shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Red side 1 (wiring half): `async_main` spawns the background metrics collector, outside
    /// any test-only code, before it waits for shutdown, and with the configured interval.
    ///
    /// A source pin rather than a behavioural test because `async_main` binds real ports and runs
    /// until a signal, so it cannot be driven from a unit test. The collector's own behaviour is
    /// tested in `metrics.rs` (`the_background_collector_collects_without_any_request`); this is
    /// the half that catches a collector that works and is never started, which is how an orphan
    /// sweep once ran zero cycles in 458 boots. Red side: removing the spawn call from
    /// `async_main` fails the first assertion.
    #[test]
    fn the_background_metrics_collector_is_spawned_by_async_main() {
        let source = include_str!("main.rs");
        let body_start = source
            .find("async fn async_main()")
            .expect("async_main must exist");
        let body_end = source[body_start..]
            .find("shutdown_task.await")
            .map(|i| body_start + i)
            .expect("async_main must await the shutdown task");
        let body = &source[body_start..body_end];
        let spawn = body.find("spawn_background_collector(").expect(
            "async_main must spawn the background metrics collector before it waits for \
             shutdown; without it OTLP exports nothing unless /metrics is requested, and every \
             shared-volume figure on /metrics stays at its UNREADABLE sentinel forever",
        );
        assert!(
            body[spawn..].contains("config.metrics.collection_interval"),
            "the collector must run at the configured metrics.collection_interval"
        );
        let test_module = source
            .find("#[cfg(test)]\nmod tests")
            .expect("test module marker");
        assert!(
            body_start + spawn < test_module,
            "the spawn must be in production code, not in the test module"
        );
    }

    fn os(args: &[&str]) -> Vec<std::ffi::OsString> {
        args.iter().map(std::ffi::OsString::from).collect()
    }

    /// `s3-proxy --config <path> retire-instance <id>` parses to the path and the id; a normal
    /// startup command line is not taken for it; a missing id or config is a usage error.
    #[test]
    fn retire_instance_parses_only_when_asked_for() {
        let parsed = parse_retire_instance(&os(&[
            "s3-proxy",
            "--config",
            "/etc/s3-proxy/config.yaml",
            "retire-instance",
            "ip-172-31-34-221:10268",
        ]))
        .expect("requested")
        .expect("valid");
        assert_eq!(
            parsed,
            (
                "/etc/s3-proxy/config.yaml".to_string(),
                "ip-172-31-34-221:10268".to_string()
            )
        );
        assert!(
            parse_retire_instance(&os(&["s3-proxy", "-c", "/etc/s3-proxy/config.yaml"])).is_none()
        );
        assert!(parse_retire_instance(&os(&[
            "s3-proxy",
            "--config",
            "/x.yaml",
            "retire-instance"
        ]))
        .expect("requested")
        .is_err());
        assert!(
            parse_retire_instance(&os(&["s3-proxy", "retire-instance", "host:1"]))
                .expect("requested")
                .is_err()
        );
    }

    /// A refusal exits 1 and writes nothing; a usage error exits 2.
    #[test]
    fn retire_instance_exit_codes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = tmp.path().join("config.yaml");
        std::fs::write(
            &config,
            format!(
                "cache:\n  cache_dir: {}\n",
                tmp.path().join("cache").display()
            ),
        )
        .unwrap();
        let code = run_retire_instance(Ok((
            config.display().to_string(),
            "host-none:1".to_string(),
        )));
        assert_eq!(code, 1, "no ledger file names the id, so it is refused");
        assert!(!tmp
            .path()
            .join("cache/metadata/_shard_state/retired_instances")
            .exists());
        let code = run_retire_instance(Ok((
            tmp.path().join("missing.yaml").display().to_string(),
            "host:1".to_string(),
        )));
        assert_eq!(code, 2);
    }

    #[test]
    fn endpoint_rejection_allows_only_get_on_the_configured_path() {
        let allowed = Request::builder()
            .method(Method::GET)
            .uri("/ready")
            .body(())
            .expect("test request must build");
        assert!(endpoint_rejection(&allowed, "/ready").is_none());

        let wrong_path = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .body(())
            .expect("test request must build");
        assert_eq!(
            endpoint_rejection(&wrong_path, "/ready")
                .expect("wrong path must be rejected")
                .status(),
            StatusCode::NOT_FOUND
        );

        let wrong_method = Request::builder()
            .method(Method::POST)
            .uri("/ready")
            .body(())
            .expect("test request must build");
        let response =
            endpoint_rejection(&wrong_method, "/ready").expect("wrong method must be rejected");
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get("allow").unwrap(), "GET");
    }

    /// The health and metrics listeners share `run_accept_loop`. An accept error, whether a
    /// per-connection one (ECONNABORTED) or resource exhaustion (EMFILE), must not end the
    /// listener: the next real connection is still served.
    ///
    /// Red side: a loop that propagates the accept error with `?` returns on the first error,
    /// drops the listener, and the client's connection is never served.
    #[tokio::test]
    async fn a_listener_serves_the_next_connection_after_accept_errors() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let loop_task = tokio::spawn(async move {
            let listener = listener;
            let mut injected = vec![
                std::io::Error::from_raw_os_error(libc::EMFILE),
                std::io::Error::from_raw_os_error(libc::ECONNABORTED),
            ];
            let listener = &listener;
            let injected = &mut injected;
            run_accept_loop(
                "Test listener",
                move || {
                    let next_error = injected.pop();
                    async move {
                        match next_error {
                            Some(e) => Err(e),
                            None => listener.accept().await.map(|(stream, _)| stream),
                        }
                    }
                },
                |mut stream: tokio::net::TcpStream| {
                    tokio::spawn(async move {
                        let _ = stream.write_all(b"served").await;
                        let _ = stream.shutdown().await;
                    });
                },
                std::future::pending::<()>(),
            )
            .await
        });

        let served = tokio::time::timeout(Duration::from_secs(5), async {
            let mut client = tokio::net::TcpStream::connect(addr).await?;
            let mut body = Vec::new();
            client.read_to_end(&mut body).await?;
            Ok::<_, std::io::Error>(body)
        })
        .await;

        let loop_state = if loop_task.is_finished() {
            format!("the accept loop had returned: {:?}", loop_task.await)
        } else {
            loop_task.abort();
            "the accept loop was still running".to_string()
        };
        match served {
            Ok(Ok(body)) if body == b"served" => {}
            other => panic!(
                "a connection after two accept errors (ECONNABORTED, then EMFILE) was not \
                 served ({:?}); {}",
                other, loop_state
            ),
        }
    }
}
