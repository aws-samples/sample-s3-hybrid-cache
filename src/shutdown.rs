//! Graceful Shutdown Module
//!
//! Handles graceful shutdown of all system components including signal handling,
//! in-flight request completion, cache lock release, connection pool cleanup,
//! and flushing of buffered logs and cache size tracking data.

use crate::cache::CacheManager;
use crate::connection_pool::ConnectionPoolManager;
use crate::journal_consolidator::JournalConsolidator;
use crate::logging::LoggerManager;
use crate::{ProxyError, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, Mutex};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Shutdown coordinator for graceful system shutdown
pub struct ShutdownCoordinator {
    cache_manager: Option<Arc<CacheManager>>,
    connection_pool: Option<Arc<Mutex<ConnectionPoolManager>>>,
    /// Logger manager for flushing access log buffer on shutdown
    logger_manager: Option<Arc<Mutex<LoggerManager>>>,
    /// Journal consolidator for running final consolidation and persisting size state
    journal_consolidator: Option<Arc<JournalConsolidator>>,
    shutdown_sender: broadcast::Sender<()>,
    shutdown_timeout: Duration,
}

impl ShutdownCoordinator {
    /// Create new shutdown coordinator
    pub fn new(shutdown_timeout: Duration) -> Self {
        let (shutdown_sender, _) = broadcast::channel(16);

        Self {
            cache_manager: None,
            connection_pool: None,
            logger_manager: None,
            journal_consolidator: None,
            shutdown_sender,
            shutdown_timeout,
        }
    }

    /// Set cache manager reference
    pub fn set_cache_manager(&mut self, cache_manager: Arc<CacheManager>) {
        self.cache_manager = Some(cache_manager);
    }

    /// Set connection pool reference
    pub fn set_connection_pool(&mut self, connection_pool: Arc<Mutex<ConnectionPoolManager>>) {
        self.connection_pool = Some(connection_pool);
    }

    /// Set logger manager reference for flushing access log buffer on shutdown
    pub fn set_logger_manager(&mut self, logger_manager: Arc<Mutex<LoggerManager>>) {
        self.logger_manager = Some(logger_manager);
    }

    /// Set journal consolidator reference for running final consolidation on shutdown
    pub fn set_journal_consolidator(&mut self, journal_consolidator: Arc<JournalConsolidator>) {
        self.journal_consolidator = Some(journal_consolidator);
    }

    /// Get shutdown receiver for components to listen for shutdown signals
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.shutdown_sender.subscribe()
    }

    /// Start listening for shutdown signals
    pub async fn listen_for_shutdown(&self) -> Result<()> {
        info!("Starting shutdown signal listener");

        // Listen for SIGINT (Ctrl+C) and SIGTERM
        let mut sigint =
            signal::unix::signal(signal::unix::SignalKind::interrupt()).map_err(|e| {
                ProxyError::SystemError(format!("Failed to create SIGINT handler: {}", e))
            })?;

        let mut sigterm =
            signal::unix::signal(signal::unix::SignalKind::terminate()).map_err(|e| {
                ProxyError::SystemError(format!("Failed to create SIGTERM handler: {}", e))
            })?;

        tokio::select! {
            _ = sigint.recv() => {
                info!("Received SIGINT, initiating graceful shutdown");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, initiating graceful shutdown");
            }
        }

        self.initiate_shutdown().await
    }

    /// Initiate graceful shutdown sequence
    pub async fn initiate_shutdown(&self) -> Result<()> {
        info!("Initiating graceful shutdown sequence");

        // Send shutdown signal to all components
        if let Err(e) = self.shutdown_sender.send(()) {
            // This can happen if no components are listening for shutdown signals
            // or if all receivers have already been dropped - this is normal during shutdown
            debug!("Shutdown signal not sent (no active receivers): {}", e);
        }

        // Perform shutdown with timeout
        match timeout(self.shutdown_timeout, self.perform_shutdown()).await {
            Ok(result) => match result {
                Ok(()) => {
                    info!("Graceful shutdown completed successfully");
                    Ok(())
                }
                Err(e) => {
                    error!("Error during graceful shutdown: {}", e);
                    Err(e)
                }
            },
            Err(_) => {
                error!(
                    "Graceful shutdown timed out after {:?}, forcing shutdown",
                    self.shutdown_timeout
                );
                Err(ProxyError::TimeoutError(
                    "Graceful shutdown timeout".to_string(),
                ))
            }
        }
    }

    /// Perform the actual shutdown operations
    async fn perform_shutdown(&self) -> Result<()> {
        info!("Performing shutdown operations");

        // Step 1: Flush access log buffer (Requirement 1.6, Task 5.1)
        if let Some(logger_manager) = &self.logger_manager {
            info!("Flushing access log buffer");
            match timeout(
                Duration::from_secs(5),
                self.flush_access_log_buffer(logger_manager),
            )
            .await
            {
                Ok(result) => {
                    if let Err(e) = result {
                        warn!("Error flushing access log buffer: {}", e);
                    }
                }
                Err(_) => {
                    warn!("Access log buffer flush timed out");
                }
            }
        }

        // Step 2: Run final consolidation cycle and persist size state (Requirement 6.1, 6.2, 6.3)
        // This replaces the old cache size tracker checkpoint writing - the consolidator now
        // handles all size tracking persistence via size_state.json
        if let Some(journal_consolidator) = &self.journal_consolidator {
            info!("Running final journal consolidation and persisting size state");
            match timeout(
                Duration::from_secs(15),
                journal_consolidator.shutdown(),
            )
            .await
            {
                Ok(result) => {
                    if let Err(e) = result {
                        warn!("Error during journal consolidator shutdown: {}", e);
                    } else {
                        info!("Journal consolidator shutdown completed successfully");
                    }
                }
                Err(_) => {
                    warn!("Journal consolidator shutdown timed out");
                }
            }
        }

        // Step 3: Release all cache locks
        if let Some(cache_manager) = &self.cache_manager {
            info!("Releasing cache locks");
            match timeout(
                Duration::from_secs(10),
                self.release_cache_locks(cache_manager),
            )
            .await
            {
                Ok(result) => {
                    if let Err(e) = result {
                        warn!("Error releasing cache locks: {}", e);
                    } else {
                        info!("Cache locks released successfully");
                    }
                }
                Err(_) => {
                    warn!("Cache lock release timed out");
                }
            }
        }

        // Step 4: Close connection pools
        if let Some(connection_pool) = &self.connection_pool {
            info!("Closing connection pools");
            match timeout(
                Duration::from_secs(15),
                self.close_connection_pools(connection_pool),
            )
            .await
            {
                Ok(result) => {
                    if let Err(e) = result {
                        warn!("Error closing connection pools: {}", e);
                    } else {
                        info!("Connection pools closed successfully");
                    }
                }
                Err(_) => {
                    warn!("Connection pool closure timed out");
                }
            }
        }

        // Step 5: Final cleanup
        info!("Performing final cleanup");
        self.final_cleanup().await?;

        info!("Shutdown operations completed");
        Ok(())
    }

    /// Release all cache locks held by this instance
    async fn release_cache_locks(&self, cache_manager: &Arc<CacheManager>) -> Result<()> {
        // Release all write locks
        cache_manager.release_all_locks().await?;

        // Flush any pending cache operations
        cache_manager.flush_pending_operations().await?;

        Ok(())
    }

    /// Flush access log buffer to disk (Task 5.1, Task 5.2)
    ///
    /// This ensures all buffered access log entries are written to disk
    /// before shutdown completes. If flush fails, logs the number of entries lost.
    async fn flush_access_log_buffer(
        &self,
        logger_manager: &Arc<Mutex<LoggerManager>>,
    ) -> Result<()> {
        let logger = logger_manager.lock().await;

        // Get pending entries count before flush attempt (for error reporting)
        let pending_count = logger.pending_entries_count();

        match logger.force_flush().await {
            Ok(result) => {
                if result.skipped {
                    debug!("Access log buffer flush skipped (no pending entries)");
                } else if result.already_in_progress {
                    debug!("Access log buffer flush already in progress");
                } else {
                    info!(
                        "Access log buffer flushed: {} entries written to disk",
                        result.entries_flushed
                    );
                }
                Ok(())
            }
            Err(e) => {
                // Task 5.2: Report number of entries lost on flush failure
                if pending_count > 0 {
                    warn!(
                        "Failed to flush access log buffer: {} ({} entries may be lost)",
                        e, pending_count
                    );
                } else {
                    warn!("Failed to flush access log buffer: {}", e);
                }
                Err(e)
            }
        }
    }

    /// Close all connection pools gracefully
    async fn close_connection_pools(
        &self,
        connection_pool: &Arc<Mutex<ConnectionPoolManager>>,
    ) -> Result<()> {
        let mut pool_manager = connection_pool.lock().await;

        // Close all connections gracefully
        pool_manager.close_all_connections().await?;

        Ok(())
    }

    /// Perform final cleanup operations
    async fn final_cleanup(&self) -> Result<()> {
        // Flush logs
        info!("Flushing logs");

        // Give a moment for final log messages to be written
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    /// Force shutdown (used when graceful shutdown fails)
    pub async fn force_shutdown(&self) -> Result<()> {
        warn!("Performing force shutdown");

        // Force close connection pools without waiting
        if let Some(connection_pool) = &self.connection_pool {
            let mut pool_manager = connection_pool.lock().await;
            if let Err(e) = pool_manager.force_close_all_connections().await {
                error!("Error during force connection pool closure: {}", e);
            }
        }

        // Force release cache locks
        if let Some(cache_manager) = &self.cache_manager {
            if let Err(e) = cache_manager.force_release_all_locks().await {
                error!("Error during force cache lock release: {}", e);
            }
        }

        warn!("Force shutdown completed");
        Ok(())
    }
}

/// Shutdown-aware component trait
pub trait ShutdownAware {
    /// Handle shutdown signal gracefully
    fn handle_shutdown(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Check if shutdown has been requested
    fn is_shutdown_requested(&self) -> bool;
}

/// Shutdown signal wrapper for components
pub struct ShutdownSignal {
    receiver: broadcast::Receiver<()>,
    shutdown_requested: bool,
}

impl ShutdownSignal {
    /// Create new shutdown signal from receiver
    pub fn new(receiver: broadcast::Receiver<()>) -> Self {
        Self {
            receiver,
            shutdown_requested: false,
        }
    }

    /// Check if shutdown has been requested (non-blocking)
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested
    }

    /// Wait for shutdown signal
    pub async fn wait_for_shutdown(&mut self) -> Result<()> {
        match self.receiver.recv().await {
            Ok(()) => {
                self.shutdown_requested = true;
                Ok(())
            }
            Err(broadcast::error::RecvError::Closed) => {
                self.shutdown_requested = true;
                Ok(())
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                self.shutdown_requested = true;
                Ok(())
            }
        }
    }

    /// Try to receive shutdown signal without blocking
    pub fn try_recv_shutdown(&mut self) -> bool {
        match self.receiver.try_recv() {
            Ok(()) => {
                self.shutdown_requested = true;
                true
            }
            Err(broadcast::error::TryRecvError::Empty) => false,
            Err(broadcast::error::TryRecvError::Closed) => {
                self.shutdown_requested = true;
                true
            }
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                self.shutdown_requested = true;
                true
            }
        }
    }
}
