//! Background Recovery System
//!
//! Provides non-blocking background recovery of orphaned range files with priority-based processing.
//! This system runs independently of cache operations and ensures recovery doesn't block other operations.

use crate::orphaned_range_recovery::{
    OrphanedRange, OrphanedRangeRecovery, RecoveryResult, ValidationStatus,
};
use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::time::{interval, sleep, Instant};
use tracing::{debug, error, info, warn};

/// Background recovery system manager
pub struct BackgroundRecoverySystem {
    orphaned_recovery: Arc<OrphanedRangeRecovery>,
    config: BackgroundRecoveryConfig,
    metrics: Arc<BackgroundRecoveryMetrics>,
    recovery_queue: Arc<RwLock<RecoveryQueue>>,
    operation_semaphore: Arc<Semaphore>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    is_running: Arc<Mutex<bool>>,
}

/// Configuration for background recovery system
#[derive(Debug, Clone)]
pub struct BackgroundRecoveryConfig {
    /// Interval between full scans for orphaned ranges
    pub scan_interval: Duration,
    /// Maximum number of concurrent recovery operations
    pub max_concurrent_operations: usize,
    /// Maximum time to spend on recovery per scan cycle
    pub max_recovery_time_per_cycle: Duration,
    /// Minimum access frequency to prioritize recovery
    pub min_access_frequency_threshold: u64,
    /// Maximum number of orphans to process per cycle
    pub max_orphans_per_cycle: usize,
    /// Delay between individual recovery operations
    pub recovery_operation_delay: Duration,
    /// Enable priority-based recovery ordering
    pub enable_priority_recovery: bool,
    /// Maximum time to spend scanning per cycle (for sharded scanning)
    pub scan_timeout: Duration,
}

impl BackgroundRecoveryConfig {
    /// Create config from SharedStorageConfig
    pub fn from_shared_storage_config(config: &crate::config::SharedStorageConfig) -> Self {
        Self {
            scan_interval: config.orphan_recovery_interval,
            max_concurrent_operations: config.recovery_max_concurrent,
            max_recovery_time_per_cycle: Duration::from_secs(60),
            min_access_frequency_threshold: 5,
            max_orphans_per_cycle: config.orphan_max_per_cycle,
            recovery_operation_delay: Duration::from_millis(50),
            enable_priority_recovery: true,
            scan_timeout: config.orphan_scan_timeout,
        }
    }
}

/// Metrics for background recovery operations
#[derive(Debug, Default)]
pub struct BackgroundRecoveryMetrics {
    pub scan_cycles_completed: std::sync::atomic::AtomicU64,
    pub recovery_cycles_completed: std::sync::atomic::AtomicU64,
    pub total_scan_time: std::sync::atomic::AtomicU64, // milliseconds
    pub total_recovery_time: std::sync::atomic::AtomicU64, // milliseconds
    pub concurrent_operations_peak: std::sync::atomic::AtomicU64,
    pub recovery_queue_size_peak: std::sync::atomic::AtomicU64,
    pub background_failures: std::sync::atomic::AtomicU64,
    pub s3_fallback_count: std::sync::atomic::AtomicU64,
}

/// Priority-based recovery queue
#[derive(Debug, Default)]
struct RecoveryQueue {
    high_priority: Vec<PrioritizedOrphan>,
    medium_priority: Vec<PrioritizedOrphan>,
    low_priority: Vec<PrioritizedOrphan>,
    processing: HashMap<String, Instant>, // cache_key -> start_time
}

/// Orphaned range with priority information for background recovery
///
/// Wraps an `OrphanedRange` with metadata used for priority-based queue management.
/// Orphans are sorted into high/medium/low priority queues based on `priority_score`
/// and processed in order during recovery cycles.
#[derive(Debug, Clone)]
struct PrioritizedOrphan {
    /// The orphaned range to recover
    orphan: OrphanedRange,
    /// Priority score (0-100) for queue ordering. Higher scores are processed first.
    /// Calculated based on file size, access patterns, and recency.
    priority_score: u64,
}

/// Recovery operation status
#[derive(Debug, Clone)]
pub enum RecoveryOperationStatus {
    Queued,
    Processing,
    Completed(RecoveryResult),
    Failed(String),
    Skipped(String),
}

impl Default for BackgroundRecoveryConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(300), // 5 minutes
            max_concurrent_operations: 3,
            max_recovery_time_per_cycle: Duration::from_secs(60), // 1 minute
            min_access_frequency_threshold: 5,
            max_orphans_per_cycle: 100,
            recovery_operation_delay: Duration::from_millis(50),
            enable_priority_recovery: true,
            scan_timeout: Duration::from_secs(30),
        }
    }
}

impl BackgroundRecoverySystem {
    /// Create a new background recovery system
    pub fn new(
        orphaned_recovery: Arc<OrphanedRangeRecovery>,
        config: BackgroundRecoveryConfig,
    ) -> Self {
        let operation_semaphore = Arc::new(Semaphore::new(config.max_concurrent_operations));

        Self {
            orphaned_recovery,
            config,
            metrics: Arc::new(BackgroundRecoveryMetrics::default()),
            recovery_queue: Arc::new(RwLock::new(RecoveryQueue::default())),
            operation_semaphore,
            shutdown_tx: None,
            is_running: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the background recovery system
    pub async fn start(&mut self) -> Result<()> {
        let mut is_running = self.is_running.lock().unwrap();
        if *is_running {
            return Err(ProxyError::CacheError(
                "Background recovery system is already running".to_string(),
            ));
        }
        *is_running = true;
        drop(is_running);

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Clone necessary components for the background task
        let orphaned_recovery = self.orphaned_recovery.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let recovery_queue = self.recovery_queue.clone();
        let operation_semaphore = self.operation_semaphore.clone();
        let is_running = self.is_running.clone();

        // Spawn the main background recovery task
        tokio::spawn(async move {
            info!("Starting background recovery system");

            let mut scan_interval = interval(config.scan_interval);
            scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = scan_interval.tick() => {
                        if let Err(e) = Self::run_recovery_cycle(
                            &orphaned_recovery,
                            &config,
                            &metrics,
                            &recovery_queue,
                            &operation_semaphore,
                        ).await {
                            error!("Background recovery cycle failed: {}", e);
                            metrics.background_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Background recovery system shutting down");
                        break;
                    }
                }
            }

            let mut is_running = is_running.lock().unwrap();
            *is_running = false;
            info!("Background recovery system stopped");
        });

        info!(
            "Background recovery system started with config: {:?}",
            self.config
        );
        Ok(())
    }

    /// Stop the background recovery system
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(()).await;

            // Wait for system to stop
            let mut attempts = 0;
            while attempts < 50 {
                // 5 seconds max wait
                {
                    let is_running = self.is_running.lock().unwrap();
                    if !*is_running {
                        break;
                    }
                }
                sleep(Duration::from_millis(100)).await;
                attempts += 1;
            }
        }

        Ok(())
    }

    /// Check if the background recovery system is running
    pub fn is_running(&self) -> bool {
        *self.is_running.lock().unwrap()
    }

    /// Get background recovery metrics
    pub fn get_metrics(&self) -> Arc<BackgroundRecoveryMetrics> {
        self.metrics.clone()
    }

    /// Get current recovery queue status
    pub async fn get_queue_status(&self) -> RecoveryQueueStatus {
        let queue = self.recovery_queue.read().await;
        RecoveryQueueStatus {
            high_priority_count: queue.high_priority.len(),
            medium_priority_count: queue.medium_priority.len(),
            low_priority_count: queue.low_priority.len(),
            processing_count: queue.processing.len(),
        }
    }

    /// Manually trigger a recovery scan (non-blocking)
    pub async fn trigger_scan(&self) -> Result<()> {
        if !self.is_running() {
            return Err(ProxyError::CacheError(
                "Background recovery system is not running".to_string(),
            ));
        }

        // Spawn a one-off scan task
        let orphaned_recovery = self.orphaned_recovery.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let recovery_queue = self.recovery_queue.clone();
        let operation_semaphore = self.operation_semaphore.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::run_recovery_cycle(
                &orphaned_recovery,
                &config,
                &metrics,
                &recovery_queue,
                &operation_semaphore,
            )
            .await
            {
                error!("Manual recovery scan failed: {}", e);
                metrics
                    .background_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });

        Ok(())
    }

    /// Main recovery cycle implementation using sharded scanning for scalability
    async fn run_recovery_cycle(
        orphaned_recovery: &Arc<OrphanedRangeRecovery>,
        config: &BackgroundRecoveryConfig,
        metrics: &Arc<BackgroundRecoveryMetrics>,
        recovery_queue: &Arc<RwLock<RecoveryQueue>>,
        operation_semaphore: &Arc<Semaphore>,
    ) -> Result<()> {
        let cycle_start = Instant::now();
        debug!("Starting background recovery cycle (sharded scanning)");

        // Phase 1: Sharded scan for orphaned ranges
        // Instead of scanning everything, scan one shard per bucket per cycle
        let scan_start = Instant::now();

        // Get list of buckets
        let buckets = match orphaned_recovery.list_buckets() {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to list buckets for orphan scan: {}", e);
                return Err(e);
            }
        };

        if buckets.is_empty() {
            debug!("No buckets found, skipping orphan scan");
            return Ok(());
        }

        let mut all_orphans = HashMap::new();
        let mut total_orphans_found = 0;

        // Scan one shard per bucket (round-robin through shards over multiple cycles)
        // Use current time to deterministically select which shard to scan
        let cycle_number = (SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            / config.scan_interval.as_secs()) as usize;

        for bucket in &buckets {
            if scan_start.elapsed() > config.scan_timeout {
                debug!("Scan timeout reached, stopping bucket iteration");
                break;
            }

            if total_orphans_found >= config.max_orphans_per_cycle {
                debug!("Max orphans per cycle reached, stopping scan");
                break;
            }

            // Get shards for this bucket
            let shards = match orphaned_recovery.list_shards(bucket) {
                Ok(s) => s,
                Err(e) => {
                    debug!("Failed to list shards for bucket {}: {}", bucket, e);
                    continue;
                }
            };

            if shards.is_empty() {
                continue;
            }

            // Select shard based on cycle number (round-robin)
            let shard_index = cycle_number % shards.len();
            let shard = &shards[shard_index];

            // Scan this shard
            let remaining_timeout = config.scan_timeout.saturating_sub(scan_start.elapsed());
            let remaining_orphans = config
                .max_orphans_per_cycle
                .saturating_sub(total_orphans_found);

            match orphaned_recovery
                .scan_shard_for_orphans(bucket, shard, remaining_timeout, remaining_orphans)
                .await
            {
                Ok(shard_orphans) => {
                    for (cache_key, orphans) in shard_orphans {
                        total_orphans_found += orphans.len();
                        all_orphans.insert(cache_key, orphans);
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to scan shard {}/{} for orphans: {}",
                        bucket, shard, e
                    );
                }
            }
        }

        let scan_duration = scan_start.elapsed();
        metrics.total_scan_time.fetch_add(
            scan_duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        metrics
            .scan_cycles_completed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if all_orphans.is_empty() {
            debug!("No orphaned ranges found during sharded scan");
            return Ok(());
        }

        info!(
            "Sharded scan found {} orphaned ranges for {} cache keys in {:?}",
            total_orphans_found,
            all_orphans.len(),
            scan_duration
        );

        // Phase 2: Prioritize and queue orphans for recovery
        Self::queue_orphans_for_recovery(all_orphans, config, recovery_queue).await;

        // Phase 3: Process recovery queue (non-blocking, time-limited)
        let recovery_start = Instant::now();
        Self::process_recovery_queue(
            orphaned_recovery,
            config,
            metrics,
            recovery_queue,
            operation_semaphore,
            config.max_recovery_time_per_cycle,
        )
        .await?;

        let recovery_duration = recovery_start.elapsed();
        metrics.total_recovery_time.fetch_add(
            recovery_duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        metrics
            .recovery_cycles_completed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let total_cycle_time = cycle_start.elapsed();
        debug!(
            "Background recovery cycle completed in {:?} (scan: {:?}, recovery: {:?})",
            total_cycle_time, scan_duration, recovery_duration
        );

        Ok(())
    }

    /// Queue orphaned ranges for recovery with priority-based ordering
    async fn queue_orphans_for_recovery(
        all_orphans: HashMap<String, Vec<OrphanedRange>>,
        config: &BackgroundRecoveryConfig,
        recovery_queue: &Arc<RwLock<RecoveryQueue>>,
    ) {
        let mut queue = recovery_queue.write().await;

        for (cache_key, orphans) in all_orphans {
            // Skip if already processing this cache key
            if queue.processing.contains_key(&cache_key) {
                debug!("Skipping cache key {} - already processing", cache_key);
                continue;
            }

            for orphan in orphans {
                // Skip orphans that don't meet minimum criteria
                if orphan.validation_status != ValidationStatus::Valid {
                    debug!("Skipping invalid orphan: {:?}", orphan.range_file_path);
                    continue;
                }

                let priority_score = Self::calculate_priority_score(&orphan, config);
                let prioritized_orphan = PrioritizedOrphan {
                    orphan,
                    priority_score,
                };

                // Queue based on priority
                if priority_score >= 80 {
                    queue.high_priority.push(prioritized_orphan);
                } else if priority_score >= 40 {
                    queue.medium_priority.push(prioritized_orphan);
                } else {
                    queue.low_priority.push(prioritized_orphan);
                }
            }
        }

        // Sort queues by priority score (descending)
        if config.enable_priority_recovery {
            queue
                .high_priority
                .sort_by(|a, b| b.priority_score.cmp(&a.priority_score));
            queue
                .medium_priority
                .sort_by(|a, b| b.priority_score.cmp(&a.priority_score));
            queue
                .low_priority
                .sort_by(|a, b| b.priority_score.cmp(&a.priority_score));
        }

        let total_queued =
            queue.high_priority.len() + queue.medium_priority.len() + queue.low_priority.len();
        debug!(
            "Queued {} orphans for recovery (high: {}, medium: {}, low: {})",
            total_queued,
            queue.high_priority.len(),
            queue.medium_priority.len(),
            queue.low_priority.len()
        );
    }

    /// Process the recovery queue with time limits and concurrency control
    async fn process_recovery_queue(
        orphaned_recovery: &Arc<OrphanedRangeRecovery>,
        config: &BackgroundRecoveryConfig,
        metrics: &Arc<BackgroundRecoveryMetrics>,
        recovery_queue: &Arc<RwLock<RecoveryQueue>>,
        operation_semaphore: &Arc<Semaphore>,
        max_time: Duration,
    ) -> Result<()> {
        let start_time = Instant::now();
        let mut processed_count = 0;

        while start_time.elapsed() < max_time && processed_count < config.max_orphans_per_cycle {
            // Get next orphan to process
            let next_orphan = {
                let mut queue = recovery_queue.write().await;
                Self::get_next_orphan_from_queue(&mut queue)
            };

            let prioritized_orphan = match next_orphan {
                Some(orphan) => orphan,
                None => {
                    debug!("No more orphans in queue");
                    break;
                }
            };

            // Mark as processing
            {
                let mut queue = recovery_queue.write().await;
                queue
                    .processing
                    .insert(prioritized_orphan.orphan.cache_key.clone(), Instant::now());

                // Update peak queue size metric
                let current_processing = queue.processing.len() as u64;
                let current_peak = metrics
                    .concurrent_operations_peak
                    .load(std::sync::atomic::Ordering::Relaxed);
                if current_processing > current_peak {
                    metrics
                        .concurrent_operations_peak
                        .store(current_processing, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Process the orphan (non-blocking)
            let orphaned_recovery_clone = orphaned_recovery.clone();
            let metrics_clone = metrics.clone();
            let recovery_queue_clone = recovery_queue.clone();
            let operation_semaphore_clone = operation_semaphore.clone();
            let cache_key = prioritized_orphan.orphan.cache_key.clone();

            tokio::spawn(async move {
                // Acquire semaphore permit for concurrency control
                let _permit = operation_semaphore_clone.acquire().await.unwrap();

                let result = orphaned_recovery_clone
                    .recover_orphan(&prioritized_orphan.orphan)
                    .await;

                // Remove from processing queue
                {
                    let mut queue = recovery_queue_clone.write().await;
                    queue.processing.remove(&cache_key);
                }

                match result {
                    Ok(RecoveryResult::Recovered(_)) => {
                        debug!("Successfully recovered orphan for cache key: {}", cache_key);
                    }
                    Ok(RecoveryResult::Cleaned(bytes)) => {
                        debug!(
                            "Cleaned up invalid orphan for cache key: {} ({} bytes)",
                            cache_key, bytes
                        );
                    }
                    Ok(RecoveryResult::Failed(error)) => {
                        warn!(
                            "Failed to recover orphan for cache key: {} - {}",
                            cache_key, error
                        );
                        metrics_clone
                            .s3_fallback_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!(
                            "Recovery operation error for cache key: {} - {}",
                            cache_key, e
                        );
                        metrics_clone
                            .background_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });

            processed_count += 1;

            // Small delay between operations to avoid overwhelming the system
            if config.recovery_operation_delay > Duration::ZERO {
                sleep(config.recovery_operation_delay).await;
            }
        }

        debug!("Processed {} orphans in recovery cycle", processed_count);
        Ok(())
    }

    /// Get the next orphan from the priority queue
    fn get_next_orphan_from_queue(queue: &mut RecoveryQueue) -> Option<PrioritizedOrphan> {
        // Try high priority first
        if !queue.high_priority.is_empty() {
            return Some(queue.high_priority.remove(0));
        }

        // Then medium priority
        if !queue.medium_priority.is_empty() {
            return Some(queue.medium_priority.remove(0));
        }

        // Finally low priority
        if !queue.low_priority.is_empty() {
            return Some(queue.low_priority.remove(0));
        }

        None
    }

    /// Calculate priority score for an orphaned range
    pub fn calculate_priority_score(
        orphan: &OrphanedRange,
        config: &BackgroundRecoveryConfig,
    ) -> u64 {
        let mut score = 0u64;

        // Access frequency (0-40 points)
        let frequency_score = (orphan.access_frequency * 40 / 100).min(40);
        score += frequency_score;

        // File size (0-20 points) - larger files get higher priority
        let size_score = if orphan.file_size > 100 * 1024 * 1024 {
            // > 100MB
            20
        } else if orphan.file_size > 10 * 1024 * 1024 {
            // > 10MB
            15
        } else if orphan.file_size > 1024 * 1024 {
            // > 1MB
            10
        } else {
            5
        };
        score += size_score;

        // Recency (0-20 points)
        let recency_score = if let Some(last_accessed) = orphan.last_accessed {
            if let Ok(duration) = SystemTime::now().duration_since(last_accessed) {
                let days_since_access = duration.as_secs() / 86400;
                if days_since_access == 0 {
                    20 // Accessed today
                } else if days_since_access <= 7 {
                    15 // Accessed within a week
                } else if days_since_access <= 30 {
                    10 // Accessed within a month
                } else {
                    5 // Older access
                }
            } else {
                5 // Future timestamp (shouldn't happen)
            }
        } else {
            5 // No access time available
        };
        score += recency_score;

        // Validation status (0-20 points)
        let validation_score = match orphan.validation_status {
            ValidationStatus::Valid => 20,
            ValidationStatus::Unknown => 10,
            ValidationStatus::Invalid => 5,
            ValidationStatus::Corrupted => 0,
        };
        score += validation_score;

        // Apply minimum threshold
        if orphan.access_frequency < config.min_access_frequency_threshold {
            score = score / 2; // Reduce priority for low-frequency orphans
        }

        score
    }
}

/// Status of the recovery queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryQueueStatus {
    pub high_priority_count: usize,
    pub medium_priority_count: usize,
    pub low_priority_count: usize,
    pub processing_count: usize,
}

impl BackgroundRecoveryMetrics {
    /// Get total scan cycles completed
    pub fn get_scan_cycles_completed(&self) -> u64 {
        self.scan_cycles_completed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total recovery cycles completed
    pub fn get_recovery_cycles_completed(&self) -> u64 {
        self.recovery_cycles_completed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get average scan time in milliseconds
    pub fn get_average_scan_time(&self) -> u64 {
        let total_time = self
            .total_scan_time
            .load(std::sync::atomic::Ordering::Relaxed);
        let cycles = self.get_scan_cycles_completed();
        if cycles > 0 {
            total_time / cycles
        } else {
            0
        }
    }

    /// Get average recovery time in milliseconds
    pub fn get_average_recovery_time(&self) -> u64 {
        let total_time = self
            .total_recovery_time
            .load(std::sync::atomic::Ordering::Relaxed);
        let cycles = self.get_recovery_cycles_completed();
        if cycles > 0 {
            total_time / cycles
        } else {
            0
        }
    }

    /// Get peak concurrent operations
    pub fn get_concurrent_operations_peak(&self) -> u64 {
        self.concurrent_operations_peak
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get total background failures
    pub fn get_background_failures(&self) -> u64 {
        self.background_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get S3 fallback count
    pub fn get_s3_fallback_count(&self) -> u64 {
        self.s3_fallback_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter};
    use crate::journal_manager::JournalManager;
    use crate::metadata_lock_manager::MetadataLockManager;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    fn create_test_system(temp_dir: &TempDir) -> BackgroundRecoverySystem {
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            std::time::Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let consolidation_trigger = Arc::new(ConsolidationTrigger::new(1024 * 1024, 100));
        let metadata_writer = Arc::new(Mutex::new(HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            consolidation_trigger,
        )));

        let orphaned_recovery = Arc::new(OrphanedRangeRecovery::new(
            temp_dir.path().to_path_buf(),
            metadata_writer,
        ));

        let config = BackgroundRecoveryConfig {
            scan_interval: Duration::from_millis(100), // Fast for testing
            max_concurrent_operations: 2,
            max_recovery_time_per_cycle: Duration::from_millis(500),
            min_access_frequency_threshold: 1,
            max_orphans_per_cycle: 10,
            recovery_operation_delay: Duration::from_millis(10),
            enable_priority_recovery: true,
            scan_timeout: Duration::from_millis(500),
        };

        BackgroundRecoverySystem::new(orphaned_recovery, config)
    }

    #[test]
    fn test_background_recovery_config_default() {
        let config = BackgroundRecoveryConfig::default();
        assert_eq!(config.scan_interval, Duration::from_secs(300));
        assert_eq!(config.max_concurrent_operations, 3);
        assert!(config.enable_priority_recovery);
    }

    #[test]
    fn test_calculate_priority_score() {
        let config = BackgroundRecoveryConfig::default();

        // Create a high-priority orphan
        let high_priority_orphan = OrphanedRange {
            cache_key: "test-bucket/high-priority.txt".to_string(),
            range_file_path: std::path::PathBuf::from("/tmp/test.bin"),
            range_spec: None,
            validation_status: ValidationStatus::Valid,
            access_frequency: 90,                   // High frequency
            last_accessed: Some(SystemTime::now()), // Recent access
            file_size: 50 * 1024 * 1024,            // 50MB
        };

        let score =
            BackgroundRecoverySystem::calculate_priority_score(&high_priority_orphan, &config);
        assert!(score >= 60); // Should be high priority

        // Create a low-priority orphan
        let low_priority_orphan = OrphanedRange {
            cache_key: "test-bucket/low-priority.txt".to_string(),
            range_file_path: std::path::PathBuf::from("/tmp/test2.bin"),
            range_spec: None,
            validation_status: ValidationStatus::Invalid,
            access_frequency: 2, // Low frequency
            last_accessed: Some(SystemTime::now() - Duration::from_secs(86400 * 60)), // 60 days ago
            file_size: 1024,     // 1KB
        };

        let low_score =
            BackgroundRecoverySystem::calculate_priority_score(&low_priority_orphan, &config);
        assert!(low_score < score); // Should be lower priority
    }

    #[tokio::test]
    async fn test_recovery_queue_operations() {
        let temp_dir = TempDir::new().unwrap();
        let system = create_test_system(&temp_dir);

        let status = system.get_queue_status().await;
        assert_eq!(status.high_priority_count, 0);
        assert_eq!(status.medium_priority_count, 0);
        assert_eq!(status.low_priority_count, 0);
        assert_eq!(status.processing_count, 0);
    }

    #[tokio::test]
    async fn test_background_recovery_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let system = create_test_system(&temp_dir);

        let metrics = system.get_metrics();
        assert_eq!(metrics.get_scan_cycles_completed(), 0);
        assert_eq!(metrics.get_recovery_cycles_completed(), 0);
        assert_eq!(metrics.get_background_failures(), 0);
        assert_eq!(metrics.get_s3_fallback_count(), 0);
    }

    #[tokio::test]
    async fn test_system_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let mut system = create_test_system(&temp_dir);

        assert!(!system.is_running());

        // Start the system
        system.start().await.unwrap();
        assert!(system.is_running());

        // Stop the system
        system.stop().await.unwrap();

        // Give it a moment to fully stop
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!system.is_running());
    }

    #[test]
    fn test_get_next_orphan_from_queue() {
        let mut queue = RecoveryQueue::default();

        // Add orphans to different priority queues
        let high_orphan = PrioritizedOrphan {
            orphan: OrphanedRange {
                cache_key: "high".to_string(),
                range_file_path: std::path::PathBuf::from("/tmp/high.bin"),
                range_spec: None,
                validation_status: ValidationStatus::Valid,
                access_frequency: 90,
                last_accessed: None,
                file_size: 1024,
            },
            priority_score: 90,
        };

        let low_orphan = PrioritizedOrphan {
            orphan: OrphanedRange {
                cache_key: "low".to_string(),
                range_file_path: std::path::PathBuf::from("/tmp/low.bin"),
                range_spec: None,
                validation_status: ValidationStatus::Valid,
                access_frequency: 10,
                last_accessed: None,
                file_size: 1024,
            },
            priority_score: 10,
        };

        queue.high_priority.push(high_orphan);
        queue.low_priority.push(low_orphan);

        // Should get high priority first
        let next = BackgroundRecoverySystem::get_next_orphan_from_queue(&mut queue).unwrap();
        assert_eq!(next.orphan.cache_key, "high");

        // Should get low priority next
        let next = BackgroundRecoverySystem::get_next_orphan_from_queue(&mut queue).unwrap();
        assert_eq!(next.orphan.cache_key, "low");

        // Should be empty now
        assert!(BackgroundRecoverySystem::get_next_orphan_from_queue(&mut queue).is_none());
    }
}
