//! Cache Initialization Coordinator
//!
//! This module provides coordinated cache initialization to eliminate redundant
//! directory scanning, provide clear startup messaging, and ensure consistency
//! between cache subsystems while preserving architectural separation.

use crate::cache_size_tracker::{CacheSizeTracker, ValidationRequestKind};
use crate::cache_types::NewCacheMetadata;
use crate::config::CacheConfig;
use crate::error::{ProxyError, Result};
use crate::journal_consolidator::SizeState;
use crate::write_cache_manager::WriteCacheManager;
use serde_json;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// What startup asks of the validation scheduler. See
/// [`CacheInitializationCoordinator::startup_validation_request`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartupValidation {
    /// The figure is validated and recent: nothing to ask.
    None,
    /// An empty cache with a zero figure: record its validation, which is exact without a scan.
    RecordEmpty,
    /// Ask the scheduler for a pass under `validation.lock`.
    Request(ValidationRequestKind),
}

/// Coordinates cache initialization across multiple subsystems
pub struct CacheInitializationCoordinator {
    /// Cache directory path
    cache_dir: PathBuf,
    /// Whether write cache is enabled
    write_cache_enabled: bool,
    /// Cache configuration
    config: CacheConfig,
}

impl CacheInitializationCoordinator {
    /// Create a new cache initialization coordinator
    pub fn new(cache_dir: PathBuf, write_cache_enabled: bool, config: CacheConfig) -> Self {
        Self {
            cache_dir,
            write_cache_enabled,
            config,
        }
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }

    /// Get the cache configuration
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Coordinate full cache initialization process
    pub async fn initialize(
        &self,
        mut write_cache_manager: Option<&mut WriteCacheManager>,
        size_tracker: &mut Option<Arc<CacheSizeTracker>>,
    ) -> Result<InitializationSummary> {
        let start_time = Instant::now();
        let mut total_errors = 0u32;
        let mut total_warnings = 0u32;

        // Initialize performance metrics collection
        let mut performance_metrics = PerformanceMetrics::new();
        performance_metrics.initial_memory_usage = get_current_memory_usage();
        let mut peak_memory = performance_metrics.initial_memory_usage;

        info!("Cache initialization: Starting coordinated initialization");
        info!("Cache initialization: Directory: {:?}", self.cache_dir);

        // Log subsystem status with prefixes
        if self.write_cache_enabled {
            info!("Write cache capacity tracking: enabled");
        } else {
            info!("Write cache: disabled");
        }
        info!("Cache size validation: enabled");

        // Phase 1: Directory Setup
        let phase1_start = Instant::now();
        let phase1_result = self.execute_phase_1().await;
        let phase1_duration = phase1_start.elapsed();
        match &phase1_result {
            Ok(_) => {} // Success already logged in execute_phase_1
            Err(e) => {
                error!(
                    "Cache initialization: Phase 1/4 failed - Directory setup: {}",
                    e
                );
                total_errors += 1;
            }
        }

        // Phase 2: load size_state.json. No `.meta` file is walked at startup.
        let phase2_start = Instant::now();
        info!("Cache initialization: Phase 2/4 - Load size state (no startup scan)");
        let scan_result = self
            .load_size_state_at_startup(&mut performance_metrics)
            .await;
        let phase2_duration = phase2_start.elapsed();

        // Phases 1 and 2 are the only two whose durations have no home on
        // `PerformanceMetrics` — phase 3 sets `subsystem_init_duration` and phase 4
        // sets `cross_validation_duration`, and phase 2's scan separately sets
        // `scan_duration`, which `detailed_report` prints. Until the retired
        // `PhaseResult` list was removed these two were collected into a field of
        // `InitializationSummary` that nothing ever read, so they were measured and
        // then discarded. Log them instead of dropping the measurement.
        debug!(
            "Cache initialization: phase 1 (directory setup) took {:?}, phase 2 (metadata scan) took {:?}",
            phase1_duration, phase2_duration
        );

        // Update peak memory usage
        let current_memory = get_current_memory_usage();
        if current_memory > peak_memory {
            peak_memory = current_memory;
        }

        let scan_summary = match &scan_result {
            Ok(results) => {
                info!(
                    "Cache initialization: Phase 2/4 completed - Metadata scan: {}",
                    results.summary_string()
                );
                if results.has_errors() {
                    total_warnings += results.error_count() as u32;
                }
                results.clone()
            }
            Err(e) => {
                error!(
                    "Cache initialization: Phase 2/4 failed - Metadata scan: {}",
                    e
                );
                total_errors += 1;
                ObjectsScanResults::empty()
            }
        };

        // Phase 3: Subsystem Initialization
        let phase3_start = Instant::now();
        info!("Cache initialization: Phase 3/4 - Subsystem initialization");
        let subsystem_result = self
            .initialize_subsystems_with_logging(
                &scan_summary,
                write_cache_manager.as_deref_mut(),
                size_tracker,
            )
            .await;
        let phase3_duration = phase3_start.elapsed();
        performance_metrics.subsystem_init_duration = phase3_duration;

        // Update peak memory usage
        let current_memory = get_current_memory_usage();
        if current_memory > peak_memory {
            peak_memory = current_memory;
        }

        let subsystem_results = match &subsystem_result {
            Ok(results) => {
                info!("Cache initialization: Phase 3/4 completed - Subsystem initialization");
                results.clone()
            }
            Err(e) => {
                error!(
                    "Cache initialization: Phase 3/4 failed - Subsystem initialization: {}",
                    e
                );
                total_errors += 1;
                SubsystemResults::empty()
            }
        };

        // No startup count of the cache (Ed, 2026-10-10). When the persisted figure is overdue,
        // missing or unreadable, the scheduler is asked for its usual pass under
        // `validation.lock`: one proxy walks while every proxy serves. See
        // `startup_validation_request`.
        if let Some(tracker) = size_tracker.as_ref() {
            match self.startup_validation_request().await {
                StartupValidation::None => {}
                StartupValidation::RecordEmpty => tracker.record_empty_cache_validation().await,
                StartupValidation::Request(kind) => tracker.request_validation(kind),
            }
        }

        // Phase 4: Cross-Validation (if both subsystems available)
        let phase4_start = Instant::now();
        info!("Cache initialization: Phase 4/4 - Cross-validation");
        let validation_result = self
            .cross_validate_subsystems(write_cache_manager.as_deref(), size_tracker.as_ref())
            .await;
        let phase4_duration = phase4_start.elapsed();
        performance_metrics.cross_validation_duration = phase4_duration;

        // Update peak memory usage
        let current_memory = get_current_memory_usage();
        if current_memory > peak_memory {
            peak_memory = current_memory;
        }

        let validation_results = match &validation_result {
            Ok(results) => {
                info!("Cache initialization: Phase 4/4 completed - Cross-validation");
                if results.has_minor_discrepancy() {
                    total_warnings += 1;
                } else if results.has_serious_discrepancy() {
                    total_errors += 1;
                }
                Some(results.clone())
            }
            Err(e) => {
                error!(
                    "Cache initialization: Phase 4/4 failed - Cross-validation: {}",
                    e
                );
                total_errors += 1;
                None
            }
        };

        let total_duration = start_time.elapsed();

        // Finalize performance metrics
        performance_metrics.peak_memory_usage = peak_memory;
        performance_metrics.final_memory_usage = get_current_memory_usage();
        performance_metrics.calculate_derived_metrics();

        info!(
            "Cache initialization: Completed in {} (errors: {}, warnings: {})",
            format_duration_human(total_duration),
            total_errors,
            total_warnings
        );

        // Log performance metrics summary (Requirement 6.4: log total time taken for performance monitoring)
        info!(
            "Cache initialization: {}",
            performance_metrics.format_summary()
        );

        // Log detailed performance report at debug level
        debug!(
            "Cache initialization: Detailed performance report:\n{}",
            performance_metrics.detailed_report()
        );

        Ok(InitializationSummary {
            total_duration,
            scan_summary,
            subsystem_results,
            validation_results,
            total_errors,
            total_warnings,
            performance_metrics,
        })
    }

    /// Execute Phase 1: Directory Setup with enhanced error recovery
    ///
    /// This method implements graceful error handling for directory setup,
    /// providing detailed error messages and recovery strategies.
    ///
    /// # Requirements
    /// - Requirement 7.2: Handle directory access failures gracefully
    /// - Requirement 7.5: Include error counts in completion summary
    async fn execute_phase_1(&self) -> Result<()> {
        info!("Cache initialization: Phase 1/4 - Directory setup");

        // Create cache directory if it doesn't exist with detailed error handling
        if !self.cache_dir.exists() {
            match std::fs::create_dir_all(&self.cache_dir) {
                Ok(_) => {
                    info!(
                        "Cache initialization: Created cache directory: {:?}",
                        self.cache_dir
                    );
                }
                Err(e) => {
                    let error_msg = match e.kind() {
                        std::io::ErrorKind::PermissionDenied => {
                            format!(
                                "Permission denied creating cache directory {:?}. \
                                Please ensure the parent directory is writable or run with appropriate privileges.",
                                self.cache_dir
                            )
                        }
                        std::io::ErrorKind::NotFound => {
                            format!(
                                "Parent directory not found for cache directory {:?}. \
                                Please ensure the parent path exists.",
                                self.cache_dir
                            )
                        }
                        _ => {
                            format!(
                                "Failed to create cache directory {:?}: {}",
                                self.cache_dir, e
                            )
                        }
                    };
                    return Err(ProxyError::CacheError(error_msg));
                }
            }
        } else {
            info!(
                "Cache initialization: Using existing cache directory: {:?}",
                self.cache_dir
            );
        }

        // Check write permissions on cache directory with enhanced error messages
        let test_file = self.cache_dir.join(".permission_test");
        match std::fs::write(&test_file, b"test") {
            Ok(_) => {
                // Clean up test file
                if let Err(cleanup_err) = std::fs::remove_file(&test_file) {
                    warn!(
                        "Cache initialization: Failed to clean up permission test file: {}. \
                        This may indicate filesystem issues but won't prevent startup.",
                        cleanup_err
                    );
                }
            }
            Err(e) => {
                let error_msg = match e.kind() {
                    std::io::ErrorKind::PermissionDenied => {
                        format!(
                            "Cache directory {:?} is not writable: {}. \
                            Please check directory permissions or run with appropriate privileges. \
                            Required permissions: read, write, execute.",
                            self.cache_dir, e
                        )
                    }
                    _ if crate::disk_cache::is_disk_full_error(&e) => {
                        format!(
                            "No space left on device for cache directory {:?}: {}. \
                            Please free up disk space or choose a different cache location.",
                            self.cache_dir, e
                        )
                    }
                    _ => {
                        format!(
                            "Cache directory {:?} write test failed: {}. \
                            Please check filesystem health and permissions.",
                            self.cache_dir, e
                        )
                    }
                };
                return Err(ProxyError::CacheError(error_msg));
            }
        }

        // Create subdirectories for different cache types with error recovery
        let mut subdirs = vec!["metadata", "ranges", "locks"];
        if self.write_cache_enabled {
            subdirs.push("mpus_in_progress");
        }

        let mut failed_subdirs = Vec::new();
        let mut created_subdirs = Vec::new();

        for subdir in &subdirs {
            let subdir_path = self.cache_dir.join(subdir);

            // Create subdirectory if it doesn't exist
            if !subdir_path.exists() {
                match std::fs::create_dir_all(&subdir_path) {
                    Ok(_) => {
                        debug!("Cache initialization: Created subdirectory: {}", subdir);
                        created_subdirs.push(subdir.to_string());
                    }
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to create cache subdirectory '{}': {}. \
                            This may affect cache functionality for this component.",
                            subdir, e
                        );
                        warn!("Cache initialization: {}", error_msg);
                        failed_subdirs.push((subdir.to_string(), error_msg));
                        continue; // Skip permission test for failed directory
                    }
                }
            }

            // Check write permissions on each subdirectory with error recovery
            let test_file = subdir_path.join(".permission_test");
            match std::fs::write(&test_file, b"test") {
                Ok(_) => {
                    // Clean up test file
                    if let Err(cleanup_err) = std::fs::remove_file(&test_file) {
                        warn!(
                            "Cache initialization: Failed to clean up permission test file in {}: {}",
                            subdir, cleanup_err
                        );
                    }
                }
                Err(e) => {
                    let error_msg = match e.kind() {
                        std::io::ErrorKind::PermissionDenied => {
                            format!(
                                "Cache subdirectory '{}' is not writable: {}. \
                                Please check permissions on {:?}. This may affect cache functionality.",
                                subdir, e, subdir_path
                            )
                        }
                        _ if crate::disk_cache::is_disk_full_error(&e) => {
                            format!(
                                "No space left on device for cache subdirectory '{}': {}. \
                                Please free up disk space.",
                                subdir, e
                            )
                        }
                        _ => {
                            format!(
                                "Cache subdirectory '{}' write test failed: {}. \
                                This may affect cache functionality for this component.",
                                subdir, e
                            )
                        }
                    };
                    warn!("Cache initialization: {}", error_msg);
                    failed_subdirs.push((subdir.to_string(), error_msg));
                }
            }
        }

        // Report results with detailed information
        if !created_subdirs.is_empty() {
            info!(
                "Cache initialization: Created {} subdirectories: {}",
                created_subdirs.len(),
                created_subdirs.join(", ")
            );
        }

        if !failed_subdirs.is_empty() {
            warn!(
                "Cache initialization: {} subdirectory operations failed. \
                Cache functionality may be limited. Failed subdirectories: {}",
                failed_subdirs.len(),
                failed_subdirs
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            // For critical subdirectories, return error
            let critical_subdirs = ["metadata", "ranges"];
            let critical_failures: Vec<_> = failed_subdirs
                .iter()
                .filter(|(name, _)| critical_subdirs.contains(&name.as_str()))
                .collect();

            if !critical_failures.is_empty() {
                let critical_names: Vec<_> = critical_failures
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect();
                return Err(ProxyError::CacheError(format!(
                    "Critical cache subdirectories failed to initialize: {}. \
                    Cache cannot function without these directories.",
                    critical_names.join(", ")
                )));
            }
        }

        info!("Cache initialization: Directory setup completed successfully");
        Ok(())
    }

    /// The startup figures, read from `size_state.json` as it stands, with no `.meta` walk.
    /// A missing or unparseable document reads as zeros here;
    /// [`Self::startup_validation_request`] is what reports and repairs that state.
    async fn load_scan_results_from_size_state(
        &self,
        performance_metrics: &mut PerformanceMetrics,
    ) -> ObjectsScanResults {
        let size_state_path = self.cache_dir.join("size_tracking").join("size_state.json");

        let state: SizeState = match tokio::fs::read_to_string(&size_state_path).await {
            Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
            Err(_) => SizeState::default(),
        };

        let read_cached_size = state.total_size.saturating_sub(state.write_cache_size);

        info!(
            "Cache initialization: no startup scan - \
             loaded from size_state.json: total={}, write_cache={}, objects={}",
            format_bytes_human(state.total_size),
            format_bytes_human(state.write_cache_size),
            state.cached_objects,
        );

        performance_metrics.scan_duration = Duration::ZERO;
        performance_metrics.files_processed = 0;

        ObjectsScanResults {
            total_objects: state.cached_objects,
            total_size: state.total_size,
            write_cached_objects: 0,
            write_cached_size: state.write_cache_size,
            read_cached_objects: state.cached_objects,
            read_cached_size,
            metadata_entries: vec![],
            scan_duration: Duration::ZERO,
            scan_errors: vec![],
            loaded_from_state_files: true,
        }
    }

    /// Phase 2: the startup figure is `size_state.json`, read as it stands. No `.meta` file is
    /// walked on this path, in any state (Ed, 2026-10-10: a proxy must not count the cache on
    /// start).
    ///
    /// # Why there is no startup count
    ///
    /// The count used to run whenever `validation.json` was more than 23 hours old, and it
    /// installed its census as an absolute figure holding `global_consolidation.lock` but not
    /// `validation.lock`. A proxy restarting while a peer's full validation was mid-walk
    /// therefore landed an absolute install between that pass's baseline and its relative
    /// install, and the peer then corrected the same drift a second time. It was also a whole
    /// walk of shared storage per overdue restart, per proxy. The scheduled validation does the
    /// same count under `validation.lock`, one proxy at a time, so startup asks for that pass
    /// instead: see [`Self::startup_validation_request`].
    ///
    /// [`Self::perform_validation_recovery_if_needed`] still runs here: it touches only
    /// `validation.lock` and `validation.json`, never a `.meta` file.
    async fn load_size_state_at_startup(
        &self,
        performance_metrics: &mut PerformanceMetrics,
    ) -> Result<ObjectsScanResults> {
        if let Err(recovery_error) = self.perform_validation_recovery_if_needed().await {
            warn!(
                "Cache initialization: Validation recovery failed: {}. Continuing.",
                recovery_error
            );
        }
        Ok(self
            .load_scan_results_from_size_state(performance_metrics)
            .await)
    }

    /// What startup must ask the validation scheduler for, if anything.
    ///
    /// - [`ValidationRequestKind::FigureUnvalidated`] when `size_state.json` is unreadable, is
    ///   missing on a cache that holds data, or holds bytes with a `cached_objects` of 0 (a
    ///   document from a release before the object count existed). The figure in force
    ///   under-counts the cache (0, or the persisted bytes with no objects), so size-based
    ///   eviction fires late, never early, until the requested full pass installs.
    ///   The statvfs free-space floor still applies throughout. Logged at WARN.
    /// - Otherwise [`ValidationRequestKind::Overdue`] when `validation.json` is older than the
    ///   23-hour gate ([`Self::check_validation_freshness`]).
    ///
    /// "Holds data" is one `read_dir` of `metadata/` ([`Self::cache_holds_data`]), not a walk.
    ///
    /// An overdue validation on a cache holding no data whose figure is zero (missing or all
    /// zeros) is [`StartupValidation::RecordEmpty`] instead: the census is 0 and equals the
    /// figure, so the record is exact. See `CacheSizeTracker::record_empty_cache_validation`.
    /// "Holding no data" means `metadata/` is absent or was listed to the end with no bucket
    /// directory. A `metadata/` that cannot be listed is not evidence of an empty cache, so it
    /// takes [`ValidationRequestKind::FigureUnvalidated`] instead.
    pub(crate) async fn startup_validation_request(&self) -> StartupValidation {
        let size_state_path = self.cache_dir.join("size_tracking").join("size_state.json");
        let mut figure_is_zero = false;
        let unvalidated = match tokio::fs::read_to_string(&size_state_path).await {
            Ok(content) => match serde_json::from_str::<SizeState>(&content) {
                Ok(state) if state.cached_objects == 0 && state.total_size > 0 => Some(format!(
                    "size_state.json holds {} but no object count",
                    format_bytes_human(state.total_size)
                )),
                Ok(state) => {
                    figure_is_zero = state.total_size == 0
                        && state.write_cache_size == 0
                        && state.cached_objects == 0;
                    None
                }
                Err(e) => Some(format!("size_state.json is unreadable ({})", e)),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                match self.cache_holds_data().await {
                    Ok(true) => {
                        Some("size_state.json is missing on a cache that holds data".to_string())
                    }
                    Ok(false) => {
                        figure_is_zero = true;
                        None
                    }
                    Err(e) => Some(format!(
                        "size_state.json is missing and metadata/ could not be listed ({})",
                        e
                    )),
                }
            }
            Err(e) => Some(format!("size_state.json is unreadable ({})", e)),
        };
        if let Some(reason) = unvalidated {
            warn!(
                "Cache size: {}. The tracked size is UNVALIDATED: it under-counts the cache, so \
                 size-based eviction fires late until the full validation requested now \
                 completes (one pass under validation.lock, run by one proxy). No startup scan \
                 is performed.",
                reason
            );
            return StartupValidation::Request(ValidationRequestKind::FigureUnvalidated);
        }
        let freshness = self.check_validation_freshness().await;
        if matches!(&freshness, Ok(info) if info.is_fresh) {
            return StartupValidation::None;
        }
        if figure_is_zero {
            match self.cache_holds_data().await {
                Ok(false) => {
                    info!(
                        "Cache initialization: empty cache with a zero size figure; recording \
                         its validation without a scan"
                    );
                    return StartupValidation::RecordEmpty;
                }
                Ok(true) => {}
                Err(e) => {
                    warn!(
                        "Cache size: the size figure is zero and metadata/ could not be listed \
                         ({}). The tracked size is UNVALIDATED: requesting a full validation \
                         rather than recording an empty cache.",
                        e
                    );
                    return StartupValidation::Request(ValidationRequestKind::FigureUnvalidated);
                }
            }
        }
        match freshness {
            Ok(info) => {
                info!(
                    "Cache initialization: validation is overdue ({}); requesting the scheduled \
                     validation now instead of counting the cache at startup",
                    info.message
                );
                StartupValidation::Request(ValidationRequestKind::Overdue)
            }
            Err(e) => {
                warn!(
                    "Cache initialization: could not check validation freshness ({}); \
                     requesting the scheduled validation now",
                    e
                );
                StartupValidation::Request(ValidationRequestKind::Overdue)
            }
        }
    }

    /// Whether `metadata/` holds anything but this cache's own `_`-prefixed state directories
    /// (`_journals`, `_shard_state`, `_write_ledger`). One directory listing: S3 bucket names
    /// cannot start with `_`, so any other entry is a bucket's data.
    ///
    /// `Ok(false)` only when `metadata/` is absent (NotFound) or was listed to the end with no
    /// such entry. Any other error, opening the directory or mid-listing, is `Err`: an
    /// unreadable `metadata/` is not evidence of an empty cache.
    async fn cache_holds_data(&self) -> std::io::Result<bool> {
        let mut entries = match tokio::fs::read_dir(self.cache_dir.join("metadata")).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_name().to_string_lossy().starts_with('_') {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Check validation freshness (23-hour validity)
    ///
    /// # Requirements
    /// - Requirement 10.3: Skip validation if completed within 23 hours
    async fn check_validation_freshness(&self) -> Result<ValidationFreshnessInfo> {
        let validation_file = self.cache_dir.join("size_tracking").join("validation.json");

        if !validation_file.exists() {
            return Ok(ValidationFreshnessInfo {
                is_fresh: false,
                message: "No validation timestamp file found".to_string(),
                last_validation: None,
            });
        }

        match tokio::fs::read_to_string(&validation_file).await {
            Ok(content) => {
                match serde_json::from_str::<serde_json::Value>(&content) {
                    Ok(json) => {
                        if let Some(last_validation_secs) =
                            json.get("last_validation").and_then(|v| v.as_u64())
                        {
                            let last_validation = std::time::SystemTime::UNIX_EPOCH
                                + std::time::Duration::from_secs(last_validation_secs);
                            let age = std::time::SystemTime::now()
                                .duration_since(last_validation)
                                .unwrap_or(std::time::Duration::from_secs(u64::MAX));

                            let is_fresh = age < std::time::Duration::from_secs(23 * 3600); // 23 hours

                            Ok(ValidationFreshnessInfo {
                                is_fresh,
                                message: if is_fresh {
                                    format!(
                                        "Last validation was {:.1} hours ago",
                                        age.as_secs_f64() / 3600.0
                                    )
                                } else {
                                    format!(
                                        "Last validation was {:.1} hours ago (stale)",
                                        age.as_secs_f64() / 3600.0
                                    )
                                },
                                last_validation: Some(last_validation),
                            })
                        } else {
                            Ok(ValidationFreshnessInfo {
                                is_fresh: false,
                                message: "Invalid validation timestamp format".to_string(),
                                last_validation: None,
                            })
                        }
                    }
                    Err(_) => Ok(ValidationFreshnessInfo {
                        is_fresh: false,
                        message: "Corrupted validation timestamp file".to_string(),
                        last_validation: None,
                    }),
                }
            }
            Err(_) => Ok(ValidationFreshnessInfo {
                is_fresh: false,
                message: "Failed to read validation timestamp file".to_string(),
                last_validation: None,
            }),
        }
    }

    /// Perform validation recovery if needed
    ///
    /// # Requirements
    /// - Requirement 10.5: Recover from failed/incomplete validation
    async fn perform_validation_recovery_if_needed(&self) -> Result<()> {
        let validation_file = self.cache_dir.join("size_tracking").join("validation.json");
        let validation_lock = self.cache_dir.join("size_tracking").join("validation.lock");

        // Remove the validation lock file ONLY if no instance currently holds the lock.
        //
        // # The liveness signal is the lock itself, never the mtime
        //
        // This used to remove `validation.lock` whenever its mtime was more than an hour old,
        // with no check on whether a validation pass was running. Two facts make that unsafe,
        // and the second is what turns it from useless into harmful:
        //
        //  - **The mtime never advances.** `try_acquire_validation_lock` opens the file with
        //    `.create(true).write(true).truncate(false)` and then only `flock`s it; nothing ever
        //    writes a byte, sets the mtime, or syncs it. So the mtime is fixed at the instant the
        //    inode was first created — possibly weeks before any given pass — and `age > 1 hour`
        //    was satisfied on essentially every start, regardless of what any instance was doing.
        //  - **`flock` binds to the open file description, not to the path.** Removing the path
        //    does not release a held lock: the holder keeps it, while the next instance to open
        //    the recreated path gets a *different inode* and therefore an *independent* lock. So
        //    the removal did not recover a stuck lock — it manufactured concurrent validation.
        //
        // Concurrent validation is not a cosmetic problem. `update_size_from_validation` is an
        // absolute install of `total_size`, `write_cache_size` and `cached_objects` that takes no
        // lock of its own, and it also calls `reset_all_delta_files()` and
        // `size_accumulator.reset()`. Two overlapping passes therefore discard each other's
        // post-scan deltas, and a rolling pass's *extrapolated* figures can land on top of a full
        // pass's census. This `flock` is the only thing serialising them.
        //
        // # There is no such thing as a stale `flock`, which is why the removal recovers nothing
        //
        // The kernel drops an `flock` when the holding descriptor closes, including on process
        // death — so a crashed instance leaves no lock behind (on NFS, after at most the
        // server's lease). A *wedged* instance still holds a live lock, and removing its path
        // would not take the lock away from it; it would only let a second instance acquire an
        // independent one, which is the outcome above. Either way, removal is not the remedy.
        //
        // What is left for this code to do is bounded housekeeping: drop the file when it is
        // provably unheld, so a deployment does not accumulate a sentinel whose mtime misleads
        // the next reader. Acquiring the lock IS the proof, so that is what is required. On
        // contention the file is left exactly as it is.
        if validation_lock.exists() {
            match std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&validation_lock)
            {
                Ok(probe) => {
                    if fs4::FileExt::try_lock(&probe).is_ok() {
                        // Nobody holds it. Remove while still holding, so an instance that
                        // opens the path between the unlink and the unlock contends with us
                        // rather than acquiring a second, independent lock.
                        info!(
                            "Cache initialization: removing an unheld validation lock file at {:?}",
                            validation_lock
                        );
                        if let Err(e) = tokio::fs::remove_file(&validation_lock).await {
                            debug!(
                                "Cache initialization: could not remove the unheld validation \
                                 lock at {:?}: {}. Harmless — it is a zero-byte sentinel.",
                                validation_lock, e
                            );
                        }
                        let _ = fs4::FileExt::unlock(&probe);
                    } else {
                        // A live holder. This is the case the old mtime test could not see, and
                        // the one where removal would have created a second concurrent pass.
                        info!(
                            "Cache initialization: another instance holds the validation lock; \
                             leaving it in place. A held lock is never stale — the kernel \
                             releases it if that instance dies."
                        );
                    }
                }
                Err(e) => {
                    debug!(
                        "Cache initialization: could not open the validation lock at {:?} to test \
                         whether it is held: {}. Leaving it alone.",
                        validation_lock, e
                    );
                }
            }
        }

        // Check for failed/incomplete validation
        if validation_file.exists() {
            if let Ok(content) = tokio::fs::read_to_string(&validation_file).await {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                    if let Some(status) = json.get("status").and_then(|s| s.as_str()) {
                        match status {
                            "in_progress" => {
                                info!("Cache initialization: Found incomplete validation, will restart validation");
                                return Ok(());
                            }
                            "failed" => {
                                info!("Cache initialization: Found failed validation, will restart validation");
                                return Ok(());
                            }
                            "completed" => {
                                // Validation completed successfully, no recovery needed
                                return Ok(());
                            }
                            _ => {
                                info!("Cache initialization: Unknown validation status '{}', will restart validation", status);
                                return Ok(());
                            }
                        }
                    }
                }
            }
            // If we can't parse the validation file, assume it needs recovery
            info!("Cache initialization: Cannot parse validation file, will restart validation");
        }

        Ok(())
    }

    /// Initialize subsystems with detailed logging
    ///
    /// This method initializes both WriteCacheManager and CacheSizeTracker with
    /// subsystem-specific logging that includes capacity information and percentages.
    ///
    /// # Requirements
    /// - Requirement 3.1: Write cache messages prefixed with "Write cache capacity tracking"
    /// - Requirement 3.2: Size tracker messages prefixed with "Cache size validation"
    /// - Requirement 3.3: Include capacity information with percentages
    /// - Requirement 3.4: Log "Write cache: disabled" when write cache is disabled
    /// - Requirement 3.5: Distinguish between total objects and write-cached objects
    async fn initialize_subsystems_with_logging(
        &self,
        scan_results: &ObjectsScanResults,
        write_cache_manager: Option<&mut WriteCacheManager>,
        size_tracker: &mut Option<Arc<CacheSizeTracker>>,
    ) -> Result<SubsystemResults> {
        let mut results = SubsystemResults::empty();

        // Report the scan's staged findings, but do NOT seed the admission counter.
        //
        // Task 27 removed `initialize_from_scan_results` and its wrapper
        // `initialize_write_cache_manager_safe` rather than repointing them, because
        // **seeding an admission counter from persisted state is the defect**, not a
        // detail of which figure it was seeded from. A repointed version preserves the
        // bug in a new shape.
        //
        // What went wrong is worth stating, because it is what makes "removed, not
        // repointed" the right call. `WriteCacheManager::current_size` counts bytes
        // **in flight** — reservations held by uploads currently streaming — released by
        // `WriteReservation::drop`. Seeding it from a scan of what is **resident** on disk
        // conflates two different quantities, and because nothing ever released the seeded
        // portion, the figure was pinned rather than tracked: on the fleet it started at
        // 157.61% of the bound at every startup and stayed there, refusing every
        // write-through PUT for months.
        //
        // Inflight_Bytes now starts at zero on every start, which is simply correct — a
        // fresh process has no uploads in flight. Residency is read live from Size_State
        // by `CacheManager::get_staging_resident_bytes` (R7.1), which is shared across the
        // fleet and cannot be pinned by a local seed.
        //
        // Spec: write-cache-accounting-and-eviction. Requirements: 3.3, 7.2
        if self.write_cache_enabled {
            if write_cache_manager.is_some() {
                info!(
                    "Write cache: scan found {} staged objects ({} total), {} staged on disk \
                     ({:.1}% of the cache). In-flight bytes start at 0; staged residency is \
                     read from shared size state, not seeded from this scan.",
                    scan_results.write_cached_objects,
                    scan_results.total_objects,
                    scan_results.format_write_cached_size(),
                    scan_results.write_cache_percentage(),
                );
                results.write_cache_initialized = true;
            } else {
                warn!("Write cache capacity tracking: Manager not provided despite being enabled");
            }
        } else {
            info!("Write cache: disabled - skipping write cache initialization");
        }

        // Initialize CacheSizeTracker with graceful error handling
        if let Some(tracker) = size_tracker {
            match self
                .initialize_size_tracker_safe(tracker, scan_results)
                .await
            {
                Ok(_usage) => {
                    results.size_tracker_initialized = true;
                }
                Err(e) => {
                    error!(
                        "Cache size validation: Initialization failed: {}. \
                        Continuing without size validation tracking.",
                        e
                    );
                    results.size_tracker_initialized = false;
                }
            }
        } else {
            warn!("Cache size validation: Tracker not provided");
        }

        Ok(results)
    }

    /// Safely initialize CacheSizeTracker with error recovery
    ///
    /// This method implements graceful error handling for size tracker
    /// initialization, ensuring that failures don't prevent proxy startup.
    ///
    /// # Requirements
    /// - Requirement 7.3: Handle subsystem initialization failures gracefully
    /// - Requirement 7.5: Include error counts in completion summary
    async fn initialize_size_tracker_safe(
        &self,
        _tracker: &Arc<CacheSizeTracker>,
        scan_results: &ObjectsScanResults,
    ) -> Result<u64> {
        info!("Cache size validation: Initializing from scan results");
        info!(
            "Cache size validation: Found {} total objects ({} write-cached, {} read-cached)",
            scan_results.total_objects,
            scan_results.write_cached_objects,
            scan_results.read_cached_objects
        );
        info!(
            "Cache size validation: Total cache usage: {} (write: {}, read: {})",
            scan_results.format_total_size(),
            scan_results.format_write_cached_size(),
            scan_results.format_read_cached_size()
        );

        // NOTE: Size tracking is now handled by JournalConsolidator (Task 12).
        // The consolidator loads size from size_state.json on startup and updates it
        // through journal entries. We no longer need to set size here.
        // The validation scan in CacheSizeTracker will correct any drift periodically.
        //
        // The set_write_cache_size() method has been removed from CacheSizeTracker
        // as the consolidator is now the single source of truth for cache size.

        let update_errors: Vec<String> = Vec::new();

        if !update_errors.is_empty() {
            warn!(
                "Cache size validation: {} update operations failed: {}",
                update_errors.len(),
                update_errors.join(", ")
            );
        }

        let total_usage = scan_results.total_size;
        let max_cache_size = self.config.max_cache_size;

        // Calculate percentage of max cache size (not percentage of current usage)
        let usage_percent = if max_cache_size > 0 {
            (total_usage as f64 / max_cache_size as f64) * 100.0
        } else {
            0.0
        };

        info!(
            "Cache initialized: {} / {} ({:.1}% of max)",
            format_bytes_human(total_usage),
            format_bytes_human(max_cache_size),
            usage_percent
        );

        // Warn if cache is over capacity
        if total_usage > max_cache_size {
            warn!(
                "Cache over capacity: {} used, {} max ({:.1}% - eviction will be triggered)",
                format_bytes_human(total_usage),
                format_bytes_human(max_cache_size),
                usage_percent
            );
        }

        Ok(total_usage)
    }

    /// Try to acquire initialization lock with retry logic
    ///
    /// This method implements distributed locking for initialization to prevent
    /// multiple instances from scanning simultaneously.
    ///
    /// # Requirements
    /// - Requirement 10.1: Acquire appropriate locks before scanning
    /// - Requirement 10.4: Retry with exponential backoff
    /// - Requirement 10.5: Release locks properly on completion/failure
    pub async fn try_acquire_initialization_lock(&self) -> Result<InitializationLock> {
        let lock_path = self.cache_dir.join("locks").join("initialization.lock");

        // Ensure locks directory exists
        if let Some(parent) = lock_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to create initialization lock directory: {}",
                    e
                ))
            })?;
        }

        let mut retry_count = 0;
        let max_retries = 5;
        let mut backoff_ms = 100;

        loop {
            match self.try_acquire_lock_once(&lock_path).await {
                Ok(lock) => {
                    info!("Cache initialization: Acquired initialization lock");
                    return Ok(lock);
                }
                Err(e) if retry_count < max_retries => {
                    retry_count += 1;
                    warn!(
                        "Cache initialization: Failed to acquire lock (attempt {}/{}): {}. Retrying in {}ms",
                        retry_count, max_retries + 1, e, backoff_ms
                    );

                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(5000); // Cap at 5 seconds
                }
                Err(e) => {
                    return Err(ProxyError::CacheError(format!(
                        "Failed to acquire initialization lock after {} attempts: {}",
                        max_retries + 1,
                        e
                    )));
                }
            }
        }
    }

    /// Try to acquire lock once (internal helper)
    async fn try_acquire_lock_once(&self, lock_path: &PathBuf) -> Result<InitializationLock> {
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to open initialization lock file: {}", e))
            })?;

        // Try to acquire exclusive lock (non-blocking)
        fs4::FileExt::try_lock(&lock_file).map_err(|e| {
            ProxyError::CacheError(format!("Failed to acquire initialization lock: {}", e))
        })?;

        debug!(
            "Cache initialization: Acquired initialization lock at {:?}",
            lock_path
        );
        Ok(InitializationLock {
            file: lock_file,
            path: lock_path.clone(),
        })
    }

    /// Coordinate full cache initialization process with distributed locking
    ///
    /// This method implements the complete initialization workflow with distributed
    /// locking to ensure only one instance performs scanning at a time.
    ///
    /// # Requirements
    /// - Requirement 10.1: Acquire appropriate locks before scanning
    /// - Requirement 10.2: Ensure consistent view after initialization
    /// - Requirement 10.3: Consistent cache state across instances
    pub async fn initialize_with_locking(
        &self,
        write_cache_manager: Option<&mut WriteCacheManager>,
        size_tracker: &mut Option<Arc<CacheSizeTracker>>,
    ) -> Result<InitializationSummary> {
        let start_time = Instant::now();

        info!("Cache initialization: Starting coordinated initialization with distributed locking");

        // Try to acquire initialization lock with retry logic
        let _initialization_lock = match self.try_acquire_initialization_lock().await {
            Ok(lock) => {
                info!("Cache initialization: Successfully acquired distributed lock");
                lock
            }
            Err(e) => {
                error!(
                    "Cache initialization: Failed to acquire distributed lock: {}",
                    e
                );
                // Fall back to non-locking initialization for single-instance deployments
                warn!("Cache initialization: Falling back to non-locking initialization");
                return self.initialize(write_cache_manager, size_tracker).await;
            }
        };

        // Perform initialization while holding the lock
        let mut result = self.initialize(write_cache_manager, size_tracker).await;

        // Ensure consistent cache state across instances after initialization
        if let Ok(ref mut summary) = result {
            match self
                .ensure_cache_state_consistency_post_init(size_tracker.as_ref())
                .await
            {
                Ok(consistency_info) => {
                    info!(
                        "Cache initialization: Cache state consistency verified: {}",
                        consistency_info
                    );
                    // Add consistency information to the summary
                    summary.total_warnings += consistency_info.warnings;
                }
                Err(e) => {
                    warn!(
                        "Cache initialization: Cache state consistency check failed: {}",
                        e
                    );
                    summary.total_warnings += 1;
                }
            }
        }

        // Lock is automatically released when _initialization_lock goes out of scope
        info!("Cache initialization: Released distributed lock");

        match &result {
            Ok(_summary) => {
                info!(
                    "Cache initialization: Completed with distributed locking in {}",
                    format_duration_human(start_time.elapsed())
                );

                // Log coordination success for multi-instance deployments
                info!("Cache initialization: Multi-instance coordination successful - cache state consistent");
            }
            Err(e) => {
                error!(
                    "Cache initialization: Failed with distributed locking: {}",
                    e
                );
            }
        }

        result
    }

    /// Ensure consistent cache state across instances (post-initialization)
    ///
    /// This method implements cache state consistency checks to ensure all instances
    /// have a consistent view of the cache after initialization. This version is called
    /// after initialization is complete and doesn't require write cache manager access.
    ///
    /// # Requirements
    /// - Requirement 10.2: Add coordination for shared cache validation
    /// - Requirement 10.3: Ensure consistent view after initialization
    pub(crate) async fn ensure_cache_state_consistency_post_init(
        &self,
        size_tracker: Option<&Arc<CacheSizeTracker>>,
    ) -> Result<CacheConsistencyInfo> {
        info!("Cache initialization: Verifying cache state consistency across instances");

        let mut consistency_info = CacheConsistencyInfo::new();

        // Check if shared cache validation is needed
        if let Some(tracker) = size_tracker {
            match self.verify_shared_cache_validation(tracker).await {
                Ok(validation_info) => {
                    consistency_info.shared_validation_status = validation_info.status.clone();
                    consistency_info.shared_validation_message =
                        Some(validation_info.message.clone());

                    if validation_info.needs_attention {
                        consistency_info.warnings += 1;
                        warn!(
                            "Cache initialization: Shared cache validation needs attention: {}",
                            validation_info.message
                        );
                    } else {
                        debug!(
                            "Cache initialization: Shared cache validation successful: {}",
                            validation_info.message
                        );
                    }
                }
                Err(e) => {
                    consistency_info.warnings += 1;
                    consistency_info.shared_validation_status = "FAILED".to_string();
                    consistency_info.shared_validation_message =
                        Some(format!("Validation failed: {}", e));
                    warn!(
                        "Cache initialization: Shared cache validation failed: {}",
                        e
                    );
                }
            }
        }

        // Verify cache directory consistency
        match self.verify_cache_directory_consistency().await {
            Ok(dir_info) => {
                consistency_info.directory_consistency_status = dir_info.status.clone();
                consistency_info.directory_consistency_message = Some(dir_info.message.clone());

                if dir_info.needs_attention {
                    consistency_info.warnings += 1;
                    warn!(
                        "Cache initialization: Directory consistency needs attention: {}",
                        dir_info.message
                    );
                } else {
                    debug!(
                        "Cache initialization: Directory consistency verified: {}",
                        dir_info.message
                    );
                }
            }
            Err(e) => {
                consistency_info.warnings += 1;
                consistency_info.directory_consistency_status = "FAILED".to_string();
                consistency_info.directory_consistency_message =
                    Some(format!("Directory check failed: {}", e));
                warn!(
                    "Cache initialization: Directory consistency check failed: {}",
                    e
                );
            }
        }

        // Note: Subsystem consistency check is already performed during regular initialization
        // so we don't need to repeat it here
        consistency_info.subsystem_consistency_status = "CHECKED_DURING_INIT".to_string();
        consistency_info.subsystem_consistency_message =
            Some("Cross-validation performed during initialization".to_string());

        info!(
            "Cache initialization: Cache state consistency check completed - {} warnings",
            consistency_info.warnings
        );

        Ok(consistency_info)
    }

    /// Verify shared cache validation status
    ///
    /// This method checks if shared cache validation is working correctly
    /// and if there are any coordination issues between instances.
    async fn verify_shared_cache_validation(
        &self,
        size_tracker: &Arc<CacheSizeTracker>,
    ) -> Result<ValidationInfo> {
        // Try to acquire validation lock to test coordination
        match size_tracker.try_acquire_validation_lock().await {
            Ok(_lock) => {
                // Successfully acquired lock - coordination is working
                Ok(ValidationInfo {
                    status: "WORKING".to_string(),
                    message: "Shared cache validation coordination is working correctly"
                        .to_string(),
                    needs_attention: false,
                })
            }
            Err(e) => {
                // Could not acquire lock - might be normal if another instance is validating
                if e.to_string().contains("Failed to acquire validation lock") {
                    Ok(ValidationInfo {
                        status: "BUSY".to_string(),
                        message: "Another instance is performing validation - coordination working"
                            .to_string(),
                        needs_attention: false,
                    })
                } else {
                    Ok(ValidationInfo {
                        status: "WARNING".to_string(),
                        message: format!("Validation lock coordination may have issues: {}", e),
                        needs_attention: true,
                    })
                }
            }
        }
    }

    /// Verify cache directory consistency
    ///
    /// This method checks if cache directories are accessible and consistent
    /// across instances sharing the same cache volume.
    async fn verify_cache_directory_consistency(&self) -> Result<ValidationInfo> {
        let mut issues = Vec::new();

        // Check critical directories exist and are accessible
        let critical_dirs = ["metadata", "ranges", "locks"];
        for dir_name in &critical_dirs {
            let dir_path = self.cache_dir.join(dir_name);

            if !dir_path.exists() {
                issues.push(format!("Critical directory '{}' does not exist", dir_name));
                continue;
            }

            // Test write access
            let test_file = dir_path.join(".consistency_test");
            match tokio::fs::write(&test_file, b"test").await {
                Ok(_) => {
                    // Clean up test file
                    let _ = tokio::fs::remove_file(&test_file).await;
                }
                Err(e) => {
                    issues.push(format!("Directory '{}' is not writable: {}", dir_name, e));
                }
            }
        }

        // Check if write cache directory exists when write cache is enabled
        if self.write_cache_enabled {
            let write_cache_dir = self.cache_dir.join("mpus_in_progress");
            if !write_cache_dir.exists() {
                issues.push(
                    "mpus_in_progress directory does not exist but write cache is enabled"
                        .to_string(),
                );
            }
        }

        if issues.is_empty() {
            Ok(ValidationInfo {
                status: "CONSISTENT".to_string(),
                message: "All cache directories are accessible and consistent".to_string(),
                needs_attention: false,
            })
        } else {
            Ok(ValidationInfo {
                status: "INCONSISTENT".to_string(),
                message: format!("Directory consistency issues found: {}", issues.join("; ")),
                needs_attention: true,
            })
        }
    }

    /// Perform cross-validation between subsystems
    ///
    /// This method implements cross-validation requirements by comparing write cache
    /// sizes between WriteCacheManager and CacheSizeTracker to detect inconsistencies.
    ///
    /// # Requirements
    /// - Requirement 4.1: Compare write cache size calculations between subsystems
    /// - Requirement 4.2: Log warning when discrepancy exceeds 5%
    /// - Requirement 4.3: Log error when discrepancy exceeds 20%
    /// - Requirement 4.4: Log successful consistency check at debug level
    /// - Requirement 4.5: Include validation in initialization summary
    pub async fn cross_validate_subsystems(
        &self,
        write_cache_manager: Option<&WriteCacheManager>,
        size_tracker: Option<&Arc<CacheSizeTracker>>,
    ) -> Result<ValidationResults> {
        info!("Cache initialization: Starting cross-validation between subsystems");

        // Check if both subsystems are available for validation
        let (manager, tracker) = match (write_cache_manager, size_tracker) {
            (Some(manager), Some(tracker)) => (manager, tracker),
            (None, Some(_)) => {
                let reason = "Write cache manager not available".to_string();
                debug!(
                    "Cache initialization: Cross-validation skipped - {}",
                    reason
                );
                return Ok(ValidationResults::not_performed(reason));
            }
            (Some(_), None) => {
                let reason = "Size tracker not available".to_string();
                debug!(
                    "Cache initialization: Cross-validation skipped - {}",
                    reason
                );
                return Ok(ValidationResults::not_performed(reason));
            }
            (None, None) => {
                let reason = "Neither subsystem available".to_string();
                debug!(
                    "Cache initialization: Cross-validation skipped - {}",
                    reason
                );
                return Ok(ValidationResults::not_performed(reason));
            }
        };

        // Get write cache sizes from both subsystems
        let manager_size = manager.current_usage();
        let tracker_size = tracker.get_write_cache_size().await;

        // RETIRED as of 2.7.0 — these two figures measure different quantities and
        // comparing them can now only produce a false alarm.
        //
        // The comparison was valid while `WriteCacheManager::current_size` was seeded from
        // a scan of resident staged bytes, because both sides were then attempting to
        // report the same thing. Task 27 removed that seeding, because seeding an
        // admission counter from persisted state was itself the defect behind the fleet's
        // months-long write-caching outage. So today:
        //
        //   manager_size  = Inflight_Bytes — bytes held by uploads currently streaming on
        //                   THIS instance, released on `WriteReservation::drop`. Zero on a
        //                   freshly started process, by design (R7.2).
        //   tracker_size  = Resident_Bytes — staged bytes on the SHARED volume, from
        //                   Size_State, fleet-wide (R7.1).
        //
        // Cross-validating those would report a discrepancy equal to the entire staged set
        // on every startup, and the old thresholds would escalate it to `error!` above 20%.
        // That is a permanent false alarm on any deployment with staged data, pointing an
        // operator at a corruption that does not exist.
        //
        // Retired rather than re-thresholded, because no threshold makes an in-flight
        // figure comparable to a residency figure — there is no correct expected
        // relationship between them at all. The accounting invariant that DOES matter,
        // `write_cache_size <= total_size`, is enforced where it belongs, by the clamp in
        // `JournalConsolidator::update_size_from_validation` (R6.4).
        //
        // Both figures are still reported, so the information is not lost, and
        // `/metrics` exposes them separately as `write_cache.inflight_bytes` and
        // `write_cache.resident_bytes`.
        //
        // Spec: write-cache-accounting-and-eviction. Requirements: 3.3, 7.1, 7.2
        info!(
            "Cache initialization: write cache figures — Inflight_Bytes (this instance): {}, \
             Resident_Bytes (shared, fleet-wide): {}. Not cross-validated: these measure \
             different quantities and are expected to differ.",
            format_bytes_human(manager_size),
            format_bytes_human(tracker_size)
        );

        // `size_discrepancy_percent` is reported as 0.0 and `write_cache_consistent` as
        // true, because there is no longer any discrepancy being measured. Both figures
        // are still carried in the result so a caller that wants them keeps them.
        let results = ValidationResults {
            write_cache_consistent: true,
            size_discrepancy_percent: 0.0,
            validation_warnings: Vec::new(),
            write_cache_manager_size: Some(manager_size),
            size_tracker_write_cache_size: Some(tracker_size),
        };

        Ok(results)
    }
}

/// Summary of cache initialization process
#[derive(Debug, Clone)]
pub struct InitializationSummary {
    /// Total initialization duration
    pub total_duration: Duration,
    /// Scan results summary
    pub scan_summary: ObjectsScanResults,
    /// Subsystem initialization results
    pub subsystem_results: SubsystemResults,
    /// Cross-validation results
    pub validation_results: Option<ValidationResults>,
    /// Total errors encountered
    pub total_errors: u32,
    /// Warnings generated
    pub total_warnings: u32,
    /// Performance metrics collected during initialization
    pub performance_metrics: PerformanceMetrics,
}

/// Performance metrics collected during cache initialization
///
/// This structure tracks detailed performance information to help optimize
/// initialization performance and identify bottlenecks.
///
/// # Requirements
/// - Requirement 6.4: Track scan duration, parse duration, total initialization time
/// - Requirement 6.5: Add memory usage monitoring
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Total time spent scanning directories
    pub scan_duration: Duration,
    /// Total time spent parsing metadata files
    pub parse_duration: Duration,
    /// Total time spent initializing subsystems
    pub subsystem_init_duration: Duration,
    /// Total time spent on cross-validation
    pub cross_validation_duration: Duration,
    /// Number of files processed during scanning
    pub files_processed: u64,
    /// Number of metadata files parsed
    pub metadata_files_parsed: u64,
    /// Peak memory usage during initialization (in bytes)
    pub peak_memory_usage: u64,
    /// Memory usage at start of initialization (in bytes)
    pub initial_memory_usage: u64,
    /// Memory usage at end of initialization (in bytes)
    pub final_memory_usage: u64,
    /// Average file processing rate (files per second)
    pub file_processing_rate: f64,
    /// Average parsing rate (files per second)
    pub parsing_rate: f64,
    /// Memory efficiency (bytes processed per byte of memory used)
    pub memory_efficiency: f64,
}

/// Get current memory usage in bytes
///
/// This function attempts to get the current memory usage of the process
/// using platform-specific methods. Falls back to 0 if unable to determine.
fn get_current_memory_usage() -> u64 {
    #[cfg(target_os = "linux")]
    {
        // On Linux, read from /proc/self/status
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            return kb * 1024; // Convert KB to bytes
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        // On macOS, use task_info
        use std::mem;

        extern "C" {
            fn mach_task_self() -> u32;
            fn task_info(
                target_task: u32,
                flavor: u32,
                task_info_out: *mut u8,
                task_info_outCnt: *mut u32,
            ) -> i32;
        }

        const TASK_BASIC_INFO: u32 = 5;
        const TASK_BASIC_INFO_COUNT: u32 = 5;

        #[repr(C)]
        struct TaskBasicInfo {
            suspend_count: u32,
            virtual_size: u64,
            resident_size: u64,
            user_time: u64,
            system_time: u64,
        }

        unsafe {
            let mut info: TaskBasicInfo = mem::zeroed();
            let mut count = TASK_BASIC_INFO_COUNT;
            let result = task_info(
                mach_task_self(),
                TASK_BASIC_INFO,
                &mut info as *mut _ as *mut u8,
                &mut count,
            );

            if result == 0 {
                return info.resident_size;
            }
        }
    }

    #[cfg(target_os = "windows")]
    {
        // On Windows, use GetProcessMemoryInfo
        use std::mem;

        extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn GetProcessMemoryInfo(
                process: *mut std::ffi::c_void,
                pmc: *mut ProcessMemoryCounters,
                cb: u32,
            ) -> i32;
        }

        #[repr(C)]
        struct ProcessMemoryCounters {
            cb: u32,
            page_fault_count: u32,
            peak_working_set_size: usize,
            working_set_size: usize,
            quota_peak_paged_pool_usage: usize,
            quota_paged_pool_usage: usize,
            quota_peak_non_paged_pool_usage: usize,
            quota_non_paged_pool_usage: usize,
            pagefile_usage: usize,
            peak_pagefile_usage: usize,
        }

        unsafe {
            let mut pmc: ProcessMemoryCounters = mem::zeroed();
            pmc.cb = mem::size_of::<ProcessMemoryCounters>() as u32;

            let result = GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb);

            if result != 0 {
                return pmc.working_set_size as u64;
            }
        }
    }

    // Fallback: return 0 if we can't determine memory usage
    0
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PerformanceMetrics {
    /// Create new performance metrics with default values
    pub fn new() -> Self {
        Self {
            scan_duration: Duration::ZERO,
            parse_duration: Duration::ZERO,
            subsystem_init_duration: Duration::ZERO,
            cross_validation_duration: Duration::ZERO,
            files_processed: 0,
            metadata_files_parsed: 0,
            peak_memory_usage: 0,
            initial_memory_usage: 0,
            final_memory_usage: 0,
            file_processing_rate: 0.0,
            parsing_rate: 0.0,
            memory_efficiency: 0.0,
        }
    }

    /// Calculate derived metrics from collected data
    pub(crate) fn calculate_derived_metrics(&mut self) {
        // Calculate file processing rate
        if self.scan_duration.as_secs_f64() > 0.0 {
            self.file_processing_rate =
                self.files_processed as f64 / self.scan_duration.as_secs_f64();
        }

        // Calculate parsing rate
        if self.parse_duration.as_secs_f64() > 0.0 {
            self.parsing_rate =
                self.metadata_files_parsed as f64 / self.parse_duration.as_secs_f64();
        }

        // Calculate memory efficiency (bytes processed per byte of peak memory used)
        if self.peak_memory_usage > 0 {
            let total_bytes_processed = self.files_processed * 1024; // Estimate 1KB per file
            self.memory_efficiency = total_bytes_processed as f64 / self.peak_memory_usage as f64;
        }
    }

    /// Get memory usage delta (final - initial)
    pub(crate) fn memory_usage_delta(&self) -> i64 {
        self.final_memory_usage as i64 - self.initial_memory_usage as i64
    }

    /// Format performance summary for logging
    pub(crate) fn format_summary(&self) -> String {
        format!(
            "Performance: scan={}ms, parse={}ms, subsystems={}ms, validation={}ms, \
             files={}, parsed={}, rate={:.1}/s, parse_rate={:.1}/s, \
             memory: initial={}, peak={}, final={}, delta={}, efficiency={:.2}",
            self.scan_duration.as_millis(),
            self.parse_duration.as_millis(),
            self.subsystem_init_duration.as_millis(),
            self.cross_validation_duration.as_millis(),
            self.files_processed,
            self.metadata_files_parsed,
            self.file_processing_rate,
            self.parsing_rate,
            format_bytes_human(self.initial_memory_usage),
            format_bytes_human(self.peak_memory_usage),
            format_bytes_human(self.final_memory_usage),
            if self.memory_usage_delta() >= 0 {
                format!("+{}", format_bytes_human(self.memory_usage_delta() as u64))
            } else {
                format!(
                    "-{}",
                    format_bytes_human((-self.memory_usage_delta()) as u64)
                )
            },
            self.memory_efficiency
        )
    }

    /// Get detailed performance report
    pub(crate) fn detailed_report(&self) -> String {
        let mut report = String::new();
        report.push_str("=== Cache Initialization Performance Report ===\n");
        report.push_str(&format!(
            "Directory Scan Duration: {} ({:.2}s)\n",
            format_duration_human(self.scan_duration),
            self.scan_duration.as_secs_f64()
        ));
        report.push_str(&format!(
            "Metadata Parse Duration: {} ({:.2}s)\n",
            format_duration_human(self.parse_duration),
            self.parse_duration.as_secs_f64()
        ));
        report.push_str(&format!(
            "Subsystem Init Duration: {} ({:.2}s)\n",
            format_duration_human(self.subsystem_init_duration),
            self.subsystem_init_duration.as_secs_f64()
        ));
        report.push_str(&format!(
            "Cross-Validation Duration: {} ({:.2}s)\n",
            format_duration_human(self.cross_validation_duration),
            self.cross_validation_duration.as_secs_f64()
        ));
        report.push_str(&format!("Files Processed: {}\n", self.files_processed));
        report.push_str(&format!(
            "Metadata Files Parsed: {}\n",
            self.metadata_files_parsed
        ));
        report.push_str(&format!(
            "File Processing Rate: {:.1} files/second\n",
            self.file_processing_rate
        ));
        report.push_str(&format!(
            "Parsing Rate: {:.1} files/second\n",
            self.parsing_rate
        ));
        report.push_str(&format!(
            "Initial Memory Usage: {} ({} bytes)\n",
            format_bytes_human(self.initial_memory_usage),
            self.initial_memory_usage
        ));
        report.push_str(&format!(
            "Peak Memory Usage: {} ({} bytes)\n",
            format_bytes_human(self.peak_memory_usage),
            self.peak_memory_usage
        ));
        report.push_str(&format!(
            "Final Memory Usage: {} ({} bytes)\n",
            format_bytes_human(self.final_memory_usage),
            self.final_memory_usage
        ));
        report.push_str(&format!(
            "Memory Usage Delta: {}\n",
            if self.memory_usage_delta() >= 0 {
                format!(
                    "+{} (+{} bytes)",
                    format_bytes_human(self.memory_usage_delta() as u64),
                    self.memory_usage_delta()
                )
            } else {
                format!(
                    "-{} ({} bytes)",
                    format_bytes_human((-self.memory_usage_delta()) as u64),
                    self.memory_usage_delta()
                )
            }
        ));
        report.push_str(&format!(
            "Memory Efficiency: {:.2} bytes processed per byte used\n",
            self.memory_efficiency
        ));
        report.push_str("===============================================");
        report
    }
}

/// Results from coordinated directory scan
#[derive(Debug, Clone)]
pub struct ObjectsScanResults {
    /// Total objects found in cache
    pub total_objects: u64,
    /// Total compressed size of all objects
    pub total_size: u64,
    /// Objects marked as write-cached
    pub write_cached_objects: u64,
    /// Compressed size of write-cached objects
    pub write_cached_size: u64,
    /// Objects marked as read-cached (or unmarked)
    pub read_cached_objects: u64,
    /// Compressed size of read-cached objects
    pub read_cached_size: u64,
    /// Metadata entries for detailed processing
    pub metadata_entries: Vec<CacheMetadataEntry>,
    /// Scan duration for performance monitoring
    pub scan_duration: Duration,
    /// Errors encountered during scan
    pub scan_errors: Vec<ScanError>,
    /// Whether results were loaded from size_state.json instead of a real scan
    pub loaded_from_state_files: bool,
}

impl ObjectsScanResults {
    /// Create empty scan results
    pub fn empty() -> Self {
        Self {
            total_objects: 0,
            total_size: 0,
            write_cached_objects: 0,
            write_cached_size: 0,
            read_cached_objects: 0,
            read_cached_size: 0,
            metadata_entries: Vec::new(),
            scan_duration: Duration::ZERO,
            scan_errors: Vec::new(),
            loaded_from_state_files: false,
        }
    }

    /// Calculate write cache usage percentage
    pub fn write_cache_percentage(&self) -> f64 {
        if self.total_size == 0 {
            0.0
        } else {
            (self.write_cached_size as f64 / self.total_size as f64) * 100.0
        }
    }

    /// Calculate read cache usage percentage
    pub fn read_cache_percentage(&self) -> f64 {
        if self.total_size == 0 {
            0.0
        } else {
            (self.read_cached_size as f64 / self.total_size as f64) * 100.0
        }
    }

    /// Format total size in human-readable format
    pub fn format_total_size(&self) -> String {
        format_bytes_human(self.total_size)
    }

    /// Format write cached size in human-readable format
    pub fn format_write_cached_size(&self) -> String {
        format_bytes_human(self.write_cached_size)
    }

    /// Format read cached size in human-readable format
    pub fn format_read_cached_size(&self) -> String {
        format_bytes_human(self.read_cached_size)
    }

    /// Get average object size
    pub fn average_object_size(&self) -> u64 {
        self.total_size.checked_div(self.total_objects).unwrap_or(0)
    }

    /// Format average object size in human-readable format
    pub fn format_average_object_size(&self) -> String {
        format_bytes_human(self.average_object_size())
    }

    /// Get error count
    pub fn error_count(&self) -> usize {
        self.scan_errors.len()
    }

    /// Check if scan had any errors
    pub fn has_errors(&self) -> bool {
        !self.scan_errors.is_empty()
    }

    /// Get summary string for logging
    pub fn summary_string(&self) -> String {
        format!(
            "{} objects ({}) - Write: {} objects ({}), Read: {} objects ({}), Errors: {}",
            self.total_objects,
            self.format_total_size(),
            self.write_cached_objects,
            self.format_write_cached_size(),
            self.read_cached_objects,
            self.format_read_cached_size(),
            self.error_count()
        )
    }
}

/// Cache metadata entry with categorization and validation
#[derive(Debug, Clone)]
pub struct CacheMetadataEntry {
    /// Path to the metadata file
    pub file_path: PathBuf,
    /// Cache key extracted from the metadata
    pub cache_key: String,
    /// Parsed metadata (None if parsing failed)
    pub metadata: Option<NewCacheMetadata>,
    /// Compressed size calculated from ranges
    pub compressed_size: u64,
    /// Whether this object was cached via PUT (write-through cache)
    pub is_write_cached: bool,
    /// Errors encountered during parsing
    pub parse_errors: Vec<String>,
}

impl CacheMetadataEntry {
    /// Create a new CacheMetadataEntry from a file path
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            cache_key: String::new(),
            metadata: None,
            compressed_size: 0,
            is_write_cached: false,
            parse_errors: Vec::new(),
        }
    }

    /// Create from file path with error handling
    pub async fn from_file(path: PathBuf) -> Result<Option<Self>> {
        let mut entry = Self::new(path.clone());

        // Read and parse metadata file
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => match serde_json::from_str::<NewCacheMetadata>(&content) {
                Ok(metadata) => {
                    entry.cache_key = metadata.cache_key.clone();
                    entry.compressed_size = metadata
                        .ranges
                        .iter()
                        .map(|range| range.compressed_size)
                        .sum();
                    entry.is_write_cached = metadata.object_metadata.is_write_cached;
                    entry.metadata = Some(metadata);
                }
                Err(e) => {
                    entry.parse_errors.push(format!("JSON parse error: {}", e));
                }
            },
            Err(e) => {
                entry.parse_errors.push(format!("File read error: {}", e));
            }
        }

        Ok(Some(entry))
    }

    /// Create from file path synchronously (for use in parallel context)
    pub fn from_file_sync(path: PathBuf) -> Self {
        let mut entry = Self::new(path.clone());

        // Read and parse metadata file
        match std::fs::read_to_string(&path) {
            Ok(content) => match serde_json::from_str::<NewCacheMetadata>(&content) {
                Ok(metadata) => {
                    entry.cache_key = metadata.cache_key.clone();
                    entry.compressed_size = metadata
                        .ranges
                        .iter()
                        .map(|range| range.compressed_size)
                        .sum();
                    entry.is_write_cached = metadata.object_metadata.is_write_cached;
                    entry.metadata = Some(metadata);
                }
                Err(e) => {
                    entry.parse_errors.push(format!("JSON parse error: {}", e));
                }
            },
            Err(e) => {
                entry.parse_errors.push(format!("File read error: {}", e));
            }
        }

        entry
    }

    /// Check if entry is valid for processing
    pub fn is_valid(&self) -> bool {
        self.metadata.is_some() && self.parse_errors.is_empty()
    }

    /// Check if entry has errors
    pub fn has_errors(&self) -> bool {
        !self.parse_errors.is_empty()
    }

    /// Get categorization for subsystem routing
    pub fn get_category(&self) -> CacheCategory {
        if !self.is_valid() {
            CacheCategory::Invalid
        } else if self.is_write_cached {
            CacheCategory::WriteCached
        } else {
            CacheCategory::ReadCached
        }
    }

    /// Get error summary string
    pub fn error_summary(&self) -> String {
        if self.parse_errors.is_empty() {
            "No errors".to_string()
        } else {
            self.parse_errors.join("; ")
        }
    }
}

/// Categorization of cache entries
#[derive(Debug, Clone, PartialEq)]
pub enum CacheCategory {
    /// Write-cached object (PUT operation)
    WriteCached,
    /// Read-cached object (GET operation)
    ReadCached,
    /// Invalid or corrupted entry
    Invalid,
}

/// Error encountered during cache scanning
#[derive(Debug, Clone)]
pub struct ScanError {
    /// Path where the error occurred
    pub path: PathBuf,
    /// Error message
    pub message: String,
    /// Error category
    pub category: ScanErrorCategory,
}

impl ScanError {
    /// Create a new scan error
    pub fn new(path: PathBuf, message: String, category: ScanErrorCategory) -> Self {
        Self {
            path,
            message,
            category,
        }
    }

    /// Create a file read error
    pub fn file_read_error(path: PathBuf, error: std::io::Error) -> Self {
        Self::new(
            path,
            format!("File read error: {}", error),
            ScanErrorCategory::FileRead,
        )
    }

    /// Create a JSON parse error
    pub fn json_parse_error(path: PathBuf, error: serde_json::Error) -> Self {
        Self::new(
            path,
            format!("JSON parse error: {}", error),
            ScanErrorCategory::JsonParse,
        )
    }

    /// Create a validation error
    pub fn validation_error(path: PathBuf, message: String) -> Self {
        Self::new(path, message, ScanErrorCategory::Validation)
    }
}

/// Category of scan error
#[derive(Debug, Clone, PartialEq)]
pub enum ScanErrorCategory {
    /// File could not be read
    FileRead,
    /// JSON parsing failed
    JsonParse,
    /// Metadata validation failed
    Validation,
    /// Directory access failed
    DirectoryAccess,
    /// Other error
    Other,
}

/// Results from subsystem initialization
#[derive(Debug, Clone)]
pub struct SubsystemResults {
    /// Whether write cache manager was initialized
    pub write_cache_initialized: bool,
    /// Whether size tracker was initialized
    pub size_tracker_initialized: bool,
}

impl SubsystemResults {
    /// Create empty subsystem results
    pub fn empty() -> Self {
        Self {
            write_cache_initialized: false,
            size_tracker_initialized: false,
        }
    }
}

/// Results from cross-validation between subsystems
#[derive(Debug, Clone)]
pub struct ValidationResults {
    /// Whether write cache sizes are consistent
    pub write_cache_consistent: bool,
    /// Size discrepancy as percentage
    pub size_discrepancy_percent: f64,
    /// Validation warnings
    pub validation_warnings: Vec<String>,
    /// Write cache manager size (if available)
    pub write_cache_manager_size: Option<u64>,
    /// Size tracker write cache size (if available)
    pub size_tracker_write_cache_size: Option<u64>,
}

impl ValidationResults {
    /// Create empty validation results
    pub fn empty() -> Self {
        Self {
            write_cache_consistent: true,
            size_discrepancy_percent: 0.0,
            validation_warnings: Vec::new(),
            write_cache_manager_size: None,
            size_tracker_write_cache_size: None,
        }
    }

    /// Create validation results indicating no cross-validation was performed
    pub(crate) fn not_performed(reason: String) -> Self {
        Self {
            write_cache_consistent: true,
            size_discrepancy_percent: 0.0,
            validation_warnings: vec![format!("Cross-validation not performed: {}", reason)],
            write_cache_manager_size: None,
            size_tracker_write_cache_size: None,
        }
    }

    /// Check if validation was actually performed
    pub fn was_performed(&self) -> bool {
        self.write_cache_manager_size.is_some() && self.size_tracker_write_cache_size.is_some()
    }

    /// Get summary string for logging
    pub fn summary_string(&self) -> String {
        if !self.was_performed() {
            return "Cross-validation not performed".to_string();
        }

        let manager_size = self.write_cache_manager_size.unwrap_or(0);
        let tracker_size = self.size_tracker_write_cache_size.unwrap_or(0);

        if self.write_cache_consistent {
            format!(
                "Write cache sizes consistent: {} vs {} ({:.1}% difference)",
                format_bytes_human(manager_size),
                format_bytes_human(tracker_size),
                self.size_discrepancy_percent
            )
        } else {
            format!(
                "Write cache size discrepancy: {} vs {} ({:.1}% difference)",
                format_bytes_human(manager_size),
                format_bytes_human(tracker_size),
                self.size_discrepancy_percent
            )
        }
    }

    /// Check if validation indicates a serious problem
    pub fn has_serious_discrepancy(&self) -> bool {
        self.was_performed() && self.size_discrepancy_percent > 20.0
    }

    /// Check if validation indicates a minor discrepancy
    pub fn has_minor_discrepancy(&self) -> bool {
        self.was_performed()
            && self.size_discrepancy_percent > 5.0
            && self.size_discrepancy_percent <= 20.0
    }
}

/// The removal predicate for a HEAD-only entry.
///
/// `Some((reason, expired_for))` when the entry holds no ranges and either its HEAD expiry
/// or, failing that, its object expiry passed more than `grace` ago. `None` otherwise.
///
/// A free function over a `NewCacheMetadata` value, so the validation pass
/// (`cache_size_tracker::stale_head_only_removable`, one day's `grace`) can evaluate the same
/// predicate twice — once on the scan's read as a filter, and again on a fresh read taken under
/// the key's metadata slot lock, which is the read the deletion acts on.
pub(crate) fn stale_head_only_entry(
    metadata: &NewCacheMetadata,
    now: std::time::SystemTime,
    grace: Duration,
) -> Option<(&'static str, Duration)> {
    let (reason, expires) = head_only_expiry(metadata)?;
    now.duration_since(expires)
        .ok()
        .filter(|expired_for| *expired_for > grace)
        .map(|expired_for| (reason, expired_for))
}

/// The instant a range-less document's HEAD expiry is judged against, with its source:
/// `head_expires_at`, or the object-level `expires_at` when HEAD was never cached or was
/// cleared. `None` when the document lists a range.
pub(crate) fn head_only_expiry(
    metadata: &NewCacheMetadata,
) -> Option<(&'static str, std::time::SystemTime)> {
    if !metadata.ranges.is_empty() {
        return None;
    }
    Some(match metadata.head_expires_at {
        Some(head_expires) => ("head_expires_at", head_expires),
        None => ("no head_expires_at, object expires_at", metadata.expires_at),
    })
}

/// Format duration in human-readable format
fn format_duration_human(duration: Duration) -> String {
    let total_ms = duration.as_millis();
    if total_ms < 1000 {
        format!("{}ms", total_ms)
    } else if total_ms < 60000 {
        format!("{:.1}s", duration.as_secs_f64())
    } else {
        let minutes = total_ms / 60000;
        let seconds = (total_ms % 60000) / 1000;
        format!("{}m{}s", minutes, seconds)
    }
}

/// Format bytes in human-readable format
fn format_bytes_human(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

/// RAII guard for initialization lock
///
/// This structure provides distributed locking for cache initialization
/// to prevent multiple instances from scanning simultaneously.
///
/// # Requirements
/// - Requirement 10.1: Acquire appropriate locks before scanning
/// - Requirement 10.5: Release locks properly on completion/failure
pub struct InitializationLock {
    /// File handle for the lock
    file: std::fs::File,
    /// Path to the lock file for debugging
    path: PathBuf,
}

impl Drop for InitializationLock {
    fn drop(&mut self) {
        // Unlock the file when the guard is dropped
        let _ = fs4::FileExt::unlock(&self.file);
        debug!(
            "Cache initialization: Released initialization lock at {:?}",
            self.path
        );
    }
}

/// Information about cache state consistency across instances
///
/// This structure tracks the results of cache consistency checks
/// to ensure all instances have a consistent view of the cache.
///
/// # Requirements
/// - Requirement 10.2: Add coordination for shared cache validation
/// - Requirement 10.3: Ensure consistent view after initialization
#[derive(Debug, Clone)]
pub struct CacheConsistencyInfo {
    /// Status of shared cache validation coordination
    pub shared_validation_status: String,
    /// Message describing shared validation status
    pub shared_validation_message: Option<String>,
    /// Status of cache directory consistency
    pub directory_consistency_status: String,
    /// Message describing directory consistency status
    pub directory_consistency_message: Option<String>,
    /// Status of subsystem consistency
    pub subsystem_consistency_status: String,
    /// Message describing subsystem consistency status
    pub subsystem_consistency_message: Option<String>,
    /// Number of warnings encountered during consistency checks
    pub warnings: u32,
}

impl Default for CacheConsistencyInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheConsistencyInfo {
    /// Create new cache consistency info
    pub fn new() -> Self {
        Self {
            shared_validation_status: "NOT_CHECKED".to_string(),
            shared_validation_message: None,
            directory_consistency_status: "NOT_CHECKED".to_string(),
            directory_consistency_message: None,
            subsystem_consistency_status: "NOT_CHECKED".to_string(),
            subsystem_consistency_message: None,
            warnings: 0,
        }
    }

    /// Get overall consistency status
    pub(crate) fn overall_status(&self) -> String {
        if self.warnings == 0 {
            "CONSISTENT".to_string()
        } else if self.warnings <= 2 {
            "MINOR_ISSUES".to_string()
        } else {
            "MAJOR_ISSUES".to_string()
        }
    }

    /// Get summary string for logging
    pub fn summary_string(&self) -> String {
        format!(
            "Overall: {}, Shared validation: {}, Directory: {}, Subsystems: {}, Warnings: {}",
            self.overall_status(),
            self.shared_validation_status,
            self.directory_consistency_status,
            self.subsystem_consistency_status,
            self.warnings
        )
    }
}

impl std::fmt::Display for CacheConsistencyInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary_string())
    }
}

/// Information about validation freshness check
#[derive(Debug, Clone)]
pub struct ValidationFreshnessInfo {
    /// Whether the validation is fresh (< 23 hours old)
    pub is_fresh: bool,
    /// Descriptive message about the freshness status
    pub message: String,
    /// Timestamp of last validation if available
    pub last_validation: Option<std::time::SystemTime>,
}

/// Information about a specific validation check
#[derive(Debug, Clone)]
pub struct ValidationInfo {
    /// Status of the validation
    pub status: String,
    /// Descriptive message about the validation result
    pub message: String,
    /// Whether this validation result needs attention
    pub needs_attention: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EvictionAlgorithm, InitializationConfig, SharedStorageConfig};
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_test_cache_config() -> CacheConfig {
        CacheConfig {
            cache_dir: PathBuf::from("/tmp/test"),
            max_cache_size: 1024 * 1024 * 1024, // 1GB
            ram_cache_enabled: false,
            max_ram_cache_size: 0,
            eviction_algorithm: EvictionAlgorithm::TinyLFU,
            write_cache_enabled: true,
            write_cache_percent: 0.5,
            write_cache_max_object_size: 1024 * 1024 * 100, // 100MB
            disk_safety_free_space_floor: 1024 * 1024 * 1024, // 1 GiB
            put_ttl: Duration::from_secs(86400),            // 1 day
            get_ttl: Duration::from_secs(3600),             // 1 hour
            head_ttl: Duration::from_secs(300),             // 5 minutes
            actively_remove_cached_data: false,
            shared_storage: SharedStorageConfig::default(),
            download_coordination: crate::config::DownloadCoordinationConfig::default(),
            range_merge_gap_threshold: 1024 * 1024, // 1MB
            eviction_buffer_percent: 5,
            ram_cache_flush_interval: Duration::from_secs(60),
            ram_cache_verification_interval: Duration::from_secs(1),
            incomplete_upload_ttl: Duration::from_secs(86400), // 1 day
            initialization: InitializationConfig::default(),
            cache_bypass_headers_enabled: true, // Default enabled
            metadata_cache: crate::config::MetadataCacheConfig::default(),
            eviction_trigger_percent: 95, // Trigger eviction at 95% capacity
            eviction_target_percent: 80,
            full_object_check_threshold: 67_108_864, // 64 MiB
            disk_streaming_threshold: 1_048_576,     // 1 MiB
            compression_batch_size: 1_048_576,       // 1 MiB
            read_cache_enabled: true,
            bucket_settings_staleness_threshold: Duration::from_secs(60),
            evaluate_conditions_from_cache: true,
            ram_cache_shard_count: 8,
            max_complete_body_bytes: 10 * 1024 * 1024, // 10 MiB
            max_metadata_file_bytes: 4 * 1024 * 1024,  // 4 MiB
            metadata_io_concurrency: 32,
            partial_range_commit_ratio: 0.5,
        }
    }

    #[test]
    fn test_coordinator_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator = CacheInitializationCoordinator::new(
            temp_dir.path().to_path_buf(),
            true,
            config.clone(),
        );

        assert_eq!(coordinator.cache_dir(), temp_dir.path());
        assert!(coordinator.write_cache_enabled);
        assert_eq!(
            coordinator.config().write_cache_enabled,
            config.write_cache_enabled
        );
    }

    #[test]
    fn test_format_duration_human() {
        assert_eq!(format_duration_human(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration_human(Duration::from_millis(1500)), "1.5s");
        assert_eq!(format_duration_human(Duration::from_secs(90)), "1m30s");
    }

    #[test]
    fn test_format_bytes_human() {
        assert_eq!(format_bytes_human(512), "512 B");
        assert_eq!(format_bytes_human(1536), "1.5 KB");
        assert_eq!(format_bytes_human(2097152), "2.0 MB");
    }

    #[test]
    fn test_objects_scan_results() {
        let mut results = ObjectsScanResults::empty();
        results.total_objects = 10;
        results.total_size = 1000;
        results.write_cached_size = 250;
        results.read_cached_size = 750;

        assert_eq!(results.write_cache_percentage(), 25.0);
        assert_eq!(results.read_cache_percentage(), 75.0);
        assert_eq!(results.format_total_size(), "1000 B");
        assert_eq!(results.format_write_cached_size(), "250 B");
        assert_eq!(results.format_read_cached_size(), "750 B");
        assert_eq!(results.average_object_size(), 100);
        assert_eq!(results.format_average_object_size(), "100 B");
        assert!(!results.has_errors());
        assert_eq!(results.error_count(), 0);
    }

    #[test]
    fn test_cache_metadata_entry() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.meta");

        let mut entry = CacheMetadataEntry::new(file_path.clone());
        entry.cache_key = "test-bucket/test-object".to_string();
        entry.compressed_size = 1024;
        entry.is_write_cached = true;

        assert_eq!(entry.file_path, file_path);
        assert_eq!(entry.cache_key, "test-bucket/test-object");
        assert_eq!(entry.compressed_size, 1024);
        assert!(entry.is_write_cached);
        assert!(!entry.is_valid()); // No metadata set
        assert!(!entry.has_errors());
        assert_eq!(entry.get_category(), CacheCategory::Invalid);
    }

    #[test]
    fn test_scan_error() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.meta");

        let error = ScanError::file_read_error(
            file_path.clone(),
            std::io::Error::new(std::io::ErrorKind::NotFound, "File not found"),
        );

        assert_eq!(error.path, file_path);
        assert!(error.message.contains("File read error"));
        assert_eq!(error.category, ScanErrorCategory::FileRead);
    }

    #[test]
    fn test_cache_category() {
        assert_eq!(CacheCategory::WriteCached, CacheCategory::WriteCached);
        assert_ne!(CacheCategory::WriteCached, CacheCategory::ReadCached);
        assert_ne!(CacheCategory::ReadCached, CacheCategory::Invalid);
    }

    #[tokio::test]
    async fn test_cross_validate_subsystems_both_available() {
        use crate::cache::CacheEvictionAlgorithm;
        use crate::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
        use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
        use crate::journal_manager::JournalManager;
        use crate::metadata_lock_manager::MetadataLockManager;
        use crate::write_cache_manager::WriteCacheManager;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let config = create_test_cache_config();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        let coordinator =
            CacheInitializationCoordinator::new(cache_dir.clone(), true, config.clone());

        // Create write cache manager with some usage
        let write_cache_manager = WriteCacheManager::new(
            cache_dir.clone(),
            1024 * 1024, // 1MB total cache
            50.0,        // 50% for write cache
            config.put_ttl,
            config.incomplete_upload_ttl,
            CacheEvictionAlgorithm::TinyLFU,
            config.write_cache_max_object_size,
        );
        // No seeding: `initialize_from_scan_results` was removed in task 27 because
        // seeding an admission counter from persisted state was the defect. In-flight
        // bytes therefore start at 0, which is what this test now exercises.

        // Create consolidator and set write cache size
        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = ConsolidationConfig::default();
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));
        consolidator.initialize().await.unwrap();
        // Set write cache size to match manager
        consolidator
            .update_size_from_validation(512, Some(512), None)
            .await;

        // Create size tracker with consolidator reference
        let size_config = CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: true,
            incomplete_upload_ttl: Duration::from_secs(86400),
            validation_max_duration: Duration::from_secs(4 * 3600),
            ..Default::default()
        };
        let size_tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, size_config, false, consolidator)
                .await
                .unwrap(),
        );

        let results = coordinator
            .cross_validate_subsystems(Some(&write_cache_manager), Some(&size_tracker))
            .await
            .unwrap();

        assert!(results.was_performed());
        assert!(results.write_cache_consistent);
        assert_eq!(results.size_discrepancy_percent, 0.0);
        assert!(results.validation_warnings.is_empty());
        // Was `Some(512)` when the manager's counter was seeded from the scan's resident
        // figure. Task 27 removed that seeding — Inflight_Bytes starts at zero on every
        // start, because a fresh process has no uploads in flight — so 0 is the correct
        // value here and 512 would now indicate the seeding had come back.
        assert_eq!(results.write_cache_manager_size, Some(0));
        assert_eq!(results.size_tracker_write_cache_size, Some(512));
    }

    /// The write-cache cross-validation is RETIRED (task 27), and this asserts the
    /// retirement rather than the thresholds it replaced.
    ///
    /// It replaces `test_cross_validate_subsystems_warning_threshold` and
    /// `..._error_threshold`, which asserted a 5% warning and a 20% `error!` escalation on
    /// the difference between `WriteCacheManager::current_usage()` and the tracker's
    /// `write_cache_size`. Those two figures measure different quantities now —
    /// Inflight_Bytes on this instance versus Resident_Bytes across the fleet — so any
    /// deployment with staged data would trip the error threshold on **every startup**.
    ///
    /// The fixture makes that concrete: the manager holds nothing in flight while the
    /// tracker reports 1000 staged bytes, which is a 100% difference under the old
    /// arithmetic and therefore the loudest possible false alarm. It must now be reported
    /// as consistent, with both figures still carried for observability.
    #[tokio::test]
    async fn cross_validation_of_inflight_against_resident_is_retired() {
        use crate::cache::CacheEvictionAlgorithm;
        use crate::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
        use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
        use crate::journal_manager::JournalManager;
        use crate::metadata_lock_manager::MetadataLockManager;
        use crate::write_cache_manager::WriteCacheManager;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let config = create_test_cache_config();
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();
        let coordinator =
            CacheInitializationCoordinator::new(cache_dir.clone(), true, config.clone());

        let write_cache_manager = WriteCacheManager::new(
            cache_dir.clone(),
            1024 * 1024,
            50.0,
            config.put_ttl,
            config.incomplete_upload_ttl,
            CacheEvictionAlgorithm::TinyLFU,
            config.write_cache_max_object_size,
        );
        // Nothing in flight — the correct state for a freshly started process (R7.2).
        assert_eq!(write_cache_manager.current_usage(), 0);

        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            ConsolidationConfig::default(),
        ));
        consolidator.initialize().await.unwrap();
        // 1000 resident staged bytes on the shared volume.
        consolidator
            .update_size_from_validation(1000, Some(1000), None)
            .await;

        let size_config = CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: false,
            incomplete_upload_ttl: Duration::from_secs(86400),
            validation_max_duration: Duration::from_secs(4 * 3600),
            ..Default::default()
        };
        let size_tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, size_config, false, consolidator)
                .await
                .unwrap(),
        );

        let results = coordinator
            .cross_validate_subsystems(Some(&write_cache_manager), Some(&size_tracker))
            .await
            .unwrap();

        assert!(
            results.write_cache_consistent,
            "0 in-flight against 1000 resident is the expected steady state, not an \
             inconsistency; reinstating the comparison makes this a permanent startup alarm"
        );
        assert_eq!(results.size_discrepancy_percent, 0.0);
        assert!(
            results.validation_warnings.is_empty(),
            "no warning may be raised for two figures that are not comparable"
        );
        // Both figures are still reported — retiring the comparison must not lose the data.
        assert_eq!(results.write_cache_manager_size, Some(0));
        assert_eq!(results.size_tracker_write_cache_size, Some(1000));
    }

    #[tokio::test]
    async fn test_cross_validate_subsystems_manager_not_available() {
        use crate::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
        use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
        use crate::journal_manager::JournalManager;
        use crate::metadata_lock_manager::MetadataLockManager;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let config = create_test_cache_config();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        let coordinator =
            CacheInitializationCoordinator::new(cache_dir.clone(), true, config.clone());

        // Create consolidator
        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = ConsolidationConfig::default();
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));
        consolidator.initialize().await.unwrap();

        let size_config = CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: true,
            incomplete_upload_ttl: Duration::from_secs(86400),
            validation_max_duration: Duration::from_secs(4 * 3600),
            ..Default::default()
        };
        let size_tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, size_config, false, consolidator)
                .await
                .unwrap(),
        );

        let results = coordinator
            .cross_validate_subsystems(
                None, // No write cache manager
                Some(&size_tracker),
            )
            .await
            .unwrap();

        assert!(!results.was_performed());
        assert!(results.write_cache_consistent); // Default to consistent when not performed
        assert_eq!(results.size_discrepancy_percent, 0.0);
        assert_eq!(results.validation_warnings.len(), 1);
        assert!(results.validation_warnings[0].contains("Write cache manager not available"));
        assert_eq!(results.write_cache_manager_size, None);
        assert_eq!(results.size_tracker_write_cache_size, None);
    }

    #[tokio::test]
    async fn test_cross_validate_subsystems_tracker_not_available() {
        use crate::cache::CacheEvictionAlgorithm;
        use crate::write_cache_manager::WriteCacheManager;

        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator = CacheInitializationCoordinator::new(
            temp_dir.path().to_path_buf(),
            true,
            config.clone(),
        );

        let write_cache_manager = WriteCacheManager::new(
            temp_dir.path().to_path_buf(),
            1024 * 1024, // 1MB total cache
            50.0,        // 50% for write cache
            config.put_ttl,
            config.incomplete_upload_ttl,
            CacheEvictionAlgorithm::TinyLFU,
            config.write_cache_max_object_size,
        );

        let results = coordinator
            .cross_validate_subsystems(
                Some(&write_cache_manager),
                None, // No size tracker
            )
            .await
            .unwrap();

        assert!(!results.was_performed());
        assert!(results.write_cache_consistent); // Default to consistent when not performed
        assert_eq!(results.size_discrepancy_percent, 0.0);
        assert_eq!(results.validation_warnings.len(), 1);
        assert!(results.validation_warnings[0].contains("Size tracker not available"));
        assert_eq!(results.write_cache_manager_size, None);
        assert_eq!(results.size_tracker_write_cache_size, None);
    }

    #[tokio::test]
    async fn test_cross_validate_subsystems_both_zero_size() {
        use crate::cache::CacheEvictionAlgorithm;
        use crate::cache_size_tracker::{CacheSizeConfig, CacheSizeTracker};
        use crate::journal_consolidator::{ConsolidationConfig, JournalConsolidator};
        use crate::journal_manager::JournalManager;
        use crate::metadata_lock_manager::MetadataLockManager;
        use crate::write_cache_manager::WriteCacheManager;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().to_path_buf();
        let config = create_test_cache_config();

        // Create required directories
        std::fs::create_dir_all(cache_dir.join("metadata/_journals")).unwrap();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        std::fs::create_dir_all(cache_dir.join("locks")).unwrap();

        let coordinator =
            CacheInitializationCoordinator::new(cache_dir.clone(), true, config.clone());

        // Both subsystems with zero size
        let write_cache_manager = WriteCacheManager::new(
            cache_dir.clone(),
            1024 * 1024, // 1MB total cache
            50.0,        // 50% for write cache
            config.put_ttl,
            config.incomplete_upload_ttl,
            CacheEvictionAlgorithm::TinyLFU,
            config.write_cache_max_object_size,
        );

        // Create consolidator
        let journal_manager = Arc::new(JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = ConsolidationConfig::default();
        let consolidator = Arc::new(JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));
        consolidator.initialize().await.unwrap();

        let size_config = CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: true,
            incomplete_upload_ttl: Duration::from_secs(86400),
            validation_max_duration: Duration::from_secs(4 * 3600),
            ..Default::default()
        };
        let size_tracker = Arc::new(
            CacheSizeTracker::new(cache_dir, size_config, false, consolidator)
                .await
                .unwrap(),
        );

        let results = coordinator
            .cross_validate_subsystems(Some(&write_cache_manager), Some(&size_tracker))
            .await
            .unwrap();

        assert!(results.was_performed());
        assert!(results.write_cache_consistent);
        assert_eq!(results.size_discrepancy_percent, 0.0);
        assert!(results.validation_warnings.is_empty());
        assert_eq!(results.write_cache_manager_size, Some(0));
        assert_eq!(results.size_tracker_write_cache_size, Some(0));
    }

    #[test]
    fn test_validation_results() {
        let mut results = ValidationResults::empty();
        assert!(results.write_cache_consistent);
        assert_eq!(results.size_discrepancy_percent, 0.0);
        assert!(results.validation_warnings.is_empty());
        assert!(!results.was_performed());

        results.write_cache_manager_size = Some(1000);
        results.size_tracker_write_cache_size = Some(900);
        results.size_discrepancy_percent = 10.0;
        results.write_cache_consistent = false;

        assert!(results.was_performed());
        assert!(!results.write_cache_consistent);
        let summary = results.summary_string();
        assert!(summary.contains("Write cache size discrepancy"));
        assert!(summary.contains("10.0% difference"));

        let not_performed = ValidationResults::not_performed("test reason".to_string());
        assert!(!not_performed.was_performed());
        assert_eq!(not_performed.validation_warnings.len(), 1);
        assert!(not_performed.validation_warnings[0].contains("test reason"));
    }

    #[test]
    fn test_validation_results_serious_threshold() {
        let mut results = ValidationResults::empty();
        results.write_cache_manager_size = Some(1000);
        results.size_tracker_write_cache_size = Some(750);
        results.size_discrepancy_percent = 25.0;
        results.write_cache_consistent = false;

        assert!(results.has_serious_discrepancy());
        assert!(!results.has_minor_discrepancy());
    }

    #[test]
    fn test_validation_results_warning_threshold() {
        let mut results = ValidationResults::empty();
        results.write_cache_manager_size = Some(1000);
        results.size_tracker_write_cache_size = Some(900);
        results.size_discrepancy_percent = 10.0;
        results.write_cache_consistent = false;

        assert!(!results.has_serious_discrepancy());
        assert!(results.has_minor_discrepancy());
    }

    #[test]
    fn test_validation_results_consistent() {
        let mut results = ValidationResults::empty();
        results.write_cache_manager_size = Some(1000);
        results.size_tracker_write_cache_size = Some(1000);
        results.size_discrepancy_percent = 0.0;
        results.write_cache_consistent = true;

        assert!(!results.has_serious_discrepancy());
        assert!(!results.has_minor_discrepancy());

        let summary = results.summary_string();
        assert!(summary.contains("Write cache sizes consistent"));
    }

    #[test]
    fn test_validation_results_not_performed() {
        let results = ValidationResults::not_performed("test reason".to_string());

        assert!(!results.has_serious_discrepancy());
        assert!(!results.has_minor_discrepancy());
        assert_eq!(results.summary_string(), "Cross-validation not performed");
    }

    #[tokio::test]
    async fn test_initialization_lock_acquisition() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator =
            CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

        // Test successful lock acquisition
        let lock = coordinator.try_acquire_initialization_lock().await.unwrap();

        // Verify lock file exists
        let lock_path = temp_dir.path().join("locks").join("initialization.lock");
        assert!(lock_path.exists());

        // Test that second acquisition fails (lock is held)
        let second_attempt = coordinator.try_acquire_initialization_lock().await;
        assert!(second_attempt.is_err());

        // Drop the lock and verify second acquisition succeeds
        drop(lock);
        let second_lock = coordinator.try_acquire_initialization_lock().await.unwrap();
        drop(second_lock);
    }

    #[tokio::test]
    async fn test_cache_consistency_info() {
        let mut info = CacheConsistencyInfo::new();

        assert_eq!(info.overall_status(), "CONSISTENT");
        assert_eq!(info.warnings, 0);
        assert_eq!(info.shared_validation_status, "NOT_CHECKED");

        // Add some warnings
        info.warnings = 1;
        assert_eq!(info.overall_status(), "MINOR_ISSUES");

        info.warnings = 3;
        assert_eq!(info.overall_status(), "MAJOR_ISSUES");

        // Test summary string
        info.shared_validation_status = "WORKING".to_string();
        info.directory_consistency_status = "CONSISTENT".to_string();
        info.subsystem_consistency_status = "CONSISTENT".to_string();

        let summary = info.summary_string();
        assert!(summary.contains("MAJOR_ISSUES"));
        assert!(summary.contains("WORKING"));
        assert!(summary.contains("CONSISTENT"));
        assert!(summary.contains("Warnings: 3"));
    }

    #[tokio::test]
    async fn test_verify_cache_directory_consistency() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator =
            CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

        // Create required directories
        for dir in &["metadata", "ranges", "locks", "mpus_in_progress"] {
            tokio::fs::create_dir_all(temp_dir.path().join(dir))
                .await
                .unwrap();
        }

        let validation_info = coordinator
            .verify_cache_directory_consistency()
            .await
            .unwrap();

        assert_eq!(validation_info.status, "CONSISTENT");
        assert!(!validation_info.needs_attention);
        assert!(validation_info
            .message
            .contains("accessible and consistent"));
    }

    #[tokio::test]
    async fn test_verify_cache_directory_consistency_missing_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator =
            CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

        // Don't create any directories - should detect missing critical directories
        let validation_info = coordinator
            .verify_cache_directory_consistency()
            .await
            .unwrap();

        assert_eq!(validation_info.status, "INCONSISTENT");
        assert!(validation_info.needs_attention);
        assert!(validation_info.message.contains("metadata"));
        assert!(validation_info.message.contains("ranges"));
        assert!(validation_info.message.contains("locks"));
    }

    #[tokio::test]
    async fn test_ensure_cache_state_consistency_post_init() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator = CacheInitializationCoordinator::new(
            temp_dir.path().to_path_buf(),
            false, // Disable write cache for simpler test
            config.clone(),
        );

        // Create required directories
        for dir in &[
            "metadata",
            "ranges",
            "locks",
            "metadata/_journals",
            "size_tracking",
        ] {
            tokio::fs::create_dir_all(temp_dir.path().join(dir))
                .await
                .unwrap();
        }

        // Create consolidator
        let cache_dir = temp_dir.path().to_path_buf();
        let journal_manager = Arc::new(crate::journal_manager::JournalManager::new(
            cache_dir.clone(),
            "test-instance".to_string(),
        ));
        let lock_manager = Arc::new(crate::metadata_lock_manager::MetadataLockManager::new(
            cache_dir.join("locks"),
            Duration::from_secs(30),
            3,
        ));
        let consolidation_config = crate::journal_consolidator::ConsolidationConfig::default();
        let consolidator = Arc::new(crate::journal_consolidator::JournalConsolidator::new(
            cache_dir.clone(),
            journal_manager,
            lock_manager,
            consolidation_config,
        ));
        consolidator.initialize().await.unwrap();

        // Create size tracker
        let size_config = crate::cache_size_tracker::CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: true,
            incomplete_upload_ttl: Duration::from_secs(86400),
            validation_max_duration: Duration::from_secs(4 * 3600),
            ..Default::default()
        };
        let size_tracker = Arc::new(
            crate::cache_size_tracker::CacheSizeTracker::new(
                cache_dir,
                size_config,
                false,
                consolidator,
            )
            .await
            .unwrap(),
        );

        let consistency_info = coordinator
            .ensure_cache_state_consistency_post_init(Some(&size_tracker))
            .await
            .unwrap();

        // Should have minimal warnings since directories are properly set up
        assert!(consistency_info.warnings <= 1); // May have 1 warning for missing write cache dirs
        assert_eq!(consistency_info.directory_consistency_status, "CONSISTENT");
        assert!(
            consistency_info.shared_validation_status == "WORKING"
                || consistency_info.shared_validation_status == "BUSY"
        );
    }
}

/// Stale-validation-lock handling in `perform_validation_recovery_if_needed`.
///
/// # Why these tests exist and what they can and cannot prove
///
/// The removal used to be gated on `mtime age > 1 hour` with no liveness check. Nothing
/// advances that mtime — the lock file is a zero-byte sentinel opened `truncate(false)` and
/// only `flock`ed — so the predicate was satisfied by any lock file older than an hour whether
/// or not a pass was running. And because `flock` binds to the open file description rather
/// than to the path, removing the path does not release a held lock: it lets the next opener
/// take an *independent* lock on a new inode, so two validation passes run at once, each doing
/// a lock-free absolute install of `total_size` / `write_cache_size` / `cached_objects`.
///
/// **These tests pin the DECISION, on the local filesystem, and nothing more.** Whether the
/// underlying survives-unlink / independent-lock behaviour holds on EFS, FSx for OpenZFS or FSx
/// for NetApp ONTAP is a property of each backend's lock implementation (NFS `flock` is emulated
/// with POSIX byte-range locks, and lease-expiry timing differs per server) and CANNOT be
/// established here. A local pass is therefore necessary and not sufficient; the shared-storage
/// arm is a fleet test, designed in `dr-adapt-findings.md` § 5 and owned by the follow-up unit.
///
/// Spec: cache-eviction-at-scale.
#[cfg(test)]
mod validation_lock_staleness_tests {
    use super::*;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    fn coordinator_over(dir: &TempDir) -> CacheInitializationCoordinator {
        let cache_dir = dir.path().to_path_buf();
        std::fs::create_dir_all(cache_dir.join("size_tracking")).unwrap();
        let config = crate::config::CacheConfig {
            cache_dir: cache_dir.clone(),
            ..Default::default()
        };
        CacheInitializationCoordinator::new(cache_dir, false, config)
    }

    fn lock_path(dir: &TempDir) -> std::path::PathBuf {
        dir.path().join("size_tracking").join("validation.lock")
    }

    /// A lock nobody holds is removed, however old or new its mtime.
    ///
    /// This is the arm that keeps the housekeeping intent alive, so the fix cannot be mistaken
    /// for "stop removing the file".
    #[tokio::test]
    async fn unheld_validation_lock_is_removed() {
        let dir = TempDir::new().unwrap();
        let coordinator = coordinator_over(&dir);
        let path = lock_path(&dir);
        std::fs::write(&path, b"").unwrap();
        assert!(
            path.exists(),
            "FIXTURE: the lock file must exist to be removed"
        );

        coordinator
            .perform_validation_recovery_if_needed()
            .await
            .expect("recovery must not error");

        assert!(
            !path.exists(),
            "an UNHELD validation lock must be removed; it survived at {:?}",
            path
        );
    }

    /// A lock a peer currently holds is LEFT IN PLACE.
    ///
    /// This is the discriminator. Against the pre-fix code it FAILS: the file's mtime is its
    /// creation time, which the fixture backdates past the old 1-hour threshold, so the old
    /// branch removed it while the lock was held — the exact step that produces two concurrent
    /// validation passes. The fix reaches the same conclusion the mtime never could, by asking
    /// the lock.
    ///
    /// The held lock is taken on a SEPARATE open file description, which is what makes the
    /// exclusion real rather than an artefact of one handle being reused.
    #[tokio::test]
    async fn held_validation_lock_is_left_alone_however_old_its_mtime() {
        let dir = TempDir::new().unwrap();
        let coordinator = coordinator_over(&dir);
        let path = lock_path(&dir);

        let holder = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        fs4::FileExt::try_lock(&holder).expect("FIXTURE: the holder must acquire the lock");

        // Backdate the mtime well past the retired 1-hour threshold, so this test would have
        // been red on the old code for the reason it is about rather than by accident of timing.
        let two_days_ago = filetime::FileTime::from_system_time(
            SystemTime::now() - Duration::from_secs(2 * 86400),
        );
        filetime::set_file_mtime(&path, two_days_ago).unwrap();

        coordinator
            .perform_validation_recovery_if_needed()
            .await
            .expect("recovery must not error");

        assert!(
            path.exists(),
            "a HELD validation lock must NOT be removed, whatever its mtime says. Removing it \
             does not release the holder's flock — it lets the next instance take an independent \
             lock on a new inode and validate concurrently, and concurrent passes each install \
             absolute size figures and reset the delta files."
        );

        drop(holder);
    }

    /// After the holder releases, the same call now removes it — so the fix defers the
    /// housekeeping rather than abandoning it.
    #[tokio::test]
    async fn lock_is_removed_once_the_holder_releases() {
        let dir = TempDir::new().unwrap();
        let coordinator = coordinator_over(&dir);
        let path = lock_path(&dir);

        let holder = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        fs4::FileExt::try_lock(&holder).expect("FIXTURE: the holder must acquire the lock");

        coordinator
            .perform_validation_recovery_if_needed()
            .await
            .unwrap();
        assert!(
            path.exists(),
            "FIXTURE: held lock should have survived pass 1"
        );

        drop(holder);

        coordinator
            .perform_validation_recovery_if_needed()
            .await
            .unwrap();
        assert!(
            !path.exists(),
            "once released, the lock file must be removed by the next recovery pass"
        );
    }
}
