//! Cache Initialization Coordinator
//!
//! This module provides coordinated cache initialization to eliminate redundant
//! directory scanning, provide clear startup messaging, and ensure consistency
//! between cache subsystems while preserving architectural separation.

use crate::cache_size_tracker::CacheSizeTracker;
use crate::cache_types::NewCacheMetadata;
use crate::cache_validator::CacheValidator;
use crate::config::CacheConfig;
use crate::error::{ProxyError, Result};
use crate::write_cache_manager::WriteCacheManager;
use fs2::FileExt;
use serde_json;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

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

    /// Check if write cache is enabled
    pub fn is_write_cache_enabled(&self) -> bool {
        self.write_cache_enabled
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
        let mut phase_results = Vec::new();
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
        phase_results.push(PhaseResult {
            phase_name: "Directory Setup".to_string(),
            duration: phase1_duration,
            success: phase1_result.is_ok(),
            error_message: phase1_result.err().map(|e| e.to_string()),
        });

        // Phase 2: Metadata Scan with Consistency Validation (enhanced for Requirements 10.1, 10.2, 10.3, 10.4, 10.5)
        let phase2_start = Instant::now();
        info!("Cache initialization: Phase 2/4 - Metadata scan with consistency validation");
        let scan_result = self
            .scan_cache_metadata_with_consistency_validation(&mut performance_metrics)
            .await;
        let phase2_duration = phase2_start.elapsed();

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

        phase_results.push(PhaseResult {
            phase_name: "Metadata Scan".to_string(),
            duration: phase2_duration,
            success: scan_result.is_ok(),
            error_message: scan_result.err().map(|e| e.to_string()),
        });

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

        phase_results.push(PhaseResult {
            phase_name: "Subsystem Initialization".to_string(),
            duration: phase3_duration,
            success: subsystem_result.is_ok(),
            error_message: subsystem_result.err().map(|e| e.to_string()),
        });

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

        phase_results.push(PhaseResult {
            phase_name: "Cross-Validation".to_string(),
            duration: phase4_duration,
            success: validation_result.is_ok(),
            error_message: validation_result.err().map(|e| e.to_string()),
        });

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
            phase_results,
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
                    std::io::ErrorKind::Other if e.to_string().contains("No space") => {
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
                        std::io::ErrorKind::Other if e.to_string().contains("No space") => {
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

    /// Perform coordinated metadata scan with consistency validation
    ///
    /// This method enhances the existing metadata scan with comprehensive consistency
    /// validation as required by the atomic metadata writes specification.
    ///
    /// # Requirements
    /// - Requirement 10.1: Integrate metadata consistency validation into Phase 2
    /// - Requirement 10.2: Check validation freshness (23-hour validity)
    /// - Requirement 10.3: Skip validation if fresh validation exists
    /// - Requirement 10.4: Perform recovery from failed/incomplete validation
    /// - Requirement 10.5: Use existing distributed locking for coordination
    async fn scan_cache_metadata_with_consistency_validation(
        &self,
        performance_metrics: &mut PerformanceMetrics,
    ) -> Result<ObjectsScanResults> {
        let scan_start = Instant::now();
        info!(
            "Cache initialization: Starting coordinated metadata scan with consistency validation"
        );

        // Check validation freshness first (Requirement 10.3)
        let validation_freshness = self.check_validation_freshness().await;
        match validation_freshness {
            Ok(freshness_info) => {
                if freshness_info.is_fresh {
                    info!(
                        "Cache initialization: Validation is fresh ({}), skipping comprehensive consistency validation",
                        freshness_info.message
                    );
                    // Perform regular scan without comprehensive validation
                    return self
                        .scan_cache_metadata_with_metrics(performance_metrics)
                        .await;
                } else {
                    info!(
                        "Cache initialization: Validation is stale ({}), performing comprehensive consistency validation",
                        freshness_info.message
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Cache initialization: Failed to check validation freshness: {}. Proceeding with comprehensive validation.",
                    e
                );
            }
        }

        // Check for failed/incomplete validation and perform recovery if needed (Requirement 10.4)
        if let Err(recovery_error) = self.perform_validation_recovery_if_needed().await {
            warn!(
                "Cache initialization: Validation recovery failed: {}. Continuing with scan.",
                recovery_error
            );
        }

        // Perform regular metadata scan
        let mut scan_results = self
            .scan_cache_metadata_with_metrics(performance_metrics)
            .await?;

        // Enhance scan results with consistency validation
        let consistency_validation_start = std::time::Instant::now();
        let consistency_results = self
            .perform_metadata_consistency_validation(&scan_results)
            .await;
        let consistency_validation_duration = consistency_validation_start.elapsed();

        match consistency_results {
            Ok(consistency_info) => {
                info!(
                    "Cache initialization: Metadata consistency validation completed in {}: {}",
                    format_duration_human(consistency_validation_duration),
                    consistency_info.summary_string()
                );

                // Add consistency validation results to scan results
                if consistency_info.corruption_count > 0 {
                    warn!(
                        "Cache initialization: Found {} corrupted metadata files during consistency validation",
                        consistency_info.corruption_count
                    );
                    // Add consistency errors to scan results
                    for error_msg in consistency_info.errors {
                        scan_results
                            .scan_errors
                            .push(crate::cache_initialization_coordinator::ScanError::new(
                            std::path::PathBuf::from("consistency_validation"),
                            error_msg,
                            crate::cache_initialization_coordinator::ScanErrorCategory::Validation,
                        ));
                    }
                }

                // Update validation timestamp to mark this validation as complete
                if let Err(e) = self.update_validation_timestamp().await {
                    warn!(
                        "Cache initialization: Failed to update validation timestamp: {}",
                        e
                    );
                }
            }
            Err(e) => {
                error!(
                    "Cache initialization: Metadata consistency validation failed: {}. Continuing with regular scan results.",
                    e
                );
                // Add validation failure as an error
                scan_results.scan_errors.push(
                    crate::cache_initialization_coordinator::ScanError::new(
                        std::path::PathBuf::from("consistency_validation"),
                        format!("Consistency validation failed: {}", e),
                        crate::cache_initialization_coordinator::ScanErrorCategory::Validation,
                    ),
                );
            }
        }

        let total_duration = scan_start.elapsed();
        info!(
            "Cache initialization: Metadata scan with consistency validation completed in {}",
            format_duration_human(total_duration)
        );

        Ok(scan_results)
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

        // Check for stale validation lock
        if validation_lock.exists() {
            if let Ok(metadata) = tokio::fs::metadata(&validation_lock).await {
                if let Ok(modified) = metadata.modified() {
                    let age = std::time::SystemTime::now()
                        .duration_since(modified)
                        .unwrap_or(std::time::Duration::ZERO);
                    if age > std::time::Duration::from_secs(3600) {
                        // 1 hour
                        info!("Cache initialization: Removing stale validation lock (age: {:.1} hours)", age.as_secs_f64() / 3600.0);
                        let _ = tokio::fs::remove_file(&validation_lock).await;
                    }
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

    /// Perform metadata consistency validation on scan results
    ///
    /// # Requirements
    /// - Requirement 10.1: Validate metadata consistency during initialization
    async fn perform_metadata_consistency_validation(
        &self,
        scan_results: &ObjectsScanResults,
    ) -> Result<ConsistencyValidationInfo> {
        let validation_start = std::time::Instant::now();
        let mut corruption_count = 0;
        let mut errors = Vec::new();
        let mut validated_count = 0;

        // Create cache validator for consistency checks
        let validator = crate::cache_validator::CacheValidator::new(self.cache_dir.clone());

        // Validate each metadata entry for consistency
        for entry in &scan_results.metadata_entries {
            if let Some(metadata) = &entry.metadata {
                validated_count += 1;

                // Perform metadata integrity validation
                match validator.validate_metadata_integrity(metadata).await {
                    Ok(integrity_result) => {
                        if !integrity_result.is_valid {
                            corruption_count += 1;
                            for error in integrity_result.errors {
                                errors.push(format!(
                                    "Metadata integrity error in {}: {}",
                                    entry.cache_key, error
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        errors.push(format!(
                            "Failed to validate metadata integrity for {}: {}",
                            entry.cache_key, e
                        ));
                    }
                }

                // Perform consistency validation (check if range files exist)
                match validator
                    .validate_consistency_integrity(&metadata.cache_key)
                    .await
                {
                    Ok(consistency_result) => {
                        if !consistency_result.is_consistent {
                            corruption_count += 1;
                            errors.push(format!(
                                "Consistency error for {}: {} missing files, {} orphaned files",
                                entry.cache_key,
                                consistency_result.missing_range_files.len(),
                                consistency_result.orphaned_range_files.len()
                            ));
                        }
                    }
                    Err(e) => {
                        errors.push(format!(
                            "Failed to validate consistency for {}: {}",
                            entry.cache_key, e
                        ));
                    }
                }
            }
        }

        let validation_duration = validation_start.elapsed();

        Ok(ConsistencyValidationInfo {
            validated_count,
            corruption_count,
            errors,
            validation_duration,
        })
    }

    /// Update validation timestamp to mark validation as complete
    ///
    /// # Requirements
    /// - Requirement 10.3: Track validation completion time
    async fn update_validation_timestamp(&self) -> Result<()> {
        let size_tracking_dir = self.cache_dir.join("size_tracking");
        tokio::fs::create_dir_all(&size_tracking_dir)
            .await
            .map_err(|e| {
                crate::error::ProxyError::CacheError(format!(
                    "Failed to create size tracking directory: {}",
                    e
                ))
            })?;

        let validation_file = size_tracking_dir.join("validation.json");
        let timestamp = std::time::SystemTime::now();
        let timestamp_secs = timestamp
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::ZERO)
            .as_secs();

        let validation_data = serde_json::json!({
            "last_validation": timestamp_secs,
            "status": "completed",
            "completed_at": timestamp_secs,
            "validation_type": "metadata_consistency"
        });

        tokio::fs::write(&validation_file, validation_data.to_string())
            .await
            .map_err(|e| {
                crate::error::ProxyError::CacheError(format!(
                    "Failed to write validation timestamp: {}",
                    e
                ))
            })?;

        Ok(())
    }

    /// Perform coordinated metadata scan with performance metrics collection and optimizations
    ///
    /// This method implements the core requirement of scanning the cache metadata
    /// once and providing results to all subsystems. It uses parallel processing,
    /// lazy parsing for disabled subsystems, and memory-efficient streaming for
    /// large directories.
    ///
    /// # Requirements
    /// - Requirement 1.1: Single coordinated scan of cache metadata
    /// - Requirement 1.2: Collect metadata for both write cache and size validation
    /// - Requirement 6.1: Single directory traversal, parallel file processing where safe
    /// - Requirement 6.2: Parse each file at most once, lazy parsing for disabled subsystems
    /// - Requirement 6.3: Memory-efficient streaming for large directories
    /// - Requirement 6.4: Track scan duration, parse duration
    /// - Requirement 6.5: Add memory usage monitoring
    /// - Requirement 7.1: Handle metadata parsing failures gracefully
    async fn scan_cache_metadata_with_metrics(
        &self,
        performance_metrics: &mut PerformanceMetrics,
    ) -> Result<ObjectsScanResults> {
        let scan_start = Instant::now();
        info!("Cache initialization: Starting coordinated metadata scan with performance tracking");

        // Create cache validator for shared utilities
        let validator = CacheValidator::new(self.cache_dir.clone());

        // Scan metadata directory for metadata files
        let metadata_dir = self.cache_dir.join("metadata");
        if !metadata_dir.exists() {
            info!("Cache initialization: Metadata directory does not exist, creating empty scan results");
            performance_metrics.scan_duration = scan_start.elapsed();
            return Ok(ObjectsScanResults::empty());
        }

        // Perform optimized parallel scan with lazy parsing and memory-efficient streaming
        let parse_start = Instant::now();
        let cache_file_results = match self
            .scan_and_process_optimized(&validator, &metadata_dir, performance_metrics)
            .await
        {
            Ok(results) => {
                debug!(
                    "Cache initialization: Successfully processed {} files",
                    results.len()
                );
                performance_metrics.files_processed = results.len() as u64;
                results
            }
            Err(e) => {
                // Graceful error recovery for directory access failures
                let error_msg = e.to_string();
                if error_msg.contains("Permission denied") {
                    error!(
                        "Cache initialization: Permission denied accessing metadata directory: {}. \
                        Check directory permissions and run with appropriate privileges.",
                        e
                    );
                } else if error_msg.contains("No such file or directory")
                    || error_msg.contains("not found")
                {
                    info!(
                        "Cache initialization: Metadata directory not found: {}. \
                        Starting with empty cache state.",
                        e
                    );
                } else {
                    warn!(
                        "Cache initialization: Directory scan failed: {}. \
                        Continuing with empty results to allow proxy startup.",
                        e
                    );
                }
                performance_metrics.scan_duration = scan_start.elapsed();
                return Ok(ObjectsScanResults::empty());
            }
        };

        performance_metrics.parse_duration = parse_start.elapsed();

        // Convert CacheFileResult to CacheMetadataEntry and collect statistics
        let mut metadata_entries = Vec::new();
        let mut scan_errors = Vec::new();
        let mut total_objects = 0u64;
        let mut total_size = 0u64;
        let mut write_cached_objects = 0u64;
        let mut write_cached_size = 0u64;
        let mut read_cached_objects = 0u64;
        let mut read_cached_size = 0u64;
        let mut parsed_files = 0u64;

        for cache_result in cache_file_results {
            // Convert to CacheMetadataEntry
            let mut entry = CacheMetadataEntry::new(cache_result.file_path.clone());
            entry.compressed_size = cache_result.compressed_size;
            entry.is_write_cached = cache_result.is_write_cached;
            entry.metadata = cache_result.metadata.clone();
            entry.parse_errors = cache_result.parse_errors.clone();

            // Extract cache key from metadata if available
            if let Some(metadata) = &cache_result.metadata {
                entry.cache_key = metadata.cache_key.clone();
                parsed_files += 1;
            }

            // Collect errors
            for error_msg in &cache_result.parse_errors {
                scan_errors.push(ScanError::new(
                    cache_result.file_path.clone(),
                    error_msg.clone(),
                    if error_msg.contains("JSON parse error") {
                        ScanErrorCategory::JsonParse
                    } else if error_msg.contains("File read error") {
                        ScanErrorCategory::FileRead
                    } else {
                        ScanErrorCategory::Other
                    },
                ));
            }

            // Update statistics only for valid entries
            if entry.is_valid() {
                total_objects += 1;
                total_size += cache_result.compressed_size;

                if cache_result.is_write_cached {
                    write_cached_objects += 1;
                    write_cached_size += cache_result.compressed_size;
                } else {
                    read_cached_objects += 1;
                    read_cached_size += cache_result.compressed_size;
                }
            }

            metadata_entries.push(entry);
        }

        let scan_duration = scan_start.elapsed();
        performance_metrics.scan_duration = scan_duration;
        performance_metrics.metadata_files_parsed = parsed_files;

        let results = ObjectsScanResults {
            total_objects,
            total_size,
            write_cached_objects,
            write_cached_size,
            read_cached_objects,
            read_cached_size,
            metadata_entries,
            scan_duration,
            scan_errors,
        };

        info!(
            "Cache initialization: Coordinated metadata scan completed in {}: {}",
            format_duration_human(scan_duration),
            results.summary_string()
        );

        // Log performance metrics for this phase
        info!(
            "Cache initialization: Scan performance - processed {} files in {}, parsing took {}, rate: {:.1} files/s",
            performance_metrics.files_processed,
            format_duration_human(performance_metrics.scan_duration),
            format_duration_human(performance_metrics.parse_duration),
            if performance_metrics.scan_duration.as_secs_f64() > 0.0 {
                performance_metrics.files_processed as f64 / performance_metrics.scan_duration.as_secs_f64()
            } else {
                0.0
            }
        );

        if results.has_errors() {
            warn!(
                "Cache initialization: Scan encountered {} errors during metadata processing",
                results.error_count()
            );
        }

        Ok(results)
    }

    /// Optimized scan and process with lazy parsing and memory-efficient streaming
    ///
    /// This method implements performance optimizations for large cache directories:
    /// - Lazy parsing: Only parse metadata needed for enabled subsystems
    /// - Memory-efficient streaming: Process files in batches to limit memory usage
    /// - Parallel processing: Use parallel processing where safe
    ///
    /// # Requirements
    /// - Requirement 6.1: Parallel file processing where safe
    /// - Requirement 6.2: Lazy parsing for disabled subsystems
    /// - Requirement 6.3: Memory-efficient streaming for large directories
    async fn scan_and_process_optimized(
        &self,
        validator: &CacheValidator,
        objects_dir: &std::path::Path,
        performance_metrics: &mut PerformanceMetrics,
    ) -> Result<Vec<crate::cache_validator::CacheFileResult>> {
        use rayon::prelude::*;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let scan_start = Instant::now();
        debug!("Starting optimized scan with lazy parsing and memory-efficient streaming");

        // First, collect all metadata file paths using memory-efficient streaming
        let metadata_files = self.scan_directory_streaming(objects_dir).await?;

        if metadata_files.is_empty() {
            debug!("No metadata files found in directory: {:?}", objects_dir);
            return Ok(Vec::new());
        }

        let total_files = metadata_files.len();
        info!(
            "Cache initialization: Found {} metadata files, processing with optimizations",
            total_files
        );

        // Determine what parsing is needed based on enabled subsystems
        let need_write_cache_parsing = self.write_cache_enabled;
        let _need_size_tracking_parsing = true; // Size tracking is always enabled

        // For lazy parsing optimization: if write cache is disabled, we can skip detailed parsing
        // of write cache specific fields and just focus on size calculation
        let lazy_parsing_mode = !need_write_cache_parsing;

        if lazy_parsing_mode {
            info!("Cache initialization: Using lazy parsing mode (write cache disabled)");
        }

        // Process files in batches for memory efficiency (Requirement 6.3)
        const BATCH_SIZE: usize = 1000; // Process 1000 files at a time to limit memory usage
        let mut all_results = Vec::with_capacity(total_files);
        let processed_count = Arc::new(AtomicU64::new(0));
        let memory_peak = Arc::new(AtomicU64::new(0));

        for (batch_index, batch) in metadata_files.chunks(BATCH_SIZE).enumerate() {
            let batch_start = Instant::now();

            // Monitor memory usage during batch processing
            let batch_memory_start = get_current_memory_usage();

            // Process batch in parallel (Requirement 6.1: parallel processing where safe)
            let batch_results: Vec<crate::cache_validator::CacheFileResult> = batch
                .par_iter()
                .map(|path| {
                    // Use optimized parsing based on subsystem requirements
                    if lazy_parsing_mode {
                        self.process_metadata_file_lazy(validator, path)
                    } else {
                        self.process_metadata_file_full(validator, path)
                    }
                })
                .collect();

            // Update processed count
            let batch_processed = batch_results.len() as u64;
            processed_count.fetch_add(batch_processed, Ordering::Relaxed);

            // Monitor peak memory usage during batch processing
            let batch_memory_end = get_current_memory_usage();
            let current_peak = memory_peak.load(Ordering::Relaxed);
            if batch_memory_end > current_peak {
                memory_peak.store(batch_memory_end, Ordering::Relaxed);
            }

            all_results.extend(batch_results);

            let batch_duration = batch_start.elapsed();
            let total_processed = processed_count.load(Ordering::Relaxed);

            debug!(
                "Cache initialization: Processed batch {}/{} ({} files) in {} - total: {}/{} ({:.1}%)",
                batch_index + 1,
                (total_files + BATCH_SIZE - 1) / BATCH_SIZE,
                batch_processed,
                format_duration_human(batch_duration),
                total_processed,
                total_files,
                (total_processed as f64 / total_files as f64) * 100.0
            );

            // Log memory usage for this batch
            debug!(
                "Cache initialization: Batch memory usage - start: {}, end: {}, peak so far: {}",
                format_bytes_human(batch_memory_start),
                format_bytes_human(batch_memory_end),
                format_bytes_human(memory_peak.load(Ordering::Relaxed))
            );

            // Optional: yield control to allow other tasks to run between batches
            // This helps prevent blocking the async runtime for too long
            if batch_index % 10 == 9 {
                // Every 10 batches
                tokio::task::yield_now().await;
            }
        }

        let total_duration = scan_start.elapsed();
        let final_processed = processed_count.load(Ordering::Relaxed);
        let final_memory_peak = memory_peak.load(Ordering::Relaxed);

        // Update performance metrics
        performance_metrics.files_processed = final_processed;
        if final_memory_peak > performance_metrics.peak_memory_usage {
            performance_metrics.peak_memory_usage = final_memory_peak;
        }

        let valid_count = all_results.iter().filter(|r| r.metadata.is_some()).count();
        let error_count = all_results
            .iter()
            .filter(|r| !r.parse_errors.is_empty())
            .count();

        info!(
            "Cache initialization: Optimized scan completed - processed {} files, {} valid, {} errors in {} \
             (rate: {:.1} files/s, peak memory: {}, lazy parsing: {})",
            final_processed,
            valid_count,
            error_count,
            format_duration_human(total_duration),
            if total_duration.as_secs_f64() > 0.0 {
                final_processed as f64 / total_duration.as_secs_f64()
            } else {
                0.0
            },
            format_bytes_human(final_memory_peak),
            if lazy_parsing_mode { "enabled" } else { "disabled" }
        );

        Ok(all_results)
    }

    /// Memory-efficient directory scanning using streaming
    ///
    /// This method scans directories using a streaming approach to minimize
    /// memory usage when dealing with large cache directories.
    ///
    /// # Requirements
    /// - Requirement 6.3: Memory-efficient streaming for large directories
    async fn scan_directory_streaming(&self, dir: &std::path::Path) -> Result<Vec<PathBuf>> {
        use walkdir::WalkDir;

        let mut metadata_files = Vec::new();
        let mut processed_entries = 0u64;
        const PROGRESS_INTERVAL: u64 = 10000; // Log progress every 10k entries

        // Use walkdir for efficient directory traversal
        for entry in WalkDir::new(dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            processed_entries += 1;

            // Log progress for very large directories
            if processed_entries % PROGRESS_INTERVAL == 0 {
                debug!(
                    "Cache initialization: Scanned {} directory entries, found {} metadata files",
                    processed_entries,
                    metadata_files.len()
                );
            }

            let path = entry.path();

            // Filter for metadata files
            if path.is_file() && path.extension().map_or(false, |ext| ext == "meta") {
                metadata_files.push(path.to_path_buf());
            }

            // Yield control periodically to prevent blocking the async runtime
            if processed_entries % 1000 == 0 {
                tokio::task::yield_now().await;
            }
        }

        debug!(
            "Cache initialization: Directory streaming scan completed - processed {} entries, found {} metadata files",
            processed_entries,
            metadata_files.len()
        );

        Ok(metadata_files)
    }

    /// Process metadata file with lazy parsing optimization
    ///
    /// This method implements lazy parsing by only extracting the minimal
    /// information needed when certain subsystems are disabled.
    ///
    /// # Requirements
    /// - Requirement 6.2: Lazy parsing for disabled subsystems
    fn process_metadata_file_lazy(
        &self,
        _validator: &CacheValidator,
        path: &PathBuf,
    ) -> crate::cache_validator::CacheFileResult {
        let mut result = crate::cache_validator::CacheFileResult {
            file_path: path.clone(),
            compressed_size: 0,
            is_write_cached: false,
            metadata: None,
            parse_errors: Vec::new(),
        };

        // In lazy parsing mode, we only need to extract size information
        // and can skip detailed metadata parsing for disabled subsystems
        match std::fs::read_to_string(path) {
            Ok(content) => {
                // Try to parse just enough to get size information
                match serde_json::from_str::<serde_json::Value>(&content) {
                    Ok(json_value) => {
                        // Extract compressed size from ranges without full parsing
                        if let Some(ranges) = json_value.get("ranges").and_then(|r| r.as_array()) {
                            let mut total_size = 0u64;
                            for range in ranges {
                                if let Some(compressed_size) =
                                    range.get("compressed_size").and_then(|s| s.as_u64())
                                {
                                    total_size += compressed_size;
                                }
                            }
                            result.compressed_size = total_size;
                        }

                        // Extract write_cached flag if available (minimal parsing)
                        if let Some(object_metadata) = json_value.get("object_metadata") {
                            if let Some(is_write_cached) = object_metadata
                                .get("is_write_cached")
                                .and_then(|w| w.as_bool())
                            {
                                result.is_write_cached = is_write_cached;
                            }
                        }

                        // In lazy mode, we still need to create a minimal metadata object
                        // for proper object counting and categorization
                        if let Some(cache_key) =
                            json_value.get("cache_key").and_then(|k| k.as_str())
                        {
                            // Create a minimal metadata object with just the essential fields
                            let minimal_metadata = NewCacheMetadata {
                                cache_key: cache_key.to_string(),
                                object_metadata: crate::cache_types::ObjectMetadata {
                                    etag: "lazy-parsed".to_string(),
                                    content_type: None,
                                    content_length: result.compressed_size,
                                    last_modified: "lazy-parsed".to_string(),
                                    is_write_cached: result.is_write_cached,
                                    ..Default::default()
                                },
                                ranges: Vec::new(), // Empty ranges in lazy mode
                                created_at: std::time::SystemTime::now(),
                                expires_at: std::time::SystemTime::now()
                                    + std::time::Duration::from_secs(3600),
                                compression_info: Default::default(),
                                ..Default::default()
                            };
                            result.metadata = Some(minimal_metadata);
                        }
                    }
                    Err(e) => {
                        result
                            .parse_errors
                            .push(format!("JSON parse error (lazy): {}", e));
                    }
                }
            }
            Err(e) => {
                result.parse_errors.push(format!("File read error: {}", e));
            }
        }

        result
    }

    /// Process metadata file with full parsing
    ///
    /// This method performs complete metadata parsing when all subsystems
    /// are enabled and need detailed information.
    fn process_metadata_file_full(
        &self,
        validator: &CacheValidator,
        path: &PathBuf,
    ) -> crate::cache_validator::CacheFileResult {
        let mut result = crate::cache_validator::CacheFileResult {
            file_path: path.clone(),
            compressed_size: 0,
            is_write_cached: false,
            metadata: None,
            parse_errors: Vec::new(),
        };

        // Full parsing for when all subsystems need detailed information
        match std::fs::read_to_string(path) {
            Ok(content) => {
                match serde_json::from_str::<NewCacheMetadata>(&content) {
                    Ok(metadata) => {
                        // Calculate compressed size from ranges
                        result.compressed_size =
                            validator.calculate_compressed_size(&metadata.ranges);
                        result.is_write_cached = metadata.object_metadata.is_write_cached;
                        result.metadata = Some(metadata);
                    }
                    Err(e) => {
                        result.parse_errors.push(format!("JSON parse error: {}", e));
                    }
                }
            }
            Err(e) => {
                result.parse_errors.push(format!("File read error: {}", e));
            }
        }

        result
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

        // Initialize WriteCacheManager if enabled with graceful error handling
        if self.write_cache_enabled {
            if let Some(manager) = write_cache_manager {
                match self
                    .initialize_write_cache_manager_safe(manager, scan_results)
                    .await
                {
                    Ok(usage) => {
                        results.write_cache_initialized = true;
                        results.write_cache_usage = usage;
                    }
                    Err(e) => {
                        error!(
                            "Write cache capacity tracking: Initialization failed: {}. \
                            Continuing without write cache capacity tracking.",
                            e
                        );
                        results.write_cache_initialized = false;
                        results.write_cache_usage = 0;
                    }
                }
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
                Ok(usage) => {
                    results.size_tracker_initialized = true;
                    results.size_tracker_usage = usage;
                }
                Err(e) => {
                    error!(
                        "Cache size validation: Initialization failed: {}. \
                        Continuing without size validation tracking.",
                        e
                    );
                    results.size_tracker_initialized = false;
                    results.size_tracker_usage = 0;
                }
            }
        } else {
            warn!("Cache size validation: Tracker not provided");
        }

        Ok(results)
    }

    /// Safely initialize WriteCacheManager with error recovery
    ///
    /// This method implements graceful error handling for write cache manager
    /// initialization, ensuring that failures don't prevent proxy startup.
    ///
    /// # Requirements
    /// - Requirement 7.3: Handle subsystem initialization failures gracefully
    /// - Requirement 7.5: Include error counts in completion summary
    async fn initialize_write_cache_manager_safe(
        &self,
        manager: &mut WriteCacheManager,
        scan_results: &ObjectsScanResults,
    ) -> Result<u64> {
        info!("Write cache capacity tracking: Initializing from scan results");
        info!(
            "Write cache capacity tracking: Found {} write-cached objects ({} total)",
            scan_results.write_cached_objects, scan_results.total_objects
        );
        info!(
            "Write cache capacity tracking: Write cache usage: {} ({:.1}% of total cache)",
            scan_results.format_write_cached_size(),
            scan_results.write_cache_percentage()
        );

        // Initialize manager with scan data using the new streamlined method
        manager.initialize_from_scan_results(
            scan_results.write_cached_size,
            scan_results.write_cached_objects,
        );

        let current_usage = manager.current_usage();
        let max_capacity = manager.max_size();
        let usage_percent = if max_capacity > 0 {
            (current_usage as f64 / max_capacity as f64) * 100.0
        } else {
            0.0
        };

        info!(
            "Write cache capacity tracking: Initialized with {} / {} ({:.1}% capacity)",
            format_bytes_human(current_usage),
            format_bytes_human(max_capacity),
            usage_percent
        );

        Ok(current_usage)
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
            .open(lock_path)
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to open initialization lock file: {}", e))
            })?;

        // Try to acquire exclusive lock (non-blocking)
        lock_file.try_lock_exclusive().map_err(|e| {
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
    pub async fn ensure_cache_state_consistency_post_init(
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

    /// Ensure consistent cache state across instances
    ///
    /// This method implements cache state consistency checks to ensure all instances
    /// have a consistent view of the cache after initialization.
    ///
    /// # Requirements
    /// - Requirement 10.2: Add coordination for shared cache validation
    /// - Requirement 10.3: Ensure consistent view after initialization
    pub async fn ensure_cache_state_consistency(
        &self,
        write_cache_manager: Option<&WriteCacheManager>,
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

        // Verify subsystem consistency if both are available
        if write_cache_manager.is_some() && size_tracker.is_some() {
            match self
                .cross_validate_subsystems(write_cache_manager, size_tracker)
                .await
            {
                Ok(validation_results) => {
                    if validation_results.was_performed() {
                        consistency_info.subsystem_consistency_status =
                            validation_results.status_string();
                        consistency_info.subsystem_consistency_message =
                            Some(validation_results.summary_string());

                        if validation_results.has_serious_discrepancy() {
                            consistency_info.warnings += 1;
                        } else if validation_results.has_minor_discrepancy() {
                            consistency_info.warnings += 1;
                        }
                    }
                }
                Err(e) => {
                    consistency_info.warnings += 1;
                    consistency_info.subsystem_consistency_status = "FAILED".to_string();
                    consistency_info.subsystem_consistency_message =
                        Some(format!("Cross-validation failed: {}", e));
                }
            }
        }

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

        info!(
            "Cache initialization: Cross-validation - WriteCacheManager: {}, CacheSizeTracker: {}",
            format_bytes_human(manager_size),
            format_bytes_human(tracker_size)
        );

        // Calculate discrepancy percentage
        let size_discrepancy_percent = if manager_size == 0 && tracker_size == 0 {
            0.0
        } else {
            let max_size = manager_size.max(tracker_size) as f64;
            let diff = (manager_size as i64 - tracker_size as i64).abs() as f64;
            (diff / max_size) * 100.0
        };

        let mut validation_warnings = Vec::new();
        let mut write_cache_consistent = true;

        // Apply threshold-based logging and validation
        if size_discrepancy_percent > 20.0 {
            // Error threshold exceeded
            let error_msg = format!(
                "Write cache size discrepancy exceeds error threshold (20%): {:.1}% difference. \
                WriteCacheManager: {}, CacheSizeTracker: {}. Consider running cache validation.",
                size_discrepancy_percent,
                format_bytes_human(manager_size),
                format_bytes_human(tracker_size)
            );
            error!("Cache initialization: {}", error_msg);
            validation_warnings.push(error_msg);
            write_cache_consistent = false;
        } else if size_discrepancy_percent > 5.0 {
            // Warning threshold exceeded
            let warning_msg = format!(
                "Write cache size discrepancy exceeds warning threshold (5%): {:.1}% difference. \
                WriteCacheManager: {}, CacheSizeTracker: {}",
                size_discrepancy_percent,
                format_bytes_human(manager_size),
                format_bytes_human(tracker_size)
            );
            warn!("Cache initialization: {}", warning_msg);
            validation_warnings.push(warning_msg);
            write_cache_consistent = false;
        } else {
            // Validation passed
            debug!(
                "Cache initialization: Cross-validation successful - write cache sizes consistent \
                ({:.1}% difference within acceptable range)",
                size_discrepancy_percent
            );
        }

        let results = ValidationResults {
            write_cache_consistent,
            size_discrepancy_percent,
            validation_warnings,
            write_cache_manager_size: Some(manager_size),
            size_tracker_write_cache_size: Some(tracker_size),
        };

        info!(
            "Cache initialization: Cross-validation completed - {}",
            results.summary_string()
        );

        Ok(results)
    }
}

/// Summary of cache initialization process
#[derive(Debug, Clone)]
pub struct InitializationSummary {
    /// Total initialization duration
    pub total_duration: Duration,
    /// Results from each phase
    pub phase_results: Vec<PhaseResult>,
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

/// Result from a single initialization phase
#[derive(Debug, Clone)]
pub struct PhaseResult {
    /// Name of the phase
    pub phase_name: String,
    /// Duration of the phase
    pub duration: Duration,
    /// Whether the phase succeeded
    pub success: bool,
    /// Error message if phase failed
    pub error_message: Option<String>,
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
    pub fn calculate_derived_metrics(&mut self) {
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
    pub fn memory_usage_delta(&self) -> i64 {
        self.final_memory_usage as i64 - self.initial_memory_usage as i64
    }

    /// Format performance summary for logging
    pub fn format_summary(&self) -> String {
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
    pub fn detailed_report(&self) -> String {
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
        if self.total_objects == 0 {
            0
        } else {
            self.total_size / self.total_objects
        }
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
    /// Write cache usage in bytes
    pub write_cache_usage: u64,
    /// Whether size tracker was initialized
    pub size_tracker_initialized: bool,
    /// Size tracker usage in bytes
    pub size_tracker_usage: u64,
}

impl SubsystemResults {
    /// Create empty subsystem results
    pub fn empty() -> Self {
        Self {
            write_cache_initialized: false,
            write_cache_usage: 0,
            size_tracker_initialized: false,
            size_tracker_usage: 0,
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
    pub fn not_performed(reason: String) -> Self {
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

    /// Get detailed discrepancy report
    pub fn detailed_discrepancy_report(&self) -> String {
        if !self.was_performed() {
            return "Cross-validation was not performed - detailed report unavailable".to_string();
        }

        let manager_size = self.write_cache_manager_size.unwrap_or(0);
        let tracker_size = self.size_tracker_write_cache_size.unwrap_or(0);
        let absolute_diff = (manager_size as i64 - tracker_size as i64).abs() as u64;

        let mut report = String::new();
        report.push_str("=== Cross-Validation Detailed Report ===\n");
        report.push_str(&format!(
            "WriteCacheManager size: {} ({} bytes)\n",
            format_bytes_human(manager_size),
            manager_size
        ));
        report.push_str(&format!(
            "CacheSizeTracker write cache size: {} ({} bytes)\n",
            format_bytes_human(tracker_size),
            tracker_size
        ));
        report.push_str(&format!(
            "Absolute difference: {} ({} bytes)\n",
            format_bytes_human(absolute_diff),
            absolute_diff
        ));
        report.push_str(&format!(
            "Percentage difference: {:.2}%\n",
            self.size_discrepancy_percent
        ));
        report.push_str(&format!(
            "Consistency status: {}\n",
            if self.write_cache_consistent {
                "CONSISTENT"
            } else {
                "INCONSISTENT"
            }
        ));

        if !self.validation_warnings.is_empty() {
            report.push_str("Warnings:\n");
            for (i, warning) in self.validation_warnings.iter().enumerate() {
                report.push_str(&format!("  {}. {}\n", i + 1, warning));
            }
        }

        report.push_str("========================================");
        report
    }

    /// Get validation status as a simple string
    pub fn status_string(&self) -> String {
        if !self.was_performed() {
            "NOT_PERFORMED".to_string()
        } else if self.write_cache_consistent {
            "CONSISTENT".to_string()
        } else if self.size_discrepancy_percent > 20.0 {
            "ERROR_THRESHOLD_EXCEEDED".to_string()
        } else if self.size_discrepancy_percent > 5.0 {
            "WARNING_THRESHOLD_EXCEEDED".to_string()
        } else {
            "INCONSISTENT".to_string()
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

    /// Get recommendation based on validation results
    pub fn get_recommendation(&self) -> Option<String> {
        if !self.was_performed() {
            return None;
        }

        if self.has_serious_discrepancy() {
            Some(
                "Consider running cache validation to identify and resolve inconsistencies. \
                  Large discrepancies may indicate cache corruption or synchronization issues."
                    .to_string(),
            )
        } else if self.has_minor_discrepancy() {
            Some(
                "Monitor cache consistency. Minor discrepancies may resolve automatically \
                  during normal operation, but persistent issues should be investigated."
                    .to_string(),
            )
        } else {
            None
        }
    }
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
        let _ = self.file.unlock();
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
    pub fn overall_status(&self) -> String {
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

/// Information about consistency validation results
#[derive(Debug, Clone)]
pub struct ConsistencyValidationInfo {
    /// Number of metadata entries validated
    pub validated_count: usize,
    /// Number of corrupted metadata entries found
    pub corruption_count: usize,
    /// List of validation errors
    pub errors: Vec<String>,
    /// Duration of validation process
    pub validation_duration: std::time::Duration,
}

impl ConsistencyValidationInfo {
    /// Get summary string for logging
    pub fn summary_string(&self) -> String {
        format!(
            "validated {} entries, found {} corrupted, {} errors in {}",
            self.validated_count,
            self.corruption_count,
            self.errors.len(),
            format_duration_human(self.validation_duration)
        )
    }
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
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    fn create_test_cache_config() -> CacheConfig {
        CacheConfig {
            cache_dir: PathBuf::from("/tmp/test"),
            max_cache_size: 1024 * 1024 * 1024, // 1GB
            ram_cache_enabled: false,
            max_ram_cache_size: 0,
            eviction_algorithm: EvictionAlgorithm::LRU,
            write_cache_enabled: true,
            write_cache_percent: 0.5,
            write_cache_max_object_size: 1024 * 1024 * 100, // 100MB
            put_ttl: Duration::from_secs(86400),            // 1 day
            get_ttl: Duration::from_secs(3600),             // 1 hour
            head_ttl: Duration::from_secs(300),             // 5 minutes
            actively_remove_cached_data: false,
            shared_storage: SharedStorageConfig::default(),
            download_coordination: crate::config::DownloadCoordinationConfig::default(),
            range_merge_gap_threshold: 1024 * 1024, // 1MB
            eviction_buffer_percent: 5,
            ram_cache_flush_interval: Duration::from_secs(60),
            ram_cache_flush_threshold: 100,
            ram_cache_flush_on_eviction: false,
            ram_cache_verification_interval: Duration::from_secs(1),
            incomplete_upload_ttl: Duration::from_secs(86400), // 1 day
            initialization: InitializationConfig::default(),
            cache_bypass_headers_enabled: true, // Default enabled
            metadata_cache: crate::config::MetadataCacheConfig::default(),
            eviction_trigger_percent: 95, // Trigger eviction at 95% capacity
            eviction_target_percent: 80,  // Reduce to 80% after eviction
            full_object_check_threshold: 67_108_864, // 64 MiB
            disk_streaming_threshold: 1_048_576, // 1 MiB
            read_cache_enabled: true,
            bucket_settings_staleness_threshold: Duration::from_secs(60),
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
        assert!(coordinator.is_write_cache_enabled());
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
    async fn test_scan_cache_metadata_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        let coordinator =
            CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

        let mut performance_metrics = PerformanceMetrics::new();
        let results = coordinator
            .scan_cache_metadata_with_metrics(&mut performance_metrics)
            .await
            .unwrap();

        assert_eq!(results.total_objects, 0);
        assert_eq!(results.total_size, 0);
        assert_eq!(results.write_cached_objects, 0);
        assert_eq!(results.read_cached_objects, 0);
        assert!(results.metadata_entries.is_empty());
        assert!(!results.has_errors());
    }

    #[tokio::test]
    async fn test_scan_cache_metadata_with_valid_files() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        // Create metadata directory
        let metadata_dir = temp_dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        // Create a valid metadata file
        let metadata = NewCacheMetadata {
            cache_key: "test-bucket/test-object".to_string(),
            object_metadata: crate::cache_types::ObjectMetadata {
                etag: "test-etag".to_string(),
                content_type: Some("text/plain".to_string()),
                content_length: 1024,
                last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
                is_write_cached: true,
                ..Default::default()
            },
            ranges: vec![crate::cache_types::RangeSpec {
                start: 0,
                end: 1023,
                file_path: "test.bin".to_string(),
                compression_algorithm: crate::compression::CompressionAlgorithm::Lz4,
                compressed_size: 512,
                uncompressed_size: 1024,
                created_at: SystemTime::now(),
                last_accessed: SystemTime::now(),
                access_count: 1,
                frequency_score: 1,
            }],
            created_at: SystemTime::now(),
            expires_at: SystemTime::now() + Duration::from_secs(3600),
            compression_info: Default::default(),
            ..Default::default()
        };

        let metadata_file = metadata_dir.join("test.meta");
        let metadata_json = serde_json::to_string(&metadata).unwrap();
        std::fs::write(&metadata_file, metadata_json).unwrap();

        let coordinator =
            CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

        let mut performance_metrics = PerformanceMetrics::new();
        let results = coordinator
            .scan_cache_metadata_with_metrics(&mut performance_metrics)
            .await
            .unwrap();

        assert_eq!(results.total_objects, 1);
        assert_eq!(results.total_size, 512);
        assert_eq!(results.write_cached_objects, 1);
        assert_eq!(results.write_cached_size, 512);
        assert_eq!(results.read_cached_objects, 0);
        assert_eq!(results.read_cached_size, 0);
        assert_eq!(results.metadata_entries.len(), 1);
        assert!(!results.has_errors());

        let entry = &results.metadata_entries[0];
        assert_eq!(entry.cache_key, "test-bucket/test-object");
        assert_eq!(entry.compressed_size, 512);
        assert!(entry.is_write_cached);
        assert!(entry.is_valid());
    }

    #[tokio::test]
    async fn test_scan_cache_metadata_with_invalid_files() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_cache_config();

        // Create metadata directory
        let metadata_dir = temp_dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).unwrap();

        // Create an invalid metadata file
        let invalid_file = metadata_dir.join("invalid.meta");
        std::fs::write(&invalid_file, "invalid json content").unwrap();

        let coordinator =
            CacheInitializationCoordinator::new(temp_dir.path().to_path_buf(), true, config);

        let mut performance_metrics = PerformanceMetrics::new();
        let results = coordinator
            .scan_cache_metadata_with_metrics(&mut performance_metrics)
            .await
            .unwrap();

        assert_eq!(results.total_objects, 0); // Invalid files don't count as objects
        assert_eq!(results.total_size, 0);
        assert_eq!(results.metadata_entries.len(), 1); // But they are still included in entries
        assert!(results.has_errors());
        assert_eq!(results.error_count(), 1);

        let entry = &results.metadata_entries[0];
        assert!(!entry.is_valid());
        assert!(entry.has_errors());
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
            CacheEvictionAlgorithm::LRU,
            config.write_cache_max_object_size,
        );
        // Initialize write cache manager with 512 bytes to match consolidator
        write_cache_manager.initialize_from_scan_results(512, 1);

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
            .update_size_from_validation(512, Some(512))
            .await;

        // Create size tracker with consolidator reference
        let size_config = CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: true,
            incomplete_upload_ttl: Duration::from_secs(86400),
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
        assert_eq!(results.write_cache_manager_size, Some(512));
        assert_eq!(results.size_tracker_write_cache_size, Some(512));
    }

    #[tokio::test]
    async fn test_cross_validate_subsystems_warning_threshold() {
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

        // Create write cache manager with 1000 bytes
        let write_cache_manager = WriteCacheManager::new(
            cache_dir.clone(),
            1024 * 1024, // 1MB total cache
            50.0,        // 50% for write cache
            config.put_ttl,
            config.incomplete_upload_ttl,
            CacheEvictionAlgorithm::LRU,
            config.write_cache_max_object_size,
        );
        // Initialize write cache manager with 1000 bytes
        write_cache_manager.initialize_from_scan_results(1000, 1);

        // Create consolidator and set write cache size to 900 (10% difference)
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
        consolidator
            .update_size_from_validation(900, Some(900))
            .await;

        // Create size tracker with consolidator reference
        let size_config = CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: true,
            incomplete_upload_ttl: Duration::from_secs(86400),
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
        assert!(!results.write_cache_consistent); // Should be inconsistent due to warning threshold
        assert_eq!(results.size_discrepancy_percent, 10.0);
        assert_eq!(results.validation_warnings.len(), 1);
        assert!(results.validation_warnings[0].contains("warning threshold"));
        assert_eq!(results.write_cache_manager_size, Some(1000));
        assert_eq!(results.size_tracker_write_cache_size, Some(900));
    }

    #[tokio::test]
    async fn test_cross_validate_subsystems_error_threshold() {
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

        // Create write cache manager with 1000 bytes
        let write_cache_manager = WriteCacheManager::new(
            cache_dir.clone(),
            1024 * 1024, // 1MB total cache
            50.0,        // 50% for write cache
            config.put_ttl,
            config.incomplete_upload_ttl,
            CacheEvictionAlgorithm::LRU,
            config.write_cache_max_object_size,
        );
        // Initialize write cache manager with 1000 bytes
        write_cache_manager.initialize_from_scan_results(1000, 1);

        // Create consolidator and set write cache size to 750 (25% difference)
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
        consolidator
            .update_size_from_validation(750, Some(750))
            .await;

        // Create size tracker with consolidator reference
        let size_config = CacheSizeConfig {
            checkpoint_interval: Duration::from_secs(300),
            validation_time_of_day: "00:00".to_string(),
            validation_enabled: true,
            incomplete_upload_ttl: Duration::from_secs(86400),
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
        assert!(!results.write_cache_consistent); // Should be inconsistent due to error threshold
        assert_eq!(results.size_discrepancy_percent, 25.0);
        assert_eq!(results.validation_warnings.len(), 1);
        assert!(results.validation_warnings[0].contains("error threshold"));
        assert!(results.validation_warnings[0].contains("Consider running cache validation"));
        assert_eq!(results.write_cache_manager_size, Some(1000));
        assert_eq!(results.size_tracker_write_cache_size, Some(750));
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
            CacheEvictionAlgorithm::LRU,
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
            CacheEvictionAlgorithm::LRU,
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
    fn test_validation_results_detailed_reporting() {
        // Test detailed discrepancy report
        let mut results = ValidationResults::empty();
        results.write_cache_manager_size = Some(1000);
        results.size_tracker_write_cache_size = Some(750);
        results.size_discrepancy_percent = 25.0;
        results.write_cache_consistent = false;
        results.validation_warnings.push("Test warning".to_string());

        let detailed_report = results.detailed_discrepancy_report();
        assert!(detailed_report.contains("Cross-Validation Detailed Report"));
        assert!(detailed_report.contains("WriteCacheManager size: 1000 B"));
        assert!(detailed_report.contains("CacheSizeTracker write cache size: 750 B"));
        assert!(detailed_report.contains("Absolute difference: 250 B"));
        assert!(detailed_report.contains("Percentage difference: 25.00%"));
        assert!(detailed_report.contains("Consistency status: INCONSISTENT"));
        assert!(detailed_report.contains("Test warning"));

        // Test status string
        assert_eq!(results.status_string(), "ERROR_THRESHOLD_EXCEEDED");
        assert!(results.has_serious_discrepancy());
        assert!(!results.has_minor_discrepancy());

        // Test recommendation
        let recommendation = results.get_recommendation().unwrap();
        assert!(recommendation.contains("Consider running cache validation"));
        assert!(recommendation.contains("cache corruption"));
    }

    #[test]
    fn test_validation_results_warning_threshold() {
        let mut results = ValidationResults::empty();
        results.write_cache_manager_size = Some(1000);
        results.size_tracker_write_cache_size = Some(900);
        results.size_discrepancy_percent = 10.0;
        results.write_cache_consistent = false;

        assert_eq!(results.status_string(), "WARNING_THRESHOLD_EXCEEDED");
        assert!(!results.has_serious_discrepancy());
        assert!(results.has_minor_discrepancy());

        let recommendation = results.get_recommendation().unwrap();
        assert!(recommendation.contains("Monitor cache consistency"));
        assert!(recommendation.contains("Minor discrepancies"));
    }

    #[test]
    fn test_validation_results_consistent() {
        let mut results = ValidationResults::empty();
        results.write_cache_manager_size = Some(1000);
        results.size_tracker_write_cache_size = Some(1000);
        results.size_discrepancy_percent = 0.0;
        results.write_cache_consistent = true;

        assert_eq!(results.status_string(), "CONSISTENT");
        assert!(!results.has_serious_discrepancy());
        assert!(!results.has_minor_discrepancy());
        assert!(results.get_recommendation().is_none());

        let summary = results.summary_string();
        assert!(summary.contains("Write cache sizes consistent"));
    }

    #[test]
    fn test_validation_results_not_performed() {
        let results = ValidationResults::not_performed("test reason".to_string());

        assert_eq!(results.status_string(), "NOT_PERFORMED");
        assert!(!results.has_serious_discrepancy());
        assert!(!results.has_minor_discrepancy());
        assert!(results.get_recommendation().is_none());

        let detailed_report = results.detailed_discrepancy_report();
        assert!(detailed_report.contains("Cross-validation was not performed"));
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
    async fn test_ensure_cache_state_consistency() {
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
            .ensure_cache_state_consistency(
                None, // No write cache manager
                Some(&size_tracker),
            )
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
