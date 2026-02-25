# Error Handling and Recovery

This document describes the comprehensive error handling and recovery mechanisms implemented in the S3 proxy cache system, specifically for the new range storage architecture.

## Table of Contents
- [Overview](#overview)
- [Error Categories](#error-categories)
  - [Corrupted Metadata Files](#1-corrupted-metadata-files-requirement-81)
  - [Missing Range Binary Files](#2-missing-range-binary-files-requirement-82)
  - [Disk Space Exhaustion](#3-disk-space-exhaustion-requirement-83)
  - [Inconsistent Metadata](#4-inconsistent-metadata-requirement-84)
  - [Partial Write Failures](#5-partial-write-failures-requirement-84)
  - [Orphaned Files](#6-orphaned-files-requirement-75)
- [Error Metrics](#error-metrics)
- [Maintenance Operations](#maintenance-operations)
- [Error Handling Best Practices](#error-handling-best-practices)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Testing Error Handling](#testing-error-handling)
- [Troubleshooting](#troubleshooting)
- [Future Enhancements](#future-enhancements)

## Overview

The cache system implements robust error handling to ensure that cache corruption, missing files, disk space issues, and other errors don't cause proxy failures. The system follows a "fail gracefully" approach where errors are logged, metrics are recorded, and the system continues serving requests.

### Design Principles

1. **Graceful Degradation**: When cache errors occur, the system falls back to direct S3 access without interruption
2. **Self-Healing**: Many errors are automatically detected and repaired without manual intervention
3. **Transparency**: End users experience no disruption during cache errors
4. **Observability**: All errors are logged and metrics are recorded for monitoring
5. **Resilience**: The system continues operating even when parts of the cache are corrupted or unavailable

### Error Recovery Flow

When an error occurs in the cache system, the following flow is typically followed:

1. **Detection**: Error is detected during cache operation
2. **Logging**: Error is logged with detailed context
3. **Metric Recording**: Error is recorded in metrics for monitoring
4. **Recovery Attempt**: System attempts automatic recovery if possible
5. **Fallback**: Request is served from S3 if cache is unavailable
6. **Cleanup**: Orphaned or corrupted files are cleaned up in background

## Error Categories

The S3 proxy cache system handles several categories of errors, each with specific detection and recovery strategies.

### 1. Corrupted Metadata Files (Requirement 8.1)

**Detection**: JSON parse failure when reading metadata files

**Recovery Strategy**:
- Log error with details including file path and error message
- Treat as cache miss (return None)
- Delete corrupted metadata file to prevent future issues
- Record metric: `corruption_metadata_total`

**Example**:
```rust
// When metadata file is corrupted
let result = cache_manager.get_metadata(cache_key).await?;
// Returns Ok(None) and deletes corrupted file
```

**Behavior**:
- System continues operating normally
- Request is served from S3 (cache miss)
- Corrupted file is automatically cleaned up
- No manual intervention required

**Prevention Tips**:
- Ensure proper shutdown procedures to prevent incomplete writes
- Monitor disk health to prevent I/O errors
- Regular backups of critical cache metadata (for disaster recovery)

### 2. Missing Range Binary Files (Requirement 8.2)

**Detection**: File not found when loading range data

**Recovery Strategy**:
- Log error with range details (start, end, cache key)
- Return error to caller (cache miss)
- Caller fetches range from S3
- Caller attempts to recache the range
- Remove missing range reference from metadata
- Record metric: `corruption_missing_range_total`

**Example**:
```rust
// When range file is missing
let result = cache_manager.load_range_data(&range_spec).await;
// Returns Err(ProxyError::CacheError("Range file not found..."))
// Caller handles by fetching from S3
```

**Behavior**:
- Cache miss is transparent to end user
- Range is fetched from S3 and served
- System attempts to recache for future requests
- Other ranges in the same object remain accessible
- Metadata is automatically updated to remove reference to missing range

**Common Causes**:
- Manual deletion of cache files
- Disk corruption or I/O errors
- Improper shutdown during write operations
- File system issues

**Prevention Tips**:
- Avoid manual manipulation of cache files
- Use proper shutdown procedures
- Monitor disk health and I/O errors
- Implement regular cache validation

### 3. Disk Space Exhaustion (Requirement 8.3)

**Detection**: Write operation fails with "No space left on device"

**Recovery Strategy**:
- Log error with severity warning including available space
- Return error to caller
- Trigger emergency cache eviction (if configured)
- Retry write operation after eviction
- Record metric: `disk_full_events_total`

**Example**:
```rust
// When disk is full
let result = cache_manager.store_range(cache_key, start, end, data, metadata).await;
// Returns Err(ProxyError::CacheError("Disk space exhausted..."))
```

**Behavior**:
- Write operation fails gracefully
- No partial writes or corrupted files
- Cache eviction frees up space
- System continues serving from S3
- Caching resumes when space is available

**Prevention and Mitigation**:
- Monitor disk space usage regularly
- Configure appropriate cache size limits
- Implement alerts for low disk space
- Use cache eviction policies effectively
- Consider expanding storage capacity proactively

**Best Practices**:
- Set cache size limits to 80-90% of available disk space
- Monitor `disk_full_events_total` metric for trends
- Implement automated scaling for cloud deployments
- Use separate disks/partitions for cache and logs

### 4. Inconsistent Metadata (Requirement 8.4)

**Detection**: Metadata references range files that don't exist

**Recovery Strategy**:
- Detect during metadata verification
- Remove missing range specs from metadata
- Update metadata atomically
- Continue serving other valid ranges
- Delete entire entry if no valid ranges remain
- Record metric: `inconsistency_fixed_total`

**Example**:
```rust
// Verify and fix inconsistent metadata
let fixed_count = cache_manager.verify_and_fix_metadata(cache_key).await?;
// Returns number of inconsistencies fixed
```

**Behavior**:
- System automatically repairs inconsistencies
- Valid ranges remain accessible
- Missing ranges are treated as cache misses
- No data loss for valid ranges
- Metadata stays consistent with actual files

**Common Scenarios**:
- Crash during range deletion
- Manual file manipulation
- Partial write failures
- Disk corruption affecting specific files

**Validation Process**:
1. Read metadata file
2. Check existence of all referenced range files
3. Identify missing ranges
4. Remove references to missing ranges
5. Update metadata atomically
6. Log and record metrics

**Prevention Tips**:
- Use atomic operations for metadata updates
- Implement proper shutdown procedures
- Avoid manual cache file manipulation
- Regular cache validation and cleanup

### 5. Partial Write Failures (Requirement 8.4)

**Detection**: `.tmp` files exist without corresponding final files

**Recovery Strategy**:
- Detect during cleanup operations
- Delete orphaned `.tmp` files
- Prevent accumulation of temporary files
- Record metric: `partial_write_cleanup_total`

**Example**:
```rust
// Clean up temporary files
cache_manager.cleanup_temp_files(cache_key).await?;
// Removes all .tmp files for the cache key
```

**Behavior**:
- Temporary files are automatically cleaned up
- No disk space wasted on partial writes
- System maintains clean cache directory
- Cleanup runs during maintenance operations

**When Partial Writes Occur**:
- System crash during file write
- Disk space exhaustion during write
- I/O errors during write operations
- Process termination during write

**Prevention Strategies**:
- Use write-ahead logging for critical operations
- Implement proper error handling for write operations
- Monitor system stability and uptime
- Use UPS or other power protection for critical systems

**Recovery Guarantees**:
- No data corruption from partial writes
- Automatic cleanup of temporary files
- No impact on valid cache entries
- System continues operating normally

### 6. Orphaned Files (Requirement 7.5)

**Detection**: Range files exist without corresponding metadata

**Recovery Strategy**:
- Detect during comprehensive cleanup
- Delete orphaned range files
- Prevent disk space waste
- Record metric: `orphaned_files_cleaned_total`

**Example**:
```rust
// Perform comprehensive cleanup
let stats = cache_manager.perform_cache_cleanup().await?;
// Returns CacheCleanupStats with counts
```

**Behavior**:
- Orphaned files are automatically detected
- Files are safely deleted
- Disk space is reclaimed
- No impact on valid cache entries

**Common Causes of Orphaned Files**:
- Crash during metadata deletion
- Manual deletion of metadata files
- Disk corruption affecting metadata but not range files
- Bugs in cache cleanup logic

**Detection Process**:
1. Scan all range files in cache directory
2. Extract cache keys from filenames
3. Verify corresponding metadata exists
4. Identify files without metadata
5. Safely delete orphaned files
6. Record metrics and log results

**Prevention Measures**:
- Atomic operations for file creation and deletion
- Proper shutdown procedures
- Regular cache validation
- Monitoring of orphaned file metrics

## Error Metrics

All error events are tracked via metrics for monitoring and alerting:

```rust
pub struct CacheMetrics {
    // ... other metrics ...
    pub corruption_metadata_total: u64,
    pub corruption_missing_range_total: u64,
    pub inconsistency_fixed_total: u64,
    pub partial_write_cleanup_total: u64,
    pub disk_full_events_total: u64,
    pub orphaned_files_cleaned_total: u64,
}
```

### Recording Metrics

Metrics are recorded through the MetricsManager:

```rust
// Record corrupted metadata
metrics_manager.record_corrupted_metadata().await;

// Record missing range file
metrics_manager.record_missing_range_file().await;

// Record inconsistency fixed
metrics_manager.record_inconsistency_fixed().await;

// Record partial write cleanup
metrics_manager.record_partial_write_cleanup().await;

// Record disk full event
metrics_manager.record_disk_full_event().await;

// Record orphaned files cleaned
metrics_manager.record_orphaned_files_cleaned(count).await;
```

### Accessing Metrics

Metrics are available via the `/metrics` endpoint:

```bash
curl http://localhost:9090/metrics
```

Example response:
```json
{
  "cache": {
    "corruption_metadata_total": 5,
    "corruption_missing_range_total": 12,
    "inconsistency_fixed_total": 8,
    "partial_write_cleanup_total": 3,
    "disk_full_events_total": 1,
    "orphaned_files_cleaned_total": 15
  }
}
```

## Maintenance Operations

### Comprehensive Cache Cleanup

The system provides a comprehensive cleanup operation that:
1. Cleans up orphaned range files
2. Cleans up temporary files
3. Verifies and fixes inconsistent metadata
4. Returns statistics about cleanup operations

```rust
let stats = cache_manager.perform_cache_cleanup().await?;
println!("Cleaned up {} temp files", stats.temp_files_cleaned);
println!("Cleaned up {} orphaned files", stats.orphaned_files_cleaned);
println!("Fixed {} inconsistencies", stats.inconsistencies_fixed);
```

### Scheduled Cleanup

It's recommended to run comprehensive cleanup periodically:

```rust
// Run cleanup every hour
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(3600));
    loop {
        interval.tick().await;
        if let Err(e) = cache_manager.perform_cache_cleanup().await {
            error!("Cache cleanup failed: {}", e);
        }
    }
});
```

## Error Handling Best Practices

### 1. Always Check Return Values

```rust
// Good
match cache_manager.get_metadata(cache_key).await {
    Ok(Some(metadata)) => {
        // Use metadata
    }
    Ok(None) => {
        // Cache miss - fetch from S3
    }
    Err(e) => {
        // Handle error
        error!("Failed to get metadata: {}", e);
    }
}

// Bad
let metadata = cache_manager.get_metadata(cache_key).await.unwrap();
```

### 2. Handle Cache Misses Gracefully

```rust
// Try to load from cache
let data = match cache_manager.load_range_data(&range_spec).await {
    Ok(data) => data,
    Err(e) => {
        // Cache miss - fetch from S3
        warn!("Cache miss for range {}-{}: {}", range_spec.start, range_spec.end, e);
        fetch_from_s3(cache_key, range_spec.start, range_spec.end).await?
    }
};
```

### 3. Log Errors with Context

```rust
// Good
error!(
    "Failed to store range {}-{} for key {}: {}",
    start, end, cache_key, e
);

// Bad
error!("Store failed: {}", e);
```

### 4. Record Metrics for Monitoring

```rust
// Always record metrics for error events
if let Err(e) = cache_manager.get_metadata(cache_key).await {
    metrics_manager.record_corrupted_metadata().await;
    error!("Corrupted metadata for key {}: {}", cache_key, e);
}
```

### 5. Clean Up on Failure

```rust
// Always clean up temporary files on failure
if let Err(e) = cache_manager.store_range(cache_key, start, end, data, metadata).await {
    // Cleanup is automatic in store_range, but for custom operations:
    let _ = cache_manager.cleanup_temp_files(cache_key).await;
    return Err(e);
}
```

## Monitoring and Alerting

### Recommended Alerts

1. **High Corruption Rate**
   - Alert when `corruption_metadata_total` increases rapidly
   - May indicate disk issues or software bugs

2. **Frequent Disk Full Events**
   - Alert when `disk_full_events_total` increases
   - May need to increase cache size limits or disk space

3. **Many Inconsistencies**
   - Alert when `inconsistency_fixed_total` is high
   - May indicate crashes or improper shutdowns

4. **Orphaned Files Accumulating**
   - Alert when `orphaned_files_cleaned_total` grows without cleanup
   - May need to run cleanup more frequently

### Monitoring Dashboard

Example Prometheus queries:

```promql
# Corruption rate (per minute)
rate(cache_corruption_metadata_total[1m])

# Disk full events (last hour)
increase(cache_disk_full_events_total[1h])

# Inconsistencies fixed (last day)
increase(cache_inconsistency_fixed_total[1d])

# Orphaned files cleaned (last hour)
increase(cache_orphaned_files_cleaned_total[1h])
```

## Testing Error Handling

The error handling system is thoroughly tested in `tests/error_handling_test.rs`:

```bash
# Run error handling tests
cargo test --test error_handling_test

# Run specific test
cargo test --test error_handling_test test_corrupted_metadata_handling
```

### Test Coverage

- Corrupted metadata handling
- Empty metadata files
- Missing range binary files
- Disk space exhaustion
- Inconsistent metadata (partial)
- Inconsistent metadata (all ranges missing)
- Temporary file cleanup
- Comprehensive cache cleanup
- Valid entries preservation
- Error recovery without crashes
- Partial write cleanup

## Troubleshooting

### Problem: High corruption rate

**Symptoms**: `corruption_metadata_total` increasing rapidly

**Possible Causes**:
- Disk errors or failing hardware
- Improper shutdowns or crashes
- Software bugs in serialization

**Solutions**:
1. Check disk health: `smartctl -a /dev/sdX`
2. Review system logs for crashes
3. Run comprehensive cleanup
4. Consider clearing cache and starting fresh

### Problem: Disk full events

**Symptoms**: `disk_full_events_total` increasing, cache writes failing

**Possible Causes**:
- Cache size limits too high for available disk space
- Other processes consuming disk space
- Eviction not working properly

**Solutions**:
1. Check disk usage: `df -h`
2. Reduce cache size limits in config
3. Verify eviction is enabled
4. Clear old cache entries manually if needed

### Problem: Many inconsistencies

**Symptoms**: `inconsistency_fixed_total` high, frequent metadata repairs

**Possible Causes**:
- Improper shutdowns
- Manual file deletions
- Bugs in deletion logic

**Solutions**:
1. Ensure graceful shutdowns
2. Don't manually delete cache files
3. Run comprehensive cleanup regularly
4. Review deletion logic for bugs

### Problem: Orphaned files accumulating

**Symptoms**: `orphaned_files_cleaned_total` growing, disk space wasted

**Possible Causes**:
- Cleanup not running frequently enough
- Bugs in deletion logic
- Crashes during deletion

**Solutions**:
1. Increase cleanup frequency
2. Run manual cleanup: `cache_manager.perform_cache_cleanup().await`
3. Review deletion logic
4. Check for crashes during cache operations

## Future Enhancements

Potential improvements to error handling:

1. **Advanced Monitoring**
   - Error rate trending and prediction
   - Centralized error reporting with monitoring system integration

2. **Proactive Cleanup**
   - Background cleanup based on error rates
   - Predictive maintenance

## Error Prevention Strategies

While the S3 proxy has robust error handling, preventing errors is always preferable to recovering from them. Here are key strategies to minimize cache errors:

### 1. Proper Shutdown Procedures

Always allow the proxy to shut down gracefully to prevent file corruption:

```bash
# Use SIGTERM for graceful shutdown
kill -TERM <proxy_pid>

# Or use systemctl for systemd-managed services
systemctl stop s3-proxy
```

Avoid force-killing the process with SIGKILL (`kill -9`) unless absolutely necessary.

### 2. Disk Health Monitoring

Regularly monitor disk health to prevent I/O errors:

```bash
# Check disk health
smartctl -a /dev/sdX

# Monitor disk I/O
iostat -x 5

# Check for filesystem errors
fsck /dev/sdX
```

### 3. Adequate Disk Space Management

Ensure sufficient disk space is always available:

```bash
# Monitor disk usage
df -h

# Set up alerts for disk space
# Configure cache size limits appropriately (typically 80-90% of available space)
```

### 4. Regular Cache Maintenance

Schedule regular cache cleanup operations:

```rust
// Run comprehensive cleanup weekly
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(7 * 24 * 3600)); // 7 days
    loop {
        interval.tick().await;
        if let Err(e) = cache_manager.perform_cache_cleanup().await {
            error!("Weekly cache cleanup failed: {}", e);
        }
    }
});
```

### 5. Backup and Recovery Planning

For critical deployments, implement backup strategies:

- Regular snapshots of cache directories (when proxy is stopped)
- Off-site backup of critical metadata
- Disaster recovery procedures documentation

### 6. Monitoring and Alerting

Implement comprehensive monitoring:

- Set up alerts for error metrics
- Monitor disk health and space
- Track cache performance metrics
- Implement log aggregation and analysis

## Conclusion

The S3 proxy's error handling system is designed to maintain availability and data integrity even when cache errors occur. By following the principles of graceful degradation, automatic recovery, and comprehensive monitoring, the system ensures that end users experience minimal disruption.

Key takeaways:

1. **Errors are handled gracefully** - Cache errors never cause proxy failures
2. **Automatic recovery** - Many errors are automatically detected and repaired
3. **Transparent to users** - End users experience no disruption during cache errors
4. **Observable** - All errors are logged and metrics are recorded
5. **Preventable** - Following best practices can minimize error occurrence

By understanding these error handling mechanisms and implementing appropriate prevention strategies, operators can ensure reliable and resilient S3 proxy deployments.