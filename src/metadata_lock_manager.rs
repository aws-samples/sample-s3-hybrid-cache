//! Enhanced Metadata Lock Manager
//!
//! Provides robust metadata locking with stale lock detection, process existence validation,
//! and atomic lock breaking for the S3 proxy's shared cache system.

use crate::{ProxyError, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tracing::{debug, error, info, warn};

/// Configuration for lock acquisition with retry
///
/// Used by `acquire_lock_with_retry` to control exponential backoff behavior.
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Maximum number of retry attempts (default: 5)
    pub max_retries: u32,
    /// Initial backoff duration (default: 50ms)
    pub initial_backoff: Duration,
    /// Maximum backoff duration (default: 2s)
    pub max_backoff: Duration,
    /// Jitter factor (0.0 to 1.0, default: 0.25 for ±25%)
    pub jitter_factor: f64,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
            jitter_factor: 0.25,
        }
    }
}

impl LockConfig {
    /// Create a new LockConfig with custom settings
    pub fn new(
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
        jitter_factor: f64,
    ) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff,
            jitter_factor: jitter_factor.clamp(0.0, 1.0),
        }
    }

    /// Calculate backoff duration for a given attempt
    ///
    /// Returns the backoff duration with exponential growth and jitter applied.
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        // Exponential backoff: initial_backoff * 2^attempt
        let base_backoff_ms = self.initial_backoff.as_millis() as u64;
        let exponential_factor = 1u64 << attempt.min(10); // Cap at 2^10 to prevent overflow
        let exponential_backoff_ms = base_backoff_ms.saturating_mul(exponential_factor);

        // Cap at max_backoff
        let capped_backoff_ms = exponential_backoff_ms.min(self.max_backoff.as_millis() as u64);

        // Add jitter (±jitter_factor of the backoff time)
        let jitter_range = (capped_backoff_ms as f64 * self.jitter_factor) as u64;
        let jitter = if jitter_range > 0 {
            let random_jitter = fastrand::u64(0..=jitter_range * 2);
            random_jitter as i64 - jitter_range as i64
        } else {
            0
        };

        let final_backoff_ms = (capped_backoff_ms as i64 + jitter).max(1) as u64;
        Duration::from_millis(final_backoff_ms)
    }
}

/// Acquire an exclusive file lock with exponential backoff and jitter
///
/// This function attempts to acquire an exclusive lock on the specified file,
/// retrying with exponential backoff if the lock is held by another process.
///
/// # Arguments
/// * `lock_path` - Path to the lock file
/// * `config` - Lock configuration controlling retry behavior
///
/// # Returns
/// * `Ok(File)` - The locked file handle (lock is held while file is open)
/// * `Err(ProxyError)` - If lock acquisition fails after all retries
///
/// # Example
/// ```ignore
/// let config = LockConfig::default();
/// let lock_file = acquire_lock_with_retry(&lock_path, &config).await?;
/// // Lock is held while lock_file is in scope
/// // Do work with the locked resource...
/// drop(lock_file); // Lock is released
/// ```
pub async fn acquire_lock_with_retry(lock_path: &Path, config: &LockConfig) -> Result<File> {
    let mut attempt = 0;

    // Ensure parent directory exists
    if let Some(parent) = lock_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to create lock directory: path={:?}, error={}",
                    parent, e
                ))
            })?;
        }
    }

    loop {
        // Try to open/create lock file
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to open lock file: path={:?}, error={}",
                    lock_path, e
                ))
            })?;

        // Try to acquire exclusive lock (non-blocking)
        match lock_file.try_lock_exclusive() {
            Ok(()) => {
                debug!(
                    "Acquired lock with retry: path={:?}, attempt={}",
                    lock_path,
                    attempt + 1
                );
                return Ok(lock_file);
            }
            Err(_) if attempt < config.max_retries => {
                // Lock is held - calculate backoff and retry
                let backoff = config.calculate_backoff(attempt);
                debug!(
                    "Lock held, retrying: path={:?}, attempt={}, backoff={:.2}ms",
                    lock_path,
                    attempt + 1,
                    backoff.as_secs_f64() * 1000.0
                );
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
            Err(e) => {
                // Max retries exceeded
                warn!(
                    "Failed to acquire lock after {} attempts: path={:?}, error={}",
                    attempt + 1,
                    lock_path,
                    e
                );
                return Err(ProxyError::LockContention(format!(
                    "Lock acquisition failed after {} retries: path={:?}",
                    attempt + 1,
                    lock_path
                )));
            }
        }
    }
}

/// Try to acquire an exclusive file lock without retrying
///
/// This is a simpler version that attempts to acquire the lock once.
///
/// # Arguments
/// * `lock_path` - Path to the lock file
///
/// # Returns
/// * `Ok(File)` - The locked file handle
/// * `Err(ProxyError)` - If lock acquisition fails
pub fn try_acquire_exclusive_lock(lock_path: &Path) -> Result<File> {
    // Ensure parent directory exists
    if let Some(parent) = lock_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ProxyError::CacheError(format!(
                    "Failed to create lock directory: path={:?}, error={}",
                    parent, e
                ))
            })?;
        }
    }

    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to open lock file: path={:?}, error={}",
                lock_path, e
            ))
        })?;

    lock_file.try_lock_exclusive().map_err(|e| {
        ProxyError::LockContention(format!(
            "Lock is held by another process: path={:?}, error={}",
            lock_path, e
        ))
    })?;

    Ok(lock_file)
}

/// Lock file content structure for enhanced metadata locking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockFileContent {
    /// Process ID of the lock holder
    pub process_id: u32,
    /// Instance ID (hostname:pid) for unique identification
    pub instance_id: String,
    /// When the lock was acquired
    pub acquired_at: SystemTime,
    /// Operation being performed under this lock
    pub operation: String,
    /// Cache key being locked
    pub cache_key: String,
}

impl LockFileContent {
    /// Create new lock file content
    pub fn new(operation: String, cache_key: String) -> Self {
        Self {
            process_id: std::process::id(),
            instance_id: get_instance_id(),
            acquired_at: SystemTime::now(),
            operation,
            cache_key,
        }
    }

    /// Check if this lock is stale based on timeout and process existence
    pub fn is_stale(&self, timeout: Duration) -> bool {
        // Check if lock has exceeded timeout
        let age = SystemTime::now()
            .duration_since(self.acquired_at)
            .unwrap_or_default();

        if age <= timeout {
            return false; // Not old enough to be stale
        }

        // Check if process still exists
        !is_process_alive(self.process_id)
    }

    /// Serialize to JSON for storage
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| ProxyError::CacheError(format!("Failed to serialize lock content: {}", e)))
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            ProxyError::CacheError(format!("Failed to deserialize lock content: {}", e))
        })
    }
}

/// Represents an acquired metadata lock
pub struct MetadataLock {
    /// Path to the lock file
    lock_file_path: PathBuf,
    /// Open file handle (keeps lock active)
    _lock_file: File,
    /// Cache key being locked
    cache_key: String,
    /// When the lock was acquired
    acquired_at: SystemTime,
}

impl MetadataLock {
    /// Create a new metadata lock
    fn new(lock_file_path: PathBuf, lock_file: File, cache_key: String) -> Self {
        Self {
            lock_file_path,
            _lock_file: lock_file,
            cache_key,
            acquired_at: SystemTime::now(),
        }
    }

    /// Get the cache key for this lock
    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }

    /// Get the lock file path
    pub fn lock_file_path(&self) -> &Path {
        &self.lock_file_path
    }

    /// Get when the lock was acquired
    pub fn acquired_at(&self) -> SystemTime {
        self.acquired_at
    }
}

impl Drop for MetadataLock {
    fn drop(&mut self) {
        // Clean up lock file when lock is dropped
        if let Err(e) = std::fs::remove_file(&self.lock_file_path) {
            // NotFound is expected on shared storage — another instance may have cleaned it up
            if e.kind() == std::io::ErrorKind::NotFound {
                debug!(
                    "Lock file already removed on drop: path={:?}",
                    self.lock_file_path
                );
            } else {
                warn!(
                    "Failed to remove lock file on drop: path={:?}, error={}",
                    self.lock_file_path, e
                );
            }
        } else {
            debug!(
                "Cleaned up lock file on drop: cache_key={}, path={:?}",
                self.cache_key, self.lock_file_path
            );
        }
    }
}

/// Enhanced metadata lock manager with stale lock detection and process validation
pub struct MetadataLockManager {
    /// Cache directory path
    cache_dir: PathBuf,
    /// Lock timeout duration
    lock_timeout: Duration,
    /// Maximum retry attempts
    max_retries: u32,
    /// Base backoff duration for exponential backoff
    base_backoff: Duration,
    /// Maximum backoff duration
    max_backoff: Duration,
}

impl MetadataLockManager {
    /// Create a new metadata lock manager
    pub fn new(cache_dir: PathBuf, lock_timeout: Duration, max_retries: u32) -> Self {
        Self {
            cache_dir,
            lock_timeout,
            max_retries,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }

    /// Create a new metadata lock manager with custom backoff settings
    pub fn new_with_backoff(
        cache_dir: PathBuf,
        lock_timeout: Duration,
        max_retries: u32,
        base_backoff: Duration,
        max_backoff: Duration,
    ) -> Self {
        Self {
            cache_dir,
            lock_timeout,
            max_retries,
            base_backoff,
            max_backoff,
        }
    }

    /// Acquire an exclusive metadata lock with stale lock detection and exponential backoff
    pub async fn acquire_lock(&self, cache_key: &str) -> Result<MetadataLock> {
        let lock_file_path = self.get_lock_file_path(cache_key);
        let mut attempt = 0;

        // Ensure parent directory exists
        if let Some(parent) = lock_file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ProxyError::CacheError(format!("Failed to create lock directory: {}", e))
            })?;
        }

        while attempt <= self.max_retries {
            match self.try_acquire_lock_once(cache_key, &lock_file_path).await {
                Ok(lock) => {
                    debug!(
                        "Successfully acquired metadata lock: cache_key={}, attempt={}, path={:?}",
                        cache_key,
                        attempt + 1,
                        lock_file_path
                    );
                    return Ok(lock);
                }
                Err(e) => {
                    if attempt == self.max_retries {
                        error!(
                            "Failed to acquire metadata lock after {} attempts: cache_key={}, error={}",
                            self.max_retries + 1, cache_key, e
                        );
                        return Err(e);
                    }

                    // Calculate exponential backoff with jitter
                    let backoff = self.calculate_backoff(attempt);
                    debug!(
                        "Lock acquisition failed, retrying: cache_key={}, attempt={}, backoff={:.2}ms, error={}",
                        cache_key, attempt + 1, backoff.as_secs_f64() * 1000.0, e
                    );

                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
            }
        }

        unreachable!("Loop should have returned or errored")
    }

    /// Try to acquire lock once (internal method)
    async fn try_acquire_lock_once(
        &self,
        cache_key: &str,
        lock_file_path: &Path,
    ) -> Result<MetadataLock> {
        // Try to open/create lock file
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(lock_file_path)
            .map_err(|e| {
                error!(
                    "Failed to open lock file: cache_key={}, path={:?}, error={}",
                    cache_key, lock_file_path, e
                );
                ProxyError::CacheError(format!(
                    "Failed to open lock file: path={:?}, error={}",
                    lock_file_path, e
                ))
            })?;

        // Try to acquire exclusive lock (non-blocking)
        match lock_file.try_lock_exclusive() {
            Ok(()) => {
                // Successfully acquired lock - write lock content
                let lock_content =
                    LockFileContent::new("metadata_update".to_string(), cache_key.to_string());

                // Write lock content to file with error handling
                if let Err(e) = std::fs::write(lock_file_path, lock_content.to_json()?) {
                    error!(
                        "Failed to write lock content: cache_key={}, path={:?}, error={}",
                        cache_key, lock_file_path, e
                    );
                    // Try to clean up the lock file on write failure
                    let _ = std::fs::remove_file(lock_file_path);
                    return Err(ProxyError::CacheError(format!(
                        "Failed to write lock content: path={:?}, error={}",
                        lock_file_path, e
                    )));
                }

                debug!(
                    "Acquired fresh metadata lock: cache_key={}, path={:?}",
                    cache_key, lock_file_path
                );

                Ok(MetadataLock::new(
                    lock_file_path.to_path_buf(),
                    lock_file,
                    cache_key.to_string(),
                ))
            }
            Err(_lock_err) => {
                debug!(
                    "Lock file is held: cache_key={}, path={:?}, checking if stale",
                    cache_key, lock_file_path
                );

                // Lock is held - check if it's stale
                match self.is_lock_stale(lock_file_path) {
                    Ok(true) => {
                        // Break stale lock and retry
                        match self.break_stale_lock(lock_file_path).await {
                            Ok(true) => {
                                info!(
                                    "Successfully broke stale lock: cache_key={}, path={:?}",
                                    cache_key, lock_file_path
                                );
                            }
                            Ok(false) => {
                                warn!(
                                    "Failed to break stale lock: cache_key={}, path={:?}",
                                    cache_key, lock_file_path
                                );
                                return Err(ProxyError::CacheError(format!(
                                    "Failed to break stale lock: cache_key={}, path={:?}",
                                    cache_key, lock_file_path
                                )));
                            }
                            Err(e) => {
                                info!(
                                    "Error breaking stale lock (likely already removed): cache_key={}, path={:?}, error={}",
                                    cache_key, lock_file_path, e
                                );
                                return Err(e);
                            }
                        }

                        // Try to acquire lock again after breaking stale lock
                        let new_lock_file = OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(lock_file_path)
                            .map_err(|e| {
                                error!(
                                    "Failed to reopen lock file after breaking stale lock: cache_key={}, path={:?}, error={}",
                                    cache_key, lock_file_path, e
                                );
                                ProxyError::CacheError(format!(
                                    "Failed to reopen lock file after breaking stale lock: path={:?}, error={}",
                                    lock_file_path, e
                                ))
                            })?;

                        new_lock_file.lock_exclusive().map_err(|e| {
                            error!(
                                "Failed to acquire lock after breaking stale lock: cache_key={}, path={:?}, error={}",
                                cache_key, lock_file_path, e
                            );
                            ProxyError::CacheError(format!(
                                "Failed to acquire lock after breaking stale lock: path={:?}, error={}",
                                lock_file_path, e
                            ))
                        })?;

                        // Write new lock content
                        let lock_content = LockFileContent::new(
                            "metadata_update".to_string(),
                            cache_key.to_string(),
                        );

                        if let Err(e) = std::fs::write(lock_file_path, lock_content.to_json()?) {
                            error!(
                                "Failed to write lock content after breaking stale lock: cache_key={}, path={:?}, error={}",
                                cache_key, lock_file_path, e
                            );
                            // Clean up on failure
                            let _ = std::fs::remove_file(lock_file_path);
                            return Err(ProxyError::CacheError(format!(
                                "Failed to write lock content after breaking stale lock: path={:?}, error={}",
                                lock_file_path, e
                            )));
                        }

                        info!(
                            "Acquired metadata lock after breaking stale lock: cache_key={}, path={:?}",
                            cache_key, lock_file_path
                        );

                        Ok(MetadataLock::new(
                            lock_file_path.to_path_buf(),
                            new_lock_file,
                            cache_key.to_string(),
                        ))
                    }
                    Ok(false) => {
                        // Lock is active - return error to trigger retry
                        debug!(
                            "Metadata lock is held by active process: cache_key={}, path={:?}",
                            cache_key, lock_file_path
                        );
                        Err(ProxyError::CacheError(format!(
                            "Metadata lock is held by active process: cache_key={}, path={:?}",
                            cache_key, lock_file_path
                        )))
                    }
                    Err(e) => {
                        error!(
                            "Failed to check if lock is stale: cache_key={}, path={:?}, error={}",
                            cache_key, lock_file_path, e
                        );
                        Err(e)
                    }
                }
            }
        }
    }

    /// Check if a lock file represents a stale lock
    fn is_lock_stale(&self, lock_file_path: &Path) -> Result<bool> {
        // Read lock file content
        let content = match std::fs::read_to_string(lock_file_path) {
            Ok(content) => content,
            Err(_) => {
                // Empty or unreadable lock file is considered stale
                debug!(
                    "Lock file is empty or unreadable, considering stale: path={:?}",
                    lock_file_path
                );
                return Ok(true);
            }
        };

        if content.trim().is_empty() {
            // Empty lock file is stale
            debug!(
                "Lock file is empty, considering stale: path={:?}",
                lock_file_path
            );
            return Ok(true);
        }

        // Parse lock content
        let lock_content = match LockFileContent::from_json(&content) {
            Ok(content) => content,
            Err(_) => {
                // Corrupted lock file is considered stale
                debug!(
                    "Lock file is corrupted, considering stale: path={:?}",
                    lock_file_path
                );
                return Ok(true);
            }
        };

        // Check if lock is stale
        let is_stale = lock_content.is_stale(self.lock_timeout);

        if is_stale {
            debug!(
                "Lock is stale: path={:?}, process_id={}, instance_id={}, age={:.2}s",
                lock_file_path,
                lock_content.process_id,
                lock_content.instance_id,
                SystemTime::now()
                    .duration_since(lock_content.acquired_at)
                    .unwrap_or_default()
                    .as_secs_f64()
            );
        } else {
            debug!(
                "Lock is active: path={:?}, process_id={}, instance_id={}, age={:.2}s",
                lock_file_path,
                lock_content.process_id,
                lock_content.instance_id,
                SystemTime::now()
                    .duration_since(lock_content.acquired_at)
                    .unwrap_or_default()
                    .as_secs_f64()
            );
        }

        Ok(is_stale)
    }

    /// Break a stale lock with consistency validation
    pub async fn break_stale_lock(&self, lock_file_path: &Path) -> Result<bool> {
        debug!("Breaking stale lock: path={:?}", lock_file_path);

        // Read lock content before breaking (for logging)
        let lock_info = match std::fs::read_to_string(lock_file_path) {
            Ok(content) => match LockFileContent::from_json(&content) {
                Ok(lock_content) => Some(format!(
                    "process_id={}, instance_id={}, operation={}",
                    lock_content.process_id, lock_content.instance_id, lock_content.operation
                )),
                Err(_) => Some("corrupted".to_string()),
            },
            Err(_) => None,
        };

        // Remove the stale lock file
        match std::fs::remove_file(lock_file_path) {
            Ok(()) => {
                info!(
                    "Successfully broke stale lock: path={:?}, lock_info={:?}",
                    lock_file_path, lock_info
                );

                Ok(true)
            }
            Err(e) => {
                info!(
                    "Failed to break stale lock (likely already removed): path={:?}, lock_info={:?}, error={}",
                    lock_file_path, lock_info, e
                );
                Err(ProxyError::CacheError(format!(
                    "Failed to remove stale lock file: {}",
                    e
                )))
            }
        }
    }

    /// Validate that a lock holder process exists
    pub async fn validate_lock_holder(&self, lock_file_path: &Path) -> Result<bool> {
        let content = std::fs::read_to_string(lock_file_path).map_err(|e| {
            ProxyError::CacheError(format!(
                "Failed to read lock file: path={:?}, error={}",
                lock_file_path, e
            ))
        })?;

        let lock_content = LockFileContent::from_json(&content)?;

        let process_exists = is_process_alive(lock_content.process_id);

        debug!(
            "Lock holder validation: path={:?}, process_id={}, exists={}",
            lock_file_path, lock_content.process_id, process_exists
        );

        Ok(process_exists)
    }

    /// Get lock file path for a cache key
    fn get_lock_file_path(&self, cache_key: &str) -> PathBuf {
        // Use the same path structure as metadata files
        let metadata_path = self.get_metadata_file_path(cache_key);
        metadata_path.with_extension("meta.lock")
    }

    /// Get metadata file path for a cache key
    ///
    /// # Panics
    /// Panics if cache_key is malformed (missing bucket/object separator).
    fn get_metadata_file_path(&self, cache_key: &str) -> PathBuf {
        let base_dir = self.cache_dir.join("metadata");

        // Use the same sharding logic as DiskCacheManager for consistency
        match crate::disk_cache::get_sharded_path(&base_dir, cache_key, ".meta") {
            Ok(path) => path,
            Err(e) => {
                panic!(
                    "Malformed cache key '{}': {}. Cache keys must be in 'bucket/object' format.",
                    cache_key, e
                );
            }
        }
    }

    /// Calculate exponential backoff with jitter
    fn calculate_backoff(&self, attempt: u32) -> Duration {
        // Exponential backoff: base_backoff * 2^attempt
        let exponential_backoff = self.base_backoff * (1u32 << attempt.min(10)); // Cap at 2^10
        let capped_backoff = exponential_backoff.min(self.max_backoff);

        // Add jitter (±25% of the backoff time)
        let jitter_range = capped_backoff.as_millis() / 4; // 25%
        let jitter = fastrand::u64(0..=jitter_range as u64 * 2) as i64 - jitter_range as i64;
        let jittered_millis = (capped_backoff.as_millis() as i64 + jitter).max(0) as u64;

        Duration::from_millis(jittered_millis)
    }
}

/// Get unique instance ID for this process
fn get_instance_id() -> String {
    format!(
        "{}:{}",
        hostname::get().unwrap_or_default().to_string_lossy(),
        std::process::id()
    )
}

/// Check if a process is alive (cross-platform)
fn is_process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // On Unix systems, use kill(pid, 0) to check if process exists
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }

    #[cfg(windows)]
    {
        // On Windows, try to open the process handle
        use std::ptr;
        use winapi::um::handleapi::CloseHandle;
        use winapi::um::processthreadsapi::OpenProcess;
        use winapi::um::winnt::PROCESS_QUERY_INFORMATION;

        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid);
            if handle != ptr::null_mut() {
                CloseHandle(handle);
                true
            } else {
                false
            }
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        // Fallback: assume process is alive (conservative approach)
        warn!(
            "Process existence check not implemented for this platform, assuming process is alive"
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_lock_file_content_creation() {
        let content = LockFileContent::new(
            "test_operation".to_string(),
            "test-bucket/test-object".to_string(),
        );

        assert_eq!(content.process_id, std::process::id());
        assert_eq!(content.operation, "test_operation");
        assert_eq!(content.cache_key, "test-bucket/test-object");
        assert!(!content.instance_id.is_empty());
    }

    #[test]
    fn test_lock_file_content_serialization() {
        let content = LockFileContent::new(
            "test_operation".to_string(),
            "test-bucket/test-object".to_string(),
        );

        let json = content.to_json().unwrap();
        let deserialized = LockFileContent::from_json(&json).unwrap();

        assert_eq!(deserialized.process_id, content.process_id);
        assert_eq!(deserialized.operation, content.operation);
        assert_eq!(deserialized.cache_key, content.cache_key);
        assert_eq!(deserialized.instance_id, content.instance_id);
    }

    #[test]
    fn test_lock_file_content_stale_detection() {
        let mut content = LockFileContent::new(
            "test_operation".to_string(),
            "test-bucket/test-object".to_string(),
        );

        // Fresh lock should not be stale
        assert!(!content.is_stale(Duration::from_secs(60)));

        // Old lock with current process should not be stale (process is alive)
        content.acquired_at = SystemTime::now() - Duration::from_secs(120);
        assert!(!content.is_stale(Duration::from_secs(60)));

        // Old lock with non-existent process should be stale
        content.process_id = 999999; // Unlikely to exist
        assert!(content.is_stale(Duration::from_secs(60)));
    }

    #[test]
    fn test_metadata_lock_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            MetadataLockManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(30), 3);

        assert_eq!(manager.cache_dir, temp_dir.path());
        assert_eq!(manager.lock_timeout, Duration::from_secs(30));
        assert_eq!(manager.max_retries, 3);
    }

    #[tokio::test]
    async fn test_lock_acquisition_basic() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            MetadataLockManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(30), 3);

        let cache_key = "test-bucket/test-object";
        let lock = manager.acquire_lock(cache_key).await.unwrap();

        assert_eq!(lock.cache_key(), cache_key);
        assert!(lock.lock_file_path().exists());

        // Lock file should contain valid JSON
        let content = std::fs::read_to_string(lock.lock_file_path()).unwrap();
        let lock_content = LockFileContent::from_json(&content).unwrap();
        assert_eq!(lock_content.cache_key, cache_key);
        assert_eq!(lock_content.process_id, std::process::id());
    }

    #[tokio::test]
    async fn test_lock_cleanup_on_drop() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            MetadataLockManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(30), 3);

        let cache_key = "test-bucket/test-object";
        let lock_file_path = {
            let lock = manager.acquire_lock(cache_key).await.unwrap();
            let path = lock.lock_file_path().to_path_buf();
            assert!(path.exists());
            path
        }; // Lock is dropped here

        // Lock file should be cleaned up
        assert!(!lock_file_path.exists());
    }

    // Tests for LockConfig and acquire_lock_with_retry

    #[test]
    fn test_lock_config_default() {
        let config = LockConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff, Duration::from_millis(50));
        assert_eq!(config.max_backoff, Duration::from_secs(2));
        assert!((config.jitter_factor - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_lock_config_custom() {
        let config = LockConfig::new(10, Duration::from_millis(100), Duration::from_secs(5), 0.5);
        assert_eq!(config.max_retries, 10);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_backoff, Duration::from_secs(5));
        assert!((config.jitter_factor - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_lock_config_jitter_clamping() {
        // Jitter factor should be clamped to [0.0, 1.0]
        let config = LockConfig::new(
            5,
            Duration::from_millis(50),
            Duration::from_secs(2),
            1.5, // Should be clamped to 1.0
        );
        assert!((config.jitter_factor - 1.0).abs() < f64::EPSILON);

        let config2 = LockConfig::new(
            5,
            Duration::from_millis(50),
            Duration::from_secs(2),
            -0.5, // Should be clamped to 0.0
        );
        assert!(config2.jitter_factor.abs() < f64::EPSILON);
    }

    #[test]
    fn test_lock_config_calculate_backoff_exponential_growth() {
        let config = LockConfig::new(
            5,
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.0, // No jitter for predictable testing
        );

        // Attempt 0: 100ms * 2^0 = 100ms
        let backoff0 = config.calculate_backoff(0);
        assert_eq!(backoff0.as_millis(), 100);

        // Attempt 1: 100ms * 2^1 = 200ms
        let backoff1 = config.calculate_backoff(1);
        assert_eq!(backoff1.as_millis(), 200);

        // Attempt 2: 100ms * 2^2 = 400ms
        let backoff2 = config.calculate_backoff(2);
        assert_eq!(backoff2.as_millis(), 400);

        // Attempt 3: 100ms * 2^3 = 800ms
        let backoff3 = config.calculate_backoff(3);
        assert_eq!(backoff3.as_millis(), 800);
    }

    #[test]
    fn test_lock_config_calculate_backoff_cap() {
        let config = LockConfig::new(
            5,
            Duration::from_millis(100),
            Duration::from_millis(500), // Cap at 500ms
            0.0,                        // No jitter
        );

        // Attempt 0: 100ms (under cap)
        assert_eq!(config.calculate_backoff(0).as_millis(), 100);

        // Attempt 1: 200ms (under cap)
        assert_eq!(config.calculate_backoff(1).as_millis(), 200);

        // Attempt 2: 400ms (under cap)
        assert_eq!(config.calculate_backoff(2).as_millis(), 400);

        // Attempt 3: 800ms -> capped to 500ms
        assert_eq!(config.calculate_backoff(3).as_millis(), 500);

        // Attempt 10: still capped at 500ms
        assert_eq!(config.calculate_backoff(10).as_millis(), 500);
    }

    #[test]
    fn test_lock_config_calculate_backoff_with_jitter() {
        let config = LockConfig::new(
            5,
            Duration::from_millis(100),
            Duration::from_secs(10),
            0.25, // ±25% jitter
        );

        // Run multiple times to verify jitter is within bounds
        for _ in 0..100 {
            let backoff = config.calculate_backoff(0);
            // Base is 100ms, jitter is ±25ms, so range is 75-125ms
            assert!(
                backoff.as_millis() >= 75,
                "Backoff {} too low",
                backoff.as_millis()
            );
            assert!(
                backoff.as_millis() <= 125,
                "Backoff {} too high",
                backoff.as_millis()
            );
        }
    }

    #[tokio::test]
    async fn test_acquire_lock_with_retry_success() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("test.lock");
        let config = LockConfig::default();

        let lock_file = acquire_lock_with_retry(&lock_path, &config).await.unwrap();
        assert!(lock_path.exists());

        // Lock should be held
        drop(lock_file);
    }

    #[tokio::test]
    async fn test_acquire_lock_with_retry_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir
            .path()
            .join("subdir")
            .join("nested")
            .join("test.lock");
        let config = LockConfig::default();

        // Directory doesn't exist yet
        assert!(!lock_path.parent().unwrap().exists());

        let lock_file = acquire_lock_with_retry(&lock_path, &config).await.unwrap();

        // Directory should be created
        assert!(lock_path.parent().unwrap().exists());
        assert!(lock_path.exists());

        drop(lock_file);
    }

    #[tokio::test]
    async fn test_try_acquire_exclusive_lock_success() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("test.lock");

        let lock_file = try_acquire_exclusive_lock(&lock_path).unwrap();
        assert!(lock_path.exists());

        drop(lock_file);
    }

    #[tokio::test]
    async fn test_try_acquire_exclusive_lock_contention() {
        let temp_dir = TempDir::new().unwrap();
        let lock_path = temp_dir.path().join("test.lock");

        // Acquire first lock
        let _lock1 = try_acquire_exclusive_lock(&lock_path).unwrap();

        // Second attempt should fail with LockContention
        let result = try_acquire_exclusive_lock(&lock_path);
        assert!(result.is_err());

        match result {
            Err(ProxyError::LockContention(_)) => {} // Expected
            Err(e) => panic!("Expected LockContention error, got: {:?}", e),
            Ok(_) => panic!("Expected error, got success"),
        }
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: journal-based-metadata-updates, Property 3: Exponential Backoff with Jitter**
    /// *For any* lock acquisition attempt that fails, the retry delay shall be:
    /// - At least `initial_backoff * 2^(attempt-1)` (exponential growth)
    /// - At most `max_backoff` (capped)
    /// - Include jitter within ±`jitter_factor` of the base delay
    /// - Stop after `max_retries` attempts
    /// **Validates: Requirements 3.2, 3.5, 3.7**
    #[quickcheck]
    fn prop_exponential_backoff_with_jitter(
        initial_backoff_ms: u16,
        max_backoff_ms: u16,
        jitter_factor_percent: u8,
        attempt: u8,
    ) -> TestResult {
        // Filter invalid inputs
        if initial_backoff_ms == 0 || max_backoff_ms == 0 {
            return TestResult::discard();
        }
        if initial_backoff_ms > max_backoff_ms {
            return TestResult::discard();
        }
        if attempt > 15 {
            // Limit attempts to prevent overflow
            return TestResult::discard();
        }

        let initial_backoff = Duration::from_millis(initial_backoff_ms as u64);
        let max_backoff = Duration::from_millis(max_backoff_ms as u64);
        let jitter_factor = (jitter_factor_percent % 100) as f64 / 100.0; // 0.0 to 0.99

        let config = LockConfig::new(5, initial_backoff, max_backoff, jitter_factor);

        let backoff = config.calculate_backoff(attempt as u32);

        // Calculate expected base backoff (without jitter)
        let exponential_factor = 1u64 << (attempt as u32).min(10);
        let expected_base_ms = (initial_backoff_ms as u64)
            .saturating_mul(exponential_factor)
            .min(max_backoff_ms as u64);

        // Calculate jitter bounds
        let jitter_range = (expected_base_ms as f64 * jitter_factor) as u64;
        let min_expected = expected_base_ms.saturating_sub(jitter_range).max(1);
        let max_expected = expected_base_ms.saturating_add(jitter_range);

        let actual_ms = backoff.as_millis() as u64;

        // Property 1: Backoff should be within jitter bounds of expected base
        if actual_ms < min_expected || actual_ms > max_expected {
            return TestResult::error(format!(
                "Backoff {} not in expected range [{}, {}] for attempt {} (base={}, jitter_factor={})",
                actual_ms, min_expected, max_expected, attempt, expected_base_ms, jitter_factor
            ));
        }

        // Property 2: Backoff should never exceed max_backoff + jitter
        let absolute_max = max_backoff_ms as u64 + (max_backoff_ms as f64 * jitter_factor) as u64;
        if actual_ms > absolute_max {
            return TestResult::error(format!(
                "Backoff {} exceeds absolute max {} for attempt {}",
                actual_ms, absolute_max, attempt
            ));
        }

        // Property 3: Backoff should be at least 1ms
        if actual_ms < 1 {
            return TestResult::error(format!(
                "Backoff {} is less than 1ms for attempt {}",
                actual_ms, attempt
            ));
        }

        TestResult::passed()
    }

    /// Property test: Exponential growth without jitter
    /// Verifies that backoff grows exponentially when jitter is disabled.
    #[quickcheck]
    fn prop_exponential_growth_no_jitter(
        initial_backoff_ms: u16,
        max_backoff_ms: u16,
    ) -> TestResult {
        // Filter invalid inputs
        if initial_backoff_ms == 0 || max_backoff_ms == 0 {
            return TestResult::discard();
        }
        if initial_backoff_ms > max_backoff_ms {
            return TestResult::discard();
        }

        let initial_backoff = Duration::from_millis(initial_backoff_ms as u64);
        let max_backoff = Duration::from_millis(max_backoff_ms as u64);

        let config = LockConfig::new(
            5,
            initial_backoff,
            max_backoff,
            0.0, // No jitter
        );

        // Verify exponential growth for first few attempts
        let mut prev_backoff = 0u64;
        for attempt in 0..5 {
            let backoff = config.calculate_backoff(attempt);
            let backoff_ms = backoff.as_millis() as u64;

            // Each backoff should be >= previous (exponential growth)
            if backoff_ms < prev_backoff {
                return TestResult::error(format!(
                    "Backoff decreased: attempt {} has {}ms, previous was {}ms",
                    attempt, backoff_ms, prev_backoff
                ));
            }

            // Backoff should be exactly initial * 2^attempt (capped at max)
            let expected =
                (initial_backoff_ms as u64 * (1u64 << attempt)).min(max_backoff_ms as u64);
            if backoff_ms != expected {
                return TestResult::error(format!(
                    "Backoff mismatch: attempt {} has {}ms, expected {}ms",
                    attempt, backoff_ms, expected
                ));
            }

            prev_backoff = backoff_ms;
        }

        TestResult::passed()
    }

    /// Property test: Max backoff cap is respected
    #[quickcheck]
    fn prop_max_backoff_cap_respected(
        initial_backoff_ms: u16,
        max_backoff_ms: u16,
        jitter_factor_percent: u8,
    ) -> TestResult {
        // Filter invalid inputs
        if initial_backoff_ms == 0 || max_backoff_ms == 0 {
            return TestResult::discard();
        }
        if initial_backoff_ms > max_backoff_ms {
            return TestResult::discard();
        }

        let initial_backoff = Duration::from_millis(initial_backoff_ms as u64);
        let max_backoff = Duration::from_millis(max_backoff_ms as u64);
        let jitter_factor = (jitter_factor_percent % 100) as f64 / 100.0;

        let config = LockConfig::new(5, initial_backoff, max_backoff, jitter_factor);

        // Test with high attempt numbers that would exceed max without cap
        for attempt in 10..15 {
            let backoff = config.calculate_backoff(attempt);
            let backoff_ms = backoff.as_millis() as u64;

            // Backoff should never exceed max + jitter
            let absolute_max =
                max_backoff_ms as u64 + (max_backoff_ms as f64 * jitter_factor) as u64;
            if backoff_ms > absolute_max {
                return TestResult::error(format!(
                    "Backoff {} exceeds max {} + jitter for attempt {}",
                    backoff_ms, max_backoff_ms, attempt
                ));
            }
        }

        TestResult::passed()
    }
}
