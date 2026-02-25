//! Cache Hit Update Buffer
//!
//! RAM buffer for cache-hit metadata updates (TTL refresh, access tracking).
//! Provides efficient I/O on shared storage by buffering updates in RAM.
//!
//! Key features:
//! - RAM buffer for cache-hit updates (reduces disk I/O dramatically)
//! - Periodic flush every 5 seconds (configurable)
//! - Per-instance journal files (no cross-instance contention on shared storage)
//! - Batched writes reduce I/O overhead
//!
//! Structure:
//! ```text
//! cache/
//! └── metadata/
//!     └── _journals/
//!         ├── instance-a.journal    # All entries from instance A
//!         └── instance-b.journal    # All entries from instance B
//! ```

use crate::cache_types::RangeSpec;
use crate::compression::CompressionAlgorithm;
use crate::error::{ProxyError, Result};
use crate::journal_manager::{JournalEntry, JournalOperation};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error};

/// Default flush interval for RAM buffer (5 seconds)
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 5;

/// Maximum entries in RAM buffer before forcing a flush
const MAX_BUFFER_ENTRIES: usize = 10000;

/// Type of cache-hit update
#[derive(Debug, Clone, PartialEq)]
pub enum CacheHitUpdateType {
    /// Refresh TTL for a cached range
    TtlRefresh {
        /// New TTL duration in seconds
        new_ttl_secs: u64,
    },
    /// Update access count and last_accessed timestamp
    AccessUpdate {
        /// Number of accesses to add
        increment: u64,
    },
}

/// Buffered cache-hit update entry waiting to be flushed to journal
#[derive(Debug, Clone)]
pub struct BufferedCacheHitUpdate {
    /// Cache key (bucket/object)
    pub cache_key: String,
    /// Range start byte
    pub range_start: u64,
    /// Range end byte
    pub range_end: u64,
    /// Type of update
    pub update_type: CacheHitUpdateType,
    /// When this update was recorded
    pub timestamp: SystemTime,
}

/// Result of a flush operation
#[derive(Debug, Default)]
pub struct FlushResult {
    /// Number of entries flushed to journal
    pub entries_flushed: u64,
    /// Duration of the flush operation
    pub duration_ms: u64,
    /// Number of errors encountered
    pub errors: u64,
}

/// Statistics about the cache hit update buffer
#[derive(Debug, Clone)]
pub struct CacheHitUpdateBufferStats {
    /// Total updates recorded since creation
    pub updates_recorded: u64,
    /// Total entries flushed to journal
    pub entries_flushed: u64,
    /// Current buffer size
    pub buffer_size: usize,
}

/// RAM buffer for cache-hit metadata updates with periodic flush to journal
///
/// This buffer collects TTL refresh and access update operations in memory,
/// then periodically flushes them to a per-instance journal file. This approach:
/// - Reduces disk I/O by ~99% (batching hundreds of updates into single writes)
/// - Eliminates cross-instance contention on journal writes
/// - Maintains durability through periodic flushes
pub struct CacheHitUpdateBuffer {
    /// Cache directory path
    cache_dir: PathBuf,
    /// Instance ID for this proxy instance
    instance_id: String,
    /// RAM buffer for updates
    buffer: Arc<Mutex<Vec<BufferedCacheHitUpdate>>>,
    /// Last flush time
    last_flush: Arc<RwLock<Instant>>,
    /// Flush interval
    flush_interval: Duration,
    /// Maximum buffer size before forced flush
    max_buffer_size: usize,
    /// Flag to indicate if flush is in progress
    flush_in_progress: AtomicBool,
    /// Counter for updates recorded
    updates_recorded: AtomicU64,
    /// Counter for entries flushed
    entries_flushed: AtomicU64,
}

impl CacheHitUpdateBuffer {
    /// Create a new cache hit update buffer with default settings
    pub fn new(cache_dir: PathBuf, instance_id: String) -> Self {
        Self::with_config(
            cache_dir,
            instance_id,
            Duration::from_secs(DEFAULT_FLUSH_INTERVAL_SECS),
            MAX_BUFFER_ENTRIES,
        )
    }

    /// Create a new cache hit update buffer with custom configuration
    pub fn with_config(
        cache_dir: PathBuf,
        instance_id: String,
        flush_interval: Duration,
        max_buffer_size: usize,
    ) -> Self {
        Self {
            cache_dir,
            instance_id,
            buffer: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            last_flush: Arc::new(RwLock::new(Instant::now())),
            flush_interval,
            max_buffer_size,
            flush_in_progress: AtomicBool::new(false),
            updates_recorded: AtomicU64::new(0),
            entries_flushed: AtomicU64::new(0),
        }
    }

    /// Get the journal file path for this instance
    fn get_journal_path(&self) -> PathBuf {
        self.cache_dir
            .join("metadata")
            .join("_journals")
            .join(format!("{}.journal", self.instance_id))
    }

    /// Record a TTL refresh (buffered in RAM)
    pub async fn record_ttl_refresh(
        &self,
        cache_key: &str,
        new_ttl: Duration,
    ) -> Result<()> {
        let entry = BufferedCacheHitUpdate {
            cache_key: cache_key.to_string(),
            range_start: 0,
            range_end: 0,
            update_type: CacheHitUpdateType::TtlRefresh {
                new_ttl_secs: new_ttl.as_secs(),
            },
            timestamp: SystemTime::now(),
        };

        self.add_to_buffer(entry).await?;
        self.maybe_flush().await
    }

    /// Record an access update (buffered in RAM)
    pub async fn record_access_update(
        &self,
        cache_key: &str,
        range_start: u64,
        range_end: u64,
        increment: u64,
    ) -> Result<()> {
        let entry = BufferedCacheHitUpdate {
            cache_key: cache_key.to_string(),
            range_start,
            range_end,
            update_type: CacheHitUpdateType::AccessUpdate { increment },
            timestamp: SystemTime::now(),
        };

        self.add_to_buffer(entry).await?;
        self.maybe_flush().await
    }

    /// Add an entry to the buffer
    async fn add_to_buffer(&self, entry: BufferedCacheHitUpdate) -> Result<()> {
        let mut buffer = self.buffer.lock().await;
        buffer.push(entry);
        self.updates_recorded.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Check if flush is needed and perform it if so
    async fn maybe_flush(&self) -> Result<()> {
        let buffer_len = {
            let buffer = self.buffer.lock().await;
            buffer.len()
        };

        let should_flush = {
            let last_flush = self.last_flush.read().await;
            let time_since_flush = last_flush.elapsed();

            // Flush if: time exceeded OR buffer is full
            time_since_flush >= self.flush_interval || buffer_len >= self.max_buffer_size
        };

        if should_flush {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush the RAM buffer to the journal file
    ///
    /// This writes all buffered entries to the per-instance journal file.
    /// The journal file is append-only with one JSON line per entry.
    pub async fn flush(&self) -> Result<FlushResult> {
        // Use compare_exchange to ensure only one flush runs at a time
        if self
            .flush_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // Another flush is in progress, skip this one
            return Ok(FlushResult::default());
        }

        let start = Instant::now();
        let mut entries_flushed = 0u64;
        let mut errors = 0u64;

        // Take all entries from the buffer
        let entries: Vec<BufferedCacheHitUpdate> = {
            let mut buffer = self.buffer.lock().await;
            std::mem::take(&mut *buffer)
        };

        if entries.is_empty() {
            self.flush_in_progress.store(false, Ordering::SeqCst);
            // Update last flush time even if empty
            {
                let mut last_flush = self.last_flush.write().await;
                *last_flush = Instant::now();
            }
            return Ok(FlushResult::default());
        }

        // Get journal path and ensure directory exists
        let journal_path = self.get_journal_path();
        if let Some(parent) = journal_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!(
                    "Failed to create journal directory: path={:?}, error={}",
                    parent, e
                );
                // Put entries back in buffer
                {
                    let mut buffer = self.buffer.lock().await;
                    buffer.extend(entries);
                }
                self.flush_in_progress.store(false, Ordering::SeqCst);
                return Err(ProxyError::CacheError(format!(
                    "Failed to create journal directory: {}",
                    e
                )));
            }
        }

        // Convert buffered entries to journal entries and build content
        let mut content = String::new();
        for entry in &entries {
            match self.to_journal_entry(entry) {
                Ok(journal_entry) => match serde_json::to_string(&journal_entry) {
                    Ok(json) => {
                        content.push_str(&json);
                        content.push('\n');
                        entries_flushed += 1;
                    }
                    Err(e) => {
                        error!(
                            "Failed to serialize journal entry: cache_key={}, error={}",
                            entry.cache_key, e
                        );
                        errors += 1;
                    }
                },
                Err(e) => {
                    error!(
                        "Failed to create journal entry: cache_key={}, error={}",
                        entry.cache_key, e
                    );
                    errors += 1;
                }
            }
        }

        // Append to journal file
        if !content.is_empty() {
            use tokio::io::AsyncWriteExt;

            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&journal_path)
                .await
            {
                Ok(mut file) => {
                    if let Err(e) = file.write_all(content.as_bytes()).await {
                        error!(
                            "Failed to write to journal file: path={:?}, error={}",
                            journal_path, e
                        );
                        errors += entries_flushed;
                        entries_flushed = 0;
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to open journal file: path={:?}, error={}",
                        journal_path, e
                    );
                    errors += entries_flushed;
                    entries_flushed = 0;
                }
            }
        }

        // Update counters and timestamps
        self.entries_flushed
            .fetch_add(entries_flushed, Ordering::Relaxed);
        {
            let mut last_flush = self.last_flush.write().await;
            *last_flush = Instant::now();
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        // Release flush lock
        self.flush_in_progress.store(false, Ordering::SeqCst);

        if entries_flushed > 0 || errors > 0 {
            debug!(
                "Cache hit update buffer flushed: entries={}, duration={}ms, errors={}",
                entries_flushed, duration_ms, errors
            );
        }

        Ok(FlushResult {
            entries_flushed,
            duration_ms,
            errors,
        })
    }

    /// Convert a buffered entry to a journal entry
    fn to_journal_entry(&self, entry: &BufferedCacheHitUpdate) -> Result<JournalEntry> {
        let (operation, new_ttl_secs, access_increment) = match &entry.update_type {
            CacheHitUpdateType::TtlRefresh { new_ttl_secs } => {
                (JournalOperation::TtlRefresh, Some(*new_ttl_secs), None)
            }
            CacheHitUpdateType::AccessUpdate { increment } => {
                (JournalOperation::AccessUpdate, None, Some(*increment))
            }
        };

        // Create a minimal RangeSpec for identification purposes
        let range_spec = RangeSpec {
            start: entry.range_start,
            end: entry.range_end,
            file_path: String::new(), // Not needed for cache-hit updates
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 0,
            uncompressed_size: 0,
            created_at: entry.timestamp,
            last_accessed: entry.timestamp,
            access_count: 0,
            frequency_score: 0,
        };

        Ok(JournalEntry {
            timestamp: entry.timestamp,
            instance_id: self.instance_id.clone(),
            cache_key: entry.cache_key.clone(),
            range_spec,
            operation,
            range_file_path: String::new(),
            metadata_version: 0,
            new_ttl_secs,
            object_ttl_secs: None, // Cache-hit updates don't set object TTL
            access_increment,
            object_metadata: None, // Cache-hit updates don't need object_metadata - .meta already exists
            metadata_written: true, // Cache-hit updates are for existing ranges, so metadata already exists
        })
    }

    /// Force flush the buffer (used during shutdown or testing)
    pub async fn force_flush(&self) -> Result<FlushResult> {
        self.flush().await
    }

    /// Get the number of entries currently in the buffer
    pub async fn buffer_len(&self) -> usize {
        let buffer = self.buffer.lock().await;
        buffer.len()
    }

    /// Get statistics about the buffer
    pub async fn get_stats(&self) -> CacheHitUpdateBufferStats {
        let buffer_size = self.buffer_len().await;
        CacheHitUpdateBufferStats {
            updates_recorded: self.updates_recorded.load(Ordering::Relaxed),
            entries_flushed: self.entries_flushed.load(Ordering::Relaxed),
            buffer_size,
        }
    }

    /// Get the instance ID
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_cache_hit_update_buffer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let buffer =
            CacheHitUpdateBuffer::new(temp_dir.path().to_path_buf(), "test-instance".to_string());

        assert_eq!(buffer.instance_id(), "test-instance");
        assert_eq!(buffer.buffer_len().await, 0);
    }

    #[tokio::test]
    async fn test_record_ttl_refresh() {
        let temp_dir = TempDir::new().unwrap();
        let buffer = CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
            Duration::from_secs(60), // Long interval to prevent auto-flush
            10000,
        );

        buffer
            .record_ttl_refresh(
                "test-bucket/test-object",
                Duration::from_secs(3600),
            )
            .await
            .unwrap();

        assert_eq!(buffer.buffer_len().await, 1);

        let stats = buffer.get_stats().await;
        assert_eq!(stats.updates_recorded, 1);
        assert_eq!(stats.entries_flushed, 0);
    }

    #[tokio::test]
    async fn test_record_access_update() {
        let temp_dir = TempDir::new().unwrap();
        let buffer = CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
            Duration::from_secs(60),
            10000,
        );

        buffer
            .record_access_update("test-bucket/test-object", 0, 8388607, 5)
            .await
            .unwrap();

        assert_eq!(buffer.buffer_len().await, 1);

        let stats = buffer.get_stats().await;
        assert_eq!(stats.updates_recorded, 1);
    }

    #[tokio::test]
    async fn test_flush_creates_journal_file() {
        let temp_dir = TempDir::new().unwrap();
        let buffer = CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
            Duration::from_secs(60),
            10000,
        );

        // Record some updates
        buffer
            .record_ttl_refresh("test-bucket/obj1", Duration::from_secs(3600))
            .await
            .unwrap();
        buffer
            .record_access_update("test-bucket/obj2", 0, 2000, 3)
            .await
            .unwrap();

        // Force flush
        let result = buffer.force_flush().await.unwrap();
        assert_eq!(result.entries_flushed, 2);
        assert_eq!(result.errors, 0);

        // Verify journal file exists
        let journal_path = temp_dir
            .path()
            .join("metadata")
            .join("_journals")
            .join("test-instance.journal");
        assert!(journal_path.exists());

        // Verify content
        let content = tokio::fs::read_to_string(&journal_path).await.unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        // Parse and verify entries
        let entry1: JournalEntry = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(entry1.cache_key, "test-bucket/obj1");
        assert_eq!(entry1.operation, JournalOperation::TtlRefresh);
        assert_eq!(entry1.new_ttl_secs, Some(3600));

        let entry2: JournalEntry = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(entry2.cache_key, "test-bucket/obj2");
        assert_eq!(entry2.operation, JournalOperation::AccessUpdate);
        assert_eq!(entry2.access_increment, Some(3));
    }

    #[tokio::test]
    async fn test_flush_clears_buffer() {
        let temp_dir = TempDir::new().unwrap();
        let buffer = CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
            Duration::from_secs(60),
            10000,
        );

        buffer
            .record_ttl_refresh("test-bucket/obj1", Duration::from_secs(3600))
            .await
            .unwrap();

        assert_eq!(buffer.buffer_len().await, 1);

        buffer.force_flush().await.unwrap();

        assert_eq!(buffer.buffer_len().await, 0);
    }

    #[tokio::test]
    async fn test_auto_flush_on_buffer_full() {
        let temp_dir = TempDir::new().unwrap();
        let buffer = CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
            Duration::from_secs(60), // Long interval
            5,                       // Small buffer size to trigger auto-flush
        );

        // Record entries up to buffer limit
        for i in 0..5 {
            buffer
                .record_ttl_refresh(
                    &format!("test-bucket/obj{}", i),
                    Duration::from_secs(3600),
                )
                .await
                .unwrap();
        }

        // Buffer should have been flushed
        let stats = buffer.get_stats().await;
        assert_eq!(stats.entries_flushed, 5);
        assert_eq!(stats.buffer_size, 0);
    }

    #[tokio::test]
    async fn test_multiple_flushes_append() {
        let temp_dir = TempDir::new().unwrap();
        let buffer = CacheHitUpdateBuffer::with_config(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
            Duration::from_secs(60),
            10000,
        );

        // First batch
        buffer
            .record_ttl_refresh("test-bucket/obj1", Duration::from_secs(3600))
            .await
            .unwrap();
        buffer.force_flush().await.unwrap();

        // Second batch
        buffer
            .record_access_update("test-bucket/obj2", 0, 2000, 5)
            .await
            .unwrap();
        buffer.force_flush().await.unwrap();

        // Verify both entries are in journal
        let journal_path = temp_dir
            .path()
            .join("metadata")
            .join("_journals")
            .join("test-instance.journal");
        let content = tokio::fs::read_to_string(&journal_path).await.unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn test_empty_flush() {
        let temp_dir = TempDir::new().unwrap();
        let buffer =
            CacheHitUpdateBuffer::new(temp_dir.path().to_path_buf(), "test-instance".to_string());

        // Flush with no entries
        let result = buffer.force_flush().await.unwrap();
        assert_eq!(result.entries_flushed, 0);
        assert_eq!(result.errors, 0);

        // Journal file should not be created
        let journal_path = temp_dir
            .path()
            .join("metadata")
            .join("_journals")
            .join("test-instance.journal");
        assert!(!journal_path.exists());
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: journal-based-metadata-updates, Property 1: Journal Routing for Cache-Hit Updates**
    /// *For any* cache-hit operation (TTL refresh, access count update) in shared storage mode,
    /// the system shall create a journal entry rather than writing directly to the metadata file.
    /// **Validates: Requirements 1.1, 1.2, 1.3**
    #[quickcheck]
    fn prop_journal_routing_for_cache_hit_updates(
        cache_key_seed: u8,
        range_start: u64,
        range_end_offset: u64,
        is_ttl_refresh: bool,
        ttl_secs: u64,
        access_increment: u8,
    ) -> TestResult {
        // Filter invalid inputs
        if ttl_secs == 0 || access_increment == 0 {
            return TestResult::discard();
        }

        let range_end = range_start.saturating_add(range_end_offset);
        let cache_key = format!("test-bucket/object-{}", cache_key_seed);

        // Create a runtime for async operations
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let buffer = CacheHitUpdateBuffer::with_config(
                temp_dir.path().to_path_buf(),
                "test-instance".to_string(),
                Duration::from_secs(60), // Long interval to prevent auto-flush
                10000,
            );

            // Record the update
            if is_ttl_refresh {
                buffer
                    .record_ttl_refresh(
                        &cache_key,
                        Duration::from_secs(ttl_secs),
                    )
                    .await
                    .unwrap();
            } else {
                buffer
                    .record_access_update(
                        &cache_key,
                        range_start,
                        range_end,
                        access_increment as u64,
                    )
                    .await
                    .unwrap();
            }

            // Property 1: Update should be buffered (not written directly)
            let buffer_len = buffer.buffer_len().await;
            if buffer_len != 1 {
                return TestResult::error(format!(
                    "Expected 1 entry in buffer, got {}",
                    buffer_len
                ));
            }

            // Property 2: No metadata file should be created (journal routing, not direct write)
            let metadata_path = temp_dir.path().join("metadata");
            let has_meta_files = if metadata_path.exists() {
                walkdir::WalkDir::new(&metadata_path)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .any(|e| e.path().extension().map_or(false, |ext| ext == "meta"))
            } else {
                false
            };
            if has_meta_files {
                return TestResult::error(
                    "Metadata file was created directly instead of using journal",
                );
            }

            // Flush to journal
            let result = buffer.force_flush().await.unwrap();

            // Property 3: Entry should be flushed to journal
            if result.entries_flushed != 1 {
                return TestResult::error(format!(
                    "Expected 1 entry flushed, got {}",
                    result.entries_flushed
                ));
            }

            // Property 4: Journal file should exist with correct content
            let journal_path = temp_dir
                .path()
                .join("metadata")
                .join("_journals")
                .join("test-instance.journal");

            if !journal_path.exists() {
                return TestResult::error("Journal file was not created");
            }

            let content = tokio::fs::read_to_string(&journal_path).await.unwrap();
            let entry: JournalEntry = match serde_json::from_str(content.trim()) {
                Ok(e) => e,
                Err(e) => {
                    return TestResult::error(format!("Failed to parse journal entry: {}", e))
                }
            };

            // Property 5: Journal entry should have correct operation type
            if is_ttl_refresh {
                if entry.operation != JournalOperation::TtlRefresh {
                    return TestResult::error(format!(
                        "Expected TtlRefresh operation, got {:?}",
                        entry.operation
                    ));
                }
                if entry.new_ttl_secs != Some(ttl_secs) {
                    return TestResult::error(format!(
                        "Expected new_ttl_secs={}, got {:?}",
                        ttl_secs, entry.new_ttl_secs
                    ));
                }
            } else {
                if entry.operation != JournalOperation::AccessUpdate {
                    return TestResult::error(format!(
                        "Expected AccessUpdate operation, got {:?}",
                        entry.operation
                    ));
                }
                if entry.access_increment != Some(access_increment as u64) {
                    return TestResult::error(format!(
                        "Expected access_increment={}, got {:?}",
                        access_increment, entry.access_increment
                    ));
                }
            }

            // Property 6: Journal entry should have correct cache key and range
            if entry.cache_key != cache_key {
                return TestResult::error(format!(
                    "Expected cache_key={}, got {}",
                    cache_key, entry.cache_key
                ));
            }
            // TTL refresh entries have range 0-0 (record_ttl_refresh doesn't take range params)
            if !is_ttl_refresh {
                if entry.range_spec.start != range_start || entry.range_spec.end != range_end {
                    return TestResult::error(format!(
                        "Expected range {}-{}, got {}-{}",
                        range_start, range_end, entry.range_spec.start, entry.range_spec.end
                    ));
                }
            } else {
                if entry.range_spec.start != 0 || entry.range_spec.end != 0 {
                    return TestResult::error(format!(
                        "Expected range 0-0 for TTL refresh, got {}-{}",
                        entry.range_spec.start, entry.range_spec.end
                    ));
                }
            }

            TestResult::passed()
        })
    }
}
