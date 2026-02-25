//! Journal Manager Module
//!
//! Provides write-ahead journaling for metadata resilience in the atomic metadata writes system.
//! Supports per-instance journal files co-located with metadata files and integration with
//! multipart upload cleanup.

use crate::cache_types::RangeSpec;
use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Journal entry for write-ahead logging of metadata operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JournalEntry {
    pub timestamp: SystemTime,
    pub instance_id: String,
    pub cache_key: String,
    pub range_spec: RangeSpec,
    pub operation: JournalOperation,
    pub range_file_path: String,
    pub metadata_version: u64, // For conflict resolution
    /// New TTL in seconds (for TtlRefresh operation)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_ttl_secs: Option<u64>,
    /// Object-level TTL in seconds (for Add operations, used by journal consolidation to set expires_at)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_ttl_secs: Option<u64>,
    /// Access count increment (for AccessUpdate operation)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_increment: Option<u64>,
    /// Object metadata from S3 response (for Add operations when .meta doesn't exist yet)
    /// Contains response_headers needed for serving cached responses with correct headers
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<crate::cache_types::ObjectMetadata>,
    /// Whether the metadata was already written to .meta file before this journal entry was created.
    /// - `true`: Range was written to .meta immediately (hybrid mode success) - consolidation should NOT count size
    /// - `false`: Range was NOT written to .meta (journal-only mode) - consolidation SHOULD count size
    /// This flag solves the size tracking problem where HybridMetadataWriter writes to .meta immediately,
    /// causing consolidation to see "range already exists" and calculate size_delta = 0.
    #[serde(default)]
    pub metadata_written: bool,
}

/// Types of journal operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JournalOperation {
    /// Add a new range to the cache
    Add,
    /// Update an existing range
    Update,
    /// Remove a range from the cache
    Remove,
    /// Refresh TTL for a cached range (cache-hit operation)
    TtlRefresh,
    /// Update access count and last_accessed timestamp (cache-hit operation)
    AccessUpdate,
}

/// Journal manager for write-ahead logging of metadata operations
pub struct JournalManager {
    cache_dir: PathBuf,
    instance_id: String,
    cleanup_integration: Option<Arc<MultipartCleanupIntegration>>,
    /// Mutex to serialize journal append operations within this instance.
    /// Prevents concurrent appends from overwriting each other (Bug 4 fix).
    append_mutex: Mutex<()>,
}

/// Integration with multipart upload cleanup system
pub struct MultipartCleanupIntegration {
    cache_dir: PathBuf,
}

impl MultipartCleanupIntegration {
    /// Create a new multipart cleanup integration
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    /// Clean up journal entries for a specific cache key during multipart upload cleanup
    pub async fn cleanup_journal_entries_for_cache_key(&self, cache_key: &str) -> Result<u64> {
        debug!(
            "Starting journal cleanup for multipart upload: cache_key={}",
            cache_key
        );

        let mut removed_count = 0;
        let journals_dir = self.cache_dir.join("metadata").join("_journals");

        if !journals_dir.exists() {
            debug!(
                "No journal directory exists during cleanup: cache_key={}",
                cache_key
            );
            return Ok(0);
        }

        // Find all per-instance journal files in the flat _journals directory
        let entries = match std::fs::read_dir(&journals_dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    "Failed to read journal directory during cleanup: cache_key={}, error={}",
                    cache_key, e
                );
                return Ok(0);
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!(
                        "Failed to read journal directory entry: cache_key={}, error={}",
                        cache_key, e
                    );
                    continue;
                }
            };

            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            // Check if this is a per-instance journal file
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.ends_with(".journal") {
                    // Remove journal entries for this cache key from this instance journal file
                    match self
                        .remove_entries_from_journal_file(&path, cache_key)
                        .await
                    {
                        Ok(count) => {
                            removed_count += count;
                            if count > 0 {
                                debug!(
                                    "Removed {} journal entries from instance journal: cache_key={}, file={:?}",
                                    count, cache_key, path
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to remove journal entries from instance journal: cache_key={}, file={:?}, error={}",
                                cache_key, path, e
                            );
                        }
                    }
                }
            }
        }

        info!(
            "Completed journal cleanup for multipart upload: cache_key={}, removed_entries={}",
            cache_key, removed_count
        );

        Ok(removed_count)
    }

    /// Remove entries for a specific cache key from a journal file
    async fn remove_entries_from_journal_file(
        &self,
        journal_path: &Path,
        cache_key: &str,
    ) -> Result<u64> {
        // Read existing entries
        let existing_entries = if journal_path.exists() {
            match tokio::fs::read_to_string(journal_path).await {
                Ok(content) => {
                    let mut entries = Vec::new();
                    for line in content.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<JournalEntry>(line) {
                            Ok(entry) => entries.push(entry),
                            Err(e) => {
                                // Parse errors can occur during concurrent writes on shared storage
                                debug!(
                                    "Failed to parse journal entry during cleanup: file={:?}, line={}, error={}",
                                    journal_path, line, e
                                );
                            }
                        }
                    }
                    entries
                }
                Err(e) => {
                    // Stale file handle and other transient errors are expected on shared storage
                    debug!(
                        "Failed to read journal file during cleanup: file={:?}, error={}",
                        journal_path, e
                    );
                    return Ok(0);
                }
            }
        } else {
            return Ok(0);
        };

        // Filter out entries for the specified cache key
        let original_count = existing_entries.len();
        let filtered_entries: Vec<JournalEntry> = existing_entries
            .into_iter()
            .filter(|entry| entry.cache_key != cache_key)
            .collect();
        let removed_count = original_count - filtered_entries.len();

        if removed_count > 0 {
            // Write back the filtered entries
            if filtered_entries.is_empty() {
                // Remove the file if no entries remain
                if let Err(e) = tokio::fs::remove_file(journal_path).await {
                    warn!(
                        "Failed to remove empty journal file: file={:?}, error={}",
                        journal_path, e
                    );
                }
            } else {
                // Write back remaining entries
                let mut content = String::new();
                for entry in &filtered_entries {
                    match serde_json::to_string(entry) {
                        Ok(json) => {
                            content.push_str(&json);
                            content.push('\n');
                        }
                        Err(e) => {
                            error!(
                                "Failed to serialize journal entry during cleanup: entry={:?}, error={}",
                                entry, e
                            );
                        }
                    }
                }

                if let Err(e) = tokio::fs::write(journal_path, content).await {
                    warn!(
                        "Failed to write filtered journal file: file={:?}, error={}",
                        journal_path, e
                    );
                }
            }
        }

        Ok(removed_count as u64)
    }
}

impl JournalManager {
    /// Create a new journal manager
    pub fn new(cache_dir: PathBuf, instance_id: String) -> Self {
        let cleanup_integration = Some(Arc::new(MultipartCleanupIntegration::new(
            cache_dir.clone(),
        )));

        Self {
            cache_dir,
            instance_id,
            cleanup_integration,
            append_mutex: Mutex::new(()),
        }
    }

    /// Set multipart cleanup integration
    pub fn set_cleanup_integration(&mut self, integration: Arc<MultipartCleanupIntegration>) {
        self.cleanup_integration = Some(integration);
    }

    /// Get journal file path for this instance (single per-instance journal)
    ///
    /// All journal entries from this instance go to a single file:
    /// `metadata/_journals/{instance_id}.journal`
    ///
    /// This matches the format used by CacheHitUpdateBuffer and simplifies
    /// journal consolidation by avoiding per-object journal files.
    pub fn get_journal_path(&self, _cache_key: &str) -> Result<PathBuf> {
        let journal_path = self
            .cache_dir
            .join("metadata")
            .join("_journals")
            .join(format!("{}.journal", self.instance_id));

        debug!(
            "Journal path constructed: instance_id={}, path={:?}",
            self.instance_id, journal_path
        );

        Ok(journal_path)
    }

    /// Append a journal entry atomically with enhanced error handling.
    ///
    /// Uses a mutex to serialize appends within this instance (Bug 4 fix).
    /// Uses non-blocking file lock to coordinate with cleanup operations.
    /// If lock is busy (cleanup in progress), creates a fresh journal file
    /// to avoid blocking cache writes (Bug 5 fix).
    pub async fn append_range_entry(&self, cache_key: &str, entry: JournalEntry) -> Result<()> {
        // Create parent directories if they don't exist
        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        if let Err(e) = tokio::fs::create_dir_all(&journals_dir).await {
            error!(
                "Failed to create journal directory: cache_key={}, path={:?}, error={}",
                cache_key, journals_dir, e
            );
            return Err(ProxyError::CacheError(format!(
                "Failed to create journal directory: {}",
                e
            )));
        }

        // Serialize the entry before acquiring locks (minimize lock hold time)
        let json_entry = serde_json::to_string(&entry).map_err(|e| {
            error!(
                "Failed to serialize journal entry: cache_key={}, error={}",
                cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize journal entry: {}", e))
        })?;

        // Acquire mutex to serialize journal appends within this instance.
        // This prevents the lost update problem where concurrent appends
        // read the same content and overwrite each other's entries.
        let _guard = self.append_mutex.lock().await;

        // Primary journal path
        let primary_journal_path = journals_dir.join(format!("{}.journal", self.instance_id));
        let lock_path = primary_journal_path.with_extension("journal.lock");

        // Try to acquire file lock (non-blocking)
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to open journal lock file: {}", e))
            })?;

        use fs2::FileExt;
        let got_lock = lock_file.try_lock_exclusive().is_ok();

        let journal_path = if got_lock {
            // Got lock - use primary journal file
            primary_journal_path
        } else {
            // Lock busy (cleanup in progress) - create fresh journal file
            // Use timestamp to ensure uniqueness
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let fresh_path =
                journals_dir.join(format!("{}:{}.journal", self.instance_id, timestamp));
            debug!(
                "Journal lock busy, using fresh journal: cache_key={}, path={:?}",
                cache_key, fresh_path
            );
            fresh_path
        };

        // For fresh journal files, just append directly (no read-modify-write needed)
        // For primary journal with lock, do read-modify-write atomically
        if got_lock {
            // Append to journal file atomically (read-modify-write with lock held)
            let temp_path = journal_path.with_extension("journal.tmp");

            // Read existing content if file exists
            let existing_content = if journal_path.exists() {
                match tokio::fs::read_to_string(&journal_path).await {
                    Ok(content) => content,
                    Err(e) => {
                        warn!(
                            "Failed to read existing journal file, starting fresh: cache_key={}, path={:?}, error={}",
                            cache_key, journal_path, e
                        );
                        String::new()
                    }
                }
            } else {
                String::new()
            };

            // Write existing content + new entry to temp file
            let new_content = format!("{}{}\n", existing_content, json_entry);

            if let Err(e) = tokio::fs::write(&temp_path, &new_content).await {
                error!(
                    "Failed to write journal temp file: cache_key={}, temp_path={:?}, error={}",
                    cache_key, temp_path, e
                );
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(ProxyError::CacheError(format!(
                    "Failed to write journal temp file: {}",
                    e
                )));
            }

            // Atomic rename
            if let Err(e) = tokio::fs::rename(&temp_path, &journal_path).await {
                error!(
                    "Failed to rename journal file: cache_key={}, temp_path={:?}, final_path={:?}, error={}",
                    cache_key, temp_path, journal_path, e
                );
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(ProxyError::CacheError(format!(
                    "Failed to rename journal file: {}",
                    e
                )));
            }
        } else {
            // Fresh journal file - just write the single entry
            let content = format!("{}\n", json_entry);
            if let Err(e) = tokio::fs::write(&journal_path, &content).await {
                error!(
                    "Failed to write fresh journal file: cache_key={}, path={:?}, error={}",
                    cache_key, journal_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to write fresh journal file: {}",
                    e
                )));
            }
        }

        debug!(
            "Appended journal entry: cache_key={}, operation={:?}, path={:?}, fresh={}",
            cache_key, entry.operation, journal_path, !got_lock
        );

        Ok(())
    }

    /// Append multiple journal entries for a single cache key in one file operation.
    /// Serializes all entries, acquires the append mutex once, and writes them
    /// in a single read-modify-write cycle.
    pub async fn append_range_entries_batch(
        &self,
        cache_key: &str,
        entries: Vec<JournalEntry>,
    ) -> Result<()> {
        // No-op for empty entries
        if entries.is_empty() {
            return Ok(());
        }

        // Create parent directories if they don't exist
        let journals_dir = self.cache_dir.join("metadata").join("_journals");
        if let Err(e) = tokio::fs::create_dir_all(&journals_dir).await {
            error!(
                "Failed to create journal directory: cache_key={}, path={:?}, error={}",
                cache_key, journals_dir, e
            );
            return Err(ProxyError::CacheError(format!(
                "Failed to create journal directory: {}",
                e
            )));
        }

        // Serialize all entries to JSON lines BEFORE acquiring any locks (minimize lock hold time)
        let mut serialized_lines = String::new();
        for entry in &entries {
            let json_entry = serde_json::to_string(entry).map_err(|e| {
                error!(
                    "Failed to serialize journal entry: cache_key={}, error={}",
                    cache_key, e
                );
                ProxyError::CacheError(format!("Failed to serialize journal entry: {}", e))
            })?;
            serialized_lines.push_str(&json_entry);
            serialized_lines.push('\n');
        }

        // Acquire mutex to serialize journal appends within this instance.
        // This prevents the lost update problem where concurrent appends
        // read the same content and overwrite each other's entries.
        let _guard = self.append_mutex.lock().await;

        // Primary journal path
        let primary_journal_path = journals_dir.join(format!("{}.journal", self.instance_id));
        let lock_path = primary_journal_path.with_extension("journal.lock");

        // Try to acquire file lock (non-blocking)
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                ProxyError::CacheError(format!("Failed to open journal lock file: {}", e))
            })?;

        use fs2::FileExt;
        let got_lock = lock_file.try_lock_exclusive().is_ok();

        let journal_path = if got_lock {
            // Got lock - use primary journal file
            primary_journal_path
        } else {
            // Lock busy (cleanup in progress) - create fresh journal file
            // Use timestamp to ensure uniqueness
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let fresh_path =
                journals_dir.join(format!("{}:{}.journal", self.instance_id, timestamp));
            debug!(
                "Journal lock busy, using fresh journal for batch: cache_key={}, path={:?}, entry_count={}",
                cache_key, fresh_path, entries.len()
            );
            fresh_path
        };

        // For fresh journal files, just write directly (no read-modify-write needed)
        // For primary journal with lock, do read-modify-write atomically
        if got_lock {
            // Append to journal file atomically (read-modify-write with lock held)
            let temp_path = journal_path.with_extension("journal.tmp");

            // Read existing content if file exists
            let existing_content = if journal_path.exists() {
                match tokio::fs::read_to_string(&journal_path).await {
                    Ok(content) => content,
                    Err(e) => {
                        warn!(
                            "Failed to read existing journal file, starting fresh: cache_key={}, path={:?}, error={}",
                            cache_key, journal_path, e
                        );
                        String::new()
                    }
                }
            } else {
                String::new()
            };

            // Write existing content + all new entries to temp file
            let new_content = format!("{}{}", existing_content, serialized_lines);

            if let Err(e) = tokio::fs::write(&temp_path, &new_content).await {
                error!(
                    "Failed to write journal temp file: cache_key={}, temp_path={:?}, error={}",
                    cache_key, temp_path, e
                );
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(ProxyError::CacheError(format!(
                    "Failed to write journal temp file: {}",
                    e
                )));
            }

            // Atomic rename
            if let Err(e) = tokio::fs::rename(&temp_path, &journal_path).await {
                error!(
                    "Failed to rename journal file: cache_key={}, temp_path={:?}, final_path={:?}, error={}",
                    cache_key, temp_path, journal_path, e
                );
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(ProxyError::CacheError(format!(
                    "Failed to rename journal file: {}",
                    e
                )));
            }
        } else {
            // Fresh journal file - write all entries
            if let Err(e) = tokio::fs::write(&journal_path, &serialized_lines).await {
                error!(
                    "Failed to write fresh journal file: cache_key={}, path={:?}, error={}",
                    cache_key, journal_path, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to write fresh journal file: {}",
                    e
                )));
            }
        }

        debug!(
            "Appended batch journal entries: cache_key={}, count={}, path={:?}, fresh={}",
            cache_key, entries.len(), journal_path, !got_lock
        );

        Ok(())
    }

    /// Get all pending journal entries for a cache key
    pub async fn get_pending_entries(&self, cache_key: &str) -> Result<Vec<JournalEntry>> {
        let journal_path = self.get_journal_path(cache_key)?;

        if !journal_path.exists() {
            return Ok(Vec::new());
        }

        let content = tokio::fs::read_to_string(&journal_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to read journal file: {}", e)))?;

        let mut entries = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<JournalEntry>(line) {
                Ok(entry) => {
                    if entry.cache_key == cache_key {
                        entries.push(entry);
                    }
                }
                Err(e) => {
                    // Parse errors can occur during concurrent writes on shared storage
                    debug!(
                        "Failed to parse journal entry: cache_key={}, line={}, error={}",
                        cache_key, line, e
                    );
                }
            }
        }

        debug!(
            "Retrieved {} pending journal entries: cache_key={}, path={:?}",
            entries.len(),
            cache_key,
            journal_path
        );

        Ok(entries)
    }

    /// Find all per-instance journal files that may contain entries for a cache key
    ///
    /// Since we now use per-instance journals (not per-object), this returns all
    /// instance journal files in the _journals directory. The caller must filter
    /// entries by cache_key when reading.
    pub async fn find_all_journal_files(&self, _cache_key: &str) -> Result<Vec<PathBuf>> {
        let journals_dir = self.cache_dir.join("metadata").join("_journals");

        if !journals_dir.exists() {
            return Ok(Vec::new());
        }

        let mut journal_files = Vec::new();
        let entries = std::fs::read_dir(&journals_dir).map_err(|e| {
            ProxyError::CacheError(format!("Failed to read journal directory: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                ProxyError::CacheError(format!("Failed to read directory entry: {}", e))
            })?;

            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                // Per-instance journals are named {instance_id}.journal
                if file_name.ends_with(".journal") {
                    journal_files.push(path);
                }
            }
        }

        debug!(
            "Found {} per-instance journal files: files={:?}",
            journal_files.len(),
            journal_files
        );

        Ok(journal_files)
    }

    /// Remove specific journal entries from the journal file
    pub async fn remove_entries(
        &self,
        cache_key: &str,
        entries_to_remove: &[JournalEntry],
    ) -> Result<()> {
        let journal_path = self.get_journal_path(cache_key)?;

        if !journal_path.exists() {
            return Ok(());
        }

        // Read existing entries
        let content = tokio::fs::read_to_string(&journal_path)
            .await
            .map_err(|e| ProxyError::CacheError(format!("Failed to read journal file: {}", e)))?;

        let mut existing_entries = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<JournalEntry>(line) {
                Ok(entry) => existing_entries.push(entry),
                Err(e) => {
                    // Parse errors can occur during concurrent writes on shared storage
                    debug!(
                        "Failed to parse journal entry during removal: cache_key={}, line={}, error={}",
                        cache_key, line, e
                    );
                }
            }
        }

        // Filter out entries to remove
        let original_count = existing_entries.len();
        let filtered_entries: Vec<JournalEntry> = existing_entries
            .into_iter()
            .filter(|entry| !entries_to_remove.contains(entry))
            .collect();
        let removed_count = original_count - filtered_entries.len();

        if removed_count > 0 {
            if filtered_entries.is_empty() {
                // Remove the file if no entries remain
                tokio::fs::remove_file(&journal_path).await.map_err(|e| {
                    ProxyError::CacheError(format!("Failed to remove empty journal file: {}", e))
                })?;
                debug!(
                    "Removed empty journal file: cache_key={}, path={:?}",
                    cache_key, journal_path
                );
            } else {
                // Write back remaining entries
                let mut new_content = String::new();
                for entry in &filtered_entries {
                    match serde_json::to_string(entry) {
                        Ok(json) => {
                            new_content.push_str(&json);
                            new_content.push('\n');
                        }
                        Err(e) => {
                            error!(
                                "Failed to serialize journal entry during removal: entry={:?}, error={}",
                                entry, e
                            );
                        }
                    }
                }

                tokio::fs::write(&journal_path, new_content)
                    .await
                    .map_err(|e| {
                        ProxyError::CacheError(format!(
                            "Failed to write filtered journal file: {}",
                            e
                        ))
                    })?;

                debug!(
                    "Updated journal file after removal: cache_key={}, removed={}, remaining={}, path={:?}",
                    cache_key, removed_count, filtered_entries.len(), journal_path
                );
            }
        }

        Ok(())
    }

    /// Clean up journal entries for a multipart upload
    pub async fn cleanup_for_multipart(&self, cache_key: &str) -> Result<()> {
        if let Some(cleanup_integration) = &self.cleanup_integration {
            cleanup_integration
                .cleanup_journal_entries_for_cache_key(cache_key)
                .await?;
        } else {
            warn!(
                "No cleanup integration available for journal cleanup: cache_key={}",
                cache_key
            );
        }
        Ok(())
    }

    /// Get all journal entries from all instances for a cache key
    ///
    /// Reads from per-instance journals: `metadata/_journals/{instance_id}.journal`
    /// Both HybridMetadataWriter and CacheHitUpdateBuffer now use this format.
    pub async fn get_all_entries_for_cache_key(
        &self,
        cache_key: &str,
    ) -> Result<Vec<JournalEntry>> {
        let mut all_entries = Vec::new();

        // Read from all per-instance journal files
        let journal_files = self.find_all_journal_files(cache_key).await?;
        for journal_file in &journal_files {
            match tokio::fs::read_to_string(journal_file).await {
                Ok(content) => {
                    for line in content.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<JournalEntry>(line) {
                            Ok(entry) => {
                                if entry.cache_key == cache_key {
                                    all_entries.push(entry);
                                }
                            }
                            Err(e) => {
                                // Parse errors can occur during concurrent writes on shared storage
                                debug!(
                                    "Failed to parse journal entry: file={:?}, error={}",
                                    journal_file, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    // Stale file handle and other transient errors are expected on shared storage
                    debug!(
                        "Failed to read journal file: file={:?}, error={}",
                        journal_file, e
                    );
                }
            }
        }

        // Sort entries by timestamp for proper ordering
        all_entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        debug!(
            "Retrieved {} journal entries for cache_key={} from {} journal files",
            all_entries.len(),
            cache_key,
            journal_files.len()
        );

        Ok(all_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::CompressionAlgorithm;
    use tempfile::TempDir;

    fn create_test_range_spec() -> RangeSpec {
        let now = SystemTime::now();
        RangeSpec {
            start: 0,
            end: 8388607,
            file_path: "test_range_0-8388607.bin".to_string(),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: 8388608,
            uncompressed_size: 8388608,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        }
    }

    #[test]
    fn test_journal_entry_serialization() {
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/test-object".to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "test_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };

        // Test serialization
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.is_empty());

        // Test deserialization
        let deserialized: JournalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.cache_key, entry.cache_key);
        assert_eq!(deserialized.operation, entry.operation);
        assert_eq!(deserialized.metadata_version, entry.metadata_version);
    }

    #[test]
    fn test_journal_entry_serialization_with_ttl_refresh() {
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/test-object".to_string(),
            range_spec,
            operation: JournalOperation::TtlRefresh,
            range_file_path: String::new(),
            metadata_version: 0,
            new_ttl_secs: Some(7200),
            object_ttl_secs: None,
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };

        // Test serialization
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("TtlRefresh"));
        assert!(json.contains("7200"));

        // Test deserialization
        let deserialized: JournalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.operation, JournalOperation::TtlRefresh);
        assert_eq!(deserialized.new_ttl_secs, Some(7200));
        assert_eq!(deserialized.access_increment, None);
    }

    #[test]
    fn test_journal_entry_serialization_with_access_update() {
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/test-object".to_string(),
            range_spec,
            operation: JournalOperation::AccessUpdate,
            range_file_path: String::new(),
            metadata_version: 0,
            new_ttl_secs: None,
            object_ttl_secs: None,
            access_increment: Some(5),
            object_metadata: None,
            metadata_written: false,
        };

        // Test serialization
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("AccessUpdate"));
        assert!(json.contains("5"));

        // Test deserialization
        let deserialized: JournalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.operation, JournalOperation::AccessUpdate);
        assert_eq!(deserialized.new_ttl_secs, None);
        assert_eq!(deserialized.access_increment, Some(5));
    }

    #[tokio::test]
    async fn test_journal_manager_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let journal_manager =
            JournalManager::new(temp_dir.path().to_path_buf(), "test-instance".to_string());

        let cache_key = "test-bucket/test-object";
        let range_spec = create_test_range_spec();
        let entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "test_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };

        // Test append
        journal_manager
            .append_range_entry(cache_key, entry.clone())
            .await
            .unwrap();

        // Test get pending entries
        let entries = journal_manager
            .get_pending_entries(cache_key)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cache_key, cache_key);
        assert_eq!(entries[0].operation, JournalOperation::Add);

        // Test find all journal files
        let journal_files = journal_manager
            .find_all_journal_files(cache_key)
            .await
            .unwrap();
        assert_eq!(journal_files.len(), 1);

        // Test remove entries
        journal_manager
            .remove_entries(cache_key, &entries)
            .await
            .unwrap();

        let remaining_entries = journal_manager
            .get_pending_entries(cache_key)
            .await
            .unwrap();
        assert_eq!(remaining_entries.len(), 0);
    }

    #[tokio::test]
    async fn test_multipart_cleanup_integration() {
        let temp_dir = TempDir::new().unwrap();
        let cleanup_integration = MultipartCleanupIntegration::new(temp_dir.path().to_path_buf());

        let cache_key = "test-bucket/test-object";
        let mut journal_manager =
            JournalManager::new(temp_dir.path().to_path_buf(), "test-instance".to_string());
        journal_manager.set_cleanup_integration(Arc::new(cleanup_integration));

        // Add some journal entries
        let range_spec = create_test_range_spec();
        let entry1 = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: cache_key.to_string(),
            range_spec: range_spec.clone(),
            operation: JournalOperation::Add,
            range_file_path: "test_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };

        let entry2 = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "test-instance".to_string(),
            cache_key: "test-bucket/other-object".to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "other_range_0-8388607.bin".to_string(),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(3600),
            access_increment: None,
            object_metadata: None,
            metadata_written: false,
        };

        journal_manager
            .append_range_entry(cache_key, entry1)
            .await
            .unwrap();
        journal_manager
            .append_range_entry("test-bucket/other-object", entry2)
            .await
            .unwrap();

        // Test cleanup for multipart
        journal_manager
            .cleanup_for_multipart(cache_key)
            .await
            .unwrap();

        // Verify that entries for cache_key are removed but others remain
        let remaining_entries = journal_manager
            .get_pending_entries(cache_key)
            .await
            .unwrap();
        assert_eq!(remaining_entries.len(), 0);

        let other_entries = journal_manager
            .get_pending_entries("test-bucket/other-object")
            .await
            .unwrap();
        assert_eq!(other_entries.len(), 1);
    }

    // Property-based tests using quickcheck
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    /// **Feature: journal-based-metadata-updates, Property 4: Journal Entry Serialization Round-Trip**
    /// *For any* valid JournalEntry (including new TtlRefresh and AccessUpdate operations),
    /// serializing to JSON and deserializing back shall produce an equivalent entry.
    /// **Validates: Requirements 1.4**
    #[quickcheck]
    fn prop_journal_entry_serialization_round_trip(
        instance_id_seed: u8,
        cache_key_seed: u8,
        operation_type: u8,
        range_start: u64,
        range_end_offset: u64,
        new_ttl_secs: Option<u64>,
        access_increment: Option<u64>,
    ) -> TestResult {
        // Generate valid range (end must be >= start)
        let range_end = range_start.saturating_add(range_end_offset);

        // Generate operation type (0-4 for Add, Update, Remove, TtlRefresh, AccessUpdate)
        let operation = match operation_type % 5 {
            0 => JournalOperation::Add,
            1 => JournalOperation::Update,
            2 => JournalOperation::Remove,
            3 => JournalOperation::TtlRefresh,
            4 => JournalOperation::AccessUpdate,
            _ => unreachable!(),
        };

        // For TtlRefresh, ensure new_ttl_secs is set; for AccessUpdate, ensure access_increment is set
        let (final_ttl, final_increment) = match operation {
            JournalOperation::TtlRefresh => (Some(new_ttl_secs.unwrap_or(3600)), None),
            JournalOperation::AccessUpdate => (None, Some(access_increment.unwrap_or(1))),
            _ => (None, None),
        };

        let now = SystemTime::now();
        let range_spec = RangeSpec {
            start: range_start,
            end: range_end,
            file_path: format!("test_range_{}-{}.bin", range_start, range_end),
            compression_algorithm: CompressionAlgorithm::Lz4,
            compressed_size: range_end.saturating_sub(range_start).saturating_add(1),
            uncompressed_size: range_end.saturating_sub(range_start).saturating_add(1),
            created_at: now,
            last_accessed: now,
            access_count: 1,
            frequency_score: 1,
        };

        let entry = JournalEntry {
            timestamp: now,
            instance_id: format!("instance-{}", instance_id_seed),
            cache_key: format!("bucket-{}/object-{}", cache_key_seed, cache_key_seed),
            range_spec,
            operation: operation.clone(),
            range_file_path: format!("test_range_{}-{}.bin", range_start, range_end),
            metadata_version: 1,
            new_ttl_secs: final_ttl,
            object_ttl_secs: match operation {
                JournalOperation::Add => Some(3600),
                _ => None,
            },
            access_increment: final_increment,
            object_metadata: None,
            metadata_written: false,
        };

        // Serialize to JSON
        let json = match serde_json::to_string(&entry) {
            Ok(j) => j,
            Err(_) => return TestResult::failed(),
        };

        // Deserialize back
        let deserialized: JournalEntry = match serde_json::from_str(&json) {
            Ok(e) => e,
            Err(_) => return TestResult::failed(),
        };

        // Verify equality of key fields
        if deserialized.instance_id != entry.instance_id {
            return TestResult::failed();
        }
        if deserialized.cache_key != entry.cache_key {
            return TestResult::failed();
        }
        if deserialized.operation != entry.operation {
            return TestResult::failed();
        }
        if deserialized.range_spec.start != entry.range_spec.start {
            return TestResult::failed();
        }
        if deserialized.range_spec.end != entry.range_spec.end {
            return TestResult::failed();
        }
        if deserialized.new_ttl_secs != entry.new_ttl_secs {
            return TestResult::failed();
        }
        if deserialized.access_increment != entry.access_increment {
            return TestResult::failed();
        }

        TestResult::passed()
    }
}
