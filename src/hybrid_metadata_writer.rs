//! Hybrid Metadata Writer Module
//!
//! Provides immediate metadata updates with journaling backup for the atomic metadata writes system.
//! Combines immediate availability with resilience through write-ahead journaling.

use crate::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use crate::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use crate::metadata_lock_manager::{MetadataLock, MetadataLockManager};
use crate::{ProxyError, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, info, warn};

/// Derive a full object's `content_length` from its single range, but ONLY when the
/// document carries no published length of its own.
///
/// Returns `true` when the length was derived, `false` when the shape did not match or
/// when the derivation was refused.
///
/// # Why this exists
///
/// Both metadata writers can be handed a range for a document they have no
/// `ObjectMetadata` for. `HybridMetadataWriter::write_immediate_only` takes no
/// `object_metadata` argument at all, and
/// `JournalConsolidator::load_or_create_metadata_with_journal_entries` falls back to
/// `ObjectMetadata::default()` when no journal entry carries one. Such a document has
/// `content_length: 0` because the length is UNKNOWN, and a single range starting at 0 is
/// the best available statement of it. Deriving it there is correct and is the case this
/// helper was written for.
///
/// # Why the `etag` check is the predicate, and what it refuses
///
/// `content_length == 0` is ambiguous: it means "unknown" on a defaulted document and
/// "this object is zero bytes" on a published one. Nothing distinguishes them by length,
/// so the discriminator is PROVENANCE — whether anything that knew the object wrote this
/// document. A defaulted `ObjectMetadata` has an empty `etag`; every publisher stores the
/// origin's `ETag` beside the length it is asserting, including a zero-byte object's. So a
/// non-empty `etag` makes `content_length: 0` a published fact, and overwriting it is not a
/// heal.
///
/// Without this check, a stale range attached to a correctly-published zero-byte object
/// produced a document that was internally CONSISTENT and described the SUPERSEDED object,
/// under the new empty object's `ETag`. Every freshness and authorisation gate then admits
/// it, which is why the length derivation is the step that made the corruption serveable
/// rather than obviously broken. `CacheManager::is_empty_object_etag_entry` detects that
/// shape on the read path; this refuses to create it.
///
/// # Why refusing leaves the range in place
///
/// The refusal skips only the derivation. The document keeps `content_length: 0` beside a
/// range, which `CacheManager::has_cached_ranges` rejects on its `content_length > 0` gate,
/// so the read forwards to S3 and the client gets the origin's bytes. Dropping the range
/// instead would discard a range that may be perfectly good whenever this predicate is
/// wrong, and would do so silently; leaving it costs a refetch and never wrong bytes.
/// `cache_validator`'s `MetadataIntegrityError::InvalidFieldValue` already reports the
/// resulting shape, so a refusal is observable rather than mute.
///
/// # One copy, deliberately
///
/// This logic was duplicated byte-identically in `add_or_update_range` and
/// `JournalConsolidator::apply_journal_entries`, which is how one of them came to be
/// guarded while the other was not. Both now call this. Do not re-inline it.
pub(crate) fn derive_full_object_content_length(metadata: &mut NewCacheMetadata) -> bool {
    if metadata.ranges.len() != 1
        || metadata.ranges[0].start != 0
        || metadata.object_metadata.content_length != 0
    {
        return false;
    }

    if !metadata.object_metadata.etag.is_empty() {
        warn!(
            "Refusing to derive content_length for a document that already published one: \
             cache_key={}, etag={}, range=0-{}. A published content_length of 0 describes a \
             zero-byte object; deriving a length from an attached range would describe a \
             different object under this object's ETag.",
            metadata.cache_key, metadata.object_metadata.etag, metadata.ranges[0].end
        );
        return false;
    }

    metadata.object_metadata.content_length = metadata.ranges[0].end + 1;
    true
}

/// Write modes for hybrid metadata writer
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteMode {
    /// Update metadata file immediately (no journal)
    Immediate,
    /// Journal-only under high contention
    JournalOnly,
    /// Both immediate + journal (default for range operations)
    Hybrid,
}

/// Size/entry-count threshold check on a key's pending journal entries.
///
/// Informational only: crossing it logs at debug level and triggers nothing. Journal
/// consolidation runs on `consolidation_interval` alone, which is why
/// `cache.shared_storage.consolidation_size_threshold` is deprecated as having no effect.
pub struct ConsolidationTrigger {
    size_threshold: u64,
    entry_count_threshold: usize,
}

impl ConsolidationTrigger {
    pub fn new(size_threshold: u64, entry_count_threshold: usize) -> Self {
        Self {
            size_threshold,
            entry_count_threshold,
        }
    }

    /// Whether the pending entries cross either threshold. No production caller since
    /// the per-fill journal re-parse that fed it was removed; consolidation is
    /// interval-driven. Kept, with its unit test, because removing the type means
    /// changing its construction in `cache.rs`, outside this change's scope.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn should_trigger(&self, journal_size: u64, entry_count: usize) -> bool {
        journal_size >= self.size_threshold || entry_count >= self.entry_count_threshold
    }
}

/// Hybrid metadata writer combining immediate updates with journaling
pub struct HybridMetadataWriter {
    cache_dir: PathBuf,
    lock_manager: Arc<MetadataLockManager>,
    journal_manager: Arc<JournalManager>,
    // Unread since the per-fill threshold check was removed; kept so the constructor
    // signature `cache.rs` calls stays unchanged (see `ConsolidationTrigger::should_trigger`).
    #[allow(dead_code)]
    consolidation_trigger: Arc<ConsolidationTrigger>,
}

impl HybridMetadataWriter {
    /// Create a new hybrid metadata writer
    pub fn new(
        cache_dir: PathBuf,
        lock_manager: Arc<MetadataLockManager>,
        journal_manager: Arc<JournalManager>,
        consolidation_trigger: Arc<ConsolidationTrigger>,
    ) -> Self {
        Self {
            cache_dir,
            lock_manager,
            journal_manager,
            consolidation_trigger,
        }
    }
    /// Write range metadata using the specified write mode
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `range_spec` - The range specification to write
    /// * `write_mode` - The write mode (Immediate, JournalOnly, or Hybrid)
    /// * `object_metadata` - Optional object metadata from S3 response (needed for JournalOnly mode
    ///   when .meta file doesn't exist yet, to preserve response_headers)
    pub async fn write_range_metadata(
        &mut self,
        cache_key: &str,
        range_spec: RangeSpec,
        write_mode: WriteMode,
        object_metadata: Option<crate::cache_types::ObjectMetadata>,
        ttl: std::time::Duration,
    ) -> Result<()> {
        self.write_range_metadata_witnessed(
            cache_key,
            range_spec,
            write_mode,
            object_metadata,
            ttl,
            None,
        )
        .await
    }

    /// [`Self::write_range_metadata`] for a GET-miss range fill carrying its version witness
    /// A journal-only `Add` records it, and the consolidator folds the `Add` only while
    /// the key's write-order marker still holds it. `None` journals no witness, as before.
    pub async fn write_range_metadata_witnessed(
        &mut self,
        cache_key: &str,
        range_spec: RangeSpec,
        write_mode: WriteMode,
        object_metadata: Option<crate::cache_types::ObjectMetadata>,
        ttl: std::time::Duration,
        fill_witness: Option<String>,
    ) -> Result<()> {
        debug!(
            "Writing range metadata: cache_key={}, range={}-{}, mode={:?}",
            cache_key, range_spec.start, range_spec.end, write_mode
        );

        match write_mode {
            WriteMode::Immediate => self.write_immediate_only(cache_key, range_spec, ttl).await,
            WriteMode::JournalOnly => {
                self.write_journal_only(cache_key, range_spec, object_metadata, ttl, fill_witness)
                    .await
            }
            WriteMode::Hybrid => self.write_hybrid(cache_key, range_spec, ttl).await,
        }
    }

    /// Write metadata immediately only (no journal)
    async fn write_immediate_only(
        &mut self,
        cache_key: &str,
        range_spec: RangeSpec,
        ttl: std::time::Duration,
    ) -> Result<()> {
        let lock = match self.lock_manager.acquire_lock(cache_key).await {
            Ok(lock) => lock,
            Err(e) => {
                error!(
                    "Failed to acquire metadata lock: cache_key={}, error={}",
                    cache_key, e
                );
                // Ensure no temporary files are left behind on lock failure
                self.cleanup_temp_files_for_cache_key(cache_key).await?;
                return Err(e);
            }
        };

        // Load existing metadata or create new
        let mut metadata = match self.load_or_create_metadata(cache_key, &lock, ttl).await {
            Ok(metadata) => metadata,
            Err(e) => {
                error!(
                    "Failed to load metadata: cache_key={}, error={}",
                    cache_key, e
                );
                // Cleanup on metadata load failure
                self.cleanup_temp_files_for_cache_key(cache_key).await?;
                return Err(e);
            }
        };

        // Add or update range
        self.add_or_update_range(&mut metadata, range_spec);

        // Write to disk with cleanup on failure
        match self.write_metadata_to_disk(&metadata, &lock).await {
            Ok(()) => {
                debug!(
                    "Successfully wrote metadata immediately: cache_key={}, ranges={}",
                    cache_key,
                    metadata.ranges.len()
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to write metadata to disk: cache_key={}, error={}",
                    cache_key, e
                );
                // Cleanup temporary files on write failure
                self.cleanup_temp_files_for_cache_key(cache_key).await?;
                Err(e)
            }
        }
    }

    /// Write to journal only (high contention fallback)
    ///
    /// # Arguments
    /// * `cache_key` - The cache key for the object
    /// * `range_spec` - The range specification to write
    /// * `object_metadata` - Optional object metadata from S3 response (needed when .meta file
    ///   doesn't exist yet, to preserve response_headers for serving cached responses)
    async fn write_journal_only(
        &mut self,
        cache_key: &str,
        range_spec: RangeSpec,
        object_metadata: Option<crate::cache_types::ObjectMetadata>,
        ttl: std::time::Duration,
        fill_witness: Option<String>,
    ) -> Result<()> {
        let journal_entry = JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: self.get_instance_id(),
            cache_key: cache_key.to_string(),
            range_spec,
            operation: JournalOperation::Add,
            range_file_path: "".to_string(), // Will be filled by consolidation
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: Some(ttl.as_secs()),
            access_increment: None,
            object_metadata, // Include object metadata for journal consolidation
            fill_witness,
        };

        match self
            .journal_manager
            .append_range_entry(cache_key, journal_entry)
            .await
        {
            Ok(()) => {
                debug!(
                    "Successfully wrote to journal only: cache_key={}",
                    cache_key
                );
            }
            Err(e) => {
                error!(
                    "Failed to write to journal: cache_key={}, error={}",
                    cache_key, e
                );
                // Journal write failure in journal-only mode is critical
                return Err(ProxyError::CacheError(format!(
                    "Journal write failed in journal-only mode: {}",
                    e
                )));
            }
        }

        // No post-append threshold check: it re-read and parsed the whole journal per
        // fill only to emit a debug line, and consolidation runs on its interval alone.
        Ok(())
    }

    /// Write both immediate and journal (hybrid mode)
    async fn write_hybrid(
        &mut self,
        cache_key: &str,
        range_spec: RangeSpec,
        ttl: std::time::Duration,
    ) -> Result<()> {
        // Try immediate write first
        match self
            .write_immediate_only(cache_key, range_spec.clone(), ttl)
            .await
        {
            Ok(()) => {
                // Immediate write succeeded, also write to journal for redundancy
                let journal_entry = JournalEntry {
                    timestamp: SystemTime::now(),
                    instance_id: self.get_instance_id(),
                    cache_key: cache_key.to_string(),
                    range_spec,
                    operation: JournalOperation::Add,
                    range_file_path: "".to_string(),
                    metadata_version: 1,
                    new_ttl_secs: None,
                    object_ttl_secs: Some(ttl.as_secs()),
                    access_increment: None,
                    object_metadata: None, // Not needed in hybrid mode - immediate write already created .meta
                    fill_witness: None,
                };

                // Journal write failure is not critical in hybrid mode since immediate write succeeded
                if let Err(e) = self
                    .journal_manager
                    .append_range_entry(cache_key, journal_entry)
                    .await
                {
                    warn!(
                        "Journal write failed in hybrid mode (immediate write succeeded): cache_key={}, error={}",
                        cache_key, e
                    );
                    // Continue - immediate write succeeded, so operation is successful
                }

                debug!(
                    "Successfully wrote metadata in hybrid mode: cache_key={}",
                    cache_key
                );

                Ok(())
            }
            Err(e) => {
                warn!(
                    "Immediate write failed in hybrid mode, falling back to journal-only: cache_key={}, error={}",
                    cache_key, e
                );

                // Fall back to journal-only with proper error handling
                // Note: In hybrid fallback, we don't have object_metadata since immediate write failed
                match self
                    .write_journal_only(cache_key, range_spec, None, ttl, None)
                    .await
                {
                    Ok(()) => {
                        info!(
                            "Successfully fell back to journal-only mode: cache_key={}",
                            cache_key
                        );
                        Ok(())
                    }
                    Err(journal_err) => {
                        error!(
                            "Both immediate and journal writes failed in hybrid mode: cache_key={}, immediate_error={}, journal_error={}",
                            cache_key, e, journal_err
                        );
                        // Cleanup any temporary files from failed operations
                        self.cleanup_temp_files_for_cache_key(cache_key).await?;
                        Err(ProxyError::CacheError(format!(
                            "Hybrid write failed: immediate={}, journal={}",
                            e, journal_err
                        )))
                    }
                }
            }
        }
    }

    /// Load existing metadata or create new metadata structure
    async fn load_or_create_metadata(
        &self,
        cache_key: &str,
        _lock: &MetadataLock,
        ttl: std::time::Duration,
    ) -> Result<NewCacheMetadata> {
        let metadata_path = self.get_metadata_file_path(cache_key)?;

        if metadata_path.exists() {
            // Load existing metadata
            let content = tokio::fs::read_to_string(&metadata_path)
                .await
                .map_err(|e| {
                    ProxyError::CacheError(format!("Failed to read metadata file: {}", e))
                })?;

            serde_json::from_str(&content).map_err(|e| {
                ProxyError::CacheError(format!("Failed to parse metadata file: {}", e))
            })
        } else {
            // Create new metadata
            let now = SystemTime::now();
            Ok(NewCacheMetadata {
                cache_key: cache_key.to_string(),
                object_metadata: ObjectMetadata::default(),
                ranges: Vec::new(),
                created_at: now,
                expires_at: now + ttl,
                compression_info: crate::cache_types::CompressionInfo::default(),
                ..Default::default()
            })
        }
    }

    /// Add or update range in metadata
    fn add_or_update_range(&self, metadata: &mut NewCacheMetadata, range_spec: RangeSpec) {
        // Check if range already exists (same start and end)
        if let Some(existing_range) = metadata
            .ranges
            .iter_mut()
            .find(|r| r.start == range_spec.start && r.end == range_spec.end)
        {
            // Update existing range
            *existing_range = range_spec;
            debug!(
                "Updated existing range: cache_key={}, range={}-{}",
                metadata.cache_key, existing_range.start, existing_range.end
            );
        } else {
            // Add new range
            debug!(
                "Adding new range: cache_key={}, range={}-{}",
                metadata.cache_key, range_spec.start, range_spec.end
            );
            metadata.ranges.push(range_spec);
        }

        // Update object metadata if this is a full object.
        //
        // Guarded rather than unconditional: see `derive_full_object_content_length`. The
        // only production route to this function is the orphan sweep's
        // `add_range_to_metadata` (via `WriteMode::Hybrid`), which already declines a range
        // a published `content_length` cannot contain — so this is defence in depth here,
        // and load-bearing at the consolidator's copy.
        derive_full_object_content_length(metadata);
    }

    /// Write metadata to disk atomically with enhanced error handling
    async fn write_metadata_to_disk(
        &self,
        metadata: &NewCacheMetadata,
        _lock: &MetadataLock,
    ) -> Result<()> {
        let metadata_path = self.get_metadata_file_path(&metadata.cache_key)?;

        // Ensure parent directory exists
        if let Some(parent) = metadata_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!(
                    "Failed to create metadata directory: cache_key={}, path={:?}, error={}",
                    metadata.cache_key, parent, e
                );
                return Err(ProxyError::CacheError(format!(
                    "Failed to create metadata directory: {}",
                    e
                )));
            }
        }

        // Serialize metadata
        let json_content = serde_json::to_string_pretty(metadata).map_err(|e| {
            error!(
                "Failed to serialize metadata: cache_key={}, error={}",
                metadata.cache_key, e
            );
            ProxyError::CacheError(format!("Failed to serialize metadata: {}", e))
        })?;

        // Atomic write: write to temp file then rename
        // Use instance-specific tmp file to avoid race conditions on shared storage
        // Format: {key}.meta.tmp.{hostname}.{pid}
        //
        // Built through the shared constructor rather than inline, because
        // `cleanup_temp_files_for_cache_key` now removes exactly this path and nothing
        // else. Two independent copies of the format string would let the cleanup miss
        // its own leftover — the failure this shape exists to avoid — so the writer and
        // its reaper share one definition.
        let temp_path = crate::cache::instance_scoped_meta_tmp_path(&metadata_path);

        // Write to temporary file
        // Synced before the rename publishes it; see `disk_cache::write_synced`.
        if let Err(e) = crate::disk_cache::write_synced(&temp_path, &json_content) {
            error!(
                "Failed to write metadata temp file: cache_key={}, temp_path={:?}, error={}",
                metadata.cache_key, temp_path, e
            );
            // Clean up temp file on write failure
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ProxyError::CacheError(format!(
                "Failed to write metadata temp file: {}",
                e
            )));
        }

        // Atomic rename
        if let Err(e) = tokio::fs::rename(&temp_path, &metadata_path).await {
            error!(
                "Failed to rename metadata file: cache_key={}, temp_path={:?}, final_path={:?}, error={}",
                metadata.cache_key, temp_path, metadata_path, e
            );
            // Clean up temp file on rename failure
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ProxyError::CacheError(format!(
                "Failed to rename metadata file: {}",
                e
            )));
        }

        debug!(
            "Successfully wrote metadata to disk: cache_key={}, path={:?}",
            metadata.cache_key, metadata_path
        );

        Ok(())
    }

    /// Remove **this instance's own** leftover metadata temp file for `cache_key`.
    ///
    /// Called only from this module's own write-failure arms, for the key whose write
    /// just failed. The single file that can legitimately be removed is therefore the
    /// one `write_metadata_to_disk` wrote moments earlier, at
    /// `cache::instance_scoped_meta_tmp_path` — `{key}.meta.tmp.{host}.{pid}` for *this*
    /// host and *this* pid. That path is constructed, not matched, so no other file in
    /// the shard directory is a candidate.
    ///
    /// # What this used to do, and why a local write failure must not reach any of it
    ///
    /// It removed three populations, two of which belong to other instances. On shared
    /// storage the deletion is invisible to whoever loses the file, which is the
    /// corollary in the cache coherency invariants: a local decision
    /// taken from local state can delete another instance's data because the disk is
    /// shared, and the deleting instance never learns it did.
    ///
    /// - It scanned the shard directory and deleted every entry matching
    ///   `starts_with(file_stem) && contains(".meta.tmp.")`. Requires a peer publishing
    ///   the same key concurrently, and then unlinks that peer's in-flight
    ///   `{key}.meta.tmp.{their_host}.{their_pid}` — after which the peer's `rename`
    ///   fails, or, if its write had completed, its publish is lost. The instance-scoped
    ///   name carries the host and pid precisely so this is avoidable.
    /// - It deleted the fixed-name `{key}.meta.tmp`. This writer never produces that
    ///   shape, so the deletion could only ever hit somebody else's: live producers
    ///   remain in `cache.rs` and `disk_cache.rs`. The fixed name identifies neither
    ///   host nor process, so it carries no liveness input at all and is
    ///   indistinguishable from a peer's publish in flight right now. No instance can
    ///   soundly reap it, and this one has no reason to try.
    /// - It scanned `cache_dir/ranges` **flat** for `{sanitized_key}*.tmp`. Range files
    ///   live at `ranges/{bucket}/{XX}/{YYY}/`, so the loop matched nothing in any
    ///   production layout — its own comment said "simplified - in production this would
    ///   use proper sharding". It is deleted rather than repointed at the sharded layout:
    ///   this writer creates no range temp files, and `{key}_{start}-{end}.bin.tmp`
    ///   carries no host or pid either, so correcting the path would convert a loop that
    ///   matches nothing into a live cross-instance deletion of a shape nobody can judge.
    ///   `{key}_{start}-{end}.bin.tmp` accordingly still has no reaper; that gap is
    ///   `META-TMP-ORPHANS-UNREAPED`'s, and an acknowledged gap is better than a reaper
    ///   that deletes live data.
    ///
    /// # Own host, own pid — and why not own host, any pid
    ///
    /// `ORPHAN-RECLAIM-DESIGN` recommends that a *sweep* may reap its own host's files
    /// whose pid is not the current pid, since a pid from a previous boot of the same
    /// host is dead. That rule is sound for a sweep and is not needed here, and it is
    /// not free: it assumes one proxy process per host per cache volume, which nothing
    /// in the code enforces. This call site needs no liveness inference whatsoever,
    /// because it is cleaning up after its own write rather than sweeping, so it takes
    /// the strictly narrower rule. A previous boot's leftover for this key is left to
    /// whatever sweep is eventually wired.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.
    async fn cleanup_temp_files_for_cache_key(&self, cache_key: &str) -> Result<()> {
        debug!(
            "Starting temp file cleanup for cache key: cache_key={}",
            cache_key
        );

        let mut cleanup_count = 0;
        let mut cleanup_errors = Vec::new();

        let metadata_path = self.get_metadata_file_path(cache_key)?;

        // The one file this instance wrote and may remove. Same constructor
        // `write_metadata_to_disk` publishes through, so the two cannot drift apart.
        let own_temp_path = crate::cache::instance_scoped_meta_tmp_path(&metadata_path);
        if own_temp_path.exists() {
            match tokio::fs::remove_file(&own_temp_path).await {
                Ok(()) => {
                    cleanup_count += 1;
                    debug!(
                        "Cleaned up this instance's metadata temp file: cache_key={}, path={:?}",
                        cache_key, own_temp_path
                    );
                }
                Err(e) => {
                    let error_msg = format!(
                        "Failed to remove this instance's metadata temp file: path={:?}, error={}",
                        own_temp_path, e
                    );
                    warn!("{}", error_msg);
                    cleanup_errors.push(error_msg);
                }
            }
        }

        if cleanup_count > 0 {
            info!(
                "Temp file cleanup completed: cache_key={}, files_cleaned={}, errors={}",
                cache_key,
                cleanup_count,
                cleanup_errors.len()
            );
        }

        // Don't fail the operation if cleanup has errors - log them but continue
        if !cleanup_errors.is_empty() {
            warn!(
                "Temp file cleanup completed with errors: cache_key={}, errors={:?}",
                cache_key, cleanup_errors
            );
        }

        Ok(())
    }

    /// Get metadata file path for a cache key.
    ///
    /// Returns `Err(ProxyError::CacheError)` if the cache key is malformed
    /// (e.g., missing bucket/object separator, or contains path-traversing
    /// segments). Callers in request-handling paths should propagate via `?`;
    /// callers in background loops should log at WARN level and skip the entry.
    fn get_metadata_file_path(&self, cache_key: &str) -> Result<PathBuf> {
        let base_dir = self.cache_dir.join("metadata");

        // The sharded construction, same as `DiskCacheManager::get_new_metadata_file_path`.
        // Name that method, not its type: `DiskCacheManager` also carries
        // `get_legacy_flat_metadata_file_path`, which is the pre-sharding FLAT path and the
        // only non-sharded `.meta` construction left in the tree.
        crate::disk_cache::get_sharded_path(&base_dir, cache_key, ".meta")
    }

    /// Get instance ID for journal entries
    fn get_instance_id(&self) -> String {
        format!(
            "{}:{}",
            hostname::get().unwrap_or_default().to_string_lossy(),
            std::process::id()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_write_mode_enum() {
        assert_eq!(WriteMode::Immediate, WriteMode::Immediate);
        assert_eq!(WriteMode::JournalOnly, WriteMode::JournalOnly);
        assert_eq!(WriteMode::Hybrid, WriteMode::Hybrid);
        assert_ne!(WriteMode::Immediate, WriteMode::Hybrid);
    }

    #[test]
    fn test_consolidation_trigger() {
        let trigger = ConsolidationTrigger::new(1024, 10);

        // Should not trigger below thresholds
        assert!(!trigger.should_trigger(512, 5));

        // Should trigger when size threshold exceeded
        assert!(trigger.should_trigger(1024, 5));

        // Should trigger when entry count threshold exceeded
        assert!(trigger.should_trigger(512, 10));

        // Should trigger when both thresholds exceeded
        assert!(trigger.should_trigger(1024, 10));
    }

    #[tokio::test]
    async fn test_hybrid_metadata_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let consolidation_trigger = Arc::new(ConsolidationTrigger::new(1024, 10));

        let writer = HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            consolidation_trigger,
        );

        assert_eq!(writer.cache_dir, temp_dir.path());
    }

    /// Fixture for the three `cleanup_temp_files_for_cache_key` assertions below.
    fn cleanup_fixture(temp_dir: &TempDir) -> HybridMetadataWriter {
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            Arc::new(ConsolidationTrigger::new(1024, 10)),
        )
    }

    /// A peer's temp path for the same key: same shard directory, same file stem, a
    /// different host and pid. Built by hand rather than through
    /// `instance_scoped_meta_tmp_path`, because that helper can only ever name *this*
    /// instance and the point is that another one exists.
    fn peer_temp_path(metadata_path: &std::path::Path) -> PathBuf {
        metadata_path.with_extension("meta.tmp.peer-host-172-31-37-46.4242")
    }

    /// Assertion 1 (the defect, red before the fix): a peer's IN-FLIGHT temp file for
    /// the same key survives a local cleanup.
    ///
    /// The pre-fix loop matched `starts_with(file_stem) && contains(".meta.tmp.")` and
    /// deleted every match, so a write failure on this instance unlinked a peer's
    /// in-flight `{key}.meta.tmp.{their_host}.{their_pid}`, after which the peer's
    /// `rename` fails or its completed publish is lost. The instance-scoped name carries
    /// the host and the pid precisely so this is avoidable.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.
    #[tokio::test]
    async fn cleanup_leaves_a_peers_in_flight_temp_file_alone() {
        let temp_dir = TempDir::new().unwrap();
        let writer = cleanup_fixture(&temp_dir);
        let cache_key = "test-bucket/contended-object";

        let metadata_path = writer.get_metadata_file_path(cache_key).unwrap();
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        let peers = peer_temp_path(&metadata_path);
        std::fs::write(&peers, b"{\"peer\":\"mid-publish\"}").unwrap();

        writer
            .cleanup_temp_files_for_cache_key(cache_key)
            .await
            .unwrap();

        assert!(
            peers.exists(),
            "a local write failure deleted a PEER's in-flight metadata temp file \
             ({:?}). The peer's rename then fails, or its publish is lost, and it never \
             learns — the local-decision / shared-effect corollary in \
             cache-coherency-invariants.md. Only this instance's own \
             {{key}}.meta.tmp.{{host}}.{{pid}} may be removed here.",
            peers
        );
    }

    /// Assertion 2 (the positive control): this instance's own leftover temp file is
    /// still cleaned up.
    ///
    /// The narrowing must not turn the cleanup off. If this reds while assertion 1 stays
    /// green the constructed path no longer matches what the writer publishes through,
    /// which is a distinct signature from the defect itself — and is exactly why
    /// `write_metadata_to_disk` was moved onto the same constructor.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.
    #[tokio::test]
    async fn cleanup_still_removes_this_instances_own_temp_file() {
        let temp_dir = TempDir::new().unwrap();
        let writer = cleanup_fixture(&temp_dir);
        let cache_key = "test-bucket/own-leftover";

        let metadata_path = writer.get_metadata_file_path(cache_key).unwrap();
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
        let own = crate::cache::instance_scoped_meta_tmp_path(&metadata_path);
        std::fs::write(&own, b"{\"ours\":\"failed mid-write\"}").unwrap();

        writer
            .cleanup_temp_files_for_cache_key(cache_key)
            .await
            .unwrap();

        assert!(
            !own.exists(),
            "POSITIVE CONTROL FAILED: this instance's own leftover temp file ({:?}) \
             survived its own cleanup, so the write-failure arms now leak a file per \
             failure. The path is constructed rather than matched, so this fails when \
             the cleanup and `write_metadata_to_disk` stop agreeing on the shape.",
            own
        );
    }

    /// Assertion 3 (the cross-instance case), constructed so local hygiene cannot pass
    /// it: this instance and a peer both have a publish in flight for the SAME key at
    /// the same moment, and this instance's write is the one that failed.
    ///
    /// Both peer artefacts are planted by direct mutation of the shared tree — the way
    /// another instance's write appears — with no local code path driven for either.
    /// The fixed-name `{key}.meta.tmp` is included because this writer never produces
    /// that shape, so any deletion of it is necessarily somebody else's file; it names
    /// neither host nor process, carries no liveness input at all, and is therefore
    /// indistinguishable from a peer's publish in flight right now.
    ///
    /// Anti-vacuity: this instance's own file must be gone in the same run. Without
    /// that, a cleanup that returned early — or one whose constructed path stopped
    /// matching — would pass on the two survival assertions while doing nothing.
    ///
    /// Spec: cache-eviction-at-scale § 16.6.
    #[tokio::test]
    async fn a_local_write_failure_deletes_only_its_own_file_while_a_peer_publishes() {
        let temp_dir = TempDir::new().unwrap();
        let writer = cleanup_fixture(&temp_dir);
        let cache_key = "test-bucket/simultaneous-publish";

        let metadata_path = writer.get_metadata_file_path(cache_key).unwrap();
        std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();

        let own = crate::cache::instance_scoped_meta_tmp_path(&metadata_path);
        let peers = peer_temp_path(&metadata_path);
        let fixed = metadata_path.with_extension("meta.tmp");
        std::fs::write(&own, b"{\"ours\":\"failed mid-write\"}").unwrap();
        std::fs::write(&peers, b"{\"peer\":\"mid-publish\"}").unwrap();
        std::fs::write(&fixed, b"{\"someone\":\"mid-publish\"}").unwrap();

        writer
            .cleanup_temp_files_for_cache_key(cache_key)
            .await
            .unwrap();

        assert!(
            !own.exists(),
            "ANTI-VACUITY: this instance's own temp file ({:?}) survived, so the \
             cleanup did nothing and the two survival assertions below prove nothing",
            own
        );
        assert!(
            peers.exists(),
            "a peer's in-flight temp file ({:?}) was deleted by this instance's \
             write-failure cleanup, while both were publishing the same key",
            peers
        );
        assert!(
            fixed.exists(),
            "the fixed-name {{key}}.meta.tmp ({:?}) was deleted. This writer never \
             produces that shape, so the file can only be another writer's; it names \
             neither host nor pid, so no instance can tell it apart from a publish in \
             flight, and none may reap it",
            fixed
        );
    }

    /// The reaper and the writer must build the same string, and this asserts it from
    /// the writer's real output rather than from two copies of a format literal.
    ///
    /// `write_metadata_to_disk` renames its temp file away on success, so the shape is
    /// not observable after a successful write. What is observable, and is what the
    /// cleanup depends on, is that the path the cleanup constructs is the
    /// instance-scoped one for this host and pid.
    #[tokio::test]
    async fn the_cleanup_path_is_this_hosts_and_this_pids_temp_file() {
        let temp_dir = TempDir::new().unwrap();
        let writer = cleanup_fixture(&temp_dir);
        let metadata_path = writer
            .get_metadata_file_path("test-bucket/shape-check")
            .unwrap();

        let own = crate::cache::instance_scoped_meta_tmp_path(&metadata_path);
        let name = own.file_name().unwrap().to_str().unwrap();
        let expected_suffix = format!(
            ".meta.tmp.{}.{}",
            gethostname::gethostname().to_string_lossy(),
            std::process::id()
        );

        assert!(
            name.ends_with(&expected_suffix),
            "the temp path the cleanup targets ({}) does not end in this instance's own \
             suffix ({}), so it neither removes this instance's leftover nor stays clear \
             of a peer's",
            name,
            expected_suffix
        );
        assert_eq!(
            own.parent(),
            metadata_path.parent(),
            "the temp file must sit beside its .meta, or the rename crosses directories"
        );
    }

    #[tokio::test]
    async fn test_get_metadata_file_path_rejects_malformed() {
        // Validates: Requirements 3.3, 5.6
        let temp_dir = TempDir::new().unwrap();
        let lock_manager = Arc::new(MetadataLockManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(30),
            3,
        ));
        let journal_manager = Arc::new(JournalManager::new(
            temp_dir.path().to_path_buf(),
            "test-instance".to_string(),
        ));
        let consolidation_trigger = Arc::new(ConsolidationTrigger::new(1024, 10));

        let writer = HybridMetadataWriter::new(
            temp_dir.path().to_path_buf(),
            lock_manager,
            journal_manager,
            consolidation_trigger,
        );

        let result = writer.get_metadata_file_path("noslash");
        assert!(
            result.is_err(),
            "Malformed cache key must return Err, got Ok"
        );
    }
}
