//! Hybrid Metadata Writer Module
//!
//! Journals each range fill as an `Add` entry; the journal consolidator folds it into the
//! key's `.meta`.
use crate::cache_types::{NewCacheMetadata, RangeSpec};
use crate::journal_manager::{JournalEntry, JournalManager, JournalOperation};
use crate::{ProxyError, Result};
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, warn};
/// Derive a full object's `content_length` from its single range, but ONLY when the
/// document carries no published length of its own.
///
/// Returns `true` when the length was derived, `false` when the shape did not match or
/// when the derivation was refused.
///
/// # Why this exists
///
/// The consolidator can be handed a range for a document it has no `ObjectMetadata` for:
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
/// The refusal logs a warning (see below), so it is observable rather than mute.
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
    /// Journal the range fill as an `Add`, for the consolidator to fold
    JournalOnly,
}
/// Journals range fills for the consolidator
pub struct HybridMetadataWriter {
    journal_manager: Arc<JournalManager>,
}
impl HybridMetadataWriter {
    /// Create a new hybrid metadata writer
    pub fn new(journal_manager: Arc<JournalManager>) -> Self {
        Self { journal_manager }
    }
    /// [`Self::write_range_metadata_witnessed`] for a GET-miss range fill carrying its version witness
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
            WriteMode::JournalOnly => {
                self.write_journal_only(cache_key, range_spec, object_metadata, ttl, fill_witness)
                    .await
            }
        }
    }

    /// Journal the range fill as an `Add`
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

    /// Get instance ID for journal entries
    fn get_instance_id(&self) -> String {
        format!(
            "{}:{}",
            hostname::get().unwrap_or_default().to_string_lossy(),
            std::process::id()
        )
    }
}
