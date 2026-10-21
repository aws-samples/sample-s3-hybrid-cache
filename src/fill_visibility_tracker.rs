//! Per-instance record of streamed GET-miss fills that are committed but not yet consolidated.
//!
//! # The window this closes
//!
//! A streamed GET miss commits through `DiskCacheManager::commit_incremental_range`, which
//! publishes the `.bin` synchronously and writes the metadata as a journal line only
//! (`WriteMode::JournalOnly`). The `.meta` appears when the journal consolidator next folds that
//! line, so until then the filling proxy's own lookup finds nothing and every re-read of the object
//! it has just cached goes to S3 again. On the fleet that window was 1-8 s, phase-locked to the 5 s
//! consolidation tick.
//!
//! This tracker holds, in process memory, what the `.meta` would say for such a fill, so the
//! filling proxy can serve it at once. It adds **no** shared-storage write: the journal-only mode
//! exists to keep contended `.meta` writes off the hot path, and that is unchanged.
//!
//! # Rules, each enforced by the callers in `cache.rs` and `disk_cache.rs`
//!
//! 1. **Disk wins.** An entry is consulted only when no `.meta` exists for the key, or when the
//!    `.meta` is HEAD-only and describes the same object version (below). Whenever any other
//!    `.meta` is seen the entry is dropped (`DropReason::DiskMeta`), however new it looks, because a
//!    peer's write is the shared truth. The consolidator drops it too, right after it writes the
//!    key's `.meta` (`DropReason::Folded`).
//!
//!    **The one exception** ([`FillVisibilityTracker::lookup_over_disk_meta`]): a `.meta` with no
//!    ranges that is not write-cached, which is what a HEAD writes, may be stood in for when
//!    [`same_object_version`] holds. A client that sends HEAD before GET (the AWS CLI's `s3 cp`
//!    does, twice) otherwise hides every fill behind the HEAD's document until the fold. The
//!    document handed out takes the earlier `created_at` and `expires_at` of the two, because the
//!    fold keeps the HEAD's (`journal_consolidator.rs`, `load_or_create_metadata_with_journal_entries`
//!    loads an existing `.meta` unchanged), so rule 2 still holds against the folded `.meta`.
//! 2. **No freshness of its own.** Stored expiry is `commit time + ttl` and live TTL is judged by
//!    `check_object_expiration`'s own verdict function against `created_at = commit time`, which is
//!    earlier than the consolidation time the `.meta` will carry, so a tracker verdict is never
//!    more generous than the `.meta`'s. A `get_ttl` of zero is never recorded and never consulted.
//! 3. **Invalidation removes it, on any proxy.** Every key-invalidation function on this
//!    instance calls [`FillVisibilityTracker::invalidate`]. A peer's acknowledged write or delete
//!    cannot remove a `.bin` no `.meta` lists yet, but it replaces the key's write-order marker
//!    (`{object}.wtok`). Each entry carries the marker state its fill was published under (the
//!    fill's version witness, [`MarkerStamp`]), and every lookup re-reads the marker and drops the
//!    entry (`DropReason::Superseded`) unless it is unchanged. That is the rule the consolidator
//!    applies before it folds the fill's journal `Add`, so an entry serves only while its fill
//!    could still be folded.
//! 4. **Bounded.** At most [`MAX_ENTRIES`] entries and [`MAX_BYTES`] of estimated entry size,
//!    oldest-first, plus an age limit of `max(4 × consolidation_interval, 30 s)`. A dropped entry
//!    is a cache miss, which is exactly the state without the tracker.
//! 5. **Local only.** A peer proxy cannot see this, so a re-read routed elsewhere still waits for
//!    consolidation.
//! 6. **Names only bytes it wrote.** An entry is recorded after the `.bin` rename and the journal
//!    append, stamped with the `.bin`'s length and mtime; every lookup re-checks the stamp and drops
//!    the entry (`DropReason::RangeFileChanged`) if the file is gone or rewritten.
//!
//! Design note: the investigation record.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use crate::cache_types::{NewCacheMetadata, ObjectMetadata};

/// True when a tracked fill and a `.meta` describe the same object version: the same non-empty
/// ETag (quote-tolerant, via [`crate::revalidation::etags_equal_ignoring_quotes`]), the same
/// non-zero `content_length`, and the same effective `Last-Modified` wherever both carry one.
///
/// The ETag must be non-empty after stripping one pair of quotes BEFORE it is compared, because
/// `etags_equal_ignoring_quotes` returns true for two identical strings, including two empty ones,
/// and an empty validator is not evidence of identity.
pub(crate) fn same_object_version(tracked: &ObjectMetadata, disk: &ObjectMetadata) -> bool {
    if crate::http_proxy::strip_etag_quotes(&tracked.etag).is_empty()
        || !crate::revalidation::etags_equal_ignoring_quotes(&tracked.etag, &disk.etag)
    {
        return false;
    }
    if tracked.content_length == 0 || tracked.content_length != disk.content_length {
        return false;
    }
    match (
        tracked.effective_last_modified(),
        disk.effective_last_modified(),
    ) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}

/// Most entries held at once. At the fleet's measured window (one 5 s consolidation interval plus
/// a sub-second cycle) this covers ~800 whole-object fills per second per instance before the
/// oldest entries start being dropped early, which only shortens the benefit for them.
pub const MAX_ENTRIES: usize = 8_192;

/// Most estimated bytes held at once, across all entries. See [`estimate_entry_bytes`].
pub const MAX_BYTES: u64 = 16 * 1024 * 1024;

/// Floor of the age limit. See [`age_limit_for`].
pub const MIN_AGE_LIMIT: Duration = Duration::from_secs(30);

/// The age limit for a given consolidation interval: `max(4 × interval, 30 s)`.
///
/// A `.meta` should appear within one interval plus one cycle. 30 s on the fleet's 5 s interval is
/// about a 4x margin over the measured steady window (2.00-8.03 s) and over the post-restart first
/// cycle on the current build (4.4-4.8 s). An entry that outlives it is dropped and the key is a
/// plain miss again, never a wrong answer.
pub(crate) fn age_limit_for(consolidation_interval: Duration) -> Duration {
    consolidation_interval.saturating_mul(4).max(MIN_AGE_LIMIT)
}

/// Why an entry left the tracker. Each reason has its own counter on `/metrics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// A `.meta` for the key was seen by a lookup.
    DiskMeta,
    /// The consolidator wrote the key's `.meta`.
    Folded,
    /// This instance invalidated the key (PUT, DELETE, stale-object invalidation, eviction).
    Invalidated,
    /// The entry or byte cap was reached and this was the oldest entry.
    Cap,
    /// The entry outlived the age limit.
    Age,
    /// The committed `.bin` is missing or no longer has the recorded length and mtime.
    RangeFileChanged,
    /// The key's write-order marker no longer holds the state the fill was published under: a
    /// write or delete of the key was acknowledged on some proxy since. Counted with
    /// `Invalidated`.
    Superseded,
}

/// Length and mtime of one committed `.bin`, as seen immediately after its rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeFileStamp {
    /// Absolute path of the `.bin`.
    pub path: PathBuf,
    /// File length in bytes, as stored (compressed).
    pub len: u64,
    /// Modification time, `None` where the filesystem does not report one.
    pub modified: Option<SystemTime>,
}

impl RangeFileStamp {
    /// Stamp `path` as it is now. `None` if it cannot be stat'ed.
    pub fn of(path: &Path) -> Option<Self> {
        let meta = std::fs::metadata(path).ok()?;
        Some(Self {
            path: path.to_path_buf(),
            len: meta.len(),
            modified: meta.modified().ok(),
        })
    }

    /// True while the file on disk still matches this stamp.
    fn still_matches(&self) -> bool {
        match std::fs::metadata(&self.path) {
            Ok(meta) => meta.len() == self.len && meta.modified().ok() == self.modified,
            Err(_) => false,
        }
    }
}

/// The state of a key's write-order marker a tracked fill was published under. See rule 3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerStamp {
    /// The key's `.meta` path; the marker is its `.wtok` sibling.
    meta_path: PathBuf,
    /// The token the marker held, or `None` when no marker existed.
    token: Option<String>,
}

impl MarkerStamp {
    /// A witnessed fill's stamp: the marker held `token` when the fill published, checked under
    /// the key's slot lock.
    pub(crate) fn witnessed(meta_path: &Path, token: &str) -> Self {
        Self {
            meta_path: meta_path.to_path_buf(),
            token: Some(token.to_string()),
        }
    }

    /// The marker as it is now, for a fill published without a witness. `None` when a marker
    /// exists but cannot be read (a stamp mid-write), so nothing can be vouched for.
    pub fn of(meta_path: &Path) -> Option<Self> {
        let token = match crate::cache::read_write_marker(meta_path) {
            Some(marker) => Some(marker.token),
            None => match std::fs::symlink_metadata(crate::cache::write_marker_path(meta_path)) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                _ => return None,
            },
        };
        Some(Self {
            meta_path: meta_path.to_path_buf(),
            token,
        })
    }

    /// True while the marker still holds the recorded state: the same token, or still no
    /// marker. A marker that exists but cannot be read matches neither.
    fn still_matches(&self) -> bool {
        match &self.token {
            Some(token) => crate::cache::read_write_marker(&self.meta_path)
                .is_some_and(|marker| &marker.token == token),
            None => matches!(
                std::fs::symlink_metadata(crate::cache::write_marker_path(&self.meta_path)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound
            ),
        }
    }
}

struct Entry {
    metadata: NewCacheMetadata,
    stamps: Vec<RangeFileStamp>,
    marker: MarkerStamp,
    recorded_at: Instant,
    seq: u64,
    bytes: u64,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<String, Entry>,
    /// Insertion order as `(key, seq)`. An item whose `seq` no longer matches the live entry is a
    /// leftover from a replaced or removed entry and is skipped.
    order: VecDeque<(String, u64)>,
    next_seq: u64,
    bytes: u64,
}

/// Cumulative counters and gauges, read by the metrics collector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FillVisibilityStats {
    pub entries: u64,
    pub bytes: u64,
    pub recorded_total: u64,
    pub hits_total: u64,
    pub dropped_disk_meta_total: u64,
    pub dropped_folded_total: u64,
    pub dropped_invalidated_total: u64,
    pub dropped_cap_total: u64,
    pub dropped_age_total: u64,
    pub dropped_range_file_changed_total: u64,
}

/// See the module documentation.
pub struct FillVisibilityTracker {
    inner: Mutex<Inner>,
    max_entries: AtomicUsize,
    max_bytes: AtomicU64,
    max_age: Duration,
    recorded: AtomicU64,
    hits: AtomicU64,
    dropped_disk_meta: AtomicU64,
    dropped_folded: AtomicU64,
    dropped_invalidated: AtomicU64,
    dropped_cap: AtomicU64,
    dropped_age: AtomicU64,
    dropped_range_file_changed: AtomicU64,
}

/// Estimated resident size of one entry: the strings it owns plus a fixed overhead for the
/// struct, the map slot and the order-queue item. An estimate, used only for the byte cap.
pub(crate) fn estimate_entry_bytes(metadata: &NewCacheMetadata) -> u64 {
    const FIXED_OVERHEAD: usize = 512;
    let om = &metadata.object_metadata;
    let headers: usize = om
        .response_headers
        .iter()
        .map(|(k, v)| k.len() + v.len() + 48)
        .sum();
    let ranges: usize = metadata
        .ranges
        .iter()
        .map(|r| r.file_path.len() + 128)
        .sum();
    // The key is held three times: the map key, the metadata, and the order queue.
    let total = FIXED_OVERHEAD
        + metadata.cache_key.len() * 3
        + om.etag.len()
        + om.last_modified.len()
        + om.content_type.as_ref().map_or(0, |c| c.len())
        + headers
        + ranges;
    total as u64
}

impl FillVisibilityTracker {
    /// A tracker with the production caps and an age limit derived from `consolidation_interval`.
    pub fn new(consolidation_interval: Duration) -> Self {
        Self::with_limits(
            MAX_ENTRIES,
            MAX_BYTES,
            age_limit_for(consolidation_interval),
        )
    }

    /// A tracker with explicit limits. Production uses [`Self::new`]; tests use this to reach the
    /// caps without allocating thousands of entries.
    pub(crate) fn with_limits(max_entries: usize, max_bytes: u64, max_age: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            max_entries: AtomicUsize::new(max_entries.max(1)),
            max_bytes: AtomicU64::new(max_bytes),
            max_age,
            recorded: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            dropped_disk_meta: AtomicU64::new(0),
            dropped_folded: AtomicU64::new(0),
            dropped_invalidated: AtomicU64::new(0),
            dropped_cap: AtomicU64::new(0),
            dropped_age: AtomicU64::new(0),
            dropped_range_file_changed: AtomicU64::new(0),
        }
    }

    /// Replace the entry and byte caps. Takes effect at the next record or stats read.
    ///
    /// Production never calls this; it exists so an integration test can reach the cap with a
    /// handful of fills instead of thousands.
    pub fn set_caps(&self, max_entries: usize, max_bytes: u64) {
        self.max_entries
            .store(max_entries.max(1), Ordering::Relaxed);
        self.max_bytes.store(max_bytes, Ordering::Relaxed);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding this lock leaves only a map of hints behind; recovering the
        // guard is safe because every entry is re-validated on lookup.
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn count_drop(&self, reason: DropReason) {
        let counter = match reason {
            DropReason::DiskMeta => &self.dropped_disk_meta,
            DropReason::Folded => &self.dropped_folded,
            DropReason::Invalidated | DropReason::Superseded => &self.dropped_invalidated,
            DropReason::Cap => &self.dropped_cap,
            DropReason::Age => &self.dropped_age,
            DropReason::RangeFileChanged => &self.dropped_range_file_changed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove `key`'s entry from `inner`, keeping the byte gauge right. Returns whether one existed.
    fn remove_locked(inner: &mut Inner, key: &str) -> bool {
        match inner.entries.remove(key) {
            Some(entry) => {
                inner.bytes = inner.bytes.saturating_sub(entry.bytes);
                true
            }
            None => false,
        }
    }

    /// Drop entries from the front of the order queue while they are aged out or a cap is
    /// exceeded. Stale queue items (replaced or removed entries) are discarded as they surface.
    fn sweep_locked(&self, inner: &mut Inner) {
        let max_entries = self.max_entries.load(Ordering::Relaxed);
        let max_bytes = self.max_bytes.load(Ordering::Relaxed);
        while let Some((key, seq)) = inner.order.front().cloned() {
            let live = inner.entries.get(&key).map(|e| (e.seq, e.recorded_at));
            let (live_seq, recorded_at) = match live {
                Some(v) => v,
                None => {
                    inner.order.pop_front();
                    continue;
                }
            };
            if live_seq != seq {
                inner.order.pop_front();
                continue;
            }
            let reason = if recorded_at.elapsed() > self.max_age {
                Some(DropReason::Age)
            } else if inner.entries.len() > max_entries || inner.bytes > max_bytes {
                Some(DropReason::Cap)
            } else {
                None
            };
            match reason {
                Some(reason) => {
                    inner.order.pop_front();
                    Self::remove_locked(inner, &key);
                    self.count_drop(reason);
                }
                None => break,
            }
        }
        // Repeated re-records of the same key leave stale queue items behind the live one. Bound
        // the queue so that pattern cannot grow it without limit.
        if inner.order.len() > max_entries.saturating_mul(2) {
            let entries = &inner.entries;
            inner
                .order
                .retain(|(k, s)| entries.get(k).is_some_and(|e| e.seq == *s));
        }
    }

    /// Record a committed fill. Replaces any existing entry for the same key.
    ///
    /// The caller has already renamed the `.bin` into place and appended the journal line; `stamps`
    /// describe the `.bin` files as they are now, and `marker` the key's write-order marker as the
    /// fill was published under it (rule 3).
    pub fn record(
        &self,
        metadata: NewCacheMetadata,
        stamps: Vec<RangeFileStamp>,
        marker: MarkerStamp,
    ) {
        let bytes = estimate_entry_bytes(&metadata);
        let key = metadata.cache_key.clone();
        let mut inner = self.lock();
        Self::remove_locked(&mut inner, &key);
        let seq = inner.next_seq;
        inner.next_seq += 1;
        inner.bytes += bytes;
        inner.order.push_back((key.clone(), seq));
        inner.entries.insert(
            key,
            Entry {
                metadata,
                stamps,
                marker,
                recorded_at: Instant::now(),
                seq,
                bytes,
            },
        );
        self.recorded.fetch_add(1, Ordering::Relaxed);
        self.sweep_locked(&mut inner);
    }

    /// The entry for `key`, if one is held, is younger than the age limit, its key's write-order
    /// marker is unchanged since the fill published (rule 3), and every `.bin` it names still
    /// matches its stamp. A failing entry is dropped with its reason.
    ///
    /// The caller must already have established that no `.meta` exists for `key` (rule 1), or go
    /// through [`Self::lookup_over_disk_meta`] with the `.meta` it read.
    pub fn lookup(&self, key: &str) -> Option<NewCacheMetadata> {
        let (metadata, stamps, marker) = {
            let mut inner = self.lock();
            let aged = inner
                .entries
                .get(key)
                .map(|e| e.recorded_at.elapsed() > self.max_age)?;
            if aged {
                Self::remove_locked(&mut inner, key);
                self.count_drop(DropReason::Age);
                return None;
            }
            let entry = inner.entries.get(key)?;
            (
                entry.metadata.clone(),
                entry.stamps.clone(),
                entry.marker.clone(),
            )
        };
        // Read and stat outside the lock: on shared storage each is a network round trip.
        if !marker.still_matches() {
            self.drop_entry(key, DropReason::Superseded);
            return None;
        }
        if stamps.iter().all(RangeFileStamp::still_matches) {
            Some(metadata)
        } else {
            self.drop_entry(key, DropReason::RangeFileChanged);
            None
        }
    }

    /// True when an entry for `key` is held (aged or not). A map probe, for callers that want to
    /// skip work when nothing is tracked.
    pub fn holds(&self, key: &str) -> bool {
        self.lock().entries.contains_key(key)
    }

    /// Rule 1's exception: the entry for `key`, standing in for `disk`, a `.meta` just read for
    /// the same key.
    ///
    /// Returned only when `disk` has no ranges, is not write-cached, and [`same_object_version`]
    /// holds against the tracked fill. The returned document is the tracked one with `created_at`
    /// and `expires_at` each the earlier of the two and the `.meta`'s HEAD fields copied across,
    /// so no freshness verdict taken from it is more generous than either document's. Every other
    /// case drops the entry (`DropReason::DiskMeta`) and returns `None`: disk wins.
    ///
    /// The caller must already have excluded a zero `get_ttl`.
    pub(crate) fn lookup_over_disk_meta(
        &self,
        key: &str,
        disk: &NewCacheMetadata,
    ) -> Option<NewCacheMetadata> {
        if disk.ranges.is_empty() && !disk.object_metadata.is_write_cached {
            if let Some(mut tracked) = self.lookup(key) {
                if same_object_version(&tracked.object_metadata, &disk.object_metadata) {
                    tracked.created_at = tracked.created_at.min(disk.created_at);
                    tracked.expires_at = tracked.expires_at.min(disk.expires_at);
                    tracked.head_expires_at = disk.head_expires_at;
                    tracked.head_last_accessed = disk.head_last_accessed;
                    tracked.head_access_count = disk.head_access_count;
                    tracked.head_cached_at = disk.head_cached_at;
                    return Some(tracked);
                }
            }
        }
        self.note_disk_meta(key);
        None
    }

    /// True when a read site would hand out this key's entry right now: an entry is held, and the
    /// `.meta` at `meta_path` is either absent or HEAD-only and of the same version
    /// ([`Self::lookup_over_disk_meta`]). Reads the `.meta` only when an entry is held. An
    /// unreadable or unparseable `.meta`, or any other `.meta`, drops the entry (disk wins).
    ///
    /// Used to count the serves authorised by something other than a live-TTL verdict (Mode B,
    /// a `304`) that the tracker's record made possible. The caller must already have excluded a
    /// zero `get_ttl`.
    pub(crate) async fn vouches(&self, key: &str, meta_path: &Path) -> bool {
        if !self.holds(key) {
            return false;
        }
        match tokio::fs::read_to_string(meta_path).await {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => self.lookup(key).is_some(),
            Ok(content) => match serde_json::from_str::<NewCacheMetadata>(&content) {
                Ok(disk) => self.lookup_over_disk_meta(key, &disk).is_some(),
                Err(_) => {
                    self.note_disk_meta(key);
                    false
                }
            },
            Err(_) => {
                self.note_disk_meta(key);
                false
            }
        }
    }

    /// Remove `key`'s entry, counting it under `reason` if one existed.
    fn drop_entry(&self, key: &str, reason: DropReason) {
        let removed = {
            let mut inner = self.lock();
            Self::remove_locked(&mut inner, key)
        };
        if removed {
            self.count_drop(reason);
        }
    }

    /// This instance invalidated `key`.
    pub fn invalidate(&self, key: &str) {
        self.drop_entry(key, DropReason::Invalidated);
    }

    /// A lookup found a `.meta` for `key`.
    pub(crate) fn note_disk_meta(&self, key: &str) {
        self.drop_entry(key, DropReason::DiskMeta);
    }

    /// The consolidator wrote `key`'s `.meta`.
    pub(crate) fn note_folded(&self, key: &str) {
        self.drop_entry(key, DropReason::Folded);
    }

    /// Count one response served from an entry's record: a live-TTL `Fresh` verdict issued from it
    /// (`check_object_expiration`), or a Mode B or post-`304` serve made while [`Self::vouches`]
    /// holds (`DiskCacheManager::refresh_object_ttl`, `CacheManager::refresh_cache_ttl`).
    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Current gauges and counters. Sweeps aged entries first so `entries` does not count them.
    pub fn stats(&self) -> FillVisibilityStats {
        let (entries, bytes) = {
            let mut inner = self.lock();
            self.sweep_locked(&mut inner);
            (inner.entries.len() as u64, inner.bytes)
        };
        FillVisibilityStats {
            entries,
            bytes,
            recorded_total: self.recorded.load(Ordering::Relaxed),
            hits_total: self.hits.load(Ordering::Relaxed),
            dropped_disk_meta_total: self.dropped_disk_meta.load(Ordering::Relaxed),
            dropped_folded_total: self.dropped_folded.load(Ordering::Relaxed),
            dropped_invalidated_total: self.dropped_invalidated.load(Ordering::Relaxed),
            dropped_cap_total: self.dropped_cap.load(Ordering::Relaxed),
            dropped_age_total: self.dropped_age.load(Ordering::Relaxed),
            dropped_range_file_changed_total: self
                .dropped_range_file_changed
                .load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_types::{ObjectMetadata, RangeSpec};
    use crate::compression::CompressionAlgorithm;

    fn meta_path_for(dir: &Path, key: &str) -> PathBuf {
        dir.join(format!("{}.meta", key.replace('/', "_")))
    }

    fn entry_for(dir: &Path, key: &str) -> (NewCacheMetadata, Vec<RangeFileStamp>, MarkerStamp) {
        let bin = dir.join(format!("{}.bin", key.replace('/', "_")));
        std::fs::write(&bin, b"range-bytes").unwrap();
        let now = SystemTime::now();
        let metadata = NewCacheMetadata {
            cache_key: key.to_string(),
            object_metadata: ObjectMetadata {
                etag: "\"e\"".to_string(),
                content_length: 11,
                ..Default::default()
            },
            ranges: vec![RangeSpec::new(
                0,
                10,
                bin.file_name().unwrap().to_string_lossy().to_string(),
                CompressionAlgorithm::Lz4,
                11,
                11,
            )],
            created_at: now,
            expires_at: now + Duration::from_secs(60),
            ..Default::default()
        };
        let stamp = RangeFileStamp::of(&bin).unwrap();
        let marker = MarkerStamp::of(&meta_path_for(dir, key)).unwrap();
        (metadata, vec![stamp], marker)
    }

    #[test]
    fn age_limit_is_four_intervals_with_a_thirty_second_floor() {
        assert_eq!(
            age_limit_for(Duration::from_secs(5)),
            Duration::from_secs(30)
        );
        assert_eq!(
            age_limit_for(Duration::from_secs(1)),
            Duration::from_secs(30)
        );
        assert_eq!(
            age_limit_for(Duration::from_secs(60)),
            Duration::from_secs(240)
        );
    }

    #[test]
    fn a_recorded_entry_is_returned_until_dropped() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(4, u64::MAX, Duration::from_secs(60));
        let (m, s, w) = entry_for(dir.path(), "b/k");
        t.record(m, s, w);
        assert!(t.lookup("b/k").is_some());
        t.note_disk_meta("b/k");
        assert!(t.lookup("b/k").is_none());
        let st = t.stats();
        assert_eq!((st.entries, st.bytes), (0, 0));
        assert_eq!(st.recorded_total, 1);
        assert_eq!(st.dropped_disk_meta_total, 1);
    }

    #[test]
    fn each_drop_reason_has_its_own_counter() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(8, u64::MAX, Duration::from_secs(60));
        for k in ["b/1", "b/2", "b/3"] {
            let (m, s, w) = entry_for(dir.path(), k);
            t.record(m, s, w);
        }
        t.note_folded("b/1");
        t.invalidate("b/2");
        // Dropping an absent key counts nothing.
        t.invalidate("b/absent");
        let st = t.stats();
        assert_eq!(st.dropped_folded_total, 1);
        assert_eq!(st.dropped_invalidated_total, 1);
        assert_eq!(st.entries, 1);
    }

    #[test]
    fn the_entry_cap_drops_the_oldest_first() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(2, u64::MAX, Duration::from_secs(60));
        for k in ["b/1", "b/2", "b/3"] {
            let (m, s, w) = entry_for(dir.path(), k);
            t.record(m, s, w);
        }
        assert!(
            t.lookup("b/1").is_none(),
            "the oldest entry must be dropped"
        );
        assert!(t.lookup("b/2").is_some());
        assert!(t.lookup("b/3").is_some());
        assert_eq!(t.stats().dropped_cap_total, 1);
    }

    #[test]
    fn the_byte_cap_drops_the_oldest_first() {
        let dir = tempfile::TempDir::new().unwrap();
        let (m, _, _) = entry_for(dir.path(), "b/size");
        let one = estimate_entry_bytes(&m);
        let t = FillVisibilityTracker::with_limits(100, one * 2, Duration::from_secs(60));
        for k in ["b/1", "b/2", "b/3"] {
            let (m, s, w) = entry_for(dir.path(), k);
            t.record(m, s, w);
        }
        let st = t.stats();
        assert_eq!(st.dropped_cap_total, 1);
        assert!(
            st.bytes <= one * 2,
            "bytes {} over cap {}",
            st.bytes,
            one * 2
        );
        assert!(t.lookup("b/1").is_none());
    }

    #[test]
    fn an_aged_entry_is_a_miss_and_is_counted() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(4, u64::MAX, Duration::from_millis(1));
        let (m, s, w) = entry_for(dir.path(), "b/old");
        t.record(m, s, w);
        std::thread::sleep(Duration::from_millis(20));
        assert!(t.lookup("b/old").is_none());
        assert_eq!(t.stats().dropped_age_total, 1);
    }

    #[test]
    fn a_rewritten_or_missing_range_file_drops_the_entry() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(4, u64::MAX, Duration::from_secs(60));
        let (m, s, w) = entry_for(dir.path(), "b/gone");
        let path = s[0].path.clone();
        t.record(m, s, w);
        std::fs::remove_file(&path).unwrap();
        assert!(t.lookup("b/gone").is_none());
        let (m, s, w) = entry_for(dir.path(), "b/rewritten");
        let path = s[0].path.clone();
        t.record(m, s, w);
        std::fs::write(&path, b"a different, longer body").unwrap();
        assert!(t.lookup("b/rewritten").is_none());
        assert_eq!(t.stats().dropped_range_file_changed_total, 2);
    }

    #[test]
    fn a_replaced_removed_or_appearing_write_marker_drops_the_entry() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(8, u64::MAX, Duration::from_secs(60));
        let marker_path =
            |key: &str| crate::cache::write_marker_path(&meta_path_for(dir.path(), key));
        let write_marker = |key: &str, token: &str| {
            let doc = serde_json::to_string(&crate::cache::WriteMarker {
                token: token.to_string(),
                cache_key: key.to_string(),
            })
            .unwrap();
            std::fs::write(marker_path(key), doc).unwrap();
        };
        let witnessed = |key: &str, token: &str| {
            let (m, s, _) = entry_for(dir.path(), key);
            write_marker(key, token);
            t.record(
                m,
                s,
                MarkerStamp::witnessed(&meta_path_for(dir.path(), key), token),
            );
            assert!(
                t.lookup(key).is_some(),
                "an unchanged marker keeps the entry"
            );
        };

        // A witnessed fill, then a stamp replaces its token.
        witnessed("b/replaced", "fill-token");
        write_marker("b/replaced", "stamp-token");
        assert!(t.lookup("b/replaced").is_none());

        // A witnessed fill whose marker is removed (a guarded store's publish, the reaper).
        witnessed("b/removed", "fill-token");
        std::fs::remove_file(marker_path("b/removed")).unwrap();
        assert!(t.lookup("b/removed").is_none());

        // An unwitnessed fill recorded with no marker, then a stamp creates one.
        let (m, s, w) = entry_for(dir.path(), "b/appeared");
        t.record(m, s, w);
        assert!(t.lookup("b/appeared").is_some());
        write_marker("b/appeared", "stamp-token");
        assert!(t.lookup("b/appeared").is_none());

        // A marker that exists but cannot be read matches no recorded state.
        let (m, s, w) = entry_for(dir.path(), "b/torn");
        t.record(m, s, w);
        std::fs::write(marker_path("b/torn"), b"").unwrap();
        assert!(t.lookup("b/torn").is_none());
        assert!(
            MarkerStamp::of(&meta_path_for(dir.path(), "b/torn")).is_none(),
            "an unreadable marker vouches for nothing"
        );

        let st = t.stats();
        assert_eq!(st.dropped_invalidated_total, 4);
        assert_eq!(st.dropped_range_file_changed_total, 0);
        assert_eq!(st.entries, 0);
    }

    fn om(etag: &str, len: u64, lm: &str) -> ObjectMetadata {
        ObjectMetadata {
            etag: etag.to_string(),
            content_length: len,
            last_modified: lm.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn same_object_version_requires_etag_length_and_agreeing_last_modified() {
        let lm = "Wed, 01 Jan 2025 00:00:00 GMT";
        assert!(same_object_version(
            &om("\"e\"", 11, lm),
            &om("\"e\"", 11, lm)
        ));
        // Quote-tolerant, as the rest of the tree compares stored ETags.
        assert!(same_object_version(&om("\"e\"", 11, lm), &om("e", 11, lm)));
        // Last-Modified is compared only where both carry one.
        assert!(same_object_version(
            &om("\"e\"", 11, lm),
            &om("\"e\"", 11, "")
        ));
        assert!(!same_object_version(
            &om("\"e\"", 11, lm),
            &om("\"f\"", 11, lm)
        ));
        assert!(!same_object_version(
            &om("\"e\"", 11, lm),
            &om("\"e\"", 12, lm)
        ));
        assert!(!same_object_version(
            &om("\"e\"", 11, lm),
            &om("\"e\"", 11, "Thu, 02 Jan 2025 00:00:00 GMT")
        ));
        // An empty validator is not evidence of identity, quoted or not.
        assert!(!same_object_version(&om("", 11, lm), &om("", 11, lm)));
        assert!(!same_object_version(
            &om("\"\"", 11, lm),
            &om("\"\"", 11, lm)
        ));
        assert!(!same_object_version(
            &om("\"e\"", 0, lm),
            &om("\"e\"", 0, lm)
        ));
    }

    #[test]
    fn lookup_over_disk_meta_stands_in_only_for_a_matching_head_only_meta() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(8, u64::MAX, Duration::from_secs(60));
        let (m, s, w) = entry_for(dir.path(), "b/k");
        let earlier = m.created_at - Duration::from_secs(5);
        t.record(m.clone(), s.clone(), w.clone());

        let head_only = NewCacheMetadata {
            cache_key: "b/k".to_string(),
            object_metadata: m.object_metadata.clone(),
            created_at: earlier,
            expires_at: earlier + Duration::from_secs(60),
            head_cached_at: Some(earlier),
            ..Default::default()
        };
        let got = t
            .lookup_over_disk_meta("b/k", &head_only)
            .expect("stands in");
        assert_eq!(got.ranges, m.ranges, "the tracked ranges are handed out");
        assert_eq!(got.created_at, earlier, "the earlier created_at governs");
        assert_eq!(got.expires_at, earlier + Duration::from_secs(60));
        assert_eq!(got.head_cached_at, Some(earlier));
        assert_eq!(t.stats().entries, 1, "a match keeps the entry");

        // A range on disk wins, even of the same version.
        let mut ranged = head_only.clone();
        ranged.ranges = m.ranges.clone();
        assert!(t.lookup_over_disk_meta("b/k", &ranged).is_none());
        assert_eq!(t.stats().dropped_disk_meta_total, 1);

        // A different version wins.
        t.record(m.clone(), s.clone(), w.clone());
        let mut other = head_only.clone();
        other.object_metadata.etag = "\"other\"".to_string();
        assert!(t.lookup_over_disk_meta("b/k", &other).is_none());

        // A write-cached zero-range document is not a HEAD-only one.
        t.record(m, s, w);
        let mut staged = head_only;
        staged.object_metadata.is_write_cached = true;
        assert!(t.lookup_over_disk_meta("b/k", &staged).is_none());
        assert_eq!(t.stats().dropped_disk_meta_total, 3);
    }

    #[test]
    fn re_recording_one_key_keeps_one_entry_and_a_bounded_queue() {
        let dir = tempfile::TempDir::new().unwrap();
        let t = FillVisibilityTracker::with_limits(4, u64::MAX, Duration::from_secs(60));
        for _ in 0..100 {
            let (m, s, w) = entry_for(dir.path(), "b/same");
            t.record(m, s, w);
        }
        assert_eq!(t.stats().entries, 1);
        assert_eq!(t.stats().dropped_cap_total, 0);
        assert!(t.lock().order.len() <= 8);
    }
}

/// A peer's acknowledged write or delete of a key ends this proxy's
/// fill-visibility entry for it, although no `.meta` exists for the peer's invalidation to remove.
/// Two `CacheManager`s over one cache directory stand in for two proxies sharing a volume.
#[cfg(test)]
mod cross_proxy_tests {
    use crate::cache::zero_length_arm_orphan_tests::{
        incompressible_body, object_metadata, setup, BODY_LEN,
    };
    use crate::cache::CacheManager;
    use std::time::Duration;

    const LONG_TTL: Duration = Duration::from_secs(315_360_000);
    const STEP: Duration = Duration::from_secs(60);

    /// A streamed, witnessed GET-miss fill of `key` on `manager`, committed the way the
    /// production streaming path commits one: the `.bin` is published, the journal `Add`
    /// appended and the fill recorded in `manager`'s tracker. No `.meta` exists afterwards.
    async fn streamed_fill(manager: &CacheManager, key: &str, body: &[u8]) {
        let witness = manager.capture_fill_witness(key).await;
        let disk_cache = manager.create_configured_disk_cache_manager();
        let mut writer = disk_cache
            .begin_incremental_range_write(key, 0, BODY_LEN as u64 - 1, true)
            .await
            .expect("fixture: the streamed writer must open")
            .expect("fixture: the streamed writer is admitted");
        writer.set_fill_witness(Some(witness));
        crate::disk_cache::DiskCacheManager::write_range_chunk(&mut writer, body)
            .expect("fixture: the chunk write must succeed");
        disk_cache
            .commit_incremental_range(writer, object_metadata(BODY_LEN as u64, "\"v1\""), LONG_TTL)
            .await
            .expect("fixture: the streamed fill must commit");
    }

    /// The bytes a GET on `manager` is served from the disk tier, through the metadata read
    /// that falls back to the fill-visibility tracker when no `.meta` exists.
    async fn served(manager: &CacheManager, key: &str) -> Option<Vec<u8>> {
        let meta = manager
            .get_metadata_cached(key)
            .await
            .expect("the metadata read must not error")?;
        let disk_cache = manager.create_configured_disk_cache_manager();
        let mut bytes = Vec::new();
        for range in &meta.ranges {
            bytes.extend(disk_cache.load_range_data(range).await.ok()?);
        }
        Some(bytes)
    }

    /// Proxy A fills `key` (v1) up to its tracker record; proxy B then acknowledges `operation`
    /// on `key`; returns what A's next GET is served.
    async fn fill_then_peer_write(
        key: &str,
        operation: Option<&str>,
    ) -> (Option<Vec<u8>>, Vec<u8>) {
        let dir = tempfile::TempDir::new().unwrap();
        let a = setup(dir.path()).await;
        let b = setup(dir.path()).await;
        let v1 = incompressible_body(21);
        tokio::time::timeout(STEP, streamed_fill(&a, key, &v1))
            .await
            .expect("fixture: the fill timed out");
        assert!(
            !a.get_new_metadata_file_path(key).exists(),
            "PRECONDITION MISSING: a .meta exists, so the tracker is not what serves the fill"
        );
        assert_eq!(
            a.fill_visibility_tracker().stats().recorded_total,
            1,
            "PRECONDITION MISSING: proxy A did not record its fill"
        );
        if let Some(op) = operation {
            tokio::time::timeout(STEP, b.invalidate_cache_unified_for_operation(key, op))
                .await
                .expect("fixture: B's invalidation timed out")
                .expect("B's acknowledged-write invalidation must succeed");
            assert!(
                !b.get_new_metadata_file_path(key).exists(),
                "PRECONDITION MISSING: B's invalidation left a .meta"
            );
        }
        let got = tokio::time::timeout(STEP, served(&a, key))
            .await
            .expect("A's read timed out");
        (got, v1)
    }

    #[tokio::test]
    async fn a_peer_put_ends_this_proxys_tracked_fill() {
        let (got, v1) = fill_then_peer_write("test-bucket/case55-peer-put", Some("PUT")).await;
        assert!(
            got.as_deref() != Some(&v1[..]),
            "proxy A served the superseded object from its fill-visibility entry after proxy B \
             acknowledged a PUT of the key: served {} bytes, served == v1: true",
            got.as_ref().map_or(0, |b| b.len())
        );
    }

    #[tokio::test]
    async fn a_peer_delete_ends_this_proxys_tracked_fill() {
        let (got, v1) =
            fill_then_peer_write("test-bucket/case55-peer-delete", Some("DELETE")).await;
        assert!(
            got.as_deref() != Some(&v1[..]),
            "proxy A served the deleted object from its fill-visibility entry after proxy B \
             acknowledged a DELETE of the key: served {} bytes, served == v1: true",
            got.as_ref().map_or(0, |b| b.len())
        );
    }

    /// Control: with no intervening write the tracked fill is still a local hit.
    #[tokio::test]
    async fn with_no_intervening_write_the_tracked_fill_still_serves() {
        let (got, v1) = fill_then_peer_write("test-bucket/case55-control", None).await;
        assert_eq!(
            got.as_ref().map(|b| b.len()),
            Some(v1.len()),
            "the tracked fill must still be served when no write intervened"
        );
        assert!(
            got.as_deref() == Some(&v1[..]),
            "the tracked fill served different bytes"
        );
    }
}
