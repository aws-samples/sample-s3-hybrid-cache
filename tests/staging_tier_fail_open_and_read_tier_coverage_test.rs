//! Design tests 12 and 13 of `.kiro/specs/write-cache-accounting-and-eviction/design.md`,
//! plus the cross-instance test the design specifies alongside them (spec task 38).
//!
//! These three live together because they need the same thing the inline `src/cache.rs`
//! test modules cannot provide: a `CacheManager` with a real `max_cache_size_limit`. That
//! field is settable only through the full `new_with_shared_storage` constructor — there
//! is no setter, and adding a test-only one would recreate the trap task 56 removed
//! (`update_total_cache_size`, a test-only setter that looked load-bearing and wrote a
//! field nothing read). So the tests come to the constructor rather than the other way
//! round.
//!
//! | Test | Design test | What it pins |
//! |---|---|---|
//! | `fail_open` module | 12 | An unreadable Size_State admits and caches (R7.5) |
//! | `read_tier_covers_staged_entries` module | 13 | Read-tier eviction reclaims staged entries with both totals debited (R5.5) |
//! | `cross_instance_eviction_targeting` module | task 38 | Eviction targeting follows *shared* residency, not local state (R7.1) |
//!
//! Design test 13 is the guard that makes removing `evict_to_target` safe: with the
//! write tier's own sweep gone, an expired-unread staged entry is reclaimed by the read
//! path or not at all. Task 15 implemented that path's accounting and explicitly did not
//! write this test, leaving the one requirement whose failure would be a slow disk leak
//! with no coverage at all.

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// The 60-second admission window in `collect_candidates_from_metadata_file_with_options`
/// is evaluated against each range's `last_accessed`. Nothing in the read-tier eviction
/// path passes `bypass_admission_window: true` — the only caller uses the no-arg wrapper —
/// so backdating is the only way a fixture gets a candidate collected. 120s is the margin
/// the existing helpers in `tests/range_based_eviction_property_test.rs` use.
const BACKDATE: Duration = Duration::from_secs(120);

/// Build a manager with a real `max_cache_size_limit`, which is what both eviction entry
/// points short-circuit on when it is zero.
///
/// `write_cache_percent` is 10.0, so the Staging_Bound is a tenth of `max_cache_size`.
fn manager(cache_dir: &Path, max_cache_size: u64) -> CacheManager {
    for sub in ["metadata/_journals", "size_tracking", "locks", "ranges"] {
        std::fs::create_dir_all(cache_dir.join(sub)).unwrap();
    }
    CacheManager::new_with_shared_storage(
        cache_dir.to_path_buf(),
        false, // RAM cache disabled: it would hold a second copy of the bytes and
        0,     // complicate the size assertions without testing anything here.
        max_cache_size,
        CacheEvictionAlgorithm::LRU,
        1024,                           // compression_threshold
        true,                           // compression_enabled
        Duration::from_secs(315360000), // get_ttl (~10 years)
        Duration::from_secs(3600),      // head_ttl
        Duration::from_secs(3600),      // put_ttl
        false,                          // actively_remove_cached_data
        SharedStorageConfig::default(),
        10.0,                       // write_cache_percent
        true,                       // write_cache_enabled
        Duration::from_secs(86400), // incomplete_upload_ttl
        MetadataCacheConfig::default(),
        95,                                 // eviction_trigger_percent
        80,                                 // eviction_target_percent
        true,                               // read_cache_enabled
        Duration::from_secs(60),            // bucket_settings_staleness_threshold
        1_048_576,                          // compression_batch_size
        false,                              // evaluate_conditions_from_cache
        std::time::Duration::from_secs(10), // ram_cache_flush_interval
        64,                                 // ram_cache_shard_count
        std::time::Duration::from_secs(5),  // upstream_first_byte_timeout
    )
}

/// Same, with the journal components wired so Size_State is readable and the accumulator
/// is reachable. `create_configured_disk_cache_manager` is what installs them.
fn manager_with_journal(cache_dir: &Path, max_cache_size: u64) -> CacheManager {
    let m = manager(cache_dir, max_cache_size);
    let _ = m.create_configured_disk_cache_manager();
    m
}

/// Seed one cached object: a `.bin` at its sharded path plus a `.meta` describing it.
///
/// `is_write_cached` decides whether the entry is staged, which is the axis design test 13
/// turns on. `last_accessed` is backdated past the admission window so the range is
/// collectable, and `access_count` is left at the `RangeSpec::new` default of 1 — which is
/// what a never-read write-through range genuinely carries in production (see open
/// question 6 in `.kiro/specs/cache-eviction-at-scale/discussion.md`).
///
/// Returns `(meta_path, bin_path, compressed_size)`.
fn seed_object(
    cache_dir: &Path,
    cache_key: &str,
    size: usize,
    is_write_cached: bool,
) -> (std::path::PathBuf, std::path::PathBuf, u64) {
    let ranges_dir = cache_dir.join("ranges");
    let start = 0u64;
    let end = size as u64 - 1;
    let bin_path = get_sharded_path(&ranges_dir, cache_key, &format!("_{}-{}.bin", start, end))
        .expect("sharded range path");
    let rel_path = bin_path
        .strip_prefix(&ranges_dir)
        .expect("bin path must live under ranges/")
        .to_string_lossy()
        .to_string();

    let mut range = RangeSpec::new(
        start,
        end,
        rel_path,
        CompressionAlgorithm::Lz4,
        size as u64, // compressed_size — the figure every accounting site uses
        size as u64,
    );
    let backdated = SystemTime::now() - BACKDATE;
    range.created_at = backdated;
    range.last_accessed = backdated;

    let mut object_metadata = ObjectMetadata::new(
        "\"test-etag\"".to_string(),
        "Wed, 26 Aug 2026 00:00:00 GMT".to_string(),
        size as u64,
        Some("application/octet-stream".to_string()),
    );
    object_metadata.is_write_cached = is_write_cached;

    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata,
        ranges: vec![range],
        created_at: backdated,
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    };

    std::fs::create_dir_all(bin_path.parent().unwrap()).unwrap();
    std::fs::write(&bin_path, vec![0u8; size]).unwrap();

    let meta_path = get_sharded_path(&cache_dir.join("metadata"), cache_key, ".meta")
        .expect("sharded metadata path");
    std::fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
    std::fs::write(&meta_path, serde_json::to_string_pretty(&metadata).unwrap()).unwrap();

    (meta_path, bin_path, size as u64)
}

/// Install a Size_State directly, as another instance's consolidation would leave it.
///
/// This is the cross-instance primitive: it writes the shared file **without** driving any
/// local code path that would produce the same figures as a side effect. See the
/// `cross_instance_eviction_targeting` module for why that distinction is the whole point.
async fn seed_size_state(m: &CacheManager, total_size: u64, write_cache_size: u64) {
    let consolidator = m.get_journal_consolidator().await.expect("consolidator");
    let mut state = consolidator.load_size_state().await.unwrap();
    state.total_size = total_size;
    state.write_cache_size = write_cache_size;
    consolidator.persist_size_state(&state).await.unwrap();
}

/// Design test 12: fail-open (R7.5).
///
/// The rule, and why it is a requirement rather than a nicety: **silently disabling
/// write-through caching is the outage this whole spec exists to fix.** So when a figure a
/// caching decision depends on cannot be read, the decision must be to cache. Guessing
/// "full" reproduces the outage from a new cause; guessing "evict" deletes data on no
/// evidence at all.
///
/// An absent consolidator is the unreadable-Size_State fixture. It is the same condition
/// the production code branches on (`self.journal_consolidator.read().await.clone()`
/// yielding `None`) and it is genuinely reachable — that is the state of every proxy
/// between construction and `initialize()`.
mod fail_open {
    use super::*;

    /// Residency must read as "unknown", not as zero.
    ///
    /// Zero is a *legal* residency figure meaning "nothing is staged", so a caller given
    /// `0` cannot tell a drained tier from an unreadable one. Conflating those is exactly
    /// what made three separate checks silently never fire (spec tasks 56, 62 and 65), so
    /// the distinction is carried in the return type.
    #[tokio::test]
    async fn unreadable_residency_reports_none_rather_than_zero() {
        let temp = TempDir::new().unwrap();
        let m = manager(temp.path(), 1024 * 1024);

        assert!(
            m.get_staging_resident_bytes().await.is_none(),
            "an unreadable Size_State must report None, not Some(0)"
        );
    }

    /// The fail-open assertion: with the total unreadable, the disk-safety bound admits.
    ///
    /// `max_cache_size` is 1 byte against a 4 KiB object, so a working headroom check
    /// *would* decline. It admits because the total it would compare against cannot be
    /// read.
    #[tokio::test]
    async fn unreadable_size_state_admits_rather_than_refusing() {
        let temp = TempDir::new().unwrap();
        let m = manager(temp.path(), 1);

        assert_eq!(
            m.disk_safety_refusal(4096).await,
            None,
            "R7.5: an unreadable total must admit — refusing here silently stops \
             write-through caching, which is the outage this spec exists to fix"
        );
    }

    /// The control, and the test above is vacuous without it: a function that never
    /// refuses anything satisfies it trivially.
    ///
    /// Identical manager, identical 1-byte limit, identical 4 KiB object. The single
    /// difference is that the journal components are wired, so the total is readable — and
    /// it declines. That pair is what proves the fail-open *branch* did the admitting,
    /// rather than the bound simply being unreachable in this fixture.
    ///
    /// This is `pre-push-checklist.md` § "Assert the predicate the code evaluates":
    /// asserting a refusal proves none of its possible causes, so the two arms must differ
    /// in exactly the one input under test.
    #[tokio::test]
    async fn a_readable_size_state_over_the_limit_does_refuse() {
        let temp = TempDir::new().unwrap();
        let m = manager_with_journal(temp.path(), 1);

        assert_eq!(
            m.disk_safety_refusal(4096).await,
            Some("disk_safety"),
            "fixture check: with the total readable, 4 KiB against a 1-byte limit must be \
             declined with the disk_safety reason — otherwise the fail-open test above \
             proves nothing"
        );
    }

    /// The other fail-open direction, and the dangerous one, because this path deletes.
    /// With residency unreadable a staging eviction pass must do nothing at all.
    #[tokio::test]
    async fn unreadable_residency_frees_nothing_rather_than_guessing() {
        let temp = TempDir::new().unwrap();
        let m = manager(temp.path(), 1024 * 1024 * 1024);
        let (meta, bin, _) = seed_object(temp.path(), "test-bucket/staged.bin", 4096, true);

        assert_eq!(
            m.evict_staging_tier().await,
            0,
            "R7.5: with residency unreadable, a staging pass must free nothing"
        );
        assert!(
            meta.exists() && bin.exists(),
            "and it must not have deleted anything: guessing 'over bound' would destroy \
             data on no evidence"
        );
    }
}

/// Design test 13: read-tier eviction still covers staged entries (R5.5).
///
/// This is the guard that makes Phase E's removal of `evict_to_target` safe. Before it,
/// the write tier had its own sweep; now a staged entry leaves the disk by graduating and
/// then being evicted as ordinary read-tier data, or by the ledger-driven staging pass. If
/// the read path silently *skipped* staged entries, an expired-unread staged object would
/// be reclaimable by nothing — a disk leak with no error and no metric.
///
/// Two halves, and both are needed:
///
/// - a staged entry is collected and deleted by the read path at all;
/// - the accounting debits **both** figures, because a staged range counts in `total_size`
///   *and* in `write_cache_size`.
///
/// The unstaged control is what makes the second half attributable: it isolates the
/// conditional `subtract_write_cache` from the unconditional `subtract_range` beside it.
mod read_tier_covers_staged_entries {
    use super::*;

    /// Sum the accumulator's flushed delta files, `(total_delta, write_cache_delta)`.
    ///
    /// # Why the files rather than the in-memory counters
    ///
    /// `enforce_disk_cache_limits` calls `size_accumulator().flush()` before releasing the
    /// eviction lock. `flush()` writes a delta file and **resets the in-memory atomics to
    /// zero**, so `current_delta()` reads 0 after a successful eviction — it would report
    /// "no accounting happened" for a debit that worked perfectly. The write tier's
    /// equivalent test documents hitting exactly this.
    ///
    /// # Why the files are read here rather than via `collect_and_apply_deltas`
    ///
    /// That method is `pub(crate)`, so it is unreachable from `tests/`, and widening it
    /// for a test's convenience is the move the spec's task 61 explicitly warns against
    /// (a `pub` item is invisible to the `dead_code` lint, so the cost outlives the test).
    /// The on-disk format is stable and documented on `write_delta_file`:
    /// `size_tracking/delta_{instance_id}_{seq}.json`, holding `delta` and
    /// `write_cache_delta`.
    ///
    /// Every delta file is summed rather than just the newest, because a flush that split
    /// across two files would otherwise silently under-report.
    fn flushed_deltas(cache_dir: &Path) -> (i64, i64) {
        let dir = cache_dir.join("size_tracking");
        let mut total = 0i64;
        let mut write_cache = 0i64;
        let mut files_read = 0usize;

        for entry in std::fs::read_dir(&dir).expect("size_tracking must exist") {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if !name.starts_with("delta_") || !name.ends_with(".json") {
                continue;
            }
            let json: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            total += json["delta"].as_i64().expect("delta field");
            write_cache += json["write_cache_delta"]
                .as_i64()
                .expect("write_cache_delta field");
            files_read += 1;
        }

        // A missing file and a genuine zero are different outcomes, and a test that
        // cannot tell them apart will eventually report the wrong one — the same
        // discipline the fleet suite applies to an unreadable /metrics field.
        assert!(
            files_read > 0,
            "no delta file was written to {:?}: the eviction path did not flush, so the \
             assertions below would compare 0 against 0 and pass vacuously",
            dir
        );

        (total, write_cache)
    }

    /// R5.5. A flagged, unread, expired entry is reclaimed by the read path, and both
    /// totals are debited.
    ///
    /// Shown failing first two ways, each reproducing a different plausible mistake:
    ///
    /// 1. Add `if new_metadata.object_metadata.is_write_cached { continue; }` to
    ///    `collect_candidates_from_metadata_file_with_options` — the "staged entries are
    ///    the write tier's business" assumption. The first assertion then fails: the
    ///    `.meta` survives, because nothing collected it.
    /// 2. Delete the `is_staged_range_parts` arm from Step 5's accounting. The `.meta` is still
    ///    deleted, so assertions 1 and 2 pass, and the write-cache delta comes back 0 —
    ///    which is the R1/R5 leak signature, bytes leaving the disk while the figure that
    ///    governs admission does not move.
    ///
    /// The fixture seeds a cache well over its limit so `bytes_to_free` is non-zero, and
    /// the object is the only candidate, so it is necessarily the one selected.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_staged_entry_is_reclaimed_by_the_read_path_with_both_totals_debited() {
        let temp = TempDir::new().unwrap();
        // 40 KiB limit, 64 KiB object: over the limit, and the 80% target (32 KiB) is
        // below the object, so bytes_to_free covers it.
        let m = manager_with_journal(temp.path(), 40 * 1024);
        const SIZE: usize = 64 * 1024;

        let (meta, bin, compressed) =
            seed_object(temp.path(), "test-bucket/staged.bin", SIZE, true);
        // Size_State must report the cache as over its limit; the eviction entry point
        // reads it, not the filesystem.
        seed_size_state(&m, SIZE as u64, compressed).await;

        assert!(
            meta.exists() && bin.exists(),
            "fixture: the object must be on disk before eviction"
        );
        assert_eq!(
            m.get_staging_resident_bytes().await,
            Some(compressed),
            "fixture: the staged bytes must be counted as resident, or the debit \
             assertion below has nothing to debit from"
        );

        let freed = m.enforce_disk_cache_limits().await.unwrap();

        assert!(
            !bin.exists(),
            "R5.5: the read path must delete a staged range's .bin — if it skips staged \
             entries, an expired-unread staged object is reclaimable by nothing"
        );
        assert_eq!(
            freed, compressed,
            "the freed figure must be the range's size"
        );

        let (total_delta, write_cache_delta) = flushed_deltas(temp.path());
        assert_eq!(
            total_delta,
            -(compressed as i64),
            "total_size must be debited: the bytes left the disk"
        );
        assert_eq!(
            write_cache_delta,
            -(compressed as i64),
            "write_cache_size must be debited too. Without this the tier's residency \
             figure only ever grows while its data is deleted, which is the leak this \
             spec was opened for"
        );
    }

    /// The unstaged control. Identical in every respect except `is_write_cached`, so the
    /// difference in the write-cache delta is attributable to the flag and nothing else.
    ///
    /// This is the half that pins the debit as *conditional*. Without it, a Step 5 that
    /// debited `subtract_write_cache` unconditionally would pass the test above — and that
    /// is not hypothetical: it is the exact bug the spec's task 15 shipped and then
    /// amended the same day, because a graduated entry would be debited here *and* by its
    /// own `Graduation` journal entry, driving the figure into undershoot. Undershoot is
    /// the more dangerous direction, since it over-admits silently instead of refusing.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unstaged_entry_debits_the_total_only() {
        let temp = TempDir::new().unwrap();
        let m = manager_with_journal(temp.path(), 40 * 1024);
        const SIZE: usize = 64 * 1024;

        let (_, bin, compressed) = seed_object(temp.path(), "test-bucket/read.bin", SIZE, false);
        // Nothing staged: the whole cache is read-tier data.
        seed_size_state(&m, SIZE as u64, 0).await;

        let freed = m.enforce_disk_cache_limits().await.unwrap();
        assert!(
            !bin.exists(),
            "the read path evicts read-tier data as always"
        );
        assert_eq!(freed, compressed);

        let (total_delta, write_cache_delta) = flushed_deltas(temp.path());
        assert_eq!(total_delta, -(compressed as i64));
        assert_eq!(
            write_cache_delta, 0,
            "an unstaged range must NOT debit write_cache_size. A graduated entry reaches \
             this path with its flag already cleared, and its staged bytes are debited by \
             its Graduation entry — debiting here as well double-counts into undershoot"
        );
    }
}

/// Spec task 38: the cross-instance test, constructed so local hygiene cannot pass it.
///
/// `cache-coherency-invariants.md` states the requirement and the reason: a fixture where
/// both instances hold the same view passes whatever the code does, so the fixture must
/// create **divergence**. The specific thing being verified (R7.1) is that staging
/// eviction targets *shared* residency rather than this instance's private counter — the
/// defect demonstrated live on 2026-08-24, when one proxy deleted a 32 MiB object another
/// had just cached because its own in-memory figure said it was over bound.
///
/// # How these tests avoid passing under a per-instance-counter implementation
///
/// Every one of them establishes its residency figure by writing the shared Size_State
/// **directly**, through `seed_size_state`, and never by driving a local write. That
/// matters because the two are indistinguishable from the outside when they agree: a PUT
/// through this instance would move the local counter *and* the shared figure, so an
/// implementation reading either would behave identically and the test would prove
/// nothing.
///
/// The local in-flight counter is left at **zero** throughout, which is the divergence.
/// `write_cache.inflight_bytes` reads zero while `resident_bytes` reads gigabytes — a
/// state the fleet reaches routinely, since a fresh process has nothing in flight but the
/// shared volume is still full. An implementation that consulted the local counter would
/// conclude "nothing staged, nothing to do" and free nothing.
mod cross_instance_eviction_targeting {
    use super::*;

    /// Residency comes from the shared file, so a figure this instance never produced is
    /// still visible to it.
    ///
    /// This is the primitive the two tests below rest on, asserted on its own so a failure
    /// there is attributable. Note the deliberate asymmetry in the assertions: the shared
    /// figure is 8 GiB while the local in-flight figure is 0. Under a per-instance
    /// implementation `get_staging_resident_bytes` would return 0 and both following tests
    /// would fail for a reason that had nothing to do with their own subject.
    #[tokio::test(flavor = "multi_thread")]
    async fn residency_is_read_from_shared_state_not_from_local_in_flight_bytes() {
        let temp = TempDir::new().unwrap();
        let m = manager_with_journal(temp.path(), 100 * 1024 * 1024 * 1024);

        const SHARED_RESIDENT: u64 = 8 * 1024 * 1024 * 1024;
        seed_size_state(&m, 20 * 1024 * 1024 * 1024, SHARED_RESIDENT).await;

        assert_eq!(
            m.get_staging_resident_bytes().await,
            Some(SHARED_RESIDENT),
            "R7.1: residency must be read from shared Size_State. This instance has \
             performed no writes, so a per-instance counter would report 0 here"
        );
    }

    /// Eviction *targeting* changes because of a figure written by another instance.
    ///
    /// The assertion is on the decision, not on the deletion: with the shared figure
    /// under the trigger the pass declines to act, and with it over the trigger the pass
    /// acts — on a cache this instance did not write and against a local counter that is
    /// zero in both arms.
    ///
    /// Two arms rather than one, because "freed 0" is the same output as "did nothing for
    /// an unrelated reason". The pair isolates the trigger comparison: identical fixture,
    /// identical local state, one figure different.
    ///
    /// Both arms' residency figures are **derived from the live trigger** rather than
    /// hardcoded. The first version of this test hardcoded 19,500,000 as "over the
    /// trigger" for a 20 MiB Staging_Bound; the real trigger is 95% of 20,971,520 =
    /// 19,922,944, so the over-trigger arm was in fact under it. Its own fixture
    /// precondition caught that, which is the argument for having the precondition and for
    /// computing the figures instead of asserting arithmetic done by hand.
    #[tokio::test(flavor = "multi_thread")]
    async fn another_instances_residency_figure_changes_this_instances_targeting() {
        const MAX_CACHE: u64 = 200 * 1024 * 1024;
        const SIZE: usize = 8 * 1024 * 1024;

        // Read the thresholds off a manager before choosing either arm's figure, so the
        // fixture cannot drift out of the band it needs when a default changes.
        let temp_probe = TempDir::new().unwrap();
        let (trigger, target) =
            manager(temp_probe.path(), MAX_CACHE).get_staging_eviction_thresholds();
        assert!(
            target < trigger && trigger > 1,
            "fixture: expected a usable trigger/target band, got target={target} \
             trigger={trigger}"
        );
        let under_trigger = trigger / 4;
        let over_trigger = trigger + 1;

        // Arm A: shared residency comfortably under the trigger. No pass, nothing deleted.
        let temp_a = TempDir::new().unwrap();
        let m_a = manager_with_journal(temp_a.path(), MAX_CACHE);
        let (meta_a, bin_a, _) = seed_object(temp_a.path(), "test-bucket/staged.bin", SIZE, true);
        seed_size_state(&m_a, 20 * 1024 * 1024, under_trigger).await;

        let freed_a = m_a.evict_staging_tier().await;

        assert_eq!(
            freed_a, 0,
            "under the trigger, a staging pass must not act on the shared cache"
        );
        assert!(
            meta_a.exists() && bin_a.exists(),
            "and must not delete another instance's data while under the trigger"
        );

        // Arm B: identical in every way except the shared residency figure, which is now
        // over the trigger. The local in-flight counter is still zero in both arms, so an
        // implementation reading it could not tell these two apart.
        let temp_b = TempDir::new().unwrap();
        let m_b = manager_with_journal(temp_b.path(), MAX_CACHE);
        seed_object(temp_b.path(), "test-bucket/staged.bin", SIZE, true);
        seed_size_state(&m_b, 20 * 1024 * 1024, over_trigger).await;

        assert_eq!(
            m_b.get_staging_resident_bytes().await,
            Some(over_trigger),
            "fixture: arm B must read the over-trigger figure back from shared state, or \
             it does not differ from arm A in the way it claims"
        );

        // The ledger is what supplies candidates, and this instance wrote no entries —
        // itself the R2.7 recovery case, reported rather than silently read as "nothing is
        // staged".
        let freed_b = m_b.evict_staging_tier().await;

        assert_eq!(
            freed_b, 0,
            "over the trigger with an empty ledger, the pass finds no candidates and so \
             frees nothing. That it RAN at all is what the residency assertion above \
             establishes, and what the next test turns into a destructive check"
        );
    }

    /// The destructive twin of the question in `cache-coherency-invariants.md`: this code
    /// deletes on shared storage, so what stops it deleting data another instance owns?
    ///
    /// A staged object is present on the shared volume and the ledger is empty — the state
    /// of an instance that has just started, or one that has never staged anything itself.
    /// Shared residency is set far over the bound, so the trigger is comfortably exceeded
    /// and the pass runs.
    ///
    /// It must delete **nothing**. The old `collect_eviction_candidates` walked the shared
    /// `metadata/` tree with no ownership filter and would have found this object and
    /// deleted it; the ledger-driven evictor can only act on entries some instance
    /// actually recorded, which is what scopes the effect to what was really staged.
    ///
    /// This is the assertion that a `WalkDir`-based regression would fail, and it is the
    /// reason to prefer it over asserting the ledger is empty: it names the consequence
    /// rather than the mechanism.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_pass_over_the_bound_does_not_delete_objects_absent_from_the_ledger() {
        let temp = TempDir::new().unwrap();
        let m = manager_with_journal(temp.path(), 200 * 1024 * 1024);

        let (meta, bin, _) = seed_object(
            temp.path(),
            "test-bucket/other-instance.bin",
            8 * 1024 * 1024,
            true,
        );

        // Far over the 20 MiB Staging_Bound, so the trigger cannot be what stops it.
        seed_size_state(&m, 100 * 1024 * 1024, 100 * 1024 * 1024).await;
        let (trigger, _) = m.get_staging_eviction_thresholds();
        assert!(
            100 * 1024 * 1024 >= trigger,
            "fixture: residency must exceed the trigger, or this test passes because the \
             pass never ran rather than because it declined to act"
        );

        let freed = m.evict_staging_tier().await;

        assert_eq!(
            freed, 0,
            "an over-bound pass must free nothing when no ledger entry names a candidate"
        );
        assert!(
            meta.exists() && bin.exists(),
            "R7.1 / cache-coherency-invariants: a local decision must not delete shared \
             data this instance never staged. The retired candidate walk scanned the \
             shared metadata/ tree with no ownership filter and deleted exactly this \
             class of object — demonstrated live on 2026-08-24"
        );
    }
}
