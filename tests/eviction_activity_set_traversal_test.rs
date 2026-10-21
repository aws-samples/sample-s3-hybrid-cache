//! The eviction pass reads the journal set ONCE, not once per candidate object.
//!
//! # The figure asserted, and why it is a count rather than a duration
//!
//! `cache.eviction_active_key_journal_reads_total` counts journal files read by the cross-instance
//! activity predicate. The defect was that `CacheManager::is_cache_entry_active` ran inside the
//! executor's per-object body, so a pass evicting K objects traversed `metadata/_journals/` K times,
//! re-parsed every line K times, and discarded K-1 of the answers.
//!
//! The obvious figures beside this one are all wrong to assert on, and one of them is wrong in a way
//! that looks right:
//!
//! - **Pass duration / `eviction_last_result.duration_ms` / the four shed rates.** These *do* include
//!   the scan cost — it is inside the measured interval by construction — so a slow pass is visible
//!   in them today. That is exactly what makes them useless here: on a tempdir fixture the duration
//!   is dominated by unlinks and lock acquisition, it moves under tree sharing, and it would pass
//!   whether or not the traversal was hoisted. Asserting on them would be the predicate substitution
//!   this project has recorded six times.
//! - **The call count of `is_cache_entry_active`.** Wrong in *both* directions: an implementation that
//!   still calls it per object but memoises inside would drop traversals while the call count held
//!   steady, and a thin wrapper would drop the call count while still traversing. The cost is the
//!   traversal, so the traversal is what is counted.
//!
//! # Why the fixture precondition is ASSERTED and not assumed
//!
//! With one candidate object, "one traversal per pass" and "one traversal per object" are the same
//! number and this test would be satisfied by construction — passing identically against the defect.
//! So the object count is asserted from the executor's own path counters before the traversal count
//! is read. A fixture that stops producing multiple candidates fails loudly here rather than going
//! quietly vacuous, which is the failure mode two groups in this repo's fleet suite sat in for
//! months.
//!
//! # Why this is its own test binary with exactly ONE eviction-driving test
//!
//! The counter is a process-global `AtomicU64` and libtest runs a binary's tests as parallel threads
//! in one process. A second pass running concurrently in this binary would add its own journal reads
//! to the delta measured here, so the safety half lives in
//! `tests/eviction_activity_set_safety_test.rs` — a separate binary, hence a separate process — and
//! asserts on per-tempdir filesystem state rather than on any global counter. This mirrors
//! `tests/eviction_pass_witness_test.rs`, which is one test per binary for the same reason.
//!
//! Spec: cache-eviction-at-scale § 4.5. Predicate provenance: `Req 19.2` is
//! the review fixes feature.
use s3_proxy::cache::{collect_eviction_executor_gauges, CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::Result;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// `ram_cache_flush_interval` for the fixture. The activity window is `2×` this and the set's
/// refresh cadence is half of it, so a pass completing in well under 5 s performs exactly one build.
const FLUSH_INTERVAL: Duration = Duration::from_secs(10);

fn create_test_cache_manager(temp_dir: &TempDir, max_cache_size: u64) -> CacheManager {
    CacheManager::new_with_shared_storage(
        temp_dir.path().to_path_buf(),
        false, // RAM cache disabled
        0,     // max_ram_cache_size
        max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,                           // compression_threshold
        true,                           // compression_enabled
        Duration::from_secs(315360000), // GET TTL (~10 years)
        Duration::from_secs(3600),      // HEAD TTL
        Duration::from_secs(3600),      // PUT TTL
        false,                          // actively_remove_cached_data
        SharedStorageConfig::default(),
        10.0,                       // write_cache_percent
        false,                      // write_cache_enabled
        Duration::from_secs(86400), // incomplete_upload_ttl
        MetadataCacheConfig::default(),
        95,                      // eviction_trigger_percent
        80,                      // eviction_target_percent
        true,                    // read_cache_enabled
        Duration::from_secs(60), // bucket_settings_staleness_threshold
        1_048_576,               // compression_batch_size
        false,                   // evaluate_conditions_from_cache
        FLUSH_INTERVAL,          // ram_cache_flush_interval
        64,                      // ram_cache_shard_count
        Duration::from_secs(5),  // upstream_first_byte_timeout
    )
}

async fn store_test_range(
    cache_manager: &CacheManager,
    cache_key: &str,
    data_size: usize,
    created_at: SystemTime,
    last_accessed: SystemTime,
) -> Result<()> {
    let (start, end) = (0u64, data_size as u64 - 1);
    let data = vec![0u8; data_size];
    let ranges_base_dir = cache_manager.get_cache_dir().join("ranges");
    let suffix = format!("_{}-{}.bin", start, end);
    let range_path = get_sharded_path(&ranges_base_dir, cache_key, &suffix)
        .expect("fixture: sharded range path");
    let relative_range_path = range_path
        .strip_prefix(&ranges_base_dir)
        .unwrap_or(&range_path)
        .to_string_lossy()
        .to_string();
    let mut range_spec = RangeSpec::new(
        start,
        end,
        relative_range_path,
        CompressionAlgorithm::Lz4,
        data_size as u64,
        data_size as u64,
    );
    range_spec.created_at = created_at;
    range_spec.last_accessed = last_accessed;
    range_spec.access_count = 10;
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::new(
            "test-etag".to_string(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            end - start + 1,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![range_spec],
        created_at,
        expires_at: SystemTime::now() + Duration::from_secs(3600),
        compression_info: Default::default(),
        ..Default::default()
    };
    std::fs::create_dir_all(range_path.parent().unwrap())?;
    std::fs::write(&range_path, &data)?;
    let metadata_path = cache_manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_path.parent().unwrap())?;
    std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;
    Ok(())
}

/// Write one journal file holding `line_count` `AccessUpdate` entries for `cache_key`.
///
/// Several lines rather than one so the file is a realistic traversal target: the per-object form
/// re-parsed every line of it for every candidate.
fn seed_peer_journal(cache_dir: &std::path::Path, cache_key: &str, line_count: usize) -> u64 {
    let journals_dir = cache_dir.join("metadata").join("_journals");
    std::fs::create_dir_all(&journals_dir).expect("fixture: journals dir");
    let journal_path = journals_dir.join("peer-instance-not-this-one.journal");
    let mut body = String::new();
    for _ in 0..line_count {
        let entry = s3_proxy::journal_manager::JournalEntry {
            timestamp: SystemTime::now(),
            instance_id: "peer-instance-not-this-one".to_string(),
            cache_key: cache_key.to_string(),
            range_spec: RangeSpec::new(
                0,
                1023,
                format!("{}_0-1023.bin", cache_key),
                CompressionAlgorithm::None,
                1024,
                1024,
            ),
            operation: s3_proxy::journal_manager::JournalOperation::AccessUpdate,
            range_file_path: format!("{}_0-1023.bin", cache_key),
            metadata_version: 1,
            new_ttl_secs: None,
            object_ttl_secs: None,
            access_increment: Some(1),
            object_metadata: None,
            fill_witness: None,
        };
        body.push_str(
            &serde_json::to_string(&entry).expect("fixture: JournalEntry must serialise"),
        );
        body.push('\n');
    }
    std::fs::write(&journal_path, body).expect("fixture: journal must be writable");

    // The traversal counter counts journal FILES read, so the count of files present is the
    // denominator of the assertion below. Derive it from the directory rather than assuming it is
    // the one file just written — anything else on this volume would otherwise silently inflate the
    // reading and make a failure unattributable.
    let files = std::fs::read_dir(&journals_dir)
        .expect("fixture: journals dir must be readable")
        .flatten()
        .filter(|e| {
            e.path().is_file()
                && e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".journal"))
        })
        .count() as u64;
    assert_eq!(
        files, 1,
        "fixture: exactly one journal file must be present, or the per-pass and per-object \
         traversal counts are not distinguishable by a simple comparison"
    );
    files
}

/// A pass over many candidate objects reads the journal set once, not once per object.
///
/// # What a failure means
///
/// A count at or near the object count implicates the activity check in
/// `CacheManager::perform_eviction_with_lock`: the `EvictionActivityOracle` built before
/// `run_leaf_groups` has been bypassed and `is_cache_entry_active` is being called from inside the
/// per-object body again. A count of zero implicates the fixture, not the product — it means the
/// predicate never ran, so the test proves nothing about the hoist and must not be read as a pass.
/// Those two are deliberately different failures with different messages.
#[tokio::test]
async fn an_eviction_pass_reads_the_journal_set_once_not_once_per_object() {
    let temp_dir = TempDir::new().unwrap();
    let max_cache_size = 1_000_000u64;
    let cache_manager = create_test_cache_manager(&temp_dir, max_cache_size);
    let now = SystemTime::now();

    // Ten 100 KB objects take the cache to ~1 MB, over the 95% trigger. Each is a single whole-object
    // range, so each is one candidate object in its own right.
    for i in 0..10u64 {
        store_test_range(
            &cache_manager,
            &format!("test-bucket/traversal-object-{}", i),
            100_000,
            now - Duration::from_secs(3600 - i * 60),
            now - Duration::from_secs(1800 - i * 60),
        )
        .await
        .unwrap();
    }

    // The journal names a key that is NOT a candidate, so the activity predicate answers "inactive"
    // for every object here and the pass is free to evict on the byte target alone. This test is
    // about the number of traversals, not about their verdict; the verdict is
    // `eviction_activity_set_safety_test.rs`.
    let journal_files = seed_peer_journal(temp_dir.path(), "test-bucket/some-other-key", 24);

    let before = collect_eviction_executor_gauges();
    cache_manager
        .enforce_disk_cache_limits()
        .await
        .expect("the eviction pass must succeed");
    let after = collect_eviction_executor_gauges();

    let objects_handled = (after.eviction_pass_path_whole_object_total
        + after.eviction_pass_path_partial_total)
        - (before.eviction_pass_path_whole_object_total + before.eviction_pass_path_partial_total);
    let journal_reads = after.eviction_active_key_journal_reads_total
        - before.eviction_active_key_journal_reads_total;

    // FIXTURE PRECONDITION, asserted rather than assumed. At one object the two rival traversal
    // counts coincide and every assertion below is satisfied by construction.
    assert!(
        objects_handled >= 3,
        "fixture: the pass must handle at least 3 candidate objects for a per-pass count to be \
         distinguishable from a per-object one (handled={objects_handled}). The byte target or the \
         fixture sizing has drifted; this is not a product failure."
    );

    // The predicate must have RUN. A zero here is the apparatus failing, not the product passing —
    // without this arm a build where the set is never constructed reads as a perfect score.
    assert!(
        journal_reads >= journal_files,
        "the cross-instance activity predicate never read the journal (reads={journal_reads}, \
         files present={journal_files}). This is an APPARATUS failure: the set was not built, so \
         the traversal count below proves nothing about the hoist. Check that \
         `build_eviction_activity_set` is reached from `perform_eviction_with_lock`."
    );

    // THE GREEN CRITERION. One build, plus at most one refresh if the pass outlived half a flush
    // interval, times the journal file count. The per-object form reads `objects_handled ×
    // journal_files`, which for this fixture is at least 3× the ceiling asserted here.
    let ceiling = 2 * journal_files;
    assert!(
        journal_reads <= ceiling,
        "the journal set must be read once per pass (plus at most one refresh), not once per \
         object: reads={journal_reads}, ceiling={ceiling}, objects handled={objects_handled}, \
         journal files={journal_files}. A count at or near the object count means the per-object \
         traversal is back in the executor's inner loop."
    );
    assert!(
        journal_reads < objects_handled * journal_files,
        "reads={journal_reads} is not strictly below the per-object cost \
         ({objects_handled} objects × {journal_files} files), so this pass paid the traversal per \
         object. This is the defect the hoist removed."
    );
}
