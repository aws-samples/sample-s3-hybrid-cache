//! Tests for shared cache coordination functionality
//!
//! Three tests were deleted from this file with the `locks/{key}.lock` per-key write
//! Two tests were deleted from this file with the `locks/{key}.lock` per-key write sentinel
//! they exercised: `test_write_lock_coordination` (the sentinel's own acquire/contend/release
//! semantics) and `test_coordinated_cache_cleanup` (a sweep and a force-invalidate helper,
//! both production-dead). Each was a test of the mechanism rather than of a property that
//! outlives it. `test_cache_entry_active_detection` is repointed at the branch that survives,
//! and `test_coordination_statistics` is narrowed to the figures that do.

use s3_proxy::{cache::CacheManager, cache_types::CacheMetadata, Result};

use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// `CacheManager::is_cache_entry_active`'s surviving predicate: a recent cross-instance
/// `AccessUpdate` journal entry.
///
/// # What branch this exercises, and why the old shape could not
///
/// The function used to consult two things: the `locks/{key}.lock` write sentinel first, and
/// then a scan of `metadata/_journals/*.journal` for an `AccessUpdate` for this key newer
/// than 2× `ram_cache_flush_interval`. The sentinel namespace is retired, so the journal scan
/// is now the whole predicate — and it is the branch that matters, being cross-instance and
/// four times longer-lived than the branch that went.
///
/// This test previously asserted `false → true → false` around a sentinel acquisition. It is
/// the only assertion anywhere on the surviving branch, so it is repointed rather than
/// deleted, and the three phases now name the journal window instead.
///
/// # It is written from a PEER instance's point of view
///
/// The signal is cross-instance by construction: the entry is written into a journal file
/// named for an instance that is not this `CacheManager`, so a predicate that only ever
/// noticed its own activity would fail phase 2. Nothing in this test drives a local cache
/// operation.
///
/// # The middle phase is made observable before it is asserted on
///
/// The characteristic vacuity of a deletion unit is a test that passes because it no longer
/// tests anything — here, a fixture that never lands an in-window `AccessUpdate` reads
/// `false → false → false` and passes. So phase 2 asserts `true` (not merely "not an
/// error"), and the journal line is read back off disk and confirmed to parse as an
/// `AccessUpdate` for this key before the predicate is consulted. A fixture that failed to
/// write, or wrote a shape the scan cannot deserialise, fails naming that cause rather than
/// reporting the predicate as `false`.
///
/// # The window is driven down, and phase 3 does not wait for it
///
/// `ram_cache_flush_interval` is set to **2 s** here (the shipped default is 10 s, giving a
/// 20 s window that no test should sit through), so the window under test is 4 s. Phase 3
/// backdates its entry to 10 s old rather than sleeping: the boundary is a comparison against
/// the entry's own timestamp, so controlling the timestamp tests it directly and the test
/// stays sub-second.
#[tokio::test]
async fn test_cache_entry_active_detection() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let flush_interval = Duration::from_secs(2);
    let cache_manager = cache_manager_with_flush_interval(temp_dir.path(), flush_interval);
    let window = flush_interval * 2;

    let cache_key = "test-bucket/active-test";
    let journals_dir = temp_dir.path().join("metadata").join("_journals");
    std::fs::create_dir_all(&journals_dir).unwrap();
    let peer_journal = journals_dir.join("peer-instance-not-this-one.journal");

    // Phase 1: no journal entry at all — the predicate's only input is absent.
    assert!(
        !cache_manager.is_cache_entry_active(cache_key).await?,
        "phase 1: with no AccessUpdate journal entry for the key the predicate must be false"
    );

    // Phase 2: a peer instance's AccessUpdate, inside the window.
    write_peer_access_update(&peer_journal, cache_key, SystemTime::now());
    assert_journal_holds_access_update(&peer_journal, cache_key);
    assert!(
        cache_manager.is_cache_entry_active(cache_key).await?,
        "phase 2: an AccessUpdate from another instance timestamped now is inside the {:?} \
         window, so the key must read as actively used. The journal line was confirmed \
         present and parseable above, so a false here is the predicate, not the fixture",
        window
    );

    // Phase 3: the same entry, backdated well past the window.
    let stale = SystemTime::now() - (window + Duration::from_secs(6));
    write_peer_access_update(&peer_journal, cache_key, stale);
    assert_journal_holds_access_update(&peer_journal, cache_key);
    assert!(
        !cache_manager.is_cache_entry_active(cache_key).await?,
        "phase 3: an AccessUpdate {:?} old is outside the {:?} window and must not count as \
         activity — otherwise the cutoff is not being applied and every key ever read stays \
         permanently unevictable",
        window + Duration::from_secs(6),
        window
    );

    Ok(())
}

/// A `CacheManager` with `ram_cache_flush_interval` set explicitly.
///
/// `new_with_defaults` hardcodes 10 s, which makes the window 20 s. Everything else here is
/// the same defaults that constructor passes.
fn cache_manager_with_flush_interval(
    cache_dir: &std::path::Path,
    flush_interval: Duration,
) -> CacheManager {
    CacheManager::new_with_shared_storage(
        cache_dir.to_path_buf(),
        false,                   // ram_cache_enabled
        0,                       // max_ram_cache_size
        10 * 1024 * 1024 * 1024, // max_cache_size
        s3_proxy::cache::CacheEvictionAlgorithm::default(),
        1024, // compression_threshold
        true, // compression_enabled
        Duration::from_secs(315_360_000),
        Duration::from_secs(315_360_000),
        Duration::from_secs(315_360_000),
        false, // actively_remove_cached_data
        s3_proxy::config::SharedStorageConfig::default(),
        10.0,  // write_cache_percent
        false, // write_cache_enabled
        Duration::from_secs(86400),
        s3_proxy::config::MetadataCacheConfig::default(),
        95,   // eviction_trigger_percent
        80,   // eviction_target_percent
        true, // read_cache_enabled
        Duration::from_secs(60),
        1_048_576, // compression_batch_size
        false,     // evaluate_conditions_from_cache
        flush_interval,
        64, // ram_cache_shard_count
        Duration::from_secs(5),
    )
}

/// Append one `AccessUpdate` line for `cache_key`, attributed to a peer instance.
///
/// The line is produced by serialising a real `JournalEntry` rather than by writing hand-rolled
/// JSON, so a schema change breaks this fixture loudly instead of leaving it writing a shape
/// the scan silently skips. The file is truncated each call so a phase's reading cannot be
/// satisfied by a previous phase's line.
fn write_peer_access_update(
    journal_path: &std::path::Path,
    cache_key: &str,
    timestamp: SystemTime,
) {
    let entry = s3_proxy::journal_manager::JournalEntry {
        timestamp,
        instance_id: "peer-instance-not-this-one".to_string(),
        cache_key: cache_key.to_string(),
        range_spec: s3_proxy::cache_types::RangeSpec::new(
            0,
            1023,
            format!("{}_0-1023.bin", cache_key),
            s3_proxy::compression::CompressionAlgorithm::None,
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
    let line = serde_json::to_string(&entry).expect("fixture: JournalEntry must serialise");
    std::fs::write(journal_path, format!("{}\n", line)).expect("fixture: journal must be writable");
}

/// Confirm the fixture's line is on disk and is the shape the scan looks for.
///
/// Without this, an absent or unparseable line and a genuinely inactive key are the same
/// reading, and the middle phase of the test above would pass vacuously.
fn assert_journal_holds_access_update(journal_path: &std::path::Path, cache_key: &str) {
    let content = std::fs::read_to_string(journal_path)
        .unwrap_or_else(|e| panic!("fixture: journal must exist at {:?}: {}", journal_path, e));
    let line = content
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_else(|| panic!("fixture: journal at {:?} holds no entry", journal_path));
    let parsed: s3_proxy::journal_manager::JournalEntry = serde_json::from_str(line)
        .unwrap_or_else(|e| panic!("fixture: journal line must deserialise: {}", e));
    assert_eq!(
        parsed.operation,
        s3_proxy::journal_manager::JournalOperation::AccessUpdate,
        "fixture: the entry must be an AccessUpdate, or the scan skips it"
    );
    assert_eq!(
        parsed.cache_key, cache_key,
        "fixture: the entry must name the key under test"
    );
}

/// `get_coordination_stats` still publishes its three surviving figures.
///
/// Narrowed rather than deleted. It used to take two `locks/{key}.lock` acquisitions purely so
/// the `locks/` directory would exist and an `active_locks` key would appear; that figure
/// counted the retired per-key write sentinels and is gone, so its name would now assert
/// per-key write activity that cannot happen. The three assertions that never depended on the
/// namespace are what the function is for, and they are worth more now that it has one fewer
/// field to publish.
#[tokio::test]
async fn test_coordination_statistics() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let stats = cache_manager.get_coordination_stats();
    assert!(stats.contains_key("cache_hits"));
    assert!(stats.contains_key("cache_misses"));
    assert!(stats.contains_key("expired_entries"));
    assert!(
        !stats.contains_key("active_locks"),
        "active_locks counted the retired per-key write sentinels and must not be published"
    );

    Ok(())
}

#[tokio::test]
async fn test_cache_consistency_check() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = CacheManager::new_with_defaults(temp_dir.path().to_path_buf(), false, 0);

    let cache_key = "test-bucket/consistency-test";
    let test_data = b"Test data for consistency check";
    let metadata = CacheMetadata {
        etag: "test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length: test_data.len() as u64,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    };

    // Store a cache entry
    cache_manager
        .store_response(cache_key, test_data, metadata)
        .await?;

    // Check consistency immediately after storage
    let is_consistent = cache_manager.check_cache_consistency(cache_key).await?;
    assert!(
        is_consistent,
        "Newly created cache entry should be consistent"
    );

    // Double-check by retrieving the entry
    let cached_response = cache_manager.get_cached_response(cache_key).await?;
    assert!(
        cached_response.is_some(),
        "Cache entry should be retrievable after storage"
    );

    // Check consistency for non-existent entry
    let non_existent_consistent = cache_manager
        .check_cache_consistency("non-existent-key")
        .await?;
    assert!(
        !non_existent_consistent,
        "Non-existent entry should not be consistent"
    );

    Ok(())
}
