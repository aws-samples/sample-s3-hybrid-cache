//! The hoisted activity set is a SAFE substitution: it still protects a peer's recent hit.
//!
//! # What this guards, and why the cost test is not enough
//!
//! `tests/eviction_activity_set_traversal_test.rs` asserts the pass stopped re-reading the journal
//! per object. On its own, the cheapest way to pass it is to delete the predicate — which would be a
//! correctness regression invisible in every figure that test reads. This file is the other half:
//! the verdict must be unchanged.
//!
//! The predicate exists because the disk is shared and RAM is not. Instance A's cache hit is
//! journalled and does not reach instance B's `.meta` until consolidation, so B deciding on
//! `.meta.last_accessed` alone would delete an object A is actively reading — and B would never
//! learn it had, because nothing propagates the deletion back. That is the cross-instance rule in
//! the cache coherency invariants § "shared storage gives local decisions fleet-wide
//! reach", and it is why the hoist had to preserve the verdict rather than merely the shape.
//!
//! # Two-sided, and the control is what makes a failure attributable
//!
//! One object carries a journalled `AccessUpdate` inside the window; the others do not. Both arms
//! are asserted in the same pass:
//!
//! - **The protected arm.** The journalled object survives.
//! - **The control arm.** Objects without a journal entry are evicted.
//!
//! Without the control, an eviction pass that silently did nothing at all — wrong config, byte
//! target already met, lock not acquired, fixture not over the trigger — would satisfy the protected
//! arm perfectly. "The object survived" and "nothing ran" are the same observation, and only the
//! control separates them.
//!
//! # ORDER IS THE EXPERIMENT
//!
//! The protected object is deliberately given the OLDEST `last_accessed` of the ten, which makes it
//! the *most* evictable candidate under TinyLFU: every object carries the same `access_count` and
//! is idle for under one half-life, so decayed frequency ties and `last_accessed` decides. So the activity predicate is the only thing that can
//! save it: if it survives, it survived on the journal entry and nothing else. Seeding it as a
//! middling candidate would let the byte target be satisfied before the pass ever reached it, and
//! the test would pass without exercising the predicate.
//!
//! # No global counters are read here
//!
//! Deliberate. The executor counters are process-global `AtomicU64`s and libtest runs a binary's
//! tests as parallel threads, so this file drives a pass and asserts only on its own tempdir's
//! filesystem state. That is what lets the traversal count be measured as a clean delta in the other
//! binary — a counter-reading test and a pass-driving test cannot share a process.
//!
//! Spec: cache-eviction-at-scale § 4.5. Predicate provenance: `Req 19.2` is
//! the review fixes feature Requirement 19.
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::disk_cache::get_sharded_path;
use s3_proxy::Result;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// `ram_cache_flush_interval`. The activity window is `2×` this, so a journal entry stamped `now` is
/// comfortably inside it and one stamped well beyond it is comfortably outside.
const FLUSH_INTERVAL: Duration = Duration::from_secs(10);
const OBJECT_BYTES: usize = 100_000;
const OBJECT_COUNT: u64 = 10;

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
    created_at: SystemTime,
    last_accessed: SystemTime,
) -> Result<()> {
    let (start, end) = (0u64, OBJECT_BYTES as u64 - 1);
    let data = vec![0u8; OBJECT_BYTES];
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
        OBJECT_BYTES as u64,
        OBJECT_BYTES as u64,
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

/// Write a peer instance's journal holding one `AccessUpdate` for `cache_key`.
fn seed_peer_access_update(cache_dir: &std::path::Path, cache_key: &str, timestamp: SystemTime) {
    let journals_dir = cache_dir.join("metadata").join("_journals");
    std::fs::create_dir_all(&journals_dir).expect("fixture: journals dir");
    let entry = s3_proxy::journal_manager::JournalEntry {
        timestamp,
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
    let line = serde_json::to_string(&entry).expect("fixture: JournalEntry must serialise");
    std::fs::write(
        journals_dir.join("peer-instance-not-this-one.journal"),
        format!("{}\n", line),
    )
    .expect("fixture: journal must be writable");
}

/// Confirm the fixture's line is on disk and is the shape the scan looks for.
///
/// Without this, an absent or unparseable line and a genuinely inactive key are the same reading,
/// and the protected arm below would be indistinguishable from a pass that simply ran out of byte
/// target.
fn assert_journal_holds_access_update(cache_dir: &std::path::Path, cache_key: &str) {
    let path = cache_dir
        .join("metadata")
        .join("_journals")
        .join("peer-instance-not-this-one.journal");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("fixture: journal must exist at {:?}: {}", path, e));
    let line = content
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_else(|| panic!("fixture: journal at {:?} holds no entry", path));
    let parsed: s3_proxy::journal_manager::JournalEntry = serde_json::from_str(line)
        .unwrap_or_else(|e| {
            panic!("fixture: journal line must deserialise as a JournalEntry: {e}")
        });
    assert_eq!(
        parsed.operation,
        s3_proxy::journal_manager::JournalOperation::AccessUpdate,
        "fixture: the entry must be an AccessUpdate, or the activity scan skips it entirely"
    );
    assert_eq!(
        parsed.cache_key, cache_key,
        "fixture: the entry must name the key under test"
    );
}

fn key_for(i: u64) -> String {
    format!("test-bucket/safety-object-{}", i)
}

/// A peer's journalled hit protects the object; objects without one are evicted in the same pass.
///
/// # What a failure means
///
/// **Protected arm red** — the journalled object was evicted. The hoisted
/// `EvictionActivityOracle` is not consulting the same predicate
/// `CacheManager::is_cache_entry_active` does: either the set builder's window/cutoff arithmetic is
/// wrong, or it is filtering on something the per-key form does not. This is the
/// cross-instance-delete-safety regression, and it is the reason the hoist needed an argument rather
/// than an assumption.
///
/// **Control arm red** — nothing was evicted at all. Not a predicate failure: the pass did not run,
/// or ran and freed nothing. Check the trigger arithmetic and the fixture sizing before looking at
/// the predicate, because in that state the protected arm above is vacuous.
#[tokio::test]
async fn a_peer_journalled_hit_survives_the_pass_while_unjournalled_objects_do_not() {
    let temp_dir = TempDir::new().unwrap();
    // Ten 100 KB objects against a 1 MB ceiling puts the cache over the 95% trigger, with a target
    // of 80% so the pass must free roughly two objects' worth.
    let cache_manager = create_test_cache_manager(&temp_dir, 1_000_000);
    let now = SystemTime::now();

    for i in 0..OBJECT_COUNT {
        store_test_range(
            &cache_manager,
            &key_for(i),
            now - Duration::from_secs(3600 - i * 60),
            // i=0 is the OLDEST last_accessed, so object 0 is the most evictable candidate under
            // TinyLFU's recency tiebreak (equal access_count). That is the point — see the module comment on order being the experiment.
            now - Duration::from_secs(1800 - i * 60),
        )
        .await
        .unwrap();
    }

    let protected = key_for(0);
    seed_peer_access_update(temp_dir.path(), &protected, now);
    assert_journal_holds_access_update(temp_dir.path(), &protected);

    let protected_meta = cache_manager.get_new_metadata_file_path(&protected);
    assert!(
        protected_meta.exists(),
        "fixture: the protected object's metadata must exist before the pass"
    );
    let present_before = (0..OBJECT_COUNT)
        .filter(|i| {
            cache_manager
                .get_new_metadata_file_path(&key_for(*i))
                .exists()
        })
        .count();
    assert_eq!(
        present_before, OBJECT_COUNT as usize,
        "fixture: all {OBJECT_COUNT} objects must be resident before the pass"
    );

    cache_manager
        .enforce_disk_cache_limits()
        .await
        .expect("the eviction pass must succeed");

    let unjournalled_evicted = (1..OBJECT_COUNT)
        .filter(|i| {
            !cache_manager
                .get_new_metadata_file_path(&key_for(*i))
                .exists()
        })
        .count();

    // CONTROL ARM FIRST. If nothing was evicted, the protected arm below proves nothing, so this
    // failure must be read before that one.
    assert!(
        unjournalled_evicted >= 2,
        "the pass evicted {unjournalled_evicted} unjournalled object(s); at least 2 were expected \
         from a 1 MB ceiling at a 95% trigger and an 80% target. Nothing was evicted, so the \
         protected-arm assertion below is VACUOUS — this is a fixture or trigger problem, not a \
         verdict about the activity predicate."
    );

    // THE PROTECTED ARM. Object 0 is the oldest and therefore the first candidate, so the journal
    // entry is the only thing that can have saved it.
    assert!(
        protected_meta.exists(),
        "the object with a peer's journalled AccessUpdate inside the \
         2x-ram_cache_flush_interval window was EVICTED, while {unjournalled_evicted} \
         unjournalled objects were also evicted in the same pass. The hoisted activity set is not \
         reproducing `is_cache_entry_active`'s verdict, which means a peer's in-progress read can \
         now have its data deleted underneath it — and on shared storage the instance that loses \
         the data cannot observe who took it."
    );
}

/// An `AccessUpdate` OLDER than the window does not protect the object.
///
/// # Why this arm exists
///
/// It is the width guard on the one above. A set builder that ignored the cutoff entirely — treating
/// the mere presence of an `AccessUpdate` as activity, whatever its age — would pass the protected
/// arm perfectly while pinning every object that had ever been read into the cache forever, which is
/// an eviction failure rather than a safety one. The rule is "a hit inside the window", not "a hit",
/// and only a stale entry can tell those apart.
#[tokio::test]
async fn an_access_update_older_than_the_window_does_not_protect_the_object() {
    let temp_dir = TempDir::new().unwrap();
    let cache_manager = create_test_cache_manager(&temp_dir, 1_000_000);
    let now = SystemTime::now();

    for i in 0..OBJECT_COUNT {
        store_test_range(
            &cache_manager,
            &key_for(i),
            now - Duration::from_secs(3600 - i * 60),
            now - Duration::from_secs(1800 - i * 60),
        )
        .await
        .unwrap();
    }

    // Object 0 is again the oldest and first candidate, but its journalled access is far outside the
    // 20 s window (2 x FLUSH_INTERVAL), so it must NOT be protected.
    let stale = key_for(0);
    let stale_stamp = now - (FLUSH_INTERVAL * 2 + Duration::from_secs(600));
    seed_peer_access_update(temp_dir.path(), &stale, stale_stamp);
    assert_journal_holds_access_update(temp_dir.path(), &stale);

    let stale_meta = cache_manager.get_new_metadata_file_path(&stale);
    assert!(
        stale_meta.exists(),
        "fixture: the object must start resident"
    );

    cache_manager
        .enforce_disk_cache_limits()
        .await
        .expect("the eviction pass must succeed");

    assert!(
        !stale_meta.exists(),
        "an AccessUpdate stamped {:?} before now — outside the {:?} window — protected the object \
         from eviction. The activity set is treating the presence of an AccessUpdate as activity \
         rather than its recency, which pins every previously-read object into the cache and turns \
         a safety predicate into an eviction stall.",
        FLUSH_INTERVAL * 2 + Duration::from_secs(600),
        FLUSH_INTERVAL * 2,
    );
}
