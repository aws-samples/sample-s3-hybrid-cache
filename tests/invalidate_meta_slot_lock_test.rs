//! `DiskCacheManager::invalidate_all_ranges` unlinks a `.meta` under the key's metadata slot
//! lock, so the unlink cannot land inside a peer publisher's read-modify-write.
//!
//! # What this asserts, and why it is not an adjacent figure
//!
//! Every `.meta` publisher holds the key's slot lock —
//! `CacheManager::store_new_metadata`, `JournalConsolidator::write_metadata_to_disk` and
//! `HybridMetadataWriter::write_metadata_to_disk` all take `&MetadataLock` as a parameter, and
//! the consolidator (the production publisher) obtains it from
//! `MetadataLockManager::try_acquire_lock`. So the resource the ordering argument rests on is a
//! single exclusive `flock` on `metadata/_meta_locks/{slot}.lock`, and **this file holds that
//! exact lock through the publishers' own API** (`MetadataLockManager`) rather than reading a
//! counter or a log line that merely correlates with it. The observable is then the existence
//! of the `.meta` and `.bin` files, which is what the unlink acts on.
//!
//! # Divergence, not a shared view
//!
//! `cache-coherency-invariants.md` § "Constructing the test" is binding: a fixture where both
//! instances share one view passes whatever the code does. So the `.meta` and `.bin` here are
//! written **directly onto the shared volume**, the way a peer instance's publish appears —
//! this instance's store path is never driven, and its RAM metadata tier is never populated.
//! The peer's lock is taken through a **separate** `MetadataLockManager` over the same cache
//! directory, which is exactly how two proxies see one volume.
//!
//! # Why this is its own test binary
//!
//! `set_slot_acquire_wait_budget` is process-global, and libtest runs a binary's tests as
//! parallel threads in one process — the trap recorded in the testing guidance under
//! "Never assert an exact delta on a process-global counter", in its state-mutating form. Every
//! test here takes `BUDGET_LOCK` before touching it and restores the previous value. **If you
//! add a test to this file that sets the budget, it must take `BUDGET_LOCK`.**
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::metadata_lock_manager::{
    set_slot_acquire_wait_budget, slot_acquire_wait_budget, slot_for_cache_key, slot_lock_path,
    MetadataLockManager,
};
use s3_proxy::ProxyError;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Serialises every test in this file that mutates the process-global wait budget.
///
/// `tokio::sync::Mutex`, not `std::sync::Mutex`: the guard is held across `.await` points.
static BUDGET_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// A short budget, so the contended arm fails fast instead of parking for the 5s default.
const TEST_BUDGET: Duration = Duration::from_millis(300);

fn disk_cache(dir: &TempDir) -> DiskCacheManager {
    DiskCacheManager::new(dir.path().to_path_buf(), false, 1024, false, 1_048_576)
}

/// Seed one cached range for `cache_key` by writing the `.meta` and the `.bin` **directly**,
/// as another instance's publish appears on the shared volume.
///
/// Returns `(metadata_path, range_file_path)`. Asserts the fixture is SHARDED, which is
/// load-bearing: on a flat fixture the sharded and legacy-flat `.meta` constructions resolve to
/// the same file, and an assertion about the sharded unlink can pass with the defect fully
/// present — the vacuity `EVICT-METAPATH-FIX` had to add a precondition assertion to prevent.
fn seed_peer_published_entry(
    dir: &TempDir,
    manager: &DiskCacheManager,
    cache_key: &str,
) -> (PathBuf, PathBuf) {
    const BODY_LEN: u64 = 512;

    let range_file_path = manager.get_new_range_file_path(cache_key, 0, BODY_LEN - 1);
    let relative_range_path = range_file_path
        .strip_prefix(dir.path().join("ranges"))
        .expect("range file must live under cache_dir/ranges")
        .to_string_lossy()
        .to_string();

    let now = SystemTime::now();
    let metadata = NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::new(
            "\"peer-published-etag\"".to_string(),
            "Wed, 01 Jan 2025 12:00:00 GMT".to_string(),
            BODY_LEN,
            Some("application/octet-stream".to_string()),
        ),
        ranges: vec![RangeSpec::new(
            0,
            BODY_LEN - 1,
            relative_range_path,
            CompressionAlgorithm::None,
            BODY_LEN,
            BODY_LEN,
        )],
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: Some(now + Duration::from_secs(60)),
        ..Default::default()
    };

    let metadata_path = manager.get_new_metadata_file_path(cache_key);
    std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
    std::fs::write(&metadata_path, serde_json::to_string(&metadata).unwrap()).unwrap();

    std::fs::create_dir_all(range_file_path.parent().unwrap()).unwrap();
    std::fs::write(&range_file_path, vec![7u8; BODY_LEN as usize]).unwrap();

    let relative_metadata_path = metadata_path
        .strip_prefix(dir.path().join("metadata"))
        .expect("metadata path must live under cache_dir/metadata");
    assert_eq!(
        relative_metadata_path.components().count(),
        4,
        "fixture must be SHARDED ({{bucket}}/{{XX}}/{{YYY}}/{{file}}.meta) or an assertion \
         about the sharded unlink is vacuous — a flat fixture is removed by the legacy flat \
         path too: key={}, relative_path={:?}",
        cache_key,
        relative_metadata_path
    );
    assert!(metadata_path.exists(), "fixture: .meta must exist");
    assert!(range_file_path.exists(), "fixture: .bin must exist");

    (metadata_path, range_file_path)
}

/// The headline. While a peer holds the key's metadata slot lock, invalidation removes
/// **nothing** and reports failure; release the lock and the same call on the same fixture
/// removes both files.
///
/// The two arms differ in exactly one thing — whether the peer holds the lock — which is what
/// makes the first arm non-vacuous. Without the second arm, a fixture whose paths the code
/// never resolves would satisfy the first arm with the acquisition removed.
#[tokio::test]
async fn invalidation_removes_nothing_while_a_peer_holds_the_slot_lock() {
    let _serialise = BUDGET_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let manager = disk_cache(&dir);
    manager.initialize().await.unwrap();

    let cache_key = "invalidate-lock-bucket/peer-published-object.bin";
    let (metadata_path, range_file_path) = seed_peer_published_entry(&dir, &manager, cache_key);

    let previous_budget = slot_acquire_wait_budget();
    set_slot_acquire_wait_budget(TEST_BUDGET);

    // The peer: a separate lock manager over the same shared cache directory, holding the slot
    // the way a publisher does mid read-modify-write. `max_retries = 0` because this
    // acquisition is uncontended and must not sit in a backoff loop.
    let peer_manager = MetadataLockManager::new(dir.path().to_path_buf(), TEST_BUDGET, 0);
    let peer_lock = peer_manager
        .acquire_lock(cache_key)
        .await
        .expect("fixture: the peer must be able to take the publishers' lock");

    // ---- ARM 1: contended. Nothing may be removed, and the failure must be reported. ----
    //
    // All three observations are taken BEFORE any of them is asserted, so a failure reports the
    // whole state rather than stopping at whichever assertion happens to be written first. A
    // panic naming only the return value cannot distinguish "reported success and removed
    // nothing" from "reported success and removed everything", and those are different defects.
    let contended = manager.invalidate_all_ranges(cache_key).await;
    let meta_survived = metadata_path.exists();
    let bin_survived = range_file_path.exists();
    let retryable = matches!(&contended, Err(ProxyError::LockContention(_)));

    assert!(
        retryable && meta_survived && bin_survived,
        "while the publishers' lock is held, invalidation must remove NOTHING and report a \
         RETRYABLE LockContention. `Ok(())` means the unlink landed inside a peer's \
         read-modify-write; a surviving error with the files gone means a failed acquisition \
         left a half-invalidation whose caller sees only an error; a non-LockContention error \
         would tell the caller to stop retrying a transient condition. \
         result={:?} retryable={} meta_survived={} bin_survived={} meta={:?} bin={:?}",
        contended,
        retryable,
        meta_survived,
        bin_survived,
        metadata_path,
        range_file_path
    );

    // ---- ARM 2: the same call, same fixture, lock released. Both files must go. ----
    drop(peer_lock);
    manager
        .invalidate_all_ranges(cache_key)
        .await
        .expect("an uncontended invalidation must succeed");
    assert!(
        !metadata_path.exists(),
        "ARM 2 is what makes ARM 1 evidence: if the `.meta` survives here too, the fixture's \
         paths are not the ones the code resolves and ARM 1 proved nothing: path={:?}",
        metadata_path
    );
    assert!(
        !range_file_path.exists(),
        "the `.bin` must be removed by the uncontended invalidation: path={:?}",
        range_file_path
    );

    set_slot_acquire_wait_budget(previous_budget);
}

/// Width control: the exclusion is per-slot, not global.
///
/// An over-broad fix — one that serialised invalidation against any held metadata lock — passes
/// the test above and fails this one. Without it, "invalidation is refused" could not be
/// attributed to *this key's* slot.
#[tokio::test]
async fn invalidation_is_unaffected_by_a_lock_held_on_a_different_slot() {
    let _serialise = BUDGET_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let manager = disk_cache(&dir);
    manager.initialize().await.unwrap();

    let cache_key = "invalidate-lock-bucket/subject-object.bin";
    let subject_slot = slot_for_cache_key(cache_key);

    // Find a key whose slot genuinely differs. Asserted rather than assumed: with 4096 slots a
    // hand-picked "other" key can collide, and a colliding one would make this control pass for
    // the wrong reason by never being contended at all.
    let other_key = (0..64)
        .map(|i| format!("invalidate-lock-bucket/unrelated-{i}.bin"))
        .find(|k| slot_for_cache_key(k) != subject_slot)
        .expect("one of 64 candidate keys must hash to a different slot");
    assert_ne!(
        slot_lock_path(dir.path(), cache_key),
        slot_lock_path(dir.path(), &other_key),
        "the control key must resolve to a different slot FILE, or this test is vacuous"
    );

    let (metadata_path, range_file_path) = seed_peer_published_entry(&dir, &manager, cache_key);

    let previous_budget = slot_acquire_wait_budget();
    set_slot_acquire_wait_budget(TEST_BUDGET);

    let peer_manager = MetadataLockManager::new(dir.path().to_path_buf(), TEST_BUDGET, 0);
    let unrelated_lock = peer_manager
        .acquire_lock(&other_key)
        .await
        .expect("fixture: the peer must be able to lock the unrelated key");

    manager
        .invalidate_all_ranges(cache_key)
        .await
        .expect("a lock held on an unrelated slot must not block this key's invalidation");
    assert!(
        !metadata_path.exists(),
        "`.meta` must still be removed when the held lock is on another slot: path={:?}",
        metadata_path
    );
    assert!(
        !range_file_path.exists(),
        "`.bin` must still be removed when the held lock is on another slot: path={:?}",
        range_file_path
    );

    drop(unrelated_lock);
    set_slot_acquire_wait_budget(previous_budget);
}
