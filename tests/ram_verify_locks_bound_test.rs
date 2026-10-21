//! `CacheManager::ram_verify_locks` must stay bounded by the keys whose coalescing state is
//! still useful, not grow with every distinct key ever verified.
//!
//! Before the change an entry was inserted per distinct `cache_key` on its first past-interval
//! verification and removed nowhere, so the map grew for the process's lifetime.
//!
//! The coalescing the map exists for is pinned separately by
//! `ram_verify_decision_table_test::concurrent_verifications_coalesce_per_key`.
//!
//! `multi_thread` is required: promotion and the RAM hit lookup go through
//! `tokio::task::block_in_place`, which panics on the current-thread runtime.

use std::time::Duration;

use s3_proxy::cache::{CacheManager, RamVerifyDiskCheck};
use s3_proxy::compression::CompressionAlgorithm;
use tempfile::TempDir;

const ETAG: &str = "\"version-a\"";

/// # RED SIDE
///
/// On the code before the change the map holds one entry per key verified: 5,000 after this loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verifying_many_distinct_keys_keeps_the_lock_map_bounded() {
    const KEYS: usize = 5_000;
    // Well above the prune floor, so an unbounded map is unmistakable, and the bound asserted
    // is the floor the implementation documents.
    const BOUND: usize = 1_024;

    let dir = TempDir::new().unwrap();
    let mut cm = CacheManager::new(dir.path().to_path_buf(), true, 8 * 1024 * 1024, 1024, true);
    // Zero: every verification is past the interval, so every call reaches the lock map, and
    // every state is stale the moment its call returns.
    cm.set_ram_cache_verification_interval(Duration::ZERO);

    let seed_key = "verify-bucket/bound-seed.bin";
    let payload: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();
    assert!(
        cm.promote_range_to_ram_cache_frame(
            seed_key,
            (0, 999),
            payload,
            CompressionAlgorithm::None,
            ETAG.to_string(),
            String::new(),
            1u64 << 40,
        ),
        "PRECONDITION MISSING: the seed range was not promoted"
    );
    let hit = cm
        .get_range_from_ram_cache_hit(seed_key, 0, 999, Duration::from_secs(3600))
        .expect("PRECONDITION MISSING: seed range not resident");

    for i in 0..KEYS {
        let key = format!("verify-bucket/bound-{i}.bin");
        assert!(
            cm.ram_verify(&key, &hit, |_k: String| async {
                RamVerifyDiskCheck::Found(ETAG.to_string())
            })
            .await,
            "a matching etag must verify"
        );
    }

    let held = cm.ram_verify_lock_count_for_tests().await;
    assert!(
        held <= BOUND,
        "ram_verify_locks holds {held} entries after verifying {KEYS} distinct keys whose \
         states are all stale; it must stay within {BOUND}"
    );
}
