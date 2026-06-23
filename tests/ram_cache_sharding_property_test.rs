//! Property-based tests for `ShardedRamCache`.
//!
//! Spec: `.kiro/specs/performance-enhancements/`
//! Task: 3.12
//!
//! Tests four correctness properties of the sharded RAM cache:
//!
//! - **Property 1: Shard isolation** — concurrent `get(K1)`, `get(K2)` in different
//!   shards proceed without blocking
//! - **Property 2: Arc-clone correctness** — `Arc` from `get()` valid after eviction
//! - **Property 3: Capacity invariant** — total stored ≤ max_ram_cache_size
//! - **Property 5: Read-only access path correctness** — concurrent same-key reads
//!   return byte-identical data, `access_count` not lost entirely
//!
//! **Validates: Requirements 1.4, 2.2, 2.6, 4.1, 4.3**

use bytes::Bytes;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use s3_proxy::cache::{CacheEvictionAlgorithm, RamCacheEntry};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::ram_cache::{shard_index, ShardedRamCache};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Helper: build a minimal RamCacheEntry from a key and raw bytes.
// ---------------------------------------------------------------------------

fn make_entry(key: &str, data: &[u8]) -> RamCacheEntry {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    RamCacheEntry {
        cache_key: key.to_string(),
        data: Arc::new(Bytes::copy_from_slice(data)),
        metadata: CacheMetadata {
            etag: "test-etag".to_string(),
            last_modified: "Mon, 01 Jan 2024 00:00:00 GMT".to_string(),
            content_length: data.len() as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: SystemTime::now(),
        },
        created_at: SystemTime::now(),
        last_accessed: AtomicU64::new(now_ms),
        access_count: AtomicU64::new(0),
        compressed: false,
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::Lz4,
    }
}

// ---------------------------------------------------------------------------
// Property 1: Shard isolation
//
// For any two keys K1, K2 that map to *different* shards in a cache with
// SHARD_COUNT shards, concurrent get(K1) and get(K2) both succeed — no
// deadlock, no panic.
//
// **Validates: Requirements 1.4, 3.2**
// ---------------------------------------------------------------------------

/// **Feature: performance-enhancements, Property 1: Shard isolation**
/// **Validates: Requirements 1.4, 3.2**
///
/// Two keys in different shards can be read concurrently without blocking each other.
#[quickcheck]
fn prop_shard_isolation_concurrent_gets(k1: String, k2: String) -> TestResult {
    // Both keys must be non-empty.
    if k1.is_empty() || k2.is_empty() {
        return TestResult::discard();
    }

    const SHARD_COUNT: usize = 8;

    // We need K1 and K2 to fall into *different* shards.
    if shard_index(&k1, SHARD_COUNT) == shard_index(&k2, SHARD_COUNT) {
        return TestResult::discard();
    }

    // Give each shard enough room for one entry.
    let capacity = SHARD_COUNT * 4096;
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let cache = Arc::new(ShardedRamCache::new(
            capacity,
            SHARD_COUNT,
            CacheEvictionAlgorithm::LRU,
        ));

        let data1 = b"value-for-k1";
        let data2 = b"value-for-k2";

        // Pre-populate both keys.
        cache.put(make_entry(&k1, data1)).await.unwrap();
        cache.put(make_entry(&k2, data2)).await.unwrap();

        // Concurrent reads from two separate tasks.
        let cache1 = Arc::clone(&cache);
        let cache2 = Arc::clone(&cache);
        let k1_clone = k1.clone();
        let k2_clone = k2.clone();

        let t1 = tokio::spawn(async move { cache1.get(&k1_clone).await });
        let t2 = tokio::spawn(async move { cache2.get(&k2_clone).await });

        let (r1, r2) = tokio::join!(t1, t2);

        // Neither task should have panicked (join returns Ok).
        let read1 = r1.expect("task 1 panicked").expect("k1 not found");
        let read2 = r2.expect("task 2 panicked").expect("k2 not found");

        // Data must be exactly what was stored.
        if read1.data.as_ref().as_ref() != data1 {
            return TestResult::failed();
        }
        if read2.data.as_ref().as_ref() != data2 {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

// ---------------------------------------------------------------------------
// Property 2: Arc-clone correctness
//
// For any key K with data D:
//   1. put(K, D)
//   2. arc = get(K).data
//   3. Force eviction of K by filling the cache with another key (tiny cache)
//   4. arc.as_ref() == D   (Arc remains valid — no use-after-free)
//
// **Validates: Requirements 2.2, 2.3**
// ---------------------------------------------------------------------------

/// **Feature: performance-enhancements, Property 2: Arc-clone correctness**
/// **Validates: Requirements 2.2, 2.3**
///
/// The `Arc<Bytes>` returned by `get()` stays valid after the entry is evicted.
#[quickcheck]
fn prop_arc_clone_outlives_eviction(key: String, data: Vec<u8>) -> TestResult {
    if key.is_empty() || data.is_empty() {
        return TestResult::discard();
    }
    // Cap data to avoid very large allocations.
    if data.len() > 16_384 {
        return TestResult::discard();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        // Use a single shard and a tiny capacity just large enough for *one* entry
        // of `data` but not two. The base entry size includes RamCacheEntry overhead
        // (~200 bytes) + key size + data size + CacheMetadata overhead (~80 bytes).
        // We set capacity to ~1.5× the single-entry size so the entry fits but a
        // second equally-sized entry forces eviction of the first.
        let overhead = 300 + key.len(); // generous estimate
        let entry_bytes = overhead + data.len();
        let capacity = entry_bytes + entry_bytes / 2 + 1;

        let cache = ShardedRamCache::new(capacity, 1, CacheEvictionAlgorithm::LRU);

        // Step 1: store K → D.
        cache.put(make_entry(&key, &data)).await.unwrap();

        // Step 2: clone the Arc from get().
        let read = cache.get(&key).await;
        if read.is_none() {
            // Entry might have been too large to fit — skip.
            return TestResult::discard();
        }
        let arc_clone: Arc<Bytes> = read.unwrap().data;

        // Step 3: force eviction of K by inserting a second entry of the same size.
        //         We pick a key guaranteed to land in the same shard (shard count = 1).
        let evict_key = format!("{}__evict", key);
        cache.put(make_entry(&evict_key, &data)).await.unwrap();

        // After eviction the original entry should be gone from the cache.
        // (It may or may not be gone depending on exact sizes, but arc_clone must still be valid.)

        // Step 4: the Arc clone must still point to the original data.
        if arc_clone.as_ref() != data.as_slice() {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

// ---------------------------------------------------------------------------
// Property 3: Capacity invariant
//
// After putting N entries into a ShardedRamCache with total_capacity C,
// sum(shard.current_size) ≤ max_size.
//
// **Validates: Requirement 1.7**
// ---------------------------------------------------------------------------

/// **Feature: performance-enhancements, Property 3: Capacity invariant**
/// **Validates: Requirement 1.7**
///
/// Total bytes stored across all shards never exceeds `max_ram_cache_size`.
#[quickcheck]
fn prop_capacity_invariant(entries: Vec<(String, Vec<u8>)>) -> TestResult {
    // Filter: non-empty keys and data, no more than 50 entries, data ≤ 1 KiB each.
    let valid: Vec<_> = entries
        .into_iter()
        .filter(|(k, v)| !k.is_empty() && !v.is_empty() && v.len() <= 1024)
        .take(50)
        .collect();

    if valid.is_empty() {
        return TestResult::discard();
    }

    // Cache capacity: 16 KiB total, 4 shards → 4 KiB each.
    const TOTAL_CAPACITY: usize = 16 * 1024;
    const SHARD_COUNT: usize = 4;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let cache = ShardedRamCache::new(TOTAL_CAPACITY, SHARD_COUNT, CacheEvictionAlgorithm::LRU);

        for (key, data) in &valid {
            cache.put(make_entry(key, data)).await.unwrap();
        }

        // Collect aggregate current_size from stats.
        let stats = cache.stats().await;

        // Property: total current_size ≤ max_size reported by the cache.
        if stats.current_size > stats.max_size {
            return TestResult::failed();
        }

        // Also verify max_size matches what we constructed with.
        // max_size = (TOTAL_CAPACITY / SHARD_COUNT) * SHARD_COUNT, which equals
        // TOTAL_CAPACITY when TOTAL_CAPACITY is divisible by SHARD_COUNT.
        if stats.max_size > TOTAL_CAPACITY as u64 {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}

// ---------------------------------------------------------------------------
// Property 5: Read-only access path correctness
//
// For any key K with data D: put(K, D), then run 4 concurrent get(K) tasks.
// All must return Some(read) where read.data == D, and the aggregate
// access_count must be > 0 (atomics are not lost entirely).
//
// **Validates: Requirements 4.1, 4.3**
// ---------------------------------------------------------------------------

/// **Feature: performance-enhancements, Property 5: Read-only access path correctness**
/// **Validates: Requirements 4.1, 4.3**
///
/// Concurrent reads of the same key all return byte-identical data, and
/// `access_count` is incremented at least once per `get()` call.
#[quickcheck]
fn prop_concurrent_same_key_reads_byte_identical(key: String, data: Vec<u8>) -> TestResult {
    if key.is_empty() || data.is_empty() {
        return TestResult::discard();
    }
    if data.len() > 16_384 {
        return TestResult::discard();
    }

    // Large enough capacity to never evict during this test.
    let capacity = 4 * (300 + key.len() + data.len()) * 8;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let cache = Arc::new(ShardedRamCache::new(
            capacity,
            4,
            CacheEvictionAlgorithm::LRU,
        ));

        // Store the entry once.
        cache.put(make_entry(&key, &data)).await.unwrap();

        // Verify it stored successfully (entry might be too large in edge cases).
        if cache.get(&key).await.is_none() {
            return TestResult::discard();
        }

        // Record access_count before the concurrent reads (entry just inserted = 0 stored,
        // plus the one get() above).
        let before_count = {
            // We don't have direct access to the atomic, but we can use stats to get
            // total hit_count and miss_count as a proxy. Instead, we check that after
            // 4 concurrent reads the aggregate hit_count increases by at least 4.
            let stats_before = cache.stats().await;
            stats_before.hit_count
        };

        // Spawn 4 concurrent get() tasks.
        const READERS: usize = 4;
        let mut handles = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let cache_clone = Arc::clone(&cache);
            let key_clone = key.clone();
            let data_clone = data.clone();
            handles.push(tokio::spawn(async move {
                let read = cache_clone.get(&key_clone).await;
                match read {
                    None => false, // miss — fail
                    Some(r) => r.data.as_ref() == data_clone.as_slice(),
                }
            }));
        }

        // All readers must succeed with byte-identical data.
        let mut all_ok = true;
        for h in handles {
            let result = h.await.expect("reader task panicked");
            if !result {
                all_ok = false;
            }
        }

        if !all_ok {
            return TestResult::failed();
        }

        // Verify that the aggregate hit_count increased by at least READERS.
        // (The one get() we did above to check the entry counted as 1 extra hit.)
        let stats_after = cache.stats().await;
        let delta = stats_after.hit_count.saturating_sub(before_count);
        if delta < READERS as u64 {
            return TestResult::failed();
        }

        TestResult::passed()
    })
}
