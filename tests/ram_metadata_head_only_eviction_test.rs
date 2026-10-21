//! `ram-tier-effectiveness:R3.10c`, guarding `ram-tier-effectiveness:R1.8` — `tasks.md` task 11.
//!
//! A HEAD_Only_Entry (a `NewCacheMetadata` carrying `ranges: []`) must be **counted** toward
//! `MetadataCacheConfig::max_entries` and must be **evicted** by the LRU like any other entry.
//! Neither half is exempt.
//!
//! # Expected to pass on current code, and what it is therefore for
//!
//! `MetadataCache::put_with_mtime`'s eviction is count-based and inspects nothing about the
//! document it evicts, so this passes today. It is a **guard**: `ram-tier-effectiveness:R1.6`'s
//! task 6 applied one admission rule at eight `metadata_cache.put` sites, and the natural next
//! refactor is to move that rule inside `put_with_mtime` itself. `ram_admission_permitted`'s own
//! doc comment records `R1.8` — this property — as the first of three reasons that refactor did
//! not land. So this file is the thing that has to exist before it can.
//!
//! # This fixture calls `put` directly, and that is the documented route rather than a shortcut
//!
//! `metadata_cache::ram_admission_permitted` (whose whole body is `!metadata.ranges.is_empty()`)
//! lives **outside** `put_with_mtime`, at each of the eight call sites. A HEAD_Only_Entry
//! therefore cannot be made resident through any production path — which is
//! `ram-tier-effectiveness:R1.6` working — and the only way to build the population this
//! requirement is about is to call the tier's own `put`. That is precisely the case
//! `ram_admission_permitted`'s doc comment anticipates: *"inside `put_with_mtime` this predicate
//! would refuse those entries outright, so that fixture could not be built and the guard the
//! spec wants before the refactor could never be written."*
//!
//! The consequence for how to read a failure here: this file says nothing about admission, and
//! must not be read as endorsing a resident HEAD_Only_Entry. Admission is
//! `tests/ram_metadata_masking_regression_test.rs`'s subject, on both of the sites that file
//! covers.
//!
//! # Incidental coverage already exists, and that is the reason this file is NOT redundant
//!
//! `metadata_cache.rs`'s inline `test_lru_eviction` fills a 3-entry tier using
//! `create_test_metadata`, which builds `ranges: Vec::new()` — so it already evicts
//! HEAD_Only_Entries, and the red side below reds it too. But its subject is LRU **ordering**,
//! its name says so, and nothing in it states or asserts that its documents are empty-`ranges`.
//! Switching it to the sibling helper `ranges_bearing_test_metadata` — a one-word edit, for
//! reasons having nothing to do with this requirement — would delete the coverage silently and
//! no assertion anywhere would notice.
//!
//! What this file adds is therefore not a new mechanism but a **named and pinned** one: the
//! empty-`ranges` shape is asserted as a fixture precondition (`assert_head_only_shape`), so a
//! future edit that stops building HEAD_Only_Entries fails here saying so instead of quietly
//! reducing what is covered.
//!
//! # Why the assertions are what they are
//!
//! `tasks.md` task 11 is explicit that the **eviction** must be asserted, "not merely that
//! nothing exempts them", so that the test can fail. Four things are read, and each names the
//! thing `put_with_mtime` actually does:
//!
//! - `len()` rising by exactly one per `put` — the size-accounting half. A HEAD_Only_Entry
//!   occupies a slot; it is not admitted-and-uncounted.
//! - `metrics().evictions` rising by exactly one across the over-capacity `put` — the counter
//!   `put_with_mtime` increments inside the `entries.remove(&victim_key).is_some()` arm, so it
//!   distinguishes "an entry was removed" from "the loop ran".
//! - `peek(victim).is_none()` — **which** entry went. `evictions` alone cannot say that, and an
//!   eviction loop that removed the wrong entry would satisfy it.
//! - `peek(touched).is_some()` where `touched` was inserted **first** — the LRU bookkeeping
//!   applies to empty-`ranges` entries. `MetadataCache::get`'s `update_lru_order` is a second
//!   place a content-dependent path could skip them, and a victim-by-insertion-order assertion
//!   would pass while it was broken.
//!
//! `peek` rather than `get` for every survivor check: `get` reorders the LRU, which is the thing
//! under observation.
//!
//! # Red side
//!
//! `put_with_mtime`'s eviction loop made to `continue` past a victim whose `ranges` is empty —
//! a content-dependent path that drains `lru_order` without removing anything, which is the
//! exact shape task 11 names. See
//! the investigation record for the verbatim panic, the
//! `grep -c` confirming the defect was in the tree before the result was read, the
//! `pgrep -fl cargo` checks at apply and at read, and the post-revert `diff` against a named
//! commit.
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use s3_proxy::metadata_cache::{MetadataCache, MetadataCacheConfig, MetadataTtls};
use std::time::{Duration, SystemTime};

/// Small enough to fill cheaply, large enough that "the victim" and "the touched entry" and
/// "the survivors" are three distinct sets.
const MAX_ENTRIES: usize = 4;

/// Non-zero. A zero-length object is the benign producer of an empty-`ranges` document; the
/// harmful HEAD_Only_Entry describes a real object whose ranges live on disk.
const OBJECT_LEN: u64 = 4096;

const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

/// Well clear of anything this test does, so no entry can go stale mid-run and make a `get`
/// return `None` for a freshness reason rather than an eviction one.
const REFRESH_INTERVAL: Duration = Duration::from_secs(600);

fn tier() -> MetadataCache {
    MetadataCache::new(MetadataCacheConfig {
        enabled: true,
        refresh_interval: REFRESH_INTERVAL,
        max_entries: MAX_ENTRIES,
    })
}

/// Both non-zero, so `MetadataTtls::forbids_admission` cannot refuse for a TTL reason — under
/// which "the entry is not resident" would not be eviction.
fn generous_ttls() -> MetadataTtls {
    MetadataTtls {
        get_ttl: Duration::from_secs(3600),
        head_ttl: Duration::from_secs(60),
    }
}

fn key(index: usize) -> String {
    format!("rt11-eviction-bucket/head-only-{}.bin", index)
}

/// A HEAD_Only_Entry: correct and current about the object's identity, carrying no ranges.
/// `head_expires_at`/`head_cached_at` are set because that is what makes it a HEAD entry rather
/// than a document that merely happens to have no ranges.
fn head_only_metadata(cache_key: &str) -> NewCacheMetadata {
    let now = SystemTime::now();
    NewCacheMetadata {
        cache_key: cache_key.to_string(),
        object_metadata: ObjectMetadata::new(
            format!("\"etag-{}\"", cache_key),
            LAST_MODIFIED.to_string(),
            OBJECT_LEN,
            Some("application/octet-stream".to_string()),
        ),
        ranges: Vec::new(),
        created_at: now,
        expires_at: now + Duration::from_secs(3600),
        compression_info: CompressionInfo::default(),
        head_expires_at: Some(now + Duration::from_secs(60)),
        head_last_accessed: Some(now),
        head_access_count: 0,
        head_cached_at: Some(now),
        last_validated_at: None,
    }
}

/// The fixture precondition this file exists to pin. Without it the test is `test_lru_eviction`
/// again: correct, and silently no longer about HEAD_Only_Entries the moment someone changes the
/// document builder.
fn assert_head_only_shape(metadata: &NewCacheMetadata) {
    assert!(
        metadata.ranges.is_empty(),
        "FIXTURE: the document for {} carries {} range(s), so it is not a HEAD_Only_Entry and \
         this test is no longer about the population R1.8 names. `ranges.is_empty()` is the \
         whole body of metadata_cache::ram_admission_permitted, so this is the same predicate \
         every admission site applies",
        metadata.cache_key,
        metadata.ranges.len()
    );
    assert_eq!(
        metadata.object_metadata.content_length, OBJECT_LEN,
        "FIXTURE: the HEAD_Only_Entry must describe a non-zero-length object. A zero-length \
         object is the benign producer of an empty-ranges document and has no ranges on disk \
         for a resident entry to mask"
    );
    assert!(
        metadata.head_expires_at.is_some(),
        "FIXTURE: the document for {} has no head_expires_at, so it is a rangeless document \
         rather than a HEAD entry",
        metadata.cache_key
    );
}

/// Fill the tier to exactly `MAX_ENTRIES` with HEAD_Only_Entries, asserting the
/// size-accounting half on the way: each one consumes a slot.
async fn fill_to_capacity(cache: &MetadataCache) {
    for index in 0..MAX_ENTRIES {
        let k = key(index);
        let metadata = head_only_metadata(&k);
        assert_head_only_shape(&metadata);

        cache.put(&k, metadata, generous_ttls()).await;

        assert_eq!(
            cache.len().await,
            index + 1,
            "SIZE ACCOUNTING (ram-tier-effectiveness:R1.8): after putting {} HEAD_Only_Entries \
             the tier reports {} entries. An empty-ranges entry that is stored but not counted \
             toward max_entries is exempt from the bound, so the tier can grow past its \
             configured size",
            index + 1,
            cache.len().await
        );
    }

    assert_eq!(
        cache.metrics().evictions,
        0,
        "FIXTURE: {} entries into a tier of max_entries={} already evicted something, so the \
         delta measured below is not attributable to the over-capacity put",
        MAX_ENTRIES,
        MAX_ENTRIES
    );
}

/// `ram-tier-effectiveness:R3.10c`. A tier full of HEAD_Only_Entries evicts on the next `put`,
/// evicts the LRU one, and keeps the entry that was touched — even though that entry was
/// inserted first.
#[tokio::test]
async fn a_tier_full_of_head_only_entries_evicts_the_lru_entry() {
    let cache = tier();
    fill_to_capacity(&cache).await;

    // Touch the OLDEST entry, so insertion order and LRU order disagree. Without this, an
    // eviction loop that ignored `lru_order` entirely and removed the oldest insertion would
    // satisfy every assertion below.
    let touched = key(0);
    assert!(
        cache.get(&touched).await.is_some(),
        "FIXTURE: the entry to be touched is not retrievable, so the LRU order this test \
         depends on was never established"
    );

    // With key(0) now most-recently-used, key(1) is the LRU and must be the victim.
    let victim = key(1);
    let evictions_before = cache.metrics().evictions;

    let overflow = key(MAX_ENTRIES);
    let metadata = head_only_metadata(&overflow);
    assert_head_only_shape(&metadata);
    cache.put(&overflow, metadata, generous_ttls()).await;

    assert_eq!(
        cache.metrics().evictions,
        evictions_before + 1,
        "EVICTION (ram-tier-effectiveness:R3.10c): putting a {}th HEAD_Only_Entry into a tier \
         of max_entries={} did not increment the eviction counter (before={} after={}). That \
         counter is incremented inside put_with_mtime's `entries.remove(victim).is_some()` arm, \
         so a flat reading means no entry was removed — HEAD_Only_Entries are exempt from \
         eviction and the tier is unbounded for them",
        MAX_ENTRIES + 1,
        MAX_ENTRIES,
        evictions_before,
        cache.metrics().evictions
    );

    assert_eq!(
        cache.len().await,
        MAX_ENTRIES,
        "EVICTION (ram-tier-effectiveness:R1.8): the tier holds {} entries against \
         max_entries={}. The bound is not being honoured for empty-ranges entries",
        cache.len().await,
        MAX_ENTRIES
    );

    assert!(
        cache.peek(&victim).await.is_none(),
        "EVICTION (ram-tier-effectiveness:R3.10c): the LRU HEAD_Only_Entry ({}) is still \
         resident after an over-capacity put. The eviction counter and the length can both be \
         satisfied by removing the WRONG entry, which is why this assertion names the victim",
        victim
    );

    assert!(
        cache.peek(&touched).await.is_some(),
        "LRU BOOKKEEPING (ram-tier-effectiveness:R1.8): {} was inserted FIRST but touched most \
         recently, and it was evicted anyway. MetadataCache::get's update_lru_order is a second \
         place a content-dependent path could skip empty-ranges entries, and a \
         victim-by-insertion-order assertion would pass while that was broken",
        touched
    );

    for index in 2..MAX_ENTRIES {
        let survivor = key(index);
        assert!(
            cache.peek(&survivor).await.is_some(),
            "EVICTION (ram-tier-effectiveness:R3.10c): {} was evicted, but only the single LRU \
             entry should have been. More than one eviction per over-capacity put means the \
             loop's exit condition is wrong for this population",
            survivor
        );
    }
    assert!(
        cache.peek(&overflow).await.is_some(),
        "EVICTION (ram-tier-effectiveness:R3.10c): the HEAD_Only_Entry whose put triggered the \
         eviction was not itself stored"
    );
}

/// The same property once more, to show it is a steady state rather than a one-off: a second
/// over-capacity `put` evicts exactly one more entry, and the next LRU one.
///
/// This is the assertion that would catch an eviction loop which happened to remove one entry
/// on a transition into capacity and then stopped.
#[tokio::test]
async fn head_only_eviction_continues_past_the_first_over_capacity_put() {
    let cache = tier();
    fill_to_capacity(&cache).await;

    for extra in 0..3usize {
        let victim = key(extra);
        let evictions_before = cache.metrics().evictions;

        let overflow = key(MAX_ENTRIES + extra);
        let metadata = head_only_metadata(&overflow);
        assert_head_only_shape(&metadata);
        cache.put(&overflow, metadata, generous_ttls()).await;

        assert_eq!(
            cache.metrics().evictions,
            evictions_before + 1,
            "EVICTION (ram-tier-effectiveness:R3.10c): over-capacity put number {} evicted \
             nothing (before={} after={}). Eviction of HEAD_Only_Entries must be a steady \
             state, not a one-off on the transition into capacity",
            extra + 1,
            evictions_before,
            cache.metrics().evictions
        );
        assert_eq!(
            cache.len().await,
            MAX_ENTRIES,
            "EVICTION (ram-tier-effectiveness:R1.8): the tier holds {} entries against \
             max_entries={} after over-capacity put number {}",
            cache.len().await,
            MAX_ENTRIES,
            extra + 1
        );
        assert!(
            cache.peek(&victim).await.is_none(),
            "EVICTION (ram-tier-effectiveness:R3.10c): {} is still resident. Nothing is touched \
             in this test, so the victims must come out in insertion order",
            victim
        );
    }
}
