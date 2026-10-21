//! The two standing guards for the RAM **range** tier: a RAM-served response describes one
//! version, and a zero `get_ttl` key has no RAM range tier at all.
//!
//! Spec: `ram-tier-effectiveness` R3.4 and R3.5 (`tasks.md` task 14), tracing to R1.3b and R1.3c.
//!
//! # Both are expected to PASS on current code, which is the whole risk
//!
//! A guard written against behaviour that already holds is indistinguishable from a guard that
//! cannot fail, and this repository has four recorded instances of exactly that (in four fleet tests). So each assertion below names the line of `src/` its failure would come from,
//! and each carries a control arm in the same test that fails if the fixture never established the
//! state the assertion needs. `tasks.md` task 14 states the two acceptance criteria that can fail;
//! both were taken, and the verbatim failures are recorded in
//! the investigation record.
//!
//! # Scope: the RANGE tier, deliberately, and only for R3.5
//!
//! R3.5's literal text — "no RAM-derived verdict short-circuits revalidation for a zero-`get_ttl`
//! key" — must NOT be asserted against the RAM **metadata** tier. That tier's zero-`get_ttl`
//! behaviour is `MetadataTtls::forbids_admission`'s deliberate `&&`: HEAD is governed by `head_ttl`,
//! which is orthogonal to `get_ttl` by design, and the Mode B `If-Match` verdict is governed by the
//! entry's stored expiry. `ram-tier-effectiveness` task 1 corrected the requirement to say so, and
//! `get-ttl-zero-enforcement` (2.8.8/2.8.9, archived) settled it. A test asserting R3.5's literal
//! text against the metadata tier would pin behaviour the project has decided against, and the
//! cheap way to make such a test green is to relax that `&&` — so it would be worse than no test.
//! Every assertion here goes through a RANGE-tier entry point.
//!
//! R3.4 has no such split: `RamRangeHit` is a range-tier type, so its self-consistency is a
//! range-tier property and there is nothing to scope away.
//!
//! # Why the disk `.meta` is mutated directly rather than written by a second instance
//!
//! `cache-coherency-invariants.md` § "Constructing the test" requires it: a fixture that drives
//! this instance's own invalidation path passes under local hygiene — an eviction-only fix — and
//! proves nothing about the invariant. Writing the new version straight into the shared `.meta`,
//! without touching this instance's RAM, is how another proxy's consolidated write actually appears
//! here, and it is the only shape under which a stale RAM entry can still be found.
//!
//! `tests/cross_instance_ram_version_mix_test.rs` covers the same topology at the
//! `load_range_data_with_cache` level with two real `CacheManager`s. This file is deliberately one
//! level lower and one instance lighter: R3.4 is a claim about where `RamRangeHit`'s validators come
//! from, and the cheapest fixture that can refute it is the strongest one.
use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::cache_types::{NewCacheMetadata, ObjectMetadata};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::config::{MetadataCacheConfig, SharedStorageConfig};
use s3_proxy::range_handler::RangeHandler;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const KEY: &str = "test-bucket/standing-guard-object";
const RANGE_START: u64 = 0;
const RANGE_END: u64 = 1023;
const LEN: u64 = RANGE_END - RANGE_START + 1;
const ETAG_A: &str = "\"version-a-etag\"";
const ETAG_B: &str = "\"version-b-etag\"";

/// A `get_ttl` no test can outlive, so a `None` from a lookup is never an expiry.
const GENEROUS_TTL: Duration = Duration::from_secs(3600);

/// Deliberately NOT `LEN`. If `RamRangeHit::content_length` were sourced from the shared `.meta`
/// rather than from the RAM entry, the guard below would read this number instead.
const DISK_CONTENT_LENGTH_AFTER_OVERWRITE: u64 = LEN + 4_096;

fn version_a_bytes() -> Vec<u8> {
    (0..LEN).map(|i| (i % 251) as u8).collect()
}

fn object_metadata(etag: &str, content_length: u64) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag.to_string(),
        last_modified: "Tue, 23 Sep 2026 00:00:00 GMT".to_string(),
        content_length,
        content_type: Some("application/octet-stream".to_string()),
        ..ObjectMetadata::default()
    }
}

/// One proxy instance's cache stack, RAM range tier enabled.
async fn make_instance(dir: &std::path::Path) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        dir.to_path_buf(),
        true,               // ram_cache_enabled — the tier under test
        64 * 1024 * 1024,   // max_ram_cache_size
        1024 * 1024 * 1024, // max_cache_size
        CacheEvictionAlgorithm::TinyLFU,
        1024,                             // compression_threshold
        false,                            // compression_enabled
        Duration::from_secs(315_360_000), // get_ttl (~10 years)
        Duration::from_secs(3600),        // head_ttl
        Duration::from_secs(3600),        // put_ttl
        false,                            // actively_remove_cached_data
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
        Duration::from_secs(10), // ram_cache_flush_interval
        64,                      // ram_cache_shard_count
        Duration::from_secs(5),  // upstream_first_byte_timeout
    ));
    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");
    let range_handler = Arc::new(RangeHandler::new(
        cache_manager.clone(),
        disk_cache_manager.clone(),
    ));
    (cache_manager, range_handler)
}

/// Fold this instance's journal entries into a real `.meta` on the shared volume, standing in for
/// the background `JournalConsolidator`'s periodic pass. Without it there is no `.meta` at all, so
/// the "another instance overwrote it" mutation below would have nothing to mutate.
async fn consolidate(cm: &Arc<CacheManager>) {
    cm.get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured in shared-storage mode")
        .consolidate_object(KEY)
        .await
        .expect("consolidation should succeed");
}

/// Seed the disk tier with version A, consolidate it, and promote version A into this instance's
/// RAM range tier. Returns version A's bytes.
async fn seed_version_a(cm: &Arc<CacheManager>, rh: &Arc<RangeHandler>) -> Vec<u8> {
    let body_a = version_a_bytes();
    rh.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &body_a,
            object_metadata(ETAG_A, LEN),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("seed disk with version A");
    consolidate(cm).await;

    assert!(
        cm.promote_range_to_ram_cache_frame(
            KEY,
            (RANGE_START, RANGE_END),
            body_a.clone(),
            CompressionAlgorithm::None,
            ETAG_A.to_string(),
            String::new(),
            // Version A's object length, which its `.meta` above also records.
            LEN,
        ),
        "PRECONDITION MISSING: version A was not promoted into the RAM range tier, so every \
         assertion below would be about an absent entry rather than about a resident one."
    );
    body_a
}

/// Overwrite the shared `.meta` in place with version B's validators, WITHOUT touching this
/// instance's RAM tier — the way a peer proxy's consolidated write appears on this volume.
///
/// The bytes on disk are left as version A's on purpose: nothing below reads them, and rewriting
/// them would add a second variable to a fixture whose whole point is that the RAM entry is the only
/// thing the lookup consults.
fn overwrite_shared_meta_as_a_peer_would(cm: &Arc<CacheManager>) {
    let meta_path = cm.get_new_metadata_file_path(KEY);
    let raw = std::fs::read_to_string(&meta_path).unwrap_or_else(|e| {
        panic!("PRECONDITION MISSING: cannot read the consolidated .meta at {meta_path:?}: {e}")
    });
    let mut meta: NewCacheMetadata = serde_json::from_str(&raw).unwrap_or_else(|e| {
        panic!("PRECONDITION MISSING: .meta at {meta_path:?} did not parse: {e}")
    });
    meta.object_metadata.etag = ETAG_B.to_string();
    meta.object_metadata.content_length = DISK_CONTENT_LENGTH_AFTER_OVERWRITE;
    std::fs::write(
        &meta_path,
        serde_json::to_string(&meta).expect("re-serialise .meta"),
    )
    .expect("write the peer's version-B .meta");
}

/// **R3.4** — a RAM-served response's `etag` and `content-length` describe the version its bytes
/// came from, not whatever the shared `.meta` says now.
///
/// # Which line a failure comes from
///
/// `CacheManager::get_range_from_ram_cache_hit` (`src/cache.rs`) builds its `RamRangeHit` from
/// `ram_read.metadata.*` — the validators stored beside the bytes at promotion time. The defect this
/// guards against is the pre-2.8.6 shape, where a response's bytes came from the RAM range tier
/// while its headers came from an independent `get_metadata_from_disk` read, so one response could
/// carry version A's bytes under version B's ETag with nothing comparing the two. A client cannot
/// detect that: the ETag it caches, revalidates with, and passes back as `If-Match` describes bytes
/// it never received. `cache-coherency-invariants.md` states the contract as "the bytes, etag, and
/// content-length must all come from the same version".
///
/// # The two halves are not equally strong, and that is stated rather than implied
///
/// `etag` is copied from the RAM entry, so a disk-sourced etag is a one-line change and the
/// assertion has a real red side (taken; see the return file). `content_length` is computed as
/// `end - start + 1` from the lookup boundaries at promotion time, so it cannot disagree with the
/// bytes by construction — its assertion is a pin on that derivation rather than a discriminator.
/// Both are asserted because R3.4 names both; only the first is evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ram_served_range_reports_the_validators_of_the_version_its_bytes_came_from() {
    let tmp = TempDir::new().expect("tempdir");
    let (cm, rh) = make_instance(tmp.path()).await;
    let body_a = seed_version_a(&cm, &rh).await;

    // CONTROL, before the mutation: the entry is genuinely resident and reports version A.
    let before = cm
        .get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, GENEROUS_TTL)
        .expect(
            "PRECONDITION MISSING: the promoted range is not visible to \
             get_range_from_ram_cache_hit under a generous get_ttl, so the assertion after the \
             overwrite would pass on an absent entry",
        );
    assert_eq!(
        before.etag, ETAG_A,
        "PRECONDITION MISSING: the RAM hit does not report version A's ETag even before any \
         overwrite, so this fixture cannot distinguish a RAM-sourced validator from a \
         disk-sourced one"
    );

    overwrite_shared_meta_as_a_peer_would(&cm);

    // CONTROL, after the mutation: the shared `.meta` really does report version B now. Without
    // this, a version-A reading below is equally consistent with "the peer's write never landed",
    // which is that mistake — a discriminator that makes the wrong answer look evidenced.
    let disk = cm
        .get_metadata_from_disk(KEY)
        .await
        .expect("read the shared .meta")
        .expect("the shared .meta must exist");
    assert_eq!(
        disk.object_metadata.etag, ETAG_B,
        "PRECONDITION MISSING: the direct .meta mutation did not land, so the guard below would \
         compare version A against version A and could not fail"
    );
    assert_eq!(
        disk.object_metadata.content_length, DISK_CONTENT_LENGTH_AFTER_OVERWRITE,
        "PRECONDITION MISSING: the direct .meta mutation did not change content_length, so the \
         content-length half of this guard would compare LEN against LEN"
    );

    let hit = cm
        .get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, GENEROUS_TTL)
        .expect(
            "PRECONDITION MISSING: the RAM entry disappeared across a direct .meta mutation. \
             Nothing in this fixture touches the RAM tier, so if this fires the fixture — not the \
             invariant — is what changed",
        );

    assert_eq!(
        hit.data.as_ref(),
        body_a.as_slice(),
        "R3.4: the RAM range tier served bytes that are neither version A's nor anything this \
         fixture wrote"
    );
    assert_eq!(
        hit.etag, ETAG_A,
        "R3.4 VIOLATED: the RAM hit carries version A's bytes and reports ETag {} — the shared \
         .meta's current version, not the version these bytes came from. That is the version-mix \
         defect: a client cannot detect it, because the ETag it caches and revalidates with \
         describes bytes it never received. RamRangeHit's validators must come from \
         ram_read.metadata, beside the bytes, and never from a second metadata source.",
        hit.etag
    );
    assert_eq!(
        hit.content_length,
        LEN,
        "R3.4 VIOLATED: the RAM hit reports content-length {} for {} bytes of version-A data. \
         Expected version A's object length {LEN}, which its RAM entry recorded at promotion; {} \
         is what the shared .meta says after the peer's overwrite.",
        hit.content_length,
        hit.data.len(),
        DISK_CONTENT_LENGTH_AFTER_OVERWRITE
    );
}

/// **R3.5, scoped to the RANGE tier** — a zero `get_ttl` leaves no RAM-derived verdict to
/// short-circuit revalidation with.
///
/// # Which line a failure comes from
///
/// Both range-tier lookups — `CacheManager::get_range_from_ram_cache_hit` and
/// `CacheManager::get_range_from_ram_cache` (`src/cache.rs`) — return `None` when the resolved
/// `get_ttl` is zero, before any shard lookup. The gate lives in the callees rather than at their
/// call sites precisely so a later caller cannot omit it: it was a doc-comment contract on
/// `ram_verify` that three callers honoured in two different idioms, with nothing failing if a
/// fourth did not (`get-ttl-zero-enforcement` R4.2).
///
/// `get_ttl: 0` is an **authorization** boundary, not only a freshness one. Operators set it so that
/// S3 evaluates IAM on every read, so a locally-answered response for such a key is a read S3 never
/// authorized. This is the one case where "the staleness window is small" is not a defence, which is
/// why the assertion is that the tier is unavailable rather than that it is bounded.
///
/// # Why both arms, and why the residency control is not optional
///
/// A `None` under a zero TTL is exactly what an absent entry, a failed promotion or a disabled tier
/// also produce. The generous-TTL control immediately above each assertion is what makes the `None`
/// attributable to the gate — the same entry, the same call, one argument different.
///
/// # What this test does NOT cover, deliberately
///
/// The RAM **metadata** tier (see this file's header) and `load_range_data_with_cache`, which takes
/// no `get_ttl` at all: its RAM arm is gated on the global flag plus a caller-supplied etag, and the
/// zero-`get_ttl` half for that path is closed upstream instead, by `bucket_settings` resolving
/// `ram_cache_eligible: false` for such keys (already covered by
/// `bucket_settings::tests`). One residual path is knowingly open — `get_ttl: 0` plus
/// `page_widening` plus a journal-only write with no `.meta` yet — and is owned by
/// `page-widening-freshness` R3. Asserting against it here would pin a gap as if it were a contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_get_ttl_leaves_the_ram_range_tier_with_no_verdict_to_give() {
    let tmp = TempDir::new().expect("tempdir");
    let (cm, rh) = make_instance(tmp.path()).await;
    let body_a = seed_version_a(&cm, &rh).await;

    // CONTROL for the hit-bearing lookup: resident, and serving the right bytes.
    let hit = cm
        .get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, GENEROUS_TTL)
        .expect(
            "PRECONDITION MISSING: the range is not resident in the RAM tier at all, so the \
             zero-TTL assertion below would pass without exercising the gate",
        );
    assert_eq!(
        hit.data.as_ref(),
        body_a.as_slice(),
        "PRECONDITION MISSING: the resident RAM entry does not hold the seeded bytes"
    );

    // CONTROL for the bytes-only lookup, which production also reaches.
    assert_eq!(
        cm.get_range_from_ram_cache(KEY, RANGE_START, RANGE_END, GENEROUS_TTL)
            .as_deref(),
        Some(body_a.as_slice()),
        "PRECONDITION MISSING: get_range_from_ram_cache does not see the resident range under a \
         generous get_ttl"
    );

    // THE GUARD. Same entry, same boundaries, same process, one argument different.
    assert!(
        cm.get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, Duration::ZERO)
            .is_none(),
        "R3.5 VIOLATED: get_range_from_ram_cache_hit returned a RAM-derived hit for a \
         zero-get_ttl key. That hit is the verdict the mainline GET paths serve on, so returning \
         it short-circuits the revalidation the operator required — and get_ttl: 0 is an \
         authorization boundary, so the response is a read S3 never authorized. The gate belongs \
         in this function, not at its three call sites."
    );
    assert!(
        cm.get_range_from_ram_cache(KEY, RANGE_START, RANGE_END, Duration::ZERO)
            .is_none(),
        "R3.5 VIOLATED: get_range_from_ram_cache returned bytes for a zero-get_ttl key. The \
         sibling gate in get_range_from_ram_cache_hit is not enough on its own — both range-tier \
         lookups are reachable from production and both must refuse."
    );
}

/// A disk hit on an offset-0 extent shorter than the object, promoted to RAM by the tiered
/// load (`CacheManager::load_range_data_with_cache`, the buffered serve's and the merge path's
/// loader), must record the object's length, not the extent's: the RAM range-hit serve reports
/// it as `Content-Range`'s instance length, and an offset-0 extent's own length passes the
/// serve's coverage filter.
// Multi-threaded: the RAM-hit lookup blocks in place on the RAM tier.
#[tokio::test(flavor = "multi_thread")]
async fn a_tiered_disk_hit_promotion_records_the_objects_length() {
    const OBJECT_LEN: u64 = 4 * LEN;
    let dir = TempDir::new().unwrap();
    let (cm, rh) = make_instance(dir.path()).await;
    rh.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &version_a_bytes(),
            object_metadata(ETAG_A, OBJECT_LEN),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("seed an offset-0 extent of a longer object");
    consolidate(&cm).await;
    // What a range serve reads before it loads the extent.
    let meta = cm
        .get_metadata_cached(KEY)
        .await
        .expect("metadata read")
        .expect("PRECONDITION MISSING: no .meta after consolidation");
    let range = meta
        .ranges
        .iter()
        .find(|r| r.start == RANGE_START && r.end == RANGE_END)
        .cloned()
        .expect("PRECONDITION MISSING: the consolidated .meta does not list the extent");
    let range = s3_proxy::cache::Range {
        start: range.start,
        end: range.end,
        data: Vec::new(),
        etag: meta.object_metadata.etag.clone(),
        last_modified: meta.object_metadata.last_modified.clone(),
        compression_algorithm: range.compression_algorithm.clone(),
    };
    let (data, ram_hit) = cm
        .load_range_data_with_cache(KEY, &range, &rh)
        .await
        .expect("disk load");
    assert!(
        !ram_hit,
        "PRECONDITION MISSING: the first load must be a disk hit"
    );
    assert_eq!(data.len() as u64, LEN);
    let hit = cm
        .get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, Duration::from_secs(3600))
        .expect("PRECONDITION MISSING: the tiered disk hit did not promote the extent to RAM");
    assert_eq!(
        hit.content_length, OBJECT_LEN,
        "a RAM entry promoted by the tiered load for bytes {RANGE_START}-{RANGE_END} of a \
         {OBJECT_LEN}-byte object records content_length {}; the RAM-hit serve would answer \
         Content-Range bytes {RANGE_START}-{RANGE_END}/{}",
        hit.content_length, hit.content_length
    );
}

/// The decline side of the tiered promotion: with this instance's RAM copy of the `.meta` cold,
/// the object's length is unknown, so the promoted entry records 0 and the RAM-hit serve's
/// coverage filter (`content_length > range.end`) declines it. The extent still loads from disk
/// with the correct bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_tiered_promotion_with_a_cold_metadata_cache_records_zero_and_is_declined() {
    const OBJECT_LEN: u64 = 4 * LEN;
    let dir = TempDir::new().unwrap();
    let (cm, rh) = make_instance(dir.path()).await;
    rh.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &version_a_bytes(),
            object_metadata(ETAG_A, OBJECT_LEN),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("seed an offset-0 extent of a longer object");
    consolidate(&cm).await;
    // Read the `.meta` from disk, not through `get_metadata_cached`, so the RAM copy stays cold.
    let meta = cm
        .get_metadata_from_disk(KEY)
        .await
        .expect("metadata read")
        .expect("PRECONDITION MISSING: no .meta after consolidation");
    let listed = meta
        .ranges
        .iter()
        .find(|r| r.start == RANGE_START && r.end == RANGE_END)
        .cloned()
        .expect("PRECONDITION MISSING: the consolidated .meta does not list the extent");
    let range = s3_proxy::cache::Range {
        start: listed.start,
        end: listed.end,
        data: Vec::new(),
        etag: meta.object_metadata.etag.clone(),
        last_modified: meta.object_metadata.last_modified.clone(),
        compression_algorithm: listed.compression_algorithm.clone(),
    };
    let (data, ram_hit) = cm
        .load_range_data_with_cache(KEY, &range, &rh)
        .await
        .expect("disk load");
    assert!(
        !ram_hit,
        "PRECONDITION MISSING: the first load must be a disk hit"
    );
    assert_eq!(
        data,
        version_a_bytes(),
        "the extent must still load from disk intact"
    );
    let hit = cm
        .get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, GENEROUS_TTL)
        .expect("PRECONDITION MISSING: the tiered disk hit did not promote the extent to RAM");
    assert_eq!(
        hit.content_length, 0,
        "with the RAM copy of the .meta cold the promotion records 0, not the extent's length {LEN}"
    );
    assert!(
        hit.content_length <= RANGE_END,
        "the RAM-hit serve's coverage filter must decline this entry"
    );
}
