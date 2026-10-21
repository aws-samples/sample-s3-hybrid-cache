//! `ram-tier-effectiveness` task 6 — admission unification at the four **unguarded** census
//! sites, plus the dead `MetadataCache::load_with_retry` contract pin.
//!
//! # What this file asserts, and how it differs from the two files either side of it
//!
//! `tests/ram_metadata_masking_regression_test.rs` (task 7) asserts the same property at
//! census **row 1** — the site that has carried the `!ranges.is_empty()` admission test since
//! v1.9.4 — and its own `UNRESOLVED` note records that the property is
//! *live-violated* at the unguarded rows. This file closes that gap: one assertion per
//! unguarded site, each reading **residency in the RAM metadata tier** after driving the site.
//!
//! `tasks.md` task 10 owns the masking *consequence* (HEAD, then a peer's consolidation, then
//! a GET whose ranges are on disk), and that test is deliberately **not** here. The predicate
//! below is admission, which is what the change actually alters; the consequence is one
//! inference further out and belongs to its own unit.
//!
//! # Why residency is the predicate, and the two figures it deliberately is not
//!
//! The entry admitted here is correct about etag, length and `Last-Modified` and wrong about
//! exactly one field, `ranges`, in one direction. So:
//!
//! - **A response is byte-identical** in the admitted and withheld cases. Only the cost of a
//!   later range lookup differs. An assertion on bytes cannot fail.
//! - **A hit-rate or request counter cannot discriminate it.** A masked lookup is a cache miss
//!   by every counter that exists, indistinguishable from a cold miss, and
//!   `MetadataCache::get` moves `hits`/`misses`/`stale_refreshes` on every call including this
//!   fixture's own apparatus reads.
//!
//! Residency is the value the guard branches on, read one step after the branch.
//!
//! # The two vacuity traps, and the guard for each
//!
//! - **Admission may be impossible here for an unrelated reason**, in which case "not
//!   resident" is not the guard acting. `MetadataTtls::forbids_admission` refuses outright when
//!   both TTLs are zero, and `MetadataCacheConfig::enabled` can switch the tier off. Every test
//!   below asserts both resolved TTLs are non-zero AND drives a **ranges-bearing control key
//!   through the same function on the same manager in the same run**, which must become
//!   resident. A subject absent from a tier that just admitted the control is the guard.
//! - **A fixture whose state never reaches the guard.** A warm ranges-bearing entry never fires
//!   it. `requirements.md` enumerates five live producers of an empty-`ranges` `.meta`; the
//!   subject here is **producer 1**, `CacheManager::create_head_only_metadata`, reached by
//!   calling `store_head_cache_entry_unified` against a key with no `.meta` on disk — a state
//!   production creates, for a **non-zero-length** object, which is the harmful case rather
//!   than the benign zero-byte one.
//!
//! Each test also asserts that the site it drives **ran** (the `.meta`'s `expires_at` moved,
//! the backfill returned `Ok(true)`, the HEAD lookup returned an entry). Without that, a site
//! that bailed out early would leave nothing resident and the assertion would pass for the
//! wrong reason — and two of these sites swallow their own errors.
//!
//! # Red side
//!
//! Every assertion below was observed **failing against the unmodified tree** before the
//! guard was added, and again against the guard removed by an inverse-patch driver afterwards.
//! See the investigation record for the verbatim panics, the
//! `grep -c` confirming the guard's absence in the tree before each result was read, the
//! `pgrep -fl cargo` checks at apply and at read, and the post-revert `diff -q`.
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CacheMetadata, NewCacheMetadata, ObjectMetadata};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::metadata_cache::MetadataTtls;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// HEAD-first, so `store_head_cache_entry_unified` takes its `create_head_only_metadata` arm
/// and leaves `ranges: []` on disk (producer 1).
const SUBJECT_KEY: &str = "rt6-admission-bucket/head-only-subject.bin";
/// Ranges-bearing from the start, so it MUST be admitted by the same function in the same run.
const CONTROL_KEY: &str = "rt6-admission-bucket/ranges-bearing-control.bin";

/// Non-zero: the zero-length producers are the benign ones, and a zero-length document changes
/// what downstream range lookups report for reasons unrelated to admission.
const OBJECT_LEN: u64 = 4096;

/// Deliberately not the empty object's MD5 — an entry claiming that ETag alongside a positive
/// length is refused by a separate pre-3.0.0 compatibility guard.
const SUBJECT_ETAG: &str = "\"rt6-subject-etag-v1\"";
const CONTROL_ETAG: &str = "\"rt6-control-etag-v1\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";
const BACKFILL_LAST_MODIFIED: &str = "Thu, 02 Jan 2025 00:00:00 GMT";

const TTL: Duration = Duration::from_secs(3600);

struct Fixture {
    _dir: TempDir,
    /// The instance under test. Owns the RAM metadata tier the assertions read.
    cache: Arc<CacheManager>,
    /// A `DiskCacheManager` over the same cache directory, used only to seed the control's
    /// ranges. It holds no `MetadataCache`, so seeding cannot populate the tier under test and
    /// the control's residency is attributable to the function being driven.
    seeder: DiskCacheManager,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let cache_dir: PathBuf = dir.path().to_path_buf();

        // `new_with_defaults` resolves both TTLs to an hour and takes
        // `MetadataCacheConfig::default()` (enabled, 5s `refresh_interval`). Both are asserted
        // rather than assumed — see `assert_apparatus`.
        let cache = Arc::new(CacheManager::new_with_defaults(cache_dir.clone(), false, 0));

        // Compression off so `.bin` arithmetic never depends on a ratio.
        let seeder = DiskCacheManager::new(cache_dir.clone(), false, 1024, false, 1_048_576);
        seeder
            .initialize()
            .await
            .expect("the seeding disk cache manager must initialize");

        Fixture {
            _dir: dir,
            cache,
            seeder,
        }
    }

    /// `Some` iff an entry for the key is resident and inside `refresh_interval`.
    async fn resident(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        self.cache.get_metadata_cache().get(cache_key).await
    }

    fn meta_path(&self, cache_key: &str) -> PathBuf {
        self.cache.get_new_metadata_file_path(cache_key)
    }

    /// Seed the subject through producer 1 and assert the state it left on disk.
    async fn seed_subject_head_only(&self) -> PathBuf {
        self.cache
            .store_head_cache_entry_unified(
                SUBJECT_KEY,
                head_response_headers(SUBJECT_ETAG),
                head_cache_metadata(SUBJECT_ETAG),
            )
            .await
            .expect("the HEAD-only seed must succeed");

        let path = self.meta_path(SUBJECT_KEY);
        let on_disk = read_meta(&path);
        assert!(
            on_disk.ranges.is_empty(),
            "FIXTURE: the seed was supposed to take create_head_only_metadata's arm and leave \
             ranges: [] on disk, but the .meta carries {} range(s). Producer 1 is no longer the \
             state under test",
            on_disk.ranges.len()
        );
        assert_eq!(
            on_disk.object_metadata.content_length, OBJECT_LEN,
            "FIXTURE: the HEAD-only document must describe a non-zero-length object"
        );
        assert!(
            self.resident(SUBJECT_KEY).await.is_none(),
            "FIXTURE: store_head_cache_entry_unified published the subject to the RAM metadata \
             tier. Census row 4 is supposed to decline (its Err arm yields None), so the \
             subject must not be resident before the site under test runs — otherwise the \
             assertion reads row 4's decision rather than the site's"
        );

        path
    }

    /// Seed the control's ranges on disk, through a manager that cannot touch the RAM tier.
    async fn seed_control_ranges(&mut self) {
        let body = vec![0xC3u8; OBJECT_LEN as usize];
        self.seeder
            .store_range(
                CONTROL_KEY,
                0,
                OBJECT_LEN - 1,
                &body,
                object_metadata(CONTROL_ETAG),
                TTL,
                false,
            )
            .await
            .expect("the control's range publish must succeed");

        let on_disk = read_meta(&self.meta_path(CONTROL_KEY));
        assert!(
            !on_disk.ranges.is_empty(),
            "FIXTURE: the control must carry ranges on disk, or it cannot prove the tier admits"
        );
        assert!(
            self.resident(CONTROL_KEY).await.is_none(),
            "FIXTURE: seeding the control populated the RAM metadata tier. The seeder holds no \
             MetadataCache, so this means the fixture changed shape and the control's residency \
             below would not be attributable to the function under test"
        );
    }
}

fn head_response_headers(etag: &str) -> HashMap<String, String> {
    // No `content-range` and no `x-amz-mp-parts-count`: a part-scoped response is refused
    // outright by `store_head_cache_entry_unified`, which would fail the seed.
    let mut headers = HashMap::new();
    headers.insert("etag".to_string(), etag.to_string());
    headers.insert("last-modified".to_string(), LAST_MODIFIED.to_string());
    headers.insert("content-length".to_string(), OBJECT_LEN.to_string());
    headers.insert(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    );
    headers
}

fn head_cache_metadata(etag: &str) -> CacheMetadata {
    CacheMetadata {
        etag: etag.to_string(),
        // `validate_head_cache_inputs` rejects an empty ETag or Last-Modified.
        last_modified: LAST_MODIFIED.to_string(),
        content_length: OBJECT_LEN,
        part_number: None,
        cache_control: None,
        access_count: 0,
        last_accessed: std::time::SystemTime::now(),
    }
}

fn object_metadata(etag: &str) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag.to_string(),
        last_modified: LAST_MODIFIED.to_string(),
        content_length: OBJECT_LEN,
        content_type: Some("application/octet-stream".to_string()),
        ..Default::default()
    }
}

fn read_meta(path: &Path) -> NewCacheMetadata {
    let raw = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("the .meta must be readable at {:?}: {}", path, e));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("the .meta at {:?} must parse: {}", path, e))
}

fn write_meta(path: &Path, metadata: &NewCacheMetadata) {
    let raw = serde_json::to_string(metadata).expect("the .meta must serialise");
    std::fs::write(path, raw).expect("the .meta must be writable");
}

/// Everything that has to hold before any residency reading means anything.
async fn assert_apparatus(f: &Fixture) {
    for key in [SUBJECT_KEY, CONTROL_KEY] {
        assert!(
            !f.cache.get_effective_get_ttl(key).await.is_zero(),
            "APPARATUS: resolved get_ttl for {} is zero, so MetadataTtls::forbids_admission \
             would refuse admission for a reason unrelated to the ranges guard",
            key
        );
        assert!(
            !f.cache.get_effective_head_ttl(key).await.is_zero(),
            "APPARATUS: resolved head_ttl for {} is zero, so MetadataTtls::forbids_admission \
             would refuse admission for a reason unrelated to the ranges guard",
            key
        );
    }
}

/// Census **row 5**, `CacheManager::get_head_cache_entry_unified`'s disk tier. The site
/// `requirements.md` traces to the live v1.9.4 masking defect: it publishes whatever
/// `read_new_cache_metadata_from_disk` returned, before the freshness check and with no
/// `ranges` test.
///
/// `ram-tier-effectiveness:R1.6`.
#[tokio::test]
async fn row5_head_disk_tier_withholds_an_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    f.seed_control_ranges().await;
    f.seed_subject_head_only().await;

    // The control first, through the same function on the same manager: a ranges-bearing
    // document read by row 5's disk tier MUST still be admitted. This is both the apparatus
    // proof and the guard's width — an over-broad change fails here.
    let control_head = f
        .cache
        .get_head_cache_entry_unified(CONTROL_KEY, TTL)
        .await
        .expect("the control's HEAD lookup must not error");
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS/WIDTH: a ranges-bearing document read through \
         get_head_cache_entry_unified's disk tier was NOT admitted to the RAM metadata tier. \
         Either admission is impossible here for a reason other than the ranges guard (tier \
         disabled, TTLs, eviction), in which case the subject assertion below proves nothing, \
         or the guard is over-broad and has cost the RAM tier an entry it should hold. HEAD \
         lookup returned: {:?}",
        control_head.is_some()
    );

    // THE SITE. A RAM miss falls through to the disk tier, which reads the empty-`ranges`
    // document and decides whether to publish it.
    let subject_head = f
        .cache
        .get_head_cache_entry_unified(SUBJECT_KEY, TTL)
        .await
        .expect("the subject's HEAD lookup must not error");
    assert!(
        subject_head.is_some(),
        "FIXTURE/R1.7: the HEAD-only entry must still answer the HEAD from the disk tier. If it \
         does not, the site under test returned before its publish decision and the residency \
         reading below is not about admission. R1.7 also forbids this change from altering \
         which entries are HEAD-fresh"
    );

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "CENSUS ROW 5: an empty-ranges document was admitted to the RAM metadata tier by \
         get_head_cache_entry_unified's disk tier. The control was admitted by the same \
         function in this same run, so the tier is live and this is the missing \
         !ranges.is_empty() admission test, not an apparatus failure. Every range lookup on \
         this key is now answered from a document that says no ranges exist, for up to one \
         metadata_cache.refresh_interval, regardless of what another instance has added to the \
         shared .meta"
    );
}

/// Census **row 8**, `CacheManager::update_metadata_expiration_unified`, reached from
/// `CacheManager::refresh_cache_ttl`. Task 6a graded this the MEDIUM defect: its only
/// precondition is that the `.meta` file exists — no coverage, `Last-Modified` or write-cache
/// condition — and it publishes the whole deserialised document.
///
/// `ram-tier-effectiveness:R1.6`.
#[tokio::test]
async fn row8_ttl_refresh_withholds_an_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    f.seed_control_ranges().await;
    let subject_meta = f.seed_subject_head_only().await;

    let control_before = read_meta(&f.meta_path(CONTROL_KEY)).expires_at;
    f.cache
        .refresh_cache_ttl(CONTROL_KEY)
        .await
        .expect("the control's TTL refresh must not error");
    assert_ne!(
        read_meta(&f.meta_path(CONTROL_KEY)).expires_at,
        control_before,
        "APPARATUS: refresh_cache_ttl did not move the control's expires_at, so \
         update_metadata_expiration_unified did not run. It swallows its own errors (a warn!, \
         not an Err), so without this check a failed metadata-lock acquisition would look like \
         a clean pass"
    );
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS/WIDTH: a ranges-bearing document was NOT admitted by \
         update_metadata_expiration_unified, so either admission is impossible here for an \
         unrelated reason or the guard is over-broad"
    );

    // THE SITE, on the empty-ranges document.
    let subject_before = read_meta(&subject_meta).expires_at;
    f.cache
        .refresh_cache_ttl(SUBJECT_KEY)
        .await
        .expect("the subject's TTL refresh must not error");
    assert_ne!(
        read_meta(&subject_meta).expires_at,
        subject_before,
        "APPARATUS: refresh_cache_ttl did not move the subject's expires_at, so the site under \
         test never reached its publish decision and the residency reading below proves nothing"
    );

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "CENSUS ROW 8: an empty-ranges document was admitted to the RAM metadata tier by \
         update_metadata_expiration_unified. The control was admitted by the same function in \
         this same run. This is the MEDIUM defect task 6a established: two concurrent GETs for \
         one key on a default config reach it through the download-coalescing waiter's 304 arm"
    );
}

/// Census **row 7**, `CacheManager::backfill_write_cache_last_modified`'s non-graduated
/// (`else`) arm. Task 6a graded this a **contract pin, not a live defect**: the arm requires
/// `is_write_cached == false`, and every producer of such a document copies `Last-Modified`
/// from an S3 GET/HEAD response, so reaching it needs an origin that omits the header. The
/// producer-1 document used here has `is_write_cached: false` and a `Last-Modified`, which is
/// why the *test* can reach the arm while production cannot — the assertion pins the code's
/// behaviour, and the severity claim is about reachability, not about this reading.
///
/// `ram-tier-effectiveness:R1.6`.
#[tokio::test]
async fn row7_backfill_non_graduated_arm_withholds_an_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    f.seed_control_ranges().await;
    let subject_meta = f.seed_subject_head_only().await;

    assert!(
        f.cache
            .backfill_write_cache_last_modified(
                CONTROL_KEY,
                BACKFILL_LAST_MODIFIED.to_string(),
                None
            )
            .await
            .expect("the control's backfill must not error"),
        "APPARATUS: the control's backfill returned false (the in-lock ETag re-check refused), \
         so the site did not reach its publish decision"
    );
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS/WIDTH: a ranges-bearing document was NOT admitted by \
         backfill_write_cache_last_modified, so either admission is impossible here for an \
         unrelated reason or the guard is over-broad"
    );

    let on_disk = read_meta(&subject_meta);
    assert!(
        !on_disk.object_metadata.is_write_cached,
        "FIXTURE: the subject must have is_write_cached == false to enter the `else` arm; \
         row 6's graduated arm is a separate test"
    );
    assert!(
        f.cache
            .backfill_write_cache_last_modified(
                SUBJECT_KEY,
                BACKFILL_LAST_MODIFIED.to_string(),
                None
            )
            .await
            .expect("the subject's backfill must not error"),
        "APPARATUS: the subject's backfill returned false, so the site never reached its \
         publish decision"
    );

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "CENSUS ROW 7: an empty-ranges document was admitted to the RAM metadata tier by \
         backfill_write_cache_last_modified's non-graduated arm. The control was admitted by \
         the same function in this same run"
    );
}

/// Census **row 6**, the same function's graduated (`was_write_cached == true`) arm. Task 6a
/// graded this a LOW-severity defect: its deterministic producer is a zero-byte object, whose
/// absent ranges mask nothing, and the harmful non-zero shape needs the journal
/// consolidator's `Remove` arm.
///
/// **The `is_write_cached: true` flag here is set by editing the producer-1 document on
/// disk**, and that is a deliberate stand-in rather than an oversight: driving the
/// consolidator's `Remove` arm to strip `ranges` from a real staged object is disproportionate
/// for an admission assertion, and the *only* field this arm branches on is the flag. Every
/// other field is producer 1's. The severity claim rests on task 6a's trace, not on this
/// fixture.
///
/// `ram-tier-effectiveness:R1.6`.
#[tokio::test]
async fn row6_backfill_graduated_arm_withholds_an_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    f.seed_control_ranges().await;
    let subject_meta = f.seed_subject_head_only().await;

    // The control takes the SAME graduated arm, so the width check below is arm-for-arm with
    // the subject rather than comparing two different branches.
    let control_meta_path = f.meta_path(CONTROL_KEY);
    let mut control_doc = read_meta(&control_meta_path);
    control_doc.object_metadata.is_write_cached = true;
    write_meta(&control_meta_path, &control_doc);

    let mut subject_doc = read_meta(&subject_meta);
    subject_doc.object_metadata.is_write_cached = true;
    write_meta(&subject_meta, &subject_doc);
    assert!(
        read_meta(&subject_meta).ranges.is_empty(),
        "FIXTURE: the edit must not have added ranges"
    );

    assert!(
        f.cache
            .backfill_write_cache_last_modified(
                CONTROL_KEY,
                BACKFILL_LAST_MODIFIED.to_string(),
                None
            )
            .await
            .expect("the control's backfill must not error"),
        "APPARATUS: the control's backfill returned false, so the graduated arm did not reach \
         its publish decision"
    );
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS/WIDTH: a ranges-bearing document was NOT admitted by the graduated arm, so \
         either admission is impossible here for an unrelated reason or the guard is over-broad"
    );

    assert!(
        f.cache
            .backfill_write_cache_last_modified(
                SUBJECT_KEY,
                BACKFILL_LAST_MODIFIED.to_string(),
                None
            )
            .await
            .expect("the subject's backfill must not error"),
        "APPARATUS: the subject's backfill returned false, so the graduated arm never reached \
         its publish decision"
    );

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "CENSUS ROW 6: an empty-ranges document was admitted to the RAM metadata tier by \
         backfill_write_cache_last_modified's graduated arm. The control was admitted through \
         the same arm in this same run"
    );
}

/// Census **row 9**, `MetadataCache::load_with_retry`, reached only via
/// `MetadataCache::get_or_load`. **A contract pin, not a fix.** `get_or_load` has no
/// production caller — its only caller repo-wide is `metadata_cache.rs`'s own `#[cfg(test)]`
/// block — so nothing in the shipped binary reaches this. The assertion exists so that
/// whatever revives `get_or_load` inherits the rule rather than rediscovering it.
///
/// `ram-tier-effectiveness:R1.6`.
#[tokio::test]
async fn row9_load_with_retry_withholds_an_empty_ranges_document_contract_pin() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    let tier = f.cache.get_metadata_cache();
    let ttls = MetadataTtls {
        get_ttl: TTL,
        head_ttl: TTL,
    };

    // The control: a ranges-bearing document through the same loader shape must be admitted.
    f.seed_control_ranges().await;
    let control_doc = read_meta(&f.meta_path(CONTROL_KEY));
    tier.get_or_load(CONTROL_KEY, || async { Ok((control_doc, None)) }, ttls)
        .await
        .expect("the control's load must succeed");
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS/WIDTH: a ranges-bearing document loaded through get_or_load was NOT \
         admitted, so either admission is impossible here for an unrelated reason or the guard \
         is over-broad"
    );

    let subject_meta = f.seed_subject_head_only().await;
    let subject_doc = read_meta(&subject_meta);
    assert!(
        subject_doc.ranges.is_empty(),
        "FIXTURE: the subject document must carry no ranges"
    );
    tier.get_or_load(SUBJECT_KEY, || async { Ok((subject_doc, None)) }, ttls)
        .await
        .expect("the subject's load must succeed");

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "CENSUS ROW 9: an empty-ranges document was admitted to the RAM metadata tier by \
         load_with_retry. This is a contract pin rather than a live defect — get_or_load has no \
         production caller — so a failure here means a future caller would inherit the v1.9.4 \
         masking shape, not that the shipped binary is masking anything today"
    );
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Census rows 2 and 3, added by `RAM-TIER-ROWS-2-AND-3-RESIDENCY-TEST`.
//
// Task 6 switched these two sites to the named predicate with **no assertion of their own**, so
// a sense inversion at either would have failed nothing under `tests/`. Row 1 is covered by
// `tests/ram_metadata_masking_regression_test.rs`; rows 5-9 by the five assertions above.
//
// **Their red side is an inverse patch, not a defect.** Task 6's substitution at these two sites
// is an IDENTITY — `ram_admission_permitted`'s body is `!metadata.ranges.is_empty()`, the same
// expression both sites already had — so there is nothing to reproduce. The honest red side is
// dropping the guard at **one row at a time**, with a `grep -c` confirming the defect is in the
// tree before each run, because a single patch covering both rows cannot show that each
// assertion is attached to its own site. See
// the investigation record.
// ─────────────────────────────────────────────────────────────────────────────────────────────

/// Census **row 2**, `CacheManager::get_metadata_classified_and_publish_to_ram`'s `Parsed` arm.
///
/// **REACHABILITY: a contract pin, NOT coverage of production behaviour.** Re-derived
/// 2026-09-22 (`RAM-TIER-T8` established it first): `CacheManager::get_metadata_classified_and_publish_to_ram` has
/// **zero** `src/` callers — `grep -rn get_metadata_classified_and_publish_to_ram src/` returns its own definition
/// at `cache.rs`, one doc mention, and `DiskCacheManager::get_metadata_classified`, which is a
/// **different method on a different type** with no RAM tier at all. Every `tests/` hit for
/// that name is on the `DiskCacheManager` one, so the RAM-admitting site below is reached by
/// nothing in the repository until this assertion. Rate it accordingly: a failure here means a
/// future caller would inherit the v1.9.4 masking shape, not that the shipped binary masks
/// anything today. the cache tier verification guidance § "Establish reachability BEFORE
/// assigning severity".
///
/// `ram-tier-effectiveness:R1.6`.
#[tokio::test]
async fn row2_classified_lookup_withholds_an_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    f.seed_control_ranges().await;
    f.seed_subject_head_only().await;

    // The control first, through the same function on the same manager. Also the guard's
    // width: an over-broad change fails here.
    let control_lookup = f
        .cache
        .get_metadata_classified_and_publish_to_ram(CONTROL_KEY)
        .await
        .expect("the control's classified lookup must not error");
    assert!(
        control_lookup.meta.is_some() && !control_lookup.corrupt,
        "APPARATUS: the control's classified lookup did not reach the Parsed arm (meta={:?}, \
         corrupt={}). Missing, TransientMiss and Corrupt all return before the publish \
         decision, so nothing below would be about admission",
        control_lookup.meta.is_some(),
        control_lookup.corrupt
    );
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS/WIDTH: a ranges-bearing document read through get_metadata_classified_and_publish_to_ram's \
         Parsed arm was NOT admitted to the RAM metadata tier. Either admission is impossible \
         here for a reason other than the ranges guard, in which case the subject assertion \
         below proves nothing, or the guard is over-broad and has cost the tier an entry it \
         should hold"
    );

    // THE SITE. A RAM miss falls through to the classified disk read, which parses the
    // empty-`ranges` document and decides whether to publish it.
    let subject_lookup = f
        .cache
        .get_metadata_classified_and_publish_to_ram(SUBJECT_KEY)
        .await
        .expect("the subject's classified lookup must not error");
    assert!(
        subject_lookup.meta.is_some() && !subject_lookup.corrupt,
        "APPARATUS: the subject's classified lookup did not reach the Parsed arm (meta={:?}, \
         corrupt={}), so the site never reached its publish decision and the residency reading \
         below proves nothing",
        subject_lookup.meta.is_some(),
        subject_lookup.corrupt
    );
    assert!(
        subject_lookup
            .meta
            .as_ref()
            .map(|m| m.ranges.is_empty())
            .unwrap_or(false),
        "FIXTURE: the document the site parsed carries ranges, so it is not the state under test"
    );

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "CENSUS ROW 2: an empty-ranges document was admitted to the RAM metadata tier by \
         get_metadata_classified_and_publish_to_ram's Parsed arm. The control was admitted by the same function \
         in this same run, so the tier is live and this is the missing !ranges.is_empty() \
         admission test. Contract pin: this site has no production caller, so a failure means \
         a future caller inherits the v1.9.4 masking shape"
    );
}

/// Census **row 3**, `CacheManager::store_head_cache_entry_unified`'s merged `Ok` arm.
///
/// **REACHABILITY: a LIVE production path**, unlike row 2 above. Re-derived 2026-09-22:
/// `store_head_cache_entry_unified` has two `src/http_proxy.rs` call sites, and the function's
/// own comments record that this path "runs on every HEAD miss and every validated cached
/// serve". The `Ok` arm is the one taken whenever a `.meta` already exists — a warm key — so it
/// is the common case rather than the exceptional one.
///
/// The arm can nonetheless produce an empty-`ranges` merged document two ways, both real: the
/// existing `.meta` is itself rangeless (census row 4's own output, which is what this fixture
/// uses), or `update_metadata_head_fields` takes its `etag_changed || size_changed` branch and
/// **clears** `ranges` because S3 reports a new version.
///
/// **The `Ok` arm is asserted to have RUN, via `head_access_count`.** The `Err` arm
/// (`create_head_only_metadata`) writes `head_access_count: 1` outright; the `Ok` arm reads the
/// existing document and calls `record_head_access`, incrementing it. So a +1 delta across the
/// call distinguishes the two arms, which matters because they are chosen by a **read failure**
/// as well as by absence, and the `Err` arm publishes nothing at all — a test that silently took
/// it would pass while asserting nothing about admission.
///
/// `ram-tier-effectiveness:R1.6`.
#[tokio::test]
async fn row3_head_store_merged_arm_withholds_an_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    f.seed_control_ranges().await;
    // This first store takes the `Err` arm (no `.meta` yet) and leaves ranges: [] on disk.
    // Row 4 declines to publish it, which `seed_subject_head_only` asserts.
    let subject_meta = f.seed_subject_head_only().await;

    // The control takes the SAME merged `Ok` arm — its `.meta` exists, seeded with ranges — so
    // the width check is arm-for-arm with the subject. Its ETag and length match the seeded
    // document's, or `update_metadata_head_fields` would clear `ranges` and the control could
    // not prove the tier admits.
    let control_meta = f.meta_path(CONTROL_KEY);
    let control_accesses_before = read_meta(&control_meta).head_access_count;
    f.cache
        .store_head_cache_entry_unified(
            CONTROL_KEY,
            head_response_headers(CONTROL_ETAG),
            head_cache_metadata(CONTROL_ETAG),
        )
        .await
        .expect("the control's HEAD store must not error");
    let control_after = read_meta(&control_meta);
    assert_eq!(
        control_after.head_access_count,
        control_accesses_before + 1,
        "APPARATUS: the control's store did not take the merged Ok arm (head_access_count {} -> \
         {}). The Err arm publishes nothing, so the width reading below would be meaningless",
        control_accesses_before,
        control_after.head_access_count
    );
    assert!(
        !control_after.ranges.is_empty(),
        "APPARATUS: the merge cleared the control's ranges, so the HEAD response disagreed with \
         the seeded document and the control can no longer prove the tier admits"
    );
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS/WIDTH: a ranges-bearing merged document was NOT admitted to the RAM metadata \
         tier by store_head_cache_entry_unified's Ok arm. Either admission is impossible here \
         for a reason other than the ranges guard, or the guard is over-broad — and this site \
         runs on every HEAD miss and every validated cached serve, so an over-broad guard here \
         costs the tier the HEAD path's whole benefit"
    );

    // THE SITE, on the empty-ranges document. The `.meta` now exists, so this second call
    // takes the merged `Ok` arm rather than the `Err` arm the seed took.
    let subject_accesses_before = read_meta(&subject_meta).head_access_count;
    f.cache
        .store_head_cache_entry_unified(
            SUBJECT_KEY,
            head_response_headers(SUBJECT_ETAG),
            head_cache_metadata(SUBJECT_ETAG),
        )
        .await
        .expect("the subject's HEAD store must not error");
    let subject_after = read_meta(&subject_meta);
    assert_eq!(
        subject_after.head_access_count,
        subject_accesses_before + 1,
        "APPARATUS: the subject's store did not take the merged Ok arm (head_access_count {} -> \
         {}), so it never reached the publish decision under test. The Err arm is reached by a \
         read FAILURE as well as by absence, and it publishes nothing — a silent fall-through \
         there would make the residency reading below pass for the wrong reason",
        subject_accesses_before,
        subject_after.head_access_count
    );
    assert!(
        subject_after.ranges.is_empty(),
        "FIXTURE: the merged document carries {} range(s), so it is not the state under test",
        subject_after.ranges.len()
    );

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "CENSUS ROW 3: an empty-ranges merged document was admitted to the RAM metadata tier by \
         store_head_cache_entry_unified's Ok arm. The control was admitted through the same arm \
         in this same run, so the tier is live. This is a LIVE production path — two \
         http_proxy call sites, reached on every HEAD miss — so every range lookup on this key \
         is now answered from a document that says no ranges exist, for up to one \
         metadata_cache.refresh_interval"
    );
}
