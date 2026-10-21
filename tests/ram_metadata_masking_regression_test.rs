//! The v1.9.4 masking regression guard: `ram-tier-effectiveness:R3.2`, written for
//! `tasks.md` task 7 and shown **passing on current code** before task 6 changes admission.
//!
//! # Two admission sites, four tests — read the site before reading a verdict
//!
//! Task 6 applied one rule (`metadata_cache::ram_admission_permitted`, whose whole body is
//! `!metadata.ranges.is_empty()`) at eight production `metadata_cache.put` sites. This file
//! covers **two** of them, and the pairs are not interchangeable:
//!
//! | Site | Reached through | Tests |
//! |---|---|---|
//! | Census rows 1/2 | `CacheManager::get_metadata_cached` | `range_lookup_sees_peer_added_ranges_after_reading_an_empty_ranges_document`, `an_empty_ranges_document_is_withheld_from_the_ram_metadata_tier` (task 7) |
//! | Census row 5 | `CacheManager::get_head_cache_entry_unified`'s disk tier | `range_lookup_sees_peer_added_ranges_after_a_head_read_the_empty_ranges_document`, `a_head_lookup_withholds_an_empty_ranges_document_from_the_ram_metadata_tier` (task 10) |
//!
//! Neither pair subsumes the other. One rule at eight call sites is exactly the shape a later
//! per-site refactor can reinstate at seven of them, and a test that reaches only rows 1/2
//! would pass throughout. Each pair carries its own control proving **its own** site admits —
//! see `seed_row5_control_and_prove_the_site_admits` for why row 5 needs a second control key
//! rather than reusing `CONTROL_KEY`.
//!
//! Everything below about the cross-instance construction, the predicate, and the vacuity
//! traps applies to both pairs. Where a claim is specific to one, it says which.
//!
//! # What v1.9.4 fixed, and what "masking" means precisely
//!
//! The v1.9.4 fix ("Don't cache empty-ranges metadata in RAM instead of bypassing",
//! 2026-03-16) added a `!ranges.is_empty()` test to the RAM admission in
//! `CacheManager::get_metadata_cached`'s disk-read branch. Without it, a `.meta` carrying
//! `ranges: []` — a HEAD-only document, among four other producers the spec enumerates —
//! becomes **resident**, and `get_metadata_cached`'s very first statement returns whatever is
//! resident with no coverage test. So for up to one `metadata_cache.refresh_interval` every
//! range lookup on that key is answered from a document that says "no ranges exist", *even
//! after another instance's consolidation has added ranges to the shared `.meta`*. The ranges
//! and their `.bin` files are on disk and nothing consults them.
//!
//! Sharper than the spec's own one-word summary, and the distinction is what this file
//! asserts: **what is masked is not the object, it is the DISK's knowledge of the object's
//! coverage.** The entry is not stale about the object's identity — etag, length and
//! `Last-Modified` are all correct and current. It is stale about one field, `ranges`, in the
//! one direction that matters, and the consequence is a full refetch from S3 of bytes that
//! are already cached locally. A test that asserted "the response is correct" would pass
//! against the defect, because the response *is* correct — only the cost is wrong.
//!
//! # Why the divergence has to be CROSS-INSTANCE, and why a same-view fixture is worse than none
//!
//! The cache coherency invariants is explicit: a
//! fixture where both instances hold the same view **passes whatever the code does**. The RAM
//! metadata tier is per-instance (`MetadataCache`, keyed on `loaded_at` vs `refresh_interval`)
//! and the disk `.meta` is the only shared medium, so a fixture that drives *this* instance's
//! own publish path keeps the local tier coherent with disk by construction — the local put
//! either refreshes or invalidates the resident copy, the masking window never opens, and the
//! test reports a pass on broken code.
//!
//! So the peer's range publish here goes through a **second `DiskCacheManager`** over the same
//! cache directory (the construction `tests/range_invalidation_divergent_instance_test.rs`
//! established, and `tests/multi_instance_atomic_metadata_writes_integration_test.rs` before
//! it), never through the reader's `CacheManager`. That manager has no `MetadataCache` of its
//! own and no reference to the reader's, so its `.meta` rename is the only thing the reader
//! can observe — exactly what a peer's consolidation looks like from here. `peer_publish`
//! asserts the reader's RAM tier was **not** touched across it, so a future edit that reached
//! for the local path fails rather than quietly under-verifying.
//!
//! # The predicate, and the two adjacent figures it deliberately is not
//!
//! The assertion reads **what a range lookup reports for the key**, through the production
//! sequence `HttpProxy::handle_get_head_request` uses for a full-object GET:
//! `CacheManager::get_metadata_cached` -> `preloaded_metadata` ->
//! `CacheManager::has_cached_ranges` and `RangeHandler::find_cached_ranges`. That is the value
//! the guard branches on, carried to the place a serve decision is made.
//!
//! Not asserted, and each for a stated reason:
//!
//! - **The disk `.meta`'s contents.** Read here only as a fixture precondition. The defect
//!   leaves the `.meta` perfectly correct; asserting on it cannot fail.
//! - **A hit-rate or request counter.** `MetadataCache`'s hit/miss counters move on every
//!   call, and `ShardedRamCache::get` counts every call whether or not it finds anything, so a
//!   rate is a poor predicate for a coverage question. It is also per-instance, which is the
//!   `cache-coherency-invariants.md` § "Metrics are per-instance too" trap in local form.
//! - **A served byte count.** Correct in both the defective and the fixed case, because the
//!   defect costs a refetch rather than the wrong bytes.
//!
//! `has_cached_ranges` is called with `Some(preloaded)` at every call site below, never
//! `None`. That is load-bearing, and for a narrower reason than an earlier version of this
//! header claimed. **Corrected 2026-09-22 (`RAM-TIER-T8`):** it said the `None` arm "falls
//! through to `CacheManager::get_head_cache_entry_unified`, which is census row 5 — an
//! **unguarded** admission site". It does not. The `None` arm calls
//! `CacheManager::get_metadata_from_disk`, whose whole body is `exists()` +
//! `std::fs::read_to_string` + `serde_json::from_str` with no `metadata_cache` reference on
//! any path (one definition, `src/cache.rs`, no trait impl shadowing it — traced by
//! `RAM-TIER-R1-1-AND-TASK-9-UNREACHABLE-AFTER-TASK-6` and re-confirmed by `RAM-TIER-T8`).
//! So it never reaches row 5 and cannot admit anything to RAM.
//!
//! What `Some(preloaded)` is actually load-bearing for: it is the production sequence — the
//! full-object GET path loads metadata once and passes it through — and it is the only arm
//! that can exhibit masking at all. A `None` here would read the shared `.meta` directly and
//! report the disk's correct ranges, so the resident document would never be consulted and
//! the fixture would pass against the defect.
//!
//! # WHICH OF THIS FILE'S ASSERTIONS STILL DISCRIMINATES, after `RAM-TIER-T8`
//!
//! Task 8 added the `R1.2` confirmation to `RangeHandler::find_cached_ranges`: a not-covered
//! verdict taken from a preloaded document is now re-checked against the shared `.meta`
//! before it becomes a miss. Both tests below still pass, and the first one still fails
//! against task 6's red side — but **on one assertion rather than two**, and that is worth
//! knowing before anyone reads a pass here as two-fold evidence.
//!
//! With the admission guard removed and an empty-`ranges` document resident:
//!
//! - `has_ranges` — `CacheManager::has_cached_ranges`, whose predicate is
//!   `!meta.ranges.is_empty()` on the preloaded document — still reports `(false, _)`. That
//!   call has no confirmation of its own, so this assertion is unchanged and remains the
//!   discriminator.
//! - `covered` — `RangeHandler::find_cached_ranges` — would now report coverage anyway,
//!   because the confirmation reads the peer's ranges off the shared `.meta`. It is
//!   therefore no longer evidence about admission.
//!
//! The second test asserts residency directly and is unaffected either way.
//!
//! Why `has_cached_ranges` was left unconfirmed: post-task-6 no empty-`ranges` document can
//! become resident, so its gate cannot suppress the coverage computation that follows it.
//! That is a property of the admission rule, not of this gate — see
//! the investigation record, which records it as an open item.
//!
//! # The state the fixture builds, and which producer it corresponds to
//!
//! `requirements.md` records **five** live producers of an empty-`ranges` `.meta`. This
//! fixture builds producer 1, `CacheManager::create_head_only_metadata`, and builds it by
//! calling the production entry point (`CacheManager::store_head_cache_entry_unified` against
//! a key with no `.meta` on disk) rather than by hand-writing a document — so the state under
//! test is one production actually creates, for a **non-zero-length** object, which is the
//! harmful case rather than the benign zero-byte one.
//!
//! # The vacuity traps this file has to survive, and the guard for each
//!
//! - **Admission might be impossible here for an unrelated reason**, in which case "the entry
//!   is not resident" is not the guard acting and the red side reads green. `MetadataTtls::forbids_admission`
//!   refuses admission outright when `get_ttl` and `head_ttl` are both zero, and
//!   `MetadataCacheConfig::enabled` can switch the tier off entirely. `assert_apparatus`
//!   checks both TTLs are non-zero, and `CONTROL_KEY` — a ranges-bearing entry read through
//!   the same function on the same manager — MUST become resident. That is the positive
//!   signal: a subject entry absent from a tier that just admitted the control is the guard.
//! - **`refresh_interval` is 5s by default, and a slow run would close the masking window**,
//!   making a resident empty-`ranges` entry stale and re-read from disk — which also reads
//!   green. The control's residency is re-asserted **after** the subject assertion, and it was
//!   admitted *earlier* than the subject would have been, so it is a conservative proxy: if
//!   the control is still fresh the window was open throughout.
//! - **The "before" reading.** The lookup MUST report no coverage before the peer's publish.
//!   Without it, `Some((true, _))` afterwards is not evidence of a change.
//!
//! # Red side
//!
//! The `!metadata.ranges.is_empty()` test enclosing the `metadata_cache.put` in
//! `CacheManager::get_metadata_cached`'s disk-read branch, replaced by an always-true local.
//! See the investigation record for the verbatim panic, the
//! `grep -c` confirming the defect was in the tree before the result was read, the
//! `pgrep -fl cargo` checks at apply and at read, and the post-revert `diff -q` against a
//! named pre-apply byte copy.
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CacheMetadata, NewCacheMetadata, ObjectMetadata, RangeLookupPurpose};
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::{RangeHandler, RangeSpec};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::RwLock;

/// The subject: HEAD-first, so `store_head_cache_entry_unified` takes its
/// `create_head_only_metadata` arm and leaves `ranges: []` on disk.
const SUBJECT_KEY: &str = "rt7-masking-bucket/head-first-object.bin";
/// The apparatus control. Ranges-bearing from the start, so it MUST be admitted to the RAM
/// metadata tier — which is what makes the subject's absence from that tier attributable to
/// the guard rather than to a tier that admits nothing.
const CONTROL_KEY: &str = "rt7-masking-bucket/ranges-bearing-control.bin";

/// Task 10's own control, and it MUST be a different key from [`CONTROL_KEY`].
///
/// Census row 5 sits in `get_head_cache_entry_unified`'s **disk** tier, which is reached only
/// after that function's RAM tier misses. Warming a key that
/// `seed_control_and_prove_admission` has already made resident returns from the RAM tier and
/// never reaches row 5's `put` at all — a control that would prove the tier admits while
/// saying nothing about the site under test.
const ROW5_CONTROL_KEY: &str = "rt7-masking-bucket/row5-ranges-bearing-control.bin";
const ROW5_CONTROL_ETAG: &str = "\"rt7-row5-control-etag-v1\"";

/// Non-zero, because a zero-length object is the benign producer and
/// `has_cached_ranges` reports `None` for it regardless of coverage.
const OBJECT_LEN: u64 = 4096;

/// Deliberately not the empty object's MD5: `has_cached_ranges` refuses an entry that claims
/// that ETag alongside a positive length, which would make every assertion below vacuous.
const SUBJECT_ETAG: &str = "\"rt7-subject-etag-v1\"";
const CONTROL_ETAG: &str = "\"rt7-control-etag-v1\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

const TTL: Duration = Duration::from_secs(3600);

struct Fixture {
    _dir: TempDir,
    /// Instance A: the reader. Owns the RAM metadata tier under test.
    reader: Arc<CacheManager>,
    /// Instance B: the peer. A separate manager over the same cache directory, with no
    /// `MetadataCache` of its own and no handle on the reader's.
    peer: DiskCacheManager,
    range_handler: RangeHandler,
}

async fn disk_instance(cache_dir: &Path) -> DiskCacheManager {
    // Compression off so `.bin` size arithmetic never depends on a ratio.
    let manager = DiskCacheManager::new(cache_dir.to_path_buf(), false, 1024, false, 1_048_576);
    manager
        .initialize()
        .await
        .expect("disk cache manager must initialize over the shared cache directory");
    // Both managers in this fixture are built here, so this pins the mode for both.
    //
    // `DiskCacheManager::new` leaves `hybrid_metadata_writer: None`, and
    // `find_pending_journal_ranges` returns `Ok(Vec::new())` without looking when that is so —
    // its own doc comment warns that any test asserting an **empty** journal-lookup result
    // passes vacuously here. The direction is what makes it safe in this file, and it is worth
    // stating rather than leaving to be re-derived: every assertion below that could be
    // affected asserts a **non-empty** result (the peer's ranges ARE found), which a vacuously
    // empty journal would make FAIL, not pass. The single empty-direction reading — the BEFORE
    // `!covered` — is a precondition whose subject is the disk `.meta`, which the fixture reads
    // and asserts on directly.
    //
    // Direct mode is also what the cross-instance construction needs: with no journal in play,
    // the peer's `.meta` rename is the ONLY channel by which its ranges can reach the reader.
    assert!(
        !manager.is_shared_storage_mode(),
        "APPARATUS: DiskCacheManager::new now configures a hybrid metadata writer, so this \
         fixture is no longer in direct mode. The reasoning above about which channel carries \
         the peer's ranges, and about which assertions a vacuous journal lookup could affect, \
         was derived for direct mode — re-derive it rather than deleting this assertion"
    );
    manager
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let cache_dir: PathBuf = dir.path().to_path_buf();

        // `new_with_defaults` resolves `get_ttl`/`head_ttl` to an hour each and takes
        // `MetadataCacheConfig::default()` (enabled, 5s `refresh_interval`). Both are
        // asserted rather than assumed — see `assert_apparatus`.
        let reader = Arc::new(CacheManager::new_with_defaults(cache_dir.clone(), false, 0));
        let reader_disk = Arc::new(RwLock::new(disk_instance(&cache_dir).await));
        let range_handler = RangeHandler::new(reader.clone(), reader_disk);
        let peer = disk_instance(&cache_dir).await;

        Fixture {
            _dir: dir,
            reader,
            peer,
            range_handler,
        }
    }

    /// The reader's own view of the RAM metadata tier for a key: `Some` iff an entry is
    /// resident and inside `refresh_interval`.
    async fn resident(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        self.reader.get_metadata_cache().get(cache_key).await
    }

    /// The production sequence a full-object GET takes, in order: the metadata lookup that
    /// admits (or declines to admit) to RAM, then the coverage verdict computed from what it
    /// returned.
    async fn range_lookup_reports(&self, cache_key: &str) -> (Option<(bool, u64)>, bool) {
        let preloaded = self
            .reader
            .get_metadata_cached(cache_key)
            .await
            .expect("metadata lookup must not error")
            .expect("the .meta exists on disk, so the lookup must return a document");

        let has_ranges = self
            .reader
            .has_cached_ranges(cache_key, Some(&preloaded))
            .await
            .expect("has_cached_ranges must not error");

        let requested = RangeSpec {
            start: 0,
            end: OBJECT_LEN - 1,
        };
        let overlap = self
            .range_handler
            .find_cached_ranges(
                cache_key,
                &requested,
                None,
                Some(&preloaded),
                // FreshServe: the stored expiry is an hour out, so a `RevalidationCandidate`
                // would let a stored-expired entry through and weaken the reading.
                RangeLookupPurpose::FreshServe,
            )
            .await
            .expect("find_cached_ranges must not error");

        (has_ranges, overlap.has_complete_coverage())
    }

    /// The warm step for census row 5: `CacheManager::get_head_cache_entry_unified`'s **disk**
    /// tier, driven through the production entry point.
    ///
    /// Nothing else reaches that site. `get_metadata_cached` is census rows 1 and 2 — a
    /// different admission site in a different function, and the one task 7's tests cover.
    /// `has_cached_ranges(key, None)` reaches no admission site at all: its `None` arm calls
    /// `CacheManager::get_metadata_from_disk`, which touches the Metadata_Cache on no path (see
    /// the header). And `get_metadata_cache().put()` would bypass the guard rather than
    /// exercise it.
    ///
    /// Row 5's `put` runs **before** the function's HEAD-freshness test and before its three
    /// poisoned-entry guards, so admission here does not depend on the HEAD being served.
    async fn warm_via_head_lookup(&self, cache_key: &str) {
        let head_ttl = self.reader.get_effective_head_ttl(cache_key).await;
        assert!(
            !head_ttl.is_zero(),
            "APPARATUS: resolved head_ttl for {} is zero. Row 5's `put` runs before the \
             freshness test so that alone would not suppress admission, but a zero head_ttl \
             alongside a zero get_ttl makes MetadataTtls::forbids_admission refuse outright, \
             and then nothing below distinguishes the ranges guard from the TTLs",
            cache_key
        );
        self.reader
            .get_head_cache_entry_unified(cache_key, head_ttl)
            .await
            .expect("the HEAD lookup must not error");
    }

    /// Instance B's publish, as a peer's consolidation appears: the shared `.meta` gains
    /// ranges and the `.bin` appears, written by a manager the reader shares nothing with
    /// except the directory.
    async fn peer_publish(&mut self, cache_key: &str, etag: &str) {
        let resident_before = self.resident(cache_key).await.map(|m| m.ranges.len());

        let body = vec![0xA7u8; OBJECT_LEN as usize];
        self.peer
            .store_range(
                cache_key,
                0,
                OBJECT_LEN - 1,
                &body,
                object_metadata(etag),
                TTL,
                false,
            )
            .await
            .expect("the peer's range publish must succeed");

        // Not decoration. If a future edit routed this through the reader's own
        // `CacheManager`, the local publish would refresh or invalidate the resident copy and
        // the masking window would never open — the fixture would then pass against the
        // defect. The peer cannot reach the reader's RAM tier, so this must be unchanged.
        let resident_after = self.resident(cache_key).await.map(|m| m.ranges.len());
        assert_eq!(
            resident_before, resident_after,
            "DIVERGENCE: the peer's publish changed the reader's RAM metadata tier for {} \
             (before={:?} after={:?}). The publish is supposed to be cross-instance — a \
             local publish path keeps the reader coherent with disk by construction and this \
             fixture would then pass whatever the admission code does",
            cache_key, resident_before, resident_after
        );
    }
}

fn head_response_headers(etag: &str) -> HashMap<String, String> {
    // No `content-range` and no `x-amz-mp-parts-count`: `store_head_cache_entry_unified`
    // refuses a part-scoped response outright, which would fail the seed rather than the
    // assertion.
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
        // `validate_head_cache_inputs` rejects an empty ETag or an empty Last-Modified, so
        // both are populated deliberately rather than defaulted.
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
        .unwrap_or_else(|e| panic!("the shared .meta must be readable at {:?}: {}", path, e));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("the shared .meta at {:?} must parse: {}", path, e))
}

/// Everything that has to be true before any reading below means anything.
///
/// Both TTLs non-zero, because `MetadataTtls::forbids_admission` refuses admission when
/// neither consumer may use the entry — and under that condition "not resident" is not the
/// ranges guard acting.
async fn assert_apparatus(f: &Fixture) {
    for key in [SUBJECT_KEY, CONTROL_KEY] {
        assert!(
            !f.reader.get_effective_get_ttl(key).await.is_zero(),
            "APPARATUS: resolved get_ttl for {} is zero, so MetadataTtls::forbids_admission \
             would refuse admission for a reason unrelated to the ranges guard",
            key
        );
        assert!(
            !f.reader.get_effective_head_ttl(key).await.is_zero(),
            "APPARATUS: resolved head_ttl for {} is zero, so MetadataTtls::forbids_admission \
             would refuse admission for a reason unrelated to the ranges guard",
            key
        );
    }
}

/// Seed the control and prove the RAM metadata tier admits on this manager, for this key
/// shape, under these TTLs.
async fn seed_control_and_prove_admission(f: &mut Fixture) {
    f.peer_publish(CONTROL_KEY, CONTROL_ETAG).await;

    let (has_ranges, covered) = f.range_lookup_reports(CONTROL_KEY).await;
    assert_eq!(
        has_ranges,
        Some((true, OBJECT_LEN)),
        "APPARATUS: the control's own range lookup does not report coverage, so the fixture's \
         peer publish is broken and nothing below is a verdict about the subject"
    );
    assert!(
        covered,
        "APPARATUS: the control's range lookup reports incomplete coverage, so the peer's \
         .bin/.meta pair is not usable and nothing below is a verdict about the subject"
    );
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS: a ranges-bearing document read through get_metadata_cached was NOT \
         admitted to the RAM metadata tier. Admission is therefore impossible here for some \
         reason other than the ranges guard (tier disabled, TTLs, eviction), and the \
         subject's absence from the tier would prove nothing"
    );
}

/// Prove **census row 5 itself** admits, on this manager, for this key shape, under these
/// TTLs: a ranges-bearing document read through `get_head_cache_entry_unified`'s disk tier MUST
/// become resident.
///
/// Needed **in addition to** `seed_control_and_prove_admission`, not instead of it. That one
/// proves the RAM metadata *tier* admits, through rows 1/2 — which leaves "row 5's `put` is
/// never reached on this fixture" as a live alternative explanation for the subject's absence
/// from the tier. This closes that, and it is the control task 10 needs rather than task 7's.
async fn seed_row5_control_and_prove_the_site_admits(f: &mut Fixture) {
    f.peer_publish(ROW5_CONTROL_KEY, ROW5_CONTROL_ETAG).await;

    // Non-vacuity of the control itself. Row 5 is behind a RAM miss, so a key that is already
    // resident would be answered from the first tier and the `put` under test never runs.
    assert!(
        f.resident(ROW5_CONTROL_KEY).await.is_none(),
        "APPARATUS: the row-5 control was already resident before its HEAD lookup, so that \
         lookup will return from get_head_cache_entry_unified's RAM tier and never reach the \
         disk-tier admission this control exists to exercise"
    );

    f.warm_via_head_lookup(ROW5_CONTROL_KEY).await;

    assert!(
        f.resident(ROW5_CONTROL_KEY).await.is_some(),
        "APPARATUS: a ranges-bearing document read through get_head_cache_entry_unified's disk \
         tier was NOT admitted to the RAM metadata tier. Census row 5's `put` is therefore not \
         reached at all here, and the subject's absence from the tier below would prove nothing \
         about the `ranges` guard"
    );
}

/// Seed the subject through producer 1 and assert the state it left.
async fn seed_subject_head_only(f: &Fixture) -> PathBuf {
    f.reader
        .store_head_cache_entry_unified(
            SUBJECT_KEY,
            head_response_headers(SUBJECT_ETAG),
            head_cache_metadata(SUBJECT_ETAG),
        )
        .await
        .expect("the HEAD-only seed must succeed");

    let meta_path = f.reader.get_new_metadata_file_path(SUBJECT_KEY);
    let on_disk = read_meta(&meta_path);
    assert!(
        on_disk.ranges.is_empty(),
        "FIXTURE: the seed was supposed to take create_head_only_metadata's arm and leave \
         ranges: [] on disk, but the .meta carries {} range(s). Producer 1 is not the state \
         under test any more",
        on_disk.ranges.len()
    );
    assert_eq!(
        on_disk.object_metadata.content_length, OBJECT_LEN,
        "FIXTURE: the HEAD-only document must describe a non-zero-length object, or \
         has_cached_ranges reports None for a length reason rather than a coverage one"
    );
    meta_path
}

/// `ram-tier-effectiveness:R3.2`. THE guard. A range lookup must see the ranges a peer added
/// to the shared `.meta`, even though this instance has just read the same key and found a
/// document with none.
#[tokio::test]
async fn range_lookup_sees_peer_added_ranges_after_reading_an_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    seed_control_and_prove_admission(&mut f).await;
    let meta_path = seed_subject_head_only(&f).await;

    // The BEFORE reading. The lookup must report no coverage here, or `Some((true, _))`
    // below is not evidence that anything changed. This call is also the one that reaches
    // the guard: it reads the empty-`ranges` document from disk and decides whether to
    // publish it to RAM.
    let (before_has_ranges, before_covered) = f.range_lookup_reports(SUBJECT_KEY).await;
    assert_eq!(
        before_has_ranges,
        Some((false, OBJECT_LEN)),
        "BEFORE: a HEAD-only document must report no coverage and a known length"
    );
    assert!(
        !before_covered,
        "BEFORE: a HEAD-only document must not report complete coverage"
    );

    // The peer's consolidation appears on the shared `.meta`. Nothing tells this instance.
    f.peer_publish(SUBJECT_KEY, SUBJECT_ETAG).await;
    let after_disk = read_meta(&meta_path);
    assert!(
        !after_disk.ranges.is_empty(),
        "FIXTURE: the peer's publish did not add ranges to the shared .meta, so there is \
         nothing for the lookup to be masked from"
    );

    // THE ASSERTION. The same production sequence, run again inside `refresh_interval`.
    let (has_ranges, covered) = f.range_lookup_reports(SUBJECT_KEY).await;
    assert_eq!(
        has_ranges,
        Some((true, OBJECT_LEN)),
        "v1.9.4 MASKING REGRESSION: the shared .meta carries {} range(s) written by the peer, \
         and the range lookup still reports no coverage. A resident empty-ranges RAM metadata \
         entry is shadowing the disk document, so this GET refetches from S3 bytes that are \
         already cached locally. The `!ranges.is_empty()` admission test in \
         CacheManager::get_metadata_cached's disk-read branch is what prevents this",
        after_disk.ranges.len()
    );
    assert!(
        covered,
        "v1.9.4 MASKING REGRESSION: find_cached_ranges reports incomplete coverage for a \
         range the peer published in full ({} range(s) on disk), so the serve decision is \
         being taken from a masked document",
        after_disk.ranges.len()
    );

    // The masking window must still have been open while the assertion above was taken.
    // The control was admitted EARLIER than the subject's first lookup, so its freshness is
    // a conservative proxy: if it is still resident, a resident empty-ranges subject entry
    // would not have been stale either, and the assertion above could genuinely have failed.
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS: the control entry went stale before the assertion was taken, so \
         metadata_cache.refresh_interval elapsed mid-test. A resident empty-ranges entry \
         would have been re-read from disk for a timing reason and the reading above cannot \
         distinguish the guard from the clock. Re-run"
    );
}

/// The direct observation, kept separate from the property above so that removing the guard
/// fails the property test on its own merits rather than tripping over this first.
///
/// This is a pin on the mechanism, not the contract: `ram-tier-effectiveness:R1.6`'s task 6
/// may move where the test lives (the standing proposal is to push it inside
/// `MetadataCache::put_with_mtime` beside `forbids_admission`). If it does, this assertion
/// should still hold — an empty-`ranges` document must not become resident — and that is the
/// reason it asserts residency rather than naming a function.
#[tokio::test]
async fn an_empty_ranges_document_is_withheld_from_the_ram_metadata_tier() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    seed_control_and_prove_admission(&mut f).await;
    seed_subject_head_only(&f).await;

    let (has_ranges, _covered) = f.range_lookup_reports(SUBJECT_KEY).await;
    assert_eq!(
        has_ranges,
        Some((false, OBJECT_LEN)),
        "FIXTURE: the subject lookup must have read the empty-ranges document"
    );

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "An empty-ranges document was admitted to the RAM metadata tier. The control key was \
         admitted in this same run, so the tier is live and this is not an apparatus failure \
         — it is the v1.9.4 admission guard gone. Every range lookup on this key is now \
         answered from a document that says no ranges exist, for up to one refresh_interval, \
         regardless of what another instance has added to the shared .meta"
    );
}

/// `ram-tier-effectiveness:R3.10a`, `tasks.md` task 10 — **the fifth site, directly**.
///
/// Task 7's property test above reaches admission through `CacheManager::get_metadata_cached`
/// (census rows 1 and 2). This one reaches it through
/// `CacheManager::get_head_cache_entry_unified`'s disk tier (census row 5), which is a
/// different site, in a different function, guarded by a different `if`. Before task 6 that
/// site published whatever the disk read returned with no `ranges` test, so a single HEAD on a
/// HEAD-only key made every range lookup on that key report "no coverage" for up to one
/// `metadata_cache.refresh_interval` — including after a peer's consolidation had added ranges
/// to the shared `.meta`.
///
/// # The premise reads as the RED side, deliberately, and both sides are asserted here
///
/// `tasks.md`'s wording — "HEAD a key so RAM holds an empty-`ranges` entry" — describes what
/// the **pre-guard** row 5 does. On the shipped tree the warm HEAD leaves **nothing** resident,
/// and that is exactly why the GET then finds the peer's ranges. One fixture, one assertion,
/// two mechanisms: red is "a resident empty-`ranges` entry masks disk", green is "admission was
/// refused, so the disk read happens". The fixture is NOT adjusted to make the premise literally
/// true on the shipped tree, because the only way to do that is to relax the guard, which
/// `ram-tier-effectiveness:R1.6` forbids.
#[tokio::test]
async fn range_lookup_sees_peer_added_ranges_after_a_head_read_the_empty_ranges_document() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;

    // Control 1 (reused from task 7): the RAM metadata tier admits at all. Seeded FIRST so its
    // freshness is the conservative proxy for the masking window at the end of the test.
    seed_control_and_prove_admission(&mut f).await;
    // Control 2 (task 10's own): census row 5's `put` is reached and live.
    seed_row5_control_and_prove_the_site_admits(&mut f).await;

    let meta_path = seed_subject_head_only(&f).await;

    // THE WARM STEP, and the whole point of this test: the empty-`ranges` document is read by
    // `get_head_cache_entry_unified`, which is where census row 5 decides whether to publish it.
    f.warm_via_head_lookup(SUBJECT_KEY).await;

    // The BEFORE reading. Without it, `Some((true, _))` below is not evidence that anything
    // changed. Note this reading is identical on both sides — on the red side it comes from the
    // entry row 5 just published, on the green side from the disk document — so it is a
    // precondition and not a discriminator.
    let (before_has_ranges, before_covered) = f.range_lookup_reports(SUBJECT_KEY).await;
    assert_eq!(
        before_has_ranges,
        Some((false, OBJECT_LEN)),
        "BEFORE: a HEAD-only document must report no coverage and a known length"
    );
    assert!(
        !before_covered,
        "BEFORE: a HEAD-only document must not report complete coverage"
    );

    // The peer's consolidation appears on the shared `.meta`. Nothing tells this instance, and
    // `peer_publish` asserts the reader's RAM tier was not touched across it.
    f.peer_publish(SUBJECT_KEY, SUBJECT_ETAG).await;
    let after_disk = read_meta(&meta_path);
    assert!(
        !after_disk.ranges.is_empty(),
        "FIXTURE: the peer's publish did not add ranges to the shared .meta, so there is \
         nothing for the lookup to be masked from"
    );

    // THE ASSERTION. `CacheManager::has_cached_ranges`, whose predicate is
    // `!meta.ranges.is_empty()` on the document `get_metadata_cached` returned — which is the
    // resident one when row 5 admitted it. `find_cached_ranges`' verdict is NOT the
    // discriminator here: task 8's `ram-tier-effectiveness:R1.2` confirmation re-reads the
    // shared `.meta` before a not-covered verdict becomes a miss, so `covered` would report
    // coverage even with row 5's guard removed. It is asserted below as a consistency check and
    // labelled as one.
    let (has_ranges, covered) = f.range_lookup_reports(SUBJECT_KEY).await;
    assert_eq!(
        has_ranges,
        Some((true, OBJECT_LEN)),
        "FIFTH-SITE MASKING (ram-tier-effectiveness:R3.10a): the shared .meta carries {} \
         range(s) written by the peer, and the range lookup still reports no coverage. A HEAD \
         on this key left an empty-ranges document resident in the RAM metadata tier, so every \
         range lookup is answered from a document saying no ranges exist and this GET refetches \
         from S3 bytes that are already cached locally. The site is the \
         `ram_admission_permitted` test enclosing the `metadata_cache.put` in \
         CacheManager::get_head_cache_entry_unified's disk tier — census row 5, NOT \
         get_metadata_cached's row 1 which task 7's test covers",
        after_disk.ranges.len()
    );
    assert!(
        covered,
        "CONSISTENCY, not the discriminator: find_cached_ranges reports incomplete coverage for \
         a range the peer published in full ({} range(s) on disk). Task 8's R1.2 confirmation \
         should make this hold even when row 5's guard is removed, so a failure here is a \
         finding about that confirmation rather than about admission",
        after_disk.ranges.len()
    );

    // The masking window must still have been open while the assertion above was taken. The
    // task-7 control was admitted EARLIEST of the three keys, so its freshness is conservative:
    // if it is still resident, a resident empty-ranges subject entry would not have gone stale
    // either, and the assertion above could genuinely have failed.
    assert!(
        f.resident(CONTROL_KEY).await.is_some(),
        "APPARATUS: the control entry went stale before the assertion was taken, so \
         metadata_cache.refresh_interval elapsed mid-test. A resident empty-ranges entry would \
         have been re-read from disk for a timing reason and the reading above cannot \
         distinguish the guard from the clock. Re-run"
    );
}

/// The direct observation of the same site, kept separate for the reason the task-7 pair is
/// split: removing row 5's guard should fail the property test above on its own merits rather
/// than tripping over this assertion first.
///
/// This is the row-5 analogue of `an_empty_ranges_document_is_withheld_from_the_ram_metadata_tier`,
/// which asserts the same property for census rows 1/2. Neither subsumes the other — task 6
/// applied one rule at eight sites, and a per-site refactor is exactly the change that can
/// reinstate it at seven of them.
#[tokio::test]
async fn a_head_lookup_withholds_an_empty_ranges_document_from_the_ram_metadata_tier() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;
    seed_row5_control_and_prove_the_site_admits(&mut f).await;
    seed_subject_head_only(&f).await;

    // Non-vacuity: the subject must not already be resident, or "still not resident" after the
    // lookup is not a statement about the lookup.
    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "APPARATUS: the HEAD-only seed made the subject resident by itself. \
         store_head_cache_entry_unified's create_head_only_metadata arm is census row 4 and is \
         supposed to reach no `put` at all, so the site under test below is no longer the one \
         that decides residency"
    );

    f.warm_via_head_lookup(SUBJECT_KEY).await;

    assert!(
        f.resident(SUBJECT_KEY).await.is_none(),
        "FIFTH-SITE ADMISSION (ram-tier-effectiveness:R1.6): an empty-ranges document was \
         admitted to the RAM metadata tier by CacheManager::get_head_cache_entry_unified's disk \
         tier. The row-5 control was admitted through the SAME site in this same run, so this is \
         not an apparatus failure — it is the `ram_admission_permitted` test at census row 5 \
         gone. Every range lookup on this key is now answered from a document that says no \
         ranges exist, for up to one refresh_interval, regardless of what another instance has \
         added to the shared .meta"
    );
}
