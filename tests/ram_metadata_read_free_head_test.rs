//! `ram-tier-effectiveness:R3.7` / `R3.1` (`R3.11` makes them the same property) — a HEAD
//! answered from a **resident**, HEAD-fresh Metadata_Cache entry performs **no**
//! Shared_Storage `.meta` read. Written for `tasks.md` task 9.
//!
//! # What this file is, and what the re-scope on 2026-09-22 removed
//!
//! Task 9 originally asserted that a **HEAD_Only_Entry** could be served from the RAM
//! metadata tier. Task 6 made that unachievable rather than merely red: all eight production
//! `metadata_cache.put` sites now test `metadata_cache::ram_admission_permitted`, census row 4
//! declines by yielding `None`, and so no rangeless document reaches the tier on any
//! production path. Ed ruled that asymmetry acceptable on 2026-09-22 (quoted under
//! `ram-tier-effectiveness:R1.1`), so the original assertion asserts a contract the design now
//! forbids.
//!
//! **Relaxing `ram_admission_permitted` would make the original assertion pass and would
//! reintroduce the v1.9.4 masking defect fleet-wide.** `ram-tier-effectiveness:R1.6`, the
//! nine-row census and the v1.9.4 fix all forbid it. Nothing in this file touches admission.
//!
//! What survives the ruling intact, and was untested until this file: the read-elimination the
//! RAM metadata tier exists for on the HEAD path. Two things deliberately **not** here:
//!
//! - **Admission refusal.** That is
//!   `tests/ram_metadata_masking_regression_test.rs`'s
//!   `an_empty_ranges_document_is_withheld_from_the_ram_metadata_tier`, shipped by task 7.
//!   `ram-tier-effectiveness:R3.11` exists to stop one guard being written twice.
//! - **The lookup gate's `ranges`-blindness.** True, recorded under
//!   `ram-tier-effectiveness:R1.1`, and reachable only by bypassing admission through
//!   `get_metadata_cache().put()` — pinning behaviour production cannot exercise, under a test
//!   name that would read as an endorsement of the contract Ed ruled against.
//!
//! # THE PREDICATE, named as a branch
//!
//! `CacheManager::get_head_cache_entry_unified`'s **RAM-tier serve arm** (`src/cache.rs`): the
//! `else` of the part-scoped / missing-`Last-Modified` / empty-object-ETag guard chain, inside
//! `if head_fresh`, inside `if let Some(metadata) = self.metadata_cache.get(..)`. That arm
//! calls `MetadataCache::record_head_hit()` and returns. Everything after it — the
//! `record_head_miss()`, the `metadata_path.exists()` test and the
//! `read_new_cache_metadata_from_disk` that is this function's **only** Shared_Storage read —
//! is unreachable once it is taken.
//!
//! So the counters below are not three readings of one thing. They are three different
//! statements about where control went:
//!
//! | Counter | Site | What a delta means |
//! |---|---|---|
//! | `head_hits` | the RAM-tier serve arm | the RAM arm answered |
//! | `head_misses` | immediately **before** the disk tier is entered | the disk tier was entered at all |
//! | `head_disk_hits` | the disk tier's own serve arm | the disk `.meta` answered |
//!
//! **`head_misses` staying flat is the read-free claim**, and it is strictly stronger than
//! `head_disk_hits` staying flat: `record_head_miss()` is unconditional and sits above the
//! `exists()` test, so a flat `head_misses` means the disk path was never entered, whereas a
//! flat `head_disk_hits` is also consistent with entering it and failing to read. A
//! `head_hits` increment on its own establishes neither — it says the RAM arm ran, not that
//! nothing else did. Asserting on it alone would be a substitution
//! (the pre-push checklist), so
//! all three are read.
//!
//! # A counter that cannot move is not a check
//!
//! Two of the three assertions are that a counter stayed **flat**, which is exactly the shape
//! that passes when the apparatus is broken. So the DISK ARM below runs first, in the same
//! test, on the same `MetadataCache`, and drives `head_misses` **and** `head_disk_hits` up by
//! one each from a key that is HEAD-fresh on disk and not resident. That is what makes the
//! subject's flat readings a verdict rather than an artefact of counters nothing increments.
//! It is the fleet suite's pattern: a negative reading needs the same instrument shown
//! positive in the same run.
//!
//! # Residency and HEAD-freshness are ESTABLISHED and ASSERTED, not assumed
//!
//! A HEAD on a cold key reaches disk on any build, so a fixture that fails to make the entry
//! resident makes both sides of the red/green pair red and proves nothing. The subject becomes
//! resident and HEAD-fresh through **production paths only**:
//!
//! 1. A peer-shaped `DiskCacheManager` over the same cache directory publishes a range, so the
//!    `.meta` carries `ranges` and `ram_admission_permitted` will allow it. That manager holds
//!    no `MetadataCache`, so seeding cannot populate the tier under test.
//! 2. `store_head_cache_entry_unified` then takes its merged `Ok` arm (census row 3), which
//!    calls `refresh_head_ttl` — setting `head_expires_at` and the `head_cached_at` anchor
//!    `is_head_fresh` reads — and publishes the merged document to the tier.
//!
//! The HEAD response's ETag and length **must match** the seeded document's: a mismatch takes
//! `update_metadata_head_fields`' `etag_changed || size_changed` branch, which **clears
//! `ranges`**, and the merged document would then be refused admission for a reason unrelated
//! to this test.
//!
//! Residency is then re-asserted **immediately before** the asserted HEAD, because
//! `metadata_cache.refresh_interval` is 5s by default and a slow run would re-read disk and
//! fail for a reason unrelated to the property. `MetadataCacheEntry::touch` moves
//! `last_accessed` only, and `is_stale` reads `loaded_at`, so the residency check does not
//! extend the window it is checking. Same shape as
//! `tests/ram_metadata_absence_confirmation_test.rs`.
//!
//! # Making the read IMPOSSIBLE, and the exact scope of that claim
//!
//! The subject's `.meta` is **renamed away** before the asserted HEAD, so a read cannot be
//! silently satisfied and the counters are corroborated by the filesystem. The claim is scoped
//! to this function: `get_head_cache_entry_unified`'s only Shared_Storage read is
//! `read_new_cache_metadata_from_disk(&self.get_new_metadata_file_path(cache_key))`, gated by
//! `metadata_path.exists()`, and that helper is a single `fs::read_to_string` of the path it is
//! given with no fallback to a journal, a legacy path or a second root. Renaming that one path
//! therefore makes this function's disk tier unreachable. It is **not** a claim that no other
//! function in the crate could find the data elsewhere — the journal tree and the write-ledger
//! tree both still exist under the same cache directory, untouched.
//!
//! # Red side
//!
//! The investigation record carries the marker, the `grep -c`
//! proving the defect was in the tree the compiler read, the `pgrep -fl cargo` checks at apply
//! and at read, and the verbatim failure.
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::{CacheMetadata, NewCacheMetadata, ObjectMetadata};
use s3_proxy::disk_cache::DiskCacheManager;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// The subject: made resident and HEAD-fresh, then its `.meta` is renamed away.
const SUBJECT_KEY: &str = "rt9-readfree-bucket/resident-head-fresh.bin";
/// The disk arm: HEAD-fresh on disk, never resident. Proves the two flat-asserted counters
/// can move in this apparatus.
const DISK_KEY: &str = "rt9-readfree-bucket/disk-only-head-fresh.bin";

const OBJECT_LEN: u64 = 4096;

/// Deliberately not the empty object's MD5 — `is_empty_object_etag_entry` refuses an entry
/// claiming that ETag alongside a positive length, which would send the RAM arm down the
/// invalidate-and-forward branch and make the readings below vacuous.
const SUBJECT_ETAG: &str = "\"rt9-subject-etag-v1\"";
const DISK_ETAG: &str = "\"rt9-disk-etag-v1\"";
/// Non-empty: `effective_last_modified().is_none()` is another RAM-arm refusal branch.
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

const TTL: Duration = Duration::from_secs(3600);

struct Fixture {
    _dir: TempDir,
    /// The instance under test. Owns the RAM metadata tier and the counters read below.
    cache: Arc<CacheManager>,
    /// A second manager over the same cache directory, used only to publish ranges. It holds
    /// no `MetadataCache`, so seeding cannot populate the tier under test.
    seeder: DiskCacheManager,
}

impl Fixture {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let cache_dir: PathBuf = dir.path().to_path_buf();

        // `new_with_defaults` resolves both TTLs to an hour and takes
        // `MetadataCacheConfig::default()` (enabled, 5s `refresh_interval`). Both asserted
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
    ///
    /// Reads `MetadataCache::hits`/`misses`, never the `head_*` counters this file asserts on,
    /// so the apparatus cannot move the figures under test.
    async fn resident(&self, cache_key: &str) -> Option<NewCacheMetadata> {
        self.cache.get_metadata_cache().get(cache_key).await
    }

    fn meta_path(&self, cache_key: &str) -> PathBuf {
        self.cache.get_new_metadata_file_path(cache_key)
    }

    /// `(head_hits, head_misses, head_disk_hits)`.
    fn head_counters(&self) -> (u64, u64, u64) {
        let m = self.cache.get_metadata_cache().metrics();
        (m.head_hits, m.head_misses, m.head_disk_hits)
    }

    /// Publish a range through the peer-shaped manager, so the `.meta` carries `ranges`.
    async fn seed_ranges(&mut self, cache_key: &str, etag: &str) {
        let body = vec![0x9Au8; OBJECT_LEN as usize];
        self.seeder
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
            .expect("the range publish must succeed");

        let on_disk = read_meta(&self.meta_path(cache_key));
        assert!(
            !on_disk.ranges.is_empty(),
            "FIXTURE: {} must carry ranges on disk, or ram_admission_permitted refuses the \
             merged document below and the entry can never become resident",
            cache_key
        );
        assert!(
            self.resident(cache_key).await.is_none(),
            "FIXTURE: seeding {} populated the RAM metadata tier. The seeder holds no \
             MetadataCache, so this means the fixture changed shape and residency below would \
             not be attributable to the production path that is supposed to establish it",
            cache_key
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

/// The HEAD response. ETag and length MUST equal the seeded document's — see the module docs
/// on `etag_changed || size_changed`.
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

/// Everything that has to hold before any reading below means anything.
async fn assert_apparatus(f: &Fixture) {
    for key in [SUBJECT_KEY, DISK_KEY] {
        assert!(
            !f.cache.get_effective_get_ttl(key).await.is_zero(),
            "APPARATUS: resolved get_ttl for {} is zero, so MetadataTtls::forbids_admission \
             would refuse admission and the entry could never become resident",
            key
        );
        assert!(
            !f.cache.get_effective_head_ttl(key).await.is_zero(),
            "APPARATUS: resolved head_ttl for {} is zero, so admission would be refused AND \
             is_head_fresh would return false — two unrelated reasons for the subject to reach \
             disk",
            key
        );
    }
}

/// `ram-tier-effectiveness:R3.7`. THE guard.
///
/// A HEAD answered from a resident, HEAD-fresh Metadata_Cache entry performs no Shared_Storage
/// `.meta` read. Arm 1 (DISK) proves the counters that arm 2 asserts flat can move; arm 2
/// (SUBJECT) is the property.
#[tokio::test]
async fn a_resident_head_fresh_entry_answers_a_head_with_no_shared_storage_read() {
    let mut f = Fixture::new().await;
    assert_apparatus(&f).await;

    // ── ARM 1, DISK: the counter-liveness proof. ──────────────────────────────────────────
    //
    // A key that is HEAD-fresh on disk and NOT resident must drive `head_misses` and
    // `head_disk_hits` up by one each. Without this, arm 2's two flat readings are
    // indistinguishable from counters that nothing increments, which is the shape a broken
    // apparatus passes.
    //
    // HEAD-freshness is written straight onto the `.meta` with the same `refresh_head_ttl`
    // the production path calls, rather than by driving `store_head_cache_entry_unified` and
    // then evicting: that would publish to the tier and the key would have to be removed from
    // it again, which is a seam this test does not need.
    f.seed_ranges(DISK_KEY, DISK_ETAG).await;
    let disk_meta = f.meta_path(DISK_KEY);
    let mut disk_doc = read_meta(&disk_meta);
    disk_doc.refresh_head_ttl(TTL);
    write_meta(&disk_meta, &disk_doc);
    assert!(
        read_meta(&disk_meta).head_expires_at.is_some(),
        "FIXTURE: the disk arm's document must carry head_expires_at, or is_head_fresh returns \
         false and the disk tier never reaches its serve arm"
    );
    assert!(
        f.resident(DISK_KEY).await.is_none(),
        "APPARATUS: the disk arm's key is resident, so its HEAD would be answered from RAM and \
         it would prove nothing about head_misses or head_disk_hits"
    );

    let (hits_before, misses_before, disk_hits_before) = f.head_counters();
    let disk_head = f
        .cache
        .get_head_cache_entry_unified(DISK_KEY, TTL)
        .await
        .expect("the disk arm's HEAD lookup must not error");
    let (hits_after, misses_after, disk_hits_after) = f.head_counters();

    assert!(
        disk_head.is_some(),
        "APPARATUS: the disk arm's HEAD was not answered at all, so neither counter moved for \
         a reason that has nothing to do with the RAM tier"
    );
    assert_eq!(
        misses_after - misses_before,
        1,
        "APPARATUS: head_misses did not move across a HEAD that had to reach disk \
         (before={} after={}). This counter is what arm 2 asserts FLAT, so if it cannot move \
         here arm 2's reading is not evidence of anything",
        misses_before,
        misses_after
    );
    assert_eq!(
        disk_hits_after - disk_hits_before,
        1,
        "APPARATUS: head_disk_hits did not move across a HEAD answered from the disk .meta \
         (before={} after={}). Same reasoning as head_misses above",
        disk_hits_before,
        disk_hits_after
    );
    assert_eq!(
        hits_after, hits_before,
        "APPARATUS: head_hits moved across a HEAD that was supposed to miss RAM (before={} \
         after={}), so the two arms are not reading distinct branches and arm 2's increment \
         would not localise the serve to the RAM tier",
        hits_before, hits_after
    );

    // ── ARM 2, SUBJECT: the property. ─────────────────────────────────────────────────────
    //
    // Established through production paths only: a peer-shaped range publish gives the `.meta`
    // its ranges, then `store_head_cache_entry_unified`'s merged `Ok` arm (census row 3) sets
    // the HEAD fields and publishes the document to the tier.
    f.seed_ranges(SUBJECT_KEY, SUBJECT_ETAG).await;
    f.cache
        .store_head_cache_entry_unified(
            SUBJECT_KEY,
            head_response_headers(SUBJECT_ETAG),
            head_cache_metadata(SUBJECT_ETAG),
        )
        .await
        .expect("the subject's HEAD store must succeed");

    let subject_meta = f.meta_path(SUBJECT_KEY);
    let on_disk = read_meta(&subject_meta);
    assert!(
        !on_disk.ranges.is_empty(),
        "FIXTURE: the merge cleared the subject's ranges, which means the HEAD response's ETag \
         or length disagreed with the seeded document and update_metadata_head_fields took its \
         etag_changed || size_changed branch. Admission is then refused for a reason unrelated \
         to this test"
    );
    assert!(
        on_disk.head_expires_at.is_some(),
        "FIXTURE: the merged document carries no head_expires_at, so is_head_fresh returns \
         false and the RAM arm cannot serve"
    );

    // Residency, asserted immediately before the reading. `metadata_cache.refresh_interval` is
    // 5s by default, so a slow run would re-read disk and fail for a reason unrelated to the
    // property; `touch` moves `last_accessed` only, so this check does not extend the window
    // it is checking.
    let resident = f.resident(SUBJECT_KEY).await.unwrap_or_else(|| {
        panic!(
            "APPARATUS: the subject is NOT resident in the RAM metadata tier, so the HEAD below \
             would reach disk on any build and both sides of the red/green pair would be red. \
             Either census row 3 declined admission, or \
             metadata_cache.refresh_interval elapsed mid-test — re-run"
        )
    });
    assert_eq!(
        resident.object_metadata.content_length, OBJECT_LEN,
        "APPARATUS: the resident document does not describe the seeded object"
    );

    // Make the read IMPOSSIBLE rather than merely unnecessary, so the counters are
    // corroborated by the filesystem. Scope of the claim: this is
    // get_head_cache_entry_unified's ONLY Shared_Storage read path — see the module docs.
    let hidden = subject_meta.with_extension("meta.hidden-by-rt9");
    std::fs::rename(&subject_meta, &hidden).expect("the subject's .meta must be renamable");
    assert!(
        !subject_meta.exists(),
        "FIXTURE: the subject's .meta is still at {:?}, so a disk read could still be \
         satisfied and the counters below would not be corroborated",
        subject_meta
    );

    let (hits_before, misses_before, disk_hits_before) = f.head_counters();
    let subject_head = f
        .cache
        .get_head_cache_entry_unified(SUBJECT_KEY, TTL)
        .await
        .expect("the subject's HEAD lookup must not error");
    let (hits_after, misses_after, disk_hits_after) = f.head_counters();

    // THE ASSERTIONS.
    let entry = subject_head.unwrap_or_else(|| {
        panic!(
            "R3.7 VIOLATION: a HEAD on a resident, HEAD-fresh entry returned no entry at all \
             once the shared .meta was renamed away. The RAM metadata tier did not answer it, \
             so the tier buys nothing on the HEAD path — counters: head_hits {}->{}, \
             head_misses {}->{}, head_disk_hits {}->{}",
            hits_before, hits_after, misses_before, misses_after, disk_hits_before, disk_hits_after
        )
    });
    assert_eq!(
        entry.metadata.content_length, OBJECT_LEN,
        "R3.7: the RAM-served HEAD reported the wrong length, so the entry answering it is not \
         the document that was made resident"
    );
    assert_eq!(
        misses_after - misses_before,
        0,
        "R3.7 VIOLATION: head_misses moved ({}->{}) across a HEAD on a resident, HEAD-fresh \
         entry. record_head_miss() is unconditional and sits ABOVE the metadata_path.exists() \
         test, so this means get_head_cache_entry_unified entered its disk tier — the RAM arm \
         did not answer, and every HEAD on a warm key pays a Shared_Storage lookup. Arm 1 \
         showed this counter moving in this same run, so this is not an apparatus failure",
        misses_before,
        misses_after
    );
    assert_eq!(
        disk_hits_after - disk_hits_before,
        0,
        "R3.7 VIOLATION: head_disk_hits moved ({}->{}) across a HEAD on a resident, HEAD-fresh \
         entry, so the disk .meta answered it. Note the shared .meta was renamed away before \
         this call, which makes a disk serve of the ORIGINAL path impossible — so a delta here \
         also means the function found a .meta somewhere this test did not hide",
        disk_hits_before,
        disk_hits_after
    );
    assert_eq!(
        hits_after - hits_before,
        1,
        "R3.7 VIOLATION: head_hits did not move ({}->{}) across a HEAD that returned an entry \
         with no disk read. The serve did not come from the RAM-tier arm, so something else \
         answered it and this file is no longer measuring the branch it names. On its own this \
         increment would NOT establish that no read happened — that is the two flat readings \
         above — but its absence means the branch under test never ran",
        hits_before,
        hits_after
    );

    // Convergence check: a read-free serve must leave the entry resident, or the next HEAD
    // inside refresh_interval pays the disk read this property exists to eliminate.
    assert!(
        f.resident(SUBJECT_KEY).await.is_some(),
        "R3.7: the RAM-tier serve dropped the resident entry, so the read-free property holds \
         for exactly one HEAD per refresh_interval rather than for every HEAD inside it"
    );

    // Leave the tree as found, so a later assertion in this file cannot inherit a hidden file.
    std::fs::rename(&hidden, &subject_meta).expect("the subject's .meta must be restorable");
}
