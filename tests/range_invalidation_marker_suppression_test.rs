//!
//! `range-invalidation-completeness:R2`, the red/green pair for `RANGE-INVAL-R2`.
//!
//! # The defect
//!
//! `DiskCacheManager::find_pending_journal_ranges` skipped every journal entry whose
//! operation was not `Add`:
//!
//! ```text
//! if entry.operation != JournalOperation::Add { continue; }
//! ```
//!
//! `JournalOperation::Remove` already existed and already had producers — and since
//! `RANGE-INVAL-R1` landed, `invalidate_all_ranges` writes one per `.bin` it deletes. So
//! immediately after an invalidation this function reported coverage for a range whose
//! file was gone. Four of its six consumers ask it for **coverage** and would report a
//! cache hit that cannot be served; three ask it for **path resolution** and would
//! resolve a `RangeSpec` pointing at a deleted file.
//!
//! # What the assertions measure, and why it is not bytes
//!
//! Every assertion below reads **the set of range files the function reports for the
//! key** (`:R6.3`), never a served byte count. A superseding object commonly has the same
//! content at the same offsets, so bytes cannot discriminate — this is the same
//! assert-the-predicate-the-code-evaluates family recorded in
//! the pre-push checklist step 8.
//!
//! # The four cases, and which of them is evidence
//!
//! | Case | Shape | Expected | Role |
//! |---|---|---|---|
//! | (i) | marker **after** its `Add`, **same** journal file | suppressed | **discriminating** — the only case that is red against the early-return shape *deterministically* |
//! | (ii) | marker in a **different instance's** journal file | suppressed | **discriminating** — cross-instance coverage |
//! | (iii) | marker **before** its `Add`, same file | suppressed | **CONTROL, not evidence** — passes with the early returns intact |
//! | (iv) | `Add` republished **after** the marker | **retained** | the two-sided half — an over-broad suppression fails only here |
//!
//! Cases (i) and (ii) exist because of a finding that is not in the clause text:
//! the function used to `return Ok(vec![range])` **mid-scan** on an `Add` that exactly
//! matched or fully contained the request — before later lines in the same file and
//! before any later journal file was opened. The exact-match return is the *normal* path
//! for a whole-object read, so a marker that sits after its `Add`, or in a peer's file,
//! was unreachable. **A fix that collects markers inside that loop and keeps the early
//! returns passes case (iii) and fails cases (i) and (ii)**, which is precisely why (iii)
//! is labelled the control: it would certify a fix that is still unsound.
//!
//! Every request below is sized to the `Add`'s **exact** extent, because that is the
//! shape that triggered the early return. A partially-overlapping request would never
//! have reached it and would prove nothing about trap 1.
//!
//! # Two guards that stop the suppression assertions passing vacuously
//!
//! Cases (i), (ii) and (iii) all assert an **empty** result, which is the dangerous
//! direction: a fixture that never reaches the function at all — shared-storage mode off,
//! journals directory missing, a cache-key mismatch — returns empty and passes all three
//! while proving nothing.
//!
//! - **(v) is the apparatus control.** An `Add` with no marker anywhere MUST be reported.
//!   It shares the manager, the journals directory and the key shape with (i)-(iii), so
//!   if it passes, an empty result there is a verdict about marker handling rather than
//!   about the fixture. If (v) fails, read nothing into (i)-(iii).
//! - **(vi) is the width guard, and what it guards is NARROWER than it reads.** A marker at a
//!   *different* extent must not suppress. Measured two-sided by
//!   `RANGE-INVAL-OWED-RED-SIDES` (red side 4): dropping the exact-`(start, end)` equality in
//!   phase 2 **alone leaves this case GREEN**, because phase 1's overlap filter discards a
//!   marker at a non-overlapping extent before it is ever accumulated, so there is nothing
//!   for the equality to reject. (vi) goes red only against an implementation that *also*
//!   lets markers bypass that overlap filter — which is the genuine "once any marker exists
//!   for the key, suppress everything" shape. So (vi) covers the pair of filters, not the
//!   equality clause on its own; a change that narrows or widens either one needs its own
//!   red side rather than inheriting this case's.
//! - **(vii) covers the extent equality alone.** Its marker (`0-2047`) overlaps the `0-1023`
//!   request, so phase 1 admits it and only the equality clause rejects it. Deleting that
//!   clause reds (vii) and nothing else.
//!
//! # Red side
//!
//! Taken twice, per the unit's return: once against the shipped `!= Add { continue }`
//! shape (deterministically red on (i), (ii) and (iii)) and once against the
//! **early-return shape** — markers collected inside the loop with both early returns
//! left in place — which is the one that proves trap 1 is closed. Under that second
//! shape case (i) is red deterministically; case (ii)'s outcome depends on `read_dir`
//! order, since a marker file enumerated before the `Add`'s file is collected in time to
//! suppress it. Stated rather than claimed. See the unit's return for the verbatim panic
//! output, the `grep -c` confirming the defect was in the tree before the result was
//! read, and the `pgrep`/`diff -q` checks.
//!
//! Two further red sides, covering the over-broad direction, were taken by
//! `RANGE-INVAL-OWED-RED-SIDES` (same protocol):
//!
//! - **The republish comparison deleted** from phase 2. Case (iv) is the only failure.
//! - **The extent equality deleted**, twice. Alone: every case passes, including (vi).
//!   Combined with markers bypassing phase 1's overlap filter: (vi) is the only failure.
//!   The header's (iv) and (vi) bullets carry what each of those means.
use s3_proxy::cache_types::RangeSpec;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::hybrid_metadata_writer::{ConsolidationTrigger, HybridMetadataWriter};
use s3_proxy::journal_manager::{JournalEntry, JournalOperation};
use s3_proxy::metadata_lock_manager::MetadataLockManager;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Every key parses into a bucket and an object key, matching the production shape.
const BUCKET: &str = "marker-suppression-bucket";

const START: u64 = 0;
const END: u64 = 1023;

/// A second, non-overlapping extent, used only by the width guard (vi).
const OTHER_START: u64 = 4096;
const OTHER_END: u64 = 5119;

/// The instance performing the invalidation — the one whose journal a real
/// `invalidate_all_ranges` writes its markers into.
const INVALIDATING_INSTANCE: &str = "rir2-invalidating-instance";
/// The instance whose journal holds the `Add`. Deliberately never the one above, so a
/// cross-instance read is exercised by construction rather than by coincidence.
const PEER_INSTANCE: &str = "rir2-peer-instance";

struct Fixture {
    _dir: TempDir,
    journals_dir: PathBuf,
    manager: DiskCacheManager,
}

/// A `DiskCacheManager` in **shared storage mode**.
///
/// The `HybridMetadataWriter` is load-bearing rather than decorative:
/// `find_pending_journal_ranges` returns `Ok(Vec::new())` immediately when
/// `is_shared_storage_mode()` is false, and `DiskCacheManager::new` leaves the writer
/// `None`. Without this the whole file would pass vacuously — which is exactly what
/// guard (v) exists to detect.
async fn seed() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let cache_dir = dir.path().to_path_buf();

    let mut manager = DiskCacheManager::new(cache_dir.clone(), false, 1024, false, 1_048_576);
    manager.initialize().await.expect("disk cache init");

    let lock_manager = Arc::new(MetadataLockManager::new(
        cache_dir.clone(),
        Duration::from_millis(300),
        0,
    ));
    let journal_manager = Arc::new(s3_proxy::journal_manager::JournalManager::new(
        cache_dir.clone(),
        INVALIDATING_INSTANCE.to_string(),
    ));
    let consolidation_trigger = Arc::new(ConsolidationTrigger::new(10 * 1024 * 1024, 1000));
    manager.set_hybrid_metadata_writer(Arc::new(Mutex::new(HybridMetadataWriter::new(
        cache_dir.clone(),
        lock_manager,
        journal_manager,
        consolidation_trigger,
    ))));
    assert!(
        manager.is_shared_storage_mode(),
        "fixture precondition: the manager must be in shared storage mode, or \
         find_pending_journal_ranges short-circuits and every assertion here is vacuous"
    );

    let journals_dir = cache_dir.join("metadata").join("_journals");
    std::fs::create_dir_all(&journals_dir).expect("journals dir");

    Fixture {
        _dir: dir,
        journals_dir,
        manager,
    }
}

/// A `RangeSpec` with `created_at` set explicitly, because the republish discriminator
/// compares exactly that field against the marker's `timestamp`.
fn range_spec(start: u64, end: u64, file_path: &str, created_at: SystemTime) -> RangeSpec {
    let len = end - start + 1;
    let mut spec = RangeSpec::new(
        start,
        end,
        file_path.to_string(),
        CompressionAlgorithm::None,
        len,
        len,
    )
    // As a 3.0.0 writer records it (`RangeSpec::is_unverified_prefix`).
    .with_file_identity(Some(s3_proxy::cache_types::RangeFileIdentity {
        size: len,
        mtime_ns: 1,
    }));
    spec.created_at = created_at;
    spec
}

fn entry(
    cache_key: &str,
    operation: JournalOperation,
    spec: RangeSpec,
    timestamp: SystemTime,
    instance_id: &str,
) -> JournalEntry {
    JournalEntry {
        timestamp,
        instance_id: instance_id.to_string(),
        cache_key: cache_key.to_string(),
        range_spec: spec,
        operation,
        // Empty, exactly as `HybridMetadataWriter::write_journal_only` writes it. Nothing
        // in this function reads it; leaving it empty keeps that true.
        range_file_path: String::new(),
        metadata_version: 0,
        new_ttl_secs: None,
        object_ttl_secs: None,
        access_increment: None,
        object_metadata: None,
        fill_witness: None,
    }
}

/// Append journal lines **in the order given**, which is the whole point of cases (i) and
/// (iii): they differ only in the order of the same two lines.
fn write_journal(fixture: &Fixture, instance_id: &str, entries: &[JournalEntry]) {
    let path = fixture
        .journals_dir
        .join(format!("{}.journal", instance_id));
    let body: String = entries
        .iter()
        .map(|e| format!("{}\n", serde_json::to_string(e).expect("serialise entry")))
        .collect();
    std::fs::write(&path, body).expect("write journal file");
}

/// The predicate every assertion reads: the set of relative range-file paths the function
/// reports for the key (`:R6.3`).
async fn reported_paths(fixture: &Fixture, cache_key: &str, start: u64, end: u64) -> Vec<String> {
    let mut paths: Vec<String> = fixture
        .manager
        .find_pending_journal_ranges(cache_key, start, end)
        .await
        .expect("journal lookup must not error")
        .into_iter()
        .map(|spec| spec.file_path)
        .collect();
    paths.sort();
    paths
}

/// A marker recorded AFTER its `Add`, in the SAME journal file.
///
/// **This is the discriminating case for trap 1.** The `Add` exactly matches the request,
/// so the pre-change code returned at that line and never read the marker two lines
/// later. It is red both against the shipped `!= Add` shape and, deterministically,
/// against the early-return shape.
#[tokio::test]
async fn case_i_a_marker_after_its_add_in_the_same_file_suppresses_the_range() {
    let fixture = seed().await;
    let key = format!("{}/case-i-object.bin", BUCKET);
    let created = SystemTime::now();
    let path = "marker-suppression-bucket/1a/2b3/case-i-object.bin_0-1023.bin";

    write_journal(
        &fixture,
        PEER_INSTANCE,
        &[
            entry(
                &key,
                JournalOperation::Add,
                range_spec(START, END, path, created),
                created,
                PEER_INSTANCE,
            ),
            entry(
                &key,
                JournalOperation::Remove,
                range_spec(START, END, path, created),
                created + Duration::from_secs(1),
                INVALIDATING_INSTANCE,
            ),
        ],
    );

    let reported = reported_paths(&fixture, &key, START, END).await;
    assert!(
        reported.is_empty(),
        "DEFECT: a Removal_Marker recorded AFTER its Add in the same journal file did \
         not suppress the range. The Add exactly matches the request, so this is the \
         early-return shape surviving: the scan returned at the Add and never reached \
         the marker. reported={:?}",
        reported
    );
}

/// A marker in a DIFFERENT instance's journal file from the `Add`.
///
/// Cross-instance coverage is inherited from the existing `read_dir(metadata/_journals)`
/// — "per-instance" describes the writer, not the reader — but inherited is not asserted,
/// so it is asserted here. A fixture where one instance holds both sides passes whatever
/// the code does.
#[tokio::test]
async fn case_ii_a_marker_in_a_peers_journal_file_suppresses_the_range() {
    let fixture = seed().await;
    let key = format!("{}/case-ii-object.bin", BUCKET);
    let created = SystemTime::now();
    let path = "marker-suppression-bucket/4c/5d6/case-ii-object.bin_0-1023.bin";

    write_journal(
        &fixture,
        PEER_INSTANCE,
        &[entry(
            &key,
            JournalOperation::Add,
            range_spec(START, END, path, created),
            created,
            PEER_INSTANCE,
        )],
    );
    write_journal(
        &fixture,
        INVALIDATING_INSTANCE,
        &[entry(
            &key,
            JournalOperation::Remove,
            range_spec(START, END, path, created),
            created + Duration::from_secs(1),
            INVALIDATING_INSTANCE,
        )],
    );

    let reported = reported_paths(&fixture, &key, START, END).await;
    assert!(
        reported.is_empty(),
        "DEFECT: a Removal_Marker in a peer instance's journal file did not suppress the \
         range. Either the scan stopped at the Add before opening the other file, or the \
         enumeration is scoped to this instance's own journal. reported={:?}",
        reported
    );
}

/// A marker recorded BEFORE its `Add`, same file.
///
/// **CONTROL, NOT EVIDENCE.** This case passes with the early returns fully intact,
/// because the marker line is read before the `Add` that would have returned. It is here
/// so that a failure in (i)/(ii) can be attributed to scan *ordering* rather than to
/// marker handling being absent altogether — not as a second demonstration of the fix.
#[tokio::test]
async fn case_iii_control_a_marker_before_its_add_suppresses_the_range() {
    let fixture = seed().await;
    let key = format!("{}/case-iii-object.bin", BUCKET);
    let marker_at = SystemTime::now();
    // Created at or before the marker was recorded, so the republish guard does NOT
    // retain it. File order and creation order are independent here, which is the whole
    // difference between this case and case (iv).
    let created = marker_at - Duration::from_secs(1);
    let path = "marker-suppression-bucket/7e/8f9/case-iii-object.bin_0-1023.bin";

    write_journal(
        &fixture,
        PEER_INSTANCE,
        &[
            entry(
                &key,
                JournalOperation::Remove,
                range_spec(START, END, path, created),
                marker_at,
                INVALIDATING_INSTANCE,
            ),
            entry(
                &key,
                JournalOperation::Add,
                range_spec(START, END, path, created),
                created,
                PEER_INSTANCE,
            ),
        ],
    );

    let reported = reported_paths(&fixture, &key, START, END).await;
    assert!(
        reported.is_empty(),
        "a Removal_Marker preceding its Add must suppress the range. This is the CONTROL \
         case — if it fails while (i) and (ii) also fail, marker handling is absent \
         entirely rather than mis-ordered. reported={:?}",
        reported
    );
}

/// A range republished AFTER its marker must be RETAINED.
///
/// The two-sided half. The discriminator is not invented here: it is the rule
/// `JournalConsolidator::apply_journal_entries`' `Remove` arm applies — exact `(start, end)`
/// match, retain when `range_spec.created_at > marker.timestamp` — guarded there by a
/// comment recording the T1-T5 re-PUT sequence and the intermittent post-overwrite cache
/// miss it was added for. An implementation that suppresses everything at the extent
/// passes (i), (ii) and (iii) and fails only this, shipping that defect back.
///
/// **"Only this" is now measured, not argued.** `RANGE-INVAL-OWED-RED-SIDES` red side 3
/// deleted the `range.created_at <= *timestamp` term from phase 2's marker search:
/// `5 passed; 1 failed`, this case the single failure, reporting `[]` against the one path
/// owed. So this is the assertion that stands between the fix and the consolidator's
/// documented post-overwrite cache miss, and it can fail.
#[tokio::test]
async fn case_iv_a_range_republished_after_the_marker_is_retained() {
    let fixture = seed().await;
    let key = format!("{}/case-iv-object.bin", BUCKET);
    let marker_at = SystemTime::now();
    // Strictly later than the marker's timestamp: the re-PUT necessarily publishes after
    // the removal was recorded.
    let republished_at = marker_at + Duration::from_secs(1);
    let path = "marker-suppression-bucket/ab/cde/case-iv-object.bin_0-1023.bin";

    write_journal(
        &fixture,
        PEER_INSTANCE,
        &[
            entry(
                &key,
                JournalOperation::Remove,
                range_spec(START, END, path, marker_at - Duration::from_secs(5)),
                marker_at,
                INVALIDATING_INSTANCE,
            ),
            entry(
                &key,
                JournalOperation::Add,
                range_spec(START, END, path, republished_at),
                republished_at,
                PEER_INSTANCE,
            ),
        ],
    );

    let reported = reported_paths(&fixture, &key, START, END).await;
    assert_eq!(
        reported,
        vec![path.to_string()],
        "DEFECT: a range republished AFTER its Removal_Marker was suppressed. That is an \
         intermittent post-overwrite cache miss — the exact defect the consolidator's \
         Remove arm carries a timestamp guard to prevent. An over-broad suppression \
         fails here and nowhere else."
    );
}

/// APPARATUS CONTROL for cases (i)-(iii).
///
/// They assert an **empty** result, which a broken fixture also produces. This case
/// shares the manager, the journals directory and the key shape with them and asserts the
/// opposite outcome, so a passing (i)-(iii) is a verdict about marker handling rather
/// than about whether the fixture reaches the function at all. If this fails, read
/// nothing into the suppression cases.
#[tokio::test]
async fn case_v_apparatus_control_an_add_with_no_marker_is_reported() {
    let fixture = seed().await;
    let key = format!("{}/case-v-object.bin", BUCKET);
    let created = SystemTime::now();
    let path = "marker-suppression-bucket/f0/123/case-v-object.bin_0-1023.bin";

    write_journal(
        &fixture,
        PEER_INSTANCE,
        &[entry(
            &key,
            JournalOperation::Add,
            range_spec(START, END, path, created),
            created,
            PEER_INSTANCE,
        )],
    );

    let reported = reported_paths(&fixture, &key, START, END).await;
    assert_eq!(
        reported,
        vec![path.to_string()],
        "APPARATUS FAILURE, not a product verdict: an unmarked journal Add was not \
         reported, so this fixture does not reach the lookup at all. The suppression \
         cases (i)-(iii) cannot be read as evidence until this passes."
    );
}

/// WIDTH GUARD: a marker at a DIFFERENT extent must not suppress.
///
/// Exact `(start, end)` matching is half the discriminator, and an implementation that
/// drops it — suppressing every range for a key once any marker exists — passes (i), (ii)
/// and (iii). It fails here.
///
/// **Read the module header's (vi) bullet before relying on this case.** Measured: the
/// extent-equality clause in phase 2 can be deleted outright and this case stays green,
/// because phase 1's overlap filter never accumulates a marker at `OTHER_START-OTHER_END`
/// for a `START-END` request. What reds here is the two filters failing *together*.
#[tokio::test]
async fn case_vi_width_guard_a_marker_at_another_extent_does_not_suppress() {
    let fixture = seed().await;
    let key = format!("{}/case-vi-object.bin", BUCKET);
    let created = SystemTime::now();
    let path = "marker-suppression-bucket/45/678/case-vi-object.bin_0-1023.bin";
    let other_path = "marker-suppression-bucket/45/678/case-vi-object.bin_4096-5119.bin";

    write_journal(
        &fixture,
        PEER_INSTANCE,
        &[
            entry(
                &key,
                JournalOperation::Add,
                range_spec(START, END, path, created),
                created,
                PEER_INSTANCE,
            ),
            entry(
                &key,
                JournalOperation::Remove,
                range_spec(
                    OTHER_START,
                    OTHER_END,
                    other_path,
                    created - Duration::from_secs(5),
                ),
                created + Duration::from_secs(1),
                INVALIDATING_INSTANCE,
            ),
        ],
    );

    let reported = reported_paths(&fixture, &key, START, END).await;
    assert_eq!(
        reported,
        vec![path.to_string()],
        "DEFECT: a Removal_Marker for a different extent ({}-{}) suppressed the requested \
         range ({}-{}). The suppression must match the extent exactly; without that it \
         is over-broad and (i)-(iii) would pass while legitimate coverage disappears.",
        OTHER_START,
        OTHER_END,
        START,
        END
    );
}

/// EXTENT-EQUALITY GUARD: a marker that OVERLAPS the request without matching its extent must
/// not suppress it.
///
/// Case (vi) cannot see phase 2's `*start == range.start && *end == range.end` clause on its
/// own, because its marker does not overlap the request and phase 1 drops it first. Here the
/// marker (`0-2047`) overlaps a `0-1023` request, so it passes phase 1's overlap filter and
/// reaches `removal_markers`; only the extent equality stops it suppressing the `0-1023` `Add`.
/// It is timestamped after the `Add`'s `created_at`, so the republish comparison does not
/// rescue the range either.
///
/// # RED SIDE
///
/// With the extent equality deleted from phase 2 (leaving `range.created_at <= *timestamp`),
/// this case reports `[]` against the one path owed and fails. That is the one-hunk defect
/// under which all six earlier cases stayed green (`RANGE-INVAL-OWED-RED-SIDES`, red side 4a).
#[tokio::test]
async fn case_vii_an_overlapping_marker_at_a_different_extent_does_not_suppress() {
    let fixture = seed().await;
    let key = format!("{}/case-vii-object.bin", BUCKET);
    let created = SystemTime::now();
    let path = "marker-suppression-bucket/9a/bcd/case-vii-object.bin_0-1023.bin";
    let wider_path = "marker-suppression-bucket/9a/bcd/case-vii-object.bin_0-2047.bin";
    const WIDER_END: u64 = 2047;

    write_journal(
        &fixture,
        PEER_INSTANCE,
        &[
            entry(
                &key,
                JournalOperation::Add,
                range_spec(START, END, path, created),
                created,
                PEER_INSTANCE,
            ),
            entry(
                &key,
                JournalOperation::Remove,
                range_spec(
                    START,
                    WIDER_END,
                    wider_path,
                    created - Duration::from_secs(5),
                ),
                created + Duration::from_secs(1),
                INVALIDATING_INSTANCE,
            ),
        ],
    );

    let reported = reported_paths(&fixture, &key, START, END).await;
    assert_eq!(
        reported,
        vec![path.to_string()],
        "DEFECT: a Removal_Marker for {}-{}, which OVERLAPS the requested range {}-{} but \
         does not match its extent, suppressed it. Phase 1's overlap filter admits this \
         marker, so only phase 2's exact extent match can reject it.",
        START,
        WIDER_END,
        START,
        END
    );
}
