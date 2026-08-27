//! Task 10 of `write-cache-accounting-and-eviction`: one shared definition of
//! "this range counts toward the write (staging) tier".
//!
//! Before this change there were **six** definition sites and three of them
//! disagreed. Enumerated and verified against the tree on 2026-08-25:
//!
//! | Site | Direction | Predicate before |
//! |---|---|---|
//! | `disk_cache.rs` `store_range` | add | flag only |
//! | `disk_cache.rs` incremental range write | add | flag only |
//! | `journal_consolidator.rs` `write_multipart_journal_entries` | add | flag **or** path |
//! | `cache.rs` eviction Step 5 | subtract | flag **or** path |
//! | `cache.rs` invalidation | subtract | flag **or** path |
//! | `cache_initialization_coordinator.rs` coordinated scan | scan | flag only |
//!
//! The sixth (invalidation) was missing from the spec's original table. It
//! already used the union form, so it did not add a disagreement, but it is a
//! definition site and now routes through the shared predicate too.
//!
//! # What these tests are and are not evidence of
//!
//! The spec framed the disagreement as an active "opposite-direction leak": a
//! range under `mpus_in_progress/` committed without the flag would be
//! subtracted on eviction having never been added, driving `write_cache_size`
//! toward undershoot (which silently over-admits). **That is not reachable from
//! a live write on this tree**, and these tests do not claim otherwise.
//!
//! Every `RangeSpec` a live write path constructs derives `file_path` from
//! `strip_prefix(cache_dir/ranges)`, whose `Err` is propagated — so a path
//! outside `ranges/` cannot produce a `RangeSpec` at all. The four producers are
//! `store_range`, `finalize_incremental_range`,
//! `store_full_object_as_range_new`, and the multipart-completion finalizer,
//! and that last one *renames* each part out of `mpus_in_progress/` into
//! `ranges/` before building its `RangeSpec`. Staged multipart parts are
//! finalized by `finalize_incremental_part`, which builds no `RangeSpec`.
//!
//! What **is** reachable, and what `path_only_*` below covers, is a `RangeSpec`
//! **deserialised from a persisted `.meta`**, which carries whatever string was
//! written by whatever version wrote it. That is why these tests round-trip
//! through serde rather than using a struct literal: a struct literal would
//! prove the predicate's arithmetic while quietly assuming the reachability
//! question away.
//!
//! Requirements: 6.2

use s3_proxy::cache_types::{
    classify_new_range_as_staged, NewCacheMetadata, ObjectMetadata, RangeSpec,
};
use s3_proxy::compression::CompressionAlgorithm;

/// The predicate as the two `disk_cache.rs` add sites and the coordinated scan
/// evaluated it before task 10. Kept here, rather than deleted, so the
/// behaviour change is asserted against something concrete instead of described
/// in a comment.
fn legacy_flag_only_predicate(_range_file_path: &str, is_write_cached: bool) -> bool {
    is_write_cached
}

const MPUS_PATH: &str = "mpus_in_progress/upload-abc/part1.bin";
const RANGES_PATH: &str = "test-bucket/ab/cde/object_0-1023.bin";

fn metadata_with(
    range_file_path: &str,
    is_write_cached: bool,
    compressed: u64,
) -> NewCacheMetadata {
    let now = std::time::SystemTime::now();
    NewCacheMetadata {
        cache_key: "test-bucket/object".to_string(),
        object_metadata: ObjectMetadata {
            is_write_cached,
            content_length: compressed,
            ..Default::default()
        },
        ranges: vec![RangeSpec::new(
            0,
            compressed.saturating_sub(1),
            range_file_path.to_string(),
            CompressionAlgorithm::Lz4,
            compressed,
            compressed,
        )],
        created_at: now,
        expires_at: now + std::time::Duration::from_secs(3600),
        ..Default::default()
    }
}

/// Round-trip through JSON, which is what actually happens to every `.meta`
/// read off the shared volume. This is the only route by which a `file_path`
/// containing `mpus_in_progress/` reaches the predicate in production.
fn round_trip(metadata: &NewCacheMetadata) -> NewCacheMetadata {
    let json = serde_json::to_string(metadata).expect("metadata serialises");
    assert!(
        json.contains("is_write_cached"),
        "sanity: the flag must survive serialisation, or the path-only case below \
         would be testing a default rather than a persisted false"
    );
    serde_json::from_str(&json).expect("metadata deserialises")
}

// ---------------------------------------------------------------------------
// The predicate's truth table
// ---------------------------------------------------------------------------

#[test]
fn flag_only_is_staged() {
    assert!(classify_new_range_as_staged(RANGES_PATH, true));
}

#[test]
fn path_only_is_staged() {
    assert!(
        classify_new_range_as_staged(MPUS_PATH, false),
        "a range under mpus_in_progress/ is staged even with the flag clear"
    );
}

#[test]
fn both_is_staged() {
    assert!(classify_new_range_as_staged(MPUS_PATH, true));
}

#[test]
fn neither_is_not_staged() {
    assert!(!classify_new_range_as_staged(RANGES_PATH, false));
}

// ---------------------------------------------------------------------------
// The behaviour change, stated as a disagreement with the predicate that the
// three flag-only sites used before task 10.
//
// This is the test the spec calls for: "a behaviour change for the path-only
// case, so it needs its own test". For a flagged range the unification is a
// no-op, so a flag-only test proves nothing — `flagged_case_is_a_no_op` below
// asserts exactly that, so the point cannot be lost later.
// ---------------------------------------------------------------------------

#[test]
fn path_only_case_is_where_unification_changes_behaviour() {
    assert!(
        !legacy_flag_only_predicate(MPUS_PATH, false),
        "the old flag-only predicate did not count this range"
    );
    assert!(
        classify_new_range_as_staged(MPUS_PATH, false),
        "the unified predicate does count it — this is the behaviour change"
    );
}

#[test]
fn flagged_case_is_a_no_op() {
    for path in [RANGES_PATH, MPUS_PATH] {
        assert_eq!(
            legacy_flag_only_predicate(path, true),
            classify_new_range_as_staged(path, true),
            "for a flagged range the two predicates must agree, which is why a \
             flag-only test cannot demonstrate task 10"
        );
    }
    assert_eq!(
        legacy_flag_only_predicate(RANGES_PATH, false),
        classify_new_range_as_staged(RANGES_PATH, false),
        "and they agree for an ordinary unflagged range too"
    );
}

// ---------------------------------------------------------------------------
// The metadata-level helpers the scan sites use, driven through serde so the
// reachable (persisted) path-only case is the one under test.
// ---------------------------------------------------------------------------

#[test]
fn deserialised_path_only_meta_reports_staged_bytes() {
    let persisted = round_trip(&metadata_with(MPUS_PATH, false, 4096));

    assert!(
        !persisted.object_metadata.is_write_cached,
        "precondition: the flag really is clear, so this is the path-only case \
         and not an accidentally-flagged one"
    );
    assert!(
        persisted.ranges[0].file_path.contains("mpus_in_progress/"),
        "precondition: the mpus path survived the round-trip"
    );

    assert!(persisted.has_staged_range());
    assert_eq!(persisted.staged_compressed_size(), 4096);
}

#[test]
fn deserialised_ordinary_meta_reports_no_staged_bytes() {
    let persisted = round_trip(&metadata_with(RANGES_PATH, false, 4096));

    assert!(!persisted.has_staged_range());
    assert_eq!(persisted.staged_compressed_size(), 0);
}

#[test]
fn deserialised_flagged_meta_reports_staged_bytes() {
    let persisted = round_trip(&metadata_with(RANGES_PATH, true, 4096));

    assert!(persisted.has_staged_range());
    assert_eq!(persisted.staged_compressed_size(), 4096);
}

/// The per-range granularity is the point, not a detail. A scan that classified
/// at object granularity (all ranges or none, from the object's flag alone) is
/// what let the coordinated scan and the accumulator report different figures
/// for identical on-disk state — the accumulator credits and debits per range.
#[test]
fn staged_bytes_are_summed_per_range_not_per_object() {
    let now = std::time::SystemTime::now();
    let metadata = NewCacheMetadata {
        cache_key: "test-bucket/mixed".to_string(),
        object_metadata: ObjectMetadata {
            is_write_cached: false,
            ..Default::default()
        },
        ranges: vec![
            RangeSpec::new(
                0,
                999,
                MPUS_PATH.to_string(),
                CompressionAlgorithm::Lz4,
                1000,
                1000,
            ),
            RangeSpec::new(
                1000,
                1999,
                RANGES_PATH.to_string(),
                CompressionAlgorithm::Lz4,
                7,
                1000,
            ),
        ],
        created_at: now,
        expires_at: now + std::time::Duration::from_secs(3600),
        ..Default::default()
    };
    let persisted = round_trip(&metadata);

    assert!(persisted.has_staged_range());
    assert_eq!(
        persisted.staged_compressed_size(),
        1000,
        "only the mpus_in_progress/ range counts; an object-granularity \
         classification would have returned either 0 or 1007"
    );
}

// ---------------------------------------------------------------------------
// The object-level/per-range granularity mismatch — R12
//
// FOUND 2026-08-27 and FIXED the same day. The former `is_staged_range` took a
// per-range argument but its only per-range input was the `mpus_in_progress/`
// path substring, which its own doc comment records as unreachable for a
// freshly-built `RangeSpec`. For all live data the predicate was therefore
// **purely object-level**, while every credit and debit is **per range**.
//
// That is fine while every range of a flagged object was staged. It stops being
// fine the moment a flagged object gains a read-tier range, because the credit
// side and the debit side then disagree about that range.
//
// The fix: `RangeSpec` records its own membership, every credit site sets it, and
// every reader goes through `is_staged_range_spec`. The old function survives,
// renamed `classify_new_range_as_staged`, as the DECISION made once per new range
// and as the fallback for a range written before the field existed — which is why
// the truth-table tests above still stand unchanged.
//
// The two tests below are the pair that matters: one asserts the fix
// (`graduation_debit_should_equal_what_was_credited`, formerly `#[ignore]`d), the
// other asserts that the pre-fix behaviour is still what legacy data gets, because
// nothing rewrites a `.meta` already on the shared volume.
// ---------------------------------------------------------------------------

/// Build a `.meta` in the mixed state: object flagged staged, carrying one range
/// that was written by the PUT and one that was written by a later GET.
///
/// Both range paths are under `ranges/`, which is what every live producer emits,
/// so the `mpus_in_progress/` arm of the predicate cannot fire for either.
///
/// `record_membership` selects which era of `.meta` is being modelled, and both
/// still occur on a live fleet:
///
/// - `true` — written by this release or later. Range A was stamped `Some(true)`
///   by the write-through PUT (`disk_cache::store_range`, or
///   `CacheManager::store_put_as_write_cached_range_with_ttl` for the buffered
///   path); range B was stamped `Some(false)` by the GET range-miss store, which
///   builds a fresh `ObjectMetadata` from the S3 response
///   (`extract_object_metadata_from_response`) and therefore carries
///   `is_write_cached: false`. Those are the same two values each site passes to
///   the accounting, so the recorded flags and the credits agree by construction.
/// - `false` — written by an earlier release, which had nowhere to record it. The
///   predicate falls back to the object flag, so the defect is still present for
///   this data until the ranges are rewritten. That is deliberate and is what the
///   upgrade note in `CHANGELOG.md` describes; a migration that guessed would
///   have to guess `false`, which is the near-zero re-grounding R12.1 exists to
///   prevent.
fn mixed_state_metadata_with(
    staged: u64,
    read_tier: u64,
    record_membership: bool,
) -> NewCacheMetadata {
    let now = std::time::SystemTime::now();
    let mk = |start: u64, end: u64, name: &str, size: u64, is_staged: bool| {
        let path = format!("test-bucket/ab/cde/{}", name);
        if record_membership {
            RangeSpec::new_staged(
                start,
                end,
                path,
                CompressionAlgorithm::Lz4,
                size,
                size,
                is_staged,
            )
        } else {
            RangeSpec::new(start, end, path, CompressionAlgorithm::Lz4, size, size)
        }
    };
    NewCacheMetadata {
        cache_key: "test-bucket/object".to_string(),
        object_metadata: ObjectMetadata {
            // Still flagged: nothing has graduated it, and appending a range does
            // not clear it — `build_or_load_metadata` takes the journal entry's
            // `object_metadata` only when CREATING a `.meta`, never when one
            // already exists (`journal_consolidator.rs:3720-3729`).
            is_write_cached: true,
            content_length: staged + read_tier,
            ..Default::default()
        },
        ranges: vec![
            mk(0, staged - 1, "object_0-4095.bin", staged, true),
            mk(
                staged,
                staged + read_tier - 1,
                "object_4096-8191.bin",
                read_tier,
                false,
            ),
        ],
        created_at: now,
        expires_at: now + std::time::Duration::from_secs(3600),
        ..Default::default()
    }
}

/// The graduation debit over-charges by the read-tier range's size.
///
/// This is an arithmetic demonstration, not a fixture-dependent one, so it does
/// not rest on reproducing the exact request sequence. The two figures it
/// compares are both taken from production code paths:
///
/// - **What was credited.** `DiskCacheManager::store_range` computes
///   `counts_as_staged` from the `ObjectMetadata` **passed to it**
///   (`disk_cache.rs:1257`). The GET range-miss path builds that fresh from the
///   S3 response headers via `extract_object_metadata_from_response`
///   (`http_proxy.rs:12620`), so it carries `is_write_cached: false` and the range
///   is credited to `total_size` only. So `write_cache_size` gained `staged` and
///   nothing else.
/// - **What was debited, before the fix.** `refresh_write_cache_ttl` debits
///   `metadata.staged_compressed_size()`, which filtered through the former
///   `is_staged_range(path, self.object_metadata.is_write_cached)` — one shared
///   object-level flag — so it returned `staged + read_tier`. It now filters through
///   `is_staged_range_spec`, which reads each range's recorded membership.
///
/// Net effect on `write_cache_size` before the fix:
/// `staged - (staged + read_tier)` = `-read_tier`. Undershoot, which is the dangerous
/// direction: it silently over-admits rather than refusing.
///
/// The same asymmetry hit read-tier eviction Step 5 and `debit_removed_ranges`, both
/// of which debited `subtract_write_cache` for a range that was never credited to it.
/// Those two are covered by `mixed_object_debits_only_what_the_staging_tier_was_credited`
/// in `cache.rs`'s `debit_removed_ranges_tests` (design test 17), because this file's
/// tests reach `staged_compressed_size()` and not the accumulator.
/// UNCHANGED IN MEANING, NARROWED IN SCOPE (2026-08-27, R12 fix).
///
/// This asserted the pre-fix behaviour for *all* data. It now asserts it for
/// **legacy** data only — a `.meta` written before per-range membership existed,
/// whose ranges deserialise as `None` and therefore still take the object-flag
/// fallback. Kept rather than deleted, and this is the point: the fix does not
/// retroactively correct a `.meta` already on the shared volume. Such an object
/// keeps over-reporting until its ranges are rewritten, which is exactly what the
/// upgrade note has to say, and a test is a better place to pin that than prose.
///
/// The recorded-membership case is `graduation_debit_should_equal_what_was_credited`
/// below, which is the same fixture with `record_membership: true`.
#[test]
fn a_legacy_flagged_object_still_classifies_every_range_as_staged() {
    const STAGED: u64 = 4096;
    const READ_TIER: u64 = 4096;

    let metadata = round_trip(&mixed_state_metadata_with(STAGED, READ_TIER, false));

    for r in &metadata.ranges {
        assert_eq!(
            r.staged, None,
            "fixture: this test is about UNRECORDED membership. A recorded value here \
             would make it assert the opposite of what it claims"
        );
    }

    // Precondition: neither range can reach the path arm of the predicate, so the
    // object flag is the only thing classifying them.
    for r in &metadata.ranges {
        assert!(
            !r.file_path.contains("mpus_in_progress/"),
            "fixture: both ranges must sit under ranges/, or this test measures the \
             path arm rather than the object-flag arm"
        );
    }

    assert_eq!(
        metadata.staged_compressed_size(),
        STAGED + READ_TIER,
        "legacy behaviour, preserved on purpose: with membership unrecorded the \
         object-level flag classifies EVERY range as staged, including one that was \
         credited to total_size only. Changing this would mean guessing a tier for \
         pre-upgrade data, and the only available guess drives write_cache_size to \
         near zero (R12.1)"
    );
}

/// THE ACCEPTANCE TEST for R12 (design test 15). Was `#[ignore]`d and failing
/// `left: 8192, right: 4096`; the `#[ignore]` was removed on 2026-08-27 when the
/// per-range flag landed, per R12.8.
///
/// It survives the fix without weakening because the assertion never moved — only
/// the fixture gained the two values production now records, each one traced to the
/// site that writes it (see `mixed_state_metadata_with`). The comparison is still
/// "the debit must equal what was credited", and the credited figure is still
/// derived from the credit sites' own behaviour rather than from the result.
#[test]
fn graduation_debit_should_equal_what_was_credited() {
    const STAGED: u64 = 4096;
    const READ_TIER: u64 = 4096;

    let metadata = round_trip(&mixed_state_metadata_with(STAGED, READ_TIER, true));

    // Precondition: the fixture must be genuinely mixed. Two ranges recorded on
    // opposite sides is the whole state under test, and a fixture where both agree
    // would pass under an object-level predicate too.
    assert_eq!(
        metadata
            .ranges
            .iter()
            .filter(|r| r.staged == Some(true))
            .count(),
        1,
        "fixture: exactly one range must record itself staged"
    );
    assert_eq!(
        metadata
            .ranges
            .iter()
            .filter(|r| r.staged == Some(false))
            .count(),
        1,
        "fixture: exactly one range must record itself UNstaged, and the object flag \
         must still be set — that combination is the defect's state"
    );
    assert!(
        metadata.object_metadata.is_write_cached,
        "fixture: the object flag must still be set, or the per-range values are not \
         being tested against anything"
    );

    // What the credit sites actually put into write_cache_size: the PUT's range only.
    // `store_range` classified the GET's range with `is_write_cached: false`, so it
    // credited `total_size` and not `write_cache_size`.
    let credited_to_staging = STAGED;

    assert_eq!(
        metadata.staged_compressed_size(),
        credited_to_staging,
        "graduation must debit only what was credited. Over-debiting by {} bytes (the \
         read-tier range) drives write_cache_size into undershoot — the direction that \
         silently over-admits rather than refusing. This is R12's acceptance test, so a \
         failure here means the per-range membership stopped being read: check that \
         `staged_compressed_size` still filters through `is_staged_range_spec` and not \
         through `classify_new_range_as_staged`, which derives every range's tier from \
         one object-level bool.",
        READ_TIER
    );
}

// ---------------------------------------------------------------------------
// Design test 16 — the migration guard (R12.1)
//
// This is the most important test in the file, and it guards a failure that is
// worse than the defect being fixed. `RangeSpec::staged` is `Option<bool>`. Had
// it been declared `#[serde(default)] bool`, serde would read `false` for every
// range in every `.meta` written by an earlier release, the first post-upgrade
// Validation_Scan would compute `write_cache_size` near zero fleet-wide, and it
// would PERSIST that figure. A drained staging tier and a mis-migrated one are
// indistinguishable from any gauge, so nothing would report it.
//
// Asserted over a **deserialised** `.meta` with the key genuinely absent from the
// JSON, never over a struct literal. A literal cannot express "absent from the
// JSON", which is the only state that occurs in production, and a literal written
// as `staged: None` would pass while proving nothing about serde's behaviour.
// ---------------------------------------------------------------------------

/// Serialise, then **remove** the `staged` key from every range object, so the
/// result is byte-equivalent to a `.meta` written by a release that predates the
/// field. Asserts the removal happened, so the fixture cannot silently degrade
/// into "the key was present and null" — which serde treats as `None` too, and
/// which would make the test pass without exercising the `#[serde(default)]` path
/// at all.
fn round_trip_without_staged_field(metadata: &NewCacheMetadata) -> NewCacheMetadata {
    let mut value: serde_json::Value = serde_json::to_value(metadata).expect("metadata serialises");
    let ranges = value
        .get_mut("ranges")
        .and_then(|r| r.as_array_mut())
        .expect("fixture: metadata must carry a ranges array");
    assert!(
        !ranges.is_empty(),
        "fixture: a .meta with no ranges cannot exercise per-range membership"
    );
    let mut removed = 0usize;
    for range in ranges.iter_mut() {
        let obj = range
            .as_object_mut()
            .expect("a range serialises as an object");
        if obj.remove("staged").is_some() {
            removed += 1;
        }
    }
    assert_eq!(
        removed,
        ranges.len(),
        "fixture: every range must have had a `staged` key to remove. If this fails, \
         the field was renamed or given a serde skip attribute, and this test is no \
         longer constructing a pre-upgrade .meta"
    );

    let json = serde_json::to_string(&value).expect("edited value re-serialises");
    assert!(
        !json.contains("staged"),
        "fixture: the key must be ABSENT from the JSON, not present-and-null. serde \
         treats null as None as well, so a null would pass this test while leaving \
         the #[serde(default)] path unexercised: {json}"
    );
    serde_json::from_str(&json).expect(
        "a .meta with no `staged` key must still deserialise. If this fails, the field \
         lost its #[serde(default)] and EVERY existing .meta on the shared volume just \
         became unreadable",
    )
}

/// A `.meta` from an earlier release keeps today's answer, in both directions.
///
/// Two-sided deliberately. The flagged arm is the one a bare `bool` breaks — it
/// would report 0 where the current code reports the full staged size. The
/// unflagged arm is the one an inverted default would break, and it costs one
/// extra assertion to rule out.
#[test]
fn a_meta_written_before_the_staged_field_falls_back_to_the_object_flag() {
    const SIZE: u64 = 4096;

    for is_write_cached in [true, false] {
        let migrated =
            round_trip_without_staged_field(&metadata_with(RANGES_PATH, is_write_cached, SIZE));

        for range in &migrated.ranges {
            assert_eq!(
                range.staged, None,
                "a range whose JSON had no `staged` key must deserialise as None, which \
                 is what routes it to the object-flag fallback. `Some(false)` here means \
                 the field is a bare bool and the migration is broken"
            );
            assert_eq!(
                s3_proxy::cache_types::is_staged_range_spec(range, is_write_cached),
                classify_new_range_as_staged(&range.file_path, is_write_cached),
                "the per-range predicate must agree with the object-level one it \
                 replaces whenever membership was not recorded. Disagreeing here is a \
                 silent re-grounding of write_cache_size on the first post-upgrade scan"
            );
        }

        // The figure the Validation_Scan re-grounds from, which is the one that
        // actually reaches the shared Size_State and gets persisted.
        assert_eq!(
            migrated.staged_compressed_size(),
            if is_write_cached { SIZE } else { 0 },
            "staged_compressed_size() over a pre-upgrade .meta (is_write_cached={}) must \
             be unchanged by this migration",
            is_write_cached
        );
    }
}

/// The legacy `mpus_in_progress/` path arm survives the migration.
///
/// R12.3 keeps that arm, but moves it into the `None` fallback — a range with
/// recorded membership needs no path heuristic. This asserts it still fires for
/// the only case that reaches it in production: an unflagged object whose
/// persisted `file_path` sits outside `ranges/`.
#[test]
fn the_legacy_path_arm_still_fires_when_membership_was_not_recorded() {
    const SIZE: u64 = 1000;

    let migrated = round_trip_without_staged_field(&metadata_with(MPUS_PATH, false, SIZE));
    let range = &migrated.ranges[0];

    assert_eq!(range.staged, None, "fixture: membership must be unrecorded");
    assert!(
        s3_proxy::cache_types::is_staged_range_spec(range, false),
        "an mpus_in_progress/ range on an UNFLAGGED object must still classify as \
         staged when membership was not recorded. Dropping the path arm from the \
         fallback would stop this range being subtracted on eviction having been \
         added by a release that did count it (R6.2a, in the opposite direction)"
    );
    assert_eq!(migrated.staged_compressed_size(), SIZE);
}

/// A range that DID record its membership ignores the object flag entirely.
///
/// The other half of the predicate, and the half the fix turns on: without it,
/// `is_staged_range_spec` could read the recorded value and then OR the object
/// flag in anyway, which would pass every fallback test above and leave the
/// mixed-state defect completely unfixed.
#[test]
fn recorded_membership_overrides_the_object_flag_in_both_directions() {
    let staged_on_unflagged_object = RangeSpec::new_staged(
        0,
        4095,
        RANGES_PATH.to_string(),
        CompressionAlgorithm::Lz4,
        4096,
        4096,
        true,
    );
    assert!(
        s3_proxy::cache_types::is_staged_range_spec(&staged_on_unflagged_object, false),
        "recorded Some(true) must win over an object flag of false"
    );

    let unstaged_on_flagged_object = RangeSpec::new_staged(
        0,
        4095,
        RANGES_PATH.to_string(),
        CompressionAlgorithm::Lz4,
        4096,
        4096,
        false,
    );
    assert!(
        !s3_proxy::cache_types::is_staged_range_spec(&unstaged_on_flagged_object, true),
        "recorded Some(false) must win over an object flag of true. This is the \
         mixed-state case R12 exists for: a read-tier range attached to a still-flagged \
         object. If the predicate ORs the flag in, the defect survives the fix"
    );

    // And the path arm must NOT resurrect a range that recorded itself unstaged —
    // otherwise a persisted mpus_in_progress/ path would override a deliberate
    // Some(false), which is the same OR bug in the other arm.
    let unstaged_under_mpus_path = RangeSpec::new_staged(
        0,
        999,
        MPUS_PATH.to_string(),
        CompressionAlgorithm::Lz4,
        1000,
        1000,
        false,
    );
    assert!(
        !s3_proxy::cache_types::is_staged_range_spec(&unstaged_under_mpus_path, false),
        "the path arm belongs to the fallback only; recorded membership must not be \
         second-guessed by a heuristic"
    );
}
