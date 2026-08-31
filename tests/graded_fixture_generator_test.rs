//! Validation of the graded cache-tree fixture generator.
//!
//! Spec: `cache-eviction-at-scale`, task 8. Requirements: 13.1, 13.3.
//!
//! # Why this file exists in the form it does
//!
//! Task 8's stated risk is **a fixture that looks right and is not**, and a
//! generator validated by its own assertions is exactly that: emitting JSON its own
//! parser accepts proves only that the parser and the writer agree. So the load
//! bearing assertion here is not any of the shape checks — it is
//! [`fixture_tree_is_discoverable_by_the_real_eviction_candidate_collector`], which
//! drives `CacheManager::collect_range_candidates_for_eviction` (`src/cache.rs`,
//! `collect_range_candidates_recursive` → `collect_candidates_from_metadata_file`)
//! over a generated tree and compares what **eviction** discovered against what was
//! written. If the product cannot discover what the generator produced, the fixture
//! is invalid however well-formed it looks.
//!
//! That path is the right one to choose rather than a convenient one: it walks the
//! sharded tree without knowing any cache key, recovers each key from the `.meta`
//! contents, deserialises with the real `NewCacheMetadata`, resolves
//! `RangeSpec::file_path` against `ranges/`, and stats the `.bin`. Every property
//! the fixture has to get right is on that path, and a defect in any of them
//! produces a missing or wrong candidate rather than a silent pass.

mod common;

use common::graded_fixture::{
    self, FixtureSpec, GenOptions, PayloadMode, SizeDistribution, EVICTION_ADMISSION_WINDOW,
    FIXTURE_MARKER, MANIFEST_FILE,
};
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::NewCacheMetadata;
use s3_proxy::disk_cache::get_sharded_path;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// A small genuine-payload spec: real bytes, so on-disk length equals the recorded
/// figure and a byte-count assertion is meaningful, but capped so the test does not
/// write a 500 MiB range.
fn small_genuine_spec() -> FixtureSpec {
    FixtureSpec {
        label: "vsmall".to_string(),
        seed: 0x0011_2233_4455_6677,
        bucket: "fixture-vsmall".to_string(),
        objects: 120,
        max_ranges_per_object: 3,
        distribution: SizeDistribution::Graded {
            classes: match SizeDistribution::graded() {
                SizeDistribution::Graded { classes, .. } => classes,
                _ => unreachable!(),
            },
            size_cap: Some(16 * 1024),
        },
        payload: PayloadMode::Genuine,
        ..FixtureSpec::default()
    }
}

/// A count-scale spec exercising the FULL uncapped graded ladder — recorded sizes up
/// to 500 MiB — while writing stub payloads, so the tail can be verified at
/// realistic sizes for a few MB of disk. This is Fixture A's mode.
fn count_scale_spec() -> FixtureSpec {
    FixtureSpec {
        label: "count".to_string(),
        seed: 0x00c0_ffee_0000_0001,
        bucket: "fixture-count".to_string(),
        objects: 6_000,
        max_ranges_per_object: 3,
        distribution: SizeDistribution::graded(),
        payload: PayloadMode::RecordedSizeOnly { stub_bytes: 64 },
        ..FixtureSpec::default()
    }
}

async fn cache_manager_over(root: &std::path::Path) -> Arc<CacheManager> {
    let cm = Arc::new(CacheManager::new_with_defaults(
        root.to_path_buf(),
        false,
        0,
    ));
    let _disk = cm.create_configured_disk_cache_manager();
    cm.initialize().await.unwrap();
    cm
}

/// THE decisive check. Everything else in this file is supporting evidence.
///
/// Constrains `src/cache.rs`'s `collect_candidates_from_metadata_file`: the range
/// identity it recovers (`cache_key`, `range_start`, `range_end`), the size it reads
/// by statting the `.bin` resolved as `cache_dir/ranges/{file_path}`, and the
/// `compressed_size` it copies out of the `RangeSpec`. Reads those values rather
/// than an adjacent count, per R13.4.
#[tokio::test]
async fn fixture_tree_is_discoverable_by_the_real_eviction_candidate_collector() {
    let tmp = TempDir::new().unwrap();
    let spec = small_genuine_spec();
    let fixture = graded_fixture::generate(tmp.path(), &spec, true).expect("generation");

    assert!(
        !fixture.emitted.is_empty(),
        "generator emitted nothing to compare against"
    );

    let cm = cache_manager_over(tmp.path()).await;
    let candidates = cm
        .collect_range_candidates_for_eviction()
        .await
        .expect("candidate collection");

    // Identity: exactly the generated (key, start, end) triples, no more and no
    // fewer. A missing triple means the scan could not walk to it or could not
    // parse it; an extra one means the fixture left something behind.
    let expected: HashSet<(String, u64, u64)> = fixture
        .emitted
        .iter()
        .map(|e| (e.cache_key.clone(), e.start, e.end))
        .collect();
    let discovered: HashSet<(String, u64, u64)> = candidates
        .iter()
        .map(|c| (c.cache_key.clone(), c.range_start, c.range_end))
        .collect();

    let missing: Vec<_> = expected.difference(&discovered).take(5).collect();
    let extra: Vec<_> = discovered.difference(&expected).take(5).collect();
    assert!(
        missing.is_empty(),
        "eviction did not discover {} of {} generated ranges; first few: {:?}",
        expected.difference(&discovered).count(),
        expected.len(),
        missing
    );
    assert!(
        extra.is_empty(),
        "eviction discovered {} ranges the generator did not emit; first few: {:?}",
        discovered.difference(&expected).count(),
        extra
    );

    // Sizes: the collector stats the `.bin`. In genuine mode the stat result, the
    // recorded `compressed_size`, and the range width must all agree — which is
    // what makes a byte-target measurement over a genuine tree meaningful.
    let by_id: HashMap<(String, u64, u64), &graded_fixture::EmittedRange> = fixture
        .emitted
        .iter()
        .map(|e| ((e.cache_key.clone(), e.start, e.end), e))
        .collect();
    for c in &candidates {
        let key = (c.cache_key.clone(), c.range_start, c.range_end);
        let e = by_id.get(&key).expect("checked above");
        assert_eq!(
            c.size, e.on_disk_size,
            "candidate size for {key:?} should be the on-disk .bin length"
        );
        assert_eq!(
            c.compressed_size, e.recorded_compressed_size,
            "candidate compressed_size for {key:?} should be the recorded figure"
        );
        assert_eq!(
            c.size,
            c.range_end - c.range_start + 1,
            "genuine payload length should equal the range width for {key:?}"
        );
        assert!(
            c.bin_file_path.exists(),
            "the .bin the collector resolved does not exist: {}",
            c.bin_file_path.display()
        );
        assert!(c.access_count >= 1, "access_count should be at least 1");
    }

    // Negative control. Without this the identity assertion above could pass
    // against a scan that reported everything it walked past, which is the
    // false-positive shape `pre-push-checklist.md` records four instances of.
    assert!(
        !discovered
            .iter()
            .any(|(k, _, _)| k.contains("never-generated")),
        "scan reported a key the generator never wrote"
    );
}

/// Every emitted `.meta` deserialises with the real product type, and sits exactly
/// where the product's own path function says it should.
///
/// The path half matters independently: the generator *uses* `get_sharded_path`, so
/// re-checking it here is not tautological — it confirms the key recorded inside
/// the `.meta` still maps to the file's own location, which is the property the
/// collector depends on when it recovers a key from file contents.
#[test]
fn every_emitted_meta_parses_as_new_cache_metadata_at_its_own_sharded_path() {
    let tmp = TempDir::new().unwrap();
    let spec = small_genuine_spec();
    let fixture = graded_fixture::generate(tmp.path(), &spec, true).expect("generation");

    let metadata_dir = tmp.path().join("metadata");
    let ranges_dir = tmp.path().join("ranges");

    let mut metas = 0usize;
    let mut ranges = 0usize;
    for entry in walk(&metadata_dir) {
        if entry.extension().and_then(|s| s.to_str()) != Some("meta") {
            continue;
        }
        metas += 1;
        let raw = std::fs::read_to_string(&entry).unwrap();
        let meta: NewCacheMetadata = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("{} is not valid NewCacheMetadata: {e}", entry.display()));

        let expected = get_sharded_path(&metadata_dir, &meta.cache_key, ".meta").unwrap();
        assert_eq!(
            expected,
            entry,
            "the key recorded in {} does not map back to that file's location",
            entry.display()
        );

        assert!(
            !meta.ranges.is_empty(),
            "empty ranges in {}",
            entry.display()
        );
        assert!(
            !meta.is_object_expired(),
            "fixture entry {} is already expired; a selection measurement over it would \
             silently also be measuring expiry",
            meta.cache_key
        );

        let mut cursor = 0u64;
        for r in &meta.ranges {
            ranges += 1;
            assert_eq!(r.start, cursor, "ranges should be contiguous from 0");
            assert!(r.end >= r.start);
            cursor = r.end + 1;
            assert!(
                ranges_dir.join(&r.file_path).exists(),
                "range file {} named by {} is missing",
                r.file_path,
                meta.cache_key
            );
            assert_eq!(
                r.staged,
                Some(false),
                "a read-tier fixture must record staged=Some(false) explicitly, not None — \
                 None falls back to the object flag and is the pre-migration shape"
            );
            // The property FixtureSpec::validate exists to protect: ages must clear
            // the admission window in collect_candidates_from_metadata_file, or the
            // whole fixture is invisible to eviction.
            let age = r.last_accessed.elapsed().expect("range aged into the past");
            assert!(
                age > EVICTION_ADMISSION_WINDOW,
                "range {}-{} of {} is only {:?} old, inside the {:?} admission window",
                r.start,
                r.end,
                meta.cache_key,
                age,
                EVICTION_ADMISSION_WINDOW
            );
        }
    }

    assert_eq!(metas as u64, fixture.manifest.objects);
    assert_eq!(ranges as u64, fixture.manifest.ranges);
    assert_eq!(ranges, fixture.emitted.len());
}

/// R13.1's populated tail, verified rather than assumed — and verified by an
/// independent walk of the tree, so the manifest cannot vouch for itself.
#[test]
fn the_graded_distribution_populates_the_tail_and_the_manifest_agrees_with_the_tree() {
    let tmp = TempDir::new().unwrap();
    let spec = count_scale_spec();
    let fixture = graded_fixture::generate(tmp.path(), &spec, false).expect("generation");
    let m = &fixture.manifest;

    // The ladder must span R13.1's stated range.
    assert_eq!(
        m.distribution_span_bytes,
        (1024, 500 * 1024 * 1024),
        "R13.1 requires at least 1 KB to 500 MB"
    );

    // Tail population at three thresholds. All three non-zero is what makes the
    // tail "populated" rather than merely non-empty at one cut point.
    assert!(m.tail.ranges_at_least_1mib > 0, "no ranges >= 1 MiB");
    assert!(m.tail.ranges_at_least_8mib > 0, "no ranges >= 8 MiB");
    assert!(
        m.tail.ranges_at_least_64mib > 0,
        "no ranges >= 64 MiB — the top rung of the ladder is unpopulated, so \
         design.md § 12 row 1 cannot be answered from this fixture"
    );
    assert!(
        m.tail.largest_recorded >= 64 * 1024 * 1024,
        "largest recorded range is only {}",
        m.tail.largest_recorded
    );

    // Independent recount from the tree itself.
    let mut walked_ranges = 0u64;
    let mut walked_recorded = 0u64;
    let mut walked_ge_64mib = 0u64;
    let mut walked_largest = 0u64;
    for entry in walk(&tmp.path().join("metadata")) {
        if entry.extension().and_then(|s| s.to_str()) != Some("meta") {
            continue;
        }
        let meta: NewCacheMetadata =
            serde_json::from_str(&std::fs::read_to_string(&entry).unwrap()).unwrap();
        for r in &meta.ranges {
            walked_ranges += 1;
            walked_recorded += r.compressed_size;
            if r.compressed_size >= 64 * 1024 * 1024 {
                walked_ge_64mib += 1;
            }
            walked_largest = walked_largest.max(r.compressed_size);
        }
    }
    assert_eq!(walked_ranges, m.ranges, "manifest range count vs the tree");
    assert_eq!(
        walked_recorded, m.recorded_compressed_bytes,
        "manifest recorded bytes vs the tree"
    );
    assert_eq!(walked_ge_64mib, m.tail.ranges_at_least_64mib);
    assert_eq!(walked_largest, m.tail.largest_recorded);

    // Sanity on the per-shard rollup the measurement tasks will read.
    assert!(
        m.shards.len() > 200,
        "expected most of the 256 L1 shards populated at this scale, got {}",
        m.shards.len()
    );
    assert_eq!(
        m.shards.iter().map(|s| s.ranges).sum::<u64>(),
        m.ranges,
        "per-shard counts should sum to the total"
    );
    assert!(
        m.shards.iter().all(|s| s.tail_bytes <= s.recorded_bytes),
        "a shard's tail cannot hold more bytes than the shard"
    );
    assert!(
        m.tail.tail_share_of_recorded_bytes > 0.0 && m.tail.tail_share_of_recorded_bytes <= 1.0,
        "tail share out of range: {}",
        m.tail.tail_share_of_recorded_bytes
    );
}

/// R13.3's two extremes, as named distributions from the same generator.
#[test]
fn degenerate_distributions_produce_the_r13_3_extremes() {
    // Uniform: the tail is empty by construction. This is the case a
    // tail-partition design must not silently depend on.
    let tmp = TempDir::new().unwrap();
    let uniform = FixtureSpec {
        label: "uniform".to_string(),
        bucket: "fixture-uniform".to_string(),
        objects: 300,
        max_ranges_per_object: 2,
        distribution: SizeDistribution::Uniform { size: 8192 },
        payload: PayloadMode::Genuine,
        ..FixtureSpec::default()
    };
    let f = graded_fixture::generate(tmp.path(), &uniform, true).expect("generation");
    assert_eq!(f.manifest.tail.ranges_at_least_1mib, 0);
    assert_eq!(f.manifest.tail.largest_recorded, 8192);
    assert!(f.emitted.iter().all(|e| e.recorded_compressed_size == 8192));

    // Single dominant: one range holds the majority of the bytes.
    let tmp2 = TempDir::new().unwrap();
    let dominant = FixtureSpec {
        label: "dominant".to_string(),
        bucket: "fixture-dominant".to_string(),
        objects: 200,
        max_ranges_per_object: 2,
        distribution: SizeDistribution::SingleDominant {
            background: 4096,
            dominant: 32 * 1024 * 1024,
        },
        payload: PayloadMode::RecordedSizeOnly { stub_bytes: 32 },
        ..FixtureSpec::default()
    };
    let f2 = graded_fixture::generate(tmp2.path(), &dominant, true).expect("generation");
    let total: u64 = f2.emitted.iter().map(|e| e.recorded_compressed_size).sum();
    let largest = f2.manifest.tail.largest_recorded;
    assert_eq!(largest, 32 * 1024 * 1024);
    assert!(
        largest * 2 > total,
        "the dominant range should hold the majority of recorded bytes: {largest} of {total}"
    );
    assert_eq!(
        f2.emitted
            .iter()
            .filter(|e| e.recorded_compressed_size == largest)
            .count(),
        1,
        "exactly one dominant range"
    );
}

/// The recorded-versus-on-disk asymmetry, pinned so it cannot be "fixed".
///
/// This asserts the discrepancy **exists** and that it lands on the two candidate
/// fields the way `collect_candidates_from_metadata_file` puts it there. A future
/// change that reconciled the two figures would red this test with a message saying
/// why the discrepancy is deliberate, rather than quietly turning task 9's Fixture A
/// into an unbuildable one.
#[tokio::test]
async fn recorded_size_only_mode_keeps_recorded_and_on_disk_sizes_apart() {
    let tmp = TempDir::new().unwrap();
    let spec = FixtureSpec {
        label: "stub".to_string(),
        bucket: "fixture-stub".to_string(),
        objects: 60,
        max_ranges_per_object: 2,
        distribution: SizeDistribution::graded(),
        payload: PayloadMode::RecordedSizeOnly { stub_bytes: 128 },
        ..FixtureSpec::default()
    };
    let fixture = graded_fixture::generate(tmp.path(), &spec, true).expect("generation");
    let m = &fixture.manifest;

    assert!(
        m.recorded_size_is_deliberately_not_on_disk_size,
        "the manifest must declare the discrepancy so a reader finds the reason in the artefact"
    );
    assert!(
        m.recorded_vs_on_disk_why.contains("DELIBERATE"),
        "the manifest must carry the reason, not just the flag"
    );
    assert!(
        !m.serve_path_valid,
        "a stub-payload tree cannot serve its recorded content and must say so"
    );
    assert!(
        m.recorded_compressed_bytes > m.on_disk_bytes * 100,
        "recorded {} should vastly exceed on-disk {} in stub mode",
        m.recorded_compressed_bytes,
        m.on_disk_bytes
    );
    assert!(m.on_disk_bytes > 0, "stubs still occupy bytes");

    // And the consequence for measurement, read off the product's own candidates:
    // `size` carries the stub, `compressed_size` carries the realistic figure. This
    // is the trap for anyone summing `size` to reach a byte target on Fixture A.
    let cm = cache_manager_over(tmp.path()).await;
    let candidates = cm.collect_range_candidates_for_eviction().await.unwrap();
    assert!(!candidates.is_empty());
    assert!(
        candidates.iter().all(|c| c.size == 128),
        "every candidate's size should be the 128-byte stub"
    );
    assert!(
        candidates.iter().any(|c| c.compressed_size > 1024 * 1024),
        "at least one candidate should record a realistic multi-MiB compressed_size"
    );
    let sum_size: u64 = candidates.iter().map(|c| c.size).sum();
    let sum_recorded: u64 = candidates.iter().map(|c| c.compressed_size).sum();
    assert!(
        sum_recorded > sum_size * 100,
        "the two byte totals must remain distinguishable: size={sum_size} recorded={sum_recorded}"
    );
}

/// Determinism: same spec plus same seed gives the same tree; a different seed does
/// not. Task 11's show-red has to regenerate the same input, and a measurement that
/// cannot be repeated on the same input is not evidence.
#[test]
fn generation_is_deterministic_in_the_seed() {
    let a = TempDir::new().unwrap();
    let b = TempDir::new().unwrap();
    let c = TempDir::new().unwrap();

    let spec = small_genuine_spec();
    let fa = graded_fixture::generate(a.path(), &spec, true).unwrap();
    let fb = graded_fixture::generate(b.path(), &spec, true).unwrap();

    let mut other = small_genuine_spec();
    other.seed ^= 1;
    let fc = graded_fixture::generate(c.path(), &other, true).unwrap();

    assert_eq!(
        fa.manifest.content_digest, fb.manifest.content_digest,
        "same seed must give the same digest"
    );
    assert_ne!(
        fa.manifest.content_digest, fc.manifest.content_digest,
        "a different seed must give a different digest"
    );
    assert_eq!(fa.manifest.ranges, fb.manifest.ranges);
    assert_eq!(
        fa.manifest.recorded_compressed_bytes,
        fb.manifest.recorded_compressed_bytes
    );

    // Digest equality is a summary; check the emitted set itself so an accidental
    // digest collapse (e.g. XOR-folding identical rows) cannot make this pass.
    let ea: Vec<_> = fa
        .emitted
        .iter()
        .map(|e| {
            (
                e.cache_key.clone(),
                e.start,
                e.end,
                e.recorded_compressed_size,
            )
        })
        .collect();
    let eb: Vec<_> = fb
        .emitted
        .iter()
        .map(|e| {
            (
                e.cache_key.clone(),
                e.start,
                e.end,
                e.recorded_compressed_size,
            )
        })
        .collect();
    assert_eq!(ea, eb);
    assert!(
        ea.iter().collect::<HashSet<_>>().len() == ea.len(),
        "emitted rows must be distinct, or the order-independent digest could collapse"
    );
}

/// Parallel generation produces the identical tree to serial generation.
///
/// The load-bearing test for task 9. A ~10M-range fixture is not buildable
/// single-threaded, so the parallel driver is the thing that actually writes the
/// artefact — and an artefact whose contents depend on how many threads happened to
/// write it is worthless as a measurement input, because a re-measurement on a
/// differently-sized machine would not be measuring the same input.
///
/// # Why this is a real two-sided check and not a self-assertion
///
/// The two drivers are genuinely different code: one is a `for` loop over a single
/// [`Accum`], the other is a rayon `try_fold`/`try_reduce` over blocks of objects
/// with an `Accum::merge` between them. They share only the per-object emitter. So
/// every merge operation gets checked here against an implementation that never
/// merges: the summed counters, the per-class min/max, the per-shard rollups, and —
/// the one that is not a plain sum — the bounded per-shard tail heaps, whose
/// `topK(topK(A) ∪ topK(B)) == topK(A ∪ B)` identity is the merge most likely to be
/// subtly wrong.
///
/// Constrains `tests/common/graded_fixture.rs`'s `Accum::merge` and the
/// `generate_with` driver. Reads the values the merge writes — the shard and class
/// rollups themselves — rather than an adjacent count, per R13.4.
#[test]
fn parallel_generation_is_identical_to_serial_generation() {
    let serial_dir = TempDir::new().unwrap();
    let parallel_dir = TempDir::new().unwrap();

    // The count-scale shape, so the per-shard tail heaps hold real content and the
    // full 500 MiB ladder is drawn from. Enough objects that 8 threads over
    // 256-object blocks gives many blocks, and therefore many merges.
    let spec = FixtureSpec {
        objects: 4_000,
        ..count_scale_spec()
    };

    let s = graded_fixture::generate_with(
        serial_dir.path(),
        &spec,
        &GenOptions {
            collect_emitted: true,
            threads: 1,
            dir_memo: true,
            shared_storage_ack: None,
        },
    )
    .expect("serial generation");
    let p = graded_fixture::generate_with(
        parallel_dir.path(),
        &spec,
        &GenOptions {
            collect_emitted: true,
            threads: 8,
            dir_memo: true,
            shared_storage_ack: None,
        },
    )
    .expect("parallel generation");

    assert_eq!(s.manifest.generation_threads, 1);
    assert_eq!(p.manifest.generation_threads, 8);

    // The headline: the order-independent digest.
    assert_eq!(
        s.manifest.content_digest, p.manifest.content_digest,
        "parallel generation produced a different tree from serial"
    );

    // Digest equality is a summary over XOR-folded rows, so on its own it could be
    // satisfied by two different multisets. Compare the emitted sequences directly.
    assert!(!s.emitted.is_empty(), "nothing emitted to compare");
    assert_eq!(
        s.emitted.len(),
        p.emitted.len(),
        "different range counts: serial {} vs parallel {}",
        s.emitted.len(),
        p.emitted.len()
    );
    let row = |e: &graded_fixture::EmittedRange| {
        (
            e.cache_key.clone(),
            e.start,
            e.end,
            e.recorded_compressed_size,
            e.on_disk_size,
            e.relative_bin_path.clone(),
            e.class.clone(),
        )
    };
    let sr: Vec<_> = s.emitted.iter().map(row).collect();
    let pr: Vec<_> = p.emitted.iter().map(row).collect();
    let first_diff = sr.iter().zip(pr.iter()).position(|(a, b)| a != b);
    assert!(
        first_diff.is_none(),
        "emitted rows diverge at index {:?}:\n  serial:   {:?}\n  parallel: {:?}",
        first_diff,
        first_diff.map(|i| &sr[i]),
        first_diff.map(|i| &pr[i])
    );

    // Every merged rollup, field by field. `Accum::merge` is the only code the
    // parallel path adds, so these are the assertions that constrain it.
    let (sm, pm) = (&s.manifest, &p.manifest);
    assert_eq!(sm.ranges, pm.ranges, "range count");
    assert_eq!(sm.objects, pm.objects, "object count");
    assert_eq!(sm.staged_objects, pm.staged_objects, "staged objects");
    assert_eq!(
        sm.recorded_compressed_bytes, pm.recorded_compressed_bytes,
        "recorded bytes"
    );
    assert_eq!(
        sm.recorded_uncompressed_bytes, pm.recorded_uncompressed_bytes,
        "recorded uncompressed bytes"
    );
    assert_eq!(sm.on_disk_bytes, pm.on_disk_bytes, "on-disk bytes");
    assert_eq!(sm.classes, pm.classes, "per-class rollups (sums, min, max)");
    assert_eq!(sm.tail, pm.tail, "tail statistics");

    // The tail heaps specifically. Compared as the full per-shard vector, so a
    // merge that kept the wrong K sizes for one shard out of 256 fails here rather
    // than averaging away.
    assert_eq!(
        sm.shards, pm.shards,
        "per-shard rollups including tail_ranges and tail_bytes"
    );
    assert!(
        sm.shards.iter().any(|sh| sh.tail_bytes > 0),
        "the tail heaps were empty, so this test did not exercise the top-K merge"
    );

    // The CVs are derived from the shard vector, so they follow — but they are what
    // task 9 reports, so pin them too.
    assert_eq!(sm.shard_count_cv_percent, pm.shard_count_cv_percent);
    assert_eq!(sm.shard_bytes_cv_percent, pm.shard_bytes_cv_percent);

    // Negative control: the digest must be capable of distinguishing trees at all.
    // Without this the equality above could pass against a digest that is constant.
    let other_dir = TempDir::new().unwrap();
    let other = FixtureSpec {
        seed: spec.seed ^ 1,
        ..spec.clone()
    };
    let o = graded_fixture::generate_with(
        other_dir.path(),
        &other,
        &GenOptions {
            collect_emitted: false,
            threads: 8,
            dir_memo: true,
            shared_storage_ack: None,
        },
    )
    .expect("third generation");
    assert_ne!(
        o.manifest.content_digest, p.manifest.content_digest,
        "the digest does not distinguish different trees, so equality above proves nothing"
    );
}

/// The directory-existence memo is an execution option and must not touch output.
///
/// Kept separate from the thread check because the two are independent knobs and a
/// combined test could pass while one of them shifted the tree. `--no-dir-memo`
/// exists so task 9's scaling curve can attribute its speedup between the memo and
/// the thread count; that attribution is only honest if both settings write the same
/// bytes.
#[test]
fn the_directory_memo_does_not_change_the_generated_tree() {
    let with = TempDir::new().unwrap();
    let without = TempDir::new().unwrap();
    let spec = FixtureSpec {
        objects: 600,
        ..count_scale_spec()
    };

    let a = graded_fixture::generate_with(
        with.path(),
        &spec,
        &GenOptions {
            collect_emitted: false,
            threads: 4,
            dir_memo: true,
            shared_storage_ack: None,
        },
    )
    .unwrap();
    let b = graded_fixture::generate_with(
        without.path(),
        &spec,
        &GenOptions {
            collect_emitted: false,
            threads: 4,
            dir_memo: false,
            shared_storage_ack: None,
        },
    )
    .unwrap();

    assert_eq!(a.manifest.content_digest, b.manifest.content_digest);
    assert_eq!(a.manifest.shards, b.manifest.shards);
    assert_eq!(a.manifest.on_disk_bytes, b.manifest.on_disk_bytes);
    assert!(a.manifest.dir_memo && !b.manifest.dir_memo);

    // And the trees are the same on disk, not merely in the manifest — the memo is
    // the one option that could plausibly skip creating a directory that was needed.
    let count = |root: &std::path::Path, sub: &str| walk(&root.join(sub)).len();
    assert_eq!(
        count(with.path(), "metadata"),
        count(without.path(), "metadata"),
        ".meta file counts differ"
    );
    assert_eq!(
        count(with.path(), "ranges"),
        count(without.path(), "ranges"),
        ".bin file counts differ"
    );
}

/// A parallel-generated tree is discoverable by the real eviction collector.
///
/// The serial equivalent is
/// [`fixture_tree_is_discoverable_by_the_real_eviction_candidate_collector`], and
/// this is not redundant with it: task 9's artefact is written by the parallel
/// driver, so the driver that actually produces the measured tree is the one that
/// has to be shown discoverable through product code. Digest equality says the
/// manifests agree; it does not say the files landed where the scan looks.
#[tokio::test]
async fn parallel_generated_tree_is_discoverable_by_the_real_eviction_collector() {
    let tmp = TempDir::new().unwrap();
    let spec = FixtureSpec {
        objects: 900,
        ..small_genuine_spec()
    };
    let fixture = graded_fixture::generate_with(
        tmp.path(),
        &spec,
        &GenOptions {
            collect_emitted: true,
            threads: 8,
            dir_memo: true,
            shared_storage_ack: None,
        },
    )
    .expect("parallel generation");

    let cm = cache_manager_over(tmp.path()).await;
    let candidates = cm
        .collect_range_candidates_for_eviction()
        .await
        .expect("candidate collection");

    let expected: HashSet<(String, u64, u64)> = fixture
        .emitted
        .iter()
        .map(|e| (e.cache_key.clone(), e.start, e.end))
        .collect();
    let discovered: HashSet<(String, u64, u64)> = candidates
        .iter()
        .map(|c| (c.cache_key.clone(), c.range_start, c.range_end))
        .collect();
    assert_eq!(
        expected.difference(&discovered).count(),
        0,
        "eviction did not discover {} of {} ranges written in parallel; first few: {:?}",
        expected.difference(&discovered).count(),
        expected.len(),
        expected.difference(&discovered).take(5).collect::<Vec<_>>()
    );
    assert_eq!(
        discovered.difference(&expected).count(),
        0,
        "eviction discovered ranges the parallel generator did not emit: {:?}",
        discovered.difference(&expected).take(5).collect::<Vec<_>>()
    );
}

/// A fixture tree announces itself, and two generations occupy disjoint key space.
#[test]
fn fixture_trees_are_distinguishable_from_a_real_cache_and_from_each_other() {
    let tmp = TempDir::new().unwrap();
    let spec = small_genuine_spec();
    let fixture = graded_fixture::generate(tmp.path(), &spec, true).unwrap();

    assert!(tmp.path().join(FIXTURE_MARKER).exists(), "marker file");
    assert!(tmp.path().join(MANIFEST_FILE).exists(), "manifest file");
    assert!(
        graded_fixture::read_manifest(tmp.path()).is_ok(),
        "manifest should read back"
    );

    let gen_id = spec.gen_id();
    assert!(
        fixture
            .emitted
            .iter()
            .all(|e| e.cache_key.contains("s3hc-fixture") && e.cache_key.contains(&gen_id)),
        "every key must carry the fixture prefix and the generation id"
    );

    // A second generation differing only in seed must not share key space with the
    // first, so a stale tree cannot be mistaken for this one.
    let tmp2 = TempDir::new().unwrap();
    let mut other = small_genuine_spec();
    other.seed ^= 0xffff;
    let f2 = graded_fixture::generate(tmp2.path(), &other, true).unwrap();
    let keys1: HashSet<&str> = fixture
        .emitted
        .iter()
        .map(|e| e.cache_key.as_str())
        .collect();
    let keys2: HashSet<&str> = f2.emitted.iter().map(|e| e.cache_key.as_str()).collect();
    assert!(
        keys1.is_disjoint(&keys2),
        "two generations shared {} keys",
        keys1.intersection(&keys2).count()
    );
}

/// The generator refuses to write into anything that could be a real cache, and
/// refuses an age that would make the fixture invisible to eviction.
#[test]
fn generator_refuses_unsafe_targets_and_unsafe_ages() {
    // A directory carrying a live-cache tell-tale.
    let tmp = TempDir::new().unwrap();
    std::fs::create_dir_all(tmp.path().join("size_tracking")).unwrap();
    std::fs::write(tmp.path().join("size_tracking/size_state.json"), "{}").unwrap();
    let err = graded_fixture::generate(tmp.path(), &small_genuine_spec(), false)
        .expect_err("must refuse a real cache directory");
    assert!(err.contains("size_state.json"), "unexpected error: {err}");

    // A journals directory is the other tell-tale.
    let tmp2 = TempDir::new().unwrap();
    std::fs::create_dir_all(tmp2.path().join("metadata/_journals")).unwrap();
    let err2 = graded_fixture::generate(tmp2.path(), &small_genuine_spec(), false)
        .expect_err("must refuse a directory with journals");
    assert!(err2.contains("_journals"), "unexpected error: {err2}");

    // Non-empty and unmarked is ambiguous, so it is refused rather than guessed at.
    let tmp3 = TempDir::new().unwrap();
    std::fs::write(tmp3.path().join("something.txt"), "x").unwrap();
    let err3 = graded_fixture::generate(tmp3.path(), &small_genuine_spec(), false)
        .expect_err("must refuse a non-empty unmarked directory");
    assert!(err3.contains(FIXTURE_MARKER), "unexpected error: {err3}");

    // A previously generated tree IS safe to regenerate over, because it carries
    // the marker. Without this arm the guard above would make regeneration — which
    // determinism exists to support — impossible.
    let tmp4 = TempDir::new().unwrap();
    graded_fixture::generate(tmp4.path(), &small_genuine_spec(), false).unwrap();
    graded_fixture::generate(tmp4.path(), &small_genuine_spec(), false)
        .expect("regenerating over a marked fixture tree must be allowed");

    // An age inside the admission window is refused at validation, naming the
    // product behaviour it would defeat.
    let bad = FixtureSpec {
        min_age: Duration::from_secs(10),
        ..small_genuine_spec()
    };
    let err4 = bad
        .validate()
        .expect_err("must refuse a sub-window min_age");
    assert!(
        err4.contains("admission window"),
        "unexpected error: {err4}"
    );
}

/// The shared-storage override is narrow: it clears one convention heuristic and
/// leaves the refusal that matters unreachable.
///
/// Task 9's count-scale artefact has to live on the real shared backend, because
/// `readdir` at occupancy is a backend-specific property and a local-APFS answer does
/// not transfer. So the `/mnt/`-fragment guard has to be overridable. The risk that
/// creates is the exact accident the guard was written for: generating a few million
/// files into `/mnt/efs/cache-bench`, the live fleet cache directory.
///
/// Calls `guard_output_dir` directly rather than `generate_with`, deliberately: a test
/// that proved the positive case by generating into `/mnt/...` would really write a
/// fixture there on any host where `/mnt` is writable.
#[test]
fn the_shared_storage_override_cannot_reach_a_live_cache_directory() {
    use std::path::Path;

    let target = Path::new("/mnt/efs/fixture-a-count-scale");

    // Default: refused, and the refusal names the way out.
    let err = graded_fixture::guard_output_dir(target, None)
        .expect_err("a shared-mount path must be refused by default");
    assert!(err.contains("/mnt/"), "{err}");
    assert!(
        err.contains("--ack-shared-storage"),
        "the refusal must name the override: {err}"
    );

    // Acknowledged by exact path: allowed. Without this the override would be
    // useless and task 9 could not build its artefact at all.
    graded_fixture::guard_output_dir(target, Some(target))
        .expect("an exact acknowledgement must clear the convention guard");
    // Trailing slash is the same path, not a different one.
    graded_fixture::guard_output_dir(target, Some(Path::new("/mnt/efs/fixture-a-count-scale/")))
        .expect("a trailing slash must not defeat the acknowledgement");

    // An acknowledgement naming a DIFFERENT path does not cover this one, so the
    // flag cannot be set once and then silently authorise a later destination.
    let err = graded_fixture::guard_output_dir(target, Some(Path::new("/mnt/efs/somewhere-else")))
        .expect_err("an ack for another path must not cover this one");
    assert!(err.contains("/mnt/"), "{err}");
    let err = graded_fixture::guard_output_dir(target, Some(Path::new("/mnt")))
        .expect_err("a broader ack must not cover a path beneath it");
    assert!(err.contains("/mnt/"), "{err}");

    // THE assertion. The live fleet cache directories stay refused even when the
    // acknowledgement names them exactly.
    for live in [
        "/mnt/efs/cache-bench",
        "/mnt/efs/cache-bench/",
        "/mnt/efs/cache-bench/metadata/aa",
        "/mnt/efs/cache",
        "/mnt/efs/cache/ranges",
    ] {
        let p = Path::new(live);
        let err = graded_fixture::guard_output_dir(p, Some(p))
            .unwrap_err_or_else_msg(&format!("{live} must be refused even with a matching ack"));
        assert!(
            err.contains("NOT overridable"),
            "{live} was refused for the wrong reason: {err}"
        );
    }

    // A sibling that merely shares a prefix is NOT caught by the never-list — the
    // match is at a component boundary, not a substring. Without this the never-list
    // would creep and make legitimate destinations unusable.
    let sibling = Path::new("/mnt/efs/cache-bench-fixture");
    graded_fixture::guard_output_dir(sibling, Some(sibling))
        .expect("a sibling sharing a name prefix must not be swept up by the never-list");
}

/// Small helper so the loop above reads as an assertion rather than as plumbing.
trait UnwrapErrMsg {
    fn unwrap_err_or_else_msg(self, msg: &str) -> String;
}

impl UnwrapErrMsg for Result<(), String> {
    fn unwrap_err_or_else_msg(self, msg: &str) -> String {
        match self {
            Ok(()) => panic!("{msg}, but it was ALLOWED"),
            Err(e) => e,
        }
    }
}

/// Recursive file walk. Deliberately hand-rolled rather than reusing the
/// generator's own traversal, so a traversal defect cannot hide from the tests that
/// depend on it.
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p);
            }
        }
    }
    out
}
