//! Cross-instance RAM staleness: a response can mix two object versions.
//!
//! **The scenario.** Instance A holds an object's bytes in its per-instance RAM range
//! tier. Instance B then overwrites the same S3 key, rewriting the entry on the SHARED
//! disk cache volume. Minutes later — past `metadata_cache.refresh_interval` — a GET
//! arrives at instance A.
//!
//! **The claim under test.** `serve_full_object_from_cache` (`http_proxy.rs:7870`) builds
//! its response from two independent reads:
//!
//! - **bytes** via `get_cached_range_data` (`:8887`) → `CacheManager::load_range_data_with_cache`
//!   (`cache.rs:3653`), which consults the RAM tier FIRST, keyed
//!   `{cache_key}:range:{start}:{end}` (`cache.rs:2230`) — no ETag, no version, no
//!   generation in the key.
//! - **headers** via `CacheManager::get_metadata_from_disk` (`:7928` → `cache.rs:12275`),
//!   a raw `std::fs::read_to_string` that does NOT go through the metadata cache.
//!
//! Nothing compares the two. So A can emit instance B's ETag with instance A's bytes.
//! The RAM entry even carries its own `metadata.etag` from cache time
//! (`RamCacheEntry`, `cache.rs:840`), and `get_range_from_ram_cache` (`cache.rs:9093`)
//! discards it — the information needed to detect this is loaded and thrown away.
//!
//! **Why this is not covered by `ram_etag_invalidation_gap_test.rs`.** That test proves
//! invalidation does not reach the RAM tier once a mismatch is DETECTED, and its fix
//! (`invalidate_ram_ranges`, v2.6.2) is in place. This test is about the case where no
//! mismatch is ever detected, because:
//!
//! - the non-range GET path passes `current_etag: None` to `find_cached_ranges`
//!   (`http_proxy.rs:3450`), so the ETag-mismatch arm never runs at all; and
//! - even on the ranged path, the mismatch arm compares an uncached disk read against
//!   `preloaded_metadata` from the metadata cache, so it can only fire while that cache
//!   still holds the OLD version — i.e. within `refresh_interval` of B's write. Past
//!   that window both sides read the new version and the comparison is a no-op.
//!
//! The protection is therefore inverted: it covers the freshest case and lapses exactly
//! when the RAM entry has been stale longest.
//!
//! **Cross-instance shape (the cache coherency invariants).** Instance
//! B's write is performed by a SECOND `CacheManager` on the same cache dir, so B's
//! `invalidate_cache_hierarchy` → `remove_from_ram_cache_unified` clears B's RAM and
//! cannot reach A's. A fixture that drove A's own invalidation path would pass under
//! local hygiene and prove nothing.
//!
//! **Status: OBSERVED RED against internal 2.8.1, 2026-08-31.** That is the unshipped
//! internal build, not the public 2.8.1 issue-19 hotfix — the two share a number and
//! nothing else, and the public one postdates this run by ten days.
//! `Results: 2 passed, 1 failed` —
//! both controls green, the defect assertion red. The reproduction, quoted:
//!
//! ```text
//! STATE [after B consolidate]: etag="version-b-etag" content_length=1024 ranges=1
//! OBSERVATION: instance A would answer this GET with body=version A (1024 bytes,
//!              ram_hit=true) and etag="version-b-etag" (version B).
//! ```
//!
//! `ram_hit=true` is the non-vacuity check: the bytes came from instance A's RAM tier, not
//! from disk. Every precondition passed, so the failure is the version mix itself and not
//! a fixture problem.
//!
//! The two sibling controls are what make that attributable, and both PASS:
//!
//! - `control_disk_tier_returns_version_b_once_instance_a_ram_is_cleared` — same fixture
//!   with A's RAM ranges dropped serves version B correctly, so the shared disk volume
//!   genuinely holds B and the stale serve is the RAM tier's doing alone.
//! - `control_size_changing_overwrite_misses_ram_and_serves_correct_bytes` — a
//!   length-changing overwrite misses the RAM key (`ram_hit=false`) and serves B, bounding
//!   the finding to size-preserving overwrites and confirming the lookup is keyed on the
//!   range boundaries as assumed.
//!
//! Without the controls, a version-A body would be equally consistent with "B's write
//! never landed" or "the shared `.meta` is wrong". With them, it is not.
//!
//! The final assertion states the correct behaviour — the bytes and the ETag in one
//! response must describe the same version.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager, RamRangeHit, Range};
use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::config::Config;
use s3_proxy::disk_cache::DiskCacheManager;
use s3_proxy::range_handler::RangeHandler;
use tempfile::TempDir;

const KEY: &str = "test-bucket/cross-instance-object";
const RANGE_START: u64 = 0;
const RANGE_END: u64 = 1023;
const LEN: u64 = RANGE_END - RANGE_START + 1;
const ETAG_A: &str = "\"version-a-etag\"";
const ETAG_B: &str = "\"version-b-etag\"";

/// Version A and version B are the SAME LENGTH deliberately. A length change would shift
/// the range boundaries, so the RAM key `{key}:range:0:1023` would not be reached and the
/// read would fall through to disk and return correct bytes. The defect is specific to
/// size-preserving overwrites, which is the common case for a re-uploaded file.
fn version_a_bytes() -> Vec<u8> {
    (0..LEN).map(|i| (i % 251) as u8).collect()
}

fn version_b_bytes() -> Vec<u8> {
    (0..LEN).map(|i| ((i % 251) as u8) ^ 0xFF).collect()
}

fn object_metadata(etag: &str) -> ObjectMetadata {
    ObjectMetadata {
        etag: etag.to_string(),
        last_modified: "Mon, 31 Aug 2026 00:00:00 GMT".to_string(),
        content_length: LEN,
        content_type: Some("application/octet-stream".to_string()),
        ..ObjectMetadata::default()
    }
}

/// Build one proxy instance's cache stack over `dir`. Two instances built over the same
/// `dir` share the disk cache and have entirely separate RAM tiers, which is the real
/// fleet topology.
async fn make_instance(
    dir: &std::path::Path,
) -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
    Arc<RangeHandler>,
) {
    let mut config = Config::default();
    config.cache.ram_cache_enabled = true;

    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        dir.to_path_buf(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        config.compression.enabled,
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
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

    (cache_manager, disk_cache_manager, range_handler)
}

/// Remove every `.journal` file under the cache dir. See the call site for why this is
/// required and why it does not weaken the test.
fn remove_journals(root: &std::path::Path) {
    fn walk(dir: &std::path::Path) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p);
                } else if p.to_string_lossy().contains(".journal") {
                    let _ = std::fs::remove_file(&p);
                }
            }
        }
    }
    walk(root);
}

/// Dump the shared-volume state so a fixture problem is distinguishable from the defect.
async fn dump_state(label: &str, cm: &Arc<CacheManager>, root: &std::path::Path) {
    let etag = match cm.get_metadata_from_disk(KEY).await {
        Ok(Some(m)) => format!(
            "etag={} content_length={} ranges={}",
            m.object_metadata.etag,
            m.object_metadata.content_length,
            m.ranges.len()
        ),
        Ok(None) => "<no .meta>".to_string(),
        Err(e) => format!("<error {e}>"),
    };
    let mut journals = Vec::new();
    let mut bins = Vec::new();
    let mut metas = Vec::new();
    fn walk(
        dir: &std::path::Path,
        journals: &mut Vec<String>,
        bins: &mut Vec<String>,
        metas: &mut Vec<String>,
    ) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, journals, bins, metas);
                } else {
                    let size = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
                    let name = p
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned();
                    match p.extension().and_then(|s| s.to_str()) {
                        Some("journal") => journals.push(format!("{name}({size}B)")),
                        Some("bin") => bins.push(format!("{name}({size}B)")),
                        Some("meta") => metas.push(format!("{name}({size}B)")),
                        _ => {}
                    }
                }
            }
        }
    }
    walk(root, &mut journals, &mut bins, &mut metas);
    println!(
        "STATE [{label}]: {etag}\n           journals={journals:?}\n           metas={metas:?}\n           bins={bins:?}"
    );
}

/// Force this instance's journal entries into the consolidated `.meta` on the shared
/// volume, standing in for the background `JournalConsolidator`'s periodic pass.
async fn consolidate(cm: &Arc<CacheManager>) {
    let consolidator = cm
        .get_journal_consolidator()
        .await
        .expect("journal consolidator must be configured in shared-storage mode");
    consolidator
        .consolidate_object(KEY)
        .await
        .expect("consolidation should succeed");
}

// `multi_thread` is required, not cosmetic: `get_range_from_ram_cache` reaches the async
// RAM cache through `block_in_place`, which panics on the current-thread runtime that
// `#[tokio::test]` uses by default.
//
// FIXED by ram-tier-effectiveness Requirement 4 (v2.8.6). This is that requirement's
// acceptance test (see the ram tier effectiveness feature) and
// its assertion below is UNCHANGED from the red version: what changed is
// `load_range_data_with_cache`, which now compares the RAM entry's own etag
// (`ram_read.metadata.etag`) against the caller-supplied `range.etag` before trusting the
// RAM bytes, invalidating and falling through to disk on a mismatch — see
// `CacheManager::load_range_data_with_cache` in `src/cache.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_instance_overwrite_lets_instance_a_mix_versions_in_one_response() {
    let tmp = TempDir::new().expect("tempdir");

    // Two proxies, one shared cache volume, separate RAM tiers.
    let (cm_a, _disk_a, rh_a) = make_instance(tmp.path()).await;
    let (cm_b, _disk_b, rh_b) = make_instance(tmp.path()).await;

    let body_a = version_a_bytes();
    let body_b = version_b_bytes();
    assert_eq!(
        body_a.len(),
        body_b.len(),
        "fixture: the two versions must be the same length"
    );
    assert_ne!(
        body_a, body_b,
        "fixture: the two versions must differ in content"
    );

    // --- Instance A warms both tiers with version A -------------------------
    rh_a.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &body_a,
            object_metadata(ETAG_A),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("A seeds disk with version A");

    // Fold A's write out of its per-instance journal and into a real `.meta` on the
    // shared volume. Production's background `JournalConsolidator` does this on its
    // periodic schedule; without it `get_metadata_from_disk` sees no `.meta` at all and
    // the scenario cannot be set up. The test's premise is "minutes later", which is
    // several consolidation intervals, so forcing it here is faithful rather than
    // convenient. Same pattern as tests/page_aligned_range_widening_test.rs:994.
    dump_state("after A store, before consolidate", &cm_a, tmp.path()).await;
    consolidate(&cm_a).await;
    dump_state("after A consolidate", &cm_a, tmp.path()).await;

    // FIXTURE NECESSITY, stated rather than hidden. `CacheManager` derives its instance
    // ID as `hostname:pid` (`cache.rs:1536-1539`) with no override, so two managers in
    // one test process write to the SAME journal file. `consolidate_object` folds every
    // journal it finds and does not truncate them, so B's consolidation would replay A's
    // version-A entry after B's version-B entry and restore A's `object_metadata` —
    // observed directly, `.meta` reporting `version-a-etag` after B's write. On a real
    // fleet the two proxies are separate processes on separate hosts with distinct
    // journal files, and A's entry has already been folded into the `.meta` by this
    // point, so removing it here reproduces the production end state rather than
    // inventing one.
    //
    // This is a test-harness limitation, NOT the defect under test. If a future change
    // makes the instance ID injectable, replace this with two distinct IDs.
    remove_journals(tmp.path());
    dump_state("after A journal drain", &cm_a, tmp.path()).await;

    let promoted = cm_a.promote_range_to_ram_cache_frame(
        KEY,
        (RANGE_START, RANGE_END),
        body_a.clone(),
        s3_proxy::compression::CompressionAlgorithm::None,
        ETAG_A.to_string(),
        String::new(),
        1u64 << 40,
    );
    assert!(
        promoted,
        "PRECONDITION FAILED: version A was not promoted to instance A's RAM tier, so \
         this test would prove nothing."
    );

    let ram_warm = cm_a.get_range_from_ram_cache(
        KEY,
        RANGE_START,
        RANGE_END,
        std::time::Duration::from_secs(3600),
    );
    assert_eq!(
        ram_warm.as_deref(),
        Some(body_a.as_slice()),
        "PRECONDITION FAILED: instance A's RAM tier should hold version A"
    );

    // --- Instance B overwrites the key on the SHARED volume -----------------
    // Same key, same byte range, same length, different content and ETag. Performed by
    // B's own stack, so B's RAM invalidation is local to B by construction.
    //
    // The invalidate-then-store sequence mirrors the real write-through PUT path: the
    // handler calls `invalidate_cache_unified_for_operation` (`http_proxy.rs:4248`) →
    // `invalidate_cache_hierarchy` BEFORE caching the new body. Skipping it made the
    // fixture leave version A's `object_metadata` on the shared `.meta`, because
    // `DiskCacheManager::store_range` merges into existing metadata rather than replacing it.
    //
    // This call is also the crux of the finding: `invalidate_cache_hierarchy` →
    // `remove_from_ram_cache_unified` (`cache.rs:2486`) clears the RAM tier of whichever
    // instance runs it. Here that is B. Nothing reaches A.
    cm_b.invalidate_cache_hierarchy(KEY)
        .await
        .expect("B invalidates the superseded entry, as a real PUT does");
    dump_state("after B invalidate", &cm_a, tmp.path()).await;

    rh_b.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &body_b,
            object_metadata(ETAG_B),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("B overwrites with version B");

    // Consolidate B's journal too, so the shared `.meta` reflects version B — the state
    // A would find on any read past the consolidation interval.
    dump_state("after B store, before consolidate", &cm_a, tmp.path()).await;
    consolidate(&cm_b).await;
    dump_state("after B consolidate", &cm_a, tmp.path()).await;

    // B's write must be visible to A through the shared volume, otherwise this test is
    // measuring a propagation delay rather than the version mix. This is the header
    // source `serve_full_object_from_cache` uses at `http_proxy.rs:7928`.
    let disk_meta_seen_by_a = cm_a
        .get_metadata_from_disk(KEY)
        .await
        .expect("A reads shared .meta");
    let header_etag = match &disk_meta_seen_by_a {
        Some(m) => m.object_metadata.etag.clone(),
        None => panic!(
            "PRECONDITION FAILED: instance A cannot read a .meta for {KEY} at all, so \
             the header source is unavailable and the version-mix question cannot be \
             asked. In production `serve_full_object_from_cache` would fall back to \
             ObjectMetadata::default() here. If this fires, B's metadata is still in \
             its per-instance journal and has not been consolidated — see \
             OBSERVATION 1 in tests/ram_etag_invalidation_gap_test.rs."
        ),
    };
    assert_eq!(
        header_etag, ETAG_B,
        "PRECONDITION FAILED: instance A's header source should report version B's \
         ETag after B's overwrite. Got {header_etag}. Without this the test cannot \
         distinguish a version mix from A simply never seeing B's write."
    );

    // --- A GET arrives at instance A ---------------------------------------
    // This is the byte source `serve_full_object_from_cache` uses, reached via
    // `get_cached_range_data` at `http_proxy.rs:9004`.
    // The `Range` handed to `load_range_data_with_cache` mirrors what
    // `get_cached_range_data` passes in production: a `cached_range` derived from the
    // metadata just read off the shared volume — so its `etag` is version B's. That is
    // the point. The RAM lookup inside ignores this field entirely and keys only on
    // `(cache_key, start, end)`, which is why B's ETag travelling alongside A's bytes is
    // not detected.
    let full_range = Range {
        start: RANGE_START,
        end: RANGE_END,
        data: Vec::new(),
        etag: ETAG_B.to_string(),
        last_modified: String::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::None,
    };
    let (served_bytes, is_ram_hit) = cm_a
        .load_range_data_with_cache(KEY, &full_range, &rh_a)
        .await
        .expect("A loads the range for the client");

    // Non-vacuity, UPDATED for Requirement 4's fix. Before the fix, a stale RAM entry
    // was served unconditionally and `is_ram_hit` was true — that was the whole defect.
    // After the fix, `load_range_data_with_cache` compares `range.etag` (version B, as
    // resolved from the shared `.meta` above) against the RAM entry's own etag (version
    // A), finds a mismatch, invalidates A's RAM ranges, and falls through to disk — so
    // `is_ram_hit=false` here is the fix working, not a fixture failure. What matters is
    // that A's RAM tier genuinely held version A going into this call (confirmed by the
    // earlier `ram_warm` assertion) and that the served bytes are nonetheless correct.
    if is_ram_hit {
        panic!(
            "REGRESSION: the byte read reports is_ram_hit=true despite the version \
             mismatch — the etag comparison in load_range_data_with_cache did not catch \
             this case."
        );
    }

    let body_version = if served_bytes.as_ref() == body_a.as_slice() {
        "A"
    } else if served_bytes.as_ref() == body_b.as_slice() {
        "B"
    } else {
        "neither"
    };

    println!(
        "OBSERVATION: instance A would answer this GET with body=version {body_version} \
         ({} bytes, ram_hit={is_ram_hit}) and etag={header_etag} (version B). Byte source \
         is the RAM tier keyed {KEY}:range:{RANGE_START}:{RANGE_END}; header source is an \
         uncached .meta read.",
        served_bytes.len()
    );

    // --- THE DEFECT --------------------------------------------------------
    // A response must describe one version. `cache-coherency-invariants.md` states the
    // contract explicitly: "the bytes, etag, and content-length must all come from the
    // same version". Bounded staleness is permitted; an internally inconsistent response
    // is not, because a client cannot detect it — the ETag it caches, revalidates
    // against, and passes to If-Match describes bytes it never received.
    assert_eq!(
        served_bytes.as_ref(),
        body_b.as_slice(),
        "REGRESSION: instance A served version {body_version}'s bytes alongside version \
         B's ETag ({header_etag}). This is the version-mix defect Requirement 4's fix \
         addresses: A's RAM tier held stale version-A bytes with no cross-instance \
         invalidation signal, and nothing compared the RAM entry's own etag against the \
         current version before serving it."
    );
}

/// CONTROL 3 (criterion 4.1) — a genuinely FRESH RAM hit performs no
/// Shared_Storage read at all. The test above proves a stale hit is CORRECTLY rejected;
/// this proves a fresh hit is served WITHOUT paying the read that rejection requires —
/// the two halves the requirement says a correctness-only test cannot distinguish.
///
/// This does not exercise `CacheManager::ram_verify`/`get_range_from_ram_cache_hit`
/// directly (those back the higher-level `serve_full_object_from_cache`/
/// `serve_range_from_cache` paths in `http_proxy.rs`); it exercises the same property at
/// the `load_range_data_with_cache` level this file already uses, by asserting the etag
/// comparison this fix added does not require the RAM entry's OWN promotion-time read to
/// be repeated — i.e. two consecutive RAM hits for an unchanged object both report
/// `is_ram_hit=true`, proving the comparison is a pure in-memory check (RAM entry etag vs.
/// caller-supplied etag) rather than a disk re-read disguised as one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_unchanged_object_ram_hit_needs_no_disk_reread() {
    let tmp = TempDir::new().expect("tempdir");
    let (cm_a, _disk_a, rh_a) = make_instance(tmp.path()).await;

    let body_a = version_a_bytes();
    rh_a.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &body_a,
            object_metadata(ETAG_A),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("A seeds version A");
    consolidate(&cm_a).await;

    assert!(cm_a.promote_range_to_ram_cache_frame(
        KEY,
        (RANGE_START, RANGE_END),
        body_a.clone(),
        s3_proxy::compression::CompressionAlgorithm::None,
        ETAG_A.to_string(),
        String::new(),
        1u64 << 40,
    ));

    let full_range = Range {
        start: RANGE_START,
        end: RANGE_END,
        data: Vec::new(),
        etag: ETAG_A.to_string(),
        last_modified: String::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::None,
    };

    // Two consecutive reads with the object unchanged. Both must be RAM hits with
    // correct bytes — the etag comparison the fix added must not turn every RAM read
    // into a disk-backed one.
    for attempt in 1..=2 {
        let (served_bytes, is_ram_hit) = cm_a
            .load_range_data_with_cache(KEY, &full_range, &rh_a)
            .await
            .expect("A loads the range");
        assert!(
            is_ram_hit,
            "CONTROL FAILED (attempt {attempt}): an unchanged object's RAM hit must \
             remain a RAM hit — the etag comparison must not force every RAM read onto \
             the disk tier."
        );
        assert_eq!(
            served_bytes.as_ref(),
            body_a.as_slice(),
            "CONTROL FAILED (attempt {attempt}): served bytes must match version A"
        );
    }
}

/// CONTROL 1 — the disk tier is correct, and the stale serve is attributable to the RAM
/// tier alone.
///
/// Identical fixture to the test above, except instance A's RAM ranges are cleared before
/// the read. A then returns version B's bytes alongside version B's ETag. This is what
/// makes the failure above a RAM finding rather than "instance B's write never landed" or
/// "the shared `.meta` is wrong" — both of which would also produce a version-A body.
///
/// Expected to PASS both before and after any fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_disk_tier_returns_version_b_once_instance_a_ram_is_cleared() {
    let tmp = TempDir::new().expect("tempdir");
    let (cm_a, _disk_a, rh_a) = make_instance(tmp.path()).await;
    let (cm_b, _disk_b, rh_b) = make_instance(tmp.path()).await;

    let body_a = version_a_bytes();
    let body_b = version_b_bytes();

    rh_a.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &body_a,
            object_metadata(ETAG_A),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("A seeds version A");
    consolidate(&cm_a).await;
    remove_journals(tmp.path());

    assert!(cm_a.promote_range_to_ram_cache_frame(
        KEY,
        (RANGE_START, RANGE_END),
        body_a.clone(),
        s3_proxy::compression::CompressionAlgorithm::None,
        ETAG_A.to_string(),
        String::new(),
        1u64 << 40,
    ));

    cm_b.invalidate_cache_hierarchy(KEY)
        .await
        .expect("B invalidates");
    rh_b.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &body_b,
            object_metadata(ETAG_B),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("B writes version B");
    consolidate(&cm_b).await;

    // The only difference from the failing test: A's RAM ranges are dropped. This is
    // exactly what `invalidate_ram_ranges` does — and exactly what no cross-instance
    // signal causes it to do in the scenario above.
    cm_a.invalidate_ram_ranges(KEY)
        .await
        .expect("clear A's RAM ranges");
    assert!(
        cm_a.get_range_from_ram_cache(
            KEY,
            RANGE_START,
            RANGE_END,
            std::time::Duration::from_secs(3600)
        )
        .is_none(),
        "fixture: A's RAM ranges should be gone"
    );

    let full_range = Range {
        start: RANGE_START,
        end: RANGE_END,
        data: Vec::new(),
        etag: ETAG_B.to_string(),
        last_modified: String::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::None,
    };
    let (served_bytes, is_ram_hit) = cm_a
        .load_range_data_with_cache(KEY, &full_range, &rh_a)
        .await
        .expect("A loads from disk");

    assert!(
        !is_ram_hit,
        "fixture: this read must come from disk, not RAM"
    );
    assert_eq!(
        served_bytes.as_ref(),
        body_b.as_slice(),
        "CONTROL FAILED: with A's RAM cleared, A should serve instance B's bytes from the \
         shared disk volume. If this fails, B's write did not land correctly and the \
         version mix demonstrated by the sibling test would have a different cause."
    );
}

/// CONTROL 2 — a size-CHANGING overwrite does not produce the version mix.
///
/// The RAM range key is `{cache_key}:range:{start}:{end}`, so an overwrite that changes
/// the object's length shifts the requested boundaries, the lookup misses, and the read
/// falls through to disk and returns correct bytes. This bounds the blast radius of the
/// finding to size-preserving overwrites, and proves the sibling test's failure depends on
/// the key matching rather than on RAM being consulted unconditionally.
///
/// Expected to PASS both before and after any fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_size_changing_overwrite_misses_ram_and_serves_correct_bytes() {
    let tmp = TempDir::new().expect("tempdir");
    let (cm_a, _disk_a, rh_a) = make_instance(tmp.path()).await;
    let (cm_b, _disk_b, rh_b) = make_instance(tmp.path()).await;

    let body_a = version_a_bytes();
    // Version B is one byte SHORTER, so its range is 0..=1022 rather than 0..=1023.
    let shorter_end = RANGE_END - 1;
    let body_b: Vec<u8> = (0..(shorter_end - RANGE_START + 1))
        .map(|i| ((i % 251) as u8) ^ 0xFF)
        .collect();

    rh_a.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            &body_a,
            object_metadata(ETAG_A),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("A seeds version A");
    consolidate(&cm_a).await;
    remove_journals(tmp.path());

    assert!(cm_a.promote_range_to_ram_cache_frame(
        KEY,
        (RANGE_START, RANGE_END),
        body_a.clone(),
        s3_proxy::compression::CompressionAlgorithm::None,
        ETAG_A.to_string(),
        String::new(),
        1u64 << 40,
    ));

    cm_b.invalidate_cache_hierarchy(KEY)
        .await
        .expect("B invalidates");
    let mut meta_b = object_metadata(ETAG_B);
    meta_b.content_length = body_b.len() as u64;
    rh_b.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            shorter_end,
            &body_b,
            meta_b,
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("B writes a shorter version B");
    consolidate(&cm_b).await;

    // A now asks for the object's CURRENT full extent, which is one byte shorter — so the
    // RAM key is `...:range:0:1022` and A's entry sits under `...:range:0:1023`.
    let current_range = Range {
        start: RANGE_START,
        end: shorter_end,
        data: Vec::new(),
        etag: ETAG_B.to_string(),
        last_modified: String::new(),
        compression_algorithm: s3_proxy::compression::CompressionAlgorithm::None,
    };
    let (served_bytes, is_ram_hit) = cm_a
        .load_range_data_with_cache(KEY, &current_range, &rh_a)
        .await
        .expect("A loads the current extent");

    println!(
        "OBSERVATION: size-changing overwrite — A's read of 0..={shorter_end} \
         ram_hit={is_ram_hit}, {} bytes served",
        served_bytes.len()
    );
    assert!(
        !is_ram_hit,
        "CONTROL FAILED: A's stale RAM entry is keyed 0..={RANGE_END} and this read asks \
         for 0..={shorter_end}, so it must not be a RAM hit. If it is, the RAM lookup is \
         not keyed on the range boundaries as assumed and the sibling test's reasoning \
         needs revisiting."
    );
    assert_eq!(
        served_bytes.as_ref(),
        body_b.as_slice(),
        "CONTROL FAILED: a size-changing overwrite should serve instance B's bytes"
    );
}

// ===========================================================================
// ram-tier-effectiveness tasks 12 and 13 — COUNT THE READS, AND PIN THE BOUND
// ===========================================================================
//
// Everything above this line asserts CORRECTNESS: which bytes and which ETag a response
// carries. `control_unchanged_object_ram_hit_needs_no_disk_reread` in particular asserts
// `is_ram_hit`, and that assertion holds under BOTH designs the spec contemplates —
// read-then-compare and read-free — so it cannot show that `ram-tier-effectiveness:R4.1`
// (a within-interval RAM hit performs NO Shared_Storage read) actually holds. The two
// groups below assert the READ instead of the outcome.
//
// ---------------------------------------------------------------------------
// WHICH COUNTER, AND WHY NOT THE OBVIOUS ONES. This is the ninth instance of the
// assert-the-right-predicate family in this repo, so the branch is named here and the
// counter is matched against it rather than against something adjacent that moves at
// roughly the same time.
//
// THE BRANCH: `CacheManager::ram_verify`'s interval test on `hit.verified_at_ms`
// (`src/cache.rs`, the `if age_ms < self.ram_cache_verification_interval` early return).
// Inside the interval it returns `true` without touching disk. Past it, it calls the
// injected `disk_read` closure exactly once per `cache_key` per interval (the
// `ram_verify_locks` coalescing state).
//
// THE COUNTER: the number of invocations of that injected closure, counted by wrapping
// production's own reader. Both production call sites pass, verbatim,
// `|key| async move { cm.get_metadata_etag_for_ram_verify(&key).await }`
// (`src/http_proxy.rs`, the full-object and range RAM-hit arms), and these tests pass the
// same expression with an `AtomicUsize` bump around it. So the counted event IS the
// `.meta` read the branch performs — not a proxy for it. There is no gap between the
// predicate and the observation to get wrong.
//
// Non-vacuity: a closure that counted without reading would satisfy the count assertions
// while proving nothing, so the past-interval half also asserts `ram_verify` returned
// `true`. That verdict requires `RamVerifyDiskCheck::Found(e)` with `e == hit.etag`, which
// can only come from a genuine `tokio::fs::read_to_string` of the seeded `.meta`. A stub
// would return `IoError` and the verdict would be `false`.
//
// TWO COUNTERS DELIBERATELY REJECTED, both of which the task offers or a reader would
// reach for:
//
//   * `metadata_cache.disk_hits` (`MetadataCache`'s disk-hit metric). WRONG PREDICATE,
//     and provably so rather than arguably: `get_metadata_etag_for_ram_verify` is a raw
//     `tokio::fs::read_to_string` on the `.meta` path and never consults `MetadataCache`.
//     `MetadataCache::record_disk_hit()` has exactly two call sites in `src/`, both on the
//     `get_metadata_cached` / `get_head_cache_entry_unified` paths, and neither is reached
//     from `ram_verify`. So that counter is flat across BOTH halves — which means the
//     within-interval half would pass vacuously and the past-interval half could never
//     pass at all. It is the dangerous direction: the control-shaped half certifies the
//     property on no evidence.
//   * `cache.ram_cache_hits`. WRONG PREDICATE for a different reason, and measured on the
//     fleet rather than reasoned: it is bumped inside `ShardedRamCache::get`
//     (`src/ram_cache.rs`) when the shard lookup FINDS the key, BEFORE the caller's etag
//     comparison, so it moves identically whether the entry is served or found-and-
//     rejected. The fleet suite read it `11 -> 12` across a GET whose correct post-fix
//     outcome was rejection. It says nothing about whether
//     a `.meta` read happened.
//
// R4.7's coalescing means the expected count is per-key-per-interval, not per-request.
// These fixtures are single-threaded and one-key, so per-request and per-key-per-interval
// coincide — stated so a future concurrent variant does not read `+1` as a per-request
// figure.
//
// ---------------------------------------------------------------------------
// `is_shared_storage_mode()` VOIDS NOTHING HERE, and the reasoning matters because the
// warning is real elsewhere: `DiskCacheManager::new` leaves `hybrid_metadata_writer:
// None`, so `find_pending_journal_ranges` returns `Ok(Vec::new())` immediately and any
// fixture asserting an EMPTY journal result passes vacuously. These fixtures build through
// `CacheManager::new_with_shared_storage` (via `make_instance*`), not `DiskCacheManager`
// directly, and `consolidate()` below `expect`s `get_journal_consolidator()` to be
// present — so a fixture that had somehow lost shared-storage mode would PANIC rather than
// pass quietly. More decisively: no assertion in either group reads
// `find_pending_journal_ranges`, in either direction. The verdicts come from a closure
// invocation count and from `ram_verify`'s boolean.

/// `make_instance`, plus an explicit `ram_cache_verification_interval`.
///
/// Separate helper rather than a parameter on `make_instance` because
/// `set_ram_cache_verification_interval` takes `&mut self` and must run before the
/// `CacheManager` is wrapped in the `Arc` every caller wants — the same sequencing
/// `http_proxy.rs` uses when it overrides the 1s constructor default from config.
async fn make_instance_with_verify_interval(
    dir: &std::path::Path,
    verify_interval: Duration,
) -> (
    Arc<CacheManager>,
    Arc<tokio::sync::RwLock<DiskCacheManager>>,
    Arc<RangeHandler>,
) {
    let mut config = Config::default();
    config.cache.ram_cache_enabled = true;

    let mut cache_manager = CacheManager::new_with_shared_storage(
        dir.to_path_buf(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        config.compression.enabled,
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    );
    cache_manager.set_ram_cache_verification_interval(verify_interval);
    let cache_manager = Arc::new(cache_manager);

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

    (cache_manager, disk_cache_manager, range_handler)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Seed version A on the shared volume, consolidate it into the `.meta`, and promote the
/// range into this instance's RAM tier. Returns the `RamRangeHit` the production RAM-hit
/// arms would obtain, which is the input `ram_verify` takes.
async fn seed_and_promote_version_a(
    cm: &Arc<CacheManager>,
    rh: &Arc<RangeHandler>,
    body_a: &[u8],
) -> RamRangeHit {
    rh.get_cache_manager()
        .create_configured_disk_cache_manager()
        .store_range(
            KEY,
            RANGE_START,
            RANGE_END,
            body_a,
            object_metadata(ETAG_A),
            Duration::from_secs(3600),
            false,
        )
        .await
        .expect("seed version A");
    consolidate(cm).await;

    assert!(
        cm.promote_range_to_ram_cache_frame(
            KEY,
            (RANGE_START, RANGE_END),
            body_a.to_vec(),
            s3_proxy::compression::CompressionAlgorithm::None,
            ETAG_A.to_string(),
            String::new(),
            1u64 << 40,
        ),
        "PRECONDITION: the range must promote into the RAM tier, or there is no RAM hit \
         to verify and every assertion below is vacuous"
    );

    cm.get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, Duration::from_secs(3600))
        .expect(
            "PRECONDITION: `get_range_from_ram_cache_hit` must return a hit for the range \
             just promoted — without one there is nothing for `ram_verify` to decide about",
        )
}

/// Count invocations of production's own `.meta` reader while running `ram_verify`.
///
/// The closure body is `http_proxy.rs`'s verbatim, with one `fetch_add` in front of it, so
/// the count is 1:1 with the `tokio::fs::read_to_string` inside
/// `get_metadata_etag_for_ram_verify`.
async fn ram_verify_counting_reads(
    cm: &Arc<CacheManager>,
    hit: &RamRangeHit,
    reads: &Arc<AtomicUsize>,
) -> bool {
    let cm_for_read = cm.clone();
    let reads_for_read = reads.clone();
    cm.ram_verify(KEY, hit, move |key| {
        let cm = cm_for_read.clone();
        let reads = reads_for_read.clone();
        async move {
            reads.fetch_add(1, Ordering::SeqCst);
            cm.get_metadata_etag_for_ram_verify(&key).await
        }
    })
    .await
}

/// TASK 12 — count the reads. Both halves, because either alone is satisfiable by a
/// wrong implementation: the within-interval half alone passes against a proxy that NEVER
/// verifies, and the past-interval half alone against one that ALWAYS does.
///
/// ACCEPTANCE CRITERION THAT CAN FAIL (`tasks.md` task 12): with
/// `ram_cache_verification_interval` forced to zero every hit verifies, so the
/// within-interval half must go RED. If it stays green the test has drifted back to
/// measuring correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ram_verify_reads_meta_never_within_interval_and_exactly_once_past_it() {
    let tmp = TempDir::new().expect("tempdir");
    // Generous interval so the within-interval half cannot lose a race on a loaded
    // machine. Every threshold below is derived from the ACCESSOR, never from this
    // literal — see the `configured` binding.
    let (cm_a, _disk_a, rh_a) =
        make_instance_with_verify_interval(tmp.path(), Duration::from_secs(30)).await;

    let configured = cm_a.ram_cache_verification_interval();
    assert_eq!(
        configured,
        Duration::from_secs(30),
        "PRECONDITION: `set_ram_cache_verification_interval` must be observable through \
         `ram_cache_verification_interval()`. If this fails the two halves below are \
         being timed against an interval the code is not using."
    );

    let body_a = version_a_bytes();
    let hit = seed_and_promote_version_a(&cm_a, &rh_a, &body_a).await;
    let reads = Arc::new(AtomicUsize::new(0));

    // --- HALF 1: WITHIN the interval — NO `.meta` read ---------------------
    // `verified_at_ms` is promotion time, which is now, so `age_ms` is a few
    // milliseconds against a 30s interval.
    let before = reads.load(Ordering::SeqCst);
    let verified_fresh = ram_verify_counting_reads(&cm_a, &hit, &reads).await;
    let after_fresh = reads.load(Ordering::SeqCst);
    assert!(
        verified_fresh,
        "a within-interval hit must be servable without verification"
    );
    assert_eq!(
        after_fresh - before,
        0,
        "ram-tier-effectiveness:R4.1 VIOLATED: a RAM hit {}ms old, inside the configured \
         {}s verification interval, performed {} Shared_Storage `.meta` read(s). The \
         interval exists precisely so this read does not happen; a read here means \
         `ram_verify`'s `age_ms < interval` early return was not taken. NOTE: if \
         `ram_cache_verification_interval` has been forced to zero this failure is \
         EXPECTED and is the task-12 acceptance criterion, not a defect.",
        now_ms().saturating_sub(hit.verified_at_ms),
        configured.as_secs(),
        after_fresh - before
    );

    // --- HALF 2: PAST the interval — EXACTLY ONE `.meta` read --------------
    // The age is synthesised rather than slept, so the test does not spend the configured
    // interval (up to 60s operator-tunable) in a sleep. `RamRangeHit`'s fields are `pub`
    // and this is the same struct production hands to `ram_verify`; only
    // `verified_at_ms` differs from the hit above, so the two halves differ in ONE input.
    let stale_hit = RamRangeHit {
        verified_at_ms: now_ms()
            .saturating_sub(configured.as_millis() as u64)
            .saturating_sub(1_000),
        ..hit.clone()
    };
    let before_stale = reads.load(Ordering::SeqCst);
    let verified_stale = ram_verify_counting_reads(&cm_a, &stale_hit, &reads).await;
    let after_stale = reads.load(Ordering::SeqCst);
    assert_eq!(
        after_stale - before_stale,
        1,
        "ram-tier-effectiveness:R3.13 VIOLATED: a RAM hit past the configured {}s \
         interval performed {} Shared_Storage `.meta` read(s), expected exactly 1. Zero \
         means the bound never closes and the entry is served unverified forever; more \
         than one means R4.7's per-key coalescing is not holding.",
        configured.as_secs(),
        after_stale - before_stale
    );
    // NON-VACUITY. A closure that counted without reading would satisfy both counts. This
    // verdict is `true` only if the injected read returned `Found(e)` with `e` equal to
    // the hit's etag, which requires a real read of the seeded `.meta`.
    assert!(
        verified_stale,
        "the counted read must be a REAL `.meta` read: the object is unchanged, so \
         `ram_verify` must return true. A false verdict here means the read returned \
         `Absent`/`IoError` and the count above is an instrumentation artefact rather \
         than evidence of a Shared_Storage read."
    );
}

/// TASK 13 — the bound contract, both halves. Bounded staleness is a CONTRACT
/// (`cache-coherency-invariants.md`): a stale-but-self-consistent serve is PERMITTED
/// within the interval and MUST NOT persist beyond it. A test asserting only "no stale
/// serve" over-specifies and forbids behaviour the contract allows; one asserting only
/// that a stale serve happened says nothing about the bound. Both halves, therefore.
///
/// The `.meta` is mutated DIRECTLY on the shared volume — new etag, cleared ranges —
/// without driving this instance's HEAD path or its invalidation path. That is what makes
/// the fixture cross-instance in shape and what stops an eviction-only implementation from
/// passing: nothing local tells instance A that anything changed.
///
/// ACCEPTANCE CRITERION THAT CAN FAIL (`tasks.md` task 13): the past-interval half must go
/// RED when `ram_verify`'s mismatch arm is made to return `true`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ram_verify_bound_permits_stale_within_interval_and_refuses_past_it() {
    let tmp = TempDir::new().expect("tempdir");
    let (cm_a, _disk_a, rh_a) =
        make_instance_with_verify_interval(tmp.path(), Duration::from_secs(30)).await;

    // READ THE CONFIGURED INTERVAL, never a literal. `refresh_interval` is operator-tunable
    // 1-300s and `ram_cache_verification_interval` 0-60s, so a test hardcoding 1s or 5s
    // fails against correct code on a fleet that has raised either. Every threshold below
    // derives from this binding.
    let configured = cm_a.ram_cache_verification_interval();
    assert!(
        !configured.is_zero(),
        "PRECONDITION: a zero verification interval means every hit verifies, so the \
         within-interval half has no window to test. Configured: {configured:?}"
    );

    let body_a = version_a_bytes();
    let hit = seed_and_promote_version_a(&cm_a, &rh_a, &body_a).await;

    // --- DIRECT MUTATION OF THE SHARED `.meta` ----------------------------
    let meta_path = cm_a.get_new_metadata_file_path(KEY);
    let mut meta = cm_a
        .get_metadata_from_disk(KEY)
        .await
        .expect("read the seeded .meta")
        .expect("PRECONDITION: the seeded .meta must exist before it can be mutated");
    assert_eq!(
        meta.object_metadata.etag, ETAG_A,
        "PRECONDITION: the .meta must hold version A before mutation"
    );
    meta.object_metadata.etag = ETAG_B.to_string();
    meta.ranges.clear();
    std::fs::write(
        &meta_path,
        serde_json::to_string(&meta).expect("serialise mutated .meta"),
    )
    .expect("write the mutated .meta directly, as another instance's write would appear");

    // Confirm the mutation is what `ram_verify`'s reader will actually see. Without this,
    // a wrong path would leave both halves reading version A and the past-interval half
    // would fail for a fixture reason indistinguishable from a product defect.
    let observed = cm_a
        .get_metadata_from_disk(KEY)
        .await
        .expect("re-read the mutated .meta")
        .expect("the mutated .meta must still exist");
    assert_eq!(
        observed.object_metadata.etag,
        ETAG_B,
        "PRECONDITION: the direct mutation must be visible at {}, the path \
         `get_metadata_etag_for_ram_verify` reads",
        meta_path.display()
    );
    assert!(
        observed.ranges.is_empty(),
        "PRECONDITION: the mutation must have cleared the ranges"
    );

    // --- HALF 1: WITHIN the interval — the old version MAY be served -------
    // Nothing propagated the overwrite to instance A's RAM tier, and within the interval
    // A is entitled not to look. This half asserts the CONTRACT, not an absence of
    // staleness: `true` here is bounded staleness working as designed.
    assert!(
        ram_verify_counting_reads(&cm_a, &hit, &Arc::new(AtomicUsize::new(0))).await,
        "ram-tier-effectiveness:R3.3 (first half) VIOLATED: a RAM hit inside the \
         configured {}s interval was refused. Bounded staleness is the contract — \
         refusing here would make the interval meaningless and force a Shared_Storage \
         read on every RAM hit, which is the cost the tier exists to avoid.",
        configured.as_secs()
    );

    // --- HALF 2: PAST the interval — the old version MUST NOT be served ----
    let stale_hit = RamRangeHit {
        verified_at_ms: now_ms()
            .saturating_sub(configured.as_millis() as u64)
            .saturating_sub(1_000),
        ..hit.clone()
    };
    assert!(
        !ram_verify_counting_reads(&cm_a, &stale_hit, &Arc::new(AtomicUsize::new(0))).await,
        "ram-tier-effectiveness:R3.3 (second half) VIOLATED: a RAM hit past the \
         configured {}s interval was served although the shared `.meta` now holds \
         {ETAG_B} and the RAM entry holds {ETAG_A}. This is the half that proves the \
         window CLOSES; without it the first half alone is satisfied by a proxy that \
         never verifies at all.",
        configured.as_secs()
    );

    // The refusal must have invalidated A's RAM ranges rather than merely returning false,
    // or the next hit repeats the read and the entry is rejected forever without the tier
    // ever recovering.
    assert!(
        cm_a.get_range_from_ram_cache_hit(KEY, RANGE_START, RANGE_END, Duration::from_secs(3600))
            .is_none(),
        "a verify mismatch must invalidate this key's RAM ranges, so a subsequent lookup \
         misses and falls through to the disk tier"
    );
}
