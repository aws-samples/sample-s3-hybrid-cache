//! `CacheManager::ram_verify` — all four arms of the decision table, both interval
//! boundaries, and the per-key coalescing property.
//!
//! Spec: `ram-tier-effectiveness` Requirement 4, criteria 2, 2a, 2b, 2c, 2d, 6, 7.
//!
//! # Why this file exists
//!
//! `ram_verify` is the function that decides whether a RAM range hit may be served
//! without a shared-storage read, and — on two of its four arms — deletes the key's
//! RAM ranges. It shipped with **zero** tests at any level: no `#[cfg(test)]` unit
//! test, nothing under `tests/`, and R4's own nominated acceptance test says in its
//! own doc comment that it does not exercise it
//! (`tests/cross_instance_ram_version_mix_test.rs`, near the top). Re-derived here
//! before this file was written: `grep -rn ram_verify src/ tests/ examples/` found
//! production call sites in `http_proxy.rs` and `config.rs` prose, and in `tests/`
//! only that one disclaimer.
//!
//! The arms are not interchangeable and the difference is the whole point.
//! `Absent` invalidates; `IoError` does not. Swap them and one direction turns an
//! NFS blip into a fleet-wide cache flush while the other serves stale bytes
//! indefinitely — and per the cache coherency invariants
//! § "Invariant 2's dangerous corollary", `invalidate_ram_ranges` acting on a local
//! misjudgement reaches the whole fleet through shared storage while being
//! invisible from the instance that loses the data.
//!
//! # The seam, and why no harness is needed
//!
//! `ram_verify` is `pub` and takes its `.meta` read as an injected closure
//! (`F: FnOnce(String) -> Fut, Fut: Future<Output = RamVerifyDiskCheck>`), so each
//! arm is reachable by returning the corresponding `RamVerifyDiskCheck` and the
//! read itself is directly countable. Production injects
//! `CacheManager::get_metadata_etag_for_ram_verify`; these tests inject a closure.
//!
//! # `multi_thread` is required, not cosmetic
//!
//! `promote_range_to_ram_cache_frame` and `get_range_from_ram_cache_hit` reach the
//! async RAM cache through `tokio::task::block_in_place`, which panics on the
//! current-thread runtime `#[tokio::test]` uses by default. Both existing RAM test
//! files record the same requirement.
//!
//! # What establishes the two preconditions, and why each is asserted
//!
//! Every assertion below is vacuous unless the entry is **resident** and — for all
//! but the first — the interval has **elapsed**. Both sides of a red/green pair
//! behave identically when either fails, so neither is assumed:
//!
//! - **Residency** comes from `promote_range_to_ram_cache_frame`, whose `bool`
//!   return is asserted, followed by a `get_range_from_ram_cache_hit` under a
//!   generous `get_ttl` which is also asserted `Some`. The second check is not
//!   redundant: promotion reports handing the entry to the RAM cache, not that it
//!   survived shard-capacity eviction.
//! - **Elapsed-ness** is a property of `hit.verified_at_ms`, which
//!   `promote_range_to_ram_cache_frame` sets to promotion time. For the
//!   decision-table arms the interval is `Duration::ZERO`, so
//!   `age_ms < interval` is false for every possible timestamp and elapsed-ness
//!   holds by construction — asserted anyway as `interval.is_zero()`. For the
//!   coalescing test the interval is 2s and elapsed-ness comes from a real sleep;
//!   the measured age is asserted against the interval before the concurrent run.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3_proxy::cache::{CacheManager, RamRangeHit, RamVerifyDiskCheck};
use s3_proxy::compression::CompressionAlgorithm;
use tempfile::TempDir;

/// Generous enough that `get_range_from_ram_cache_hit`'s own zero-`get_ttl` gate
/// (`get-ttl-zero-enforcement` R4.2) never fires in these tests — the subject here
/// is the verification interval, which is a different bound.
const GENEROUS_TTL: Duration = Duration::from_secs(3600);
const MAX_RAM: u64 = 8 * 1024 * 1024;
const RANGE_START: u64 = 0;
const RANGE_END: u64 = 999;
const ETAG: &str = "\"version-a\"";
const OTHER_ETAG: &str = "\"version-b\"";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn payload() -> Vec<u8> {
    (0..1000).map(|i| (i % 251) as u8).collect()
}

/// A `CacheManager` with the RAM tier enabled and an explicit verification
/// interval. `CacheManager::new` defaults the interval to 1s; production overrides
/// it from `cache.ram_cache_verification_interval` through the same setter.
fn manager(interval: Duration) -> (Arc<CacheManager>, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let mut cm = CacheManager::new(
        dir.path().to_path_buf(),
        true, // ram_cache_enabled
        MAX_RAM,
        1024, // compression_threshold
        true, // compression_enabled
    );
    cm.set_ram_cache_verification_interval(interval);
    assert_eq!(
        cm.ram_cache_verification_interval(),
        interval,
        "PRECONDITION MISSING: set_ram_cache_verification_interval did not take, so \
         every interval-dependent assertion below would measure the 1s default instead"
    );
    (Arc::new(cm), dir)
}

/// Make one range resident in RAM under `key` with `etag`, and assert it really is.
/// `CompressionAlgorithm::None` stores and reads back verbatim, so no LZ4 frame is
/// needed — the same helper shape the in-module RAM range tests use.
fn seed_resident(cm: &CacheManager, key: &str, etag: &str) {
    let promoted = cm.promote_range_to_ram_cache_frame(
        key,
        (RANGE_START, RANGE_END),
        payload(),
        CompressionAlgorithm::None,
        etag.to_string(),
        String::new(),
        1u64 << 40,
    );
    assert!(
        promoted,
        "PRECONDITION MISSING: the range for {key} was not promoted to RAM, so any \
         assertion about verifying or invalidating it would pass without reaching the \
         decision table. Check ram_cache_enabled and max_ram_cache_size."
    );
    let hit = resident_hit(cm, key);
    assert!(
        hit.is_some(),
        "PRECONDITION MISSING: {key} is not resident in RAM after promotion (accepted \
         by promote, then dropped by shard-capacity eviction?), so the arms below would \
         be measuring an absent entry"
    );
    let hit = hit.unwrap();
    assert_eq!(
        hit.etag, etag,
        "PRECONDITION MISSING: the resident entry for {key} does not carry the etag it \
         was seeded with, so the match/mismatch arms would be keyed off the wrong value"
    );
}

fn resident_hit(cm: &CacheManager, key: &str) -> Option<RamRangeHit> {
    cm.get_range_from_ram_cache_hit(key, RANGE_START, RANGE_END, GENEROUS_TTL)
}

/// An injected `.meta` read that must never run. Declaring the return type here
/// (rather than inlining `async { panic!() }`) keeps the `!` coercion explicit and
/// leaves no unreachable expression for the compiler to lint.
async fn disk_read_must_not_run(_cache_key: String) -> RamVerifyDiskCheck {
    panic!(
        "R4.1/R4.2: ram_verify performed a shared-storage read for an entry verified \
         INSIDE cache.ram_cache_verification_interval. The read-free path is the \
         requirement; a `true` return with a read behind it does not satisfy it. \
         Branch: the `age_ms < self.ram_cache_verification_interval` early return in \
         CacheManager::ram_verify."
    );
}

/// 1. Within the interval, `ram_verify` performs no read AT ALL.
///
/// Branch whose failure this reports: the
/// `age_ms < self.ram_cache_verification_interval.as_millis()` early `return true`
/// in `CacheManager::ram_verify`. The assertion reads the same value that branch
/// reads — `hit.verified_at_ms` against the configured interval — and the
/// precondition below states the measured age rather than assuming it.
///
/// The panicking closure IS the assertion. A counter would be weaker: `0` and
/// "never called" are indistinguishable, so a fixture that failed to invoke the
/// closure for an unrelated reason would read as a pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn within_the_interval_ram_verify_reads_nothing() {
    const INTERVAL: Duration = Duration::from_secs(60);
    let key = "verify-bucket/within-interval.bin";
    let (cm, _dir) = manager(INTERVAL);
    seed_resident(&cm, key, ETAG);

    let hit = resident_hit(&cm, key).expect("resident after seed_resident");
    let age_ms = now_ms().saturating_sub(hit.verified_at_ms);
    assert!(
        age_ms < INTERVAL.as_millis() as u64,
        "PRECONDITION MISSING: the entry's age is {age_ms}ms against a \
         {}ms interval, so it is NOT inside the interval and this test would be \
         exercising the verify path instead of the read-free one",
        INTERVAL.as_millis()
    );

    let verified = cm.ram_verify(key, &hit, disk_read_must_not_run).await;
    assert!(
        verified,
        "R4.1: a RAM hit verified inside cache.ram_cache_verification_interval must be \
         serveable. Branch: the early `return true` in CacheManager::ram_verify."
    );
}

/// 2. Past the interval: exactly one read, the match arm, and the timestamp moves.
///
/// Branches whose failure this reports: the
/// `RamVerifyDiskCheck::Found(current_etag) if current_etag == hit.etag` match guard
/// (return value), and the `self.ram_touch_verified(&hit.range_cache_key)` call
/// inside it (timestamp). The timestamp half is what R4.2a adds over R4.2, and a
/// `true` return does not evidence it — a body that matched and touched nothing
/// would satisfy the return assertion alone.
///
/// Also asserts the closure is handed the **object** `cache_key`, not
/// `hit.range_cache_key`. Those differ (`{key}:range:{start}:{end}`), and production
/// uses the argument to locate the shared `.meta`, so passing the range key would
/// read the wrong path and land on the `Absent` arm for every verification.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn past_the_interval_one_read_matches_and_refreshes_the_timestamp() {
    let key = "verify-bucket/match-arm.bin";
    let (cm, _dir) = manager(Duration::ZERO);
    assert!(
        cm.ram_cache_verification_interval().is_zero(),
        "PRECONDITION MISSING: a non-zero interval leaves elapsed-ness dependent on \
         wall-clock timing rather than holding by construction"
    );
    seed_resident(&cm, key, ETAG);

    let hit = resident_hit(&cm, key).expect("resident after seed_resident");
    let before_ms = hit.verified_at_ms;

    // The timestamp is millisecond-granular, so without this the "advanced" check
    // could fail for a promotion and a touch landing in the same millisecond.
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        now_ms() > before_ms,
        "PRECONDITION MISSING: no measurable time has passed since promotion, so a \
         refreshed timestamp would be indistinguishable from an unrefreshed one"
    );

    let reads = Arc::new(AtomicUsize::new(0));
    let seen_key: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let r = reads.clone();
    let sk = seen_key.clone();
    let verified = cm
        .ram_verify(key, &hit, move |k: String| async move {
            r.fetch_add(1, Ordering::SeqCst);
            *sk.lock().unwrap() = Some(k);
            RamVerifyDiskCheck::Found(ETAG.to_string())
        })
        .await;

    assert!(
        verified,
        "R4.2a: a shared `.meta` etag equal to the RAM entry's own etag must verify the \
         entry. Branch: the `Found(current_etag) if current_etag == hit.etag` guard."
    );
    assert_eq!(
        reads.load(Ordering::SeqCst),
        1,
        "R4.2: past the interval the proxy must read the shared `.meta` EXACTLY once"
    );
    assert_eq!(
        seen_key.lock().unwrap().as_deref(),
        Some(key),
        "R4.2: the injected read must be handed the object cache_key. It was handed \
         something else — if that is hit.range_cache_key, production would look for a \
         `.meta` that does not exist and take the Absent arm on every verification."
    );

    let after =
        resident_hit(&cm, key).expect("R4.2a: a matching verification must not remove the entry");
    assert!(
        after.verified_at_ms > before_ms,
        "R4.2a: the entry's verification timestamp must be refreshed on a match \
         (before={before_ms}, after={}). Branch: the ram_touch_verified call inside the \
         match guard. Without it every subsequent hit re-reads the `.meta` and the \
         read-free path in R4.1 never applies.",
        after.verified_at_ms
    );
}

/// 3. Mismatch: `false` AND the key's RAM ranges are gone.
///
/// Branch whose failure this reports: the unguarded
/// `RamVerifyDiskCheck::Found(current_etag)` arm and its `invalidate_ram_ranges`
/// call. The invalidation is the requirement; `false` alone is satisfied by a body
/// that returns `false` and invalidates nothing, which would leave the superseded
/// bytes readable by the next hit — the defect
/// `tests/cross_instance_ram_version_mix_test.rs` documents.
///
/// Control: a second resident key, asserted still `Some` afterwards, so the
/// invalidation is shown to be key-scoped rather than a cache flush. That control
/// is the one that matters for the corollary in
/// `cache-coherency-invariants.md` — an over-broad invalidation here is invisible
/// locally and reaches the fleet through shared storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatch_returns_false_and_invalidates_only_that_key() {
    let subject = "verify-bucket/mismatch-subject.bin";
    let control = "verify-bucket/mismatch-control.bin";
    let (cm, _dir) = manager(Duration::ZERO);
    seed_resident(&cm, subject, ETAG);
    seed_resident(&cm, control, ETAG);

    let hit = resident_hit(&cm, subject).expect("resident after seed_resident");
    let verified = cm
        .ram_verify(subject, &hit, |_k: String| async move {
            RamVerifyDiskCheck::Found(OTHER_ETAG.to_string())
        })
        .await;

    assert!(
        !verified,
        "R4.2b: a shared `.meta` etag that differs from the RAM entry's must not verify \
         it. Branch: the unguarded `Found(current_etag)` arm."
    );
    assert!(
        resident_hit(&cm, subject).is_none(),
        "R4.2b: the mismatched key's RAM ranges must be invalidated, not merely \
         reported stale. Branch: the invalidate_ram_ranges call in the mismatch arm. \
         Still resident means the next hit serves the superseded version's bytes."
    );
    assert!(
        resident_hit(&cm, control).is_some(),
        "R4.2b CONTROL: invalidation must be scoped to the mismatched cache_key. An \
         unrelated resident key was removed too, which is a cache flush driven by one \
         key's verdict — invisible on the instance that loses the data and fleet-wide \
         through shared storage."
    );
}

/// 4. Absent: `false` AND the key's RAM ranges are gone.
///
/// Branch whose failure this reports: the `RamVerifyDiskCheck::Absent` arm and its
/// `invalidate_ram_ranges` call.
///
/// This is a SEPARATE test from the mismatch case, deliberately, and not a
/// parameterised repeat of it. R4.2's own note records that this requirement was
/// first written with only the match/mismatch arms and that collapsing absence into
/// mismatch is what the two-arm framing got wrong: absence and I/O failure need
/// **opposite** treatments, and a table that reaches the same conclusion for
/// "different etag" and "no etag at all" cannot express that. A parameterised case
/// would also pass if `Absent` fell through to the mismatch arm, which is precisely
/// the collapse being guarded against.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_shared_metadata_returns_false_and_invalidates_only_that_key() {
    let subject = "verify-bucket/absent-subject.bin";
    let control = "verify-bucket/absent-control.bin";
    let (cm, _dir) = manager(Duration::ZERO);
    seed_resident(&cm, subject, ETAG);
    seed_resident(&cm, control, ETAG);

    let hit = resident_hit(&cm, subject).expect("resident after seed_resident");
    let verified = cm
        .ram_verify(subject, &hit, |_k: String| async move {
            RamVerifyDiskCheck::Absent
        })
        .await;

    assert!(
        !verified,
        "R4.2c: absence is not a match. Another instance may have deleted the object, \
         and serving RAM bytes for an object with no shared entry is the same defect in \
         a different shape. Branch: the `Absent` arm."
    );
    assert!(
        resident_hit(&cm, subject).is_none(),
        "R4.2c: an absent shared `.meta` must invalidate the key's RAM ranges. Branch: \
         the invalidate_ram_ranges call in the `Absent` arm. Still resident means a \
         deleted object keeps being served from this instance's memory."
    );
    assert!(
        resident_hit(&cm, control).is_some(),
        "R4.2c CONTROL: invalidation must be scoped to this cache_key. This is the \
         sharpest of the four arms for over-reach: if `Absent` carried `IoError`'s \
         breadth, or invalidated more than its key, a transient shared-storage lookup \
         failure would flush the fleet's RAM tier."
    );
}

/// 5. I/O error: fails OPEN — `true`, ranges still present, timestamp UNCHANGED.
///
/// Branch whose failure this reports: the `RamVerifyDiskCheck::IoError` arm, which
/// must return `true` and call NEITHER `invalidate_ram_ranges` NOR
/// `ram_touch_verified`.
///
/// The timestamp half is the clause's explicit "SHALL NOT be updated, so the next
/// request retries", and it is the only assertion that distinguishes
/// fail-open-**and-retry** from fail-open-**and-forget**. A body that touched the
/// timestamp on an I/O error would pass the first two assertions while converting a
/// failed check into a successful one for a whole interval.
///
/// The elapsed-time precondition below is what stops "unchanged" being vacuous: if
/// no measurable time had passed since promotion, a touched timestamp would be
/// numerically identical to an untouched one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn io_error_fails_open_without_invalidating_or_refreshing() {
    let key = "verify-bucket/io-error.bin";
    let (cm, _dir) = manager(Duration::ZERO);
    seed_resident(&cm, key, ETAG);

    let hit = resident_hit(&cm, key).expect("resident after seed_resident");
    let before_ms = hit.verified_at_ms;

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        now_ms() > before_ms,
        "PRECONDITION MISSING: no measurable time has passed since promotion, so an \
         UNCHANGED timestamp is indistinguishable from a refreshed one and the \
         retry half of this assertion would be vacuous"
    );

    let verified = cm
        .ram_verify(key, &hit, |_k: String| async move {
            RamVerifyDiskCheck::IoError
        })
        .await;

    assert!(
        verified,
        "R4.2d: a transient shared-storage read failure is not evidence of staleness, \
         so this arm must fail OPEN. Branch: the `IoError` arm's `true`."
    );
    let after = resident_hit(&cm, key).expect(
        "R4.2d: an I/O error must NOT invalidate the key's RAM ranges. Invalidating \
         here converts an NFS blip into a fleet-wide cache flush — and because the \
         effect is on shared storage, the instance that made the decision never \
         learns it did.",
    );
    assert_eq!(
        after.verified_at_ms, before_ms,
        "R4.2d: the verification timestamp must NOT be updated on an I/O error, so the \
         next request retries the read rather than treating the failure as a successful \
         check. Branch: the absence of a ram_touch_verified call in the `IoError` arm."
    );
}

/// 6. A zero interval verifies EVERY hit (R4.6).
///
/// Branch whose failure this reports: the `else` side of
/// `if state.last_checked.elapsed() < self.ram_cache_verification_interval` — with a
/// zero interval the comparison is false for every possible elapsed time, so the
/// read must run on both calls. This is the configured opt-out from the read-free
/// path, and a single read across two hits would mean setting the field to `0`
/// silently leaves one hit unverified.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_interval_verifies_on_every_hit() {
    let key = "verify-bucket/zero-interval.bin";
    let (cm, _dir) = manager(Duration::ZERO);
    seed_resident(&cm, key, ETAG);

    let hit = resident_hit(&cm, key).expect("resident after seed_resident");
    let reads = Arc::new(AtomicUsize::new(0));

    for call in 1..=2 {
        let r = reads.clone();
        let verified = cm
            .ram_verify(key, &hit, move |_k: String| async move {
                r.fetch_add(1, Ordering::SeqCst);
                RamVerifyDiskCheck::Found(ETAG.to_string())
            })
            .await;
        assert!(
            verified,
            "R4.6: call {call} returned false against a matching etag, so the failure \
             below would not be attributable to the read count"
        );
    }

    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "R4.6: with cache.ram_cache_verification_interval = 0, EVERY RAM hit must \
         verify. Two hits produced fewer than two reads, so the zero opt-out leaves \
         some hits served unverified."
    );
}

/// The coalescing property (R4.7), with its control.
///
/// `N` concurrent `ram_verify` calls for the SAME `cache_key`, past the interval,
/// must perform **exactly one** `.meta` read between them. Branch whose failure this
/// reports: the `state.last_checked.elapsed() < self.ram_cache_verification_interval`
/// reuse branch that returns `state.last_result.clone()` instead of calling
/// `disk_read`.
///
/// **Control, and it runs in the same test on the same apparatus**: the same `N`
/// over `N` DISTINCT `cache_key`s must produce exactly `N` reads. A `1` on its own
/// is equally produced by a verification path that never fires at all; the
/// distinct-key arm is the same closure, runtime and manager shown able to move the
/// counter. It is asserted FIRST so a dead apparatus is reported as a dead
/// apparatus rather than as coalescing.
///
/// The injected read sleeps, deliberately. `RamVerifyState::last_checked` is an
/// `Instant` compared against the interval, so a closure that returns instantly can
/// let a later caller observe an already-elapsed state and read again *legitimately*
/// — the window has to be real for the property to be under test. The interval (2s)
/// is two orders of magnitude above that sleep (50ms), which is the margin that
/// keeps the reuse branch reachable under parallel suite load.
///
/// Why it matters beyond coverage: R4.11's 1s default is justified in the spec
/// **entirely** on this property — one `.meta` read per second no matter the request
/// rate. Losing it converts the default from per-key-per-second to per-request on
/// hot keys, and nothing in `/metrics` would show it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_verifications_coalesce_per_key() {
    const INTERVAL: Duration = Duration::from_secs(2);
    const N: usize = 8;
    const READ_DELAY: Duration = Duration::from_millis(50);
    let same_key = "verify-bucket/coalesce-same.bin";

    let (cm, _dir) = manager(INTERVAL);
    seed_resident(&cm, same_key, ETAG);
    let distinct_keys: Vec<String> = (0..N)
        .map(|i| format!("verify-bucket/coalesce-distinct-{i}.bin"))
        .collect();
    for key in &distinct_keys {
        seed_resident(&cm, key, ETAG);
    }

    // One sleep covers both arms: `verified_at_ms` is set at promotion time for every
    // key seeded above, and nothing below touches the distinct keys' timestamps.
    tokio::time::sleep(INTERVAL + Duration::from_millis(250)).await;

    let same_hit = resident_hit(&cm, same_key).expect("resident after seed_resident");
    let age_ms = now_ms().saturating_sub(same_hit.verified_at_ms);
    assert!(
        age_ms >= INTERVAL.as_millis() as u64,
        "PRECONDITION MISSING: the entry's age is {age_ms}ms against a {}ms interval, \
         so ram_verify would take its read-free early return and BOTH arms below would \
         report zero reads — which the same-key assertion would read as coalescing",
        INTERVAL.as_millis()
    );

    // --- Same key: expect exactly 1 read across N concurrent callers -------------
    let same_reads = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let cm = cm.clone();
        let hit = same_hit.clone();
        let r = same_reads.clone();
        handles.push(tokio::spawn(async move {
            cm.ram_verify(same_key, &hit, move |_k: String| async move {
                r.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(READ_DELAY).await;
                RamVerifyDiskCheck::Found(ETAG.to_string())
            })
            .await
        }));
    }
    for h in handles {
        assert!(
            h.await.expect("same-key verification task panicked"),
            "R4.7: a coalesced verification against a matching etag must still verify \
             the entry — every caller sharing the one read gets that read's result"
        );
    }

    // --- Distinct keys: expect exactly N reads (the control) ---------------------
    let distinct_reads = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(N);
    for key in distinct_keys {
        let cm = cm.clone();
        let r = distinct_reads.clone();
        let hit = resident_hit(&cm, &key)
            .unwrap_or_else(|| panic!("PRECONDITION MISSING: {key} not resident"));
        handles.push(tokio::spawn(async move {
            cm.ram_verify(&key, &hit, move |_k: String| async move {
                r.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(READ_DELAY).await;
                RamVerifyDiskCheck::Found(ETAG.to_string())
            })
            .await
        }));
    }
    for h in handles {
        assert!(
            h.await.expect("distinct-key verification task panicked"),
            "R4.7 CONTROL: a distinct-key verification against a matching etag must \
             verify the entry"
        );
    }

    let distinct = distinct_reads.load(Ordering::SeqCst);
    let same = same_reads.load(Ordering::SeqCst);
    assert_eq!(
        distinct, N,
        "R4.7 CONTROL FAILED, so the same-key figure below means nothing: {N} \
         verifications over {N} DISTINCT keys produced {distinct} reads, not {N}. The \
         apparatus cannot move the counter, and a same-key reading of 1 would be \
         indistinguishable from a verification path that never fires."
    );
    assert_eq!(
        same, 1,
        "R4.7: {N} concurrent verifications of ONE cache_key past the interval must \
         perform exactly one shared `.meta` read between them; this run performed \
         {same} (the distinct-key control correctly performed {distinct}). Branch: the \
         `state.last_checked.elapsed() < interval` reuse branch in \
         CacheManager::ram_verify. Without it the cost is per-request rather than \
         per-key-per-interval, which is the property R4.11's 1s default rests on, and \
         nothing in /metrics would show the regression."
    );
}
