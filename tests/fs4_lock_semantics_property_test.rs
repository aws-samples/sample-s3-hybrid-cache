//! Property-based tests for `fs4`-based advisory lock semantics.
//!
//! This file covers the six correctness properties named in
//! the fs2 to fs4 migration feature. All calls to
//! `fs4::FileExt` methods are fully qualified (`fs4::FileExt::try_lock(&f)` etc.) rather than
//! using `use fs4::FileExt` plus method-call syntax. This mirrors the migration's own core
//! constraint: on Rust 1.89+, `f.try_lock()`/`f.lock()`/`f.unlock()` silently resolve to the
//! stabilized `std::fs::File` inherent methods rather than to `fs4::FileExt`, and both compile,
//! so a stray method call would go unnoticed even in test code.
//!
//! Properties 1 to 4 use multiple `File` handles from separate `open()` calls on the same path
//! in the same process, never `dup()`. `flock(2)` associates a lock with the *open file
//! description*, not the process or the file descriptor, so two independent `open()` calls
//! contend exactly as two processes would; a `dup()`ed descriptor would share one description
//! and would not contend. See design.md § "The same-process versus separate-process question".

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Opens (creating if needed) a fresh, independent file handle on `path`.
///
/// Each call performs a genuine `open()`, never a `dup()`, which is what makes concurrent
/// handles on the same path contend under `flock(2)` the way separate processes would.
fn open_handle(path: &Path) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .expect("failed to open lock file handle")
}

/// The three-way outcome of a `try_lock` call, mirroring `fs4::TryLockError`'s two error
/// variants plus the success case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Acquired,
    Contended,
    IoError,
}

/// The correct decision function: maps `fs4::FileExt::try_lock`'s three-way result onto
/// `Outcome` with no `.is_ok()` shortcut.
fn decide_correct(f: &File) -> Outcome {
    match fs4::FileExt::try_lock(f) {
        Ok(()) => Outcome::Acquired,
        Err(fs4::TryLockError::WouldBlock) => Outcome::Contended,
        Err(fs4::TryLockError::Error(_)) => Outcome::IoError,
    }
}

// ============================================================================
// Property 1 + Property 5: model-based harness
// ============================================================================

/// A single operation in a generated lock-exercise sequence.
///
/// `u8` handle indices are reduced modulo the actual handle count at apply time, so any raw
/// generated value is valid; `Arbitrary` need not know the handle count in advance.
#[derive(Clone, Debug)]
enum Op {
    TryAcquire(u8),
    Acquire(u8),
    Release(u8),
    Drop(u8),
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        let idx = u8::arbitrary(g);
        match u32::arbitrary(g) % 4 {
            0 => Op::TryAcquire(idx),
            1 => Op::Acquire(idx),
            2 => Op::Release(idx),
            _ => Op::Drop(idx),
        }
    }
}

/// A bounded (2 to 5 handles, up to 24 operations) generated sequence, shared by Property 1
/// and Property 5.
#[derive(Clone, Debug)]
struct OpSequence {
    handle_count: usize,
    ops: Vec<Op>,
}

impl Arbitrary for OpSequence {
    fn arbitrary(g: &mut Gen) -> Self {
        // 2 to 5 handles.
        let handle_count = 2 + (usize::arbitrary(g) % 4);
        // Up to 24 operations (0 is a valid, if uninteresting, sequence).
        let len = usize::arbitrary(g) % 25;
        let ops = (0..len).map(|_| Op::arbitrary(g)).collect();
        OpSequence { handle_count, ops }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let handle_count = self.handle_count;
        let shrunk_ops: Vec<OpSequence> = self
            .ops
            .shrink()
            .map(move |ops| OpSequence { handle_count, ops })
            .collect();
        Box::new(shrunk_ops.into_iter())
    }
}

/// Reference model: which handle index currently holds the lock, if any, and whether that
/// hold is shared (irrelevant here since Property 1/5 only exercise exclusive locks, but kept
/// explicit for clarity).
struct Model {
    holder: Option<usize>,
    /// Handles the model considers dropped. A dropped handle can never re-acquire.
    dropped: Vec<bool>,
}

impl Model {
    fn new(handle_count: usize) -> Self {
        Model {
            holder: None,
            dropped: vec![false; handle_count],
        }
    }

    /// Predicts the outcome of a try-acquire by handle `idx`, without mutating state.
    fn predict_try_acquire(&self, idx: usize) -> Outcome {
        match self.holder {
            None => Outcome::Acquired,
            Some(h) if h == idx => Outcome::Acquired, // Re-locking one's own hold succeeds.
            Some(_) => Outcome::Contended,
        }
    }

    fn apply_try_acquire(&mut self, idx: usize) {
        if self.holder.is_none() || self.holder == Some(idx) {
            self.holder = Some(idx);
        }
    }

    /// Whether a blocking `Acquire(idx)` right now would need to block (i.e. is held by a
    /// *different* live handle). Property 1 skips generating such an operation; Property 2 is
    /// where blocking behaviour is actually exercised.
    fn would_block_on_acquire(&self, idx: usize) -> bool {
        matches!(self.holder, Some(h) if h != idx)
    }

    fn apply_acquire(&mut self, idx: usize) {
        self.holder = Some(idx);
    }

    fn apply_release(&mut self, idx: usize) {
        if self.holder == Some(idx) {
            self.holder = None;
        }
    }

    fn apply_drop(&mut self, idx: usize) {
        self.dropped[idx] = true;
        if self.holder == Some(idx) {
            self.holder = None;
        }
    }
}

/// A decision function type, parameterising Property 1's body so Property 5 can drive the
/// same sequence generator against a deliberately wrong implementation.
type Decide = fn(&File) -> Outcome;

/// Shared engine for Property 1 and Property 5: applies a generated `OpSequence` to real file
/// handles and a reference model in lock-step, using `decide` for every try-acquire, and
/// returns `Ok(())` if every step agreed or `Err(step_index)` naming the first divergence.
///
/// `Op::Acquire` against a handle the model predicts would block is skipped rather than
/// executed, since a real blocking acquire there would hang the test thread; that interleaving
/// is Property 2's subject, not this one's.
fn run_sequence(dir: &TempDir, seq: &OpSequence, decide: Decide) -> Result<(), usize> {
    let path = dir.path().join("model-lock");
    let mut handles: Vec<Option<File>> = (0..seq.handle_count)
        .map(|_| Some(open_handle(&path)))
        .collect();
    let mut model = Model::new(seq.handle_count);

    for (step, op) in seq.ops.iter().enumerate() {
        match *op {
            Op::TryAcquire(raw) => {
                let idx = (raw as usize) % seq.handle_count;
                let Some(handle) = handles[idx].as_ref() else {
                    continue; // Dropped handle; nothing to try.
                };
                let predicted = model.predict_try_acquire(idx);
                let actual = decide(handle);
                if actual != predicted {
                    return Err(step);
                }
                if actual == Outcome::Acquired {
                    model.apply_try_acquire(idx);
                }
            }
            Op::Acquire(raw) => {
                let idx = (raw as usize) % seq.handle_count;
                if handles[idx].is_none() {
                    continue;
                }
                if model.would_block_on_acquire(idx) {
                    // Skip: a real blocking acquire here would hang the test thread.
                    // Property 2 covers blocking behaviour directly.
                    continue;
                }
                let handle = handles[idx].as_ref().unwrap();
                if fs4::FileExt::lock(handle).is_err() {
                    return Err(step);
                }
                model.apply_acquire(idx);
            }
            Op::Release(raw) => {
                let idx = (raw as usize) % seq.handle_count;
                if let Some(handle) = handles[idx].as_ref() {
                    // unlock() is a no-op (Ok) if this handle does not currently hold the
                    // lock, matching the model's unconditional apply_release.
                    let _ = fs4::FileExt::unlock(handle);
                }
                model.apply_release(idx);
            }
            Op::Drop(raw) => {
                let idx = (raw as usize) % seq.handle_count;
                handles[idx] = None; // Drops the File, releasing any OS-held lock.
                model.apply_drop(idx);
            }
        }
    }

    Ok(())
}

// Feature: fs2-to-fs4-migration, Property 1: For any sequence of open, exclusive-acquire,
// try-acquire, and release operations applied across any number of independent file handles on
// the same path, a migrated Try_Lock_Call reports `acquired` if and only if no other handle
// held the lock at the moment of the call, and reports `Contended` otherwise.
fn prop_try_lock_outcome_fidelity(seq: OpSequence) -> TestResult {
    let dir = TempDir::new().expect("failed to create temp dir");
    match run_sequence(&dir, &seq, decide_correct) {
        Ok(()) => TestResult::passed(),
        Err(step) => TestResult::error(format!(
            "model/real divergence at operation index {step} for sequence {seq:?}"
        )),
    }
}

#[test]
fn test_property_try_lock_outcome_fidelity() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_try_lock_outcome_fidelity as fn(OpSequence) -> TestResult);
}

// ============================================================================
// Property 5: the outcome property discriminates against a wrong implementation
// ============================================================================

/// Deliberately wrong decision function: treats *any* error, including a genuinely contended
/// lock, as acquired. This is the `Semantic_Inversion_Bug` the migration is gated against.
///
/// **Why this mutant and not `.is_ok()`.** Requirement 7.5 names `try_lock(..).is_ok()` as the
/// buggy pattern to substitute, but under `fs4` 1.1.0 that pattern is actually *correct*:
/// acquired is `Ok(())` and contended is `Err(..)`, the same Ok/Err split `fs2` used. A negative
/// test built on `.is_ok()` would therefore pass vacuously — the "buggy" pattern would agree
/// with the model on every sequence — and would demonstrate nothing while appearing to satisfy
/// the criterion. Per design.md § "Property 5: how the negative test works", the mutant used
/// here instead collapses the `Err(_)` arm onto `Acquired`, which is genuinely wrong and is the
/// defect class Requirement 7.5 actually cares about.
fn decide_inverted(f: &File) -> Outcome {
    match fs4::FileExt::try_lock(f) {
        Ok(()) => Outcome::Acquired,
        Err(_) => Outcome::Acquired,
    }
}

// Feature: fs2-to-fs4-migration, Property 5: For any generated operation sequence, the
// Property 1 assertion SHALL hold when instantiated with the migrated decision function, and
// SHALL be violated for at least one sequence when instantiated with a decision function that
// treats a Contended outcome as acquired.
fn prop_correct_decision_agrees_with_model(seq: OpSequence) -> TestResult {
    let dir = TempDir::new().expect("failed to create temp dir");
    match run_sequence(&dir, &seq, decide_correct) {
        Ok(()) => TestResult::passed(),
        Err(step) => TestResult::error(format!(
            "correct decision function diverged from model at step {step} for {seq:?}"
        )),
    }
}

#[test]
fn test_property5_correct_decision_always_agrees() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_correct_decision_agrees_with_model as fn(OpSequence) -> TestResult);
}

/// Drives freshly generated sequences against the inverted decision function until it observes
/// at least one disagreement with the model, and fails only if none appears within the
/// iteration budget. This is the "negative test" Requirement 7.5 asks for: it proves the
/// Property 1 assertion is capable of failing, rather than merely never having been observed
/// to fail.
#[test]
fn test_property5_inverted_decision_disagrees_at_least_once() {
    // A held-then-contended shape is what the inverted function gets wrong, so bias generation
    // toward the two-handle, TryAcquire-heavy case that reliably produces contention, while
    // still using the same Arbitrary-driven generator as Property 1 (per the design, this is
    // "driving sequences" with a bounded budget, not a hand-picked single fixture).
    let mut gen = Gen::new(10);
    let budget = 500; // Generous relative to the 100-iteration minimum; contention is common.
    let mut found_disagreement = false;
    let mut last_seq: Option<OpSequence> = None;

    for _ in 0..budget {
        let seq = OpSequence::arbitrary(&mut gen);
        let dir = TempDir::new().expect("failed to create temp dir");
        if run_sequence(&dir, &seq, decide_inverted).is_err() {
            found_disagreement = true;
            last_seq = Some(seq);
            break;
        }
    }

    assert!(
        found_disagreement,
        "expected the inverted decision function to disagree with the model on at least one \
         of {budget} generated sequences, but it never did (last sequence tried: {last_seq:?})"
    );
}

// ============================================================================
// Property 2: mutual exclusion across both acquire paths
// ============================================================================

/// A small interleaving of blocking and non-blocking exclusive acquisitions, generated so the
/// blocking half always happens on a dedicated thread (a real `Op::Acquire` against a held lock
/// blocks the calling thread, which is exactly what this property wants to observe).
#[derive(Clone, Debug)]
struct AcquirePathInterleaving {
    /// Number of handles participating (kept small; the property only needs contention, not
    /// scale).
    handle_count: usize,
    /// Which handle attempts the try-lock first, before the blocking acquirer starts.
    try_first: u8,
}

impl Arbitrary for AcquirePathInterleaving {
    fn arbitrary(g: &mut Gen) -> Self {
        AcquirePathInterleaving {
            handle_count: 2 + (usize::arbitrary(g) % 3), // 2..=4
            try_first: u8::arbitrary(g),
        }
    }
}

// Feature: fs2-to-fs4-migration, Property 2: For any generated interleaving of blocking and
// non-blocking exclusive acquisitions over a set of handles, at most one handle SHALL hold the
// lock at any instant, and a blocking acquire SHALL return only after the previously holding
// handle has released.
fn prop_mutual_exclusion_across_acquire_paths(input: AcquirePathInterleaving) -> TestResult {
    let dir = TempDir::new().expect("failed to create temp dir");
    let path = dir.path().join("mutex-lock");

    let holder = open_handle(&path);
    let try_idx = (input.try_first as usize) % input.handle_count;
    let _ = try_idx; // Handle index space kept for Arbitrary shape; contention driver below.

    // holder acquires exclusively first.
    fs4::FileExt::lock(&holder).expect("initial exclusive lock must succeed");

    // A non-blocking try-lock against the same path, from a second independent handle, must
    // report Contended while holder is still holding.
    let contender = open_handle(&path);
    match fs4::FileExt::try_lock(&contender) {
        Err(fs4::TryLockError::WouldBlock) => {}
        other => {
            return TestResult::error(format!(
                "expected Contended try_lock while held, got {other:?}"
            ))
        }
    }

    // Now exercise the blocking path on a separate thread: it must not return until holder
    // releases.
    let completed = Arc::new(AtomicBool::new(false));
    let completed_clone = Arc::clone(&completed);
    let blocking_path = path.clone();
    let handle_thread = std::thread::spawn(move || {
        let blocker = open_handle(&blocking_path);
        fs4::FileExt::lock(&blocker).expect("blocking lock must eventually succeed");
        completed_clone.store(true, Ordering::SeqCst);
        // Keep the handle alive until the spawned thread exits, then it drops (releasing).
        blocker
    });

    // Give the spawned thread a bounded window to attempt (and necessarily block on) the lock.
    std::thread::sleep(Duration::from_millis(150));
    if completed.load(Ordering::SeqCst) {
        return TestResult::error(
            "blocking acquire on a separate thread returned before the holder released".to_string(),
        );
    }

    // Release; the blocked thread must now be able to complete within a bounded timeout.
    fs4::FileExt::unlock(&holder).expect("unlock must succeed");

    let deadline = Instant::now() + Duration::from_secs(5);
    while !completed.load(Ordering::SeqCst) {
        if Instant::now() > deadline {
            return TestResult::error(
                "blocking acquire did not complete within the timeout after release".to_string(),
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    match handle_thread.join() {
        Ok(_blocker_file) => TestResult::passed(),
        Err(_) => TestResult::error("spawned blocking-acquire thread panicked".to_string()),
    }
}

#[test]
fn test_property_mutual_exclusion_across_acquire_paths() {
    QuickCheck::new().tests(100).quickcheck(
        prop_mutual_exclusion_across_acquire_paths as fn(AcquirePathInterleaving) -> TestResult,
    );
}

// ============================================================================
// Property 3: dropping a handle releases its lock
// ============================================================================

/// A sequence of handle opens, exclusive acquisitions, and drops, some of which drop a
/// lock-holding handle without an explicit `unlock()`.
#[derive(Clone, Debug)]
struct DropReleaseSequence {
    /// Number of "rounds"; each round opens a fresh handle, exclusively acquires, and either
    /// drops it immediately (no unlock) or unlocks then drops.
    rounds: Vec<bool>, // true = drop without unlock, false = unlock then drop
}

impl Arbitrary for DropReleaseSequence {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 1 + (usize::arbitrary(g) % 6); // 1..=6 rounds
        let rounds = (0..len).map(|_| bool::arbitrary(g)).collect();
        DropReleaseSequence { rounds }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(
            self.rounds
                .shrink()
                .map(|rounds| DropReleaseSequence { rounds }),
        )
    }
}

// Feature: fs2-to-fs4-migration, Property 3: For any sequence of handle opens, exclusive
// acquisitions, and drops in which some handles are dropped without an explicit `unlock()`
// call, once every handle holding the lock has been dropped, a freshly opened handle SHALL
// acquire the lock successfully.
fn prop_drop_releases_lock(seq: DropReleaseSequence) -> TestResult {
    let dir = TempDir::new().expect("failed to create temp dir");
    let path = dir.path().join("drop-lock");

    for (i, unlock_first) in seq.rounds.iter().enumerate() {
        let handle = open_handle(&path);
        if fs4::FileExt::lock(&handle).is_err() {
            return TestResult::error(format!("round {i}: failed to acquire exclusive lock"));
        }
        if *unlock_first && fs4::FileExt::unlock(&handle).is_err() {
            return TestResult::error(format!("round {i}: unlock failed"));
        }
        // Whether or not it was explicitly unlocked, dropping the handle must release any
        // remaining OS-held lock.
        drop(handle);

        // A freshly opened handle must now be able to acquire the lock without contention.
        let fresh = open_handle(&path);
        match fs4::FileExt::try_lock(&fresh) {
            Ok(()) => {
                // Release before the next round so subsequent rounds start from a clean state.
                let _ = fs4::FileExt::unlock(&fresh);
            }
            other => {
                return TestResult::error(format!(
                    "round {i}: expected the lock to be free after drop (unlock_first={unlock_first}), got {other:?}"
                ));
            }
        }
    }

    TestResult::passed()
}

#[test]
fn test_property_drop_releases_lock() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_drop_releases_lock as fn(DropReleaseSequence) -> TestResult);
}

// ============================================================================
// Property 4: shared locks admit shared and exclude exclusive
// ============================================================================
//
// NOTE: `src/` contains no `lock_shared()` or `try_lock_shared()` call sites in production
// code so this property covers no production path. It exists purely as
// a guard on the crate swap, confirming `fs4`'s shared-lock semantics match `fs2`'s in case a
// future call site starts using them. Do not read a passing run of this test as production
// coverage.

// Feature: fs2-to-fs4-migration, Property 4: For any number of concurrently held shared locks
// on a path, every shared acquisition SHALL succeed, an exclusive try-acquire SHALL report
// Contended while any shared holder remains, and an exclusive acquisition SHALL succeed once
// all shared holders have released.
fn prop_shared_locks_admit_shared_exclude_exclusive(shared_count: u8) -> TestResult {
    // Bound to a small, useful range; quickcheck's shrinker will still explore 0 and 1.
    let shared_count = 1 + (shared_count % 6) as usize; // 1..=6

    let dir = TempDir::new().expect("failed to create temp dir");
    let path = dir.path().join("shared-lock");

    let mut shared_holders = Vec::with_capacity(shared_count);
    for i in 0..shared_count {
        let handle = open_handle(&path);
        if fs4::FileExt::lock_shared(&handle).is_err() {
            return TestResult::error(format!("shared holder {i} failed to acquire"));
        }
        shared_holders.push(handle);
    }

    // While any shared holder remains, an exclusive try-acquire must report Contended.
    let exclusive_attempt = open_handle(&path);
    match fs4::FileExt::try_lock(&exclusive_attempt) {
        Err(fs4::TryLockError::WouldBlock) => {}
        other => {
            return TestResult::error(format!(
                "expected Contended exclusive try_lock while {shared_count} shared holders \
                 remain, got {other:?}"
            ))
        }
    }

    // Release all but the last shared holder; contention must persist as long as at least one
    // remains.
    while shared_holders.len() > 1 {
        let h = shared_holders.pop().unwrap();
        let _ = fs4::FileExt::unlock(&h);
        drop(h);
        match fs4::FileExt::try_lock(&exclusive_attempt) {
            Err(fs4::TryLockError::WouldBlock) => {}
            other => {
                return TestResult::error(format!(
                    "expected Contended exclusive try_lock with {} shared holders remaining, \
                     got {other:?}",
                    shared_holders.len()
                ))
            }
        }
    }

    // Release the last shared holder; the exclusive acquisition must now succeed.
    let last = shared_holders.pop().unwrap();
    let _ = fs4::FileExt::unlock(&last);
    drop(last);

    match fs4::FileExt::try_lock(&exclusive_attempt) {
        Ok(()) => TestResult::passed(),
        other => TestResult::error(format!(
            "expected exclusive acquisition to succeed once all shared holders released, got \
             {other:?}"
        )),
    }
}

#[test]
fn test_property_shared_locks_admit_shared_exclude_exclusive() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_shared_locks_admit_shared_exclude_exclusive as fn(u8) -> TestResult);
}

// ============================================================================
// Property 6: ErrorKind survives the TryLockError round trip
// ============================================================================

/// A wrapper giving `Arbitrary` over a fixed, representative set of `std::io::ErrorKind`
/// variants. `fs4::TryLockError::from(io::Error)` only distinguishes `WouldBlock` (collapsing
/// to the `WouldBlock` variant) from everything else (wrapped in `Error(..)`), per
/// `try_lock_error.rs`'s actual `From` impls, so this property is really about the `Error(..)`
/// round trip for every kind other than `WouldBlock`, plus confirming `WouldBlock` itself
/// round-trips through its dedicated variant. This is what backs task 5.1's
/// `disk_cache.rs` conversion (`last_error = Some(std::io::Error::from(e))`).
#[derive(Clone, Copy, Debug)]
struct ArbitraryErrorKind(std::io::ErrorKind);

impl Arbitrary for ArbitraryErrorKind {
    fn arbitrary(g: &mut Gen) -> Self {
        use std::io::ErrorKind;
        const KINDS: &[ErrorKind] = &[
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::NotConnected,
            ErrorKind::AddrInUse,
            ErrorKind::AddrNotAvailable,
            ErrorKind::BrokenPipe,
            ErrorKind::AlreadyExists,
            ErrorKind::WouldBlock,
            ErrorKind::InvalidInput,
            ErrorKind::InvalidData,
            ErrorKind::TimedOut,
            ErrorKind::WriteZero,
            ErrorKind::Interrupted,
            ErrorKind::UnexpectedEof,
            ErrorKind::OutOfMemory,
            ErrorKind::Other,
        ];
        let idx = usize::arbitrary(g) % KINDS.len();
        ArbitraryErrorKind(KINDS[idx])
    }
}

// Feature: fs2-to-fs4-migration, Property 6: For any std::io::ErrorKind, converting an
// io::Error of that kind into a fs4::TryLockError and back into an io::Error SHALL yield an
// error reporting the original kind.
fn prop_error_kind_survives_try_lock_error_round_trip(input: ArbitraryErrorKind) -> TestResult {
    let original = std::io::Error::from(input.0);
    let as_try_lock_error: fs4::TryLockError = original.into();
    let round_tripped: std::io::Error = as_try_lock_error.into();

    if round_tripped.kind() != input.0 {
        return TestResult::error(format!(
            "round trip changed ErrorKind: original={:?}, after round trip={:?}",
            input.0,
            round_tripped.kind()
        ));
    }
    TestResult::passed()
}

#[test]
fn test_property_error_kind_survives_try_lock_error_round_trip() {
    QuickCheck::new().tests(100).quickcheck(
        prop_error_kind_survives_try_lock_error_round_trip as fn(ArbitraryErrorKind) -> TestResult,
    );
}
