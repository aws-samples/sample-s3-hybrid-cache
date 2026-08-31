//! Graded cache-tree fixture generator.
//!
//! Spec: `cache-eviction-at-scale`, task 8. Requirements: 13.1 (and 13.3's
//! degenerate shapes, which are expressed here as named distributions so the
//! extremes tests use the same generator rather than a second one).
//!
//! # What this emits, and why it does not go through the proxy
//!
//! A schema-valid cache tree written directly: sharded `.meta` files holding real
//! [`NewCacheMetadata`], and `.bin` range files at the paths those `.meta` files
//! name. The selection code reads `.meta` and `.bin` and does not care how they
//! arrived, so driving traffic through the proxy would buy nothing and cost orders
//! of magnitude in generation time at the 10M-range scale task 9 needs.
//!
//! Placement is delegated to the product's own
//! [`s3_proxy::disk_cache::get_sharded_path`] rather than reimplemented, so a
//! future change to the sharding scheme moves the fixture with it instead of
//! silently producing a tree the scan cannot walk.
//!
//! # The failure mode this is designed against
//!
//! A fixture that looks right and is not. Two consequences run through the whole
//! file:
//!
//! 1. **Validation is by read-back through product code**, not by this module's own
//!    assertions. See `tests/graded_fixture_generator_test.rs`, which drives
//!    `CacheManager::collect_range_candidates_for_eviction` over a generated tree
//!    and compares what eviction *discovered* against what was written. A generator
//!    whose output only its own parser accepts proves nothing.
//! 2. **The tree is self-describing.** [`generate`] writes a
//!    `FIXTURE_MANIFEST.json` recording counts, the size histogram, per-L1-shard
//!    distribution, tail occupancy at a configurable cap, the payload mode, and the
//!    seed. A measurement task must be able to establish what it *should* see
//!    without re-walking the tree.
//!
//! # Ages are backdated deliberately
//!
//! `CacheManager::collect_candidates_from_metadata_file` applies an unconditional
//! 60-second admission window (`src/cache.rs`, in the `for range_spec in
//! &new_metadata.ranges` loop) and skips any range whose `last_accessed` is inside
//! it. A fixture written with `SystemTime::now()` ages is therefore **invisible to
//! eviction**, and the resulting empty candidate list looks exactly like a
//! selection defect. Every range here is aged into
//! `[FixtureSpec::min_age, FixtureSpec::max_age]`, and [`FixtureSpec::validate`]
//! refuses a `min_age` that does not clear the window.

// The module is shared by an example CLI (`examples/graded_cache_fixture_gen.rs`,
// via `#[path]`) and by `tests/`. Neither uses the whole surface, so `dead_code`
// fires on whichever items the current target does not touch. This is a deliberate
// public-API surface for fixture tooling, not unreachable code.
#![allow(dead_code)]

use s3_proxy::cache_types::{
    CompressionInfo, NewCacheMetadata, ObjectMetadata, RangeSpec, UploadState,
};
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::disk_cache::get_sharded_path;
use std::collections::{BTreeMap, BinaryHeap};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Marker file written at the fixture root.
///
/// Two jobs: it makes a fixture tree distinguishable from a real cache by
/// inspection, and it is the token [`generate`] looks for when deciding whether an
/// existing non-empty output directory is safe to overwrite.
pub const FIXTURE_MARKER: &str = ".s3hc-fixture";

/// Manifest filename at the fixture root.
pub const MANIFEST_FILE: &str = "FIXTURE_MANIFEST.json";

/// Key-space prefix every generated object carries, immediately under the bucket.
///
/// Scoping, in the sense `pre-push-checklist.md` requires of the fleet tooling: a
/// generated key is self-identifying, so a stale fixture artefact cannot be
/// mistaken for live cache content by anything that reads the key.
pub const FIXTURE_KEY_PREFIX: &str = "s3hc-fixture";

/// Path fragments [`generate`] refuses to write beneath by default. See
/// [`guard_output_dir`].
///
/// This is a **convention** heuristic: these fragments usually mean a shared cache
/// mount. It is overridable via [`GenOptions::shared_storage_ack`], because task 9's
/// artefact has to live on the real shared backend — `readdir` at occupancy is a
/// backend-specific property and a local-APFS answer does not transfer. The guards
/// that are *not* overridable are the ones below: [`NEVER_GENERATE_UNDER`] and the
/// live-cache content tell-tales.
pub const SHARED_STORAGE_PATH_FRAGMENTS: &[&str] = &["/mnt/", "/efs/", "cache-bench"];

/// Paths that are refused unconditionally, acknowledgement or not.
///
/// These are the fleet's actual cache directories on the shared volume. Generating
/// into `/mnt/efs/cache-bench` would corrupt the live fleet cache, which is the
/// accident [`SHARED_STORAGE_PATH_FRAGMENTS`] exists to prevent — so relaxing that
/// heuristic must not relax this. `/mnt/efs/cache` is included because it also holds
/// cache-shaped state (a `cache_rules.json`) even though the running proxies do not
/// read it, and a fixture there would be indistinguishable from cache content to
/// anyone later inspecting the volume.
///
/// A fragment match here is on a **path prefix at a component boundary**, not a
/// substring, so a sibling like `/mnt/efs/cache-fixtures` is unaffected while
/// `/mnt/efs/cache-bench/anything` is refused.
pub const NEVER_GENERATE_UNDER: &[&str] = &["/mnt/efs/cache-bench", "/mnt/efs/cache"];

/// The default tail cap the manifest reports occupancy against.
///
/// This is `design.md` § 4.2's per-shard `tail_max_entries`, and § 12 row 1 is the
/// question it exists to answer. Reported rather than enforced — the generator has
/// no opinion on the cap, it just measures the tree against it.
pub const DEFAULT_TAIL_CAP: usize = 4096;

/// One rung of the graded size ladder. Sizes are drawn log-uniformly within it.
#[derive(Debug, Clone, PartialEq)]
pub struct SizeClass {
    pub name: &'static str,
    /// Inclusive lower bound, bytes.
    pub min_bytes: u64,
    /// Inclusive upper bound, bytes.
    pub max_bytes: u64,
    /// Share of the total range count. Shares are normalised, so they need not sum
    /// to exactly 1.0.
    pub share: f64,
}

/// The size distribution a fixture draws from.
#[derive(Debug, Clone, PartialEq)]
pub enum SizeDistribution {
    /// The R13.1 shape: a graded ladder with a populated tail.
    Graded {
        classes: Vec<SizeClass>,
        /// Clamp every drawn size to at most this. Lets a unit test exercise the
        /// full class ladder without writing a 500 MiB payload; `None` in
        /// production use.
        size_cap: Option<u64>,
    },
    /// R13.3's uniform-size extreme, where the tail is empty by construction.
    Uniform { size: u64 },
    /// R13.3's single-dominant-range extreme.
    SingleDominant { background: u64, dominant: u64 },
}

impl SizeDistribution {
    /// The R13.1 ladder: 1 KiB to 500 MiB, six rungs, heavy-tailed.
    ///
    /// # Why a piecewise mixture rather than a lognormal
    ///
    /// R13.1 requires a **populated** tail, and § 12 row 1's question is about the
    /// bytes held by the largest `tail_max_entries` per shard. A lognormal's tail
    /// occupancy at a given N is an emergent property — it can come out empty, and
    /// whether it does depends on parameters nobody can read off the requirement.
    /// Explicit class shares make tail population a stated input, and the manifest
    /// reports what was actually achieved so the claim is checkable rather than
    /// assumed.
    ///
    /// The 2% `huge` share is set by task 10 rather than chosen freely: at its
    /// ~100K ranges it yields ~2,000 objects above 64 MiB, which is the "a few
    /// thousand large objects" that task asks for. The same shares at task 9's
    /// ~10M ranges give ~200K, which spread over 256 L1 shards is ~780 per shard —
    /// deliberately below `DEFAULT_TAIL_CAP`, so the cap binds on a mixture of the
    /// top three rungs rather than on `huge` alone. That is § 12 row 1's actual
    /// question, not a convenience.
    pub fn graded() -> Self {
        SizeDistribution::Graded {
            classes: vec![
                SizeClass {
                    name: "tiny",
                    min_bytes: 1024,
                    max_bytes: 8 * 1024,
                    share: 0.40,
                },
                SizeClass {
                    name: "small",
                    min_bytes: 8 * 1024,
                    max_bytes: 64 * 1024,
                    share: 0.25,
                },
                SizeClass {
                    name: "medium",
                    min_bytes: 64 * 1024,
                    max_bytes: 1024 * 1024,
                    share: 0.18,
                },
                SizeClass {
                    name: "large",
                    min_bytes: 1024 * 1024,
                    max_bytes: 8 * 1024 * 1024,
                    share: 0.10,
                },
                SizeClass {
                    name: "xlarge",
                    min_bytes: 8 * 1024 * 1024,
                    max_bytes: 64 * 1024 * 1024,
                    share: 0.05,
                },
                SizeClass {
                    name: "huge",
                    min_bytes: 64 * 1024 * 1024,
                    max_bytes: 500 * 1024 * 1024,
                    share: 0.02,
                },
            ],
            size_cap: None,
        }
    }

    /// The span this distribution covers, as (min, max) bytes. Used by a test to
    /// assert the ladder reaches R13.1's 1 KB and 500 MB bounds without generating
    /// a 500 MiB payload.
    pub fn span(&self) -> (u64, u64) {
        match self {
            SizeDistribution::Graded { classes, size_cap } => {
                let lo = classes.iter().map(|c| c.min_bytes).min().unwrap_or(0);
                let hi = classes.iter().map(|c| c.max_bytes).max().unwrap_or(0);
                (lo, size_cap.map(|c| hi.min(c)).unwrap_or(hi))
            }
            SizeDistribution::Uniform { size } => (*size, *size),
            SizeDistribution::SingleDominant {
                background,
                dominant,
            } => (*background.min(dominant), *background.max(dominant)),
        }
    }
}

/// Whether a `.bin` file's length equals the size recorded for it in `.meta`.
///
/// # READ THIS BEFORE "FIXING" A SIZE DISCREPANCY
///
/// [`PayloadMode::RecordedSizeOnly`] makes the two **deliberately disagree**, and
/// that disagreement is the whole point of task 9's Fixture A. Do not reconcile
/// them. Do not make the payload match the recorded size "for consistency". Doing
/// either turns an 81 GB fixture into an order-2 PB one and destroys the artefact.
///
/// The reason the modes exist: count-scale and byte-scale are separate questions.
/// Task 9 needs ~10M ranges to measure selection cost, per-shard statistics and
/// `readdir` at occupancy — none of which read a payload byte. Task 10 needs
/// genuine payloads to measure filesystem allocation against recorded
/// `compressed_size`, and per-size-class deletion throughput. Coupling the two
/// would make the first artefact unbuildable.
///
/// The manifest records the mode, `recorded_size_is_deliberately_not_on_disk_size`,
/// and a prose `why`, so the reason travels with the artefact rather than only
/// living here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadMode {
    /// `.bin` length == recorded `compressed_size`. Task 10's Fixture B.
    ///
    /// A real range read over this tree would succeed: the recorded compression
    /// algorithm is `None`, so the bytes are the content.
    Genuine,
    /// `.bin` is a fixed small stub while `.meta` records the drawn realistic size.
    /// Task 9's Fixture A.
    ///
    /// # Consequence for any byte-valued measurement
    ///
    /// `collect_candidates_from_metadata_file` stats the `.bin` and puts the
    /// **actual file length** in `RangeEvictionCandidate::size`, falling back to
    /// `compressed_size` only when the stat fails. So on a `RecordedSizeOnly` tree
    /// the candidate's `size` carries the stub and its `compressed_size` carries
    /// the realistic figure, and anything that sums `size` to reach a byte target
    /// will measure the stubs. Use `compressed_size`, or use a `Genuine` tree.
    /// `tests/graded_fixture_generator_test.rs` pins this asymmetry so it cannot be
    /// "tidied" away unnoticed.
    RecordedSizeOnly { stub_bytes: usize },
}

impl PayloadMode {
    pub fn label(&self) -> &'static str {
        match self {
            PayloadMode::Genuine => "genuine",
            PayloadMode::RecordedSizeOnly { .. } => "recorded_size_only",
        }
    }
}

/// Everything a fixture generation is determined by.
///
/// Determinism is a hard requirement (task 11's show-red has to regenerate the same
/// input, and a measurement that cannot be repeated on the same input is not
/// evidence), so this struct plus the code in this file is the complete
/// specification of the output. No wall-clock input reaches a drawn value; the only
/// place `SystemTime::now()` appears is the age *base*, and ages are stored as
/// offsets from it, which is why the manifest digest is stable while the absolute
/// timestamps are not.
#[derive(Debug, Clone)]
pub struct FixtureSpec {
    /// Short human label, part of the generation id.
    pub label: String,
    /// The seed. Same seed plus same spec gives the same tree, byte for byte apart
    /// from the age base.
    pub seed: u64,
    /// Bucket component of every cache key, and therefore the top-level directory
    /// under `metadata/` and `ranges/`.
    pub bucket: String,
    /// Number of distinct objects (`.meta` files).
    pub objects: u64,
    /// Each object gets 1..=this many contiguous ranges.
    pub max_ranges_per_object: u32,
    pub distribution: SizeDistribution,
    pub payload: PayloadMode,
    /// Share of objects flagged `is_write_cached` with `staged: Some(true)` on
    /// every range. 0.0 gives a pure read-tier fixture, which is what the eviction
    /// questions want; a non-zero share exercises the tier attribution in
    /// `is_staged_range_spec`.
    pub staged_fraction: f64,
    /// Youngest `last_accessed`. Must exceed the 60s admission window.
    pub min_age: Duration,
    /// Oldest `last_accessed`.
    pub max_age: Duration,
    /// Cap the manifest reports per-shard tail occupancy against.
    pub tail_cap: usize,
}

/// The admission window in `collect_candidates_from_metadata_file`. Mirrored here
/// only so [`FixtureSpec::validate`] can refuse an age that would make the fixture
/// invisible to eviction; the product value is the authority.
pub const EVICTION_ADMISSION_WINDOW: Duration = Duration::from_secs(60);

impl Default for FixtureSpec {
    fn default() -> Self {
        Self {
            label: "graded".to_string(),
            seed: 0x5eed_1234_5678_9abc,
            bucket: "fixture-graded".to_string(),
            objects: 1_000,
            max_ranges_per_object: 4,
            distribution: SizeDistribution::graded(),
            payload: PayloadMode::Genuine,
            staged_fraction: 0.0,
            min_age: Duration::from_secs(3600),
            max_age: Duration::from_secs(30 * 24 * 3600),
            tail_cap: DEFAULT_TAIL_CAP,
        }
    }
}

impl FixtureSpec {
    /// Generation id. Appears in every cache key and in the manifest, so two
    /// generations with different seeds or sizes occupy disjoint key space and
    /// cannot be confused for one another.
    pub fn gen_id(&self) -> String {
        format!("{}-{:016x}-{}", self.label, self.seed, self.objects)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.objects == 0 {
            return Err("objects must be > 0".to_string());
        }
        if self.max_ranges_per_object == 0 {
            return Err("max_ranges_per_object must be > 0".to_string());
        }
        if self.min_age <= EVICTION_ADMISSION_WINDOW {
            return Err(format!(
                "min_age {:?} does not clear the {:?} eviction admission window in \
                 CacheManager::collect_candidates_from_metadata_file; every range would be \
                 skipped as ineligible and the fixture would look like a selection defect",
                self.min_age, EVICTION_ADMISSION_WINDOW
            ));
        }
        if self.max_age < self.min_age {
            return Err("max_age must be >= min_age".to_string());
        }
        if !(0.0..=1.0).contains(&self.staged_fraction) {
            return Err("staged_fraction must be in [0.0, 1.0]".to_string());
        }
        if self.tail_cap == 0 {
            return Err("tail_cap must be > 0".to_string());
        }
        match &self.distribution {
            SizeDistribution::Graded { classes, size_cap } => {
                if classes.is_empty() {
                    return Err("graded distribution needs at least one class".to_string());
                }
                for c in classes {
                    if c.min_bytes == 0 || c.max_bytes < c.min_bytes {
                        return Err(format!("class {} has an invalid byte range", c.name));
                    }
                    if c.share < 0.0 {
                        return Err(format!("class {} has a negative share", c.name));
                    }
                }
                if classes.iter().map(|c| c.share).sum::<f64>() <= 0.0 {
                    return Err("graded class shares sum to zero".to_string());
                }
                if let Some(cap) = size_cap {
                    if *cap == 0 {
                        return Err("size_cap must be > 0 when set".to_string());
                    }
                }
            }
            SizeDistribution::Uniform { size } => {
                if *size == 0 {
                    return Err("uniform size must be > 0".to_string());
                }
            }
            SizeDistribution::SingleDominant {
                background,
                dominant,
            } => {
                if *background == 0 || *dominant == 0 {
                    return Err("single-dominant sizes must be > 0".to_string());
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Deterministic PRNG
// ---------------------------------------------------------------------------

/// SplitMix64. Hand-rolled deliberately.
///
/// Two reasons rather than one. The crate has `fastrand` available, but a
/// third-party generator's output stream is not contractually stable across
/// versions, and a fixture whose contents change when a dependency is bumped
/// defeats the determinism requirement in a way nobody would notice until a
/// re-measurement disagreed. Second, SplitMix64 is seekable: `stream(seed, i)`
/// gives object `i`'s generator without generating objects `0..i`, so the output is
/// index-addressable and a later parallel generator for tasks 9 and 10 produces the
/// identical tree.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The generator for a named sub-stream of `seed`. Mixing the index through the
    /// same finaliser before seeding avoids the correlated first outputs a raw
    /// `seed + i` would give.
    pub fn stream(seed: u64, index: u64) -> Self {
        let mut s = Self::new(seed ^ 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(index.wrapping_add(1)));
        // Discard one output so adjacent indices decorrelate fully.
        s.next_u64();
        s
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in [lo, hi] inclusive.
    pub fn next_range_u64(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.next_u64() % (hi - lo + 1)
    }

    /// Log-uniform in [lo, hi]. This is what makes each rung of the ladder spread
    /// across its decade rather than clustering at the top, which a linear draw
    /// would do.
    pub fn next_log_uniform(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        let l = (lo as f64).ln();
        let h = (hi as f64).ln();
        let v = (l + self.next_f64() * (h - l)).exp();
        (v.round() as u64).clamp(lo, hi)
    }
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

/// Per-size-class rollup.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClassStats {
    pub name: String,
    pub ranges: u64,
    pub recorded_bytes: u64,
    pub on_disk_bytes: u64,
    pub min_recorded: u64,
    pub max_recorded: u64,
}

/// Per-L1-shard rollup, aggregated across buckets, keyed by the two hex digits the
/// sharding scheme derives from BLAKE3 of the object key.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardStats {
    pub shard: String,
    pub ranges: u64,
    pub recorded_bytes: u64,
    /// Ranges counted in `tail_bytes` — `min(tail_cap, ranges)`.
    pub tail_ranges: u64,
    /// Recorded bytes held by this shard's largest `tail_ranges` ranges. § 12
    /// row 1's numerator.
    pub tail_bytes: u64,
}

/// Tail population, the R13.1 property that has to be checked rather than assumed.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TailStats {
    pub ranges_at_least_1mib: u64,
    pub ranges_at_least_8mib: u64,
    pub ranges_at_least_64mib: u64,
    pub bytes_at_least_1mib: u64,
    pub bytes_at_least_8mib: u64,
    pub bytes_at_least_64mib: u64,
    pub largest_recorded: u64,
    /// Share of total recorded bytes held by the union of every shard's tail at
    /// `tail_cap`. This is the figure § 12 row 1's rule is evaluated against.
    pub tail_share_of_recorded_bytes: f64,
}

/// Measured generation throughput. Constraint 4 of task 8: this figure is what
/// sizes the infrastructure decision for tasks 9 and 10, so it is measured and
/// recorded rather than estimated.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ThroughputStats {
    pub elapsed_secs: f64,
    pub ranges_per_sec: f64,
    pub on_disk_bytes_per_sec: f64,
    pub recorded_bytes_per_sec: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FixtureManifest {
    pub schema_version: u32,
    pub generator: String,
    pub generator_crate_version: String,
    pub generated_at_epoch_secs: u64,

    pub label: String,
    pub gen_id: String,
    pub seed: u64,
    pub bucket: String,
    pub key_prefix: String,

    pub distribution: String,
    pub distribution_span_bytes: (u64, u64),
    pub payload_mode: String,
    pub payload_stub_bytes: Option<u64>,
    /// The Fixture-A discrepancy, stated in the artefact.
    pub recorded_size_is_deliberately_not_on_disk_size: bool,
    pub recorded_vs_on_disk_why: String,
    /// Whether a real range read over this tree would return the recorded content.
    /// False for `RecordedSizeOnly`.
    pub serve_path_valid: bool,

    pub objects: u64,
    pub ranges: u64,
    pub staged_objects: u64,
    pub recorded_compressed_bytes: u64,
    pub recorded_uncompressed_bytes: u64,
    pub on_disk_bytes: u64,

    pub min_age_secs: u64,
    pub max_age_secs: u64,
    pub eviction_admission_window_secs: u64,

    pub tail_cap: u64,
    pub classes: Vec<ClassStats>,
    pub shards: Vec<ShardStats>,
    pub tail: TailStats,
    /// Coefficient of variation of per-shard range counts, percent. Task 12 wants
    /// byte uniformity; this is the count figure it will compare against.
    pub shard_count_cv_percent: f64,
    /// Coefficient of variation of per-shard recorded bytes, percent.
    pub shard_bytes_cv_percent: f64,

    pub throughput: ThroughputStats,
    /// Order-independent digest over every emitted range. Same spec plus same seed
    /// gives the same digest, which is how a re-generation is checked rather than
    /// trusted. Order-independent (XOR-folded per-range hashes) so the parallel
    /// generator is verifiable against a serial one — see
    /// `parallel_generation_is_identical_to_serial_generation`.
    pub content_digest: String,

    /// Worker threads the tree was written with. **Not** part of the spec, and it
    /// must not appear in `content_digest`: thread count is an execution detail and
    /// a tree whose contents moved with it would be unverifiable. Recorded so a
    /// throughput figure is attributable.
    #[serde(default = "one")]
    pub generation_threads: u64,
    /// Whether the directory-existence memo was on. Also execution-only, recorded
    /// for the same reason — it changes syscall count, never output.
    #[serde(default)]
    pub dir_memo: bool,
}

fn one() -> u64 {
    1
}

/// How to run a generation, as opposed to what to generate.
///
/// The split from [`FixtureSpec`] is the point. `FixtureSpec` plus the code in this
/// file is the complete specification of the *output*; nothing here may change a
/// single emitted byte. The test that holds that line is
/// `parallel_generation_is_identical_to_serial_generation`.
#[derive(Debug, Clone)]
pub struct GenOptions {
    /// Retain an [`EmittedRange`] per range. Suppressed at scale — 10M entries do
    /// not fit comfortably in memory alongside the generation.
    pub collect_emitted: bool,
    /// Worker threads. 1 drives the same per-object emitter in a plain loop, which
    /// is the serial reference the parallel path is checked against.
    pub threads: usize,
    /// Skip `create_dir_all` for a directory this worker has already created.
    ///
    /// Kept switchable rather than always-on so its contribution to throughput can
    /// be measured separately from the thread count's. Task 8's recorded
    /// single-threaded rates predate it, so a like-for-like comparison against them
    /// needs this off.
    pub dir_memo: bool,

    /// Acknowledge that the output root is deliberately on shared storage.
    ///
    /// Must be **exactly equal** to the `root` passed to [`generate_with`], so the
    /// operator names the destination twice and a broad or stale acknowledgement
    /// cannot cover a different path. Suppresses only the
    /// [`SHARED_STORAGE_PATH_FRAGMENTS`] convention check; [`NEVER_GENERATE_UNDER`],
    /// the live-cache content tell-tales, and the non-empty-unmarked refusal all
    /// still apply.
    pub shared_storage_ack: Option<PathBuf>,
}

impl Default for GenOptions {
    fn default() -> Self {
        Self {
            collect_emitted: false,
            threads: 1,
            dir_memo: true,
            shared_storage_ack: None,
        }
    }
}

impl GenOptions {
    pub fn serial(collect_emitted: bool) -> Self {
        Self {
            collect_emitted,
            ..Self::default()
        }
    }

    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

/// Description of one emitted range, returned so a caller can assert against the
/// tree without re-deriving paths.
#[derive(Debug, Clone)]
pub struct EmittedRange {
    pub cache_key: String,
    pub start: u64,
    pub end: u64,
    pub recorded_compressed_size: u64,
    pub on_disk_size: u64,
    pub relative_bin_path: String,
    pub class: String,
}

#[derive(Debug)]
pub struct GeneratedFixture {
    pub root: PathBuf,
    pub manifest: FixtureManifest,
    /// Populated only when [`generate`] is called with `collect_emitted = true`.
    /// Suppressed at scale, where 10M entries would not fit comfortably in memory
    /// alongside the generation.
    pub emitted: Vec<EmittedRange>,
}

/// Refuse to write into anything that might be a real cache.
///
/// Constraint 5 of task 8, expressed as code rather than as a note. Three guards,
/// in increasing order of how much they would cost to get wrong:
/// a path under a mount point a proxy is likely reading; a directory carrying
/// live-cache tell-tales (`size_tracking/size_state.json`, `metadata/_journals/`);
/// and a non-empty directory with no fixture marker, which is the ambiguous case
/// and so is refused rather than guessed at.
/// `pub` so the guard can be tested for its **verdict** without a generation
/// attempting real I/O. A test that proved "an acknowledged shared path is allowed"
/// by calling [`generate_with`] on `/mnt/...` would, on any host where `/mnt` happens
/// to be writable (a CI container running as root, for one), actually write a fixture
/// there. Testing the decision function directly removes that.
pub fn guard_output_dir(root: &Path, shared_storage_ack: Option<&Path>) -> Result<(), String> {
    let display = root.to_string_lossy().to_string();

    // Unconditional, and first: the fleet's own cache directories. Not overridable,
    // because this is the accident the whole guard exists to prevent, and an
    // acknowledgement flag that could reach it would make the flag the accident.
    let normalised = display.trim_end_matches('/').to_string();
    for never in NEVER_GENERATE_UNDER {
        if normalised == *never || normalised.starts_with(&format!("{never}/")) {
            return Err(format!(
                "refusing to generate into {display}: {never} is a live fleet cache directory. \
                 This refusal is NOT overridable by shared_storage_ack. Pick a sibling path on \
                 the same volume instead."
            ));
        }
    }

    // Path fragments that conventionally denote a shared cache mount (EFS, FSx, NFS)
    // rather than scratch space. Coarse on purpose: a false refusal costs one
    // `--out` flag, whereas generating a few hundred thousand files into a volume a
    // proxy is reading is not something a later check can undo.
    //
    // Overridable, because task 9's count-scale artefact MUST live on the real
    // shared backend — `readdir` at occupancy is backend-specific and a local answer
    // does not transfer. The acknowledgement has to name the same path, so it cannot
    // be set once and then quietly cover a later, different destination.
    let acked = shared_storage_ack
        .is_some_and(|a| a.to_string_lossy().trim_end_matches('/') == normalised.as_str());
    if !acked {
        for forbidden in SHARED_STORAGE_PATH_FRAGMENTS {
            if display.contains(forbidden) {
                return Err(format!(
                    "refusing to generate into {display}: the path contains {forbidden:?}, which \
                     conventionally denotes a shared cache mount a proxy may be reading. Generate \
                     into local scratch space, or — if shared storage is the point, as it is for \
                     the count-scale fixture — acknowledge this exact path via \
                     GenOptions::shared_storage_ack (CLI: --ack-shared-storage {display}). \
                     Confirm the proxy fleet is stopped first."
                ));
            }
        }
    }

    if root.join("size_tracking").join("size_state.json").exists() {
        return Err(format!(
            "refusing to generate into {display}: it contains size_tracking/size_state.json, so it \
             is a real cache directory"
        ));
    }
    if root.join("metadata").join("_journals").is_dir() {
        return Err(format!(
            "refusing to generate into {display}: it contains metadata/_journals/, so it is a real \
             cache directory"
        ));
    }

    if root.exists() {
        let mut entries = match std::fs::read_dir(root) {
            Ok(e) => e,
            Err(e) => return Err(format!("cannot read {display}: {e}")),
        };
        let non_empty = entries.next().is_some();
        if non_empty && !root.join(FIXTURE_MARKER).exists() {
            return Err(format!(
                "refusing to generate into {display}: it is non-empty and carries no {FIXTURE_MARKER} \
                 marker, so it cannot be established as a previous fixture tree"
            ));
        }
    }

    Ok(())
}

/// Pick a class index from normalised shares.
fn pick_class(classes: &[SizeClass], total_share: f64, rng: &mut SplitMix64) -> usize {
    let mut r = rng.next_f64() * total_share;
    for (i, c) in classes.iter().enumerate() {
        if r < c.share {
            return i;
        }
        r -= c.share;
    }
    classes.len() - 1
}

/// Draw one range size, and name the class it came from.
fn draw_size(dist: &SizeDistribution, rng: &mut SplitMix64, is_first_range: bool) -> (u64, String) {
    match dist {
        SizeDistribution::Graded { classes, size_cap } => {
            let total: f64 = classes.iter().map(|c| c.share).sum();
            let idx = pick_class(classes, total, rng);
            let c = &classes[idx];
            let mut size = rng.next_log_uniform(c.min_bytes, c.max_bytes);
            if let Some(cap) = size_cap {
                size = size.min(*cap).max(1);
            }
            (size, c.name.to_string())
        }
        SizeDistribution::Uniform { size } => (*size, "uniform".to_string()),
        SizeDistribution::SingleDominant {
            background,
            dominant,
        } => {
            // The dominant range is deterministically the first range of the first
            // object, so R13.3's single-dominant extreme is a property of the
            // fixture rather than of a lucky draw.
            if is_first_range {
                (*dominant, "dominant".to_string())
            } else {
                (*background, "background".to_string())
            }
        }
    }
}

/// Everything one worker accumulates, and the whole of what has to survive a merge.
///
/// Every field is combined by an **associative and commutative** operation — sum,
/// min, max, XOR, or top-K union — which is what makes the parallel driver's result
/// independent of how objects were partitioned across threads. A field that could
/// not be merged that way would have to be computed after generation from the tree
/// instead; none currently is.
#[derive(Default)]
struct Accum {
    class_stats: BTreeMap<String, ClassStats>,
    shard_ranges: BTreeMap<String, u64>,
    shard_bytes: BTreeMap<String, u64>,
    /// Bounded min-heap per shard: the largest `tail_cap` recorded sizes.
    ///
    /// Merging two bounded top-K heaps is exact, not approximate:
    /// `topK(A ∪ B) == topK(topK(A) ∪ topK(B))`, because anything dropped from a
    /// local heap is already below K local elements and therefore below K elements
    /// of the union. This is why per-shard tail occupancy comes out identical to a
    /// serial run rather than merely close to it.
    shard_tail: BTreeMap<String, BinaryHeap<std::cmp::Reverse<u64>>>,
    total_ranges: u64,
    staged_objects: u64,
    recorded_compressed: u64,
    recorded_uncompressed: u64,
    on_disk: u64,
    tail: TailStats,
    digest: [u8; 32],
    emitted: Vec<EmittedRange>,
    /// Directories this worker has already created. Worker-local, and deliberately
    /// **not** merged — a thread re-creating a directory another thread made is one
    /// wasted `mkdir` returning `EEXIST`, whereas sharing the set across threads
    /// would need a lock on the hot path.
    dirs: std::collections::HashSet<PathBuf>,
}

impl Accum {
    fn merge(&mut self, other: Accum, tail_cap: usize) {
        for (name, oc) in other.class_stats {
            match self.class_stats.get_mut(&name) {
                Some(c) => {
                    c.ranges += oc.ranges;
                    c.recorded_bytes += oc.recorded_bytes;
                    c.on_disk_bytes += oc.on_disk_bytes;
                    c.min_recorded = c.min_recorded.min(oc.min_recorded);
                    c.max_recorded = c.max_recorded.max(oc.max_recorded);
                }
                None => {
                    self.class_stats.insert(name, oc);
                }
            }
        }
        for (s, n) in other.shard_ranges {
            *self.shard_ranges.entry(s).or_insert(0) += n;
        }
        for (s, n) in other.shard_bytes {
            *self.shard_bytes.entry(s).or_insert(0) += n;
        }
        for (s, heap) in other.shard_tail {
            let mine = self.shard_tail.entry(s).or_default();
            for v in heap {
                mine.push(v);
                if mine.len() > tail_cap {
                    mine.pop();
                }
            }
        }
        self.total_ranges += other.total_ranges;
        self.staged_objects += other.staged_objects;
        self.recorded_compressed += other.recorded_compressed;
        self.recorded_uncompressed += other.recorded_uncompressed;
        self.on_disk += other.on_disk;

        let t = &mut self.tail;
        let o = other.tail;
        t.ranges_at_least_1mib += o.ranges_at_least_1mib;
        t.ranges_at_least_8mib += o.ranges_at_least_8mib;
        t.ranges_at_least_64mib += o.ranges_at_least_64mib;
        t.bytes_at_least_1mib += o.bytes_at_least_1mib;
        t.bytes_at_least_8mib += o.bytes_at_least_8mib;
        t.bytes_at_least_64mib += o.bytes_at_least_64mib;
        t.largest_recorded = t.largest_recorded.max(o.largest_recorded);

        for (d, b) in self.digest.iter_mut().zip(other.digest.iter()) {
            *d ^= *b;
        }
        self.emitted.extend(other.emitted);
    }
}

/// Immutable per-generation context, shared by every worker.
struct EmitCtx {
    metadata_dir: PathBuf,
    ranges_dir: PathBuf,
    gen_id: String,
    age_base: SystemTime,
    min_age: u64,
    max_age: u64,
    stub_bytes: Option<usize>,
    tail_cap: usize,
    collect_emitted: bool,
    dir_memo: bool,
}

/// `create_dir_all`, skipped when this worker knows it already ran.
///
/// `std::fs::create_dir_all` is already safe against a concurrent creation of the
/// same path — it treats `AlreadyExists` on a directory as success — so the memo is
/// purely about not paying for the syscall, not about correctness under threads.
fn ensure_dir(
    dirs: &mut std::collections::HashSet<PathBuf>,
    dir: &Path,
    memo: bool,
) -> Result<(), String> {
    if memo && dirs.contains(dir) {
        return Ok(());
    }
    std::fs::create_dir_all(dir).map_err(|e| format!("create dir {}: {e}", dir.display()))?;
    if memo {
        dirs.insert(dir.to_path_buf());
    }
    Ok(())
}

/// Emit one object: its `.bin` range files, its `.meta`, and its contribution to
/// every rollup.
///
/// Depends on `obj_index` and the spec alone — `SplitMix64::stream(seed, obj_index)`
/// is seekable, so object `i` is addressable without generating `0..i`. That is what
/// makes the parallel driver possible, and it is why this function takes an index
/// rather than being a step in a loop.
fn emit_object(
    ctx: &EmitCtx,
    spec: &FixtureSpec,
    obj_index: u64,
    acc: &mut Accum,
) -> Result<(), String> {
    let mut rng = SplitMix64::stream(spec.seed, obj_index);

    // Every 17th key ends in `.bin`, preserving the adversarial property Phase
    // 0's orphan census relied on: the `_{start}-{end}.bin` suffix strip must
    // be anchored, and a key that itself ends in `.bin` is what catches an
    // unanchored one.
    let extension = if obj_index.is_multiple_of(17) {
        "bin"
    } else {
        "dat"
    };
    let object_key = format!(
        "{}/{}/{:09}/object-{:09}.{}",
        FIXTURE_KEY_PREFIX,
        ctx.gen_id,
        obj_index / 1000,
        obj_index,
        extension
    );
    let cache_key = format!("{}/{}", spec.bucket, object_key);

    let n_ranges = rng.next_range_u64(1, spec.max_ranges_per_object as u64);
    let is_staged = rng.next_f64() < spec.staged_fraction;
    if is_staged {
        acc.staged_objects += 1;
    }

    let meta_path = get_sharded_path(&ctx.metadata_dir, &cache_key, ".meta")
        .map_err(|e| format!("sharded path for {cache_key}: {e}"))?;
    let shard = shard_of(&meta_path).ok_or_else(|| {
        format!(
            "could not read L1 shard out of {} — the sharding layout changed",
            meta_path.display()
        )
    })?;

    let mut ranges: Vec<RangeSpec> = Vec::with_capacity(n_ranges as usize);
    let mut cursor: u64 = 0;

    for range_index in 0..n_ranges {
        let is_first_overall = obj_index == 0 && range_index == 0;
        let (size, class) = draw_size(&spec.distribution, &mut rng, is_first_overall);
        let start = cursor;
        let end = start + size - 1;
        cursor = end + 1;

        let bin_path =
            get_sharded_path(&ctx.ranges_dir, &cache_key, &format!("_{start}-{end}.bin"))
                .map_err(|e| format!("sharded range path for {cache_key}: {e}"))?;
        let relative = bin_path
            .strip_prefix(&ctx.ranges_dir)
            .map_err(|e| format!("relative range path: {e}"))?
            .to_string_lossy()
            .to_string();

        if let Some(parent) = bin_path.parent() {
            ensure_dir(&mut acc.dirs, parent, ctx.dir_memo)?;
        }

        let payload_len = ctx.stub_bytes.map(|s| s as u64).unwrap_or(size);
        write_payload(&bin_path, payload_len, obj_index, range_index)?;

        // `last_accessed` spread across [min_age, max_age] so LRU and
        // TinyLFU orderings are both non-degenerate; `created_at` is at
        // least as old as `last_accessed`, which is what the product
        // guarantees.
        let access_age = rng.next_range_u64(ctx.min_age, ctx.max_age);
        let create_age = rng.next_range_u64(access_age, ctx.max_age);
        let last_accessed = ctx.age_base - Duration::from_secs(access_age);
        let created_at = ctx.age_base - Duration::from_secs(create_age);
        // Access counts spread over roughly a decade so a frequency-aware
        // policy has signal. Deliberately not uniform: most ranges are read
        // once or twice.
        let access_count = 1 + (rng.next_f64().powi(3) * 60.0) as u64;

        let mut spec_range = RangeSpec::new_staged(
            start,
            end,
            relative.clone(),
            CompressionAlgorithm::None,
            size,
            size,
            is_staged,
        );
        spec_range.created_at = created_at;
        spec_range.last_accessed = last_accessed;
        spec_range.access_count = access_count;

        // ---- stats ----
        acc.total_ranges += 1;
        acc.recorded_compressed += size;
        acc.recorded_uncompressed += size;
        acc.on_disk += payload_len;
        let cs = acc.class_stats.entry(class.clone()).or_insert(ClassStats {
            name: class.clone(),
            ranges: 0,
            recorded_bytes: 0,
            on_disk_bytes: 0,
            min_recorded: u64::MAX,
            max_recorded: 0,
        });
        cs.ranges += 1;
        cs.recorded_bytes += size;
        cs.on_disk_bytes += payload_len;
        cs.min_recorded = cs.min_recorded.min(size);
        cs.max_recorded = cs.max_recorded.max(size);

        *acc.shard_ranges.entry(shard.clone()).or_insert(0) += 1;
        *acc.shard_bytes.entry(shard.clone()).or_insert(0) += size;
        let heap = acc.shard_tail.entry(shard.clone()).or_default();
        heap.push(std::cmp::Reverse(size));
        if heap.len() > ctx.tail_cap {
            heap.pop();
        }

        if size >= 1024 * 1024 {
            acc.tail.ranges_at_least_1mib += 1;
            acc.tail.bytes_at_least_1mib += size;
        }
        if size >= 8 * 1024 * 1024 {
            acc.tail.ranges_at_least_8mib += 1;
            acc.tail.bytes_at_least_8mib += size;
        }
        if size >= 64 * 1024 * 1024 {
            acc.tail.ranges_at_least_64mib += 1;
            acc.tail.bytes_at_least_64mib += size;
        }
        acc.tail.largest_recorded = acc.tail.largest_recorded.max(size);

        // Order-independent digest: hash the range's identity and recorded
        // figures, then XOR-fold. Ages are folded in as OFFSETS so the digest
        // does not move with wall-clock time.
        let h = blake3::hash(
            format!(
                "{cache_key}|{start}|{end}|{size}|{relative}|{access_age}|{create_age}|\
                 {access_count}|{is_staged}|{payload_len}"
            )
            .as_bytes(),
        );
        for (d, b) in acc.digest.iter_mut().zip(h.as_bytes().iter()) {
            *d ^= *b;
        }

        if ctx.collect_emitted {
            acc.emitted.push(EmittedRange {
                cache_key: cache_key.clone(),
                start,
                end,
                recorded_compressed_size: size,
                on_disk_size: payload_len,
                relative_bin_path: relative,
                class,
            });
        }

        ranges.push(spec_range);
    }

    let content_length: u64 = ranges.iter().map(|r| r.uncompressed_size).sum();
    let oldest_created = ranges.iter().map(|r| r.created_at).min().unwrap();

    let metadata = NewCacheMetadata {
        cache_key: cache_key.clone(),
        object_metadata: ObjectMetadata {
            etag: format!("\"{}\"", hex32(&blake3::hash(cache_key.as_bytes()))),
            last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            upload_state: UploadState::Complete,
            cumulative_size: content_length,
            compressed_size: content_length,
            is_write_cached: is_staged,
            ..Default::default()
        },
        ranges,
        created_at: oldest_created,
        // Far future: nothing in the fixture should be treated as expired, so
        // a selection measurement is not silently also measuring expiry.
        expires_at: ctx.age_base + Duration::from_secs(365 * 24 * 3600),
        compression_info: CompressionInfo {
            body_algorithm: CompressionAlgorithm::None,
            original_size: Some(content_length),
            compressed_size: Some(content_length),
            file_extension: Some(extension.to_string()),
        },
        ..Default::default()
    };

    if let Some(parent) = meta_path.parent() {
        ensure_dir(&mut acc.dirs, parent, ctx.dir_memo)?;
    }
    let json = serde_json::to_string(&metadata)
        .map_err(|e| format!("serialize metadata for {cache_key}: {e}"))?;
    std::fs::write(&meta_path, json).map_err(|e| format!("write {}: {e}", meta_path.display()))?;

    Ok(())
}

/// Generate a fixture tree under `root`, serially.
///
/// `root` is a cache directory in the product's sense — `metadata/` and `ranges/`
/// are created beneath it — but it must not be a real one; see [`guard_output_dir`].
///
/// This is the reference implementation the parallel driver is verified against.
/// Use [`generate_with`] for anything at task 9 or 10 scale.
pub fn generate(
    root: &Path,
    spec: &FixtureSpec,
    collect_emitted: bool,
) -> Result<GeneratedFixture, String> {
    generate_with(root, spec, &GenOptions::serial(collect_emitted))
}

/// Generate a fixture tree under `root` with an explicit execution plan.
///
/// # Objects per chunk
///
/// Work is partitioned into contiguous blocks of objects rather than individual
/// objects, so one [`Accum`] merge covers a block. Too small and merging the
/// per-shard tail heaps dominates; too large and the tail of the run leaves threads
/// idle. 256 is a compromise sized against task 9's ~5M objects, where it gives
/// ~19,500 blocks — three orders of magnitude more than the thread count, so the
/// straggler at the end costs a fraction of a percent.
pub fn generate_with(
    root: &Path,
    spec: &FixtureSpec,
    opts: &GenOptions,
) -> Result<GeneratedFixture, String> {
    spec.validate()?;
    guard_output_dir(root, opts.shared_storage_ack.as_deref())?;
    if opts.threads == 0 {
        return Err("threads must be > 0".to_string());
    }

    let started = std::time::Instant::now();
    let gen_id = spec.gen_id();
    let metadata_dir = root.join("metadata");
    let ranges_dir = root.join("ranges");
    std::fs::create_dir_all(&metadata_dir).map_err(|e| format!("create metadata dir: {e}"))?;
    std::fs::create_dir_all(&ranges_dir).map_err(|e| format!("create ranges dir: {e}"))?;
    std::fs::write(
        root.join(FIXTURE_MARKER),
        format!(
            "generated by tests/common/graded_fixture.rs (cache-eviction-at-scale task 8)\n\
             gen_id={gen_id}\nseed={:#018x}\n\
             This tree is a TEST FIXTURE, not a cache. Do not point a proxy at it.\n",
            spec.seed
        ),
    )
    .map_err(|e| format!("write marker: {e}"))?;

    // Ages are offsets from one base captured here, so the tree is internally
    // consistent and the digest (which hashes offsets, not absolutes) is stable.
    let age_base = SystemTime::now();
    let min_age = spec.min_age.as_secs();
    let max_age = spec.max_age.as_secs();

    let stub_bytes = match spec.payload {
        PayloadMode::Genuine => None,
        PayloadMode::RecordedSizeOnly { stub_bytes } => Some(stub_bytes),
    };

    let ctx = EmitCtx {
        metadata_dir: metadata_dir.clone(),
        ranges_dir: ranges_dir.clone(),
        gen_id: gen_id.clone(),
        age_base,
        min_age,
        max_age,
        stub_bytes,
        tail_cap: spec.tail_cap,
        collect_emitted: opts.collect_emitted,
        dir_memo: opts.dir_memo,
    };

    const OBJECTS_PER_CHUNK: u64 = 256;
    let chunks = spec.objects.div_ceil(OBJECTS_PER_CHUNK);

    let mut acc = if opts.threads <= 1 {
        // The serial reference: same emitter, no pool, no merge.
        let mut acc = Accum::default();
        for obj_index in 0..spec.objects {
            emit_object(&ctx, spec, obj_index, &mut acc)?;
        }
        acc
    } else {
        use rayon::prelude::*;
        // An explicit pool rather than the global one, so `threads` means what it
        // says even when a caller (a test, or a future benchmark harness) has
        // already sized the global pool for something else.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opts.threads)
            .build()
            .map_err(|e| format!("build rayon pool: {e}"))?;
        let tail_cap = spec.tail_cap;
        pool.install(|| {
            (0..chunks as usize)
                .into_par_iter()
                .try_fold(Accum::default, |mut acc, chunk| {
                    let lo = chunk as u64 * OBJECTS_PER_CHUNK;
                    let hi = (lo + OBJECTS_PER_CHUNK).min(spec.objects);
                    for obj_index in lo..hi {
                        emit_object(&ctx, spec, obj_index, &mut acc)?;
                    }
                    Ok::<Accum, String>(acc)
                })
                .try_reduce(Accum::default, |mut a, b| {
                    a.merge(b, tail_cap);
                    Ok(a)
                })
        })?
    };

    // The digest is order-independent, but `emitted` is a sequence and the parallel
    // driver's block ordering is not contractual. Sorting makes the two drivers
    // comparable element by element, which is a stronger check than digest equality
    // alone — an accidental digest collapse cannot hide behind it. Keys are
    // zero-padded, so lexicographic order is object order, and `start` orders within
    // an object. A no-op on the serial path.
    acc.emitted
        .sort_by(|x, y| (&x.cache_key, x.start).cmp(&(&y.cache_key, y.start)));

    let Accum {
        class_stats,
        shard_ranges,
        shard_bytes,
        shard_tail,
        total_ranges,
        staged_objects,
        recorded_compressed,
        recorded_uncompressed,
        on_disk,
        mut tail,
        digest,
        emitted,
        dirs: _,
    } = acc;
    // ---- roll up ----
    let mut shards: Vec<ShardStats> = shard_ranges
        .keys()
        .map(|s| {
            let heap = shard_tail.get(s);
            let tail_bytes: u64 = heap
                .map(|h| h.iter().map(|std::cmp::Reverse(v)| *v).sum())
                .unwrap_or(0);
            let tail_ranges = heap.map(|h| h.len() as u64).unwrap_or(0);
            ShardStats {
                shard: s.clone(),
                ranges: shard_ranges[s],
                recorded_bytes: shard_bytes[s],
                tail_ranges,
                tail_bytes,
            }
        })
        .collect();
    shards.sort_by(|a, b| a.shard.cmp(&b.shard));

    let tail_bytes_total: u64 = shards.iter().map(|s| s.tail_bytes).sum();
    tail.tail_share_of_recorded_bytes = if recorded_compressed == 0 {
        0.0
    } else {
        tail_bytes_total as f64 / recorded_compressed as f64
    };

    let elapsed = started.elapsed().as_secs_f64();
    let manifest = FixtureManifest {
        schema_version: 1,
        generator: "tests/common/graded_fixture.rs (cache-eviction-at-scale task 8)".to_string(),
        generator_crate_version: env!("CARGO_PKG_VERSION").to_string(),
        generated_at_epoch_secs: age_base
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        label: spec.label.clone(),
        gen_id: gen_id.clone(),
        seed: spec.seed,
        bucket: spec.bucket.clone(),
        key_prefix: FIXTURE_KEY_PREFIX.to_string(),
        distribution: match &spec.distribution {
            SizeDistribution::Graded { size_cap, .. } => match size_cap {
                Some(c) => format!("graded(size_cap={c})"),
                None => "graded".to_string(),
            },
            SizeDistribution::Uniform { size } => format!("uniform({size})"),
            SizeDistribution::SingleDominant {
                background,
                dominant,
            } => format!("single_dominant(bg={background},dom={dominant})"),
        },
        distribution_span_bytes: spec.distribution.span(),
        payload_mode: spec.payload.label().to_string(),
        payload_stub_bytes: stub_bytes.map(|s| s as u64),
        recorded_size_is_deliberately_not_on_disk_size: stub_bytes.is_some(),
        recorded_vs_on_disk_why: if stub_bytes.is_some() {
            "DELIBERATE. This is the count-scale artefact (task 9): `.bin` files are small stubs \
             while `.meta` records realistic compressed_size/uncompressed_size. Count-scale and \
             byte-scale are separate questions and coupling them makes a ~10M-range fixture \
             unbuildable. DO NOT reconcile the two figures. Byte-valued measurements over this \
             tree must read RangeSpec.compressed_size, NOT the on-disk length that \
             collect_candidates_from_metadata_file puts in RangeEvictionCandidate::size."
                .to_string()
        } else {
            "Not applicable: payloads are genuine, so on-disk length equals recorded \
             compressed_size."
                .to_string()
        },
        serve_path_valid: stub_bytes.is_none(),
        objects: spec.objects,
        ranges: total_ranges,
        staged_objects,
        recorded_compressed_bytes: recorded_compressed,
        recorded_uncompressed_bytes: recorded_uncompressed,
        on_disk_bytes: on_disk,
        min_age_secs: min_age,
        max_age_secs: max_age,
        eviction_admission_window_secs: EVICTION_ADMISSION_WINDOW.as_secs(),
        tail_cap: spec.tail_cap as u64,
        classes: class_stats
            .into_values()
            .map(|mut c| {
                if c.min_recorded == u64::MAX {
                    c.min_recorded = 0;
                }
                c
            })
            .collect(),
        shard_count_cv_percent: cv_percent(shards.iter().map(|s| s.ranges as f64)),
        shard_bytes_cv_percent: cv_percent(shards.iter().map(|s| s.recorded_bytes as f64)),
        shards,
        tail,
        throughput: ThroughputStats {
            elapsed_secs: elapsed,
            ranges_per_sec: if elapsed > 0.0 {
                total_ranges as f64 / elapsed
            } else {
                0.0
            },
            on_disk_bytes_per_sec: if elapsed > 0.0 {
                on_disk as f64 / elapsed
            } else {
                0.0
            },
            recorded_bytes_per_sec: if elapsed > 0.0 {
                recorded_compressed as f64 / elapsed
            } else {
                0.0
            },
        },
        content_digest: hex32_bytes(&digest),
        generation_threads: opts.threads as u64,
        dir_memo: opts.dir_memo,
    };

    std::fs::write(
        root.join(MANIFEST_FILE),
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("serialize manifest: {e}"))?,
    )
    .map_err(|e| format!("write manifest: {e}"))?;

    Ok(GeneratedFixture {
        root: root.to_path_buf(),
        manifest,
        emitted,
    })
}

/// Read a manifest back from a fixture root.
pub fn read_manifest(root: &Path) -> Result<FixtureManifest, String> {
    let raw = std::fs::read_to_string(root.join(MANIFEST_FILE))
        .map_err(|e| format!("read manifest: {e}"))?;
    serde_json::from_str(&raw).map_err(|e| format!("parse manifest: {e}"))
}

/// Write a `.bin` payload of `len` bytes, content derived from the indices so two
/// different ranges never hold identical bytes (which would let a byte-exactness
/// assertion pass against the wrong file).
fn write_payload(path: &Path, len: u64, obj_index: u64, range_index: u64) -> Result<(), String> {
    use std::io::Write;

    const CHUNK: usize = 256 * 1024;
    let seed = obj_index
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(range_index);
    let mut pattern = vec![0u8; CHUNK];
    for (i, b) in pattern.iter_mut().enumerate() {
        *b = (seed as usize + i) as u8;
    }

    let file =
        std::fs::File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?;
    let mut w = std::io::BufWriter::new(file);
    let mut written: u64 = 0;
    while written < len {
        let n = ((len - written) as usize).min(CHUNK);
        w.write_all(&pattern[..n])
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        written += n as u64;
    }
    w.flush()
        .map_err(|e| format!("flush {}: {e}", path.display()))?;
    Ok(())
}

/// Extract the L1 shard (the `XX` component) from a sharded path
/// `.../{bucket}/{XX}/{YYY}/{file}`.
///
/// Read off the produced path rather than re-derived from the key, so the manifest
/// describes the tree as it exists rather than as the generator intended.
fn shard_of(path: &Path) -> Option<String> {
    let comps: Vec<_> = path.components().collect();
    if comps.len() < 3 {
        return None;
    }
    comps[comps.len() - 3]
        .as_os_str()
        .to_str()
        .map(|s| s.to_string())
}

fn cv_percent(values: impl Iterator<Item = f64>) -> f64 {
    let v: Vec<f64> = values.collect();
    if v.len() < 2 {
        return 0.0;
    }
    let n = v.len() as f64;
    let mean = v.iter().sum::<f64>() / n;
    if mean == 0.0 {
        return 0.0;
    }
    let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    var.sqrt() / mean * 100.0
}

fn hex32(hash: &blake3::Hash) -> String {
    hash.to_hex().as_str()[..32].to_string()
}

fn hex32_bytes(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
