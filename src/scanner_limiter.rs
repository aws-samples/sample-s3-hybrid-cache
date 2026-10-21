//! Scanner-concurrency limiter (cache-eviction-at-scale § 16.6.13 K2).
//!
//! [`ScannerLimiter`] is the K2 control law, pure and I/O-free: the caller feeds it one sample per
//! shard visit ([`ScannerLimiter::record_visit`]) and closes each pass with
//! [`ScannerLimiter::end_pass`], which returns the concurrency `k` the next pass should respect.
//! It reads no storage, owns no clock and draws no randomness; the probe's uniform draw is
//! injected so every decision is reproducible from its inputs.
//!
//! - **Level attribution DISCARDS a visit whose `n` changed** (Ed's ruling, 2026-09-08). A visit
//!   that started at `n_start` other scanners and ended at a different `n_end` ran at no single
//!   level, so it is counted in [`ScannerLimiter::samples_discarded`] and contributes nothing.
//!   Summarising it at either end biases `Â[j] = j·ρ̂[j]` toward higher levels, because the error
//!   in `j` is multiplied by `j`. That is the unsafe direction on a backend whose aggregate falls
//!   past a peak.
//! - **`ρ̂[j]` is an EWMA at α = 1/4** of the per-visit rate (`.meta` files read per second) at
//!   level `j = n + 1`. It is deliberately NOT `ledger_repair`'s `ewma_blend`, whose α is 1/8.
//! - **`k` moves to the argmax of `Â[j]`** over levels `1..=k_max` with at least
//!   [`N_MIN_SAMPLES`] samples, only when that level beats the current level's estimate by more
//!   than [`MARGIN`], and at most once per [`HOLD_DOWN_PASSES`] passes. Otherwise, with probability
//!   [`PROBE_PROBABILITY`], it probes `k + 1`, capped at `k_max`. Growth is additive and shrink is
//!   a single jump: AIMD in the safe direction. `k` never goes below 1.
//!
//! **Homogeneous-fleet premise.** Each instance tunes from its own observations, and
//! `Â[j] = j·ρ̂[j]` turns one instance's per-visit rate into a fleet figure by assuming every
//! scanner at level `j` sees the same rate. On a mixed fleet (different instance types, network
//! paths or mount options) that does not hold, and the instances' `k` values disagree. The
//! effective fleet ceiling is then the MAXIMUM `k` over instances, not a consensus: a scanner
//! defers only while `n ≥` its own `k`, so the instance with the largest `k` keeps admitting
//! itself up to that level.

use std::collections::BTreeMap;
use std::time::Duration;

/// EWMA weight of a new per-visit rate sample (K2 step 3).
pub const ALPHA: f64 = 0.25;

/// A level needs this many kept samples before the argmax may consider it (K2 step 5, `N_min`).
pub const N_MIN_SAMPLES: u64 = 3;

/// A level must beat the current level's `Â` by more than this fraction to move `k` (K2 `γ`).
pub const MARGIN: f64 = 0.10;

/// At most one change of `k` per this many passes. Applies to argmax moves and probes alike.
pub const HOLD_DOWN_PASSES: u32 = 4;

/// Per-pass probability of probing `k + 1` when the argmax did not move `k` (K2 step 6).
pub const PROBE_PROBABILITY: f64 = 0.125;

/// Why the most recent [`ScannerLimiter::end_pass`] left `k` where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjustReason {
    /// `k` moved to a level whose `Â` beat the current level's by more than [`MARGIN`].
    Argmax,
    /// `k` moved to `k + 1` on the injected draw.
    Probe,
    /// `k` did not change: inside the hold-down, no level cleared the margin and no probe fired,
    /// or the probe was already at `k_max`.
    Hold,
    /// `k_max` is 0 (no scanner document seen), so `k` sits at its floor of 1.
    Floor,
    /// The current level has fewer than [`N_MIN_SAMPLES`] samples, so the argmax could not judge
    /// it, and no probe fired.
    InsufficientSamples,
}

impl AdjustReason {
    /// The `/metrics` spelling of this reason.
    pub fn as_str(self) -> &'static str {
        match self {
            AdjustReason::Argmax => "argmax",
            AdjustReason::Probe => "probe",
            AdjustReason::Hold => "hold",
            AdjustReason::Floor => "floor",
            AdjustReason::InsufficientSamples => "insufficient_samples",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Level {
    rate: f64,
    samples: u64,
}

/// The K2 controller for one scan kind on one instance. See the module docs.
#[derive(Debug, Clone)]
pub struct ScannerLimiter {
    k: u32,
    k_max: u32,
    levels: BTreeMap<u32, Level>,
    samples_discarded: u64,
    passes_since_change: u32,
    last_reason: AdjustReason,
}

impl ScannerLimiter {
    /// Start at `k = k_max`, which is today's behaviour: no instance defers until the controller
    /// has evidence. A `k_max` of 0 starts at the floor, `k = 1`.
    pub fn new(k_max: u32) -> Self {
        Self {
            k: k_max.max(1),
            k_max,
            levels: BTreeMap::new(),
            samples_discarded: 0,
            // The first pass may change k: there is no prior change to hold down.
            passes_since_change: HOLD_DOWN_PASSES,
            last_reason: AdjustReason::InsufficientSamples,
        }
    }

    /// Update the ceiling (the number of distinct instances that published a scanner document
    /// within one lease). Lowering it below `k` clamps `k` at once; raising it leaves `k` where it
    /// is, and the probe climbs toward it.
    pub fn set_k_max(&mut self, k_max: u32) {
        self.k_max = k_max;
        self.k = self.k.min(k_max).max(1);
    }

    /// Record one shard visit. `n_start` and `n_end` are the live peer-scanner counts read at the
    /// visit's start and end. A visit whose `n` changed is discarded and counted. A visit with zero
    /// elapsed time carries no rate and is ignored without being counted.
    pub fn record_visit(&mut self, n_start: u32, n_end: u32, metas_read: u64, elapsed: Duration) {
        if n_start != n_end {
            self.samples_discarded = self.samples_discarded.saturating_add(1);
            return;
        }
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return;
        }
        let rate = metas_read as f64 / secs;
        let level = self.levels.entry(n_start.saturating_add(1)).or_default();
        level.rate = if level.samples == 0 {
            rate
        } else {
            level.rate + ALPHA * (rate - level.rate)
        };
        level.samples = level.samples.saturating_add(1);
    }

    /// Close a pass and return the `k` for the next one. `draw` is a uniform draw in `[0, 1)`;
    /// the probe fires when `draw < PROBE_PROBABILITY`.
    pub fn end_pass(&mut self, draw: f64) -> u32 {
        self.passes_since_change = self.passes_since_change.saturating_add(1);
        if self.k_max == 0 {
            self.k = 1;
            self.last_reason = AdjustReason::Floor;
            return self.k;
        }
        self.k = self.k.min(self.k_max).max(1);
        if self.passes_since_change < HOLD_DOWN_PASSES {
            self.last_reason = AdjustReason::Hold;
            return self.k;
        }

        let current = self.aggregate_estimate(self.k);
        if let Some(current) = current {
            if let Some((best, best_a)) = self.best_level() {
                if best != self.k && best_a > (1.0 + MARGIN) * current {
                    return self.change_to(best, AdjustReason::Argmax);
                }
            }
        }
        if draw < PROBE_PROBABILITY && self.k < self.k_max {
            return self.change_to(self.k + 1, AdjustReason::Probe);
        }
        self.last_reason = if current.is_some() {
            AdjustReason::Hold
        } else {
            AdjustReason::InsufficientSamples
        };
        self.k
    }

    /// The concurrency the current pass should respect: defer while `n ≥ k`.
    pub fn k(&self) -> u32 {
        self.k
    }

    /// The ceiling last set by [`ScannerLimiter::new`] or [`ScannerLimiter::set_k_max`].
    pub fn k_max(&self) -> u32 {
        self.k_max
    }

    /// Visits discarded because `n` changed across them.
    pub fn samples_discarded(&self) -> u64 {
        self.samples_discarded
    }

    /// Why the most recent [`ScannerLimiter::end_pass`] chose its `k`.
    pub fn last_adjust_reason(&self) -> AdjustReason {
        self.last_reason
    }

    /// `ρ̂[level]`, the EWMA per-visit rate at `level`, if any sample was kept there.
    #[cfg(test)]
    pub fn rate_estimate(&self, level: u32) -> Option<f64> {
        self.levels
            .get(&level)
            .filter(|l| l.samples > 0)
            .map(|l| l.rate)
    }

    /// Kept samples at `level`.
    pub fn samples_at(&self, level: u32) -> u64 {
        self.levels.get(&level).map_or(0, |l| l.samples)
    }

    /// Every level holding at least one kept sample, ascending, as `(level, ρ̂, samples)`.
    pub fn sampled_levels(&self) -> impl Iterator<Item = (u32, f64, u64)> + '_ {
        self.levels
            .iter()
            .filter(|(_, l)| l.samples > 0)
            .map(|(&j, l)| (j, l.rate, l.samples))
    }

    /// `Â[level] = level·ρ̂[level]`, only for a level with at least [`N_MIN_SAMPLES`] samples.
    ///
    /// The estimated fleet-wide read rate at `level` concurrent scanners: the curve `k` is the
    /// argmax of, published as `scanner_aggregate_estimate_by_level_{fold,repair}`.
    pub fn aggregate_estimate(&self, level: u32) -> Option<f64> {
        self.levels
            .get(&level)
            .filter(|l| l.samples >= N_MIN_SAMPLES)
            .map(|l| f64::from(level) * l.rate)
    }

    /// The qualified level in `1..=k_max` with the largest `Â`; ties go to the lower level.
    fn best_level(&self) -> Option<(u32, f64)> {
        let mut best: Option<(u32, f64)> = None;
        for &level in self.levels.range(1..=self.k_max).map(|(j, _)| j) {
            if let Some(a) = self.aggregate_estimate(level) {
                if best.is_none_or(|(_, b)| a > b) {
                    best = Some((level, a));
                }
            }
        }
        best
    }

    fn change_to(&mut self, k: u32, reason: AdjustReason) -> u32 {
        self.k = k.min(self.k_max).max(1);
        self.passes_since_change = 0;
        self.last_reason = reason;
        self.k
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// splitmix64: a seeded, deterministic uniform source for the synthetic fleet and the draws.
    struct Rng(u64);
    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn uniform(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }
        /// Uniform integer in `1..=hi`.
        fn level(&mut self, hi: u32) -> u32 {
            1 + ((self.uniform() * f64::from(hi)) as u32).min(hi - 1)
        }
    }

    const VISITS_PER_PASS: usize = 64;
    /// Fraction of visits during which a burst of peers starts or stops (cadences align, so peer
    /// transitions arrive together). Such a visit ran at its START level for nearly all of its
    /// duration, and its END reading lands anywhere in `1..=k_max`.
    const P_CHANGE: f64 = 0.25;
    const NOISE: f64 = 0.02;

    /// Drive `passes` passes of the synthetic fleet against `per_instance(j)` (obj/s per scanner
    /// at level j). Half the visits run at level k, half at a level uniform in `1..=k` (peers
    /// finishing at different times). Returns k after each pass.
    fn simulate(
        lim: &mut ScannerLimiter,
        per_instance: &dyn Fn(u32) -> f64,
        passes: usize,
        seed: u64,
    ) -> Vec<u32> {
        let mut rng = Rng(seed);
        let mut ks = Vec::with_capacity(passes);
        for _ in 0..passes {
            let k = lim.k();
            let k_max = lim.k_max().max(1);
            for _ in 0..VISITS_PER_PASS {
                let j_start = if rng.uniform() < 0.5 { k } else { rng.level(k) };
                let mut j_end = j_start;
                if rng.uniform() < P_CHANGE && k_max > 1 {
                    j_end = rng.level(k_max);
                    if j_end == j_start {
                        j_end = j_start % k_max + 1;
                    }
                }
                let rate = per_instance(j_start) * (1.0 + NOISE * (2.0 * rng.uniform() - 1.0));
                let elapsed = Duration::from_secs(10);
                let metas = (rate * 10.0).round() as u64;
                lim.record_visit(j_start - 1, j_end - 1, metas, elapsed);
            }
            ks.push(lim.end_pass(rng.uniform()));
        }
        ks
    }

    fn changes(ks: &[u32]) -> usize {
        ks.windows(2).filter(|w| w[0] != w[1]).count()
    }

    /// FSxZ shape: aggregate rises to a peak at 3 then declines; per-instance rate falls with j.
    fn fsxz(j: u32) -> f64 {
        let a = match j {
            1 => 1000.0,
            2 => 1700.0,
            _ => 2000.0 * (3.0 / f64::from(j)).powf(0.3),
        };
        a / f64::from(j)
    }

    /// FSxN shape: aggregate keeps rising to at least 24.
    fn fsxn(j: u32) -> f64 {
        1000.0 * f64::from(j).powf(0.8) / f64::from(j)
    }

    /// M4's step: the aggregate at 5 is 4.9% below the aggregate at 3, with 4 between them.
    fn step(j: u32) -> f64 {
        let a = match j {
            1 => 450.0,
            2 => 800.0,
            3 => 1000.0,
            4 => 975.0,
            _ => 951.0,
        };
        a / f64::from(j)
    }

    #[test]
    fn fsxz_curve_settles_in_peak_region_from_k_max_24() {
        let mut lim = ScannerLimiter::new(24);
        let ks = simulate(&mut lim, &fsxz, 200, 0xF5A2);
        eprintln!(
            "fsxz: changes={} first={:?} tail={:?}",
            changes(&ks),
            &ks[..12],
            &ks[180..]
        );
        let settled = &ks[40..];
        assert!(
            settled.iter().all(|k| (3..=7).contains(k)),
            "k left 3..=7 after pass 40: {settled:?}"
        );
        // Non-vacuity: the controller actually moved off k_max.
        assert!(ks[..40].iter().any(|&k| k < 24), "k never left 24: {ks:?}");
    }

    #[test]
    fn fsxn_curve_climbs_to_k_max() {
        // A fleet that grew from one instance to 24: k starts at 1 and must climb by probes.
        let mut lim = ScannerLimiter::new(1);
        lim.set_k_max(24);
        assert_eq!(lim.k(), 1);
        let ks = simulate(&mut lim, &fsxn, 600, 0xF5A3);
        let reached = ks.iter().position(|&k| k == 24);
        eprintln!(
            "fsxn: reached 24 at pass {reached:?}, changes={}",
            changes(&ks)
        );
        let at = reached.expect("k never reached k_max = 24");
        assert!(
            ks[at..].iter().all(|&k| k == 24),
            "k fell back after reaching 24: {:?}",
            &ks[at..]
        );
        // And a fleet that starts at k_max stays there.
        let mut lim = ScannerLimiter::new(24);
        let ks = simulate(&mut lim, &fsxn, 200, 0xF5A4);
        assert!(ks.iter().all(|&k| k == 24), "fsxn from 24 moved: {ks:?}");
    }

    #[test]
    fn step_of_4_9_percent_does_not_oscillate() {
        let mut lim = ScannerLimiter::new(5);
        let ks = simulate(&mut lim, &step, 200, 0x0005_C111);
        let n = changes(&ks);
        eprintln!("step: changes={n} tail={:?}", &ks[180..]);
        assert!(n <= 1, "k changed {n} times over 200 passes: {ks:?}");
        assert!(ks.iter().all(|k| (3..=5).contains(k)), "{ks:?}");
    }

    #[test]
    fn k_stays_within_one_and_k_max() {
        let curves: [&dyn Fn(u32) -> f64; 3] = [&fsxz, &fsxn, &step];
        for seed in 0..40u64 {
            for (ci, curve) in curves.iter().enumerate() {
                let mut rng = Rng(seed * 7 + ci as u64);
                let mut lim = ScannerLimiter::new(rng.level(30));
                for _ in 0..10 {
                    // k_max changes mid-run, including to 0 (no scanner document seen).
                    let k_max = (rng.uniform() * 31.0) as u32;
                    lim.set_k_max(k_max);
                    assert!(lim.k() >= 1 && lim.k() <= k_max.max(1));
                    for k in simulate(&mut lim, *curve, 30, rng.next_u64()) {
                        assert!(k >= 1, "k reached 0");
                        assert!(k <= k_max.max(1), "k={k} above k_max={k_max}");
                    }
                    if k_max == 0 {
                        assert_eq!(lim.last_adjust_reason(), AdjustReason::Floor);
                    }
                }
            }
        }
    }

    #[test]
    fn changed_n_is_discarded_and_counted() {
        let mut lim = ScannerLimiter::new(4);
        lim.record_visit(1, 2, 100, Duration::from_secs(1));
        assert_eq!(lim.samples_discarded(), 1);
        assert_eq!(lim.samples_at(2), 0);
        assert_eq!(lim.samples_at(3), 0);
        lim.record_visit(1, 1, 100, Duration::from_secs(1));
        lim.record_visit(1, 1, 200, Duration::from_secs(1));
        assert_eq!(lim.samples_at(2), 2);
        // EWMA at α = 1/4: 100 + (200 - 100) / 4.
        assert_eq!(lim.rate_estimate(2), Some(125.0));
        lim.record_visit(0, 0, 5, Duration::ZERO);
        assert_eq!(lim.samples_at(1), 0);
        assert_eq!(lim.samples_discarded(), 1);
    }

    #[test]
    fn hold_down_allows_one_change_per_four_passes() {
        let mut lim = ScannerLimiter::new(8);
        for _ in 0..3 {
            lim.record_visit(7, 7, 10, Duration::from_secs(1)); // level 8: Â = 80
            lim.record_visit(1, 1, 100, Duration::from_secs(1)); // level 2: Â = 200
        }
        assert_eq!(lim.end_pass(0.9), 2);
        assert_eq!(lim.last_adjust_reason(), AdjustReason::Argmax);
        for _ in 0..3 {
            assert_eq!(lim.end_pass(0.0), 2);
            assert_eq!(lim.last_adjust_reason(), AdjustReason::Hold);
        }
        assert_eq!(lim.end_pass(0.0), 3);
        assert_eq!(lim.last_adjust_reason(), AdjustReason::Probe);
        for _ in 0..3 {
            lim.end_pass(0.0);
        }
        // Level 3 has no samples: the argmax cannot judge it and no probe fires.
        assert_eq!(lim.end_pass(0.9), 3);
        assert_eq!(lim.last_adjust_reason(), AdjustReason::InsufficientSamples);
    }
}
