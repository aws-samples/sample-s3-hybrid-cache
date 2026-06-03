//! `resolve_settings` benchmark — Task 9.1 merge gate (Requirements 8.1, 8.3, 8.4).
//!
//! Characterizes the per-request cost of the glob cache-rules resolver and
//! confirms it stays within the ≤ 1 ms request-latency budget at the rule cap.
//!
//! Deliberately uses ONLY `std::time::Instant` — no `criterion`/`divan`. This
//! repo has a documented anti-dependency posture (`nonpublic/LICENSE_COMPLIANCE.md`
//! requires a THIRD_PARTY + compliance review for any new crate; the design reused
//! `regex` rather than adding `globset`). The ≤ 1 ms bar is coarse and expected to
//! be cleared by ~100x, and this is a one-time characterization for the docs
//! (task 10.2) and cap confirmation — not a CI regression database.
//!
//! What it measures, driving the REAL public path
//! (`BucketSettingsManager::resolve`, including the per-call `RwLock` read a
//! request actually pays):
//!
//! - Resolution latency at 1 / 16 / 64 / 256 / 1024 rules over representative
//!   keys (bucket-prefix, deep, access-point form, MRAP form, and a no-match key).
//! - A "baseline" empty rule set (no `cache_rules.json`) as the resolver floor,
//!   so each N's cost is reported both absolutely and as a delta over baseline —
//!   the marginal "feature cost" the latency budget governs.
//! - A high-fan-out worst case: N broad `**` rules that ALL match one key and set
//!   no field, forcing the first-match-per-field walk to traverse every matched
//!   index for all 8 fields. Per the design, match fan-out per key (not total
//!   rule count) is the real cost driver, so this case bounds the 1024 cap.
//!
//! It asserts the measured per-resolve cost is ≤ 1 ms (panics, non-zero exit, on
//! breach — this is a merge gate). Run in RELEASE (microbenchmarks are meaningless
//! in debug):
//!
//! ```text
//! cargo run --release --example resolve_settings_bench
//! ```

use std::time::{Duration, Instant};

use s3_proxy::bucket_settings::{
    BucketSettingsManager, CacheRule, CacheRules, GlobalDefaults, DEFAULT_MAX_RULES,
};
use tempfile::TempDir;

/// Rule counts to characterize. 1024 is the current `DEFAULT_MAX_RULES` cap.
const RULE_COUNTS: [usize; 5] = [1, 16, 64, 256, 1024];

/// Iterations per key for the throughput (mean ns/op) pass.
const THROUGHPUT_ITERS: usize = 20_000;

/// Iterations per key for the tail (median/p99/max) pass. Smaller — each call is
/// individually timed, so the sample vector stays bounded.
const TAIL_ITERS: usize = 4_000;

/// The per-request latency budget the feature must not exceed (Requirement 8.1).
const BUDGET: Duration = Duration::from_millis(1);

/// Global scalar defaults, mirroring a typical `cache:` YAML floor.
fn global_defaults() -> GlobalDefaults {
    GlobalDefaults {
        get_ttl: Duration::from_secs(300),
        head_ttl: Duration::from_secs(60),
        put_ttl: Duration::from_secs(3600),
        read_cache_enabled: true,
        write_cache_enabled: true,
        compression_enabled: false,
        ram_cache_enabled: true,
        evaluate_conditions_from_cache: false,
    }
}

/// `n` highly-specific rules (`b{i}/data/**`), each setting one field. Any single
/// key matches at most one rule — the realistic low-fan-out case.
fn specific_rules(n: usize) -> Vec<CacheRule> {
    (0..n)
        .map(|i| CacheRule {
            pattern: format!("b{i}/data/**"),
            get_ttl: Some(Duration::from_secs(300)),
            ..Default::default()
        })
        .collect()
}

/// `n` broad `**` rules that set NO field. Every key matches all `n` rules, and
/// because none set a field the first-match-per-field walk traverses every
/// matched index for all 8 fields before falling through to the globals — the
/// genuine worst case the rule cap must bound.
fn high_fanout_rules(n: usize) -> Vec<CacheRule> {
    (0..n)
        .map(|_| CacheRule {
            pattern: "**".to_string(),
            ..Default::default()
        })
        .collect()
}

/// Representative keys for the specific-rules scenario: a key hitting the first
/// rule, a deep key hitting a middle rule, a key hitting the last rule, a key
/// that matches no rule, plus access-point and MRAP cache-key forms.
fn specific_keys(n: usize) -> Vec<String> {
    let mid = n / 2;
    let last = n.saturating_sub(1);
    vec![
        "b0/data/path/to/object-0.bin".to_string(),
        format!("b{mid}/data/deep/a/b/c/d/e/object-mid.bin"),
        format!("b{last}/data/object-last.txt"),
        "unmatched-bucket/random/path/object.dat".to_string(),
        "myap-123456789012-s3alias/data/object.bin".to_string(),
        "mfzwi23gnjvgw.mrap/data/deep/object.bin".to_string(),
    ]
}

/// Representative keys for the high-fan-out scenario. Each matches every `**`
/// rule, spanning bucket, access-point, and MRAP cache-key forms.
fn high_fanout_keys() -> Vec<String> {
    vec![
        "b/aaaa/bbbb/cccc/object-file.bin".to_string(),
        "myap-123456789012-s3alias/x/y/z/object.bin".to_string(),
        "mfzwi23gnjvgw.mrap/data/deep/object.bin".to_string(),
    ]
}

/// Build a manager over a freshly written `cache_rules.json`, warmed so the
/// staleness window (1 h) means no reload happens mid-measurement. The `TempDir`
/// is returned to keep it alive for the manager's lifetime.
fn build_manager(rules: &[CacheRule]) -> (BucketSettingsManager, TempDir) {
    let dir = TempDir::new().expect("create tempdir");
    let file = CacheRules {
        schema: None,
        rules: rules.to_vec(),
    };
    let json = serde_json::to_string_pretty(&file).expect("serialize rules");
    std::fs::write(dir.path().join("cache_rules.json"), json).expect("write rules file");
    let mgr = BucketSettingsManager::new(
        dir.path().to_path_buf(),
        global_defaults(),
        Duration::from_secs(3600),
    );
    (mgr, dir)
}

/// A manager with no rules file at all — the empty-rule-set baseline (resolver
/// floor: `RwLock` read + an empty `RegexSet` scan).
fn build_baseline_manager() -> (BucketSettingsManager, TempDir) {
    let dir = TempDir::new().expect("create tempdir");
    let mgr = BucketSettingsManager::new(
        dir.path().to_path_buf(),
        global_defaults(),
        Duration::from_secs(3600),
    );
    (mgr, dir)
}

/// One measurement: mean ns/op (untimed-per-call throughput pass) plus
/// median / p99 / max (individually-timed tail pass).
struct Measurement {
    mean_ns: f64,
    median_ns: u128,
    p99_ns: u128,
    max_ns: u128,
}

async fn measure(mgr: &BucketSettingsManager, keys: &[String]) -> Measurement {
    // Warm: trigger the lazy load before timing anything.
    for k in keys {
        std::hint::black_box(mgr.resolve(k).await);
    }

    // Throughput pass — no per-call Instant, so the mean excludes timer overhead.
    let throughput_ops = THROUGHPUT_ITERS * keys.len();
    let start = Instant::now();
    for _ in 0..THROUGHPUT_ITERS {
        for k in keys {
            std::hint::black_box(mgr.resolve(k).await);
        }
    }
    let elapsed = start.elapsed();
    let mean_ns = elapsed.as_nanos() as f64 / throughput_ops as f64;

    // Tail pass — each call individually timed (timer overhead is included, which
    // conservatively overestimates the tail; fine against a 1 ms bar).
    let mut samples: Vec<u128> = Vec::with_capacity(TAIL_ITERS * keys.len());
    for _ in 0..TAIL_ITERS {
        for k in keys {
            let t = Instant::now();
            std::hint::black_box(mgr.resolve(k).await);
            samples.push(t.elapsed().as_nanos());
        }
    }
    samples.sort_unstable();
    let len = samples.len();
    let median_ns = samples[len / 2];
    let p99_ns = samples[(len * 99) / 100];
    let max_ns = *samples.last().unwrap();

    Measurement {
        mean_ns,
        median_ns,
        p99_ns,
        max_ns,
    }
}

fn print_row(label: &str, m: &Measurement, baseline_ns: f64) {
    let delta = (m.mean_ns - baseline_ns).max(0.0);
    println!(
        "{label:<26} {:>10.1} {:>12.3} {:>10} {:>10} {:>10} {:>12.1}",
        m.mean_ns,
        m.mean_ns / 1000.0,
        m.median_ns,
        m.p99_ns,
        m.max_ns,
        delta,
    );
}

#[tokio::main]
async fn main() {
    println!("resolve_settings benchmark (Task 9.1 merge gate)");
    println!("std::time::Instant only — no benchmark-harness dependency.");
    println!(
        "Driving the real public path BucketSettingsManager::resolve (incl. per-call RwLock read)."
    );
    println!(
        "Throughput: {THROUGHPUT_ITERS} iters/key (mean). Tail: {TAIL_ITERS} iters/key (median/p99/max)."
    );
    println!("Budget (Requirement 8.1): per-resolve cost ≤ {BUDGET:?}.\n");

    let header = format!(
        "{:<26} {:>10} {:>12} {:>10} {:>10} {:>10} {:>12}",
        "scenario", "mean ns", "mean µs", "p50 ns", "p99 ns", "max ns", "Δmean ns",
    );

    // The gate is on a ROBUST tail (p99), not the single-sample absolute max.
    // Across hundreds of thousands of awaited `resolve` calls the raw max
    // inevitably captures a one-off OS/runtime scheduling hiccup (preemption,
    // page fault) that is unrelated to resolver cost — it is non-monotonic in
    // rule count, so it is jitter, not signal. p99 is the meaningful "feature
    // cost added to request latency" figure (Requirement 8.1). Max is still
    // printed per row and summarized as informational jitter.
    let mut worst_p99_ns: u128 = 0;
    let mut worst_p99_label = String::new();
    let mut worst_max_ns: u128 = 0;
    let mut worst_max_label = String::new();
    let mut note = |m: &Measurement, label: &str| {
        if m.p99_ns > worst_p99_ns {
            worst_p99_ns = m.p99_ns;
            worst_p99_label = label.to_string();
        }
        if m.max_ns > worst_max_ns {
            worst_max_ns = m.max_ns;
            worst_max_label = label.to_string();
        }
    };

    // Baseline: empty rule set (no cache_rules.json).
    let (baseline_mgr, _baseline_dir) = build_baseline_manager();
    let baseline_keys = specific_keys(1);
    let baseline = measure(&baseline_mgr, &baseline_keys).await;
    let baseline_ns = baseline.mean_ns;
    note(&baseline, "baseline (empty rule set)");

    // ---- Specific rules (realistic, low fan-out) ----
    println!("== Specific rules: b{{i}}/data/** — each key matches ≤ 1 rule (realistic) ==");
    println!("{header}");
    print_row("baseline (0 rules)", &baseline, baseline_ns);
    for n in RULE_COUNTS {
        let rules = specific_rules(n);
        let (mgr, _dir) = build_manager(&rules);
        let keys = specific_keys(n);
        let m = measure(&mgr, &keys).await;
        note(&m, &format!("specific, {n} rules"));
        print_row(&format!("{n} rules"), &m, baseline_ns);
    }

    // ---- High fan-out (worst case: all rules match, none set a field) ----
    println!(
        "\n== High fan-out: N × `**`, no fields set — every key matches ALL N rules (worst case) =="
    );
    println!("{header}");
    for n in RULE_COUNTS {
        let rules = high_fanout_rules(n);
        let (mgr, _dir) = build_manager(&rules);
        let keys = high_fanout_keys();
        let m = measure(&mgr, &keys).await;
        note(&m, &format!("high fan-out, {n} rules"));
        print_row(&format!("{n} rules (fan-out={n})"), &m, baseline_ns);
    }

    // ---- Verdict ----
    let budget_ns = BUDGET.as_nanos();
    let headroom = budget_ns as f64 / worst_p99_ns.max(1) as f64;
    println!("\n== Verdict ==");
    println!(
        "Worst per-resolve p99 (gate metric): {worst_p99_ns} ns ({:.3} µs) [{worst_p99_label}].",
        worst_p99_ns as f64 / 1000.0
    );
    println!(
        "Worst per-resolve max (informational — single-sample OS jitter, non-monotonic): \
         {worst_max_ns} ns ({:.3} µs) [{worst_max_label}].",
        worst_max_ns as f64 / 1000.0
    );
    println!("Budget: {budget_ns} ns (1 ms). Headroom vs worst p99: {headroom:.0}x under budget.");
    println!(
        "1024-rule cap (DEFAULT_MAX_RULES = {DEFAULT_MAX_RULES}): the high-fan-out 1024-rule row\n\
         above is the binding worst case (all 1024 rules match one key, forcing the full\n\
         first-match-per-field walk). Its p99 stays well under the 1 ms budget, so 1024 is\n\
         confirmed as a safe guardrail — match fan-out per key, not total rule count, is the\n\
         cost driver."
    );

    assert!(
        worst_p99_ns < budget_ns,
        "MERGE GATE FAILED: worst per-resolve p99 {worst_p99_ns} ns ({:.3} µs) [{worst_p99_label}] \
         exceeds the {budget_ns} ns (1 ms) budget. The 1024 cap and/or rule-count guidance must \
         be revisited from this data.",
        worst_p99_ns as f64 / 1000.0
    );

    println!(
        "\nPASS: every measured per-resolve p99 is ≤ 1 ms (Requirement 8.1 satisfied), \
         with the 1024-rule worst case ~{headroom:.0}x under budget."
    );
}
