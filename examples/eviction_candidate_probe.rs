//! Drive the real eviction candidate collector over a cache tree and report what it
//! discovered.
//!
//! Spec: `cache-eviction-at-scale`, task 9 (fixture verification). Requirements: 13.1,
//! 13.4.
//!
//! # What this is for, and what it deliberately is not
//!
//! Task 9's artefact is only valid if **product** code can discover it. A generator
//! validated by its own read-back proves that its writer and its parser agree and
//! nothing else, which is task 8's stated risk. So this binary calls
//! `CacheManager::collect_range_candidates_for_eviction` — the same entry point the
//! disk eviction pass uses — over a tree on the real shared backend, and reports the
//! identity and byte figures it recovered so they can be compared against the
//! fixture's manifest.
//!
//! It is **not** a measurement of eviction. It does not drive a pass, does not run to
//! a byte target, and reports no throughput conclusion. Selection cost at scale, the
//! byte-target failure, and eviction-versus-ingest are tasks 11 to 13, and mixing a
//! "does the artefact work" check with a "how fast is the product" measurement is how
//! a fixture defect gets reported as a product defect. Keep them apart.
//!
//! # `size` versus `compressed_size` — read this before using the output
//!
//! `collect_candidates_from_metadata_file` stats the `.bin` and puts the **actual file
//! length** in `RangeEvictionCandidate::size`, falling back to `compressed_size` only
//! when the stat fails. On a count-scale fixture (`PayloadMode::RecordedSizeOnly`) the
//! `.bin` is a small stub while the `.meta` records a realistic size, so `size` is the
//! stub and `compressed_size` is the figure a byte-valued measurement wants. This
//! binary prints both sums side by side precisely so that the asymmetry is impossible
//! to miss, rather than being a footnote someone reads after publishing a wrong number.
//!
//! # Usage
//!
//! ```text
//! cargo run --release --example eviction_candidate_probe -- \
//!     --cache-dir /mnt/efs/fixture-a-count-scale-subset --subset
//!
//! # whole tree, with manifest agreement asserted
//! cargo run --release --example eviction_candidate_probe -- \
//!     --cache-dir tmp/fixture-small
//! ```
//!
//! `--subset` says the tree is a bounded slice of a larger fixture (a few L1 shards),
//! so counts are compared as "at most the manifest's" rather than "equal to". State
//! the bound wherever the output is quoted; a subset does not establish that the whole
//! artefact is discoverable, only that its content is well formed where it was checked.

use s3_proxy::cache::CacheManager;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

fn usage() -> ! {
    eprintln!(
        "eviction_candidate_probe — run the real eviction candidate collector over a tree\n\
         \n\
         \x20 --cache-dir <dir>   cache root (containing metadata/ and ranges/)\n\
         \x20 --subset            the tree is a bounded slice of a larger fixture\n\
         \x20 --show <n>          print the first n candidates (default 0)\n"
    );
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cache_dir: Option<PathBuf> = None;
    let mut subset = false;
    let mut show = 0usize;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--cache-dir" => {
                cache_dir = Some(PathBuf::from(args.get(i + 1).unwrap_or_else(|| usage())));
                i += 2;
            }
            "--subset" => {
                subset = true;
                i += 1;
            }
            "--show" => {
                show = args
                    .get(i + 1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                i += 2;
            }
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown argument: {other}");
                usage();
            }
        }
    }
    let cache_dir = cache_dir.unwrap_or_else(|| {
        eprintln!("--cache-dir is required");
        usage()
    });

    println!("cache dir : {}", cache_dir.display());
    println!(
        "mode      : {}",
        if subset { "SUBSET" } else { "whole tree" }
    );

    // The manifest is optional: this probe is useful against any cache tree, not only
    // a fixture. When present it is the thing being checked against.
    let manifest: Option<serde_json::Value> =
        std::fs::read_to_string(cache_dir.join("FIXTURE_MANIFEST.json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
    if let Some(m) = &manifest {
        println!(
            "manifest  : gen_id={} ranges={} payload={} digest={}",
            m["gen_id"].as_str().unwrap_or("?"),
            m["ranges"],
            m["payload_mode"].as_str().unwrap_or("?"),
            m["content_digest"].as_str().unwrap_or("?")
        );
    } else {
        println!("manifest  : none found (agreement will not be checked)");
    }

    let cm = Arc::new(CacheManager::new_with_defaults(cache_dir.clone(), false, 0));
    let _disk = cm.create_configured_disk_cache_manager();
    if let Err(e) = cm.initialize().await {
        eprintln!("cache manager initialize failed: {e}");
        std::process::exit(1);
    }

    let started = std::time::Instant::now();
    let candidates = match cm.collect_range_candidates_for_eviction().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("collect_range_candidates_for_eviction failed: {e}");
            std::process::exit(1);
        }
    };
    let elapsed = started.elapsed().as_secs_f64();

    let mut sum_size: u64 = 0;
    let mut sum_compressed: u64 = 0;
    let mut min_compressed = u64::MAX;
    let mut max_compressed = 0u64;
    let mut keys: HashSet<&str> = HashSet::new();
    let mut missing_bin = 0usize;
    let mut non_fixture_keys = 0usize;
    let mut ident: HashSet<(&str, u64, u64)> = HashSet::new();
    for c in &candidates {
        sum_size += c.size;
        sum_compressed += c.compressed_size;
        min_compressed = min_compressed.min(c.compressed_size);
        max_compressed = max_compressed.max(c.compressed_size);
        keys.insert(c.cache_key.as_str());
        ident.insert((c.cache_key.as_str(), c.range_start, c.range_end));
        if !c.bin_file_path.exists() {
            missing_bin += 1;
        }
        if !c.cache_key.contains("s3hc-fixture") {
            non_fixture_keys += 1;
        }
    }
    if min_compressed == u64::MAX {
        min_compressed = 0;
    }

    println!("\n--- what the product collector discovered ---");
    println!("candidates              : {}", candidates.len());
    println!("distinct (key,start,end): {}", ident.len());
    println!("distinct cache keys     : {}", keys.len());
    println!("collection wall clock   : {elapsed:.2} s  (NOT a throughput result — see the");
    println!("                          module docs; selection cost is task 11/13)");
    println!("sum of `size`           : {sum_size}   <- STAT of the .bin on disk");
    println!("sum of `compressed_size`: {sum_compressed}   <- RECORDED figure in .meta");
    if sum_size != sum_compressed {
        let ratio = sum_compressed as f64 / sum_size.max(1) as f64;
        println!(
            "  these differ by {ratio:.1}x. Expected on a count-scale fixture: `size` is the\n\
             \x20 stub and `compressed_size` is the realistic size. A byte-target measurement\n\
             \x20 over this tree MUST use compressed_size."
        );
    }
    println!("compressed_size min/max : {min_compressed} / {max_compressed}");

    let mut failures: Vec<String> = Vec::new();

    println!("\n--- structural ---");
    let dup = candidates.len().saturating_sub(ident.len());
    report(
        &mut failures,
        "no duplicate (key,start,end) triples",
        dup == 0,
        &format!("{dup} duplicates"),
    );
    report(
        &mut failures,
        "every candidate's .bin resolves to an existing file",
        missing_bin == 0,
        &format!("{missing_bin} missing"),
    );
    // Negative control: without it the count assertions below could be satisfied by a
    // scan that reported everything it walked past, which is the false-positive shape
    // this repo has recorded four instances of.
    report(
        &mut failures,
        "every key carries the fixture prefix (nothing foreign was swept in)",
        non_fixture_keys == 0,
        &format!("{non_fixture_keys} foreign keys"),
    );

    if let Some(m) = &manifest {
        let expected = m["ranges"].as_u64().unwrap_or(0);
        let expected_objects = m["objects"].as_u64().unwrap_or(0);
        println!("\n--- manifest agreement ---");
        if subset {
            report(
                &mut failures,
                "candidates <= manifest ranges (subset)",
                (candidates.len() as u64) <= expected,
                &format!("{} > {expected}", candidates.len()),
            );
            report(
                &mut failures,
                "candidates > 0 (the subset actually contained content)",
                !candidates.is_empty(),
                "empty",
            );
            println!(
                "  BOUND: this is {:.2}% of the fixture's {expected} ranges. It establishes that\n\
                 \x20 product code parses, path-resolves and stats REAL fixture content on this\n\
                 \x20 backend. It does NOT establish anything about the other {:.2}%, nor about\n\
                 \x20 selection cost at full scale.",
                candidates.len() as f64 / expected.max(1) as f64 * 100.0,
                100.0 - candidates.len() as f64 / expected.max(1) as f64 * 100.0
            );
        } else {
            report(
                &mut failures,
                "candidates == manifest ranges",
                candidates.len() as u64 == expected,
                &format!("{} != {expected}", candidates.len()),
            );
            report(
                &mut failures,
                "distinct keys == manifest objects",
                keys.len() as u64 == expected_objects,
                &format!("{} != {expected_objects}", keys.len()),
            );
            let man_compressed = m["recorded_compressed_bytes"].as_u64().unwrap_or(0);
            report(
                &mut failures,
                "sum of compressed_size == manifest recorded_compressed_bytes",
                sum_compressed == man_compressed,
                &format!("{sum_compressed} != {man_compressed}"),
            );
        }
    }

    for c in candidates.iter().take(show) {
        println!(
            "  {} [{}..{}] size={} compressed={} access={} bin={}",
            c.cache_key,
            c.range_start,
            c.range_end,
            c.size,
            c.compressed_size,
            c.access_count,
            c.bin_file_path.display()
        );
    }

    println!("\n=== VERDICT ===");
    if failures.is_empty() {
        println!("PASS: the product collector discovered this tree cleanly.");
    } else {
        println!("FAIL: {}", failures.join("; "));
        std::process::exit(1);
    }
}

fn report(failures: &mut Vec<String>, name: &str, ok: bool, detail: &str) {
    println!("  {} {name}", if ok { "OK  " } else { "FAIL" });
    if !ok {
        println!("       {detail}");
        failures.push(name.to_string());
    }
}
