//! CLI front end for the graded cache-tree fixture generator.
//!
//! Spec: `cache-eviction-at-scale`, task 8. Requirements: 13.1.
//!
//! The generator itself lives in `tests/common/graded_fixture.rs` and is pulled in
//! here with `#[path]`. That split is deliberate: the module has to be reachable
//! from `tests/`, because task 8's validation requirement is that the fixture is
//! read back through **product** code (`CacheManager::collect_range_candidates_for_eviction`)
//! inside the gated test suite, and `cargo test` does not run `#[test]`s that live
//! in an example. Keeping one copy of the generator and two entry points avoids the
//! alternative, which is a generator whose CLI and whose tested path are different
//! code.
//!
//! # This does NOT build Fixture A or Fixture B
//!
//! Those are tasks 9 and 10 and they need an infrastructure decision that has not
//! been taken. This binary is the tool; the two artefacts are separate work. Use
//! `--dry-run` to print the plan and the projected footprint without writing.
//!
//! # Usage
//!
//! ```text
//! cargo run --release --example graded_cache_fixture_gen -- \
//!     --out tmp/fixture-small --objects 2000 --seed 1 --payload genuine
//!
//! # count-scale shape (Fixture A's mode), stub payloads, realistic recorded sizes
//! cargo run --release --example graded_cache_fixture_gen -- \
//!     --out tmp/fixture-count --objects 200000 --max-ranges 5 --payload stub:512
//!
//! # R13.3 degenerate shapes
//! cargo run --release --example graded_cache_fixture_gen -- \
//!     --out tmp/fixture-uniform --objects 5000 --distribution uniform:65536
//! cargo run --release --example graded_cache_fixture_gen -- \
//!     --out tmp/fixture-dominant --objects 5000 --distribution dominant:4096:536870912
//! ```

#[path = "../tests/common/graded_fixture.rs"]
mod graded_fixture;

use graded_fixture::{FixtureSpec, GenOptions, PayloadMode, SizeDistribution, DEFAULT_TAIL_CAP};
use std::path::PathBuf;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "graded_cache_fixture_gen — emit a schema-valid graded cache tree\n\
         \n\
         Required:\n\
         \x20 --out <dir>                  output root (must not be a real cache dir)\n\
         \n\
         Optional:\n\
         \x20 --objects <n>                distinct objects / .meta files (default 1000)\n\
         \x20 --max-ranges <n>             max ranges per object (default 4)\n\
         \x20 --seed <u64>                 default 0x5eed123456789abc\n\
         \x20 --label <s>                  default 'graded'\n\
         \x20 --bucket <s>                 default 'fixture-<label>'\n\
         \x20 --payload genuine|stub:<n>   default genuine\n\
         \x20 --distribution graded|graded:cap:<n>|uniform:<n>|dominant:<bg>:<dom>\n\
         \x20 --staged-fraction <f>        default 0.0\n\
         \x20 --min-age-secs <n>           default 3600 (must exceed the 60s admission window)\n\
         \x20 --max-age-secs <n>           default 2592000\n\
         \x20 --tail-cap <n>               default {DEFAULT_TAIL_CAP}\n\
         \x20 --threads <n>                worker threads (default 1 = the serial reference)\n\
         \x20 --no-dir-memo                re-run create_dir_all per file; slower, identical output\n\
         \x20 --ack-shared-storage <dir>   allow a shared-mount --out; must repeat --out exactly.\n\
         \x20                              Stop the proxy fleet first. Never permits the live\n\
         \x20                              cache dirs, which are refused unconditionally.\n\
         \x20 --dry-run                    print the plan and projected footprint, write nothing\n\
         \n\
         --threads and --no-dir-memo are EXECUTION options: they change how long the\n\
         run takes and nothing about what is written. A tree generated at --threads 8\n\
         has the same content_digest as the same spec at --threads 1; the test\n\
         parallel_generation_is_identical_to_serial_generation holds that line.\n"
    );
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut out: Option<PathBuf> = None;
    let mut spec = FixtureSpec::default();
    let mut bucket_override: Option<String> = None;
    let mut dry_run = false;
    let mut opts = GenOptions::default();

    let mut i = 0;
    while i < args.len() {
        let need = |i: usize| -> String {
            args.get(i + 1).cloned().unwrap_or_else(|| {
                eprintln!("missing value for {}", args[i]);
                usage()
            })
        };
        match args[i].as_str() {
            "--out" => {
                out = Some(PathBuf::from(need(i)));
                i += 2;
            }
            "--objects" => {
                spec.objects = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--max-ranges" => {
                spec.max_ranges_per_object = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--seed" => {
                let v = need(i);
                spec.seed = if let Some(hex) = v.strip_prefix("0x") {
                    u64::from_str_radix(hex, 16).unwrap_or_else(|_| usage())
                } else {
                    v.parse().unwrap_or_else(|_| usage())
                };
                i += 2;
            }
            "--label" => {
                spec.label = need(i);
                i += 2;
            }
            "--bucket" => {
                bucket_override = Some(need(i));
                i += 2;
            }
            "--payload" => {
                let v = need(i);
                spec.payload = if v == "genuine" {
                    PayloadMode::Genuine
                } else if let Some(n) = v.strip_prefix("stub:") {
                    PayloadMode::RecordedSizeOnly {
                        stub_bytes: n.parse().unwrap_or_else(|_| usage()),
                    }
                } else {
                    eprintln!("--payload must be 'genuine' or 'stub:<bytes>'");
                    usage()
                };
                i += 2;
            }
            "--distribution" => {
                let v = need(i);
                spec.distribution = parse_distribution(&v);
                i += 2;
            }
            "--staged-fraction" => {
                spec.staged_fraction = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--min-age-secs" => {
                spec.min_age = Duration::from_secs(need(i).parse().unwrap_or_else(|_| usage()));
                i += 2;
            }
            "--max-age-secs" => {
                spec.max_age = Duration::from_secs(need(i).parse().unwrap_or_else(|_| usage()));
                i += 2;
            }
            "--tail-cap" => {
                spec.tail_cap = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--threads" => {
                opts.threads = need(i).parse().unwrap_or_else(|_| usage());
                i += 2;
            }
            "--no-dir-memo" => {
                opts.dir_memo = false;
                i += 1;
            }
            "--ack-shared-storage" => {
                opts.shared_storage_ack = Some(PathBuf::from(need(i)));
                i += 2;
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown argument: {other}");
                usage();
            }
        }
    }

    spec.bucket = bucket_override.unwrap_or_else(|| format!("fixture-{}", spec.label));
    let out = out.unwrap_or_else(|| {
        eprintln!("--out is required");
        usage()
    });

    if let Err(e) = spec.validate() {
        eprintln!("invalid spec: {e}");
        std::process::exit(1);
    }

    let (span_lo, span_hi) = spec.distribution.span();
    println!("gen_id          : {}", spec.gen_id());
    println!("out             : {}", out.display());
    println!("seed            : {:#018x}", spec.seed);
    println!("objects         : {}", spec.objects);
    println!("max ranges/obj  : {}", spec.max_ranges_per_object);
    println!("payload mode    : {}", spec.payload.label());
    println!(
        "threads         : {} ({})",
        opts.threads,
        if opts.threads <= 1 {
            "serial reference"
        } else {
            "parallel"
        }
    );
    println!("dir memo        : {}", opts.dir_memo);
    println!("size span       : {span_lo} .. {span_hi} bytes");
    println!(
        "ages            : {}s .. {}s",
        spec.min_age.as_secs(),
        spec.max_age.as_secs()
    );

    if dry_run {
        println!("\n--dry-run: nothing written.");
        return;
    }

    match graded_fixture::generate_with(&out, &spec, &opts) {
        Ok(fixture) => {
            let m = &fixture.manifest;
            println!("\n--- generated ---");
            println!("objects           : {}", m.objects);
            println!("ranges            : {}", m.ranges);
            println!(
                "recorded bytes    : {} ({:.2} GiB)",
                m.recorded_compressed_bytes,
                m.recorded_compressed_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );
            println!(
                "on-disk bytes     : {} ({:.2} GiB)",
                m.on_disk_bytes,
                m.on_disk_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );
            println!("digest            : {}", m.content_digest);
            println!("\nclasses:");
            for c in &m.classes {
                println!(
                    "  {:<10} ranges={:<10} recorded={:<16} min={:<11} max={}",
                    c.name, c.ranges, c.recorded_bytes, c.min_recorded, c.max_recorded
                );
            }
            println!("\ntail:");
            println!(
                "  >=1MiB   ranges={:<10} bytes={}",
                m.tail.ranges_at_least_1mib, m.tail.bytes_at_least_1mib
            );
            println!(
                "  >=8MiB   ranges={:<10} bytes={}",
                m.tail.ranges_at_least_8mib, m.tail.bytes_at_least_8mib
            );
            println!(
                "  >=64MiB  ranges={:<10} bytes={}",
                m.tail.ranges_at_least_64mib, m.tail.bytes_at_least_64mib
            );
            println!("  largest recorded range: {}", m.tail.largest_recorded);
            println!(
                "  tail@{} share of recorded bytes: {:.4}",
                m.tail_cap, m.tail.tail_share_of_recorded_bytes
            );
            println!("\nshards: {} L1 shards populated", m.shards.len());
            println!(
                "  count CV: {:.2}%   bytes CV: {:.2}%",
                m.shard_count_cv_percent, m.shard_bytes_cv_percent
            );
            println!("\nthroughput (MEASURE THIS, do not estimate it):");
            println!(
                "  threads        : {}  dir_memo: {}",
                m.generation_threads, m.dir_memo
            );
            println!("  elapsed        : {:.3} s", m.throughput.elapsed_secs);
            println!("  ranges/s       : {:.1}", m.throughput.ranges_per_sec);
            println!(
                "  objects/s      : {:.1}",
                m.objects as f64 / m.throughput.elapsed_secs.max(1e-9)
            );
            println!(
                "  on-disk MiB/s  : {:.2}",
                m.throughput.on_disk_bytes_per_sec / (1024.0 * 1024.0)
            );
            println!(
                "  recorded MiB/s : {:.2}",
                m.throughput.recorded_bytes_per_sec / (1024.0 * 1024.0)
            );
            println!(
                "\nmanifest written to {}/FIXTURE_MANIFEST.json",
                out.display()
            );
        }
        Err(e) => {
            eprintln!("generation failed: {e}");
            std::process::exit(1);
        }
    }
}

fn parse_distribution(v: &str) -> SizeDistribution {
    if v == "graded" {
        return SizeDistribution::graded();
    }
    if let Some(rest) = v.strip_prefix("graded:cap:") {
        let cap: u64 = rest.parse().unwrap_or_else(|_| usage());
        if let SizeDistribution::Graded { classes, .. } = SizeDistribution::graded() {
            return SizeDistribution::Graded {
                classes,
                size_cap: Some(cap),
            };
        }
        unreachable!("graded() returns Graded");
    }
    if let Some(rest) = v.strip_prefix("uniform:") {
        return SizeDistribution::Uniform {
            size: rest.parse().unwrap_or_else(|_| usage()),
        };
    }
    if let Some(rest) = v.strip_prefix("dominant:") {
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() != 2 {
            eprintln!("--distribution dominant:<background>:<dominant>");
            usage();
        }
        return SizeDistribution::SingleDominant {
            background: parts[0].parse().unwrap_or_else(|_| usage()),
            dominant: parts[1].parse().unwrap_or_else(|_| usage()),
        };
    }
    eprintln!("unrecognised --distribution: {v}");
    usage()
}
