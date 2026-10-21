//! `s3-proxy bench-metadata` end to end, against a temporary cache directory: it completes,
//! reads every `.meta` file it is given, parses its own JSON, and writes nothing.

use s3_proxy::bench_metadata::BenchReport;
use s3_proxy::cache_types::{CompressionInfo, NewCacheMetadata, ObjectMetadata};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

const N: usize = 48;

/// Every path under `root` with its length, modification time and contents.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, (bool, u64, SystemTime, Vec<u8>)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = entry.path();
            let md = std::fs::symlink_metadata(&p).unwrap();
            let body = if md.is_file() {
                std::fs::read(&p).unwrap()
            } else {
                Vec::new()
            };
            if md.is_dir() {
                stack.push(p.clone());
            }
            out.insert(p, (md.is_dir(), md.len(), md.modified().unwrap(), body));
        }
    }
    out
}

fn write_cache(root: &Path) {
    let now = SystemTime::now();
    for i in 0..N {
        // Spread across L1 and L2 shards so enumeration crosses directories.
        let dir = root.join(format!("metadata/bench-bucket/{:02x}/{:03x}", i % 4, i % 3));
        std::fs::create_dir_all(&dir).unwrap();
        let meta = NewCacheMetadata {
            cache_key: format!("bench-bucket/object-{i}"),
            object_metadata: ObjectMetadata::default(),
            ranges: Vec::new(),
            created_at: now,
            expires_at: now,
            compression_info: CompressionInfo::default(),
            head_expires_at: None,
            head_last_accessed: None,
            head_access_count: 0,
            head_cached_at: None,
            last_validated_at: None,
        };
        std::fs::write(
            dir.join(format!("object-{i}.meta")),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
    }
    // Internal trees the command must skip, and a non-metadata tree it must ignore.
    std::fs::create_dir_all(root.join("metadata/_journals")).unwrap();
    std::fs::write(root.join("metadata/_journals/host:1.journal"), b"{}\n").unwrap();
    std::fs::create_dir_all(root.join("ranges/bench-bucket")).unwrap();
    std::fs::write(root.join("ranges/bench-bucket/x_0-9.bin"), b"0123456789").unwrap();
}

fn bench(cache_dir: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_s3-proxy"))
        .arg("bench-metadata")
        .arg("--cache-dir")
        .arg(cache_dir)
        .args(extra)
        .output()
        .expect("run s3-proxy bench-metadata")
}

#[test]
fn bench_metadata_reads_every_meta_and_writes_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let cache = tmp.path().join("cache");
    write_cache(&cache);
    let before = snapshot(tmp.path());

    // JSON, two levels: the files split into two disjoint sets of N/2.
    let out = bench(&cache, &["--concurrency", "1,4", "--json"]);
    assert!(
        out.status.success(),
        "exit {:?}, stderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let report: BenchReport =
        serde_json::from_slice(&out.stdout).expect("bench-metadata --json parses as BenchReport");
    assert_eq!(report.enumeration.paths_collected, N);
    assert_eq!(report.files_per_level, N / 2);
    assert_eq!(report.levels.len(), 2);
    assert_eq!(
        report
            .levels
            .iter()
            .map(|l| l.concurrency)
            .collect::<Vec<_>>(),
        vec![1, 4]
    );
    for level in &report.levels {
        assert_eq!(level.files_read, N / 2, "level {}", level.concurrency);
        assert_eq!(level.errors, 0, "level {}", level.concurrency);
        assert!(level.objects_per_s > 0.0);
    }

    // Text, one level: every file in one pass.
    let out = bench(&cache, &["--concurrency", "8"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("obj/s"), "{text}");
    assert!(text.contains(&format!("{N} paths collected")), "{text}");

    let after = snapshot(tmp.path());
    let added: Vec<_> = after.keys().filter(|k| !before.contains_key(*k)).collect();
    assert!(added.is_empty(), "bench-metadata created {added:?}");
    assert!(
        before == after,
        "bench-metadata changed the cache tree (lengths, mtimes or contents differ)"
    );
}

#[test]
fn bench_metadata_refuses_a_missing_cache_dir_and_bad_flags() {
    let tmp = tempfile::TempDir::new().unwrap();
    let out = bench(&tmp.path().join("absent"), &[]);
    assert_eq!(out.status.code(), Some(1));
    let out = bench(tmp.path(), &["--concurrency", "zero"]);
    assert_eq!(out.status.code(), Some(2));
}
