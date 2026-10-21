//! The danger-band publication guard, asserted **at the boundaries it is wired into**.
//!
//! Spec: `cache-eviction-at-scale` task 36 (A4). `src/capacity_guard.rs`'s own unit tests
//! cover the guard's mechanism — the band edge, the single-flighted refresh, the fleet-wide
//! sum, the lease. This file covers a different property, and it is the one A4 is about:
//! that each enumerated publication boundary **honours** a decline.
//!
//! # Why this is not the same test as the mechanism tests
//!
//! A boundary can call the guard, read a decline, and publish anyway. The mechanical
//! `every_enumerated_publication_path_is_wired_at_a_boundary` test proves the call exists;
//! nothing there proves the answer is acted on. That distinction is exactly the recorded
//! failure mode in this repo — an assertion that measures an adjacent quantity — so the
//! assertions below read the **filesystem**: no `.bin`, no `.meta`, no `.tmp`. A returned
//! `Ok(None)` is a claim; an absent file is the effect.
//!
//! # Forcing a decline deterministically
//!
//! Every arm sets an unsatisfiable free-space floor (`u64::MAX / 8`), so the band edge is
//! above any real volume's free space and the exact in-band check cannot fit the fill. That
//! is a decline for the **capacity** reason and no other: the fixtures are small, well
//! inside every other gate, and the admitting arms in this file prove the same fixtures are
//! published when the floor permits it.
//!
//! # One temp dir per arm, deliberately
//!
//! `capacity_guard::guard_for` caches one guard per cache directory and keeps the floor it
//! was first created with — correct in production, where the floor is startup
//! configuration and cannot change within a process. So an admitting arm and a declining
//! arm cannot share a directory, and each arm below gets its own.

use s3_proxy::cache_types::{ObjectMetadata, UploadState};
use s3_proxy::disk_cache::DiskCacheManager;
use std::time::Duration;
use tempfile::TempDir;

/// An unsatisfiable floor: the band edge exceeds any volume's free space, so every fill
/// enters the danger band and then fails the exact check.
const UNSATISFIABLE_FLOOR: u64 = u64::MAX / 8;

/// A floor of `0` disables the guard, which is what an unconfigured manager gets.
const GUARD_DISABLED: u64 = 0;

async fn manager(temp: &TempDir, floor: u64) -> DiskCacheManager {
    let mut m = DiskCacheManager::new(temp.path().to_path_buf(), true, 1024, false, 1_048_576);
    m.set_disk_safety_free_space_floor(floor);
    m.initialize().await.unwrap();
    m
}

fn metadata(content_length: u64) -> ObjectMetadata {
    ObjectMetadata {
        etag: "guard-test-etag".to_string(),
        last_modified: "Wed, 21 Oct 2015 07:28:00 GMT".to_string(),
        content_length,
        content_type: Some("application/octet-stream".to_string()),
        upload_state: UploadState::Complete,
        cumulative_size: content_length,
        parts: Vec::new(),
        ..Default::default()
    }
}

/// **P1 — `store_range`.** A declined buffered range publishes nothing.
///
/// A failure implicates the `if let Some(detail) = admission.decline() { return Ok(()) }`
/// block in `DiskCacheManager::store_range` (`src/disk_cache.rs`). The assertion reads the
/// two files that block prevents — the `.bin` and the `.meta` — rather than the returned
/// `Ok(())`, because `Ok(())` is also what a successful store returns. That is the whole
/// point: the return value cannot discriminate, so the filesystem has to.
///
/// This is also the arm that covers **P2**, **P5** and **P6**, which reach disk through
/// this function and have no write of their own.
#[tokio::test]
async fn p1_store_range_declined_publishes_no_bytes_and_no_metadata() {
    let temp = TempDir::new().unwrap();
    let mut m = manager(&temp, UNSATISFIABLE_FLOOR).await;
    let key = "guard-bucket/p1-declined";
    let data = vec![7u8; 4096];

    let result = m
        .store_range(
            key,
            0,
            4095,
            &data,
            metadata(4096),
            Duration::from_secs(3600),
            true,
        )
        .await;

    // A decline is NOT an error: the client's request is untouched and only the cache copy
    // is skipped, so the caller must not see a failure it would log or propagate.
    assert!(
        result.is_ok(),
        "a capacity decline must not surface as an error: {:?}",
        result.err()
    );
    assert!(
        !m.get_new_range_file_path(key, 0, 4095).exists(),
        "the range .bin must not exist after a declined store_range"
    );
    assert!(
        !m.get_new_metadata_file_path(key).exists(),
        "the .meta must not exist after a declined store_range"
    );
}

/// **P1, admitting arm.** The same fixture, the same call, a permitting floor — published.
///
/// This is what makes the declining arm above attributable. Without it, a `store_range`
/// that was broken for some unrelated reason would produce identical assertions, and the
/// declining arm would pass while proving nothing about the guard.
#[tokio::test]
async fn p1_store_range_admitted_publishes_normally() {
    let temp = TempDir::new().unwrap();
    let mut m = manager(&temp, GUARD_DISABLED).await;
    let key = "guard-bucket/p1-admitted";
    let data = vec![7u8; 4096];

    m.store_range(
        key,
        0,
        4095,
        &data,
        metadata(4096),
        Duration::from_secs(3600),
        true,
    )
    .await
    .expect("an unguarded store must succeed");

    assert!(
        m.get_new_range_file_path(key, 0, 4095).exists(),
        "the range .bin must exist when the guard admits"
    );
}

/// **P3a — `begin_incremental_range_write`.** A declined GET-response tee opens no file.
///
/// A failure implicates the decline block in `begin_incremental_range_write`
/// (`src/disk_cache.rs`). Asserted two ways, because either alone is weak: the returned
/// value must be `Ok(None)` — `Ok` because a decline is not a failure, `None` because there
/// is no writer — **and** no `.tmp` may exist under `ranges/`, which is the effect the
/// caller cannot fake.
///
/// The `.tmp` check matters specifically: `begin_*` creates the temp file with
/// `create_new(true)` before returning, so a guard placed *after* that call would leave an
/// orphan `.tmp` on every declined fill and this assertion would catch it.
#[tokio::test]
async fn p3a_incremental_range_write_declined_opens_no_temp_file() {
    let temp = TempDir::new().unwrap();
    let m = manager(&temp, UNSATISFIABLE_FLOOR).await;
    let key = "guard-bucket/p3a-declined";

    let writer = m
        .begin_incremental_range_write(key, 0, 65_535, true)
        .await
        .expect("a capacity decline must not surface as an error");

    assert!(
        writer.is_none(),
        "the guard declined, so no writer may be handed back"
    );
    assert_eq!(
        count_files_with_extension(&temp.path().join("ranges"), "tmp"),
        0,
        "a declined write must not leave an orphaned .tmp — the guard has to run BEFORE \
         the temp file is created"
    );
}

/// **P3a, admitting arm.** A permitting floor hands back a writer and creates its `.tmp`.
#[tokio::test]
async fn p3a_incremental_range_write_admitted_returns_a_writer() {
    let temp = TempDir::new().unwrap();
    let m = manager(&temp, GUARD_DISABLED).await;
    let key = "guard-bucket/p3a-admitted";

    let writer = m
        .begin_incremental_range_write(key, 0, 65_535, true)
        .await
        .expect("an unguarded begin must succeed");
    assert!(
        writer.is_some(),
        "the guard is disabled, so this must admit"
    );
    assert_eq!(
        count_files_with_extension(&temp.path().join("ranges"), "tmp"),
        1,
        "an admitted write opens exactly one .tmp"
    );
    DiskCacheManager::abort_incremental_range(writer.unwrap());
}

/// **P3c / P8 — `begin_incremental_part_write`.** A declined `UploadPart` stages nothing.
///
/// A failure implicates the decline block in `begin_incremental_part_write`
/// (`src/disk_cache.rs`). `u4-get-publication.md` records that this path applied none of
/// the write-cache sink's three gates, and that its only size control
/// (`check_streaming_capacity`) fires **only** when Content-Length is absent — so a
/// properly sized part bypassed everything. The hint passed here is `Some(..)`, i.e.
/// exactly the case that check skips.
#[tokio::test]
async fn p3c_incremental_part_write_declined_stages_nothing() {
    let temp = TempDir::new().unwrap();
    let m = manager(&temp, UNSATISFIABLE_FLOOR).await;
    let part_path = temp
        .path()
        .join("mpus_in_progress")
        .join("guard-upload")
        .join("part1.bin");

    let writer = m
        .begin_incremental_part_write(
            part_path.clone(),
            "guard-bucket/p3c-declined",
            true,
            Some(8 * 1024 * 1024),
        )
        .await
        .expect("a capacity decline must not surface as an error");

    assert!(
        writer.is_none(),
        "the guard declined, so no part writer may be handed back"
    );
    assert!(
        !part_path.exists(),
        "the part .bin must not exist after a declined part write"
    );
    assert_eq!(
        count_files_with_extension(part_path.parent().unwrap(), "tmp"),
        0,
        "a declined part write must not leave an orphaned .tmp"
    );
}

/// **P3c / P8, admitting arm**, and it also pins the documented residual: with **no**
/// Content-Length hint the incoming figure is 0 and the guard admits.
///
/// That is deliberate rather than a hole — the guard cannot reserve against a size nobody
/// knows, and inventing one would either refuse legitimate parts or reserve a fiction. It
/// is asserted here so a future change that starts refusing hint-less parts fails loudly
/// and names the decision, instead of silently disabling caching for chunked uploads.
#[tokio::test]
async fn p3c_incremental_part_write_with_no_size_hint_is_admitted_even_at_an_unsatisfiable_floor() {
    let temp = TempDir::new().unwrap();
    let m = manager(&temp, UNSATISFIABLE_FLOOR).await;
    let part_path = temp
        .path()
        .join("mpus_in_progress")
        .join("guard-upload-nohint")
        .join("part1.bin");

    let writer = m
        .begin_incremental_part_write(part_path.clone(), "guard-bucket/p3c-nohint", true, None)
        .await
        .expect("no error");

    assert!(
        writer.is_some(),
        "with no Content-Length the incoming figure is 0, which admits — this is the \
         documented residual that `check_streaming_capacity` covers instead"
    );
    DiskCacheManager::abort_incremental_range(writer.unwrap());
}

/// Count files whose name ends in `.{ext}` anywhere under `dir`. Absent dir counts as 0.
///
/// Recursive because `ranges/` is BLAKE3-sharded three levels deep, so a non-recursive
/// listing of it would find nothing and this helper would report 0 for every case —
/// passing the declining arms vacuously and failing the admitting ones, which is how the
/// mistake would announce itself.
fn count_files_with_extension(dir: &std::path::Path, ext: &str) -> usize {
    fn walk(dir: &std::path::Path, ext: &str, count: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, ext, count);
            } else if path.file_name().is_some_and(|n| {
                // Range temps end `.tmp.{host}.{pid}`, so match the infix form too.
                let n = n.to_string_lossy();
                n.ends_with(&format!(".{ext}")) || n.contains(&format!(".{ext}."))
            }) {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    walk(dir, ext, &mut count);
    count
}
