//! Cached bytes are fresh for `get_ttl` from when they were fetched, on every serve path.
//!
//! Each test drives a real `HttpProxy` in front of a mock origin and counts the requests that
//! reach the origin. A cached range read again after `get_ttl` has passed must go to the origin,
//! whether the range has been folded into a `.meta` since, is still a pending journal `Add`, or
//! has been promoted to RAM as a widened page.
#[path = "common/proxy_rig.rs"]
mod proxy_rig;

use std::time::Duration;

use proxy_rig::{Rig, RigOptions};

const OBJECT_LEN: usize = 4096;

/// Wait until the fill of `start..=end` is a pending journal `Add`, so the fill has committed
/// before the test starts timing its age.
async fn wait_pending(rig: &Rig, raw_path: &str, start: u64, end: u64) {
    let key = Rig::cache_key(raw_path);
    let until = tokio::time::Instant::now() + proxy_rig::STEP;
    loop {
        let pending = rig
            .disk
            .read()
            .await
            .find_pending_journal_ranges(&key, start, end)
            .await
            .unwrap_or_default();
        if pending.iter().any(|r| r.start <= start && r.end >= end) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "no pending journal Add covering {start}-{end} of {raw_path} within {:?}",
            proxy_rig::STEP
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn write_widening_rule(rig: &Rig, get_ttl: &str) {
    std::fs::write(
        rig.dir.path().join("cache_rules.json"),
        format!(
            r#"{{"rules":[{{"pattern":"**","page_widening":true,"page_size":1024,"get_ttl":"{get_ttl}"}}]}}"#
        ),
    )
    .unwrap();
}

/// A range fetched 1.5 s before the fold that first lists it, with `get_ttl` 1 s, is already
/// expired when the `.meta` appears: the `.meta` is stamped from the fetch, not the fold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_folded_after_its_ttl_is_fetched_again() {
    let rig = Rig::new(RigOptions {
        consolidate: false,
        get_ttl: Duration::from_secs(1),
        ..RigOptions::default()
    })
    .await;
    let path = "/test-bucket/fold-after-ttl.bin";
    let data = rig.mock.put(path, OBJECT_LEN);
    let (_, body) = rig
        .get(path, &[("range", "bytes=0-1023".to_string())])
        .await;
    assert_eq!(&body[..], &data[0..1024]);
    wait_pending(&rig, path, 0, 1023).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let consolidator = rig
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("shared-storage rig has a consolidator");
    consolidator
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    rig.wait_meta(path).await;
    let listed = rig
        .cache_manager
        .get_metadata_from_disk(&Rig::cache_key(path))
        .await
        .unwrap()
        .expect("the .meta");
    assert!(
        listed.ranges.iter().any(|r| r.start == 0 && r.end == 1023),
        "fixture: the fold must list the fetched range, so the origin hit below is the TTL's \
         doing and not the fold's"
    );
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=0-1023".to_string())])
        .await;
    assert_eq!(&body[..], &data[0..1024]);
    let after = rig.mock.hits(path);
    assert!(
        after > before,
        "a range fetched 1.5 s ago with get_ttl 1 s was served from the cache after the fold \
         (origin hits {before} before the read, {after} after)"
    );
}

/// Control for the test above: the same fill folded 1.5 s after its fetch, with `get_ttl` 3600 s,
/// is listed by the `.meta` and served from the cache, so the test above cannot pass because the
/// fold stopped listing the range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a2_a_range_folded_inside_its_ttl_is_served() {
    let rig = Rig::new(RigOptions {
        consolidate: false,
        get_ttl: Duration::from_secs(3600),
        ..RigOptions::default()
    })
    .await;
    let path = "/test-bucket/fold-inside-ttl.bin";
    let data = rig.mock.put(path, OBJECT_LEN);
    let (_, body) = rig
        .get(path, &[("range", "bytes=0-1023".to_string())])
        .await;
    assert_eq!(&body[..], &data[0..1024]);
    wait_pending(&rig, path, 0, 1023).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let consolidator = rig
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("shared-storage rig has a consolidator");
    consolidator
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    rig.wait_meta(path).await;
    let listed = rig
        .cache_manager
        .get_metadata_from_disk(&Rig::cache_key(path))
        .await
        .unwrap()
        .expect("the .meta");
    assert!(
        listed.ranges.iter().any(|r| r.start == 0 && r.end == 1023),
        "fixture: the fold must list the fetched range"
    );
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=0-1023".to_string())])
        .await;
    assert_eq!(&body[..], &data[0..1024]);
    assert_eq!(
        rig.mock.hits(path),
        before,
        "a range folded 1.5 s after its fetch with get_ttl 3600 s went to the origin"
    );
}
/// Page widening's journal arm: a widened page still pending in the journal, 2.5 s after its
/// fetch with `get_ttl` 1 s, is not coverage for another sub-range of the page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_widened_journal_page_past_its_ttl_is_fetched_again() {
    let rig = Rig::new(RigOptions {
        consolidate: false,
        ..RigOptions::default()
    })
    .await;
    write_widening_rule(&rig, "1s");
    let path = "/test-bucket/widened-journal-ttl.bin";
    let data = rig.mock.put(path, OBJECT_LEN);
    let (_, body) = rig.get(path, &[("range", "bytes=10-99".to_string())]).await;
    assert_eq!(&body[..], &data[10..=99]);
    wait_pending(&rig, path, 0, 1023).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=200-299".to_string())])
        .await;
    assert_eq!(&body[..], &data[200..=299]);
    let after = rig.mock.hits(path);
    assert!(
        after > before,
        "a widened page pending in the journal 2.5 s after its fetch with get_ttl 1 s was \
         served from the cache (origin hits {before} before the read, {after} after)"
    );
}

/// Control for `b`: inside a long `get_ttl` the pending widened page is still served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_widened_journal_page_inside_its_ttl_is_served() {
    let rig = Rig::new(RigOptions {
        consolidate: false,
        ..RigOptions::default()
    })
    .await;
    write_widening_rule(&rig, "3600s");
    let path = "/test-bucket/widened-journal-long-ttl.bin";
    let data = rig.mock.put(path, OBJECT_LEN);
    let (_, body) = rig.get(path, &[("range", "bytes=10-99".to_string())]).await;
    assert_eq!(&body[..], &data[10..=99]);
    wait_pending(&rig, path, 0, 1023).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=200-299".to_string())])
        .await;
    assert_eq!(&body[..], &data[200..=299]);
    let after = rig.mock.hits(path);
    assert_eq!(
        after, before,
        "a widened page pending in the journal inside get_ttl 3600 s went to the origin"
    );
}

/// Seed a widened page on disk, read it once more so it is promoted to RAM, and wait until RAM
/// holds it. Returns the object's bytes.
async fn seed_ram_page(rig: &Rig, path: &str) -> bytes::Bytes {
    let data = rig.mock.put(path, OBJECT_LEN);
    let (_, body) = rig.get(path, &[("range", "bytes=10-99".to_string())]).await;
    assert_eq!(&body[..], &data[10..=99]);
    rig.wait_meta(path).await;
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=200-299".to_string())])
        .await;
    assert_eq!(&body[..], &data[200..=299]);
    assert_eq!(
        rig.mock.hits(path),
        before,
        "the second read of a fresh widened page went to the origin"
    );
    let key = Rig::cache_key(path);
    let until = tokio::time::Instant::now() + proxy_rig::STEP;
    while rig
        .cache_manager
        .get_range_from_ram_cache_hit(&key, 0, 1023, Duration::from_secs(3600))
        .is_none()
    {
        assert!(
            tokio::time::Instant::now() < until,
            "the widened page was not promoted to RAM within {:?}",
            proxy_rig::STEP
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    data
}

/// A widened page promoted to RAM from the journal, before any `.meta` lists it, is not served
/// from RAM 1.5 s after its fetch with `get_ttl` 1 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_widened_ram_page_from_the_journal_past_its_ttl_is_fetched_again() {
    let rig = Rig::new(RigOptions {
        consolidate: false,
        ram_cache: true,
        ..RigOptions::default()
    })
    .await;
    write_widening_rule(&rig, "1s");
    let path = "/test-bucket/widened-ram-journal-ttl.bin";
    let data = rig.mock.put(path, OBJECT_LEN);
    let (_, body) = rig.get(path, &[("range", "bytes=10-99".to_string())]).await;
    assert_eq!(&body[..], &data[10..=99]);
    wait_pending(&rig, path, 0, 1023).await;
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=200-299".to_string())])
        .await;
    assert_eq!(&body[..], &data[200..=299]);
    assert_eq!(
        rig.mock.hits(path),
        before,
        "the second read of a fresh journal-pending widened page went to the origin"
    );
    let key = Rig::cache_key(path);
    let until = tokio::time::Instant::now() + proxy_rig::STEP;
    while rig
        .cache_manager
        .get_range_from_ram_cache_hit(&key, 0, 1023, Duration::from_secs(3600))
        .is_none()
    {
        assert!(
            tokio::time::Instant::now() < until,
            "the journal-pending widened page was not promoted to RAM within {:?}",
            proxy_rig::STEP
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=300-399".to_string())])
        .await;
    assert_eq!(&body[..], &data[300..=399]);
    let after = rig.mock.hits(path);
    assert!(
        after > before,
        "a widened page promoted to RAM from the journal, 1.5 s after its fetch with get_ttl \
         1 s, was served from the cache (origin hits {before} before the read, {after} after)"
    );
}

/// A widened page in RAM, 1.5 s after its fetch with `get_ttl` 1 s, is not served from RAM.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_widened_ram_page_past_its_ttl_is_fetched_again() {
    let rig = Rig::new(RigOptions {
        ram_cache: true,
        ..RigOptions::default()
    })
    .await;
    write_widening_rule(&rig, "1s");
    let path = "/test-bucket/widened-ram-ttl.bin";
    let data = seed_ram_page(&rig, path).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=300-399".to_string())])
        .await;
    assert_eq!(&body[..], &data[300..=399]);
    let after = rig.mock.hits(path);
    assert!(
        after > before,
        "a widened page in RAM 1.5 s after its fetch with get_ttl 1 s was served from the cache \
         (origin hits {before} before the read, {after} after)"
    );
}

/// Control for `d`: inside a long `get_ttl` the RAM page is still served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_widened_ram_page_inside_its_ttl_is_served() {
    let rig = Rig::new(RigOptions {
        ram_cache: true,
        ..RigOptions::default()
    })
    .await;
    write_widening_rule(&rig, "3600s");
    let path = "/test-bucket/widened-ram-long-ttl.bin";
    let data = seed_ram_page(&rig, path).await;
    let before = rig.mock.hits(path);
    let (_, body) = rig
        .get(path, &[("range", "bytes=300-399".to_string())])
        .await;
    assert_eq!(&body[..], &data[300..=399]);
    assert_eq!(
        rig.mock.hits(path),
        before,
        "a widened page in RAM inside get_ttl 3600 s went to the origin"
    );
}
