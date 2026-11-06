//! Range slicing across cached ranges, with the ranges made by ranged GETs through a real
//! `HttpProxy`.
//!
//! When a cached range is larger than the requested range the proxy must return exactly the
//! requested bytes: the cached data is sliced, and slices from several ranges are
//! concatenated. A request that spans a gap must report the gap as missing.
//!
//! The cached state is what the request path's range fills leave, so these tests see the
//! extents production stores (a fill that overlaps a cached extent stores only the missing part,
//! and the lookup still covers the request).
#[path = "common/proxy_rig.rs"]
mod proxy_rig;

use proxy_rig::{Rig, RigOptions};
use s3_proxy::cache_types::RangeLookupPurpose;
use s3_proxy::range_handler::RangeSpec;
use std::time::Duration;

#[tokio::test]
async fn test_range_slice_multiple_cached_ranges() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/test-bucket/slice-multiple.bin";
    let object = rig.mock.put(path, 4000);
    let cache_key = Rig::cache_key(path);
    // Three overlapping ranged GETs: 0-1099, 1000-2099 and 2000-3099.
    for (start, end) in [(0u64, 1099u64), (1000, 2099), (2000, 3099)] {
        let body = rig
            .get(path, &[("range", format!("bytes={}-{}", start, end))])
            .await
            .1;
        assert_eq!(body, object.slice(start as usize..=end as usize));
        // Let the fill land before the next request so each one sees the earlier ones.
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    // Client requests bytes 0-2999. Wait until the cache covers it.
    let requested = RangeSpec {
        start: 0,
        end: 2999,
    };
    let until = tokio::time::Instant::now() + proxy_rig::STEP;
    let overlap = loop {
        let overlap = rig
            .range_handler
            .find_cached_ranges(
                &cache_key,
                &requested,
                None,
                None,
                RangeLookupPurpose::FreshServe,
            )
            .await
            .unwrap();
        if overlap.missing_ranges.is_empty() {
            break overlap;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "the three fills did not cover 0-2999 within {:?}: missing {:?}",
            proxy_rig::STEP,
            overlap.missing_ranges.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve from cache"
    );
    assert!(
        overlap.cached_ranges.len() >= 3,
        "three fills cannot cover 0-2999 with fewer than three ranges, got {}",
        overlap.cached_ranges.len()
    );
    // Merging slices each cached range to the request and concatenates them.
    let merged = rig
        .range_handler
        .merge_range_segments(&cache_key, &requested, &overlap.cached_ranges, &[])
        .await
        .unwrap();
    assert_eq!(
        merged.data.len(),
        3000,
        "Merged data should be exactly the requested size"
    );
    assert_eq!(
        &merged.data[..],
        &object[0..3000],
        "Merged data should match the object's bytes 0-2999"
    );
    assert_eq!(
        merged.segments_merged,
        overlap.cached_ranges.len(),
        "Every cached range should have been merged"
    );
    assert_eq!(
        merged.bytes_from_cache, 3000,
        "All 3000 bytes should be from cache"
    );
    assert_eq!(merged.bytes_from_s3, 0, "No bytes should be from S3");
    assert_eq!(
        merged.cache_efficiency, 100.0,
        "Cache efficiency should be 100%"
    );
}

#[tokio::test]
async fn test_range_slice_multiple_cached_ranges_with_gaps() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/test-bucket/slice-gaps.bin";
    rig.mock.put(path, 5100);
    let cache_key = Rig::cache_key(path);
    // Cached: 0-1999 and 3000-5099. Not cached: 2000-2999.
    rig.seed_range(path, 0, 1999).await;
    rig.seed_range(path, 3000, 5099).await;
    let overlap = rig
        .range_handler
        .find_cached_ranges(
            &cache_key,
            &RangeSpec {
                start: 0,
                end: 4999,
            },
            None,
            None,
            RangeLookupPurpose::FreshServe,
        )
        .await
        .unwrap();
    assert_eq!(
        overlap.cached_ranges.len(),
        2,
        "Should find 2 cached ranges"
    );
    assert_eq!(
        overlap.missing_ranges.len(),
        1,
        "Should have 1 missing range"
    );
    assert!(
        !overlap.can_serve_from_cache,
        "Should NOT be able to serve entirely from cache due to gap"
    );
    assert_eq!(overlap.missing_ranges[0].start, 2000);
    assert_eq!(overlap.missing_ranges[0].end, 2999);
}
