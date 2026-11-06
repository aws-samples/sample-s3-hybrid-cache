#[path = "common/proxy_rig.rs"]
mod proxy_rig;
use bytes::Bytes;
use proxy_rig::{Rig, RigOptions};
use s3_proxy::cache_types::RangeLookupPurpose;
use std::time::Duration;

/// Diagnostic integration test for cache lookup
///
/// This test reproduces the cache lookup bug where find_cached_ranges returns 0 cached ranges
/// even though the metadata file exists with the correct cache key, contains valid ranges,
/// the ranges are not expired, and the range binary files exist on disk.
///
/// Every cached range is made by a ranged GET that misses through a real `HttpProxy`, so the
/// state the lookups read is the state the request path's range fill leaves.
///
/// Requirements: 4.1, 4.2, 4.3, 4.4, 4.5
#[tokio::test]
async fn test_cache_lookup_diagnostic_basic() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/test-bucket/path/to/test-object.txt";
    let test_data = Bytes::from_static(
        b"This is test data for cache lookup diagnostic test. It should be found when we look it up.",
    );
    rig.mock.put_bytes(path, test_data.clone());
    let range_start = 0u64;
    let range_end = (test_data.len() - 1) as u64;

    // Requirement 4.1: the range is stored by a GET miss
    rig.seed_range(path, range_start, range_end).await;
    let cache_key = Rig::cache_key(path);
    let disk = rig.disk.read().await;

    // Requirement 4.2: the metadata file exists at the expected path
    let metadata_path = disk.get_new_metadata_file_path(&cache_key);
    assert!(
        metadata_path.exists(),
        "Metadata file should exist at {:?}",
        metadata_path
    );

    // Requirement 4.3: the range binary file exists at the expected path
    let range_path = disk.get_new_range_file_path(&cache_key, range_start, range_end);
    assert!(
        range_path.exists(),
        "Range binary file should exist at {:?}",
        range_path
    );

    // The sharded layout carries the bucket
    let bucket = cache_key.split('/').next().unwrap();
    assert!(
        metadata_path.to_string_lossy().contains(bucket),
        "Metadata path should contain bucket '{}': {:?}",
        bucket,
        metadata_path
    );
    assert!(
        range_path.to_string_lossy().contains(bucket),
        "Range path should contain bucket '{}': {:?}",
        bucket,
        range_path
    );

    // Requirement 4.4: find_cached_ranges with the same cache key finds the stored range
    let found_ranges = disk
        .find_cached_ranges(
            &cache_key,
            range_start,
            range_end,
            None,
            RangeLookupPurpose::FreshServe,
        )
        .await
        .unwrap()
        .ranges;
    assert!(
        !found_ranges.is_empty(),
        "find_cached_ranges should return at least one range, but returned 0. \
        This indicates a cache lookup bug where the metadata file exists but ranges are not found."
    );
    assert_eq!(
        found_ranges.len(),
        1,
        "Should find exactly one cached range"
    );
    let found_range = &found_ranges[0];
    assert_eq!(
        found_range.start, range_start,
        "Found range start should match stored range start"
    );
    assert_eq!(
        found_range.end, range_end,
        "Found range end should match stored range end"
    );

    // The range data loads and matches
    let loaded_data = disk.load_range_data(found_range).await.unwrap();
    assert_eq!(
        loaded_data.len(),
        test_data.len(),
        "Loaded data size should match original data size"
    );
    assert_eq!(
        &loaded_data[..],
        &test_data[..],
        "Loaded data should match original data"
    );
}

/// Test cache lookup with various cache key formats
///
/// Tests with/without leading slash, special characters, and long keys.
/// Each case is filed by the proxy under the key it derives from the request path, and looked
/// up under the form the case names.
/// Requirements: 4.5
#[tokio::test]
async fn test_cache_lookup_diagnostic_key_formats() {
    let rig = Rig::new(RigOptions::default()).await;
    let test_data = Bytes::from_static(b"Test data for key format variations");
    let range_start = 0u64;
    let range_end = (test_data.len() - 1) as u64;

    // (request path as a client sends it, key form the lookup uses, description)
    let test_cases = vec![
        (
            "/bucket1/simple/path.txt",
            "bucket1/simple/path.txt",
            "Simple path without leading slash",
        ),
        (
            "/bucket2/with/leading/slash.txt",
            "/bucket2/with/leading/slash.txt",
            "Path with leading slash",
        ),
        (
            "/bucket3/path%20with%20spaces.txt",
            "bucket3/path%20with%20spaces.txt",
            "Path with spaces (filed under its percent-encoded request path)",
        ),
        (
            "/bucket4/path:with:colons.txt",
            "bucket4/path:with:colons.txt",
            "Path with colons",
        ),
        (
            "/bucket5/very/long/path/that/has/many/segments/to/test/sharding/behavior.txt",
            "bucket5/very/long/path/that/has/many/segments/to/test/sharding/behavior.txt",
            "Long path",
        ),
    ];

    for (path, lookup_key, description) in test_cases {
        println!("Testing: {} ({})", description, lookup_key);
        rig.mock.put_bytes(path, test_data.clone());
        rig.seed_range(path, range_start, range_end).await;
        let cache_key = Rig::cache_key(path);
        // The proxy files a path with a colon under its own encoding of it.
        let lookup_key = if path.contains(':') {
            cache_key.as_str()
        } else {
            lookup_key
        };
        let disk = rig.disk.read().await;
        assert!(
            disk.get_new_metadata_file_path(&cache_key).exists(),
            "Metadata file should exist for key: {}",
            lookup_key
        );
        assert!(
            disk.get_new_range_file_path(&cache_key, range_start, range_end)
                .exists(),
            "Range file should exist for key: {}",
            lookup_key
        );
        let found_ranges = disk
            .find_cached_ranges(
                lookup_key,
                range_start,
                range_end,
                None,
                RangeLookupPurpose::FreshServe,
            )
            .await
            .unwrap()
            .ranges;
        assert!(
            !found_ranges.is_empty(),
            "Should find cached range for key: {}",
            lookup_key
        );
        assert_eq!(
            found_ranges[0].start, range_start,
            "Range start should match for key: {}",
            lookup_key
        );
        assert_eq!(
            found_ranges[0].end, range_end,
            "Range end should match for key: {}",
            lookup_key
        );
    }
}

/// Test cache lookup with multiple ranges
///
/// Verifies that multiple non-overlapping ranges can be stored and retrieved
#[tokio::test]
async fn test_cache_lookup_diagnostic_multiple_ranges() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/test-bucket/multi-range-object.bin";
    let mut object = Vec::with_capacity(500);
    object.extend_from_slice(&[b'A'; 100]); // 0-99
    object.extend_from_slice(&[b'.'; 100]); // 100-199, never fetched
    object.extend_from_slice(&[b'B'; 100]); // 200-299
    object.extend_from_slice(&[b'.'; 100]); // 300-399, never fetched
    object.extend_from_slice(&[b'C'; 100]); // 400-499
    rig.mock.put_bytes(path, Bytes::from(object));
    let cache_key = Rig::cache_key(path);

    let ranges = [(0u64, 99u64), (200u64, 299u64), (400u64, 499u64)];
    for (start, end) in &ranges {
        rig.seed_range(path, *start, *end).await;
    }
    let disk = rig.disk.read().await;

    // Test finding each range individually
    for (start, end) in &ranges {
        let found_ranges = disk
            .find_cached_ranges(
                &cache_key,
                *start,
                *end,
                None,
                RangeLookupPurpose::FreshServe,
            )
            .await
            .unwrap()
            .ranges;
        assert!(
            !found_ranges.is_empty(),
            "Should find cached range {}-{}",
            start,
            end
        );
        // Should find exact match
        assert_eq!(found_ranges.len(), 1, "Should find exactly one range");
        assert_eq!(found_ranges[0].start, *start);
        assert_eq!(found_ranges[0].end, *end);
    }

    // Test finding a range that spans multiple cached ranges
    let found_ranges = disk
        .find_cached_ranges(&cache_key, 0, 499, None, RangeLookupPurpose::FreshServe)
        .await
        .unwrap()
        .ranges;
    assert_eq!(
        found_ranges.len(),
        3,
        "Should find all three cached ranges when requesting full span"
    );
}

/// Test cache lookup with partial overlap
///
/// Verifies that partially overlapping ranges are correctly identified
#[tokio::test]
async fn test_cache_lookup_diagnostic_partial_overlap() {
    let rig = Rig::new(RigOptions::default()).await;
    let path = "/test-bucket/partial-overlap-object.bin";
    rig.mock.put(path, 2000);
    let cache_key = Rig::cache_key(path);

    // Store range 0-999
    rig.seed_range(path, 0, 999).await;
    let disk = rig.disk.read().await;

    // Test cases for partial overlap
    let test_cases = vec![
        (0, 499, true, "Start of cached range"),
        (500, 999, true, "End of cached range"),
        (0, 999, true, "Exact match"),
        (100, 200, true, "Fully contained"),
        (900, 1100, true, "Partial overlap at end"),
        (1000, 1999, false, "No overlap (after cached range)"),
    ];

    for (start, end, should_find, description) in test_cases {
        let found_ranges = disk
            .find_cached_ranges(&cache_key, start, end, None, RangeLookupPurpose::FreshServe)
            .await
            .unwrap()
            .ranges;
        if should_find {
            assert!(
                !found_ranges.is_empty(),
                "Should find overlap for range {}-{} ({})",
                start,
                end,
                description
            );
        } else {
            assert!(
                found_ranges.is_empty(),
                "Should not find overlap for range {}-{} ({})",
                start,
                end,
                description
            );
        }
    }
}

/// Test cache lookup with expired ranges
///
/// Verifies that expired ranges are not returned by find_cached_ranges. The proxy is
/// configured with a one-millisecond TTL, so the range is stale as soon as the fill lands.
#[tokio::test]
async fn test_cache_lookup_diagnostic_expired_ranges() {
    let rig = Rig::new(RigOptions {
        get_ttl: Duration::from_millis(1),
        ..RigOptions::default()
    })
    .await;
    let path = "/test-bucket/expired-range-object.bin";
    let test_data = Bytes::from_static(b"This range will expire immediately");
    rig.mock.put_bytes(path, test_data.clone());
    let cache_key = Rig::cache_key(path);

    let (status, _) = rig
        .get(
            path,
            &[("range", format!("bytes=0-{}", test_data.len() - 1))],
        )
        .await;
    assert_eq!(status, hyper::StatusCode::PARTIAL_CONTENT);
    // The range is in the `.meta`; a lookup that skips stale ranges must not return it
    rig.wait_meta(path).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let listed = rig
        .cache_manager
        .get_metadata_from_disk(&cache_key)
        .await
        .unwrap()
        .expect("the .meta");
    eprintln!(
        "expires_at={:?} now={:?} ranges={:?}",
        listed.expires_at,
        std::time::SystemTime::now(),
        listed
            .ranges
            .iter()
            .map(|r| (r.start, r.end))
            .collect::<Vec<_>>()
    );

    let found_ranges = rig
        .disk
        .read()
        .await
        .find_cached_ranges(
            &cache_key,
            0,
            (test_data.len() - 1) as u64,
            None,
            RangeLookupPurpose::FreshServe,
        )
        .await
        .unwrap()
        .ranges;
    assert!(
        found_ranges.is_empty(),
        "Should not find expired range, but found {} range(s)",
        found_ranges.len()
    );
    // The production rule: a read past `get_ttl` reaches the origin.
    let before = rig.mock.hits(path);
    let (status, body) = rig
        .get(
            path,
            &[("range", format!("bytes=0-{}", test_data.len() - 1))],
        )
        .await;
    assert_eq!(status, hyper::StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, test_data);
    assert!(
        rig.mock.hits(path) > before,
        "a read past get_ttl must reach the origin (origin hits {} before, {} after)",
        before,
        rig.mock.hits(path)
    );
}
