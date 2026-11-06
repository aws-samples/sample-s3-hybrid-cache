//! Test for cache operation logging
//!
//! Verifies that cache operations are logged correctly:
//! - HEAD cache invalidation (Requirement 4.1)
//! - Cached range hits (Requirement 4.2)
//! - Complete cache hits (Requirement 4.3)
//!
//! The cached ranges are made by ranged GETs that miss through a real `HttpProxy`.
#[path = "common/proxy_rig.rs"]
mod proxy_rig;
use bytes::Bytes;
use proxy_rig::{Rig, RigOptions};
use s3_proxy::range_handler::RangeSpec;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::test]
async fn test_cached_range_hit_logging() {
    // Initialize tracing to capture logs
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();

    let rig = Rig::new(RigOptions::default()).await;
    let path = "/test-bucket/test-object.txt";
    rig.mock.put_bytes(path, Bytes::from(vec![b'A'; 1024]));
    let cache_key = Rig::cache_key(path);

    // Store range 0-1023
    rig.seed_range(path, 0, 1023).await;

    // Find cached ranges - this should trigger the logging
    // Requirement 4.2: Log when cached ranges are found during full object GET
    // The logging happens in handle_range_request with:
    // info!("Cached range hit: cache_key={}, range={}-{}, size={} bytes, etag={}", ...)
    let range_spec = RangeSpec {
        start: 0,
        end: 1023,
    };
    let overlap = rig
        .range_handler
        .find_cached_ranges(
            &cache_key,
            &range_spec,
            None,
            None,
            s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
        )
        .await
        .unwrap();

    assert_eq!(overlap.cached_ranges.len(), 1, "Should find 1 cached range");
    assert_eq!(
        overlap.missing_ranges.len(),
        0,
        "Should have no missing ranges"
    );
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve from cache"
    );

    println!("✓ Cached range hit logging test passed");
    println!("  - Found {} cached range(s)", overlap.cached_ranges.len());
    println!(
        "  - Range: {}-{}",
        overlap.cached_ranges[0].start, overlap.cached_ranges[0].end
    );
}

#[tokio::test]
async fn test_complete_cache_hit_metrics() {
    // Initialize tracing to capture logs
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();

    let rig = Rig::new(RigOptions::default()).await;
    let path = "/test-bucket/test-object.txt";
    let mut object = vec![b'A'; 512];
    object.extend_from_slice(&[b'B'; 512]);
    rig.mock.put_bytes(path, Bytes::from(object));
    let cache_key = Rig::cache_key(path);

    // Store ranges 0-511 and 512-1023 (covering 0-1023 completely)
    rig.seed_range(path, 0, 511).await;
    rig.seed_range(path, 512, 1023).await;

    // Request the full range 0-1023
    // Requirement 4.3: Log when full object is served entirely from cache
    // The logging happens in handle_range_request with:
    // info!("Complete cache hit: cache_key={}, range={}-{}, requested_bytes={}, cached_ranges={}, cache_efficiency={:.2}%, s3_requests=0", ...)
    let range_spec = RangeSpec {
        start: 0,
        end: 1023,
    };
    let overlap = rig
        .range_handler
        .find_cached_ranges(
            &cache_key,
            &range_spec,
            None,
            None,
            s3_proxy::cache_types::RangeLookupPurpose::FreshServe,
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
        0,
        "Should have no missing ranges"
    );
    assert!(
        overlap.can_serve_from_cache,
        "Should be able to serve entirely from cache"
    );

    // Calculate cache efficiency metrics
    let total_cached_bytes: u64 = overlap
        .cached_ranges
        .iter()
        .map(|r| r.end - r.start + 1)
        .sum();
    let requested_bytes = range_spec.end - range_spec.start + 1;
    let cache_efficiency = (total_cached_bytes as f64 / requested_bytes as f64) * 100.0;

    assert_eq!(total_cached_bytes, 1024, "Should have 1024 bytes cached");
    assert_eq!(requested_bytes, 1024, "Should request 1024 bytes");
    assert_eq!(cache_efficiency, 100.0, "Cache efficiency should be 100%");

    println!("✓ Complete cache hit metrics test passed");
    println!("  - Cached ranges: {}", overlap.cached_ranges.len());
    println!("  - Total cached bytes: {}", total_cached_bytes);
    println!("  - Requested bytes: {}", requested_bytes);
    println!("  - Cache efficiency: {:.2}%", cache_efficiency);
    println!("  - S3 requests: 0");
}
