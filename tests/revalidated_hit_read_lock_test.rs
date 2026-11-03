//! A revalidated cache hit does not wait for readers of the disk cache manager.
//!
//! With `get_ttl: 0` every cache hit revalidates with S3, and an origin `304` records the
//! validation on the object's `.meta` (`DiskCacheManager::record_origin_validation`). That call
//! took the manager's WRITE lock, so every revalidated hit in the process queued behind every
//! holder of its read lock, and tokio's lock is fair, so new readers then queued behind the
//! waiting hit. A read lock is held, for example, by the pending-journal lookup while it reads
//! every journal file. The `.meta` write is ordered by the key's metadata slot lock, the same
//! lock that orders it against peer processes, so the hit needs only the read lock.

mod common;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use common::expired_fixture::{body_of, conditional_requests, not_modified, test_config, zero_ttl};
use common::expired_fixture::{Fixture, SeedSpec, OLD_FILL};
use common::StubS3Client;

const OBJECT_SIZE: u64 = 4096;
/// How long the test lets a revalidated hit take while another task holds the read lock. A hit
/// that needs the write lock waits until that reader lets go, which here is never.
const HIT_BUDGET: Duration = Duration::from_secs(5);

/// A revalidated range hit is served while another task holds the disk cache manager's read
/// lock.
///
/// # What is asserted, and why
///
/// - **The hit completes within `HIT_BUDGET`** while the read lock is held for the whole call.
/// - **It revalidated and served the cached bytes**: a conditional request reached S3 and the
///   body is the seeded fill, so the call took the post-`304` arm that records the validation,
///   not a miss that never touches the lock.
///
/// # RED SIDE
///
/// On 3.0.0 the post-`304` arm awaits `disk_cache.write()`, which cannot be granted while the
/// read guard is held, so the call does not complete and the first assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revalidated_hit_is_served_while_the_disk_cache_is_read_locked() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let cache_key = "bucket/revalidated-hit-read-lock.bin";
    let (start, end) = (0u64, 1023u64);
    let etag = fixture.seed(
        cache_key,
        &SeedSpec::expired(vec![(start, end)], OBJECT_SIZE, "\"read-lock-v1\""),
    );
    fixture.invalidate_metadata_cache(cache_key).await;
    let stub = StubS3Client::new().with_response_for_etag(etag.clone(), not_modified(&etag));

    // Held for the whole hit, as a pending-journal lookup holds it while it scans.
    let reader = fixture.disk_cache_manager.clone().read_owned().await;
    let started = Instant::now();
    let served = tokio::time::timeout(HIT_BUDGET, async {
        let response = fixture
            .range_get(
                cache_key,
                &format!("bytes={start}-{end}"),
                HashMap::new(),
                &zero_ttl(),
                fixture.production_current_etag(cache_key),
                stub.clone().into_trait_object(),
            )
            .await;
        body_of(response).await
    })
    .await;
    let elapsed = started.elapsed();
    drop(reader);

    let body = served.unwrap_or_else(|_| {
        panic!(
            "a revalidated hit did not complete within {HIT_BUDGET:?} while another task held \
             the disk cache manager's read lock"
        )
    });
    println!("revalidated hit served in {elapsed:?} under a held read lock");
    assert_eq!(
        body,
        vec![OLD_FILL; (end - start + 1) as usize],
        "the hit must serve the cached bytes"
    );
    assert_eq!(
        conditional_requests(&stub.captured()).len(),
        1,
        "the hit must revalidate with S3 once"
    );
}
