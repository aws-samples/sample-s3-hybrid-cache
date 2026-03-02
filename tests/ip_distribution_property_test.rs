//! Property-based tests for IP distribution round-robin
//!
//! **Property 1: Round-robin distributes requests evenly across N IPs**
//!
//! *For any* set of 1-16 IP addresses, after sending N×100 requests through the
//! IpDistributor, each IP SHALL receive between (N×100/len - tolerance) and
//! (N×100/len + tolerance) requests, verifying even distribution.
//!
//! **Validates: Requirements 1.3**

use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use s3_proxy::connection_pool::IpDistributor;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

// ============================================================================
// Test Data Generators
// ============================================================================

/// Represents a set of 1-16 unique IP addresses for property testing.
#[derive(Debug, Clone)]
struct IpSet(Vec<IpAddr>);

impl Arbitrary for IpSet {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate 1-16 unique IPs
        let count = (u8::arbitrary(g) % 16) + 1;
        let ips: Vec<IpAddr> = (0..count)
            .map(|i| IpAddr::V4(Ipv4Addr::new(10, 0, 0, i + 1)))
            .collect();
        IpSet(ips)
    }
}

/// Represents a multiplier N for the number of request rounds (1-20).
#[derive(Debug, Clone, Copy)]
struct RequestMultiplier(usize);

impl Arbitrary for RequestMultiplier {
    fn arbitrary(g: &mut Gen) -> Self {
        // Generate multiplier 1-20
        let n = (usize::arbitrary(g) % 20) + 1;
        RequestMultiplier(n)
    }
}

// ============================================================================
// Property 1: Round-robin distributes requests evenly across N IPs
// ============================================================================

/// **Property 1: Round-robin distributes requests evenly across N IPs**
///
/// For any set of 1-16 IPs and multiplier N, sending N×len requests through
/// the IpDistributor results in each IP receiving exactly N requests.
/// This holds because round-robin is deterministic and N×len is an exact
/// multiple of the IP count.
///
/// **Validates: Requirements 1.3**
#[quickcheck]
fn prop_round_robin_perfect_distribution(ips: IpSet, mult: RequestMultiplier) -> TestResult {
    let ip_vec = ips.0;
    let len = ip_vec.len();
    let total_requests = mult.0 * len;

    let distributor = IpDistributor::new(ip_vec.clone());

    // Count how many times each IP is selected
    let mut counts: HashMap<IpAddr, usize> = HashMap::new();
    for _ in 0..total_requests {
        let ip = distributor.select_ip().expect("should return an IP");
        *counts.entry(ip).or_insert(0) += 1;
    }

    // With deterministic round-robin and total_requests = N * len,
    // each IP must receive exactly N requests
    let expected = mult.0;
    for ip in &ip_vec {
        let count = counts.get(ip).copied().unwrap_or(0);
        if count != expected {
            return TestResult::error(format!(
                "IP {} received {} requests, expected exactly {} \
                 (total={}, ips={})",
                ip, count, expected, total_requests, len
            ));
        }
    }

    TestResult::passed()
}

/// **Property 1 (non-multiple variant): Round-robin distributes within tolerance**
///
/// For any set of 1-16 IPs and arbitrary request count, each IP receives
/// between floor(total/len) and ceil(total/len) requests. This covers
/// non-multiple request counts where perfect equality isn't possible.
///
/// **Validates: Requirements 1.3**
#[quickcheck]
fn prop_round_robin_bounded_distribution(ips: IpSet, mult: RequestMultiplier) -> TestResult {
    let ip_vec = ips.0;
    let len = ip_vec.len();
    // Use N×100 as specified in the task (non-multiple of len in general)
    let total_requests = mult.0 * 100;

    let distributor = IpDistributor::new(ip_vec.clone());

    let mut counts: HashMap<IpAddr, usize> = HashMap::new();
    for _ in 0..total_requests {
        let ip = distributor.select_ip().expect("should return an IP");
        *counts.entry(ip).or_insert(0) += 1;
    }

    let min_expected = total_requests / len;
    let max_expected = (total_requests + len - 1) / len; // ceil division

    for ip in &ip_vec {
        let count = counts.get(ip).copied().unwrap_or(0);
        if count < min_expected || count > max_expected {
            return TestResult::error(format!(
                "IP {} received {} requests, expected between {} and {} \
                 (total={}, ips={})",
                ip, count, min_expected, max_expected, total_requests, len
            ));
        }
    }

    // Verify total requests accounted for
    let total_counted: usize = counts.values().sum();
    if total_counted != total_requests {
        return TestResult::error(format!(
            "Total counted {} != total sent {}",
            total_counted, total_requests
        ));
    }

    TestResult::passed()
}
