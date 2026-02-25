//! Presigned URL parsing and expiration checking demo
//!
//! This example demonstrates how to parse AWS SigV4 presigned URLs
//! and check their expiration status without making requests to S3.
//!
//! Run with: cargo run --release --example presigned_url_demo

use s3_proxy::presigned_url::{parse_presigned_url, PresignedUrlInfo};
use std::collections::HashMap;

fn main() {
    println!("=== Presigned URL Parsing Demo ===\n");

    // Example 1: Valid presigned URL (not expired)
    println!("Example 1: Valid presigned URL");
    let mut query_params = HashMap::new();
    query_params.insert(
        "X-Amz-Algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    );
    query_params.insert("X-Amz-Date".to_string(), "20260115T120000Z".to_string()); // Future date
    query_params.insert("X-Amz-Expires".to_string(), "3600".to_string()); // 1 hour
    query_params.insert(
        "X-Amz-Credential".to_string(),
        "AKIAIOSFODNN7EXAMPLE/20260115/us-east-1/s3/aws4_request".to_string(),
    );
    query_params.insert("X-Amz-SignedHeaders".to_string(), "host".to_string());
    query_params.insert("X-Amz-Signature".to_string(), "abc123def456...".to_string());

    if let Some(info) = parse_presigned_url(&query_params) {
        print_presigned_url_info(&info);
    } else {
        println!("Failed to parse presigned URL");
    }

    println!("\n{}\n", "=".repeat(60));

    // Example 2: Expired presigned URL
    println!("Example 2: Expired presigned URL");
    let mut query_params = HashMap::new();
    query_params.insert(
        "X-Amz-Algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    );
    query_params.insert("X-Amz-Date".to_string(), "20240115T120000Z".to_string()); // Past date
    query_params.insert("X-Amz-Expires".to_string(), "3600".to_string()); // 1 hour
    query_params.insert("X-Amz-Signature".to_string(), "xyz789...".to_string());

    if let Some(info) = parse_presigned_url(&query_params) {
        print_presigned_url_info(&info);
    } else {
        println!("Failed to parse presigned URL");
    }

    println!("\n{}\n", "=".repeat(60));

    // Example 3: Regular (non-presigned) URL
    println!("Example 3: Regular URL (not presigned)");
    let mut query_params = HashMap::new();
    query_params.insert("versionId".to_string(), "abc123".to_string());
    query_params.insert("partNumber".to_string(), "1".to_string());

    if let Some(info) = parse_presigned_url(&query_params) {
        print_presigned_url_info(&info);
    } else {
        println!("Not a presigned URL (missing X-Amz-Algorithm)");
    }

    println!("\n{}\n", "=".repeat(60));

    // Example 4: Proxy behavior demonstration
    println!("Example 4: Proxy behavior with presigned URLs");
    let mut query_params = HashMap::new();
    query_params.insert(
        "X-Amz-Algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    );
    query_params.insert("X-Amz-Date".to_string(), "20260115T120000Z".to_string());
    query_params.insert("X-Amz-Expires".to_string(), "7200".to_string()); // 2 hours

    if let Some(info) = parse_presigned_url(&query_params) {
        println!(
            "Presigned URL expires in: {} seconds",
            info.expires_in_seconds
        );

        if let Some(time_remaining) = info.time_until_expiration() {
            println!("Time remaining: {} seconds", time_remaining.as_secs());
            println!("\nProxy behavior:");
            println!("  - Expired URLs: Rejected with 403 Forbidden (before cache lookup)");
            println!("  - Valid URLs: Proceed to cache lookup or S3 forwarding");
        }
    }
}

fn print_presigned_url_info(info: &PresignedUrlInfo) {
    println!("Presigned URL Information:");
    println!(
        "  Algorithm: {}",
        info.algorithm.as_deref().unwrap_or("N/A")
    );
    println!("  Signed at: {:?}", info.signed_at);
    println!(
        "  Expires in: {} seconds ({} hours)",
        info.expires_in_seconds,
        info.expires_in_seconds / 3600
    );
    println!("  Expires at: {:?}", info.expires_at);

    if info.is_expired() {
        println!("  Status: ❌ EXPIRED");
        if let Some(time_since) = info.time_since_expiration() {
            println!("  Expired {} seconds ago", time_since.as_secs());
        }
    } else {
        println!("  Status: ✅ VALID");
        if let Some(time_until) = info.time_until_expiration() {
            println!("  Expires in {} seconds", time_until.as_secs());
        }
    }
}
