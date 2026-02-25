# S3 Proxy Testing Guide

Comprehensive testing documentation for validating S3 proxy functionality, including conditional request handling, cache behavior, and AWS CLI compatibility.

## Table of Contents

- [Test Suite Overview](#test-suite-overview)
- [Prerequisites](#prerequisites)
- [Running Tests](#running-tests)
- [Test Scenarios](#test-scenarios)
- [Example Test Run](#example-test-run)
- [Test Output Format](#test-output-format)
- [Key Validation Points](#key-validation-points)
- [Troubleshooting](#troubleshooting)
- [Expected Results](#expected-results)

---

## Test Suite Overview

The S3 proxy includes comprehensive Rust integration tests in the `tests/` directory.

## Prerequisites

### Required Software

1. **Python 3** with required packages:
   ```bash
   pip install boto3 requests
   ```

2. **AWS CLI** installed and configured:
   ```bash
   # Install AWS CLI
   curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
   unzip awscliv2.zip
   sudo ./aws/install
   
   # Configure credentials
   aws configure
   ```

3. **Environment Setup (Recommended)**:
   ```bash
   # Set to automatically route S3 traffic through proxy
   export AWS_ENDPOINT_URL_S3=http://s3.us-east-1.amazonaws.com
   ```
   
   This eliminates the need for `--endpoint-url` on every command when working with buckets in that region. For buckets in other regions, specify both `--region` and `--endpoint-url` explicitly.

3. **S3 Proxy running**:
   ```bash
   sudo cargo run --release -- -c config/config.example.yaml
   ```

4. **AWS credentials** with read/write permissions to the test bucket

## Running Tests

### Rust Integration Tests

```bash
# Run all Rust tests (always use --release)
cargo test --release

# Run specific test file
cargo test --release --test integration_test

# Run with output
cargo test --release -- --nocapture
```

## Test Scenarios

### 1. Basic Operations
- GET requests for small, medium, and large files
- HEAD requests for metadata validation
- PUT operations through the proxy

### 2. Conditional Requests
- **If-Match** with matching and non-matching ETags
- **If-None-Match** with matching and non-matching ETags
- **If-Modified-Since** with past and future dates
- **If-Unmodified-Since** with past and future dates

### 3. Range Requests
- Single byte range requests
- Range requests from middle of files
- Validation of range content accuracy

### 4. Cache Behavior
- Multiple requests to measure cache performance
- Cache hit/miss timing analysis
- Cache invalidation verification

### 5. AWS CLI Integration
- Standard AWS CLI operations through the proxy
- Error handling validation

### 6. Error Handling
- 404 Not Found scenarios
- Invalid conditional headers
- Network error resilience

## Example Test Run

```bash
$ cargo test --release

running 87 tests
test cache_statistics_test::test_cache_hit_rate ... ok
test disk_cache_test::test_disk_cache_operations ... ok
test integration_test::test_basic_get_request ... ok
test range_get_test::test_range_request_handling ... ok
...

test result: ok. 87 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Test Output Format

The Rust test suite provides standard cargo test output with pass/fail status for each test.

## Key Validation Points

### Conditional Request Compliance
- All conditional requests are forwarded to S3 (not decided by proxy)
- S3 responses are correctly returned to client
- Cache is managed based on S3 response:
  - 200 OK → invalidate old cache, cache new data
  - 304 Not Modified → refresh TTL, keep cache
  - 412 Precondition Failed → no cache changes

### Cache Behavior
- Non-conditional requests served from cache when available
- Cache performance improvements visible in timing
- Proper cache invalidation on object changes

### HTTP Compliance
- Correct status codes returned to client
- Proper handling of ETags and Last-Modified headers
- Range request accuracy

## Troubleshooting

### Common Issues

1. **AWS credentials not configured**:
   ```bash
   aws configure
   # or set environment variables:
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   ```

2. **Proxy not running**:
   ```bash
   sudo cargo run --release -- -c config/config.example.yaml
   ```

3. **Permission denied on bucket**:
   - Ensure AWS credentials have s3:GetObject, s3:PutObject, s3:DeleteObject permissions
   - Verify bucket exists and is accessible

4. **Connection refused**:
   - Check proxy is running on expected port (default: 80)
   - Verify firewall settings
   - Try different endpoint URL

5. **Region-related errors**:
   - The script auto-detects bucket region via HeadBucket operation
   - If auto-detection fails, specify region explicitly: `-r us-west-2`
   - Ensure your AWS credentials have access to the bucket in its region

### Debug Mode

For detailed debugging, enable trace logging when running tests:

```bash
RUST_LOG=debug cargo test --release -- --nocapture
```

## Expected Results

For a properly functioning proxy:

- **All conditional requests** should be forwarded to S3
- **Cache invalidation** should only occur on S3 200 responses
- **304 responses** should refresh cache TTL without invalidation
- **412 responses** should not modify cache state
- **Performance** should show cache benefits for repeated requests
- **AWS CLI compatibility** should be maintained

Any deviations from these expectations indicate issues that need investigation.