//! Integration test for part number caching end-to-end workflow
//!
//! Tests Requirements: All from part-number-caching spec
//!
//! This test validates:
//! - Request part 1 (cache miss)
//! - Verify part stored and metadata populated
//! - Request part 1 again (cache hit)
//! - Verify response headers match
