//! Integration test: aws-chunked decoder must not panic on crafted chunk headers.
//!
//! The overflow lived in `decode_aws_chunked`, which is invoked on the
//! signed-PUT path. A full listener-plus-upstream test would require
//! spinning up both the proxy and an S3 surrogate; the meaningful
//! invariant is that the decoder's public API — exercised from a separate
//! crate (tests/) — returns `Err` instead of panicking on the crafted
//! payload, and remains usable across repeated calls with mixed
//! valid/invalid inputs. That's what a panic-taking-down-the-task would
//! violate and what we check here.
//!
//! Validates: Requirements 1.3, 1.4

use s3_proxy::aws_chunked_decoder::decode_aws_chunked;

/// The crafted chunk header from Requirement 1.3: declares `usize::MAX`
/// (0xffffffffffffffff) as the first chunk size. On a pre-fix build this
/// wrapped `pos + chunk_size` past `body.len()`, passed the EOF check, and
/// then panicked on the slice index.
#[test]
fn test_decoder_rejects_usize_max_chunk_header() {
    // Validates: Requirements 1.3
    let body: &[u8] = b"ffffffffffffffff;chunk-signature=0\r\n";
    let result = decode_aws_chunked(body);
    assert!(
        result.is_err(),
        "Expected Err for usize::MAX chunk header, got {:?}",
        result
    );
}

/// Companion to the above: `usize::MAX - 1` must also be rejected. Covers
/// the "checked_add with the trailing CRLF" arm of the fix.
#[test]
fn test_decoder_rejects_near_usize_max_chunk_header() {
    // Validates: Requirements 1.3
    let body: &[u8] = b"fffffffffffffffe;chunk-signature=0\r\n";
    let result = decode_aws_chunked(body);
    assert!(
        result.is_err(),
        "Expected Err for usize::MAX - 1 chunk header, got {:?}",
        result
    );
}

/// After the crafted body is rejected, a subsequent well-formed body must
/// still decode correctly. This is the "process survived" check: a panic
/// in `decode_aws_chunked` would have taken down the calling tokio task,
/// which in turn would mean the decoder itself is no longer trustworthy
/// for subsequent inputs. Verifying that a mixed valid/invalid sequence
/// round-trips proves the function has no lingering state corruption and
/// the rejection path in Requirement 1.4 leaves nothing behind.
#[test]
fn test_decoder_survives_and_serves_subsequent_legitimate_request() {
    // Validates: Requirements 1.3, 1.4
    let crafted: &[u8] = b"ffffffffffffffff;chunk-signature=0\r\n";
    let rejected = decode_aws_chunked(crafted);
    assert!(rejected.is_err(), "Crafted payload should be rejected");

    // Well-formed aws-chunked body: one data chunk "hello" then terminator.
    // Format: "<hex-size>;chunk-signature=<sig>\r\n<data>\r\n0;chunk-signature=<sig>\r\n\r\n"
    let legitimate: &[u8] = b"5;chunk-signature=abc\r\nhello\r\n0;chunk-signature=abc\r\n\r\n";
    let decoded = decode_aws_chunked(legitimate)
        .expect("Legitimate aws-chunked body must still decode after a rejected one");
    assert_eq!(
        decoded, b"hello",
        "Decoder must round-trip a well-formed body after rejecting a crafted one"
    );
}
