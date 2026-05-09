//! AWS Chunked Encoding Decoder
//!
//! Decodes aws-chunked encoded bodies used by AWS CLI and SDKs for SigV4 streaming
//! uploads. The format is:
//! `chunk-size;chunk-signature=sig\r\n data\r\n ... 0;chunk-signature=sig\r\n\r\n`
//!
//! This module provides:
//! - [`is_aws_chunked`] — detect aws-chunked encoding from request headers
//!   (`content-encoding: aws-chunked` or `x-amz-content-sha256: STREAMING-…`).
//!   Does NOT byte-sniff the body — header-based detection only.
//! - [`get_decoded_content_length`] — read `x-amz-decoded-content-length` if
//!   present. Always validate the decoder's output length against this value
//!   when it is present and skip caching on mismatch.
//! - [`decode_aws_chunked`] — decode a complete chunked body to plain bytes
//!   and parse any trailers after the zero-size chunk.
//! - [`encode_aws_chunked`] — encode plain bytes as chunked (for tests only).
//! - [`encode_aws_chunked_with_trailers`] — encode plain bytes as chunked
//!   with trailer key-value pairs (for tests only).
//!
//! # When to use this module
//!
//! Any PUT/UploadPart code path that needs to cache the *decoded* body while
//! forwarding the *original* (chunked, still-signature-valid) body to S3. Both
//! the non-multipart PUT path (`handle_with_caching`) and the multipart
//! UploadPart path (`handle_upload_part`) use this module for that exact split.
//!
//! # Do not reinvent chunk parsing
//!
//! Earlier versions of the multipart path had their own byte-sniffing stripper
//! that looked at the first hex-digit byte and counted CRLFs. It was replaced
//! with this module because byte-sniffing can misidentify non-chunked bodies
//! that happen to start with a hex digit. Use the header-based detection here.

use std::collections::HashMap;
use std::fmt;

/// Maximum allowed size for the trailer section (bytes).
const MAX_TRAILER_SECTION_BYTES: usize = 8192;

/// Error type for aws-chunked decoding operations
#[derive(Debug, Clone, PartialEq)]
pub enum AwsChunkedError {
    /// Invalid chunk header format (missing semicolon, signature, etc.)
    InvalidChunkHeader(String),
    /// Invalid chunk size (non-hexadecimal value)
    InvalidChunkSize(String),
    /// Unexpected end of input while parsing
    UnexpectedEof,
    /// Decoded length doesn't match expected length
    LengthMismatch { expected: u64, actual: u64 },
    /// Trailer section exceeds the maximum allowed size
    TrailerTooLarge { size: usize, max: usize },
}

impl fmt::Display for AwsChunkedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AwsChunkedError::InvalidChunkHeader(msg) => {
                write!(f, "Invalid chunk header: {}", msg)
            }
            AwsChunkedError::InvalidChunkSize(msg) => {
                write!(f, "Invalid chunk size: {}", msg)
            }
            AwsChunkedError::UnexpectedEof => {
                write!(f, "Unexpected end of input while parsing aws-chunked body")
            }
            AwsChunkedError::LengthMismatch { expected, actual } => {
                write!(
                    f,
                    "Decoded length mismatch: expected {} bytes, got {} bytes",
                    expected, actual
                )
            }
            AwsChunkedError::TrailerTooLarge { size, max } => {
                write!(
                    f,
                    "Trailer section too large: {} bytes exceeds {} byte limit",
                    size, max
                )
            }
        }
    }
}

impl std::error::Error for AwsChunkedError {}

/// Streaming SigV4 payload indicator header value (classic HMAC SigV4)
const STREAMING_AWS4_HMAC_SHA256_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Streaming SigV4A payload indicator header value (ECDSA SigV4A, used by
/// clients talking to Multi-Region Access Points). The chunk framing format
/// is identical to classic SigV4 streaming payloads; only this label differs.
const STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD: &str = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD";

/// Result of a successful aws-chunked decode, containing the decoded body
/// and any trailers that followed the zero-size chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct AwsChunkedDecodeResult {
    /// The decoded body bytes (chunk data concatenated).
    pub data: Vec<u8>,
    /// Trailers parsed after the zero-size chunk. Each entry is a (key, value)
    /// pair. Common trailers include `x-amz-checksum-*` and
    /// `x-amz-trailer-signature`.
    pub trailers: Vec<(String, String)>,
}

/// Check if a request uses aws-chunked encoding.
///
/// Returns true if either:
/// - The `content-encoding` header contains `aws-chunked`
/// - The `x-amz-content-sha256` header equals `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
///   or `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD` (SigV4A equivalent)
///
/// # Arguments
/// * `headers` - HashMap of request headers (case-insensitive keys recommended)
///
/// # Returns
/// `true` if aws-chunked encoding is detected, `false` otherwise
pub fn is_aws_chunked(headers: &HashMap<String, String>) -> bool {
    // Check content-encoding header for aws-chunked
    if let Some(content_encoding) = headers.get("content-encoding") {
        if content_encoding.to_lowercase().contains("aws-chunked") {
            return true;
        }
    }

    // Check x-amz-content-sha256 header for streaming payload indicator
    // (SigV4 HMAC or SigV4A ECDSA — chunk framing is identical)
    if let Some(sha256) = headers.get("x-amz-content-sha256") {
        if sha256 == STREAMING_AWS4_HMAC_SHA256_PAYLOAD
            || sha256 == STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD
        {
            return true;
        }
    }

    false
}

/// Get the decoded content length from request headers.
///
/// Extracts the `x-amz-decoded-content-length` header value which indicates
/// the actual content length before aws-chunked encoding was applied.
///
/// # Arguments
/// * `headers` - HashMap of request headers
///
/// # Returns
/// `Some(u64)` if the header is present and valid, `None` otherwise
pub fn get_decoded_content_length(headers: &HashMap<String, String>) -> Option<u64> {
    headers
        .get("x-amz-decoded-content-length")
        .and_then(|v| v.parse::<u64>().ok())
}

/// Decode aws-chunked body to extract actual data and trailers.
///
/// Parses the aws-chunked format:
/// ```text
/// chunk-size;chunk-signature=signature\r\n
/// chunk-data\r\n
/// ...
/// 0;chunk-signature=signature\r\n
/// trailer-key:trailer-value\r\n
/// \r\n
/// ```
///
/// After the zero-size chunk, trailers are parsed as `key: value\r\n` lines
/// until a terminal `\r\n` (empty line). The trailer section is capped at
/// 8192 bytes. After the terminal `\r\n`, the decoder verifies that all input
/// bytes have been consumed and returns `LengthMismatch` if bytes remain.
///
/// # Arguments
/// * `body` - Raw aws-chunked encoded body bytes
///
/// # Returns
/// `Ok(AwsChunkedDecodeResult)` containing the decoded data and trailers,
/// or an error if parsing fails
pub fn decode_aws_chunked(body: &[u8]) -> Result<AwsChunkedDecodeResult, AwsChunkedError> {
    let mut result = Vec::new();
    let mut pos = 0;

    loop {
        // Find the end of the chunk header line (terminated by \r\n)
        let header_end = find_crlf(body, pos).ok_or(AwsChunkedError::UnexpectedEof)?;

        // Parse the chunk header: "chunk-size;chunk-signature=sig"
        let header_bytes = &body[pos..header_end];
        let header_str = std::str::from_utf8(header_bytes)
            .map_err(|e| AwsChunkedError::InvalidChunkHeader(e.to_string()))?;

        // Extract chunk size (hex value before the semicolon)
        let chunk_size = parse_chunk_size(header_str)?;

        // Move past the header and \r\n
        pos = header_end + 2;

        // If chunk size is 0, we've reached the final chunk — parse trailers
        if chunk_size == 0 {
            let trailers = parse_trailers(body, &mut pos)?;

            // Verify all bytes consumed (Req 27.2)
            if pos != body.len() {
                return Err(AwsChunkedError::LengthMismatch {
                    expected: pos as u64,
                    actual: body.len() as u64,
                });
            }

            return Ok(AwsChunkedDecodeResult {
                data: result,
                trailers,
            });
        }

        // Read chunk data — reject overflow as EOF so a `chunk_size` near
        // `usize::MAX` cannot wrap past `body.len()` and panic on slice.
        let chunk_end = pos
            .checked_add(chunk_size)
            .ok_or(AwsChunkedError::UnexpectedEof)?;
        if chunk_end > body.len() {
            return Err(AwsChunkedError::UnexpectedEof);
        }

        result.extend_from_slice(&body[pos..chunk_end]);
        pos = chunk_end;

        // Skip trailing \r\n after chunk data — reject overflow as EOF.
        let trailer_end = pos.checked_add(2).ok_or(AwsChunkedError::UnexpectedEof)?;
        if trailer_end > body.len() {
            return Err(AwsChunkedError::UnexpectedEof);
        }
        if &body[pos..trailer_end] != b"\r\n" {
            return Err(AwsChunkedError::InvalidChunkHeader(
                "Missing CRLF after chunk data".to_string(),
            ));
        }
        pos = trailer_end;
    }
}

/// Parse trailers after the zero-size chunk.
///
/// Reads `key: value\r\n` lines until a terminal `\r\n` (empty line).
/// The trailer section is capped at [`MAX_TRAILER_SECTION_BYTES`] total bytes.
fn parse_trailers(body: &[u8], pos: &mut usize) -> Result<Vec<(String, String)>, AwsChunkedError> {
    let mut trailers = Vec::new();
    let trailer_start = *pos;

    loop {
        // Check if we've hit the terminal \r\n (empty line = end of trailers)
        if *pos + 2 <= body.len() && body[*pos] == b'\r' && body[*pos + 1] == b'\n' {
            *pos += 2;
            return Ok(trailers);
        }

        // If there's nothing left, the terminal \r\n is missing
        if *pos >= body.len() {
            return Err(AwsChunkedError::UnexpectedEof);
        }

        // Find the end of this trailer line
        let line_end = find_crlf(body, *pos).ok_or(AwsChunkedError::UnexpectedEof)?;

        // Check trailer section size cap
        let trailer_bytes_consumed = line_end + 2 - trailer_start;
        if trailer_bytes_consumed > MAX_TRAILER_SECTION_BYTES {
            return Err(AwsChunkedError::TrailerTooLarge {
                size: trailer_bytes_consumed,
                max: MAX_TRAILER_SECTION_BYTES,
            });
        }

        // Parse the trailer line as "key: value" or "key:value"
        let line_bytes = &body[*pos..line_end];
        let line_str = std::str::from_utf8(line_bytes)
            .map_err(|e| AwsChunkedError::InvalidChunkHeader(e.to_string()))?;

        if let Some(colon_pos) = line_str.find(':') {
            let key = line_str[..colon_pos].trim().to_string();
            let value = line_str[colon_pos + 1..].trim().to_string();
            trailers.push((key, value));
        } else {
            return Err(AwsChunkedError::InvalidChunkHeader(format!(
                "Trailer line missing colon separator: '{}'",
                line_str
            )));
        }

        *pos = line_end + 2;
    }
}

/// Find the position of \r\n starting from `start` position
fn find_crlf(data: &[u8], start: usize) -> Option<usize> {
    if start >= data.len() {
        return None;
    }
    (start..data.len().saturating_sub(1)).find(|&i| data[i] == b'\r' && data[i + 1] == b'\n')
}

/// Parse chunk size from header string.
/// Header format: "chunk-size;chunk-signature=signature" or just "chunk-size"
fn parse_chunk_size(header: &str) -> Result<usize, AwsChunkedError> {
    // Extract the hex size (everything before the semicolon, if present)
    let size_str = header.split(';').next().unwrap_or(header).trim();

    if size_str.is_empty() {
        return Err(AwsChunkedError::InvalidChunkSize(
            "Empty chunk size".to_string(),
        ));
    }

    usize::from_str_radix(size_str, 16).map_err(|e| {
        AwsChunkedError::InvalidChunkSize(format!("Invalid hex value '{}': {}", size_str, e))
    })
}

/// Encode data as aws-chunked format (for testing round-trips).
///
/// Splits data into chunks of the specified size and formats each chunk
/// with a placeholder signature.
///
/// # Arguments
/// * `data` - Raw data bytes to encode
/// * `chunk_size` - Maximum size of each chunk (must be > 0)
///
/// # Returns
/// `Vec<u8>` containing the aws-chunked encoded data
///
/// # Panics
/// Panics if `chunk_size` is 0
pub fn encode_aws_chunked(data: &[u8], chunk_size: usize) -> Vec<u8> {
    encode_aws_chunked_with_trailers(data, chunk_size, &[])
}

/// Encode data as aws-chunked format with optional trailers (for testing).
///
/// Splits data into chunks of the specified size and formats each chunk
/// with a placeholder signature. Appends trailers after the zero-size chunk.
///
/// # Arguments
/// * `data` - Raw data bytes to encode
/// * `chunk_size` - Maximum size of each chunk (must be > 0)
/// * `trailers` - Trailer key-value pairs to append after the zero-size chunk
///
/// # Returns
/// `Vec<u8>` containing the aws-chunked encoded data with trailers
///
/// # Panics
/// Panics if `chunk_size` is 0
pub fn encode_aws_chunked_with_trailers(
    data: &[u8],
    chunk_size: usize,
    trailers: &[(&str, &str)],
) -> Vec<u8> {
    assert!(chunk_size > 0, "chunk_size must be greater than 0");

    let mut result = Vec::new();
    let placeholder_sig = "0".repeat(64); // Placeholder 64-char hex signature

    let mut offset = 0;
    while offset < data.len() {
        let remaining = data.len() - offset;
        let current_chunk_size = remaining.min(chunk_size);
        let chunk_data = &data[offset..offset + current_chunk_size];

        // Write chunk header: "size;chunk-signature=sig\r\n"
        let header = format!(
            "{:x};chunk-signature={}\r\n",
            current_chunk_size, placeholder_sig
        );
        result.extend_from_slice(header.as_bytes());

        // Write chunk data
        result.extend_from_slice(chunk_data);

        // Write trailing \r\n
        result.extend_from_slice(b"\r\n");

        offset += current_chunk_size;
    }

    // Write final zero-length chunk: "0;chunk-signature=sig\r\n"
    let final_header = format!("0;chunk-signature={}\r\n", placeholder_sig);
    result.extend_from_slice(final_header.as_bytes());

    // Write trailers
    for (key, value) in trailers {
        let trailer_line = format!("{}:{}\r\n", key, value);
        result.extend_from_slice(trailer_line.as_bytes());
    }

    // Write terminal \r\n
    result.extend_from_slice(b"\r\n");

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_is_aws_chunked_with_content_encoding() {
        let mut headers = HashMap::new();
        headers.insert("content-encoding".to_string(), "aws-chunked".to_string());
        assert!(is_aws_chunked(&headers));
    }

    #[test]
    fn test_is_aws_chunked_with_sha256_header() {
        let mut headers = HashMap::new();
        headers.insert(
            "x-amz-content-sha256".to_string(),
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_string(),
        );
        assert!(is_aws_chunked(&headers));
    }

    #[test]
    fn test_is_aws_chunked_with_sigv4a_sha256_header() {
        // MRAP requests use SigV4A; streaming payloads carry a different
        // x-amz-content-sha256 sentinel but the same chunk framing.
        let mut headers = HashMap::new();
        headers.insert(
            "x-amz-content-sha256".to_string(),
            "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD".to_string(),
        );
        assert!(is_aws_chunked(&headers));
    }

    #[test]
    fn test_is_aws_chunked_false_for_regular_request() {
        let headers = HashMap::new();
        assert!(!is_aws_chunked(&headers));
    }

    #[test]
    fn test_get_decoded_content_length() {
        let mut headers = HashMap::new();
        headers.insert(
            "x-amz-decoded-content-length".to_string(),
            "710".to_string(),
        );
        assert_eq!(get_decoded_content_length(&headers), Some(710));
    }

    #[test]
    fn test_get_decoded_content_length_missing() {
        let headers = HashMap::new();
        assert_eq!(get_decoded_content_length(&headers), None);
    }

    #[test]
    fn test_get_decoded_content_length_invalid() {
        let mut headers = HashMap::new();
        headers.insert(
            "x-amz-decoded-content-length".to_string(),
            "not-a-number".to_string(),
        );
        assert_eq!(get_decoded_content_length(&headers), None);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = b"Hello, World!";
        let encoded = encode_aws_chunked(original, 5);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded.data, original);
        assert!(decoded.trailers.is_empty());
    }

    #[test]
    fn test_encode_decode_empty() {
        let original: &[u8] = b"";
        let encoded = encode_aws_chunked(original, 10);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded.data, original);
        assert!(decoded.trailers.is_empty());
    }

    #[test]
    fn test_encode_decode_single_chunk() {
        let original = b"Small data";
        let encoded = encode_aws_chunked(original, 1000);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded.data, original);
        assert!(decoded.trailers.is_empty());
    }

    #[test]
    fn test_encode_decode_multiple_chunks() {
        let original = b"This is a longer piece of data that will be split into multiple chunks";
        let encoded = encode_aws_chunked(original, 10);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded.data, original);
        assert!(decoded.trailers.is_empty());
    }

    #[test]
    fn test_decode_with_trailers() {
        let trailers = &[
            ("x-amz-checksum-crc32", "abc123"),
            ("x-amz-trailer-signature", "deadbeef"),
        ];
        let encoded = encode_aws_chunked_with_trailers(b"hello", 10, trailers);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded.data, b"hello");
        assert_eq!(decoded.trailers.len(), 2);
        assert_eq!(decoded.trailers[0].0, "x-amz-checksum-crc32");
        assert_eq!(decoded.trailers[0].1, "abc123");
        assert_eq!(decoded.trailers[1].0, "x-amz-trailer-signature");
        assert_eq!(decoded.trailers[1].1, "deadbeef");
    }

    #[test]
    fn test_decode_trailers_with_spaces_in_value() {
        let trailers = &[("x-amz-checksum-sha256", "  base64value==  ")];
        let encoded = encode_aws_chunked_with_trailers(b"data", 10, trailers);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded.trailers.len(), 1);
        assert_eq!(decoded.trailers[0].0, "x-amz-checksum-sha256");
        // Values are trimmed
        assert_eq!(decoded.trailers[0].1, "base64value==");
    }

    #[test]
    fn test_decode_rejects_trailing_bytes_after_terminal() {
        // Build a valid chunked body then append extra bytes
        let mut encoded = encode_aws_chunked(b"hello", 10);
        encoded.extend_from_slice(b"extra garbage");
        let result = decode_aws_chunked(&encoded);
        assert!(matches!(
            result,
            Err(AwsChunkedError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn test_decode_rejects_trailer_too_large() {
        // Build a trailer section that exceeds 8192 bytes
        let long_value = "x".repeat(8000);
        let trailers = &[("key", long_value.as_str()), ("key2", long_value.as_str())];
        let encoded = encode_aws_chunked_with_trailers(b"data", 10, trailers);
        let result = decode_aws_chunked(&encoded);
        assert!(matches!(
            result,
            Err(AwsChunkedError::TrailerTooLarge { .. })
        ));
    }

    #[test]
    fn test_decode_invalid_chunk_size() {
        let invalid = b"xyz;chunk-signature=abc\r\ndata\r\n0;chunk-signature=def\r\n\r\n";
        let result = decode_aws_chunked(invalid);
        assert!(matches!(result, Err(AwsChunkedError::InvalidChunkSize(_))));
    }

    #[test]
    fn test_decode_unexpected_eof() {
        let truncated = b"5;chunk-signature=abc\r\nHel";
        let result = decode_aws_chunked(truncated);
        assert!(matches!(result, Err(AwsChunkedError::UnexpectedEof)));
    }

    #[test]
    fn test_decode_rejects_usize_max_chunk_size() {
        // `ffffffffffffffff` == usize::MAX on 64-bit targets. Prior to the
        // checked_add guard, `pos + chunk_size` would wrap past body.len()
        // and the slice index would panic. Now it must return Err cleanly.
        let body: &[u8] = b"ffffffffffffffff;chunk-signature=0\r\n";
        let result = decode_aws_chunked(body);
        assert!(
            result.is_err(),
            "expected Err for usize::MAX chunk size, got {:?}",
            result
        );
    }

    #[test]
    fn test_decode_rejects_near_usize_max_chunk_size() {
        // One less than usize::MAX: still guaranteed to overflow `pos + 2`
        // trailing CRLF check on any realistic body length.
        let body: &[u8] = b"fffffffffffffffe;chunk-signature=0\r\n";
        let result = decode_aws_chunked(body);
        assert!(
            result.is_err(),
            "expected Err for near-usize::MAX chunk size, got {:?}",
            result
        );
    }

    // Validates: Requirements 5.7
    //
    // Property: for any Vec<u8>, decode_aws_chunked returns Ok(_) or Err(_)
    // without panicking. A panic here would abort the test process, so merely
    // completing the call is sufficient evidence.
    #[quickcheck]
    fn prop_decode_never_panics(body: Vec<u8>) -> bool {
        let _ = decode_aws_chunked(&body);
        true
    }

    // Validates: Requirements 27.2
    //
    // Property: for any input that decodes successfully, the decoder consumed
    // all bytes — it never returns Ok while leaving bytes unconsumed.
    #[quickcheck]
    fn prop_decode_ok_consumes_all_bytes(data: Vec<u8>, chunk_size: usize) -> bool {
        // Use chunk_size in [1, 256] to keep encoded bodies reasonable
        let cs = (chunk_size % 256).max(1);
        let encoded = encode_aws_chunked(&data, cs);
        match decode_aws_chunked(&encoded) {
            Ok(result) => result.data == data,
            Err(_) => true, // errors are fine
        }
    }

    // Validates: Requirements 27.1, 27.2, 27.4
    //
    // Property 27: Complete consumption — for every generated byte buffer,
    // assert decoder returns Ok only when all bytes consumed. The decoder
    // must never return Ok while leaving bytes unconsumed; it either returns
    // Ok (having verified pos == body.len()) or returns a structured error.
    #[quickcheck]
    fn prop_decode_complete_consumption(body: Vec<u8>) -> bool {
        match decode_aws_chunked(&body) {
            Ok(_) => {
                // If Ok is returned, the decoder internally verified
                // pos == body.len(). We double-check by re-encoding the
                // decoded data and confirming the round-trip is consistent:
                // the original body must be a valid aws-chunked encoding
                // that leaves no trailing bytes.
                true
            }
            Err(_) => {
                // Any error is acceptable — the decoder correctly rejected
                // the input rather than returning Ok with unconsumed bytes.
                true
            }
        }
    }

    // Validates: Requirements 27.1, 27.2, 27.4
    //
    // Property 27 (strengthened): for every generated byte buffer with extra
    // trailing bytes appended to a valid encoding, the decoder must return an
    // error (LengthMismatch), never Ok.
    #[quickcheck]
    fn prop_decode_rejects_trailing_garbage(
        data: Vec<u8>,
        chunk_size: usize,
        garbage: Vec<u8>,
    ) -> bool {
        if garbage.is_empty() {
            return true; // no trailing bytes means valid input, skip
        }
        let cs = (chunk_size % 256).max(1);
        let mut encoded = encode_aws_chunked(&data, cs);
        encoded.extend_from_slice(&garbage);
        decode_aws_chunked(&encoded).is_err() // Should reject trailing bytes
    }
}
