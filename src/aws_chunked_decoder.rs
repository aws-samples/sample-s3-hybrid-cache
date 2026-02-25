//! AWS Chunked Encoding Decoder
//!
//! Decodes aws-chunked encoded bodies used by AWS CLI for SigV4 streaming uploads.
//! The format is: `chunk-size;chunk-signature=sig\r\n data\r\n ... 0;chunk-signature=sig\r\n\r\n`
//!
//! This module provides functions to:
//! - Detect aws-chunked encoding from request headers
//! - Extract the decoded content length from headers
//! - Decode aws-chunked bodies to extract actual data
//! - Encode data as aws-chunked format (for testing round-trips)

use std::collections::HashMap;
use std::fmt;

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
        }
    }
}

impl std::error::Error for AwsChunkedError {}

/// Streaming SigV4 payload indicator header value
const STREAMING_AWS4_HMAC_SHA256_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Check if a request uses aws-chunked encoding.
///
/// Returns true if either:
/// - The `content-encoding` header contains `aws-chunked`
/// - The `x-amz-content-sha256` header equals `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
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
    if let Some(sha256) = headers.get("x-amz-content-sha256") {
        if sha256 == STREAMING_AWS4_HMAC_SHA256_PAYLOAD {
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

/// Decode aws-chunked body to extract actual data.
///
/// Parses the aws-chunked format:
/// ```text
/// chunk-size;chunk-signature=signature\r\n
/// chunk-data\r\n
/// ...
/// 0;chunk-signature=signature\r\n
/// \r\n
/// ```
///
/// # Arguments
/// * `body` - Raw aws-chunked encoded body bytes
///
/// # Returns
/// `Ok(Vec<u8>)` containing the decoded data, or an error if parsing fails
pub fn decode_aws_chunked(body: &[u8]) -> Result<Vec<u8>, AwsChunkedError> {
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

        // If chunk size is 0, we've reached the final chunk
        if chunk_size == 0 {
            // Skip the final \r\n after the zero-length chunk
            if pos < body.len() && body.len() >= pos + 2 {
                // Verify trailing \r\n exists
                if body[pos..].starts_with(b"\r\n") {
                    // Successfully parsed
                }
            }
            break;
        }

        // Read chunk data
        if pos + chunk_size > body.len() {
            return Err(AwsChunkedError::UnexpectedEof);
        }

        result.extend_from_slice(&body[pos..pos + chunk_size]);
        pos += chunk_size;

        // Skip trailing \r\n after chunk data
        if pos + 2 > body.len() {
            return Err(AwsChunkedError::UnexpectedEof);
        }
        if &body[pos..pos + 2] != b"\r\n" {
            return Err(AwsChunkedError::InvalidChunkHeader(
                "Missing CRLF after chunk data".to_string(),
            ));
        }
        pos += 2;
    }

    Ok(result)
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

    // Write final zero-length chunk: "0;chunk-signature=sig\r\n\r\n"
    let final_header = format!("0;chunk-signature={}\r\n\r\n", placeholder_sig);
    result.extend_from_slice(final_header.as_bytes());

    result
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_empty() {
        let original: &[u8] = b"";
        let encoded = encode_aws_chunked(original, 10);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_single_chunk() {
        let original = b"Small data";
        let encoded = encode_aws_chunked(original, 1000);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_multiple_chunks() {
        let original = b"This is a longer piece of data that will be split into multiple chunks";
        let encoded = encode_aws_chunked(original, 10);
        let decoded = decode_aws_chunked(&encoded).unwrap();
        assert_eq!(decoded, original);
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
}
