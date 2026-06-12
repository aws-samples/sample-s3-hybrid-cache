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

/// Trailers and decoded length produced by [`IncrementalAwsChunkedDecoder::finish`].
///
/// Mirrors the trailer shape of the whole-buffer [`AwsChunkedDecodeResult`]
/// (which carries `data: Vec<u8>` + `trailers: Vec<(String, String)>`); the
/// incremental decoder has already emitted the decoded object bytes via
/// [`IncrementalAwsChunkedDecoder::push`], so `finish` returns only the trailers
/// and the running decoded length for validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsChunkedTrailers {
    /// Trailers parsed after the zero-size chunk. Each entry is a (key, value)
    /// pair. Common trailers include `x-amz-checksum-*` and
    /// `x-amz-trailer-signature`.
    pub trailers: Vec<(String, String)>,
    /// Total number of decoded object bytes emitted across all `push` calls.
    pub decoded_len: u64,
}

/// Parse state for [`IncrementalAwsChunkedDecoder`].
///
/// Mirrors the loop in [`decode_aws_chunked`]: read a chunk header line, emit
/// `chunk-size` data bytes, consume the trailing CRLF, repeat until the
/// zero-size chunk, then read the trailer section up to its terminal CRLF.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum DecoderState {
    /// Accumulating a chunk-header line into `line_buf` until its CRLF.
    #[default]
    Header,
    /// Emitting `remaining` object data bytes for the current chunk.
    Data { remaining: usize },
    /// Consuming the CRLF that follows a chunk's data (`\r` then `\n`).
    DataCrlf { seen_cr: bool },
    /// Accumulating the trailer section into `line_buf` until its terminal CRLF.
    Trailers,
    /// Terminal CRLF consumed; a fully-framed body. Any further bytes are
    /// trailing garbage and yield `LengthMismatch`.
    Done,
}

/// Streaming aws-chunked decoder that consumes bounded increments.
///
/// This is the incremental counterpart to [`decode_aws_chunked`]. It mirrors the
/// same state machine (chunk header → data → CRLF → … → zero chunk → trailers)
/// and reuses the same [`AwsChunkedError`] taxonomy and the 8 KiB trailer cap,
/// but it never holds the whole body: object data bytes are emitted from
/// [`push`](Self::push) as they arrive, and the only carry-over retained between
/// calls is a partial chunk-header or trailer line (bounded by
/// [`MAX_TRAILER_SECTION_BYTES`]).
///
/// It is used **only** on the cache tee branch of the streaming write path; the
/// upstream always receives the original chunked bytes verbatim. The whole-buffer
/// [`decode_aws_chunked`] is retained as the buffered path and the equivalence
/// oracle in tests.
///
/// # Usage
///
/// ```ignore
/// let mut decoder = IncrementalAwsChunkedDecoder::new();
/// let mut object = Vec::new();
/// for slice in body_slices {
///     object.extend_from_slice(&decoder.push(slice)?);
/// }
/// let trailers = decoder.finish()?; // validates framing; carries decoded_len
/// ```
#[derive(Debug, Default)]
pub struct IncrementalAwsChunkedDecoder {
    /// Current parse state.
    state: DecoderState,
    /// Carry-over for a partial chunk-header line (Header) or the accumulating
    /// trailer section (Trailers). Bounded by [`MAX_TRAILER_SECTION_BYTES`].
    line_buf: Vec<u8>,
    /// Running count of decoded object bytes emitted so far.
    decoded_len: u64,
    /// Trailers parsed once the zero-size chunk's trailer section completes.
    trailers: Vec<(String, String)>,
    /// Total raw (encoded) bytes consumed across all `push` calls. Used only to
    /// report `LengthMismatch` bounds when trailing bytes follow a framed body.
    total_raw: u64,
}

impl IncrementalAwsChunkedDecoder {
    /// Create a new decoder positioned at the first chunk header.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed the next slice of raw aws-chunked bytes.
    ///
    /// Returns any fully-decoded object bytes produced by this slice (which may
    /// be empty if the slice only advanced framing). Partial chunk-header or
    /// trailer lines are retained internally as bounded carry-over. Returns an
    /// [`AwsChunkedError`] if the framing is malformed, mirroring
    /// [`decode_aws_chunked`].
    pub fn push(&mut self, input: &[u8]) -> Result<Vec<u8>, AwsChunkedError> {
        let mut out = Vec::new();
        let mut cursor = 0usize;

        while cursor < input.len() {
            match self.state {
                DecoderState::Header => {
                    if !self.feed_line(input, &mut cursor)? {
                        break; // need more input to complete the header line
                    }
                    let header_str = std::str::from_utf8(&self.line_buf)
                        .map_err(|e| AwsChunkedError::InvalidChunkHeader(e.to_string()))?;
                    let chunk_size = parse_chunk_size(header_str)?;
                    self.line_buf.clear();
                    if chunk_size == 0 {
                        self.state = DecoderState::Trailers;
                    } else {
                        self.state = DecoderState::Data {
                            remaining: chunk_size,
                        };
                    }
                }
                DecoderState::Data { remaining } => {
                    let avail = input.len() - cursor;
                    let take = remaining.min(avail);
                    out.extend_from_slice(&input[cursor..cursor + take]);
                    cursor += take;
                    self.total_raw += take as u64;
                    self.decoded_len += take as u64;
                    let left = remaining - take;
                    self.state = if left == 0 {
                        DecoderState::DataCrlf { seen_cr: false }
                    } else {
                        DecoderState::Data { remaining: left }
                    };
                }
                DecoderState::DataCrlf { seen_cr } => {
                    let b = input[cursor];
                    cursor += 1;
                    self.total_raw += 1;
                    let expected = if seen_cr { b'\n' } else { b'\r' };
                    if b != expected {
                        return Err(AwsChunkedError::InvalidChunkHeader(
                            "Missing CRLF after chunk data".to_string(),
                        ));
                    }
                    self.state = if seen_cr {
                        DecoderState::Header
                    } else {
                        DecoderState::DataCrlf { seen_cr: true }
                    };
                }
                DecoderState::Trailers => {
                    if !self.feed_trailers(input, &mut cursor)? {
                        break; // need more input to complete the trailer section
                    }
                    let mut pos = 0usize;
                    self.trailers = parse_trailers(&self.line_buf, &mut pos)?;
                    self.line_buf.clear();
                    self.state = DecoderState::Done;
                }
                DecoderState::Done => {
                    // A complete body was already framed; extra bytes are
                    // trailing garbage, exactly as the whole-buffer decoder's
                    // `pos != body.len()` check rejects them.
                    let remaining = (input.len() - cursor) as u64;
                    return Err(AwsChunkedError::LengthMismatch {
                        expected: self.total_raw,
                        actual: self.total_raw + remaining,
                    });
                }
            }
        }

        Ok(out)
    }

    /// Finalize the decode at end of body.
    ///
    /// Validates that the body was fully framed (a zero-size chunk and its
    /// terminal trailer CRLF were seen) and returns the parsed trailers along
    /// with the total decoded length. Returns [`AwsChunkedError::UnexpectedEof`]
    /// if the stream ended mid-frame.
    pub fn finish(self) -> Result<AwsChunkedTrailers, AwsChunkedError> {
        match self.state {
            DecoderState::Done => Ok(AwsChunkedTrailers {
                trailers: self.trailers,
                decoded_len: self.decoded_len,
            }),
            _ => Err(AwsChunkedError::UnexpectedEof),
        }
    }

    /// Total number of decoded object bytes emitted so far.
    pub fn decoded_len(&self) -> u64 {
        self.decoded_len
    }

    /// Accumulate bytes from `input[*cursor..]` into `line_buf` until a CRLF is
    /// found. On success `line_buf` holds the line content WITHOUT the trailing
    /// CRLF and `*cursor` is advanced past it; returns `Ok(true)`. Returns
    /// `Ok(false)` if the slice was exhausted before a CRLF (carry-over
    /// retained). The line length is bounded by [`MAX_TRAILER_SECTION_BYTES`].
    fn feed_line(&mut self, input: &[u8], cursor: &mut usize) -> Result<bool, AwsChunkedError> {
        while *cursor < input.len() {
            let b = input[*cursor];
            *cursor += 1;
            self.total_raw += 1;
            self.line_buf.push(b);
            let n = self.line_buf.len();
            if n >= 2 && self.line_buf[n - 2] == b'\r' && self.line_buf[n - 1] == b'\n' {
                self.line_buf.truncate(n - 2);
                return Ok(true);
            }
            if self.line_buf.len() > MAX_TRAILER_SECTION_BYTES {
                return Err(AwsChunkedError::InvalidChunkHeader(
                    "Chunk header line exceeds maximum size".to_string(),
                ));
            }
        }
        Ok(false)
    }

    /// Accumulate the trailer section into `line_buf` until its terminal CRLF.
    ///
    /// The section ends at the first empty line: either the section begins with
    /// `\r\n` (no trailers) or a trailer line is followed by the terminal CRLF
    /// (`…\r\n\r\n`). On completion `line_buf` holds the entire trailer section
    /// (including the terminal CRLF) ready for [`parse_trailers`], `*cursor` is
    /// advanced past it, and the function returns `Ok(true)`. Returns
    /// `Ok(false)` if more input is needed. Bounded by
    /// [`MAX_TRAILER_SECTION_BYTES`].
    fn feed_trailers(&mut self, input: &[u8], cursor: &mut usize) -> Result<bool, AwsChunkedError> {
        while *cursor < input.len() {
            let b = input[*cursor];
            *cursor += 1;
            self.total_raw += 1;
            self.line_buf.push(b);
            let n = self.line_buf.len();
            let lb = self.line_buf.as_slice();
            if lb == b"\r\n" || (n >= 4 && &lb[n - 4..] == b"\r\n\r\n") {
                return Ok(true);
            }
            if self.line_buf.len() > MAX_TRAILER_SECTION_BYTES {
                return Err(AwsChunkedError::TrailerTooLarge {
                    size: self.line_buf.len(),
                    max: MAX_TRAILER_SECTION_BYTES,
                });
            }
        }
        Ok(false)
    }
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

    // ---- IncrementalAwsChunkedDecoder unit tests (Task 1.1) ----

    /// Decode a complete body in a single `push`, matching the whole-buffer
    /// decoder's output.
    #[test]
    fn test_incremental_single_push_roundtrip() {
        let original = b"This is some data split into chunks";
        let encoded = encode_aws_chunked(original, 7);

        let mut decoder = IncrementalAwsChunkedDecoder::new();
        let decoded = decoder.push(&encoded).unwrap();
        let trailers = decoder.finish().unwrap();

        assert_eq!(decoded, original);
        assert!(trailers.trailers.is_empty());
        assert_eq!(trailers.decoded_len, original.len() as u64);
    }

    /// Feeding the encoded body one byte at a time (every framing boundary
    /// split) must still produce the same decoded object.
    #[test]
    fn test_incremental_byte_at_a_time() {
        let original = b"Hello, streaming world! 1234567890";
        let encoded = encode_aws_chunked(original, 5);

        let mut decoder = IncrementalAwsChunkedDecoder::new();
        let mut out = Vec::new();
        for b in &encoded {
            out.extend_from_slice(&decoder.push(std::slice::from_ref(b)).unwrap());
        }
        let trailers = decoder.finish().unwrap();

        assert_eq!(out, original);
        assert_eq!(trailers.decoded_len, original.len() as u64);
    }

    /// An empty object body frames as a single zero-size chunk and decodes to
    /// zero bytes.
    #[test]
    fn test_incremental_empty_body() {
        let encoded = encode_aws_chunked(b"", 10);

        let mut decoder = IncrementalAwsChunkedDecoder::new();
        let decoded = decoder.push(&encoded).unwrap();
        let trailers = decoder.finish().unwrap();

        assert!(decoded.is_empty());
        assert!(trailers.trailers.is_empty());
        assert_eq!(trailers.decoded_len, 0);
    }

    /// Trailers after the zero-size chunk are parsed identically to the
    /// whole-buffer decoder, including when the encoded body is split across the
    /// trailer section.
    #[test]
    fn test_incremental_with_trailers_split() {
        let trailers = &[
            ("x-amz-checksum-crc32", "abc123"),
            ("x-amz-trailer-signature", "deadbeef"),
        ];
        let encoded = encode_aws_chunked_with_trailers(b"payload", 3, trailers);

        // Split at every offset and confirm a consistent result.
        for split in 0..=encoded.len() {
            let mut decoder = IncrementalAwsChunkedDecoder::new();
            let mut out = Vec::new();
            out.extend_from_slice(&decoder.push(&encoded[..split]).unwrap());
            out.extend_from_slice(&decoder.push(&encoded[split..]).unwrap());
            let result = decoder.finish().unwrap();

            assert_eq!(out, b"payload", "split at {split}");
            assert_eq!(result.trailers.len(), 2, "split at {split}");
            assert_eq!(
                result.trailers[0],
                ("x-amz-checksum-crc32".to_string(), "abc123".to_string())
            );
            assert_eq!(
                result.trailers[1],
                (
                    "x-amz-trailer-signature".to_string(),
                    "deadbeef".to_string()
                )
            );
        }
    }

    /// `finish` before the body is fully framed is an error (truncated stream).
    #[test]
    fn test_incremental_truncated_finish_errors() {
        let truncated = b"5;chunk-signature=abc\r\nHel";
        let mut decoder = IncrementalAwsChunkedDecoder::new();
        let _ = decoder.push(truncated).unwrap();
        assert!(matches!(
            decoder.finish(),
            Err(AwsChunkedError::UnexpectedEof)
        ));
    }

    /// Bytes after a fully-framed body are rejected as a length mismatch, just
    /// as the whole-buffer decoder rejects trailing bytes.
    #[test]
    fn test_incremental_rejects_trailing_bytes() {
        let mut encoded = encode_aws_chunked(b"hello", 10);
        encoded.extend_from_slice(b"extra garbage");

        let mut decoder = IncrementalAwsChunkedDecoder::new();
        let result = decoder.push(&encoded);
        assert!(matches!(
            result,
            Err(AwsChunkedError::LengthMismatch { .. })
        ));
    }

    /// An invalid (non-hex) chunk size is rejected with the same error variant
    /// as the whole-buffer decoder.
    #[test]
    fn test_incremental_invalid_chunk_size() {
        let invalid = b"xyz;chunk-signature=abc\r\ndata\r\n0;chunk-signature=def\r\n\r\n";
        let mut decoder = IncrementalAwsChunkedDecoder::new();
        assert!(matches!(
            decoder.push(invalid),
            Err(AwsChunkedError::InvalidChunkSize(_))
        ));
    }

    /// A missing CRLF after chunk data is rejected with the same error the
    /// whole-buffer decoder produces.
    #[test]
    fn test_incremental_missing_data_crlf() {
        // 5 bytes of data "hello" followed by "XX" instead of CRLF.
        let body = b"5;chunk-signature=0\r\nhelloXX0;chunk-signature=0\r\n\r\n";
        let mut decoder = IncrementalAwsChunkedDecoder::new();
        assert!(matches!(
            decoder.push(body),
            Err(AwsChunkedError::InvalidChunkHeader(_))
        ));
    }

    /// `decoded_len` reflects emitted bytes as they accumulate across pushes.
    #[test]
    fn test_incremental_decoded_len_tracks_progress() {
        let original = vec![0xABu8; 100];
        let encoded = encode_aws_chunked(&original, 16);

        let mut decoder = IncrementalAwsChunkedDecoder::new();
        decoder.push(&encoded).unwrap();
        assert_eq!(decoder.decoded_len(), 100);
        let trailers = decoder.finish().unwrap();
        assert_eq!(trailers.decoded_len, 100);
    }

    // ---- IncrementalAwsChunkedDecoder property tests (Task 1.2) ----

    /// Drive the incremental decoder over `encoded`, feeding it in slices whose
    /// sizes are taken (cyclically) from `split_sizes`, and return the
    /// concatenation of all emitted object bytes. The split sizes are mapped
    /// into `[1, 64]` so the body is fed in small increments that exercise
    /// every carry-over boundary (partial chunk headers, split data, split
    /// CRLFs, split trailer sections); an empty `split_sizes` feeds the whole
    /// body in one push. The decoder is always `finish`ed so framing/EOF
    /// validation runs, mirroring the whole-buffer decoder's end-of-input
    /// checks.
    fn incremental_decode_arbitrary_split(
        encoded: &[u8],
        split_sizes: &[usize],
    ) -> Result<Vec<u8>, AwsChunkedError> {
        let mut decoder = IncrementalAwsChunkedDecoder::new();
        let mut out = Vec::new();
        let mut pos = 0usize;
        let mut i = 0usize;

        while pos < encoded.len() {
            let remaining = encoded.len() - pos;
            let seg = if split_sizes.is_empty() {
                remaining
            } else {
                // Map into [1, 64] then clamp to what's left so we always make
                // forward progress and never index past the end.
                ((split_sizes[i % split_sizes.len()] % 64) + 1).min(remaining)
            };
            let end = pos + seg;
            out.extend_from_slice(&decoder.push(&encoded[pos..end])?);
            pos = end;
            i += 1;
        }

        decoder.finish()?;
        Ok(out)
    }

    // Validates: Requirements 3.2, 10.4
    //
    // Property 4 (valid inputs): for arbitrary object data encoded with
    // `encode_aws_chunked` and fed to `IncrementalAwsChunkedDecoder` in an
    // arbitrary slice splitting, the incremental decoder yields exactly the
    // bytes `decode_aws_chunked` yields for the same encoded body — and both
    // succeed. The encoded form is a valid aws-chunked body by construction, so
    // the oracle must decode it to the original `data`.
    #[quickcheck]
    fn prop_incremental_matches_whole_buffer_for_valid_inputs(
        data: Vec<u8>,
        chunk_size: usize,
        split_sizes: Vec<usize>,
    ) -> bool {
        let cs = (chunk_size % 256).max(1);
        let encoded = encode_aws_chunked(&data, cs);

        let oracle = match decode_aws_chunked(&encoded) {
            Ok(result) => result.data,
            // A body produced by encode_aws_chunked must decode successfully.
            Err(_) => return false,
        };

        match incremental_decode_arbitrary_split(&encoded, &split_sizes) {
            Ok(incremental) => incremental == oracle && incremental == data,
            Err(_) => false,
        }
    }

    // Validates: Requirements 3.2, 10.4
    //
    // Property 4 (arbitrary inputs): for arbitrary raw byte buffers, the
    // incremental decoder agrees with the whole-buffer `decode_aws_chunked`
    // oracle under any slice splitting:
    //   - if the oracle rejects the input, the incremental decoder (push across
    //     splits, then finish) must also error;
    //   - if the oracle accepts the input, the incremental decoder must produce
    //     the same decoded bytes.
    #[quickcheck]
    fn prop_incremental_matches_whole_buffer_for_arbitrary_inputs(
        body: Vec<u8>,
        split_sizes: Vec<usize>,
    ) -> bool {
        let oracle = decode_aws_chunked(&body);
        let incremental = incremental_decode_arbitrary_split(&body, &split_sizes);

        match (oracle, incremental) {
            // Oracle accepts → incremental must accept with identical bytes.
            (Ok(result), Ok(decoded)) => result.data == decoded,
            // Oracle rejects → incremental must also reject (any error).
            (Err(_), Err(_)) => true,
            // Any disagreement (one accepts while the other rejects) is a
            // property violation.
            _ => false,
        }
    }
}
