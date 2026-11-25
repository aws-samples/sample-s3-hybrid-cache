//! S3's `append-md5` trailer, and what a cache fill stores when a response carries one.
//!
//! The AWS SDK for Java 2.x sync `GetObject` sends `x-amz-te: append-md5` by default
//! (`ChecksumsEnabledValidator.getObjectChecksumEnabledPerRequest`, SDK 2.59.1). S3 then
//! appends the 16-byte MD5 of the bytes it sends to the body and answers
//! `x-amz-transfer-encoding: append-md5`, with `Content-Length` covering the trailer. A ranged
//! or `partNumber` request is answered `200` rather than `206`, with no `Content-Range` and
//! with `x-amz-content-range: bytes a-b/N` naming the slice. The SDK removes the trailer only
//! when the response carries `x-amz-transfer-encoding: append-md5`
//! (`getObjectChecksumEnabledPerResponse`), so every other client reads it as object bytes.
//!
//! A fill therefore stores the object's own bytes under the headers S3 would have sent without
//! `x-amz-te`: [`fill_shape`] gives those headers and the object length, and
//! [`TrailerSplitter`] or [`verify_buffered`] check the trailer against the stored bytes
//! before anything commits. The client that asked still receives S3's response unchanged.
//! A cache hit carries neither header, which the SDK accepts as it accepts an entry another
//! client filled.
use std::collections::HashMap;

use md5::{Digest, Md5};

/// The trailer's length: one MD5 digest.
pub(crate) const TRAILER_LEN: u64 = 16;

/// The response header that announces the trailer.
pub(crate) const TRANSFER_ENCODING_HEADER: &str = "x-amz-transfer-encoding";

/// The header S3 sends in place of `Content-Range` on a ranged or part answer with a trailer.
pub(crate) const CONTENT_RANGE_HEADER: &str = "x-amz-content-range";

/// How a successful GET response must be treated before any of its body is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FillShape {
    /// No trailer: store the response as S3 sent it.
    Plain,
    /// A trailer: store only the first `object_len` body bytes, under `headers`, once the
    /// trailer matches them. `headers` carry `content-length: object_len`, no
    /// `x-amz-transfer-encoding` and no `x-amz-content-range`, and, when S3 named the slice,
    /// `content-range` with S3's value.
    Trailer {
        headers: HashMap<String, String>,
        object_len: u64,
    },
    /// A trailer the proxy cannot remove with certainty (an unknown encoding, no usable
    /// length, or a slice whose extent disagrees with the length): store nothing.
    Unstorable,
}

fn header<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
}

/// `bytes a-b/N` as `(a, b, N)`.
fn parse_byte_range(value: &str) -> Option<(u64, u64, u64)> {
    let rest = value.trim().strip_prefix("bytes ")?;
    let (span, total) = rest.split_once('/')?;
    let (start, end) = span.split_once('-')?;
    let (start, end, total) = (
        start.trim().parse::<u64>().ok()?,
        end.trim().parse::<u64>().ok()?,
        total.trim().parse::<u64>().ok()?,
    );
    (start <= end && end < total).then_some((start, end, total))
}

/// What a fill of this response may store. See [`FillShape`].
pub(crate) fn fill_shape(headers: &HashMap<String, String>) -> FillShape {
    let Some(encoding) = header(headers, TRANSFER_ENCODING_HEADER) else {
        // S3 sends `x-amz-content-range` only with the trailer. Alone it is a shape this
        // module has not seen, so it is not stored rather than guessed at.
        return if header(headers, CONTENT_RANGE_HEADER).is_some() {
            FillShape::Unstorable
        } else {
            FillShape::Plain
        };
    };
    if !encoding.trim().eq_ignore_ascii_case("append-md5") {
        return FillShape::Unstorable;
    }
    let Some(wire_len) =
        header(headers, "content-length").and_then(|v| v.trim().parse::<u64>().ok())
    else {
        return FillShape::Unstorable;
    };
    if wire_len < TRAILER_LEN || header(headers, "content-range").is_some() {
        return FillShape::Unstorable;
    }
    let object_len = wire_len - TRAILER_LEN;
    let slice = match header(headers, CONTENT_RANGE_HEADER) {
        Some(value) => match parse_byte_range(value) {
            Some((start, end, _)) if end - start + 1 == object_len => {
                Some(value.trim().to_string())
            }
            _ => return FillShape::Unstorable,
        },
        None => None,
    };
    let mut stored: HashMap<String, String> = headers
        .iter()
        .filter(|(k, _)| {
            !k.eq_ignore_ascii_case(TRANSFER_ENCODING_HEADER)
                && !k.eq_ignore_ascii_case(CONTENT_RANGE_HEADER)
                && !k.eq_ignore_ascii_case("content-length")
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    stored.insert("content-length".to_string(), object_len.to_string());
    if let Some(slice) = slice {
        stored.insert("content-range".to_string(), slice);
    }
    FillShape::Trailer {
        headers: stored,
        object_len,
    }
}

/// Whether the stored bytes matched the trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrailerVerdict {
    /// The trailer is the MD5 of the stored bytes.
    Match,
    /// A complete trailer that is not the MD5 of the stored bytes.
    Mismatch,
    /// The body ended before the object bytes and a whole trailer had arrived, or ran past it.
    Incomplete,
}

impl std::fmt::Display for TrailerVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TrailerVerdict::Match => "append-md5 trailer matches",
            TrailerVerdict::Mismatch => "append-md5 trailer does not match the stored bytes",
            TrailerVerdict::Incomplete => "append-md5 body did not end with exactly one trailer",
        })
    }
}

/// Splits a streamed body into the object bytes to store and the trailer to check, hashing the
/// object bytes as they pass.
pub(crate) struct TrailerSplitter {
    remaining: u64,
    md5: Md5,
    trailer: Vec<u8>,
    overrun: bool,
}

impl TrailerSplitter {
    pub(crate) fn new(object_len: u64) -> Self {
        Self {
            remaining: object_len,
            md5: Md5::new(),
            trailer: Vec::with_capacity(TRAILER_LEN as usize),
            overrun: false,
        }
    }

    /// The object bytes in `chunk`. Bytes past the object are kept as the trailer.
    pub(crate) fn object_part<'a>(&mut self, chunk: &'a [u8]) -> &'a [u8] {
        let take = (self.remaining.min(chunk.len() as u64)) as usize;
        let (object, rest) = chunk.split_at(take);
        self.md5.update(object);
        self.remaining -= take as u64;
        if self.trailer.len() + rest.len() > TRAILER_LEN as usize {
            self.overrun = true;
        } else {
            self.trailer.extend_from_slice(rest);
        }
        object
    }

    pub(crate) fn verify(self) -> TrailerVerdict {
        if self.overrun || self.remaining > 0 || self.trailer.len() != TRAILER_LEN as usize {
            return TrailerVerdict::Incomplete;
        }
        if self.md5.finalize().as_slice() == self.trailer.as_slice() {
            TrailerVerdict::Match
        } else {
            TrailerVerdict::Mismatch
        }
    }
}

/// [`TrailerSplitter`] over a whole buffered body: its first `object_len` bytes are what may be
/// stored, on [`TrailerVerdict::Match`].
pub(crate) fn verify_buffered(body: &[u8], object_len: u64) -> TrailerVerdict {
    let mut splitter = TrailerSplitter::new(object_len);
    splitter.object_part(body);
    splitter.verify()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn md5_of(data: &[u8]) -> Vec<u8> {
        Md5::digest(data).to_vec()
    }

    #[test]
    fn md5_matches_rfc_1321_vectors() {
        assert_eq!(hex::encode(md5_of(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(
            hex::encode(md5_of(b"abc")),
            "900150983cd24fb0d6963f7d28e17f72"
        );
    }

    #[test]
    fn a_response_without_the_trailer_is_plain() {
        let h = with(&[("content-length", "1024"), ("etag", "\"e\"")]);
        assert_eq!(fill_shape(&h), FillShape::Plain);
    }

    #[test]
    fn a_whole_object_trailer_is_stored_at_the_object_length_without_its_headers() {
        let h = with(&[
            ("content-length", "1040"),
            ("x-amz-transfer-encoding", "append-md5"),
            ("etag", "\"e\""),
        ]);
        let FillShape::Trailer {
            headers,
            object_len,
        } = fill_shape(&h)
        else {
            panic!("not a trailer shape");
        };
        assert_eq!(object_len, 1024);
        assert_eq!(
            headers.get("content-length").map(String::as_str),
            Some("1024")
        );
        assert!(!headers.contains_key("x-amz-transfer-encoding"));
        assert!(!headers.contains_key("content-range"));
        assert_eq!(headers.get("etag").map(String::as_str), Some("\"e\""));
    }

    #[test]
    fn a_part_answer_becomes_a_content_range_at_its_offset() {
        let h = with(&[
            ("Content-Length", "116"),
            ("X-Amz-Transfer-Encoding", "append-md5"),
            ("X-Amz-Content-Range", "bytes 100-199/400"),
            ("x-amz-mp-parts-count", "4"),
        ]);
        let FillShape::Trailer {
            headers,
            object_len,
        } = fill_shape(&h)
        else {
            panic!("not a trailer shape");
        };
        assert_eq!(object_len, 100);
        assert_eq!(
            headers.get("content-range").map(String::as_str),
            Some("bytes 100-199/400")
        );
        assert!(headers
            .keys()
            .all(|k| !k.eq_ignore_ascii_case("x-amz-content-range")
                && !k.eq_ignore_ascii_case("x-amz-transfer-encoding")));
        assert_eq!(
            headers.get("content-length").map(String::as_str),
            Some("100")
        );
    }

    #[test]
    fn shapes_the_proxy_cannot_strip_are_not_stored() {
        for h in [
            with(&[
                ("content-length", "116"),
                ("x-amz-transfer-encoding", "other"),
            ]),
            with(&[("x-amz-transfer-encoding", "append-md5")]),
            with(&[
                ("content-length", "15"),
                ("x-amz-transfer-encoding", "append-md5"),
            ]),
            with(&[
                ("content-length", "116"),
                ("x-amz-transfer-encoding", "append-md5"),
                ("x-amz-content-range", "bytes 0-9/400"),
            ]),
            with(&[
                ("content-length", "116"),
                ("x-amz-transfer-encoding", "append-md5"),
                ("content-range", "bytes 0-99/400"),
            ]),
            with(&[
                ("content-length", "100"),
                ("x-amz-content-range", "bytes 0-99/400"),
            ]),
        ] {
            assert_eq!(fill_shape(&h), FillShape::Unstorable, "{h:?}");
        }
    }

    #[test]
    fn the_splitter_keeps_the_trailer_out_of_the_stored_bytes_across_chunk_boundaries() {
        let object: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let mut wire = object.clone();
        wire.extend_from_slice(&md5_of(&object));
        for chunk_len in [1usize, 7, 16, 999, 1000, 1001, 1016] {
            let mut splitter = TrailerSplitter::new(object.len() as u64);
            let mut stored = Vec::new();
            for chunk in wire.chunks(chunk_len) {
                stored.extend_from_slice(splitter.object_part(chunk));
            }
            assert_eq!(stored, object, "chunk {chunk_len}");
            assert_eq!(
                splitter.verify(),
                TrailerVerdict::Match,
                "chunk {chunk_len}"
            );
        }
    }

    #[test]
    fn a_wrong_short_or_long_trailer_is_refused() {
        let object = b"hello world".to_vec();
        let mut wrong = object.clone();
        wrong.extend_from_slice(&[0u8; 16]);
        assert_eq!(verify_buffered(&wrong, 11), TrailerVerdict::Mismatch);
        let mut right = object.clone();
        right.extend_from_slice(&md5_of(&object));
        assert_eq!(verify_buffered(&right, 11), TrailerVerdict::Match);
        assert_eq!(
            verify_buffered(&right[..26], 11),
            TrailerVerdict::Incomplete
        );
        let mut long = right.clone();
        long.push(0);
        assert_eq!(verify_buffered(&long, 11), TrailerVerdict::Incomplete);
    }
}
