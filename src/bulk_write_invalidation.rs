//! Which cache keys an S3 write invalidates when the object key is not in the request path.
//!
//! Two S3 writes name their objects somewhere other than the URI: DeleteObjects
//! (`POST /{bucket}?delete`) carries decoded key names in an XML body, and a browser form
//! POST upload (`POST /{bucket}`, `multipart/form-data`) carries the key in a `key` form
//! field. A cache key holds the object key in one canonical spelling
//! (`CacheManager::generate_cache_key`), so a decoded key maps to exactly one cache key
//! ([`canonical_cache_key`]). This module parses the two request shapes and decides which
//! keys a response proves, or leaves possible, were written.
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};

use crate::inflight_ledger::{InflightLedger, Reservation};

/// Concurrent invalidations per bulk write; each takes a key's slot lock and writes its
/// write-order marker on shared storage.
pub(crate) const INVALIDATION_CONCURRENCY: usize = 16;

/// The largest DeleteObjects result document read to find its `<Error>` keys (a result
/// for 1,000 keys of 1,024 bytes is about 1.1 MiB). A larger one is streamed through
/// unread and every requested key is invalidated.
pub(crate) const DELETE_RESULT_BOUND: u64 = 4 << 20;

/// The cache-key prefix for a request addressed to a bucket root: the bucket for
/// path-style (`/bucket`) and virtual-hosted (`/` on `bucket.s3...`) requests, or the
/// access-point alias. `None` when the request path names more than a bucket, which is
/// not a DeleteObjects or form POST target.
pub(crate) fn bucket_cache_prefix(path: &str, host: &str) -> Option<String> {
    let key = crate::cache::CacheManager::generate_cache_key(path, Some(host));
    let prefix = key.trim_end_matches('/');
    if prefix.is_empty() || prefix.contains('/') {
        return None;
    }
    Some(prefix.to_string())
}

/// The cache key every GET of the decoded `object_key` in the bucket behind
/// `bucket_prefix` produces, whichever spelling of its path the client sent.
pub(crate) fn canonical_cache_key(bucket_prefix: &str, object_key: &str) -> String {
    format!(
        "{}/{}",
        bucket_prefix,
        crate::cache::encode_s3_object_key(object_key.as_bytes(), false)
    )
}

/// Whether the query string carries the `delete` subresource (DeleteObjects).
pub(crate) fn is_delete_objects_query(query: Option<&str>) -> bool {
    query.is_some_and(|q| {
        q.split('&')
            .any(|pair| pair.split('=').next() == Some("delete"))
    })
}

// ── XML ────────────────────────────────────────────────────────────────────

fn local_name(tag: &str) -> &str {
    let name = tag
        .split(|c: char| c.is_ascii_whitespace() || c == '/')
        .next()
        .unwrap_or("");
    name.rsplit(':').next().unwrap_or(name)
}

fn unescape_into(text: &str, out: &mut String) -> Option<()> {
    let mut rest = text;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let after = &rest[amp + 1..];
        let semi = after.find(';')?;
        let entity = &after[..semi];
        match entity {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ => {
                let code = if let Some(hex) = entity
                    .strip_prefix("#x")
                    .or_else(|| entity.strip_prefix("#X"))
                {
                    u32::from_str_radix(hex, 16).ok()?
                } else {
                    entity.strip_prefix('#')?.parse::<u32>().ok()?
                };
                out.push(char::from_u32(code)?);
            }
        }
        rest = &after[semi + 1..];
    }
    out.push_str(rest);
    Some(())
}

/// The end of a tag starting at `start` (which is `<`), honouring quoted attribute values.
fn tag_end(xml: &str, start: usize) -> Option<usize> {
    let mut quote: Option<u8> = None;
    for (i, &b) in xml.as_bytes()[start..].iter().enumerate() {
        match quote {
            Some(q) if b == q => quote = None,
            Some(_) => {}
            None if b == b'"' || b == b'\'' => quote = Some(b),
            None if b == b'>' => return Some(start + i),
            None => {}
        }
    }
    None
}

/// Whether the pseudo-attributes of a leading `<?xml ...?>` declaration describe a document
/// this reader decodes correctly: `version="1.0"`, then an optional `encoding` of `UTF-8`
/// (case-insensitive), then an optional `standalone` of any value, in that order. Any other
/// version or encoding would make a conforming parser read different key text.
fn declaration_is_readable(attrs: &str) -> bool {
    let mut rest = attrs.trim();
    let mut last = None;
    while !rest.is_empty() {
        let Some(eq) = rest.find('=') else {
            return false;
        };
        let name = rest[..eq].trim();
        let after = rest[eq + 1..].trim_start();
        let Some(quote @ ('"' | '\'')) = after.chars().next() else {
            return false;
        };
        let body = &after[1..];
        let Some(end) = body.find(quote) else {
            return false;
        };
        let value = &body[..end];
        let position = match name {
            "version" if value == "1.0" => 0,
            "encoding" if value.eq_ignore_ascii_case("UTF-8") => 1,
            "standalone" => 2,
            _ => return false,
        };
        if last.is_some_and(|l| l >= position) || (last.is_none() && position != 0) {
            return false;
        }
        last = Some(position);
        let tail = &body[end + 1..];
        if !tail.is_empty() && !tail.starts_with(|c: char| c.is_ascii_whitespace()) {
            return false;
        }
        rest = tail.trim_start();
    }
    last.is_some()
}

/// The one reader for the two S3 documents here. Returns the root element's local name
/// and, for every `Key` element, its parent's local name and decoded text.
///
/// It accepts elements, text, the five predefined and numeric character references, one
/// `<?xml ...?>` declaration at offset 0 (version 1.0, UTF-8 or no encoding), and well-formed comments between elements. It
/// returns `None` for everything else, so no partial key list is ever produced: a DOCTYPE,
/// any other processing instruction, a CDATA section, a comment inside a `Key`, a comment
/// holding `--`, an unterminated comment, an unknown entity, mismatched or unclosed tags,
/// a second root element, and text that is not UTF-8. AWS SDKs and the CLI send none of
/// those. Line endings are normalised as XML 1.0 requires.
fn read_keys(xml: &[u8]) -> Option<(String, Vec<(String, String)>)> {
    let raw = std::str::from_utf8(xml).ok()?;
    let normalised = raw.replace("\r\n", "\n").replace('\r', "\n");
    let xml = normalised.as_str();
    let mut stack: Vec<String> = Vec::new();
    let mut root: Option<String> = None;
    let mut keys = Vec::new();
    let mut key_text: Option<String> = None;
    let mut i = 0;
    // One declaration, only at offset 0, holding no markup.
    if xml.starts_with("<?xml") && xml[5..].starts_with(|c: char| c.is_ascii_whitespace()) {
        let close = xml.find("?>")?;
        if xml[5..close].contains('<') || !declaration_is_readable(&xml[5..close]) {
            return None;
        }
        i = close + 2;
    }
    while i < xml.len() {
        let rest = &xml[i..];
        if let Some(comment) = rest.strip_prefix("<!--") {
            // A comment inside a key would leave the key text to a reading of its own.
            if key_text.is_some() {
                return None;
            }
            let end = comment.find("-->")?;
            // `--` may not appear inside a comment, which also rules out `--->`.
            if comment[..end].contains("--") || comment[..end].ends_with('-') {
                return None;
            }
            i += 4 + end + 3;
        } else if rest.starts_with("<?") || rest.starts_with("<!") {
            // A processing instruction other than the declaration, a DOCTYPE or CDATA.
            return None;
        } else if rest.starts_with("</") {
            let end = tag_end(xml, i)?;
            let name = local_name(xml[i + 2..end].trim());
            if stack.pop().as_deref() != Some(name) {
                return None;
            }
            if name == "Key" {
                let text = key_text.take()?;
                keys.push((stack.last().cloned().unwrap_or_default(), text));
            }
            i = end + 1;
        } else if rest.starts_with('<') {
            let end = tag_end(xml, i)?;
            let inner = &xml[i + 1..end];
            let name = local_name(inner).to_string();
            if name.is_empty() || key_text.is_some() {
                return None;
            }
            if inner.ends_with('/') {
                if stack.is_empty() && root.is_some() {
                    return None; // a second root element
                }
                if name == "Key" {
                    keys.push((stack.last().cloned().unwrap_or_default(), String::new()));
                }
                if root.is_none() {
                    root = Some(name);
                }
            } else {
                if root.is_none() {
                    root = Some(name.clone());
                } else if stack.is_empty() {
                    return None; // a second root element
                }
                if name == "Key" {
                    key_text = Some(String::new());
                }
                stack.push(name);
            }
            i = end + 1;
        } else {
            let next = rest.find('<').map(|p| i + p).unwrap_or(xml.len());
            if let Some(text) = key_text.as_mut() {
                unescape_into(&xml[i..next], text)?;
            }
            i = next;
        }
    }
    if !stack.is_empty() {
        return None;
    }
    Some((root?, keys))
}

/// The object keys a DeleteObjects request body names, or `None` when it cannot be read.
pub(crate) fn parse_delete_request(xml: &[u8]) -> Option<Vec<String>> {
    let (root, keys) = read_keys(xml)?;
    if root != "Delete" {
        return None;
    }
    Some(
        keys.into_iter()
            .filter(|(parent, _)| parent == "Object")
            .map(|(_, key)| key)
            .collect(),
    )
}

/// Why a DeleteObjects request body the reader refused was refused, for the one warning
/// logged per refusal. A classification of the raw text for the log only; it never decides
/// whether a body is refused.
pub(crate) fn refusal_cause(xml: &[u8]) -> &'static str {
    let Ok(text) = std::str::from_utf8(xml) else {
        return "text that is not UTF-8";
    };
    if text.starts_with("<?xml")
        && text[5..].starts_with(|c: char| c.is_ascii_whitespace())
        && text
            .find("?>")
            .is_some_and(|close| !declaration_is_readable(&text[5..close]))
    {
        return "an XML declaration other than version 1.0 in UTF-8";
    }
    let after_declaration = if text.starts_with("<?xml") {
        text.find("?>").map_or(text, |close| &text[close + 2..])
    } else {
        text
    };
    if text.contains("<!DOCTYPE") {
        "a DOCTYPE"
    } else if text.contains("<![CDATA[") {
        "a CDATA section"
    } else if after_declaration.contains("<?") {
        "a processing instruction other than a leading XML declaration"
    } else if text
        .rfind("<!--")
        .is_some_and(|c| !text[c..].contains("-->"))
    {
        "an unterminated comment"
    } else if !text.contains("Key>") {
        "no Key element visible"
    } else {
        "malformed or unreadable XML"
    }
}

/// What a DeleteObjects result document reports.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct DeleteResultKeys {
    /// `<Deleted><Key>`: listed only in non-quiet mode.
    pub(crate) deleted: Vec<String>,
    /// `<Error><Key>`: listed in both modes. S3 did not delete these.
    pub(crate) errors: Vec<String>,
}

/// The keys a DeleteObjects result document reports, or `None` when it is not a
/// `DeleteResult` this reader understands (including a whole-request `<Error>` document).
pub(crate) fn parse_delete_result(xml: &[u8]) -> Option<DeleteResultKeys> {
    let (root, keys) = read_keys(xml)?;
    if root != "DeleteResult" {
        return None;
    }
    let mut result = DeleteResultKeys::default();
    for (parent, key) in keys {
        match parent.as_str() {
            "Deleted" => result.deleted.push(key),
            "Error" => result.errors.push(key),
            _ => {}
        }
    }
    Some(result)
}

/// The keys a DeleteObjects exchange may have deleted, given what could be read.
///
/// * A `2xx` with a readable result: every requested key, less a key S3 reported an
///   `<Error>` for on every request entry naming it (one entry per `VersionId`: an error
///   for one version leaves the key's other entries deleted), plus every key it reported
///   `<Deleted>`. Quiet mode lists no deleted keys, which is why the request's list is
///   the base.
/// * A `2xx` with an unreadable result, a `5xx`, or no response at all: every requested
///   key, since S3 may have deleted any of them.
/// * Any other status: none. S3 rejected the request before deleting.
pub(crate) fn keys_to_invalidate(
    status: Option<hyper::StatusCode>,
    requested: Option<&[String]>,
    result: Option<&DeleteResultKeys>,
) -> Vec<String> {
    let requested = requested.unwrap_or(&[]);
    let mut out: Vec<String> = match status {
        Some(s) if s.is_success() => match result {
            Some(result) => {
                let mut entries: HashMap<&str, usize> = HashMap::new();
                for k in requested {
                    *entries.entry(k.as_str()).or_default() += 1;
                }
                let mut errors: HashMap<&str, usize> = HashMap::new();
                for k in &result.errors {
                    *errors.entry(k.as_str()).or_default() += 1;
                }
                let refused = |k: &str| errors.get(k).copied().unwrap_or(0) >= entries[k];
                requested
                    .iter()
                    .filter(|k| !refused(k.as_str()))
                    .chain(result.deleted.iter())
                    .cloned()
                    .collect()
            }
            None => requested.to_vec(),
        },
        Some(s) if s.is_server_error() => requested.to_vec(),
        None => requested.to_vec(),
        Some(_) => Vec::new(),
    };
    let mut seen = HashSet::new();
    out.retain(|k| seen.insert(k.clone()));
    out
}

// ── Form POST ──────────────────────────────────────────────────────────────

/// The cache key of the object a form POST response names, for when the form fields
/// could not be read. S3 names the stored object in its `Location` header: the object's
/// URL on a `2xx`, and on a `303` the `success_action_redirect` URL with `bucket`, `key`
/// and `etag` query parameters added. `None` when neither names an object in the bucket
/// behind `bucket_prefix`.
///
/// Amazon S3 form-encodes the key in both places: `/` as `%2F`, a space as `+`, a `+` as
/// `%2B`, `%` as `%25` and non-ASCII as UTF-8 `%XX` (key `dir/a b` gives
/// `.../bucket/dir%2Fa+b` on a `2xx` and `key=dir%2Fa+b` on a `303`). The `2xx` path is
/// read with S3's request-path rules, under which `+` is also a space and `%2F` a `/`, so
/// both branches decode to the stored key.
pub(crate) fn form_post_response_cache_key(
    status: hyper::StatusCode,
    location: Option<&str>,
    bucket_prefix: &str,
) -> Option<String> {
    let uri: hyper::Uri = location?.parse().ok()?;
    if status.is_redirection() {
        let key = uri.query()?.split('&').find_map(|pair| {
            let (name, value) = pair.split_once('=')?;
            (name == "key").then_some(value)
        })?;
        let key = form_decode(key)?;
        return (!key.is_empty()).then(|| canonical_cache_key(bucket_prefix, &key));
    }
    let host = uri.authority()?.as_str();
    let key = crate::cache::CacheManager::generate_cache_key(uri.path(), Some(host));
    let in_bucket = key
        .strip_prefix(bucket_prefix)
        .is_some_and(|rest| rest.len() > 1 && rest.starts_with('/'));
    in_bucket.then_some(key)
}

/// `application/x-www-form-urlencoded` decoding of one value.
fn form_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

/// The `boundary` parameter of a `multipart/form-data` content type.
pub(crate) fn multipart_boundary(content_type: &str) -> Option<String> {
    let mut params = content_type.split(';');
    if !params
        .next()?
        .trim()
        .eq_ignore_ascii_case("multipart/form-data")
    {
        return None;
    }
    for param in params {
        let (name, value) = param.split_once('=')?;
        if name.trim().eq_ignore_ascii_case("boundary") {
            let value = value.trim().trim_matches('"');
            if value.is_empty() || value.len() > 200 {
                return None;
            }
            return Some(value.to_string());
        }
    }
    None
}

/// `name` and `filename` candidates from a part's `Content-Disposition` header.
fn disposition(headers: &str) -> (Option<String>, Vec<String>) {
    let mut name = None;
    let mut filenames = Vec::new();
    for line in headers.split("\r\n") {
        let Some((header, value)) = line.split_once(':') else {
            continue;
        };
        if !header.trim().eq_ignore_ascii_case("content-disposition") {
            continue;
        }
        let mut rest = value;
        while let Some(semi) = rest.find(';') {
            rest = &rest[semi + 1..];
            let Some((param, tail)) = rest.split_once('=') else {
                break;
            };
            let param = param.trim().to_ascii_lowercase();
            let tail = tail.trim_start();
            let (raw, consumed) = if let Some(quoted) = tail.strip_prefix('"') {
                // A quoted value ends at the first unescaped quote.
                let bytes = quoted.as_bytes();
                let mut end = None;
                let mut j = 0;
                while j < bytes.len() {
                    if bytes[j] == b'\\' {
                        j += 2;
                        continue;
                    }
                    if bytes[j] == b'"' {
                        end = Some(j);
                        break;
                    }
                    j += 1;
                }
                let end = end.unwrap_or(bytes.len());
                (&quoted[..end], (end + 2).min(tail.len()))
            } else {
                let end = tail.find(';').unwrap_or(tail.len());
                (tail[..end].trim(), end)
            };
            match param.as_str() {
                "name" => name = Some(raw.to_string()),
                "filename" => {
                    filenames.push(raw.to_string());
                    // Backslash-escaped quoting, and the HTML form encoding of `"`, CR, LF.
                    filenames.push(raw.replace("\\\"", "\"").replace("\\\\", "\\"));
                    filenames.push(
                        raw.replace("%22", "\"")
                            .replace("%0D", "\r")
                            .replace("%0A", "\n"),
                    );
                }
                "filename*" => {
                    if let Some((_, encoded)) = raw.split_once("''") {
                        if let Ok(decoded) =
                            percent_encoding::percent_decode_str(encoded).decode_utf8()
                        {
                            filenames.push(decoded.into_owned());
                        }
                    }
                }
                _ => {}
            }
            rest = &tail[consumed.min(tail.len())..];
        }
    }
    let mut seen = HashSet::new();
    filenames.retain(|f| seen.insert(f.clone()));
    (name, filenames)
}

/// The object keys a form POST's `key` field can resolve to. S3 replaces `${filename}`
/// with the uploaded file's name, keeping only the text after the last `/` or `\`; the
/// unstripped name is included too, so a client that sent a path is covered either way.
pub(crate) fn resolve_form_keys(key_fields: &[String], filenames: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for key in key_fields {
        if key.contains("${filename}") {
            for filename in filenames {
                let base = filename
                    .rsplit(['/', '\\'])
                    .next()
                    .unwrap_or(filename.as_str());
                out.push(key.replace("${filename}", base));
                out.push(key.replace("${filename}", filename));
            }
        } else {
            out.push(key.clone());
        }
    }
    let mut seen = HashSet::new();
    out.retain(|k| seen.insert(k.clone()));
    out
}

enum Phase {
    /// Looking for the first delimiter, from `search_from`.
    Preamble { search_from: usize },
    /// Just after a delimiter at `at`.
    AfterDelimiter { at: usize },
    /// In a part's headers starting at `start`.
    Headers { start: usize, search_from: usize },
    /// In a non-file part's value starting at `start`.
    Value {
        is_key: bool,
        start: usize,
        search_from: usize,
    },
}

/// What a [`FormFieldScanner`] established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FormScan {
    /// The `key` field values seen before the file part, and the file part's filename
    /// candidates (empty when no file part was reached).
    Fields {
        keys: Vec<String>,
        filenames: Vec<String>,
    },
    /// The fields could not be read: the prefix before the file part exceeded the bound
    /// or the in-flight ledger, the body ended first, or a value was not UTF-8.
    Unknown,
}

/// Reads a form POST's fields from the body as it streams past, stopping at the `file`
/// part, which S3 requires to be last. Only the bytes before the file's content are
/// retained, at most `bound` of them, reserved against the in-flight ledger.
pub(crate) struct FormFieldScanner {
    delimiter: Vec<u8>,
    buf: Vec<u8>,
    bound: usize,
    reservation: Option<Reservation>,
    phase: Phase,
    keys: Vec<String>,
    filenames: Vec<String>,
    done: Option<FormScan>,
}

fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if haystack.len() < needle.len() || from > haystack.len() - needle.len() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

impl FormFieldScanner {
    pub(crate) fn new(boundary: &str, bound: usize, ledger: &Arc<InflightLedger>) -> Self {
        let mut delimiter = b"--".to_vec();
        delimiter.extend_from_slice(boundary.as_bytes());
        Self {
            delimiter,
            buf: Vec::new(),
            bound,
            reservation: ledger.try_reserve(0),
            phase: Phase::Preamble { search_from: 0 },
            keys: Vec::new(),
            filenames: Vec::new(),
            done: None,
        }
    }

    fn fail(&mut self) {
        self.done = Some(FormScan::Unknown);
        self.buf = Vec::new();
        self.reservation = None;
    }

    fn finish(&mut self) {
        self.done = Some(FormScan::Fields {
            keys: std::mem::take(&mut self.keys),
            filenames: std::mem::take(&mut self.filenames),
        });
        self.buf = Vec::new();
        self.reservation = None;
    }

    /// Feed the next body bytes.
    pub(crate) fn feed(&mut self, data: &[u8]) {
        if self.done.is_some() {
            return;
        }
        if self.buf.len() + data.len() > self.bound {
            self.fail();
            return;
        }
        let grown = match self.reservation.as_mut() {
            Some(r) => {
                let needed = (self.buf.len() + data.len()) as u64;
                needed <= r.held_bytes() || r.try_grow(needed - r.held_bytes())
            }
            None => false,
        };
        if !grown {
            self.fail();
            return;
        }
        self.buf.extend_from_slice(data);
        self.advance();
    }

    fn advance(&mut self) {
        loop {
            let delim_len = self.delimiter.len();
            match self.phase {
                Phase::Preamble { search_from } => {
                    match find(&self.buf, &self.delimiter, search_from) {
                        Some(pos) => {
                            self.phase = Phase::AfterDelimiter {
                                at: pos + delim_len,
                            }
                        }
                        None => {
                            self.phase = Phase::Preamble {
                                search_from: self.buf.len().saturating_sub(delim_len),
                            };
                            return;
                        }
                    }
                }
                Phase::AfterDelimiter { at } => {
                    if self.buf.len() < at + 2 {
                        return;
                    }
                    if &self.buf[at..at + 2] == b"--" {
                        // The closing delimiter with no file part.
                        self.finish();
                        return;
                    }
                    // Transport padding up to the line's CRLF.
                    match find(&self.buf, b"\r\n", at) {
                        Some(crlf) => {
                            self.phase = Phase::Headers {
                                start: crlf + 2,
                                search_from: crlf + 2,
                            }
                        }
                        None => return,
                    }
                }
                Phase::Headers { start, search_from } => {
                    match find(&self.buf, b"\r\n\r\n", search_from) {
                        Some(end) => {
                            let Ok(headers) = std::str::from_utf8(&self.buf[start..end]) else {
                                self.fail();
                                return;
                            };
                            let (name, filenames) = disposition(headers);
                            let name = name.unwrap_or_default();
                            if name.eq_ignore_ascii_case("file") {
                                self.filenames = filenames;
                                self.finish();
                                return;
                            }
                            self.phase = Phase::Value {
                                is_key: name.eq_ignore_ascii_case("key"),
                                start: end + 4,
                                search_from: end + 4,
                            };
                        }
                        None => {
                            self.phase = Phase::Headers {
                                start,
                                search_from: self.buf.len().saturating_sub(3).max(start),
                            };
                            return;
                        }
                    }
                }
                Phase::Value {
                    is_key,
                    start,
                    search_from,
                } => {
                    let mut needle = b"\r\n".to_vec();
                    needle.extend_from_slice(&self.delimiter);
                    match find(&self.buf, &needle, search_from) {
                        Some(end) => {
                            if is_key {
                                match std::str::from_utf8(&self.buf[start..end]) {
                                    Ok(value) => self.keys.push(value.to_string()),
                                    Err(_) => {
                                        self.fail();
                                        return;
                                    }
                                }
                            }
                            self.phase = Phase::AfterDelimiter {
                                at: end + needle.len(),
                            };
                        }
                        None => {
                            self.phase = Phase::Value {
                                is_key,
                                start,
                                search_from: self.buf.len().saturating_sub(needle.len()).max(start),
                            };
                            return;
                        }
                    }
                }
            }
        }
    }

    /// What was established. A body that ended before the file part is `Unknown`.
    pub(crate) fn outcome(&self) -> FormScan {
        self.done.clone().unwrap_or(FormScan::Unknown)
    }
}

/// A request body that feeds every data frame to a [`FormFieldScanner`] as it is
/// forwarded, without retaining it.
pub(crate) struct ScanningBody<B> {
    inner: B,
    scanner: Arc<Mutex<FormFieldScanner>>,
}

impl<B> ScanningBody<B> {
    pub(crate) fn new(inner: B, scanner: Arc<Mutex<FormFieldScanner>>) -> Self {
        Self { inner, scanner }
    }
}

impl<B> Body for ScanningBody<B>
where
    B: Body<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &polled {
            if let Some(data) = frame.data_ref() {
                if let Ok(mut scanner) = self.scanner.lock() {
                    scanner.feed(data);
                }
            }
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_cache_key_is_what_every_spelling_of_a_get_produces() {
        let key = canonical_cache_key("bkt", "dir/a b+c!(1)\u{fc}%.txt");
        assert_eq!(key, "bkt/dir/a%20b%2Bc%21%281%29%C3%BC%25.txt");
        for path in [
            "/bkt/dir/a%20b%2Bc%21%281%29%C3%BC%25.txt",
            "/bkt/dir/a+b%2Bc!(1)%c3%bc%25.txt",
            "/bkt/dir%2Fa%20b%2bc%21(1)%C3%BC%25.txt",
        ] {
            assert_eq!(
                crate::cache::CacheManager::generate_cache_key(path, Some("s3.amazonaws.com")),
                key,
                "{}",
                path
            );
        }
    }

    #[test]
    fn bucket_prefix_for_path_style_and_virtual_hosted() {
        assert_eq!(
            bucket_cache_prefix("/bkt", "s3.us-west-2.amazonaws.com").as_deref(),
            Some("bkt")
        );
        assert_eq!(
            bucket_cache_prefix("/bkt/", "s3.us-west-2.amazonaws.com").as_deref(),
            Some("bkt")
        );
        assert_eq!(
            bucket_cache_prefix("/", "bkt.s3.us-west-2.amazonaws.com").as_deref(),
            Some("bkt")
        );
        assert_eq!(bucket_cache_prefix("/bkt/key", "s3.amazonaws.com"), None);
        assert_eq!(bucket_cache_prefix("/", "s3.amazonaws.com"), None);
        // The candidate keys match what a GET produces in both addressing styles.
        let key = &canonical_cache_key("bkt", "a b");
        assert_eq!(
            key,
            &crate::cache::CacheManager::generate_cache_key(
                "/a%20b",
                Some("bkt.s3.us-west-2.amazonaws.com")
            )
        );
        assert_eq!(
            key,
            &crate::cache::CacheManager::generate_cache_key(
                "/bkt/a%20b",
                Some("s3.us-west-2.amazonaws.com")
            )
        );
    }

    #[test]
    fn delete_query_detection() {
        assert!(is_delete_objects_query(Some("delete")));
        assert!(is_delete_objects_query(Some("delete=")));
        assert!(is_delete_objects_query(Some("x-id=DeleteObjects&delete")));
        assert!(!is_delete_objects_query(Some("deleted")));
        assert!(!is_delete_objects_query(Some("uploads")));
        assert!(!is_delete_objects_query(None));
    }

    #[test]
    fn delete_request_keys_are_decoded() {
        let xml = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Quiet>true</Quiet>\
            <Object><Key>a &amp; b&lt;c&gt;&#233;&#x41;</Key><VersionId>v1</VersionId></Object>\
            <Object><Key>line\r\nbreak</Key></Object></Delete>";
        assert_eq!(
            parse_delete_request(xml).unwrap(),
            vec!["a & b<c>\u{e9}A".to_string(), "line\nbreak".to_string()]
        );
    }

    #[test]
    fn unreadable_delete_requests_are_none() {
        for xml in [
            &b"<Delete><Object><Key>a</Object></Key></Delete>"[..],
            b"<Delete><Object><Key>a&bogus;</Key></Object></Delete>",
            b"<!DOCTYPE x><Delete/>",
            b"<Delete><!-- c -- d --></Delete>",
            b"<Delete><Object><Key>a</Key></Object>",
            b"<Other><Object><Key>a</Key></Object></Other>",
            b"\xff\xfe",
        ] {
            assert_eq!(parse_delete_request(xml), None, "{:?}", xml);
        }
    }

    #[test]
    fn the_one_reader_accepts_a_declaration_and_comments_between_elements() {
        assert_eq!(
            parse_delete_request(
                b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!-- a --><Delete><Quiet>true</Quiet><!-- c --><Object><Key>a &amp; b</Key></Object>\
                  <s3:Object><s3:Key>c&lt;d</s3:Key></s3:Object><!--x--></Delete><!-- tail -->"
            ),
            Some(vec!["a & b".to_string(), "c<d".to_string()])
        );
        // A comment may hold markup-looking text, quotes and `<?`.
        assert_eq!(
            parse_delete_request(
                b"<Delete><!-- <Key>hidden</Key> <?p \" --><Object><Key>k</Key></Object></Delete>"
            ),
            Some(vec!["k".to_string()])
        );
        // A key named twice (two VersionIds) is listed twice.
        assert_eq!(
            parse_delete_request(
                b"<Delete><Object><Key>k</Key><VersionId>1</VersionId></Object>\
                  <Object><Key>k</Key><VersionId>2</VersionId></Object></Delete>"
            ),
            Some(vec!["k".to_string(), "k".to_string()])
        );
    }

    #[test]
    fn the_declaration_must_be_version_1_0_in_utf_8() {
        let body = |decl: &str| {
            format!("{decl}<Delete><Object><Key>k\u{e9}</Key></Object></Delete>").into_bytes()
        };
        let want = Some(vec!["k\u{e9}".to_string()]);
        for decl in [
            "",
            "<?xml version=\"1.0\"?>",
            "<?xml version='1.0'?>",
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>",
            "<?xml version=\"1.0\" standalone=\"no\"?>",
        ] {
            assert_eq!(parse_delete_request(&body(decl)), want, "{}", decl);
        }
        for decl in [
            "<?xml version=\"1.1\"?>",
            "<?xml version=\"1.0\" encoding=\"UTF-16\"?>",
            "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>",
            "<?xml encoding=\"UTF-8\"?>",
            "<?xml ?>",
            "<?xml version=\"1.0\" bogus=\"x\"?>",
            "<?xml encoding=\"UTF-8\" version=\"1.0\"?>",
        ] {
            assert_eq!(parse_delete_request(&body(decl)), None, "{}", decl);
        }
        assert_eq!(
            refusal_cause(&body("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>")),
            "an XML declaration other than version 1.0 in UTF-8"
        );
    }

    #[test]
    fn a_self_closing_second_root_is_refused() {
        assert_eq!(
            parse_delete_request(b"<Delete><Object><Key>k</Key></Object></Delete><X/>"),
            None
        );
        assert_eq!(
            parse_delete_request(b"<Delete/><Delete><Object><Key>k</Key></Object></Delete>"),
            None
        );
        // A self-closing element inside the root is still fine.
        assert_eq!(
            parse_delete_request(b"<Delete><Quiet/><Object><Key>k</Key></Object></Delete>"),
            Some(vec!["k".to_string()])
        );
    }

    #[test]
    fn the_one_reader_refuses_what_it_does_not_fully_understand() {
        let bodies: [(&str, &[u8]); 16] = [
            ("DOCTYPE", b"<!DOCTYPE d><Delete><Object><Key>e</Key></Object></Delete>"),
            (
                "the DOCTYPE literal",
                b"<!DOCTYPE Delete SYSTEM \"<!--\"><Delete><Object><?p '?><Key>ab</Key></Object><Object><?q '?><Key>cd</Key></Object><!-- x -->\"></Delete>",
            ),
            (
                "CDATA key",
                b"<Delete><Object><Key><![CDATA[a</Key>b]]></Key></Object></Delete>",
            ),
            (
                "PI inside a key",
                b"<Delete><Object><Key>a<?p?>b</Key></Object></Delete>",
            ),
            (
                "PI between Objects",
                b"<Delete><Object><Key>a</Key></Object><?p?><Object><Key>b</Key></Object></Delete>",
            ),
            (
                "declaration plus a second PI",
                b"<?xml version=\"1.0\"?><?p \"?><Delete><Object><Key>k</Key></Object></Delete>",
            ),
            (
                "two PIs whose quotes pair",
                b"<Delete><Object><Key>z</Key></Object><Object><?p \"?><Key>ab</Key></Object><Object><?q \"?><Key>cd</Key></Object></Delete>",
            ),
            (
                "declaration not at offset 0",
                b" <?xml version=\"1.0\"?><Delete><Object><Key>k</Key></Object></Delete>",
            ),
            (
                "declaration that never closes",
                b"<?xml version=\"1.0\"<Delete><Object><Key>k</Key></Object></Delete>",
            ),
            (
                "unknown entity",
                b"<Delete><Object><Key>e&custom;</Key></Object></Delete>",
            ),
            (
                "double hyphen in a comment",
                b"<Delete><!-- a -- b --><Object><Key>k</Key></Object></Delete>",
            ),
            (
                "comment ending in three hyphens",
                b"<Delete><!-- a ---><Object><Key>k</Key></Object></Delete>",
            ),
            (
                "unterminated comment",
                b"<Delete><Object><Key>z</Key></Object><!-- never closed <Object><Key>ab</Key></Object></Delete>",
            ),
            (
                "comment inside a key",
                b"<Delete><Object><Key>x<!--y-->z</Key></Object></Delete>",
            ),
            (
                "tag that never ends",
                b"<Delete><Object><Key>z</Key></Object><Object><Key>ab</Key></Object><Obj",
            ),
            ("not UTF-8", b"\xff\xfe"),
        ];
        for (what, body) in bodies {
            assert_eq!(parse_delete_request(body), None, "{}", what);
        }
    }

    #[test]
    fn refusal_cause_names_the_construct() {
        assert_eq!(refusal_cause(b"<!DOCTYPE d><Delete/>"), "a DOCTYPE");
        assert_eq!(
            refusal_cause(b"<Delete><Key><![CDATA[x]]></Key></Delete>"),
            "a CDATA section"
        );
        assert_eq!(
            refusal_cause(b"<?xml version=\"1.0\"?><Delete><?p?></Delete>"),
            "a processing instruction other than a leading XML declaration"
        );
        assert_eq!(
            refusal_cause(b"<Delete><Key>k</Key><!-- open"),
            "an unterminated comment"
        );
        assert_eq!(refusal_cause(b"<Delete/>"), "no Key element visible");
        assert_eq!(
            refusal_cause(b"<Delete><Key>k</Key></Delete"),
            "malformed or unreadable XML"
        );
        assert_eq!(refusal_cause(b"\xff"), "text that is not UTF-8");
    }

    #[test]
    fn a_version_error_does_not_exempt_the_keys_other_entries() {
        use hyper::StatusCode;
        // `k` named twice (two VersionIds), one refused: still invalidated.
        let requested = vec!["k".to_string(), "k".to_string(), "j".to_string()];
        let result = DeleteResultKeys {
            deleted: vec![],
            errors: vec!["k".into(), "j".into()],
        };
        assert_eq!(
            keys_to_invalidate(Some(StatusCode::OK), Some(&requested), Some(&result)),
            vec!["k"]
        );
        // Both entries refused: exempt.
        let result = DeleteResultKeys {
            deleted: vec![],
            errors: vec!["k".into(), "k".into()],
        };
        assert_eq!(
            keys_to_invalidate(Some(StatusCode::OK), Some(&requested), Some(&result)),
            vec!["j"]
        );
    }

    #[test]
    fn form_post_response_names_its_object() {
        use hyper::StatusCode;
        assert_eq!(
            form_post_response_cache_key(
                StatusCode::NO_CONTENT,
                Some("https://bkt.s3.us-west-2.amazonaws.com/up/a%20b.txt"),
                "bkt"
            )
            .as_deref(),
            Some("bkt/up/a%20b.txt")
        );
        assert_eq!(
            form_post_response_cache_key(
                StatusCode::CREATED,
                Some("http://127.0.0.1:9000/bkt/up/a+b.txt"),
                "bkt"
            )
            .as_deref(),
            Some("bkt/up/a%20b.txt")
        );
        assert_eq!(
            form_post_response_cache_key(
                StatusCode::SEE_OTHER,
                Some("https://example.com/done?bucket=bkt&key=up%2Fa+b%2B.txt&etag=%22e%22"),
                "bkt"
            )
            .as_deref(),
            Some("bkt/up/a%20b%2B.txt")
        );
        assert_eq!(
            form_post_response_cache_key(
                StatusCode::NO_CONTENT,
                Some("https://other.s3.amazonaws.com/k"),
                "bkt"
            ),
            None
        );
        assert_eq!(
            form_post_response_cache_key(StatusCode::NO_CONTENT, None, "bkt"),
            None
        );
    }

    /// The `Location` values Amazon S3 returned for form POST uploads to
    /// a us-west-2 test bucket (2026-10-08, bucket name replaced by `amzn-s3-demo-bucket`) of the keys `a+b`, `a b`, `a%b` and
    /// `ü` under `-formpost-probe/`: default `204`, path-style `204`, and `303` to a
    /// `success_action_redirect`.
    #[test]
    fn form_post_location_as_amazon_s3_spells_it() {
        use hyper::StatusCode;
        let bucket = "amzn-s3-demo-bucket";
        let vhost = "https://amzn-s3-demo-bucket.s3.dualstack.us-west-2.amazonaws.com";
        let path = "https://s3.dualstack.us-west-2.amazonaws.com/amzn-s3-demo-bucket";
        for (spelled, want) in [
            ("case2-formpost-probe%2Fa%2Bb", "a%2Bb"),
            ("case2-formpost-probe%2Fa+b", "a%20b"),
            ("case2-formpost-probe%2Fa%25b", "a%25b"),
            ("case2-formpost-probe%2F%C3%BC", "%C3%BC"),
        ] {
            let want = format!("{}/case2-formpost-probe/{}", bucket, want);
            for location in [
                format!("{}/{}", vhost, spelled),
                format!("{}/{}", path, spelled),
            ] {
                assert_eq!(
                    form_post_response_cache_key(StatusCode::NO_CONTENT, Some(&location), bucket)
                        .as_deref(),
                    Some(want.as_str()),
                    "{}",
                    location
                );
            }
            let redirect = format!(
                "https://example.com/done?bucket=amzn-s3-demo-bucket&key={}&etag=%228c3b777d4d7b71ddec00af6748933292%22",
                spelled
            );
            assert_eq!(
                form_post_response_cache_key(StatusCode::SEE_OTHER, Some(&redirect), bucket)
                    .as_deref(),
                Some(want.as_str()),
                "{}",
                redirect
            );
        }
    }

    #[test]
    fn delete_result_errors_and_deleted() {
        let xml = b"<DeleteResult><Deleted><Key>a</Key></Deleted><Error><Key>b</Key><Code>AccessDenied</Code></Error></DeleteResult>";
        assert_eq!(
            parse_delete_result(xml).unwrap(),
            DeleteResultKeys {
                deleted: vec!["a".into()],
                errors: vec!["b".into()]
            }
        );
        assert_eq!(
            parse_delete_result(b"<Error><Code>InternalError</Code></Error>"),
            None
        );
    }

    #[test]
    fn keys_to_invalidate_by_status_and_result() {
        use hyper::StatusCode;
        let requested = vec!["a".to_string(), "b".to_string()];
        let result = DeleteResultKeys {
            deleted: vec![],
            errors: vec!["b".into()],
        };
        assert_eq!(
            keys_to_invalidate(Some(StatusCode::OK), Some(&requested), Some(&result)),
            vec!["a"]
        );
        assert_eq!(
            keys_to_invalidate(Some(StatusCode::OK), Some(&requested), None),
            requested
        );
        assert_eq!(
            keys_to_invalidate(
                Some(StatusCode::INTERNAL_SERVER_ERROR),
                Some(&requested),
                None
            ),
            requested
        );
        assert_eq!(keys_to_invalidate(None, Some(&requested), None), requested);
        assert!(keys_to_invalidate(Some(StatusCode::FORBIDDEN), Some(&requested), None).is_empty());
        let deleted_only = DeleteResultKeys {
            deleted: vec!["z".into()],
            errors: vec![],
        };
        assert_eq!(
            keys_to_invalidate(Some(StatusCode::OK), None, Some(&deleted_only)),
            vec!["z"]
        );
    }

    fn form_body(boundary: &str, fields: &[(&str, &str)], filename: &str) -> Vec<u8> {
        let mut body = String::new();
        for (name, value) in fields {
            body.push_str(&format!(
                "--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n{}\r\n",
                boundary, name, value
            ));
        }
        body.push_str(&format!(
            "--{}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{}\"\r\n\r\nFILEBYTES\r\n--{}--\r\n",
            boundary, filename, boundary
        ));
        body.into_bytes()
    }

    fn scan(body: &[u8], chunk: usize, bound: usize) -> FormScan {
        let ledger = Arc::new(InflightLedger::new(1 << 30));
        let mut scanner = FormFieldScanner::new("XyZ", bound, &ledger);
        for piece in body.chunks(chunk) {
            scanner.feed(piece);
        }
        scanner.outcome()
    }

    #[test]
    fn scanner_reads_key_and_filename_in_any_chunking() {
        let body = form_body(
            "XyZ",
            &[
                ("acl", "private"),
                ("Key", "up/${filename}"),
                ("policy", "p\r\nq"),
            ],
            "C:\\dir\\photo 1.jpg",
        );
        for chunk in [1, 2, 3, 7, 64, body.len()] {
            match scan(&body, chunk, 1 << 20) {
                FormScan::Fields { keys, filenames } => {
                    assert_eq!(keys, vec!["up/${filename}"]);
                    let resolved = resolve_form_keys(&keys, &filenames);
                    assert!(
                        resolved.contains(&"up/photo 1.jpg".to_string()),
                        "{:?}",
                        resolved
                    );
                }
                other => panic!("chunk {}: {:?}", chunk, other),
            }
        }
    }

    #[test]
    fn scanner_stops_retaining_at_the_file_part() {
        let mut body = form_body("XyZ", &[("key", "k")], "f");
        body.extend(std::iter::repeat_n(b'x', 4 << 20));
        assert_eq!(
            scan(&body, 4096, 64 * 1024),
            FormScan::Fields {
                keys: vec!["k".into()],
                filenames: vec!["f".into()]
            }
        );
    }

    #[test]
    fn scanner_unknown_when_fields_exceed_the_bound_or_body_ends() {
        let big = "v".repeat(70 * 1024);
        let body = form_body("XyZ", &[("policy", &big), ("key", "k")], "f");
        assert_eq!(scan(&body, 4096, 64 * 1024), FormScan::Unknown);
        let truncated = &form_body("XyZ", &[("key", "k")], "f")[..20];
        assert_eq!(scan(truncated, 4, 1 << 20), FormScan::Unknown);
    }

    #[test]
    fn boundary_parsing() {
        assert_eq!(
            multipart_boundary("multipart/form-data; boundary=----abc").as_deref(),
            Some("----abc")
        );
        assert_eq!(
            multipart_boundary("Multipart/Form-Data; charset=utf-8; boundary=\"q q\"").as_deref(),
            Some("q q")
        );
        assert_eq!(multipart_boundary("application/xml"), None);
        assert_eq!(multipart_boundary("multipart/form-data"), None);
    }

    #[test]
    fn filename_variants() {
        let (name, filenames) = disposition(
            "Content-Disposition: form-data; name=\"file\"; filename=\"a%22b.txt\"; filename*=UTF-8''%C3%A9.txt",
        );
        assert_eq!(name.as_deref(), Some("file"));
        assert!(filenames.contains(&"a%22b.txt".to_string()));
        assert!(filenames.contains(&"a\"b.txt".to_string()));
        assert!(filenames.contains(&"\u{e9}.txt".to_string()));
    }
}
