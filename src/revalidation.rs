//! What a conditional-revalidation response proves about the **cached** entry.
//!
//! Spec: the expired entry revalidation feature Requirement 8.
//!
//! # The defect this exists to remove
//!
//! Both mainline revalidation sites used to branch on the HTTP **status alone**:
//! `304` was taken as proof the cached copy was current, and `200`/`206` as proof
//! it was superseded. Neither reads the `ETag` the response carries.
//!
//! That inference is only valid when the validator sent was the proxy's **own**
//! cached ETag. R4.5 (correctly) stops the proxy clobbering a client's
//! precondition, so when a client sends its own conditional the upstream request
//! is answering the *client's* question, and its status says nothing about the
//! cache. Two measured consequences, both against real S3 (2026-08-31):
//!
//! - `If-None-Match: *` on an existing object returns **304**. Under status-only
//!   inference a superseded entry was treated as current, its bytes served, and
//!   its `expires_at` walked forward — poisoning it for later unconditional
//!   clients.
//! - `If-None-Match: "bogus"` with a `Range` returns **206** on an *unchanged*
//!   object. Under status-only inference that wiped the whole key, giving cache
//!   destruction and egress amplification from ordinary read access.
//! - A client `If-Match` carrying the *cached* ETag succeeds with **200**/`206`,
//!   which under status-only inference also wiped the key — cache destruction
//!   from an entirely legitimate request. Not reported by the scanner; found
//!   while designing this fix, and the most likely of the three to happen by
//!   accident, since the CRT and Rust transfer managers stamp `If-Match` on every
//!   ranged chunk.
//!
//! # The rule
//!
//! Ask what the response proves about the origin's current ETag, not what its
//! status implies:
//!
//! | Response carries | Verdict |
//! |---|---|
//! | an ETag equal to the cached one | [`CacheVerdict::Current`] |
//! | an ETag different from the cached one | [`CacheVerdict::Stale`] |
//! | no ETag, or nothing cached to compare | [`CacheVerdict::Undetermined`] |
//!
//! This is sound for every status that carries a validator, and it needs no
//! knowledge of what the client asked. **Measured (2026-08-31, real S3):** the
//! `ETag` is present on `304` — including the `If-None-Match: *` case — and on
//! `206`, and absent on `412`, which has no representation. RFC 9110 §15.4.5
//! requires a `304` to carry the validators a `200` would have sent, so the
//! useful cases are the specified ones rather than an S3 quirk.
//!
//! [`CacheVerdict::Undetermined`] is the fail-safe arm: make no cache-state
//! decision, refresh nothing, invalidate nothing, and pass the response through.
//! Being unable to tell is not evidence either way, and the previous code's
//! mistake was precisely treating an uninformative response as informative.
//!
//! # What this deliberately does NOT do
//!
//! It does not look at the request. An earlier design gated the whole
//! revalidation-outcome block on "did the client send a conditional?", which is
//! sound but discards information the response demonstrably carries — including
//! the most common conditional case in practice, a client that previously read
//! *through this proxy* and is therefore sending `If-None-Match: <cached ETag>`,
//! which is decisive in both directions. Reading the response keeps that
//! optimisation and is simpler.
//!
//! It also does not compare against an ETag re-read from the cached `.meta` at
//! decision time. `changed_range_revalidation_stale_serve_test.rs` records why
//! that trap matters: `CacheManager::get_object_etag` reads the same `.meta` the
//! lookup came from, so a guard built that way compares a value with itself. The
//! comparison here is stored-metadata ETag against **response** ETag — two
//! independent sources.

use std::collections::HashMap;

/// What a revalidation response proves about the cached entry's currency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheVerdict {
    /// The origin's current ETag equals the cached one. The cached bytes are
    /// current: refresh the TTL and serve them.
    Current,
    /// The origin's current ETag differs from the cached one. The cached bytes
    /// are superseded: invalidate all coverage and refetch.
    Stale,
    /// The response carries no comparable validator. It proves nothing about the
    /// cached entry, so make no cache-state decision at all — do not refresh the
    /// TTL, do not invalidate, and pass the response through.
    Undetermined,
}

/// Strip a weak-validator prefix and surrounding whitespace.
///
/// S3 does not emit weak ETags, so this is not load-bearing today. It is explicit
/// rather than accidentally correct: a `W/"x"` compared byte-wise against `"x"`
/// would read as a version change and trigger a needless invalidation, and the
/// failure would look like a cache-coherency bug rather than a parsing one.
///
/// This function does NOT strip quotes. [`classify`] compares its output through
/// [`etags_equal_ignoring_quotes`], which tolerates one balanced pair of surrounding
/// quotes, because the cache has not always stored the quoted form S3 sends:
/// multipart completions were stored unquoted.
/// `pub(crate)` rather than private so `CacheManager::is_empty_object_etag_entry` can
/// compare against the empty-object ETag through the SAME normalisation this module's
/// verdicts use. Duplicating the trim-and-strip there would let the two drift, and a
/// detector that disagreed with `classify` about what two ETags mean would fire, or fail
/// to fire, for reasons invisible at either call site. Behaviour is unchanged.
pub(crate) fn normalize_etag(raw: &str) -> &str {
    let trimmed = raw.trim();
    trimmed.strip_prefix("W/").unwrap_or(trimmed)
}

/// Compare two ETags as equal when they differ ONLY in one balanced pair of
/// surrounding double quotes.
///
/// Exists because the cache has not always stored the quoted form. Multipart
/// completions were stored with their quotes stripped (`abc-2`) while every
/// other writer, and every S3 response, carries them (`"abc-2"`), so a raw
/// comparison of the two read as a version change and invalidated a correct
/// entry. Entries written that way are still on disk after an upgrade, so the
/// comparison sites tolerate the difference rather than relying on the writer
/// fix alone.
///
/// Deliberately narrow, so it can never make two different validators equal:
///
/// - At most ONE balanced pair is removed, via
///   [`crate::http_proxy::strip_etag_quotes`]. Lopsided quotes are left in place,
///   so `"abc` and `abc` stay different.
/// - Nothing else is normalised. A `W/` prefix is not stripped, so a weak ETag
///   never becomes equal to a strong one here; `W/"x"` begins with `W`, not a
///   quote, and is left intact. Callers that already treat weak and strong as
///   equal (see [`classify`]) strip `W/` themselves first.
/// - An opaque tag that is empty after stripping does not gain a new equality:
///   `""` and the empty string compare exactly as they did byte-wise. An empty
///   validator is not evidence that two entries describe the same object.
pub(crate) fn etags_equal_ignoring_quotes(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let (a_opaque, b_opaque) = (
        crate::http_proxy::strip_etag_quotes(a),
        crate::http_proxy::strip_etag_quotes(b),
    );
    !a_opaque.is_empty() && a_opaque == b_opaque
}

/// Read the origin's current ETag from a response header map.
///
/// `S3Response::headers` preserves the wire casing rather than lowercasing (see
/// `s3_client.rs`, which inserts `key.clone()`), so both spellings are tried —
/// the same defensive pattern already used at the `s3_response.headers` read
/// sites in `http_proxy.rs`. Hyper normalises on receipt in practice; relying on
/// that silently would make a future non-hyper transport break the comparison
/// into an `Undetermined` that looks like a missing header.
pub(crate) fn response_etag(headers: &HashMap<String, String>) -> Option<&str> {
    headers
        .get("etag")
        .or_else(|| headers.get("ETag"))
        .map(|s| s.as_str())
}

/// Classify what `response_headers` proves about a cached entry whose stored
/// validator is `cached_etag`.
///
/// Pure, so the whole rule is testable without a cache, a socket, or a proxy.
pub fn classify(
    response_headers: &HashMap<String, String>,
    cached_etag: Option<&str>,
) -> CacheVerdict {
    let (origin, cached) = match (response_etag(response_headers), cached_etag) {
        (Some(origin), Some(cached)) => (origin, cached),
        // No validator on the response, or nothing stored to compare it against.
        // Either way this exchange is uninformative about the cached entry.
        _ => return CacheVerdict::Undetermined,
    };

    // An empty validator is not a validator. Treating `""` as comparable would
    // make two entries with missing ETags look equal to each other.
    if normalize_etag(origin).is_empty() || normalize_etag(cached).is_empty() {
        return CacheVerdict::Undetermined;
    }

    // Quote-tolerant: an entry stored unquoted (older multipart completions) is
    // the same version as an origin ETag that differs from it only in quoting.
    if etags_equal_ignoring_quotes(normalize_etag(origin), normalize_etag(cached)) {
        CacheVerdict::Current
    } else {
        CacheVerdict::Stale
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn matching_etag_is_current() {
        let h = headers(&[("etag", "\"v1\"")]);
        assert_eq!(classify(&h, Some("\"v1\"")), CacheVerdict::Current);
    }

    #[test]
    fn differing_etag_is_stale() {
        let h = headers(&[("etag", "\"v2\"")]);
        assert_eq!(classify(&h, Some("\"v1\"")), CacheVerdict::Stale);
    }

    /// The S1 shape. `If-None-Match: *` yields a `304` carrying the origin's
    /// CURRENT ETag, which is what makes a superseded entry detectable. Under
    /// status-only inference this exact response was read as "cache is current".
    #[test]
    fn not_modified_carrying_a_newer_etag_is_stale_not_current() {
        let h = headers(&[("etag", "\"v2\"")]);
        assert_eq!(
            classify(&h, Some("\"v1\"")),
            CacheVerdict::Stale,
            "a 304 whose ETag differs from the cached one proves the cache is superseded"
        );
    }

    /// The S2 and S4 shape. A `206` whose ETag MATCHES means the object did not
    /// change, whatever made S3 answer with a body. Status-only inference wiped
    /// the key here.
    #[test]
    fn partial_content_carrying_the_cached_etag_is_current_not_stale() {
        let h = headers(&[("etag", "\"v1\"")]);
        assert_eq!(
            classify(&h, Some("\"v1\"")),
            CacheVerdict::Current,
            "a 206 whose ETag equals the cached one must not be read as a version change"
        );
    }

    /// A `412` carries no representation and therefore no validator.
    #[test]
    fn no_etag_on_the_response_is_undetermined() {
        let h = headers(&[("content-length", "0")]);
        assert_eq!(classify(&h, Some("\"v1\"")), CacheVerdict::Undetermined);
    }

    #[test]
    fn no_cached_etag_is_undetermined() {
        let h = headers(&[("etag", "\"v1\"")]);
        assert_eq!(classify(&h, None), CacheVerdict::Undetermined);
    }

    #[test]
    fn empty_validators_are_undetermined_not_equal() {
        assert_eq!(
            classify(&headers(&[("etag", "")]), Some("")),
            CacheVerdict::Undetermined,
            "two missing validators must not compare equal to each other"
        );
        assert_eq!(
            classify(&headers(&[("etag", "  ")]), Some("\"v1\"")),
            CacheVerdict::Undetermined
        );
    }

    #[test]
    fn wire_casing_of_the_header_name_does_not_change_the_verdict() {
        assert_eq!(
            classify(&headers(&[("ETag", "\"v1\"")]), Some("\"v1\"")),
            CacheVerdict::Current
        );
        assert_eq!(
            classify(&headers(&[("ETag", "\"v2\"")]), Some("\"v1\"")),
            CacheVerdict::Stale
        );
    }

    #[test]
    fn weak_validator_prefix_is_stripped_before_comparing() {
        assert_eq!(
            classify(&headers(&[("etag", "W/\"v1\"")]), Some("\"v1\"")),
            CacheVerdict::Current,
            "a weak/strong spelling difference is not a version change"
        );
    }

    #[test]
    fn surrounding_whitespace_does_not_change_the_verdict() {
        assert_eq!(
            classify(&headers(&[("etag", "  \"v1\"  ")]), Some("\"v1\"")),
            CacheVerdict::Current
        );
    }

    /// Multipart ETags carry a `-<parts>` suffix and must compare byte-wise, so
    /// the same object part count is Current and a differing one is Stale.
    #[test]
    fn multipart_etags_compare_byte_wise() {
        assert_eq!(
            classify(&headers(&[("etag", "\"abc-3\"")]), Some("\"abc-3\"")),
            CacheVerdict::Current
        );
        assert_eq!(
            classify(&headers(&[("etag", "\"abc-4\"")]), Some("\"abc-3\"")),
            CacheVerdict::Stale,
            "a different part count is a different representation"
        );
    }

    /// The multipart quote mismatch. This test used to assert the opposite
    /// (`quoting_difference_is_treated_as_a_version_change`) on the premise that
    /// "the cache stores what S3 sent". It did not: CompleteMultipartUpload entries
    /// were stored with their quotes stripped, so every TTL revalidation of one read
    /// `Stale` against S3's quoted ETag and refetched an unchanged object. The two
    /// ETags below differ ONLY in quoting — both quoted or both bare would pass with
    /// the defect present.
    #[test]
    fn unquoted_stored_multipart_etag_is_current_against_quoted_origin() {
        assert_eq!(
            classify(&headers(&[("etag", "\"abc-2\"")]), Some("abc-2")),
            CacheVerdict::Current,
            "an entry stored unquoted must be Current against the same ETag quoted"
        );
        assert_eq!(
            classify(&headers(&[("etag", "abc-2")]), Some("\"abc-2\"")),
            CacheVerdict::Current,
            "tolerance must be symmetric"
        );
    }

    /// The permissive-direction guard: quote tolerance must never hide a content
    /// difference.
    #[test]
    fn quoting_and_content_both_differing_is_stale() {
        assert_eq!(
            classify(&headers(&[("etag", "\"abc-2\"")]), Some("abd-2")),
            CacheVerdict::Stale
        );
        assert_eq!(
            classify(&headers(&[("etag", "\"abc-2\"")]), Some("abc-3")),
            CacheVerdict::Stale,
            "a different part count is still a different object"
        );
    }

    /// Lopsided quotes are not a balanced pair and are not stripped, so they remain
    /// a difference. Empty-after-stripping validators gain no equality.
    #[test]
    fn quote_tolerance_is_narrow() {
        assert!(!etags_equal_ignoring_quotes("\"abc", "abc"));
        assert!(!etags_equal_ignoring_quotes("abc\"", "abc"));
        assert!(!etags_equal_ignoring_quotes("\"\"", ""));
        assert!(!etags_equal_ignoring_quotes("W/\"abc\"", "\"abc\""));
        assert!(!etags_equal_ignoring_quotes("W/\"abc\"", "abc"));
        assert!(etags_equal_ignoring_quotes("\"abc\"", "abc"));
        assert!(etags_equal_ignoring_quotes("W/\"abc\"", "W/\"abc\""));
        // The empty-validator guard in `classify` is unchanged: a bare empty string
        // on either side is still Undetermined, even against a quoted empty.
        assert_eq!(
            classify(&headers(&[("etag", "\"\"")]), Some("")),
            CacheVerdict::Undetermined
        );
    }
}
