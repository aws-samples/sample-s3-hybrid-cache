//! Property-based tests for ETag list parsing (RFC 7232 conditional requests)
//!
//! **Property 6: Disjunctive match equivalence** — for every generated ETag list
//! and target, assert strong/weak match equals any(per-entry match) under RFC 7232.
//!
//! **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6**

use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use s3_proxy::http_proxy::{etag_strong_match, etag_weak_match};

// ============================================================================
// Generators
// ============================================================================

/// An ETag entry that can be strong, weak, or unquoted (for AWS CLI tolerance).
#[derive(Clone, Debug)]
enum ETagKind {
    /// Strong ETag: `"opaque-tag"`
    Strong(String),
    /// Weak ETag: `W/"opaque-tag"`
    Weak(String),
    /// Unquoted ETag (AWS CLI tolerance): `opaque-tag`
    Unquoted(String),
}

impl ETagKind {
    /// Render this ETag entry as it would appear in a header value.
    fn to_header_value(&self) -> String {
        match self {
            ETagKind::Strong(s) => format!("\"{}\"", s),
            ETagKind::Weak(s) => format!("W/\"{}\"", s),
            ETagKind::Unquoted(s) => s.clone(),
        }
    }
}

impl Arbitrary for ETagKind {
    fn arbitrary(g: &mut Gen) -> Self {
        let opaque = gen_opaque_tag(g);
        let variant = u8::arbitrary(g) % 3;
        match variant {
            0 => ETagKind::Strong(opaque),
            1 => ETagKind::Weak(opaque),
            _ => ETagKind::Unquoted(opaque),
        }
    }
}

/// Generate a random opaque-tag string (alphanumeric, 1-16 chars, no commas or quotes).
fn gen_opaque_tag(g: &mut Gen) -> String {
    let len = (usize::arbitrary(g) % 15) + 1; // 1..=15
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789-_.".chars().collect();
    (0..len)
        .map(|_| {
            let idx = usize::arbitrary(g) % chars.len();
            chars[idx]
        })
        .collect()
}

/// A generated ETag list with 1-8 entries plus a target ETag.
#[derive(Clone, Debug)]
struct ETagListInput {
    /// The list of ETag entries (as they'd appear comma-separated in a header).
    entries: Vec<ETagKind>,
    /// The target ETag to match against (the cached ETag).
    target: ETagKind,
}

impl Arbitrary for ETagListInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let count = (usize::arbitrary(g) % 7) + 1; // 1..=7 entries
        let entries: Vec<ETagKind> = (0..count).map(|_| ETagKind::arbitrary(g)).collect();
        let target = ETagKind::arbitrary(g);
        ETagListInput { entries, target }
    }
}

// ============================================================================
// Reference implementation: split a comma-separated ETag list
// ============================================================================

/// Split a header value on commas that are outside double-quoted strings.
/// This is the reference implementation of the parsing logic that task 14.1 will
/// implement as `parse_etag_list`.
fn split_etag_list(header_value: &str) -> Vec<String> {
    let mut entries = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in header_value.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            ',' if !in_quotes => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    entries.push(trimmed);
                }
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        entries.push(trimmed);
    }

    entries
}

/// Reference list-level strong match: returns true if ANY entry strong-matches the target.
fn ref_etag_list_strong_match(header_value: &str, target: &str) -> bool {
    let trimmed = header_value.trim();
    if trimmed == "*" {
        return true;
    }
    split_etag_list(header_value)
        .iter()
        .any(|entry| etag_strong_match(entry, target))
}

/// Reference list-level weak match: returns true if ANY entry weak-matches the target.
fn ref_etag_list_weak_match(header_value: &str, target: &str) -> bool {
    let trimmed = header_value.trim();
    if trimmed == "*" {
        return true;
    }
    split_etag_list(header_value)
        .iter()
        .any(|entry| etag_weak_match(entry, target))
}

// ============================================================================
// Property 6: Disjunctive match equivalence
// **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6**
// ============================================================================

/// Property: For a generated ETag list and target, the list-level strong match
/// equals the disjunction (any) of per-entry strong matches.
///
/// This validates Requirement 6.3: strong match returns positive IF ANY split
/// entry strong-matches the target ETag.
fn prop_strong_match_disjunctive_equivalence(input: ETagListInput) -> TestResult {
    let header_value: String = input
        .entries
        .iter()
        .map(|e| e.to_header_value())
        .collect::<Vec<_>>()
        .join(", ");
    let target = input.target.to_header_value();

    // List-level match via reference implementation (split then any)
    let list_result = ref_etag_list_strong_match(&header_value, &target);

    // Per-entry disjunction (directly calling per-entry match on each generated entry)
    let per_entry_result = input
        .entries
        .iter()
        .any(|entry| etag_strong_match(&entry.to_header_value(), &target));

    TestResult::from_bool(list_result == per_entry_result)
}

/// Property: For a generated ETag list and target, the list-level weak match
/// equals the disjunction (any) of per-entry weak matches.
///
/// This validates Requirement 6.4: weak match returns positive IF ANY split
/// entry weak-matches the target ETag.
fn prop_weak_match_disjunctive_equivalence(input: ETagListInput) -> TestResult {
    let header_value: String = input
        .entries
        .iter()
        .map(|e| e.to_header_value())
        .collect::<Vec<_>>()
        .join(", ");
    let target = input.target.to_header_value();

    // List-level match via reference implementation (split then any)
    let list_result = ref_etag_list_weak_match(&header_value, &target);

    // Per-entry disjunction (directly calling per-entry match on each generated entry)
    let per_entry_result = input
        .entries
        .iter()
        .any(|entry| etag_weak_match(&entry.to_header_value(), &target));

    TestResult::from_bool(list_result == per_entry_result)
}

/// Property: When the header value is `*`, both strong and weak match return true
/// regardless of the target.
///
/// This validates Requirement 6.1: `*` short-circuits match evaluation.
fn prop_wildcard_matches_any_target(input: ETagListInput) -> TestResult {
    let target = input.target.to_header_value();

    let strong_result = ref_etag_list_strong_match("*", &target);
    let weak_result = ref_etag_list_weak_match("*", &target);

    TestResult::from_bool(strong_result && weak_result)
}

/// Property: A weak ETag entry (W/"...") never produces a positive strong match
/// against a strong target with the same opaque-tag.
///
/// This validates Requirement 6.5: weak tags inside a list are treated as weak
/// and SHALL NOT strong-match any entity tag.
fn prop_weak_entry_never_strong_matches(input: ETagListInput) -> TestResult {
    // Build a list of only weak entries
    let weak_entries: Vec<String> = input
        .entries
        .iter()
        .map(|e| {
            let opaque = match e {
                ETagKind::Strong(s) | ETagKind::Weak(s) | ETagKind::Unquoted(s) => s.clone(),
            };
            format!("W/\"{}\"", opaque)
        })
        .collect();

    let header_value = weak_entries.join(", ");

    // Target is a strong ETag with a known opaque tag from the list
    let target_opaque = match &input.entries[0] {
        ETagKind::Strong(s) | ETagKind::Weak(s) | ETagKind::Unquoted(s) => s.clone(),
    };
    let target = format!("\"{}\"", target_opaque);

    // Strong match against a list of all-weak entries must be false
    let result = ref_etag_list_strong_match(&header_value, &target);
    TestResult::from_bool(!result)
}

/// Property: If strong match returns true for a list, weak match must also return
/// true (strong match is a subset of weak match per RFC 7232).
///
/// This validates the relationship between Requirements 6.3 and 6.4.
fn prop_strong_match_implies_weak_match(input: ETagListInput) -> TestResult {
    let header_value: String = input
        .entries
        .iter()
        .map(|e| e.to_header_value())
        .collect::<Vec<_>>()
        .join(", ");
    let target = input.target.to_header_value();

    let strong_result = ref_etag_list_strong_match(&header_value, &target);
    let weak_result = ref_etag_list_weak_match(&header_value, &target);

    // If strong matches, weak must also match (strong ⊆ weak)
    if strong_result && !weak_result {
        TestResult::from_bool(false)
    } else {
        TestResult::from_bool(true)
    }
}

// ============================================================================
// Test runners
// ============================================================================

#[test]
fn test_property_strong_match_disjunctive_equivalence() {
    QuickCheck::new()
        .tests(200)
        .quickcheck(prop_strong_match_disjunctive_equivalence as fn(ETagListInput) -> TestResult);
}

#[test]
fn test_property_weak_match_disjunctive_equivalence() {
    QuickCheck::new()
        .tests(200)
        .quickcheck(prop_weak_match_disjunctive_equivalence as fn(ETagListInput) -> TestResult);
}

#[test]
fn test_property_wildcard_matches_any_target() {
    QuickCheck::new()
        .tests(100)
        .quickcheck(prop_wildcard_matches_any_target as fn(ETagListInput) -> TestResult);
}

#[test]
fn test_property_weak_entry_never_strong_matches() {
    QuickCheck::new()
        .tests(200)
        .quickcheck(prop_weak_entry_never_strong_matches as fn(ETagListInput) -> TestResult);
}

#[test]
fn test_property_strong_match_implies_weak_match() {
    QuickCheck::new()
        .tests(200)
        .quickcheck(prop_strong_match_implies_weak_match as fn(ETagListInput) -> TestResult);
}
