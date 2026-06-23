//! Shared path-component validation for filesystem safety.
//!
//! This module provides a single source of truth for validating strings before
//! they are used as path components (e.g. cache subdirectory names, upload IDs).
//! Both `parse_cache_key` and `uploadId` validation call through this helper so
//! the two defenses cannot drift apart.

/// Returns `true` if `component` is safe to use as a single filesystem path
/// segment — i.e. it will not escape a parent directory or inject special bytes.
///
/// A component is **unsafe** (returns `false`) if any of the following hold:
/// - It is empty.
/// - It contains a forward slash (`/`) or backslash (`\`).
/// - It contains the substring `..` (covers both the literal two-dot name and
///   any embedded traversal attempt like `foo..bar`).
/// - It contains a NUL byte (`0x00`) or any ASCII control character in the
///   range 0x00–0x1F or the DEL character 0x7F.
///
/// Everything else — normal filenames, alphanumeric strings, hyphens, single
/// dots (`.`), underscores, Unicode — is accepted.
///
/// # Examples
/// ```
/// use s3_proxy::path_safety::is_safe_path_component;
///
/// assert!(is_safe_path_component("valid-upload-id"));
/// assert!(is_safe_path_component("abc123"));
/// assert!(is_safe_path_component("file.txt"));
/// assert!(is_safe_path_component(".hidden"));
///
/// assert!(!is_safe_path_component(""));
/// assert!(!is_safe_path_component("../etc"));
/// assert!(!is_safe_path_component("a/b"));
/// assert!(!is_safe_path_component("a\\b"));
/// assert!(!is_safe_path_component("has\x00null"));
/// assert!(!is_safe_path_component("ctrl\x01char"));
/// ```
pub fn is_safe_path_component(component: &str) -> bool {
    if component.is_empty() {
        return false;
    }

    // Reject any embedded ".." substring (covers "..", "../x", "x/..", "x..y")
    if component.contains("..") {
        return false;
    }

    // Byte-level scan for path separators and control characters
    for b in component.bytes() {
        match b {
            b'/' | b'\\' => return false,
            0x00..=0x1F | 0x7F => return false,
            _ => {}
        }
    }

    true
}

/// Returns `true` if `relative_path` is safe to join onto a base directory without
/// escaping it. Unlike `is_safe_path_component`, this accepts forward slashes as
/// legitimate path separators (the value may be a multi-segment relative path like
/// `"bucket/XX/YYY/filename.bin"`).
///
/// A path is **unsafe** (returns `false`) if any of the following hold:
/// - It is empty.
/// - It starts with `/` or `\` (absolute path).
/// - Any path component is `..` (traversal).
/// - It contains a backslash anywhere (Windows-style separator / injection).
/// - It contains NUL bytes or ASCII control characters.
///
/// # Examples
/// ```
/// use s3_proxy::path_safety::is_safe_relative_file_path;
///
/// assert!(is_safe_relative_file_path("bucket/XX/YYY/file.bin"));
/// assert!(is_safe_relative_file_path("simple.bin"));
///
/// assert!(!is_safe_relative_file_path(""));
/// assert!(!is_safe_relative_file_path("/absolute/path"));
/// assert!(!is_safe_relative_file_path("../escape"));
/// assert!(!is_safe_relative_file_path("bucket/../../etc/passwd"));
/// assert!(!is_safe_relative_file_path("a\\b"));
/// ```
pub fn is_safe_relative_file_path(relative_path: &str) -> bool {
    if relative_path.is_empty() {
        return false;
    }

    // Reject absolute paths
    if relative_path.starts_with('/') || relative_path.starts_with('\\') {
        return false;
    }

    // Reject backslashes anywhere (Windows path separator / injection)
    if relative_path.contains('\\') {
        return false;
    }

    // Reject control characters and NUL
    for b in relative_path.bytes() {
        match b {
            0x00..=0x1F | 0x7F => return false,
            _ => {}
        }
    }

    // Check each path component for ".." traversal
    for component in relative_path.split('/') {
        if component == ".." {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Rejection cases ---

    #[test]
    fn rejects_empty_string() {
        assert!(!is_safe_path_component(""));
    }

    #[test]
    fn rejects_forward_slash() {
        assert!(!is_safe_path_component("a/b"));
        assert!(!is_safe_path_component("/leading"));
        assert!(!is_safe_path_component("trailing/"));
    }

    #[test]
    fn rejects_backslash() {
        assert!(!is_safe_path_component("a\\b"));
        assert!(!is_safe_path_component("\\leading"));
        assert!(!is_safe_path_component("trailing\\"));
    }

    #[test]
    fn rejects_dot_dot_traversal() {
        assert!(!is_safe_path_component(".."));
        assert!(!is_safe_path_component("../etc/passwd"));
        assert!(!is_safe_path_component("foo/.."));
        assert!(!is_safe_path_component("foo..bar")); // substring match
        assert!(!is_safe_path_component("..hidden"));
    }

    #[test]
    fn rejects_nul_byte() {
        assert!(!is_safe_path_component("has\x00null"));
        assert!(!is_safe_path_component("\x00"));
    }

    #[test]
    fn rejects_control_characters() {
        // 0x01 through 0x1F
        assert!(!is_safe_path_component("tab\there"));
        assert!(!is_safe_path_component("newline\nhere"));
        assert!(!is_safe_path_component("cr\rhere"));
        assert!(!is_safe_path_component("\x01"));
        assert!(!is_safe_path_component("\x1F"));
        // DEL (0x7F)
        assert!(!is_safe_path_component("del\x7Fchar"));
    }

    // --- Valid cases ---

    #[test]
    fn accepts_normal_alphanumeric() {
        assert!(is_safe_path_component("abc123"));
        assert!(is_safe_path_component("UPLOAD-ID-42"));
        assert!(is_safe_path_component(
            "2Hoj0CxQnbMljdfMrU3bYHPJFSRPCmLzSHBfSIz4k"
        ));
    }

    #[test]
    fn accepts_hyphens_underscores() {
        assert!(is_safe_path_component("my-upload-id"));
        assert!(is_safe_path_component("my_upload_id"));
        assert!(is_safe_path_component("a-b-c"));
    }

    #[test]
    fn accepts_single_dot() {
        // A single dot is fine (not traversal)
        assert!(is_safe_path_component("."));
        assert!(is_safe_path_component(".hidden"));
        assert!(is_safe_path_component("file.txt"));
        assert!(is_safe_path_component("a.b.c"));
    }

    #[test]
    fn accepts_unicode() {
        assert!(is_safe_path_component("日本語"));
        assert!(is_safe_path_component("café"));
        assert!(is_safe_path_component("emöji🎉"));
    }

    #[test]
    fn accepts_typical_s3_upload_ids() {
        // Real S3 upload IDs are base64-like strings
        assert!(is_safe_path_component(
            "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"
        ));
        assert!(is_safe_path_component(
            "2Hoj0CxQnbMljdfMrU3bYHPJFSRPCmLzSHBfSIz4k.I7fCSSntLmCDMg7yvPBZo"
        ));
    }

    // --- is_safe_relative_file_path tests ---

    #[test]
    fn relative_path_rejects_empty() {
        assert!(!is_safe_relative_file_path(""));
    }

    #[test]
    fn relative_path_rejects_absolute() {
        assert!(!is_safe_relative_file_path("/absolute/path"));
        assert!(!is_safe_relative_file_path("/etc/passwd"));
        assert!(!is_safe_relative_file_path("\\windows\\path"));
    }

    #[test]
    fn relative_path_rejects_dot_dot_traversal() {
        assert!(!is_safe_relative_file_path(".."));
        assert!(!is_safe_relative_file_path("../escape"));
        assert!(!is_safe_relative_file_path("bucket/../../../etc/passwd"));
        assert!(!is_safe_relative_file_path("bucket/XX/../../escape"));
        assert!(!is_safe_relative_file_path("valid/path/.."));
    }

    #[test]
    fn relative_path_rejects_backslash() {
        assert!(!is_safe_relative_file_path("a\\b"));
        assert!(!is_safe_relative_file_path("bucket\\file.bin"));
    }

    #[test]
    fn relative_path_rejects_control_chars() {
        assert!(!is_safe_relative_file_path("has\x00null"));
        assert!(!is_safe_relative_file_path("new\nline"));
        assert!(!is_safe_relative_file_path("tab\there"));
    }

    #[test]
    fn relative_path_accepts_valid_range_paths() {
        assert!(is_safe_relative_file_path("bucket/XX/YYY/filename.bin"));
        assert!(is_safe_relative_file_path(
            "my-bucket/ab/cde/obj_0-1024.bin"
        ));
        assert!(is_safe_relative_file_path("simple.bin"));
        assert!(is_safe_relative_file_path(
            "mpus_in_progress/bucket/upload-id/part1.bin"
        ));
    }

    #[test]
    fn relative_path_allows_single_dot_components() {
        // Single dots in path components are fine (e.g. hidden dirs, extensions)
        assert!(is_safe_relative_file_path("bucket/.hidden/file.bin"));
        assert!(is_safe_relative_file_path("./relative"));
    }
}
