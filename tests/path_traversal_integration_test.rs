//! Integration test: malformed cache keys must not create files outside cache_dir.
//!
//! The proxy's path-traversal defense lives in `parse_cache_key` and
//! `get_sharded_path` — a Cache_Key whose bucket segment is `.`, `..`, empty,
//! or contains path separators / control bytes is rejected before any
//! filesystem path is constructed. This test drives that defense directly
//! with representative traversal payloads (including `/../etc/passwd`, the
//! request-path form) and confirms no file is written outside the cache
//! directory for any of them.
//!
//! Validates: Requirements 2.4, 4.1

use s3_proxy::disk_cache::{get_sharded_path, parse_cache_key};
use tempfile::TempDir;

/// Cache keys that must be rejected because they would escape the cache dir
/// or contain bytes that have no business in a filesystem path segment.
const TRAVERSAL_KEYS: &[&str] = &[
    "../etc/passwd",
    "../../secret",
    "/../etc/passwd", // form produced by a raw `GET /../etc/passwd` request line
    "./bucket/key",
    "/./etc/passwd",
    "..//etc/passwd",
    "bu\0cket/key",
];

#[test]
fn test_parse_cache_key_rejects_path_traversal() {
    // Validates: Requirements 2.4, 4.1
    for key in TRAVERSAL_KEYS {
        let result = parse_cache_key(key);
        assert!(
            result.is_err(),
            "parse_cache_key should reject traversal key {:?}, got {:?}",
            key,
            result
        );
    }
}

#[test]
fn test_get_sharded_path_never_escapes_cache_dir() {
    // Validates: Requirements 2.4, 4.1
    //
    // Set up a sentinel parent directory containing only the cache dir.
    // If any call to `get_sharded_path` were to succeed and return a path
    // outside `cache_dir`, a subsequent file write at that path would show
    // up as a new entry under `sentinel_parent` — which we would detect.
    let sentinel_parent = TempDir::new().unwrap();
    let cache_dir = sentinel_parent.path().join("cache");
    let base_dir = cache_dir.join("metadata");
    std::fs::create_dir_all(&base_dir).unwrap();

    let before: Vec<_> = std::fs::read_dir(sentinel_parent.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();

    for key in TRAVERSAL_KEYS {
        let result = get_sharded_path(&base_dir, key, ".meta");
        assert!(
            result.is_err(),
            "get_sharded_path should reject traversal key {:?}, got {:?}",
            key,
            result
        );
    }

    let after: Vec<_> = std::fs::read_dir(sentinel_parent.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name())
        .collect();

    assert_eq!(
        before, after,
        "No files or directories should have been created outside cache_dir"
    );
}

#[test]
fn test_get_sharded_path_accepts_well_formed_key() {
    // Sanity check: a well-formed cache key still produces a path inside base_dir.
    // Without this, the traversal rejection tests above could pass trivially
    // if `get_sharded_path` were broken for every input.
    //
    // Validates: Requirements 2.6 (no regression for valid keys)
    let cache_dir = TempDir::new().unwrap();
    let base_dir = cache_dir.path().join("metadata");

    let path = get_sharded_path(&base_dir, "my-bucket/path/to/obj", ".meta").unwrap();
    assert!(
        path.starts_with(&base_dir),
        "Well-formed cache key should produce a path under base_dir: {:?}",
        path
    );
}
