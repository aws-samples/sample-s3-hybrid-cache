//! Bug condition exploration — dependency security advisories
//!
//! Spec: `.kiro/specs/dependency-security-upgrades/`
//!
//! This test is Task 1 of the bugfix workflow. It reads `Cargo.lock` and
//! asserts that none of the three known-vulnerable crate versions are present.
//!
//! **On the pre-fix workspace this test FAILS** — that is the point. The
//! three assertion failures prove the bug exists and identify exactly which
//! advisory cluster is still open.
//!
//! # Vulnerabilities tracked
//!
//! - **lz4_flex 0.11.5** — RUSTSEC-2026-0041 (CVSS 8.2 high, yanked).
//!   Decompressing malformed LZ4 frames can leak uninitialized memory or
//!   stale bytes from a reused output buffer. Reached via the disk-cache
//!   `FrameDecoder` path in `src/compression.rs`.
//!
//! - **rustls-webpki 0.103.8** — RUSTSEC-2026-0104, RUSTSEC-2026-0098,
//!   RUSTSEC-2026-0099, RUSTSEC-2026-0049. Four advisories covering CRL
//!   parsing panics, URI/wildcard name-constraint bypass, and CRL
//!   distribution-point matching bugs. Reached transitively through
//!   `rustls 0.23.x`, `tokio-rustls 0.26.x`, and dev-dep `ureq 2.x`.
//!
//! - **idna 0.4.0** — RUSTSEC-2024-0421. Accepts Punycode labels that
//!   decode to pure-ASCII, enabling IDN homograph bypasses. Reached only
//!   through `trust-dns-proto 0.23.x` → `trust-dns-resolver 0.23.x`.
//!
//! # How the parser works
//!
//! `Cargo.lock` is a TOML file with repeated `[[package]]` sections. Each
//! section looks like:
//!
//! ```toml
//! [[package]]
//! name = "lz4_flex"
//! version = "0.11.5"
//! source = "registry+https://github.com/rust-lang/crates.io-index"
//! checksum = "..."
//! ```
//!
//! The parser splits on `\n[[package]]`, then for each block extracts the
//! `name` and `version` values using simple line-by-line string scanning.
//! No new crate dependencies are introduced.
//!
//! # Re-use as fix-checking guard (Task 1.3)
//!
//! Once the three clusters are fixed, this test passes and stays in the
//! suite as a regression guard. Any future accidental downgrade of
//! `lz4_flex`, `rustls-webpki`, or `idna` to a vulnerable version will
//! immediately surface as a named assertion failure.

/// Parse `Cargo.lock` content into a list of `(name, version)` pairs,
/// one entry per `[[package]]` block.
///
/// The parser is intentionally minimal: it splits on `[[package]]`
/// boundaries and extracts the first `name = "..."` and `version = "..."`
/// lines it finds in each block. This is sufficient for the flat structure
/// of a `Cargo.lock` file and introduces no new dependencies.
fn parse_cargo_lock_packages(content: &str) -> Vec<(String, String)> {
    let mut packages = Vec::new();

    // Split on [[package]] boundaries. The first element before the first
    // [[package]] marker is the file header (workspace metadata) and
    // contains no package entries — it is harmlessly skipped because it
    // will have neither a `name` nor a `version` line in the expected form.
    for block in content.split("[[package]]") {
        let mut name: Option<String> = None;
        let mut version: Option<String> = None;

        for line in block.lines() {
            let line = line.trim();

            if name.is_none() {
                if let Some(rest) = line.strip_prefix("name = \"") {
                    if let Some(value) = rest.strip_suffix('"') {
                        name = Some(value.to_string());
                    }
                }
            }

            if version.is_none() {
                if let Some(rest) = line.strip_prefix("version = \"") {
                    if let Some(value) = rest.strip_suffix('"') {
                        version = Some(value.to_string());
                    }
                }
            }

            if name.is_some() && version.is_some() {
                break;
            }
        }

        if let (Some(n), Some(v)) = (name, version) {
            packages.push((n, v));
        }
    }

    packages
}

/// Returns `true` if the package list contains an entry with the given
/// exact name and version.
fn contains_package(packages: &[(String, String)], name: &str, version: &str) -> bool {
    packages.iter().any(|(n, v)| n == name && v == version)
}

#[test]
fn no_vulnerable_dependency_versions_in_cargo_lock() {
    // Locate Cargo.lock relative to the manifest directory so the test
    // works regardless of the working directory when `cargo test` is run.
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set by cargo test");
    let lock_path = std::path::Path::new(&manifest_dir).join("Cargo.lock");

    let content = std::fs::read_to_string(&lock_path).unwrap_or_else(|e| {
        panic!(
            "Failed to read Cargo.lock at {}: {}",
            lock_path.display(),
            e
        )
    });

    let packages = parse_cargo_lock_packages(&content);

    // --- Cluster A: lz4_flex -----------------------------------------------
    // RUSTSEC-2026-0041 (CVSS 8.2 high, yanked from crates.io).
    // Decompressing malformed LZ4 frames can leak uninitialized memory or
    // stale output-buffer bytes. Fix: upgrade to lz4_flex >= 0.11.6.
    assert!(
        !contains_package(&packages, "lz4_flex", "0.11.5"),
        "SECURITY: lz4_flex 0.11.5 is present in Cargo.lock. \
         This version is yanked and has RUSTSEC-2026-0041 (CVSS 8.2 high): \
         decompressing malformed LZ4 frames can leak uninitialized memory or \
         stale output-buffer bytes into the HTTP response body. \
         Fix: upgrade lz4_flex to >= 0.11.6 in Cargo.toml and run `cargo update -p lz4_flex`."
    );

    // --- Cluster B: rustls-webpki ------------------------------------------
    // RUSTSEC-2026-0104 (reachable panic in CRL parsing),
    // RUSTSEC-2026-0098 (URI name constraints incorrectly accepted),
    // RUSTSEC-2026-0099 (wildcard name constraints incorrectly accepted),
    // RUSTSEC-2026-0049 (CRL distribution-point matching bug).
    // Fix: upgrade rustls / tokio-rustls / ureq so that rustls-webpki
    // resolves to >= 0.103.13.
    assert!(
        !contains_package(&packages, "rustls-webpki", "0.103.8"),
        "SECURITY: rustls-webpki 0.103.8 is present in Cargo.lock. \
         This version has four open advisories: \
         RUSTSEC-2026-0104 (reachable panic in CRL parsing), \
         RUSTSEC-2026-0098 (URI name constraints incorrectly accepted), \
         RUSTSEC-2026-0099 (wildcard name constraints incorrectly accepted), \
         RUSTSEC-2026-0049 (CRL distribution-point matching bug). \
         Fix: run `cargo update -p rustls -p tokio-rustls -p ureq -p rustls-webpki` \
         and verify `cargo tree -i rustls-webpki` shows >= 0.103.13."
    );

    // --- Cluster C: idna ---------------------------------------------------
    // RUSTSEC-2024-0421. Accepts Punycode labels that decode to pure-ASCII,
    // enabling IDN homograph bypasses in hostname handling. Reached only
    // through trust-dns-proto 0.23.x -> trust-dns-resolver 0.23.x.
    // Fix: migrate from trust-dns-resolver to hickory-resolver 0.24,
    // which pulls idna >= 1.0.0.
    assert!(
        !contains_package(&packages, "idna", "0.4.0"),
        "SECURITY: idna 0.4.0 is present in Cargo.lock. \
         This version has RUSTSEC-2024-0421: accepts Punycode labels that \
         decode to pure-ASCII, enabling IDN homograph bypasses in hostname \
         handling. It is reached via trust-dns-proto 0.23.x -> \
         trust-dns-resolver 0.23.x. \
         Fix: migrate to hickory-resolver 0.24 (which pulls idna >= 1.0.0) \
         and update the import paths in src/connection_pool.rs, \
         src/tcp_proxy.rs, src/error.rs, and src/logging.rs."
    );
}
