//! Every upstream request context is built through the two builders in
//! `src/s3_client.rs`, which append our `Via` entry. A production struct literal
//! elsewhere in `src/` would skip that, so this test scans `src/` for one.
//!
//! `#[cfg(test)]` modules are excluded: they build contexts
//! directly to drive the client. Comment lines are skipped.

use std::path::{Path, PathBuf};

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Count `S3RequestContext {` struct literals outside `#[cfg(test)]` modules.
///
/// A file can hold several test modules and production code between them, so each
/// `#[cfg(test)]` + `mod name {` at column 0 is skipped up to its closing `}` at column 0.
fn production_literals(text: &str) -> usize {
    let lines: Vec<&str> = text.lines().collect();
    let mut count = 0;
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        // Only an inline `mod name {` opens a block to skip; `mod name;` is a declaration.
        if line == "#[cfg(test)]"
            && lines
                .get(i + 1)
                .is_some_and(|l| l.starts_with("mod ") && l.trim_end().ends_with('{'))
        {
            i += 2;
            while i < lines.len() && lines[i] != "}" {
                i += 1;
            }
            i += 1;
            continue;
        }
        // A literal starts the expression (`S3RequestContext {`, `= S3RequestContext {`);
        // a signature (`-> S3RequestContext {`) and the definition do not count.
        if !line.trim_start().starts_with("//")
            && line.contains("S3RequestContext {")
            && !line.contains("-> S3RequestContext")
            && !line.contains("struct S3RequestContext")
        {
            count += 1;
        }
        i += 1;
    }
    count
}

#[test]
fn cfg_test_mod_declaration_does_not_start_a_skip() {
    let text = "#[cfg(test)]\nmod x;\nfn f() {\n    let c = S3RequestContext {\n    };\n}\n";
    assert_eq!(
        production_literals(text),
        1,
        "literal after `mod x;` must count"
    );
}

#[test]
fn cfg_test_inline_mod_is_skipped() {
    let text = "#[cfg(test)]\nmod tests {\n    let c = S3RequestContext {\n    };\n}\nfn f() {\n    let c = S3RequestContext {\n    };\n}\n";
    assert_eq!(production_literals(text), 1);
}

#[test]
fn only_the_two_builders_construct_an_s3_request_context() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rs_files(&src, &mut files);
    assert!(
        files.len() > 20,
        "PRECONDITION: src/ scan found too few files"
    );

    let mut offenders = Vec::new();
    let mut builder_literals = 0;
    for f in &files {
        let n = production_literals(&std::fs::read_to_string(f).expect("read"));
        if f.file_name().is_some_and(|n| n == "s3_client.rs") {
            builder_literals = n;
        } else if n > 0 {
            offenders.push((f.display().to_string(), n));
        }
    }
    assert!(
        offenders.is_empty(),
        "S3RequestContext struct literal outside the builders (no Via): {:?}",
        offenders
    );
    assert_eq!(
        builder_literals, 2,
        "s3_client.rs must hold exactly the two builder literals"
    );
}
