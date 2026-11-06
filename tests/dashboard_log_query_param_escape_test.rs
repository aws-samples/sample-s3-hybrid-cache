//! A rejected `limit` or `level` query value must not forge a log line: CR and LF in the
//! value appear escaped in the warning, not as a line break.

use hyper::Uri;
use s3_proxy::dashboard::parse_log_query_params;
use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn captured_output_for(query_uri: &str) -> String {
    let capture = Capture::default();
    let sink = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let uri: Uri = query_uri.parse().expect("uri");
        let _ = parse_log_query_params(&uri);
    });
    let bytes = capture.0.lock().unwrap().clone();
    String::from_utf8(bytes).expect("utf8")
}

fn assert_no_forged_line(out: &str) {
    assert!(!out.is_empty(), "the rejection was not logged: {out:?}");
    assert!(
        !out.lines().any(|l| l.starts_with("FORGED")),
        "a value forged a log line: {out:?}"
    );
}

#[test]
fn rejected_limit_value_cannot_forge_a_log_line() {
    assert_no_forged_line(&captured_output_for("/api/logs?limit=%0AFORGED"));
}

#[test]
fn rejected_level_value_cannot_forge_a_log_line() {
    assert_no_forged_line(&captured_output_for("/api/logs?level=A%0AFORGED"));
}

fn captured_debug_output_for(query_uri: &str) -> String {
    let capture = Capture::default();
    let sink = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let uri: Uri = query_uri.parse().expect("uri");
        let _ = parse_log_query_params(&uri);
    });
    let bytes = capture.0.lock().unwrap().clone();
    String::from_utf8(bytes).expect("utf8")
}

#[test]
fn since_value_cannot_forge_a_debug_log_line() {
    assert_no_forged_line(&captured_debug_output_for("/api/logs?since=%0AFORGED"));
}

#[test]
fn unknown_parameter_value_cannot_forge_a_debug_log_line() {
    assert_no_forged_line(&captured_debug_output_for("/api/logs?zzz=%0AFORGED"));
}
