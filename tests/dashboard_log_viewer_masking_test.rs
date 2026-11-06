//! The dashboard log viewer masks presigned-URL credentials in lines that an older proxy
//! wrote to the application log before the writer masked them.

use s3_proxy::dashboard::{LogQueryParams, LogReader};
use tempfile::TempDir;

#[tokio::test]
async fn a_presigned_line_in_a_log_file_is_masked_in_the_viewer() {
    let dir = TempDir::new().unwrap();
    let host_dir = dir.path().join("fixture-host");
    std::fs::create_dir_all(&host_dir).unwrap();
    let line = "2026-01-23T15:24:13.161401787+00:00 DEBUG ThreadId(05) s3_proxy::http_proxy: src/http_proxy.rs:1234: GET cache HIT for /bucket/object.bin?X-Amz-Credential=AKIDFIXTURE%2F20260101&X-Amz-Signature=needle0123abc&X-Amz-Expires=60 range=0-9\n";
    std::fs::write(host_dir.join("s3-proxy.log.2026-01-23"), line).unwrap();
    let reader = LogReader::new(dir.path().to_path_buf(), "fixture-host".to_string());
    let entries = reader
        .read_recent_logs(LogQueryParams {
            limit: Some(10),
            level_filter: None,
            text_filter: None,
            since: None,
        })
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert!(
        !entry.message.contains("needle0123abc"),
        "message: {}",
        entry.message
    );
    assert!(entry.message.contains("X-Amz-Signature=REDACTED"));
    assert!(entry.message.contains("X-Amz-Expires=60 range=0-9"));
    assert!(
        entry.fields.values().all(|v| !v.contains("needle0123abc")),
        "fields: {:?}",
        entry.fields
    );
}
