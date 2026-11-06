//! Request-level checks of the access-log record and the application-log writer.
//!
//! Own `[[test]]` target: `LoggerManager::initialize` installs the process-global tracing
//! subscriber, and the application-log checks read the file it writes, so no other test may
//! share this process. The subscriber is installed once, at debug level, by the first test
//! that builds a logger; every test uses the same application-log directory.
mod common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::logging::{AccessLogMode, LoggerManager, LoggingConfig};
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

const HOST: &str = "s3.us-west-2.amazonaws.com";
const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKIDFIXTURE/20260101/us-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fixture"; // gitleaks:allow
/// The documented number of space-separated fields in an access-log record.
const RECORD_FIELDS: usize = 25;

static APP_LOGS: OnceLock<TempDir> = OnceLock::new();

fn app_log_dir() -> PathBuf {
    APP_LOGS
        .get_or_init(|| TempDir::new().expect("app log dir"))
        .path()
        .to_path_buf()
}

fn logger_in(dir: &Path) -> Arc<tokio::sync::Mutex<LoggerManager>> {
    let mut logger = LoggerManager::new(LoggingConfig {
        access_log_dir: dir.join("access"),
        app_log_dir: app_log_dir(),
        access_log_enabled: true,
        access_log_mode: AccessLogMode::All,
        hostname: "fixture-host".to_string(),
        log_level: "debug".to_string(),
        // Long enough that only the test's own flush writes the file.
        access_log_flush_interval: Duration::from_secs(3600),
        access_log_buffer_size: 1000,
        access_log_retention_days: 30,
        app_log_retention_days: 30,
        log_cleanup_interval: Duration::from_secs(86400),
        access_log_file_rotation_interval: Duration::from_secs(300),
    });
    logger.initialize().expect("logger init");
    Arc::new(tokio::sync::Mutex::new(logger))
}

fn read_tree(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(read_tree(&path));
        } else {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            out.extend(text.lines().map(str::to_string));
        }
    }
    out
}

async fn access_log_lines(
    logger: &Arc<tokio::sync::Mutex<LoggerManager>>,
    dir: &Path,
) -> Vec<String> {
    logger.lock().await.force_flush().await.expect("flush");
    read_tree(&dir.join("access"))
}

fn app_log_text() -> String {
    read_tree(&app_log_dir().join("fixture-host")).join("\n")
}

/// Split a record into its fields: spaces separate them except inside `"..."` or `[...]`,
/// and a backslash keeps the next character from closing either.
fn record_fields(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let (mut quoted, mut bracketed, mut escaped) = (false, false, false);
    for c in line.chars() {
        if escaped {
            escaped = false;
            cur.push(c);
            continue;
        }
        match c {
            '\\' => {
                escaped = true;
                cur.push(c);
            }
            '"' => {
                quoted = !quoted;
                cur.push(c);
            }
            '[' if !quoted => {
                bracketed = true;
                cur.push(c);
            }
            ']' if !quoted => {
                bracketed = false;
                cur.push(c);
            }
            ' ' if !quoted && !bracketed => fields.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    fields.push(cur);
    fields
}

fn test_config(cache_dir: PathBuf) -> Arc<Config> {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = true;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 64 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.cache.write_cache_enabled = true;
    Arc::new(config)
}

fn ok_body(body: &[u8]) -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_header("content-length", body.len().to_string())
        .with_header("etag", "\"fixture-etag\"")
        .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        .with_header("content-type", "application/octet-stream")
        .with_body(Bytes::copy_from_slice(body))
}

/// One GET with the request line written byte for byte, so a client library cannot refuse or
/// rewrite the path under test. Returns the status code.
async fn raw_get(addr: SocketAddr, path: &str, extra_headers: &[&str]) -> u16 {
    let mut stream = TcpStream::connect(addr).await.expect("connect to proxy");
    let mut request = format!("GET {path} HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n");
    for header in extra_headers {
        request.push_str(header);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(20), stream.read_to_end(&mut response)).await;
    let text = String::from_utf8_lossy(&response);
    text.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

async fn start(
    tmp: &TempDir,
    logger: Arc<tokio::sync::Mutex<LoggerManager>>,
    body: &[u8],
) -> ProxyServer {
    let config = test_config(tmp.path().join("cache"));
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let stub = Arc::new(StubS3Client::new().with_default(ok_body(body)));
    spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        stub as Arc<dyn S3ClientApi + Send + Sync>,
        logger,
    )
    .await
}

async fn make_cache_infra(config: &Arc<Config>) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        false, // compression disabled — stored bytes stay 1:1 with the body
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");

    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));
    (cache_manager, range_handler)
}

struct ProxyServer {
    addr: SocketAddr,
    _shutdown_tx: oneshot::Sender<()>,
}

/// Serve the real `HttpProxy::handle_request` on a loopback port, so requests go through the
/// genuine dispatch — including `validate_host_header`, which is where the port-stripped host
/// the origin identity is derived from comes from.
async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
    logger: Arc<tokio::sync::Mutex<LoggerManager>>,
) -> ProxyServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let request_semaphore = Arc::new(Semaphore::new(config.server.max_concurrent_requests));
    let inflight_tracker = Arc::new(InFlightTracker::new());

    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, peer) = match accept {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    let io = TokioIo::new(stream);
                    let config = Arc::clone(&config);
                    let cache_manager = Arc::clone(&cache_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let inflight_tracker = Arc::clone(&inflight_tracker);
                    let request_semaphore = Arc::clone(&request_semaphore);
                    let logger = Arc::clone(&logger);

                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let inflight_tracker = Arc::clone(&inflight_tracker);
                            let request_semaphore = Arc::clone(&request_semaphore);
                    let logger = Arc::clone(&logger);
                            let inflight_ledger = s3_client.get_inflight_ledger();
                            async move {
                                HttpProxy::handle_request(
                                    req,
                                    peer,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    None,
                                    Some(logger),
                                    inflight_tracker,
                                    None,
                                    None,
                                    None,
                                    inflight_ledger,
                                )
                                .await
                            }
                        });
                        if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                            eprintln!("proxy connection error: {}", e);
                        }
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    ProxyServer {
        addr,
        _shutdown_tx: shutdown_tx,
    }
}

/// Run one request whose path holds `needle`, and return the access-log record that names it.
async fn record_for(path: &str, needle: &str) -> (u16, String) {
    let tmp = TempDir::new().unwrap();
    let logs = TempDir::new().unwrap();
    let logger = logger_in(logs.path());
    let server = start(&tmp, Arc::clone(&logger), b"fixture body").await;
    let status = raw_get(server.addr, path, &[&format!("Authorization: {AUTH}")]).await;
    let lines = access_log_lines(&logger, logs.path()).await;
    let line = lines
        .into_iter()
        .find(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("no access-log record names {needle:?} (status {status})"));
    (status, line)
}

/// A `"` in the bucket segment or in the key leaves the record whole and is escaped in
/// fields 1, 2 and 8.
#[tokio::test]
async fn a_quote_in_the_bucket_segment_keeps_the_record_whole() {
    let (status, line) = record_for("/bq\"x/object.bin", "bq").await;
    let fields = record_fields(&line);
    eprintln!("bucket-segment quote: status {status}, record: {line}");
    assert_eq!(fields.len(), RECORD_FIELDS, "record: {line}");
    assert_eq!(fields[0], "bq\\\"x", "record: {line}");
    assert_eq!(fields[1], "bq\\\"x", "record: {line}");
    assert_eq!(fields[7], "\"/bq\\\"x/object.bin\"", "record: {line}");
}

#[tokio::test]
async fn a_quote_in_the_key_keeps_the_record_whole() {
    let (status, line) = record_for("/bucket/kq\"y.bin", "kq").await;
    let fields = record_fields(&line);
    eprintln!("key quote: status {status}, record: {line}");
    assert_eq!(fields.len(), RECORD_FIELDS, "record: {line}");
    assert_eq!(fields[7], "\"/bucket/kq\\\"y.bin\"", "record: {line}");
}

/// A path holding `\"` reaches the record with the backslash escaped before the quote:
/// backslash, backslash, backslash, quote. A parser that honours backslash escapes reads
/// that as an escaped backslash and an escaped quote, so the record keeps its 25 fields.
#[tokio::test]
async fn a_backslash_before_a_quote_in_a_path_is_written_escaped() {
    let (status, line) = record_for("/bucket/bs\\\"z.bin", "bs").await;
    eprintln!("backslash-quote: status {status}, record: {line}");
    assert!(
        line.contains("bs\\\\\\\"z.bin"),
        "the path reached the record in a different shape: {line}"
    );
    assert_eq!(record_fields(&line).len(), RECORD_FIELDS, "record: {line}");
}
/// A presigned range read served from the RAM tier writes its path to the application log at
/// debug level; the signature must not appear there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_presigned_range_hit_from_ram_leaves_no_signature_in_the_application_log() {
    let tmp = TempDir::new().unwrap();
    let logs = TempDir::new().unwrap();
    let logger = logger_in(logs.path());
    let body = vec![7u8; 4096];
    let server = start(&tmp, Arc::clone(&logger), &body).await;
    let fill = raw_get(
        server.addr,
        "/ramlog/object.bin",
        &[&format!("Authorization: {AUTH}")],
    )
    .await;
    assert_eq!(fill, 200, "the fill request");
    let now = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let path = format!(
        "/ramlog/object.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDFIXTURE%2F20260101%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date={now}&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=needle0123abc" // gitleaks:allow
    );
    let mut reached_ram = false;
    for _ in 0..40 {
        let status = raw_get(server.addr, &path, &["Range: bytes=0-99"]).await;
        assert!(
            status == 200 || status == 206,
            "presigned range read: {status}"
        );
        if app_log_text().contains("source=ram") {
            reached_ram = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let text = app_log_text();
    assert!(
        reached_ram,
        "the RAM serve path was never reached, so this test measures nothing"
    );
    assert!(
        !text.contains("needle0123abc"),
        "a signature reached the application log"
    );
    assert!(text.contains("REDACTED"), "no masked value in the log");
}
