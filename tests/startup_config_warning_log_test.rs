//! A warning `Config::load` emits reaches the application log.
//!
//! `async_main` loads the configuration before it initialises logging, so a warning emitted
//! during the load has no subscriber unless it is held and replayed. This runs the built binary
//! with a config carrying a removed key and waits for the warning in the application log file.
//! Red side: the replay removed (a lever that skips the replay), which is the earlier ordering.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Every file under `dir`, concatenated.
fn read_tree(dir: &Path) -> String {
    let mut text = String::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return text;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            text.push_str(&read_tree(&path));
        } else if let Ok(content) = std::fs::read_to_string(&path) {
            text.push_str(&content);
        }
    }
    text
}

#[test]
fn a_removed_config_key_is_warned_in_the_application_log() {
    let dir = tempfile::tempdir().unwrap();
    let app_logs = dir.path().join("app-logs");
    let config_path = dir.path().join("config.yaml");
    let config = format!(
        "server:\n  http_port: {}\n  https_port: {}\n\
         cache:\n  cache_dir: \"{}\"\n  ram_cache_flush_threshold: 0.5\n\
         logging:\n  access_log_dir: \"{}\"\n  app_log_dir: \"{}\"\n  log_level: \"info\"\n\
         health:\n  port: {}\n\
         metrics:\n  port: {}\n\
         dashboard:\n  enabled: false\n",
        free_port(),
        free_port(),
        dir.path().join("cache").display(),
        dir.path().join("access-logs").display(),
        app_logs.display(),
        free_port(),
        free_port(),
    );
    std::fs::write(&config_path, config).unwrap();
    let mut child = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_s3-proxy"))
            .arg("-c")
            .arg(&config_path)
            .env_remove("RUST_LOG")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn s3-proxy"),
    );
    const WARNING: &str = "Configuration field 'cache.ram_cache_flush_threshold' has been removed";
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let logged = read_tree(&app_logs);
        if logged.contains(WARNING) {
            assert_eq!(
                logged.matches(WARNING).count(),
                1,
                "the warning must be logged once; application log: {logged}"
            );
            return;
        }
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!(
                "PROXY EXITED ({status}) without logging the removed-key warning; application \
                 log: {logged}"
            );
        }
        assert!(
            Instant::now() < deadline,
            "STARTUP WARNING NOT IN APPLICATION LOG: no line containing {WARNING:?} within 30s \
             of start; application log ({} bytes): {}",
            logged.len(),
            logged.chars().take(2000).collect::<String>()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
