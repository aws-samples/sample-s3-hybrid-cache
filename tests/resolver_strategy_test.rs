//! The DNS lookup strategy is set explicitly at every resolver site, through one helper.
//!
//! A change to the resolver library's default, or a new resolver built without the helper,
//! changes which address family the proxy dials first. These tests fail on either.
use hickory_resolver::config::{LookupIpStrategy, ResolveHosts, ResolverConfig};
use s3_proxy::config::{Config, ConnectionPoolConfig, DnsIpStrategy};
use s3_proxy::connection_pool::{build_resolver, resolver_options};
use std::path::{Path, PathBuf};

const ALL: [(DnsIpStrategy, LookupIpStrategy); 4] = [
    (DnsIpStrategy::Ipv6AndIpv4, LookupIpStrategy::Ipv6AndIpv4),
    (DnsIpStrategy::Ipv4AndIpv6, LookupIpStrategy::Ipv4AndIpv6),
    (DnsIpStrategy::Ipv4Only, LookupIpStrategy::Ipv4Only),
    (DnsIpStrategy::Ipv6Only, LookupIpStrategy::Ipv6Only),
];

#[test]
fn the_default_strategy_is_ipv6_first_and_named_explicitly() {
    assert_eq!(DnsIpStrategy::default(), DnsIpStrategy::Ipv6AndIpv4);
    assert_eq!(
        resolver_options(DnsIpStrategy::default()).ip_strategy,
        LookupIpStrategy::Ipv6AndIpv4
    );
}

#[test]
fn the_helper_applies_each_strategy_and_never_reads_the_hosts_file() {
    for (ours, theirs) in ALL {
        let opts = resolver_options(ours);
        assert_eq!(opts.ip_strategy, theirs, "{ours:?}");
        assert_eq!(opts.use_hosts_file, ResolveHosts::Never, "{ours:?}");
    }
}

#[tokio::test]
async fn a_built_resolver_carries_the_strategy() {
    for (ours, theirs) in ALL {
        let resolver = build_resolver(ResolverConfig::default(), ours).expect("resolver builds");
        assert_eq!(resolver.options().ip_strategy, theirs, "{ours:?}");
    }
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Lines of code (not comments) in `src/` containing `needle`, as `file:line` strings.
fn src_hits(needle: &str) -> Vec<String> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    let mut hits = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read source file");
        for (i, line) in text.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if line.contains(needle) {
                let rel = file.strip_prefix(&src).unwrap().display();
                hits.push(format!("{rel}:{}", i + 1));
            }
        }
    }
    hits
}

#[test]
fn no_resolver_is_built_outside_the_helper() {
    for needle in [
        "TokioResolver::builder",
        "Resolver::builder",
        "ResolverOpts::default()",
        "ResolverOpts {",
    ] {
        let hits = src_hits(needle);
        assert_eq!(
            hits.len(),
            1,
            "`{needle}` must appear once in src/ (inside the shared helper in connection_pool.rs), found: {hits:?}"
        );
        assert!(
            hits[0].starts_with("connection_pool.rs:"),
            "`{needle}` must live in connection_pool.rs, found: {hits:?}"
        );
    }
}

#[test]
fn config_without_the_field_gets_the_default() {
    let yaml = r#"
max_connections_per_ip: 10
dns_refresh_interval: "60s"
connection_timeout: "10s"
idle_timeout: "30s"
"#;
    let pool: ConnectionPoolConfig = serde_yaml_ng::from_str(yaml).expect("pool config parses");
    assert_eq!(pool.dns_ip_strategy, DnsIpStrategy::Ipv6AndIpv4);

    let minimal = r#"
cache:
  cache_dir: "./cache"
logging:
  access_log_dir: "./logs/access"
  app_log_dir: "./logs/app"
"#;
    let config: Config = serde_yaml_ng::from_str(minimal).expect("minimal config parses");
    assert_eq!(
        config.connection_pool.dns_ip_strategy,
        DnsIpStrategy::Ipv6AndIpv4
    );
}

#[test]
fn an_explicit_value_applies() {
    for (name, expected) in [
        ("ipv6_and_ipv4", DnsIpStrategy::Ipv6AndIpv4),
        ("ipv4_and_ipv6", DnsIpStrategy::Ipv4AndIpv6),
        ("ipv4_only", DnsIpStrategy::Ipv4Only),
        ("ipv6_only", DnsIpStrategy::Ipv6Only),
    ] {
        let yaml = format!(
            "cache:\n  cache_dir: \"./cache\"\nlogging:\n  access_log_dir: \"./logs/access\"\n  app_log_dir: \"./logs/app\"\nconnection_pool:\n  dns_ip_strategy: {name}\n"
        );
        let config: Config = serde_yaml_ng::from_str(&yaml).expect("config parses");
        assert_eq!(config.connection_pool.dns_ip_strategy, expected, "{name}");
    }
}
/// Lines of `src/` outside inline test modules containing `needle`, as
/// (`file:line`, the line and the four after it). A file's inline tests start at a
/// `#[cfg(test)]` line followed by a `mod` line and end at its closing brace.
fn production_hits(needle: &str) -> Vec<(String, String)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    let mut hits = Vec::new();
    for file in files {
        let rel = file.strip_prefix(&src).unwrap().display().to_string();
        if rel.contains("tests") {
            continue;
        }
        let text = std::fs::read_to_string(&file).expect("read source file");
        let lines: Vec<&str> = text.lines().collect();
        // Brace depth inside an inline test module; `None` outside one.
        let mut in_tests: Option<i64> = None;
        for (i, line) in lines.iter().enumerate() {
            if in_tests.is_none()
                && line.trim() == "#[cfg(test)]"
                && lines
                    .get(i + 1)
                    .is_some_and(|next| next.trim_start().starts_with("mod "))
            {
                in_tests = Some(0);
                continue;
            }
            if let Some(depth) = in_tests {
                let depth =
                    depth + line.matches('{').count() as i64 - line.matches('}').count() as i64;
                in_tests = if depth <= 0 && line.contains('}') {
                    None
                } else {
                    Some(depth)
                };
                continue;
            }
            if line.trim_start().starts_with("//") || !line.contains(needle) {
                continue;
            }
            let call = lines[i..(i + 5).min(lines.len())].join(" ");
            hits.push((format!("{rel}:{}", i + 1), call));
        }
    }
    hits.sort();
    hits
}
/// Every production resolver is built by the helper from the configured strategy:
/// exactly the four known sites call it, each passes a `dns_ip_strategy` value, the
/// TCP proxy's constructor is called with one, and no production code names the
/// default strategy outside `config.rs`.
#[test]
fn every_production_resolver_site_passes_the_configured_strategy() {
    let sites: Vec<(String, String)> = production_hits("build_resolver(")
        .into_iter()
        .filter(|(_, call)| !call.contains("fn build_resolver("))
        .collect();
    let files: Vec<&str> = sites
        .iter()
        .map(|(loc, _)| loc.split(':').next().unwrap())
        .collect();
    assert_eq!(
        files,
        vec![
            "connection_pool.rs",
            "http_proxy.rs",
            "tcp_proxy.rs",
            "tls_proxy_listener.rs"
        ],
        "the production build_resolver sites changed: {sites:?}"
    );
    for (loc, call) in &sites {
        assert!(
            call.contains("dns_ip_strategy"),
            "{loc} must pass the configured dns_ip_strategy: {call}"
        );
    }
    let constructors: Vec<(String, String)> =
        production_hits("TcpProxy::new_with_dns_ip_strategy(");
    assert!(!constructors.is_empty(), "HttpsProxy builds its TcpProxy");
    for (loc, call) in &constructors {
        assert!(
            call.contains("dns_ip_strategy,"),
            "{loc} must pass the configured dns_ip_strategy: {call}"
        );
    }
    let defaults: Vec<String> = production_hits("DnsIpStrategy::default()")
        .into_iter()
        .map(|(loc, _)| loc)
        .filter(|loc| !loc.starts_with("config.rs:"))
        .collect();
    assert!(
        defaults.is_empty(),
        "production code must take the strategy from config, not the default: {defaults:?}"
    );
}
