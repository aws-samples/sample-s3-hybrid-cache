//! `cache.write_cache_max_object_size` reaches the write-cache admission check.
//!
//! Drives the production construction path: `HttpProxy::new` builds the `CacheManager` from
//! `Config`, `CacheManager::initialize` builds the `WriteCacheManager`, and
//! `get_write_cache_max_object_size` reads the same `max_object_size` that `try_reserve`
//! refuses on. A value other than the 256 MiB default is used so a hardcoded default
//! anywhere on the path fails the assertion.

use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn configured_write_cache_max_object_size_reaches_write_cache_manager() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tmp = TempDir::new().unwrap();

    const CONFIGURED: u64 = 64 * 1024 * 1024;
    let mut config = Config::default();
    config.cache.cache_dir = tmp.path().to_path_buf();
    config.cache.write_cache_enabled = true;
    config.cache.write_cache_max_object_size = CONFIGURED;

    let proxy = HttpProxy::new(SocketAddr::from(([127, 0, 0, 1], 0)), Arc::new(config))
        .expect("HttpProxy::new");
    let cache_manager = proxy.get_cache_manager();
    cache_manager.initialize().await.expect("cache init");

    assert_eq!(
        cache_manager.get_write_cache_max_object_size().await,
        CONFIGURED,
        "cache.write_cache_max_object_size must reach WriteCacheManager::max_object_size"
    );
}
