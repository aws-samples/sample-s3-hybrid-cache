//! OS trust store, loaded once per process.
//!
//! Centralises the `rustls-native-certs` call so the outbound-TLS construction
//! sites in `s3_client.rs`, `https_connector.rs` and `http_proxy.rs` share one
//! parsed store.
//!
//! Loading parses every certificate in the system bundle and costs
//! milliseconds of CPU. The signed-write path builds a TLS connector for every
//! request, so reloading there capped signed PUTs near 100 requests/s on a
//! 96-core host; the shared store removes that cost. As before, a changed OS
//! trust store takes effect on restart.
//!
//! The loader is best-effort: individual cert parse failures are logged as
//! warnings and skipped. Loading fails only when zero certs loaded
//! successfully, which indicates a completely broken OS trust store. A failed
//! load is not remembered, so the next call retries.

use std::sync::{Arc, OnceLock};

use rustls::RootCertStore;
use tracing::warn;

use crate::{ProxyError, Result};

static ROOT_CERT_STORE: OnceLock<Arc<RootCertStore>> = OnceLock::new();

/// Return the process-wide OS trust store, loading it on first use.
pub fn root_cert_store() -> Result<Arc<RootCertStore>> {
    if let Some(store) = ROOT_CERT_STORE.get() {
        return Ok(Arc::clone(store));
    }
    let store = Arc::new(load_native_root_cert_store()?);
    Ok(Arc::clone(ROOT_CERT_STORE.get_or_init(|| store)))
}

/// Load the OS trust store into a [`RootCertStore`].
///
/// Individual cert loading errors are logged at `warn` level and skipped.
/// Returns `Err` only when zero certs loaded successfully.
fn load_native_root_cert_store() -> Result<RootCertStore> {
    let result = rustls_native_certs::load_native_certs();

    for err in &result.errors {
        warn!("Skipping malformed native cert: {}", err);
    }

    if result.certs.is_empty() {
        return Err(ProxyError::TlsError(format!(
            "No usable native certs loaded; {} error(s) during load",
            result.errors.len()
        )));
    }

    let mut root_store = RootCertStore::empty();
    for cert in result.certs {
        if let Err(e) = root_store.add(cert) {
            warn!("Failed to add cert to root store: {}", e);
        }
    }

    Ok(root_store)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_cert_store_returns_at_least_one_cert() {
        let store = root_cert_store().expect("OS trust store should load at least one cert");
        assert!(
            !store.is_empty(),
            "RootCertStore should contain at least one certificate"
        );
    }

    #[test]
    fn root_cert_store_is_parsed_once_and_shared() {
        let first = root_cert_store().expect("OS trust store should load");
        let second = root_cert_store().expect("OS trust store should load");
        assert!(
            Arc::ptr_eq(&first, &second),
            "every caller must share one parsed store, not re-read the OS bundle"
        );
    }
}
