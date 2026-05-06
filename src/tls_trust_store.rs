//! OS trust store loader.
//!
//! Centralises the `rustls-native-certs` call so the three outbound-TLS
//! construction sites in `s3_client.rs` and `http_proxy.rs` share one
//! implementation.
//!
//! The loader is best-effort: individual cert parse failures are logged as
//! warnings and skipped. The call fails only when zero certs loaded
//! successfully, which indicates a completely broken OS trust store.

use rustls::RootCertStore;
use tracing::warn;

use crate::{ProxyError, Result};

/// Load the OS trust store into a [`RootCertStore`].
///
/// Individual cert loading errors are logged at `warn` level and skipped.
/// Returns `Err` only when zero certs loaded successfully.
pub fn load_root_cert_store() -> Result<RootCertStore> {
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
    fn load_root_cert_store_returns_at_least_one_cert() {
        let store = load_root_cert_store().expect("OS trust store should load at least one cert");
        assert!(
            !store.is_empty(),
            "RootCertStore should contain at least one certificate"
        );
    }
}
