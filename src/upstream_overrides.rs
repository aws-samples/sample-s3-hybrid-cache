//! Upstream Transport Overrides — per-destination transport matcher.
//!
//! Parses `connection_pool.upstream_overrides` into a matcher that resolves a
//! request's Upstream_Target `(host, port)` to a [`TransportMode`]. The lookup is
//! the sole decision point for the caching-egress transport and is purely additive:
//! a target with no matching entry returns `None`, leaving the existing
//! verified-TLS-on-443 behaviour untouched.
//!
//! Matching mirrors the existing `EndpointOverrides` semantics (exact wins first,
//! then the longest matching suffix, case-insensitive). An IP-literal matcher
//! matches that IP exactly; a DNS-name matcher covers the name itself and any
//! subdomain so a single entry serves both path-style and virtual-hosted
//! addressing.

use crate::config::{UpstreamOverrideConfig, UpstreamScheme};
use std::collections::HashMap;
use std::net::IpAddr;
use tracing::warn;

/// The resolved upstream transport for a request.
///
/// Derived from `(scheme, validate_tls)` at parse time: `http` → [`Plaintext`],
/// `https` + `validate_tls: true` → [`TlsValidated`], `https` +
/// `validate_tls: false` → [`TlsUnvalidated`].
///
/// [`Plaintext`]: TransportMode::Plaintext
/// [`TlsValidated`]: TransportMode::TlsValidated
/// [`TlsUnvalidated`]: TransportMode::TlsUnvalidated
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    /// Plaintext HTTP — no TLS handshake.
    Plaintext,
    /// HTTPS, verifying the server certificate against the system trust store.
    TlsValidated,
    /// HTTPS, with no certificate verification (no chain or hostname check).
    TlsUnvalidated,
}

impl TransportMode {
    /// Derive the transport mode from the configured scheme and `validate_tls`.
    fn from_scheme(scheme: &UpstreamScheme, validate_tls: bool) -> Self {
        match scheme {
            UpstreamScheme::Http => TransportMode::Plaintext,
            UpstreamScheme::Https if validate_tls => TransportMode::TlsValidated,
            UpstreamScheme::Https => TransportMode::TlsUnvalidated,
        }
    }
}

impl TransportMode {
    /// Whether this mode waives a security protection — `Plaintext` (cleartext
    /// egress) or `TlsUnvalidated` (no MITM protection on the proxy→origin leg).
    /// `TlsValidated` waives nothing.
    pub fn is_protection_waiving(self) -> bool {
        matches!(
            self,
            TransportMode::Plaintext | TransportMode::TlsUnvalidated
        )
    }
}

/// A configured override whose transport waives a security protection, surfaced
/// for the startup warning (Requirement 1.6). `TlsValidated` entries are never
/// included (Requirement 1.7).
#[derive(Debug, Clone)]
pub struct ProtectionWaivingEntry {
    /// The configured endpoint, reconstructed as `host:port`.
    pub endpoint: String,
    /// The resolved mode — always `Plaintext` or `TlsUnvalidated`.
    pub mode: TransportMode,
}

impl ProtectionWaivingEntry {
    /// Human-readable description of the specific protection being waived.
    pub fn waived_protection(&self) -> &'static str {
        match self.mode {
            TransportMode::Plaintext => "cleartext egress (no TLS)",
            TransportMode::TlsUnvalidated => "certificate validation disabled",
            // Not reachable — protection-waiving entries are only Plaintext /
            // TlsUnvalidated — but kept total to avoid a panic path.
            TransportMode::TlsValidated => "none",
        }
    }
}

/// Parsed upstream transport overrides.
///
/// Holds an exact `(host, port) → TransportMode` table (IP literals and the
/// exact form of DNS names) plus a longest-first suffix table for DNS-name
/// subdomain coverage. At resolve time the exact match wins first, then the
/// longest matching suffix, case-insensitively. An empty matcher means the
/// feature is off and every destination gets Secure_Default_Behaviour.
#[derive(Debug, Clone)]
pub struct UpstreamOverrides {
    /// IP literals and the exact form of DNS-name entries.
    exact: HashMap<(String, u16), TransportMode>,
    /// DNS-name subdomain rules `(".name", port, mode)`, sorted longest-first
    /// for most-specific-wins semantics.
    suffixes: Vec<(String, u16, TransportMode)>,
}

impl UpstreamOverrides {
    /// Parse from the raw config map. Malformed entries and label-less matchers
    /// (a bare `*`) are logged and skipped — parsing never aborts or panics, so
    /// a bad dev line cannot take the proxy down.
    pub fn from_config(raw: &HashMap<String, UpstreamOverrideConfig>) -> Self {
        let mut exact: HashMap<(String, u16), TransportMode> = HashMap::new();
        let mut suffixes: Vec<(String, u16, TransportMode)> = Vec::new();

        for (key, cfg) in raw {
            // Split on the LAST ':' so IPv6 literals (host before the port colon)
            // survive; the port is the trailing component.
            let Some((host_raw, port_raw)) = key.rsplit_once(':') else {
                warn!(
                    "Invalid upstream_overrides key '{}': expected 'host:port'; skipping",
                    key
                );
                continue;
            };

            // Numeric port in 1..=65535 (u16 parse rejects out-of-range; reject 0).
            let port = match port_raw.parse::<u16>() {
                Ok(p) if p != 0 => p,
                _ => {
                    warn!(
                        "Invalid port '{}' in upstream_overrides key '{}': must be 1-65535; skipping",
                        port_raw, key
                    );
                    continue;
                }
            };

            // Normalize the host: strip surrounding brackets (IPv6) and lowercase
            // for case-insensitive matching (Requirement 2.5).
            let host = host_raw
                .strip_prefix('[')
                .and_then(|h| h.strip_suffix(']'))
                .unwrap_or(host_raw)
                .trim()
                .to_ascii_lowercase();

            if host.is_empty() {
                warn!("Empty host in upstream_overrides key '{}'; skipping", key);
                continue;
            }

            // Reject a bare `*` or any matcher with no literal label — it would
            // match any host, including link-local/internal addresses, and reopen
            // the relay hole (Requirement 2.6).
            if !host_has_literal_label(&host) {
                warn!(
                    "Rejecting upstream_overrides matcher '{}' (key '{}'): a bare '*' or \
                     label-less matcher would match any host; skipping",
                    host, key
                );
                continue;
            }

            let mode = TransportMode::from_scheme(&cfg.scheme, cfg.validate_tls);

            if host.parse::<IpAddr>().is_ok() {
                // IP literal — exact match only (no subdomain semantics;
                // IP endpoints use path-style addressing only).
                exact.insert((host, port), mode);
            } else {
                // DNS name — matches the name itself (exact) and any subdomain
                // (suffix), covering path-style and virtual-hosted addressing.
                exact.insert((host.clone(), port), mode);
                suffixes.push((format!(".{host}"), port, mode));
            }
        }

        // Longest suffix first so the most-specific DNS rule wins (Requirement 2.4).
        suffixes.sort_by_key(|(suffix, _, _)| std::cmp::Reverse(suffix.len()));

        Self { exact, suffixes }
    }

    /// Resolve an Upstream_Target `(host, port)` to a [`TransportMode`].
    ///
    /// Exact `(host, port)` wins first; then the longest matching DNS suffix at
    /// the same port. Host comparison is case-insensitive. `None` means no entry
    /// matched and the caller applies Secure_Default_Behaviour.
    pub fn resolve(&self, host: &str, port: u16) -> Option<TransportMode> {
        let host = host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host)
            .to_ascii_lowercase();

        if let Some(mode) = self.exact.get(&(host.clone(), port)) {
            return Some(*mode);
        }
        for (suffix, suffix_port, mode) in &self.suffixes {
            if *suffix_port == port && host.ends_with(suffix.as_str()) {
                return Some(*mode);
            }
        }
        None
    }

    /// Whether any overrides (exact or suffix) are configured. An empty matcher
    /// leaves the egress at Secure_Default_Behaviour for every destination.
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.suffixes.is_empty()
    }

    /// The configured entries whose transport waives a security protection
    /// (`Plaintext` or `TlsUnvalidated`), for the startup warning. Each
    /// configured key yields exactly one entry — DNS names are reported once even
    /// though they populate both the exact and suffix tables.
    pub fn protection_waiving_entries(&self) -> Vec<ProtectionWaivingEntry> {
        self.exact
            .iter()
            .filter(|(_, mode)| mode.is_protection_waiving())
            .map(|((host, port), mode)| ProtectionWaivingEntry {
                endpoint: format!("{host}:{port}"),
                mode: *mode,
            })
            .collect()
    }
}

/// Whether `host` contains at least one literal DNS label — a non-empty label
/// that is not the `*` wildcard. A host with no literal label (e.g. `*`, `*.`,
/// `.`) would match any host and is rejected (Requirement 2.6).
fn host_has_literal_label(host: &str) -> bool {
    host.split('.')
        .any(|label| !label.is_empty() && label != "*")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{UpstreamOverrideConfig, UpstreamScheme};

    /// Build an `UpstreamOverrideConfig` value directly (bypassing YAML) so the
    /// matcher tests exercise `from_config`/`resolve` in isolation.
    fn cfg(scheme: UpstreamScheme, validate_tls: bool) -> UpstreamOverrideConfig {
        UpstreamOverrideConfig {
            scheme,
            validate_tls,
        }
    }

    fn http() -> UpstreamOverrideConfig {
        cfg(UpstreamScheme::Http, true)
    }

    fn https_validated() -> UpstreamOverrideConfig {
        cfg(UpstreamScheme::Https, true)
    }

    fn https_unvalidated() -> UpstreamOverrideConfig {
        cfg(UpstreamScheme::Https, false)
    }

    fn build(entries: &[(&str, UpstreamOverrideConfig)]) -> UpstreamOverrides {
        let map: HashMap<String, UpstreamOverrideConfig> = entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        UpstreamOverrides::from_config(&map)
    }

    // ---- IP literal: exact match only, no subdomain semantics (Req 2.1, 8) ----

    #[test]
    fn ip_literal_matches_exactly_only() {
        let o = build(&[("127.0.0.1:9000", http())]);

        assert_eq!(o.resolve("127.0.0.1", 9000), Some(TransportMode::Plaintext));
        // An IP literal has no subdomain form — nothing "ends with" it.
        assert_eq!(o.resolve("bucket.127.0.0.1", 9000), None);
        // A different IP does not match.
        assert_eq!(o.resolve("127.0.0.2", 9000), None);
        // Same IP, different port does not match.
        assert_eq!(o.resolve("127.0.0.1", 9001), None);
    }

    // ---- DNS name covers itself + multi-label subdomains (Req 2.2, 8) ----

    #[test]
    fn dns_name_covers_itself_and_multi_label_subdomains_same_port() {
        let o = build(&[("store.local:9000", https_validated())]);

        // The name itself (path-style addressing).
        assert_eq!(
            o.resolve("store.local", 9000),
            Some(TransportMode::TlsValidated)
        );
        // Single-label subdomain (virtual-hosted bucket).
        assert_eq!(
            o.resolve("bucket.store.local", 9000),
            Some(TransportMode::TlsValidated)
        );
        // Multi-label subdomain (dotted bucket name).
        assert_eq!(
            o.resolve("my.bucket.store.local", 9000),
            Some(TransportMode::TlsValidated)
        );
    }

    #[test]
    fn dns_suffix_does_not_match_unrelated_or_partial_label() {
        let o = build(&[("store.local:9000", https_validated())]);

        // Unrelated host.
        assert_eq!(o.resolve("example.com", 9000), None);
        // A host that merely ends with the same characters but not on a label
        // boundary must NOT match (e.g. "mystore.local" vs ".store.local").
        assert_eq!(o.resolve("mystore.local", 9000), None);
    }

    #[test]
    fn dns_subdomain_only_matches_at_configured_port() {
        let o = build(&[("store.local:9000", https_validated())]);

        // Subdomain match is port-scoped.
        assert_eq!(o.resolve("bucket.store.local", 9001), None);
        assert_eq!(o.resolve("store.local", 443), None);
    }

    // ---- Port is part of the key (Req 2.3) ----

    #[test]
    fn port_is_part_of_key_same_host_two_modes() {
        let o = build(&[
            ("host.local:8080", http()),
            ("host.local:9000", https_validated()),
        ]);

        assert_eq!(
            o.resolve("host.local", 8080),
            Some(TransportMode::Plaintext)
        );
        assert_eq!(
            o.resolve("host.local", 9000),
            Some(TransportMode::TlsValidated)
        );
        // A third port has no entry.
        assert_eq!(o.resolve("host.local", 1234), None);
    }

    // ---- Precedence: exact beats suffix; longest suffix wins (Req 2.4) ----

    #[test]
    fn exact_match_beats_suffix_match() {
        // `api.store.local` is configured exactly as Plaintext, while the broader
        // `store.local` suffix would resolve it to TlsValidated. Exact wins.
        let o = build(&[
            ("store.local:9000", https_validated()),
            ("api.store.local:9000", http()),
        ]);

        assert_eq!(
            o.resolve("api.store.local", 9000),
            Some(TransportMode::Plaintext),
            "exact (host,port) entry must win over a broader DNS suffix"
        );
        // A sibling not configured exactly still falls to the broader suffix.
        assert_eq!(
            o.resolve("other.store.local", 9000),
            Some(TransportMode::TlsValidated)
        );
    }

    #[test]
    fn longest_suffix_wins_among_suffix_matches() {
        // Two overlapping DNS suffixes; the most specific (longest) must win for a
        // host under both.
        let o = build(&[
            ("store.local:9000", https_validated()),
            ("inner.store.local:9000", http()),
        ]);

        // `bucket.inner.store.local` ends with both `.store.local` and
        // `.inner.store.local`; the longer suffix wins.
        assert_eq!(
            o.resolve("bucket.inner.store.local", 9000),
            Some(TransportMode::Plaintext),
            "longest (most specific) DNS suffix must win"
        );
        // A host under only the broader suffix takes the broader rule.
        assert_eq!(
            o.resolve("bucket.store.local", 9000),
            Some(TransportMode::TlsValidated)
        );
    }

    // ---- Case-insensitive host matching (Req 2.5) ----

    #[test]
    fn host_matching_is_case_insensitive() {
        // Mixed-case key and mixed-case lookups both resolve.
        let o = build(&[("Store.Local:9000", https_validated())]);

        assert_eq!(
            o.resolve("store.local", 9000),
            Some(TransportMode::TlsValidated)
        );
        assert_eq!(
            o.resolve("STORE.LOCAL", 9000),
            Some(TransportMode::TlsValidated)
        );
        assert_eq!(
            o.resolve("Bucket.Store.Local", 9000),
            Some(TransportMode::TlsValidated)
        );
    }

    // ---- Bare `*` / label-less matchers are rejected (Req 2.6, 8) ----

    #[test]
    fn bare_wildcard_and_label_less_matchers_are_rejected() {
        let o = build(&[("*:9000", http()), ("*.:9000", http()), (".:9000", http())]);

        assert!(
            o.is_empty(),
            "bare '*' and label-less matchers must be skipped, not loaded"
        );
        assert_eq!(o.resolve("anything.com", 9000), None);
    }

    #[test]
    fn wildcard_with_literal_label_is_not_loaded_as_bare() {
        // `*.store.local` contains a literal label, so it is not rejected as a bare
        // wildcard. It is stored by its normalized host; the leading `*` label has
        // no special glob semantics here (DNS-suffix coverage is the mechanism), so
        // it simply does not resolve plain subdomains. The key point for Req 2.6 is
        // that it does NOT match arbitrary hosts.
        let o = build(&[("*.store.local:9000", http())]);

        assert_eq!(
            o.resolve("link-local.internal", 9000),
            None,
            "a matcher with a literal label must never match arbitrary hosts"
        );
    }

    // ---- Malformed entries are skipped (Req 2.6 robustness) ----

    #[test]
    fn malformed_entries_are_skipped() {
        let o = build(&[
            ("no-colon-here", http()),       // missing ':' -> no port
            ("host.local:0", http()),        // port 0 rejected
            ("host.local:70000", http()),    // port out of u16 range
            ("host.local:notaport", http()), // non-numeric port
            (":9000", http()),               // empty host
        ]);

        assert!(
            o.is_empty(),
            "all malformed entries must be skipped, leaving an empty matcher"
        );
    }

    #[test]
    fn malformed_entries_skipped_but_valid_entries_kept() {
        // A bad line must not take down sibling valid entries.
        let o = build(&[
            ("host.local:notaport", http()),
            ("good.local:9000", https_validated()),
        ]);

        assert!(!o.is_empty());
        assert_eq!(
            o.resolve("good.local", 9000),
            Some(TransportMode::TlsValidated)
        );
        assert_eq!(o.resolve("host.local", 9000), None);
    }

    // ---- Empty / absent config (Req 1.2 / 10) ----

    #[test]
    fn empty_config_is_empty_and_resolves_none() {
        let o = UpstreamOverrides::from_config(&HashMap::new());

        assert!(o.is_empty());
        assert_eq!(o.resolve("anything.com", 80), None);
        assert_eq!(o.resolve("127.0.0.1", 9000), None);
        assert!(o.protection_waiving_entries().is_empty());
    }

    // ---- Mode derivation incl. validate_tls default-true (Req 1.5, Property 4) ----

    #[test]
    fn mode_derivation_http_is_plaintext() {
        let o = build(&[("h.local:9000", http())]);
        assert_eq!(o.resolve("h.local", 9000), Some(TransportMode::Plaintext));
    }

    #[test]
    fn mode_derivation_https_default_is_validated() {
        // validate_tls defaults to true (secure by default).
        let o = build(&[("h.local:9000", https_validated())]);
        assert_eq!(
            o.resolve("h.local", 9000),
            Some(TransportMode::TlsValidated)
        );
    }

    #[test]
    fn mode_derivation_https_validate_false_is_unvalidated() {
        let o = build(&[("h.local:9000", https_unvalidated())]);
        assert_eq!(
            o.resolve("h.local", 9000),
            Some(TransportMode::TlsUnvalidated)
        );
    }

    // ---- Protection-waiving classification ----

    #[test]
    fn protection_waiving_entries_lists_only_plaintext_and_unvalidated() {
        let o = build(&[
            ("plain.local:9000", http()),
            ("validated.local:9000", https_validated()),
            ("skip.local:9000", https_unvalidated()),
        ]);

        let mut endpoints: Vec<String> = o
            .protection_waiving_entries()
            .into_iter()
            .map(|e| e.endpoint)
            .collect();
        endpoints.sort();

        assert_eq!(
            endpoints,
            vec![
                "plain.local:9000".to_string(),
                "skip.local:9000".to_string()
            ],
            "only Plaintext and TlsUnvalidated entries are protection-waiving; \
             TlsValidated must not be listed"
        );
    }

    #[test]
    fn ip_and_dns_entries_coexist() {
        // Regression: an IP-literal exact entry and a DNS-suffix entry on the same
        // port must both resolve independently.
        let o = build(&[
            ("10.0.0.5:9000", http()),
            ("store.local:9000", https_unvalidated()),
        ]);

        assert_eq!(o.resolve("10.0.0.5", 9000), Some(TransportMode::Plaintext));
        assert_eq!(
            o.resolve("bucket.store.local", 9000),
            Some(TransportMode::TlsUnvalidated)
        );
    }
}
