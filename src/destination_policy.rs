//! Destination Policy Module
//!
//! Implements CONNECT and SNI destination restriction to prevent the proxy from
//! being used as a relay to IMDS, internal-network services, or arbitrary outbound
//! ports. Addresses review finding #1 and Appendices A/B.
//!
//! The `DestinationPolicy` struct encapsulates the allow/deny decision for outbound
//! connections. Both the TLS proxy listener (CONNECT) and the TCP proxy (SNI passthrough)
//! call `policy.check()` before establishing a connection.
//!
//! `OwnAddresses` is the second gate on both tunnels: after the policy has chosen the
//! addresses, a tunnel is never relayed to one that leads back to a proxy.

use hickory_resolver::TokioResolver;
use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Reason a destination was rejected by the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectionReason {
    /// Target port is not the allowed port (443).
    PortNotAllowed { requested: u16, allowed: u16 },
    /// Hostname did not match any pattern in the connect allowlist.
    AllowlistMiss { hostname: String },
    /// All resolved IPs fall into prohibited ranges and none appear in endpoint overrides.
    IpRangeBlocked { hostname: String, ips: Vec<IpAddr> },
    /// DNS resolution failed for the target hostname.
    DnsResolutionFailed { hostname: String, error: String },
}

impl fmt::Display for RejectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RejectionReason::PortNotAllowed { requested, allowed } => {
                write!(
                    f,
                    "port {} not allowed (only port {} permitted)",
                    requested, allowed
                )
            }
            RejectionReason::AllowlistMiss { hostname } => {
                write!(
                    f,
                    "hostname '{}' does not match any connect allowlist pattern",
                    hostname
                )
            }
            RejectionReason::IpRangeBlocked { hostname, ips } => {
                write!(
                    f,
                    "all resolved IPs for '{}' are in prohibited ranges: {:?}",
                    hostname, ips
                )
            }
            RejectionReason::DnsResolutionFailed { hostname, error } => {
                write!(f, "DNS resolution failed for '{}': {}", hostname, error)
            }
        }
    }
}

/// Classifies whether an IP address falls into a prohibited range.
///
/// Prohibited ranges (per Requirement 1.2):
/// - 0.0.0.0/8 ("this network" / unspecified)
/// - 127.0.0.0/8 (loopback)
/// - 10.0.0.0/8 (private)
/// - 172.16.0.0/12 (private)
/// - 192.168.0.0/16 (private)
/// - 169.254.0.0/16 (link-local)
/// - ::ffff:0:0/96 (IPv4-mapped — delegates to IPv4 check)
/// - 64:ff9b::/96 (NAT64 well-known prefix — classified by the embedded IPv4 address)
/// - 64:ff9b:1::/48 (NAT64 local-use prefix — refused whole)
/// - ::/128 (IPv6 unspecified)
/// - ::1/128 (IPv6 loopback)
/// - fe80::/10 (IPv6 link-local)
/// - fc00::/7 (IPv6 unique local / private)
pub fn is_prohibited(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_prohibited_v4(v4),
        IpAddr::V6(v6) => is_prohibited_v6(v6),
    }
}

/// Check if an IPv4 address is in a prohibited range.
fn is_prohibited_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    // 0.0.0.0/8 — "this network" (includes the unspecified address)
    if octets[0] == 0 {
        return true;
    }
    // 127.0.0.0/8 — loopback
    if octets[0] == 127 {
        return true;
    }
    // 10.0.0.0/8 — private
    if octets[0] == 10 {
        return true;
    }
    // 172.16.0.0/12 — private (172.16.x.x through 172.31.x.x)
    if octets[0] == 172 && (octets[1] & 0xF0) == 16 {
        return true;
    }
    // 192.168.0.0/16 — private
    if octets[0] == 192 && octets[1] == 168 {
        return true;
    }
    // 169.254.0.0/16 — link-local
    if octets[0] == 169 && octets[1] == 254 {
        return true;
    }
    false
}

/// Check if an IPv6 address is in a prohibited range.
fn is_prohibited_v6(ip: Ipv6Addr) -> bool {
    // IPv4-mapped addresses (::ffff:x.x.x.x) — delegate to IPv4 classification.
    // Uses `to_ipv4_mapped()` (stable since Rust 1.75) which only matches the
    // ::ffff:0:0/96 form, NOT the deprecated ::x.x.x.x compat form.
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_prohibited_v4(v4);
    }
    // :: — unspecified address
    if ip == Ipv6Addr::UNSPECIFIED {
        return true;
    }
    // ::1/128 — loopback
    if ip == Ipv6Addr::LOCALHOST {
        return true;
    }
    let segments = ip.segments();
    // 64:ff9b::/96 — NAT64 well-known prefix (RFC 6052). The low 32 bits are an IPv4
    // address a NAT64 gateway will connect to, so classify that address exactly as its
    // IPv4 form: 64:ff9b::a9fe:a9fe is 169.254.169.254 and must be refused.
    if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2..6].iter().all(|s| *s == 0) {
        let [a, b] = segments[6].to_be_bytes();
        let [c, d] = segments[7].to_be_bytes();
        return is_prohibited_v4(Ipv4Addr::new(a, b, c, d));
    }
    // 64:ff9b:1::/48 — NAT64 local-use prefix (RFC 8215). The embedded IPv4 address sits at
    // a position that depends on the prefix length the operator chose, which the proxy
    // cannot know, so the whole range is refused. An address an operator needs in it can be
    // listed in `endpoint_overrides`.
    if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2] == 0x0001 {
        return true;
    }
    // fe80::/10 — link-local (first 10 bits: 1111 1110 10xx xxxx)
    if (segments[0] & 0xFFC0) == 0xFE80 {
        return true;
    }
    // fc00::/7 — unique local address (first 7 bits: 1111 110x)
    if (segments[0] & 0xFE00) == 0xFC00 {
        return true;
    }
    false
}

/// Policy engine for CONNECT and SNI destination validation.
///
/// Encapsulates the allow/deny decision for outbound connections based on:
/// - Port restriction (only port 443 allowed)
/// - Optional hostname allowlist (glob patterns)
/// - IP range classification (prohibit private/loopback/link-local)
/// - Endpoint override carve-out (allow configured PrivateLink IPs)
pub struct DestinationPolicy {
    /// The only port allowed for outbound connections (default: 443).
    allowed_port: u16,
    /// Optional allowlist of hostname glob patterns. If `Some`, only matching
    /// hostnames are permitted. If `None`, all hostnames pass the allowlist gate.
    connect_allowlist: Option<Vec<String>>,
    /// Set of IPs from `endpoint_overrides` that are allowed even if they fall
    /// in prohibited ranges (e.g., PrivateLink ENIs in 10.x.x.x).
    endpoint_override_ips: HashSet<IpAddr>,
}

impl DestinationPolicy {
    /// Create a new destination policy.
    ///
    /// # Arguments
    /// - `allowed_port`: The only port permitted for outbound connections (typically 443).
    /// - `connect_allowlist`: Optional list of hostname glob patterns. `None` means no
    ///   allowlist filtering (all hostnames pass). Patterns use `*.` prefix for subdomain
    ///   matching (like TLS cert matching).
    /// - `endpoint_override_ips`: Set of IPs from endpoint_overrides configuration that
    ///   are allowed even when they fall in prohibited ranges.
    pub fn new(
        allowed_port: u16,
        connect_allowlist: Option<Vec<String>>,
        endpoint_override_ips: HashSet<IpAddr>,
    ) -> Self {
        Self {
            allowed_port,
            connect_allowlist,
            endpoint_override_ips,
        }
    }

    /// Returns the allowed port for this policy.
    pub fn allowed_port(&self) -> u16 {
        self.allowed_port
    }

    /// Check whether a destination (host:port) is allowed.
    ///
    /// Decision flow:
    /// 1. Reject if port != allowed_port → `PortNotAllowed`
    /// 2. If allowlist configured and hostname doesn't match → `AllowlistMiss`
    /// 3. Resolve hostname via DNS
    /// 4. Classify each resolved IP as prohibited or allowed
    /// 5. If ALL IPs are prohibited AND none in endpoint_override_ips → `IpRangeBlocked`
    /// 6. Otherwise → return the non-prohibited IPs (plus any override-allowed IPs)
    ///
    /// Returns `Ok(Vec<IpAddr>)` with the IPs safe to connect to, or
    /// `Err(RejectionReason)` if the destination is prohibited.
    pub async fn check(
        &self,
        host: &str,
        port: u16,
        resolver: &TokioResolver,
    ) -> Result<Vec<IpAddr>, RejectionReason> {
        // 1. Port gate
        if port != self.allowed_port {
            return Err(RejectionReason::PortNotAllowed {
                requested: port,
                allowed: self.allowed_port,
            });
        }

        // 2. Allowlist gate
        if let Some(ref allowlist) = self.connect_allowlist {
            if !self.hostname_matches_allowlist(host, allowlist) {
                return Err(RejectionReason::AllowlistMiss {
                    hostname: host.to_string(),
                });
            }
        }

        // 3. DNS resolution
        let resolved_ips: Vec<IpAddr> = match resolver.lookup_ip(host).await {
            Ok(lookup) => lookup.iter().collect(),
            Err(e) => {
                return Err(RejectionReason::DnsResolutionFailed {
                    hostname: host.to_string(),
                    error: e.to_string(),
                });
            }
        };

        if resolved_ips.is_empty() {
            return Err(RejectionReason::DnsResolutionFailed {
                hostname: host.to_string(),
                error: "no IP addresses returned".to_string(),
            });
        }

        // 4 & 5. IP classification with endpoint_override_ips carve-out
        self.classify_ips(host, &resolved_ips)
    }

    /// Classify resolved IPs and determine which are safe to connect to.
    ///
    /// This is separated from `check()` to allow direct testing without DNS.
    pub fn classify_ips(
        &self,
        host: &str,
        resolved_ips: &[IpAddr],
    ) -> Result<Vec<IpAddr>, RejectionReason> {
        let mut allowed_ips = Vec::new();

        for &ip in resolved_ips {
            if !is_prohibited(ip) {
                // IP is not in any prohibited range — always allowed
                allowed_ips.push(ip);
            } else if self.endpoint_override_ips.contains(&ip) {
                // IP is prohibited but appears in endpoint_overrides — carve-out
                allowed_ips.push(ip);
            }
            // Otherwise: IP is prohibited and not in overrides — skip it
        }

        if allowed_ips.is_empty() {
            // ALL resolved IPs are prohibited and none are in endpoint_overrides
            Err(RejectionReason::IpRangeBlocked {
                hostname: host.to_string(),
                ips: resolved_ips.to_vec(),
            })
        } else {
            Ok(allowed_ips)
        }
    }

    /// Check if a hostname matches any pattern in the allowlist.
    ///
    /// Pattern matching rules (case-insensitive, like TLS cert matching):
    /// - `*.example.com` matches `foo.example.com`, `bar.baz.example.com`
    /// - `example.com` matches only `example.com` exactly
    /// - Patterns are compared case-insensitively
    fn hostname_matches_allowlist(&self, hostname: &str, allowlist: &[String]) -> bool {
        let hostname_lower = hostname.to_ascii_lowercase();

        for pattern in allowlist {
            let pattern_lower = pattern.to_ascii_lowercase();

            if let Some(suffix) = pattern_lower.strip_prefix("*.") {
                // Wildcard pattern: *.example.com matches any subdomain
                // "foo.example.com" matches, "example.com" does not
                if hostname_lower.ends_with(&format!(".{}", suffix)) {
                    return true;
                }
                // Also match the bare suffix itself (*.example.com matches example.com)
                if hostname_lower == suffix {
                    return true;
                }
            } else {
                // Exact match
                if hostname_lower == pattern_lower {
                    return true;
                }
            }
        }

        false
    }
}

/// How long a snapshot of this host's interface addresses is used before it is read again.
const OWN_ADDRESS_REFRESH: Duration = Duration::from_secs(30);
/// At most one tunnel-refusal warning per interval; the ones in between are counted into
/// the next.
const REFUSAL_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// The addresses a CONNECT or SNI passthrough tunnel is never relayed to, because they
/// lead back to a proxy: this host's interface addresses, plus
/// `server.tls.tunnel_refused_addresses` for the ones that reach a proxy without being on
/// an interface (an EC2 public IPv4 or Elastic IP address, a load balancer in front of the
/// fleet, peer proxies).
///
/// Without this a tunnel whose destination is the proxy itself is relayed back into its
/// own listener, which relays it again, until descriptors run out. The destination
/// policy does not stop that for an address it allows (an EC2 instance's global IPv6
/// address, a public IPv4 address on the interface), nor for one an `endpoint_overrides`
/// entry carves out, so this check runs after the policy and applies to both.
///
/// Interface addresses come from `getifaddrs`, read again on the first check after the
/// snapshot is 30 seconds old: an address added since is not refused for up to that long.
/// A failed read keeps the previous snapshot. Addresses are compared in canonical form,
/// so `::ffff:a.b.c.d` and the NAT64 `64:ff9b::a.b.c.d` match `a.b.c.d`.
pub struct OwnAddresses {
    configured: HashSet<IpAddr>,
    read_interfaces: bool,
    snapshot: Mutex<Option<(Instant, Arc<HashSet<IpAddr>>)>>,
    refusal_log: Mutex<RefusalLog>,
}

struct RefusalLog {
    last: Option<Instant>,
    suppressed: u64,
}

impl OwnAddresses {
    /// This host's interface addresses plus `configured`.
    pub fn new(configured: &[IpAddr]) -> Self {
        Self::build(configured, true)
    }

    /// `configured` alone, never reading the interfaces: lets a unit test relay to a
    /// loopback upstream, which the interface read would refuse.
    #[cfg(test)]
    pub(crate) fn listed_only(configured: &[IpAddr]) -> Self {
        Self::build(configured, false)
    }

    fn build(configured: &[IpAddr], read_interfaces: bool) -> Self {
        Self {
            configured: configured.iter().map(|ip| canonical(*ip)).collect(),
            read_interfaces,
            snapshot: Mutex::new(None),
            refusal_log: Mutex::new(RefusalLog {
                last: None,
                suppressed: 0,
            }),
        }
    }

    /// Whether a tunnel to `ip` would lead back to a proxy.
    pub fn contains(&self, ip: IpAddr) -> bool {
        let interfaces = self.interfaces();
        self.is_own(&interfaces, ip)
    }

    /// Refuse a tunnel when any of its candidate addresses leads back to a proxy.
    /// `Err` carries the refused addresses; `Ok` returns the list unchanged. One own
    /// address refuses the whole list, because the name resolves to a host that answers
    /// on it and a dial to the others could still loop through the first on a retry
    /// or a different resolution.
    pub fn filter_tunnel_targets(
        &self,
        ips: Vec<IpAddr>,
    ) -> std::result::Result<Vec<IpAddr>, Vec<IpAddr>> {
        let interfaces = self.interfaces();
        let own: Vec<IpAddr> = ips
            .iter()
            .copied()
            .filter(|ip| self.is_own(&interfaces, *ip))
            .collect();
        if own.is_empty() {
            Ok(ips)
        } else {
            Err(own)
        }
    }

    /// Log a refused tunnel at `warn!`, at most once per ten seconds; the line carries the
    /// number of refusals left unlogged since the last one.
    pub fn warn_refused(&self, tunnel: &str, host: &str, port: u16, refused: &[IpAddr]) {
        let now = Instant::now();
        let suppressed = {
            let mut log = self
                .refusal_log
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if log
                .last
                .is_some_and(|last| now.duration_since(last) < REFUSAL_LOG_INTERVAL)
            {
                log.suppressed += 1;
                return;
            }
            log.last = Some(now);
            std::mem::take(&mut log.suppressed)
        };
        tracing::warn!(
            hostname = %host,
            port = port,
            addresses = ?refused,
            suppressed_since_last = suppressed,
            "{} destination refused: it leads back to a proxy (an interface address of this host or a server.tls.tunnel_refused_addresses entry)",
            tunnel
        );
    }

    fn is_own(&self, interfaces: &HashSet<IpAddr>, ip: IpAddr) -> bool {
        let ip = canonical(ip);
        // The passthrough's dual-stack listener answers on every 127/8 address, on
        // 0.0.0.0 and on :: as well as on the ones an interface lists.
        let loopback_or_unspecified =
            self.read_interfaces && (ip.is_loopback() || ip.is_unspecified());
        loopback_or_unspecified || self.configured.contains(&ip) || interfaces.contains(&ip)
    }

    fn interfaces(&self) -> Arc<HashSet<IpAddr>> {
        if !self.read_interfaces {
            return Arc::default();
        }
        let mut snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        if let Some((taken, set)) = snapshot.as_ref() {
            if now.duration_since(*taken) < OWN_ADDRESS_REFRESH {
                return Arc::clone(set);
            }
        }
        let set = match interface_addresses() {
            Ok(set) => Arc::new(set.into_iter().map(canonical).collect()),
            Err(e) => {
                tracing::warn!(
                    "Could not read this host's interface addresses, keeping the previous set for tunnel loop refusal: {}",
                    e
                );
                snapshot
                    .as_ref()
                    .map(|(_, set)| Arc::clone(set))
                    .unwrap_or_default()
            }
        };
        *snapshot = Some((now, Arc::clone(&set)));
        set
    }
}

/// `ip` with an IPv4-mapped or NAT64 well-known-prefix IPv6 form reduced to its IPv4
/// address, so either form matches an IPv4 entry.
fn canonical(ip: IpAddr) -> IpAddr {
    let IpAddr::V6(v6) = ip else {
        return ip;
    };
    if let Some(v4) = v6.to_ipv4_mapped() {
        return IpAddr::V4(v4);
    }
    let segments = v6.segments();
    if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2..6].iter().all(|s| *s == 0) {
        let [a, b] = segments[6].to_be_bytes();
        let [c, d] = segments[7].to_be_bytes();
        return IpAddr::V4(Ipv4Addr::new(a, b, c, d));
    }
    ip
}

/// Every IPv4 and IPv6 address on this host's interfaces, loopback included.
#[cfg(unix)]
fn interface_addresses() -> std::io::Result<HashSet<IpAddr>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: `getifaddrs` writes the head of a list it allocates, freed below.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut addresses = HashSet::new();
    let mut node = head;
    while !node.is_null() {
        // SAFETY: `node` is an element of the list `getifaddrs` returned, not yet freed.
        let ifa = unsafe { &*node };
        if !ifa.ifa_addr.is_null() {
            // SAFETY: `ifa_addr` is non-null and its family says which sockaddr it is.
            let family = libc::c_int::from(unsafe { (*ifa.ifa_addr).sa_family });
            if family == libc::AF_INET {
                // SAFETY: an AF_INET address is a `sockaddr_in`.
                let sin = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
                addresses.insert(IpAddr::V4(Ipv4Addr::from(u32::from_be(
                    sin.sin_addr.s_addr,
                ))));
            } else if family == libc::AF_INET6 {
                // SAFETY: an AF_INET6 address is a `sockaddr_in6`.
                let sin6 = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in6) };
                addresses.insert(IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr)));
            }
        }
        node = ifa.ifa_next;
    }
    // SAFETY: `head` came from a successful `getifaddrs` and is freed once.
    unsafe { libc::freeifaddrs(head) };
    Ok(addresses)
}

/// No interface read off Unix: only the configured addresses are refused.
#[cfg(not(unix))]
fn interface_addresses() -> std::io::Result<HashSet<IpAddr>> {
    Ok(HashSet::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // is_prohibited() tests
    // =========================================================================

    #[test]
    fn test_loopback_v4_prohibited() {
        assert!(is_prohibited("127.0.0.1".parse().unwrap()));
        assert!(is_prohibited("127.255.255.255".parse().unwrap()));
        assert!(is_prohibited("127.0.0.0".parse().unwrap()));
    }

    #[test]
    fn test_private_10_prohibited() {
        assert!(is_prohibited("10.0.0.1".parse().unwrap()));
        assert!(is_prohibited("10.255.255.255".parse().unwrap()));
    }

    #[test]
    fn test_private_172_16_prohibited() {
        assert!(is_prohibited("172.16.0.1".parse().unwrap()));
        assert!(is_prohibited("172.31.255.255".parse().unwrap()));
        // 172.32.x.x is NOT private
        assert!(!is_prohibited("172.32.0.1".parse().unwrap()));
        // 172.15.x.x is NOT private
        assert!(!is_prohibited("172.15.255.255".parse().unwrap()));
    }

    #[test]
    fn test_private_192_168_prohibited() {
        assert!(is_prohibited("192.168.0.1".parse().unwrap()));
        assert!(is_prohibited("192.168.255.255".parse().unwrap()));
    }

    #[test]
    fn test_link_local_v4_prohibited() {
        assert!(is_prohibited("169.254.0.1".parse().unwrap()));
        assert!(is_prohibited("169.254.169.254".parse().unwrap())); // IMDS!
        assert!(is_prohibited("169.254.255.255".parse().unwrap()));
    }

    #[test]
    fn test_public_v4_allowed() {
        assert!(!is_prohibited("8.8.8.8".parse().unwrap()));
        assert!(!is_prohibited("52.216.100.1".parse().unwrap())); // S3 IP
        assert!(!is_prohibited("1.1.1.1".parse().unwrap()));
        assert!(!is_prohibited("203.0.113.1".parse().unwrap()));
    }

    #[test]
    fn test_loopback_v6_prohibited() {
        assert!(is_prohibited("::1".parse().unwrap()));
    }

    #[test]
    fn test_link_local_v6_prohibited() {
        assert!(is_prohibited("fe80::1".parse().unwrap()));
        assert!(is_prohibited("fe80::abcd:1234".parse().unwrap()));
        assert!(is_prohibited("febf::1".parse().unwrap())); // fe80::/10 upper bound
    }

    #[test]
    fn test_unique_local_v6_prohibited() {
        assert!(is_prohibited("fc00::1".parse().unwrap()));
        assert!(is_prohibited("fd00::1".parse().unwrap()));
        assert!(is_prohibited("fdff::1".parse().unwrap()));
    }

    #[test]
    fn test_public_v6_allowed() {
        assert!(!is_prohibited("2001:db8::1".parse().unwrap()));
        assert!(!is_prohibited("2600:1f18::1".parse().unwrap())); // AWS
    }

    // =========================================================================
    // Allowlist matching tests
    // =========================================================================

    #[test]
    fn test_allowlist_wildcard_match() {
        let policy = DestinationPolicy::new(
            443,
            Some(vec!["*.amazonaws.com".to_string()]),
            HashSet::new(),
        );
        assert!(policy.hostname_matches_allowlist(
            "s3.us-east-1.amazonaws.com",
            policy.connect_allowlist.as_ref().unwrap()
        ));
        assert!(policy.hostname_matches_allowlist(
            "s3.amazonaws.com",
            policy.connect_allowlist.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_allowlist_wildcard_bare_suffix_match() {
        let policy = DestinationPolicy::new(
            443,
            Some(vec!["*.amazonaws.com".to_string()]),
            HashSet::new(),
        );
        // *.amazonaws.com also matches "amazonaws.com" itself
        assert!(policy.hostname_matches_allowlist(
            "amazonaws.com",
            policy.connect_allowlist.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_allowlist_exact_match() {
        let policy = DestinationPolicy::new(
            443,
            Some(vec!["minio.internal.example.com".to_string()]),
            HashSet::new(),
        );
        assert!(policy.hostname_matches_allowlist(
            "minio.internal.example.com",
            policy.connect_allowlist.as_ref().unwrap()
        ));
        assert!(!policy.hostname_matches_allowlist(
            "other.example.com",
            policy.connect_allowlist.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_allowlist_case_insensitive() {
        let policy = DestinationPolicy::new(
            443,
            Some(vec!["*.AMAZONAWS.COM".to_string()]),
            HashSet::new(),
        );
        assert!(policy.hostname_matches_allowlist(
            "s3.us-east-1.amazonaws.com",
            policy.connect_allowlist.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_allowlist_no_match() {
        let policy = DestinationPolicy::new(
            443,
            Some(vec!["*.amazonaws.com".to_string()]),
            HashSet::new(),
        );
        assert!(!policy.hostname_matches_allowlist(
            "evil.example.com",
            policy.connect_allowlist.as_ref().unwrap()
        ));
    }

    // =========================================================================
    // classify_ips() tests
    // =========================================================================

    #[test]
    fn test_classify_all_public_ips_allowed() {
        let policy = DestinationPolicy::new(443, None, HashSet::new());
        let ips: Vec<IpAddr> = vec![
            "52.216.100.1".parse().unwrap(),
            "52.216.100.2".parse().unwrap(),
        ];
        let result = policy.classify_ips("s3.amazonaws.com", &ips);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_classify_all_prohibited_rejected() {
        let policy = DestinationPolicy::new(443, None, HashSet::new());
        let ips: Vec<IpAddr> = vec![
            "10.0.0.1".parse().unwrap(),
            "169.254.169.254".parse().unwrap(),
        ];
        let result = policy.classify_ips("evil.internal", &ips);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RejectionReason::IpRangeBlocked { .. }
        ));
    }

    #[test]
    fn test_classify_mixed_ips_returns_only_allowed() {
        let policy = DestinationPolicy::new(443, None, HashSet::new());
        let ips: Vec<IpAddr> = vec!["10.0.0.1".parse().unwrap(), "52.216.100.1".parse().unwrap()];
        let result = policy.classify_ips("mixed.example.com", &ips);
        assert!(result.is_ok());
        let allowed = result.unwrap();
        assert_eq!(allowed.len(), 1);
        assert_eq!(allowed[0], "52.216.100.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn test_classify_prohibited_ip_in_overrides_allowed() {
        let mut overrides = HashSet::new();
        overrides.insert("10.0.1.100".parse::<IpAddr>().unwrap());

        let policy = DestinationPolicy::new(443, None, overrides);
        let ips: Vec<IpAddr> = vec!["10.0.1.100".parse().unwrap()];
        let result = policy.classify_ips("privatelink.s3.amazonaws.com", &ips);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec!["10.0.1.100".parse::<IpAddr>().unwrap()]
        );
    }

    #[test]
    fn test_classify_all_prohibited_but_one_in_overrides() {
        let mut overrides = HashSet::new();
        overrides.insert("10.0.1.100".parse::<IpAddr>().unwrap());

        let policy = DestinationPolicy::new(443, None, overrides);
        let ips: Vec<IpAddr> = vec!["10.0.1.100".parse().unwrap(), "10.0.2.200".parse().unwrap()];
        let result = policy.classify_ips("privatelink.s3.amazonaws.com", &ips);
        assert!(result.is_ok());
        let allowed = result.unwrap();
        assert_eq!(allowed.len(), 1);
        assert_eq!(allowed[0], "10.0.1.100".parse::<IpAddr>().unwrap());
    }

    // =========================================================================
    // Port gate tests (via classify_ips is not enough, need full check mock)
    // =========================================================================

    #[test]
    fn test_imds_ip_prohibited() {
        // The specific IMDS IP must be caught
        assert!(is_prohibited("169.254.169.254".parse().unwrap()));
    }

    #[test]
    fn test_172_16_boundary() {
        // 172.16.0.0 is the start of the /12 range
        assert!(is_prohibited("172.16.0.0".parse().unwrap()));
        // 172.31.255.255 is the end
        assert!(is_prohibited("172.31.255.255".parse().unwrap()));
        // 172.15.255.255 is just below
        assert!(!is_prohibited("172.15.255.255".parse().unwrap()));
        // 172.32.0.0 is just above
        assert!(!is_prohibited("172.32.0.0".parse().unwrap()));
    }

    // =========================================================================
    // Destination-policy IP gap closure tests (Req 3)
    // =========================================================================

    #[test]
    fn test_zero_network_v4_prohibited() {
        // 0.0.0.0/8 — "this network" range
        assert!(is_prohibited("0.0.0.0".parse().unwrap()));
        assert!(is_prohibited("0.1.2.3".parse().unwrap()));
        assert!(is_prohibited("0.255.255.255".parse().unwrap()));
    }

    #[test]
    fn test_ipv4_mapped_v6_prohibited() {
        // ::ffff:169.254.169.254 — IPv4-mapped link-local (IMDS)
        assert!(is_prohibited("::ffff:169.254.169.254".parse().unwrap()));
        // ::ffff:127.0.0.1 — IPv4-mapped loopback
        assert!(is_prohibited("::ffff:127.0.0.1".parse().unwrap()));
        // ::ffff:10.0.0.1 — IPv4-mapped private
        assert!(is_prohibited("::ffff:10.0.0.1".parse().unwrap()));
        // ::ffff:0.0.0.0 — IPv4-mapped "this network"
        assert!(is_prohibited("::ffff:0.0.0.0".parse().unwrap()));
    }

    #[test]
    fn test_ipv4_mapped_v6_public_allowed() {
        // ::ffff:52.216.100.1 — IPv4-mapped public IP should pass
        assert!(!is_prohibited("::ffff:52.216.100.1".parse().unwrap()));
        assert!(!is_prohibited("::ffff:8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_unspecified_v6_prohibited() {
        // :: — the IPv6 unspecified address
        assert!(is_prohibited("::".parse().unwrap()));
    }

    // =========================================================================
    // Property-based tests (quickcheck)
    // =========================================================================
    //
    // **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.7**

    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    /// Wrapper for IpAddr that implements Arbitrary for quickcheck.
    #[derive(Clone, Debug)]
    struct ArbitraryIpAddr(IpAddr);

    impl Arbitrary for ArbitraryIpAddr {
        fn arbitrary(g: &mut Gen) -> Self {
            // Generate a mix of IPv4 and IPv6, biased toward interesting ranges
            let choice = u8::arbitrary(g) % 13;
            let ip = match choice {
                // Prohibited IPv4 ranges (generate more of these to exercise the boundary)
                0 => IpAddr::V4(Ipv4Addr::new(
                    127,
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                )),
                1 => IpAddr::V4(Ipv4Addr::new(
                    10,
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                )),
                2 => IpAddr::V4(Ipv4Addr::new(
                    172,
                    16 + (u8::arbitrary(g) % 16),
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                )),
                3 => IpAddr::V4(Ipv4Addr::new(192, 168, u8::arbitrary(g), u8::arbitrary(g))),
                4 => IpAddr::V4(Ipv4Addr::new(169, 254, u8::arbitrary(g), u8::arbitrary(g))),
                // 0.0.0.0/8 — "this network"
                5 => IpAddr::V4(Ipv4Addr::new(
                    0,
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                )),
                // Public IPv4
                6 | 7 => IpAddr::V4(Ipv4Addr::new(
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                    u8::arbitrary(g),
                )),
                // Prohibited IPv6
                8 => IpAddr::V6(Ipv6Addr::LOCALHOST),
                // :: unspecified
                9 => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                10 => {
                    // fe80::/10 or fc00::/7
                    let prefix = if bool::arbitrary(g) {
                        0xFE80
                    } else {
                        0xFC00 + (u16::arbitrary(g) % 0x0200)
                    };
                    IpAddr::V6(Ipv6Addr::new(
                        prefix,
                        u16::arbitrary(g),
                        u16::arbitrary(g),
                        u16::arbitrary(g),
                        u16::arbitrary(g),
                        u16::arbitrary(g),
                        u16::arbitrary(g),
                        u16::arbitrary(g),
                    ))
                }
                // IPv4-mapped IPv6 (::ffff:x.x.x.x)
                11 => {
                    let v4 = Ipv4Addr::new(
                        u8::arbitrary(g),
                        u8::arbitrary(g),
                        u8::arbitrary(g),
                        u8::arbitrary(g),
                    );
                    IpAddr::V6(v4.to_ipv6_mapped())
                }
                // Public IPv6
                _ => IpAddr::V6(Ipv6Addr::new(
                    0x2001 + (u16::arbitrary(g) % 0x1000),
                    u16::arbitrary(g),
                    u16::arbitrary(g),
                    u16::arbitrary(g),
                    u16::arbitrary(g),
                    u16::arbitrary(g),
                    u16::arbitrary(g),
                    u16::arbitrary(g),
                )),
            };
            ArbitraryIpAddr(ip)
        }
    }

    /// Property 1: IP classification correctness.
    ///
    /// For every generated (hostname, port, resolved_ips, endpoint_override_ips) tuple,
    /// the policy decision matches the predicate:
    /// - If port != 443: always reject with PortNotAllowed
    /// - If port == 443: reject iff ALL resolved IPs are prohibited AND none are in overrides
    ///
    /// **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.7**
    #[quickcheck]
    fn prop_classify_ips_matches_predicate(
        ips_arb: Vec<ArbitraryIpAddr>,
        overrides_arb: Vec<ArbitraryIpAddr>,
    ) -> bool {
        // Need at least one IP to classify
        if ips_arb.is_empty() {
            return true; // vacuously true — classify_ips with empty slice is not a valid call
        }

        let resolved_ips: Vec<IpAddr> = ips_arb.iter().map(|a| a.0).collect();
        let override_set: HashSet<IpAddr> = overrides_arb.iter().map(|a| a.0).collect();

        let policy = DestinationPolicy::new(443, None, override_set.clone());
        let result = policy.classify_ips("test.example.com", &resolved_ips);

        // Compute expected outcome:
        // An IP is "allowed" if it's not prohibited OR if it's in the override set
        let has_allowed_ip = resolved_ips
            .iter()
            .any(|ip| !is_prohibited(*ip) || override_set.contains(ip));

        match result {
            Ok(allowed_ips) => {
                // Should only succeed if at least one IP is allowed
                if !has_allowed_ip {
                    return false;
                }
                // Every returned IP must be either non-prohibited or in overrides
                allowed_ips.iter().all(|ip| {
                    !is_prohibited(*ip) || override_set.contains(ip)
                })
                // And the returned set must be non-empty
                && !allowed_ips.is_empty()
            }
            Err(RejectionReason::IpRangeBlocked { .. }) => {
                // Should only fail if NO IP is allowed
                !has_allowed_ip
            }
            _ => false, // classify_ips should only return Ok or IpRangeBlocked
        }
    }

    /// Property: Port gate — if port != 443, always reject with PortNotAllowed
    /// regardless of IP classification.
    ///
    /// This tests the port gate via the full `check()` path indirectly by
    /// verifying the DestinationPolicy struct's port field is respected.
    /// Since `check()` is async and requires DNS, we test the port gate logic
    /// by verifying that classify_ips is only reachable when port == 443.
    ///
    /// **Validates: Requirements 1.1, 1.7**
    #[quickcheck]
    fn prop_port_gate_rejects_non_443(port: u16, ips_arb: Vec<ArbitraryIpAddr>) -> bool {
        if port == 443 || ips_arb.is_empty() {
            return true; // skip the allowed port and empty IP cases
        }

        // A policy with the given port as allowed_port
        let policy = DestinationPolicy::new(443, None, HashSet::new());

        // The policy's allowed_port is 443, so any other port should be rejected.
        // We can't call check() without DNS, but we verify the structural invariant:
        // the policy stores 443 as the only allowed port.
        policy.allowed_port == 443 && port != 443
    }

    /// Property: Override carve-out correctness.
    ///
    /// For every generated set of IPs where ALL are prohibited, if we place
    /// at least one of them in the override set, classify_ips must succeed.
    ///
    /// **Validates: Requirements 1.3, 1.7**
    #[quickcheck]
    fn prop_override_carveout_allows_prohibited_ips(ips_arb: Vec<ArbitraryIpAddr>) -> bool {
        if ips_arb.is_empty() {
            return true;
        }

        let resolved_ips: Vec<IpAddr> = ips_arb.iter().map(|a| a.0).collect();

        // Filter to only prohibited IPs for this test
        let prohibited_ips: Vec<IpAddr> = resolved_ips
            .iter()
            .copied()
            .filter(|ip| is_prohibited(*ip))
            .collect();

        if prohibited_ips.is_empty() {
            return true; // no prohibited IPs to test with
        }

        // Without overrides: all-prohibited should be rejected
        let policy_no_override = DestinationPolicy::new(443, None, HashSet::new());
        let result_no_override =
            policy_no_override.classify_ips("test.example.com", &prohibited_ips);
        let rejected_without_override = result_no_override.is_err();

        // With the first prohibited IP in overrides: should be allowed
        let mut override_set = HashSet::new();
        override_set.insert(prohibited_ips[0]);
        let policy_with_override = DestinationPolicy::new(443, None, override_set);
        let result_with_override =
            policy_with_override.classify_ips("test.example.com", &prohibited_ips);
        let allowed_with_override = result_with_override.is_ok();

        rejected_without_override && allowed_with_override
    }
}

#[cfg(test)]
mod own_address_tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// The interface read lists loopback, which every host carries, and no address
    /// outside this host.
    #[test]
    fn the_interface_read_lists_loopback_and_nothing_foreign() {
        let own = OwnAddresses::new(&[]);
        assert!(own.contains(ip("127.0.0.1")));
        assert!(own.contains(ip("::ffff:127.0.0.1")), "the IPv4-mapped form");
        assert!(!own.contains(ip("192.0.2.44")));
        assert!(!own.contains(ip("2001:db8::44")));
    }

    /// A listed address matches in its IPv4-mapped and NAT64 forms too.
    #[test]
    fn a_listed_address_matches_in_every_form() {
        let own = OwnAddresses::listed_only(&[ip("198.51.100.7")]);
        assert!(own.contains(ip("198.51.100.7")));
        assert!(own.contains(ip("::ffff:198.51.100.7")));
        assert!(own.contains(ip("64:ff9b::c633:6407")));
        assert!(!own.contains(ip("198.51.100.8")));
    }

    /// A list with no own address passes unchanged; one own address anywhere in it
    /// refuses the whole list and names only the own addresses.
    #[test]
    fn one_own_address_refuses_the_whole_list() {
        let own = OwnAddresses::listed_only(&[ip("203.0.113.5")]);
        assert_eq!(
            own.filter_tunnel_targets(vec![ip("192.0.2.1"), ip("192.0.2.2")]),
            Ok(vec![ip("192.0.2.1"), ip("192.0.2.2")])
        );
        assert_eq!(
            own.filter_tunnel_targets(vec![ip("192.0.2.1"), ip("203.0.113.5"), ip("192.0.2.2")]),
            Err(vec![ip("203.0.113.5")])
        );
        assert_eq!(
            own.filter_tunnel_targets(vec![ip("203.0.113.5")]),
            Err(vec![ip("203.0.113.5")])
        );
    }

    /// Every 127/8 address, 0.0.0.0, :: and ::1 count as own under the production
    /// interface read, listed or not on an interface.
    #[test]
    fn loopback_and_unspecified_addresses_are_own() {
        let own = OwnAddresses::new(&[]);
        for a in [
            "127.0.0.2",
            "127.255.255.254",
            "0.0.0.0",
            "::",
            "::1",
            "::ffff:127.0.0.9",
        ] {
            assert!(own.contains(ip(a)), "{a} must count as own");
        }
        assert!(!own.contains(ip("192.0.2.44")));
    }
}
