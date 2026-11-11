//! NAT64 addresses are classified by the IPv4 address they embed.
use s3_proxy::destination_policy::is_prohibited;
use std::net::IpAddr;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

#[test]
fn well_known_prefix_embedding_a_prohibited_address_is_refused() {
    // 169.254.169.254 (instance metadata), 127.0.0.1, 10.0.0.1, 192.168.1.1, 172.16.0.1, 0.0.0.0
    for s in [
        "64:ff9b::a9fe:a9fe",
        "64:ff9b::7f00:1",
        "64:ff9b::a00:1",
        "64:ff9b::c0a8:101",
        "64:ff9b::ac10:1",
        "64:ff9b::",
    ] {
        assert!(is_prohibited(ip(s)), "{s} must be refused");
    }
}

#[test]
fn well_known_prefix_embedding_a_public_address_is_allowed() {
    // 52.216.100.1 and 8.8.8.8
    for s in ["64:ff9b::34d8:6401", "64:ff9b::808:808"] {
        assert!(!is_prohibited(ip(s)), "{s} must be allowed");
    }
}

#[test]
fn the_embedded_address_decides_exactly_as_its_ipv4_form() {
    for s in [
        "169.254.169.254",
        "127.0.0.1",
        "10.1.2.3",
        "52.216.100.1",
        "8.8.8.8",
    ] {
        let v4: std::net::Ipv4Addr = s.parse().unwrap();
        let [a, b, c, d] = v4.octets();
        let nat64 = format!(
            "64:ff9b::{:x}:{:x}",
            u16::from_be_bytes([a, b]),
            u16::from_be_bytes([c, d])
        );
        assert_eq!(
            is_prohibited(ip(&nat64)),
            is_prohibited(IpAddr::V4(v4)),
            "{nat64} versus {s}"
        );
    }
}

#[test]
fn nearby_prefixes_are_not_swept_into_the_rule() {
    // 64:ff9b:0:1::/64 is not the well-known /96: bits 64 to 95 are not zero.
    assert!(!is_prohibited(ip("64:ff9b::1:0:a9fe:a9fe")));
    // 64:ff9c:: is a different /32.
    assert!(!is_prohibited(ip("64:ff9c::a9fe:a9fe")));
}

#[test]
fn local_use_prefix_is_refused_whole() {
    for s in [
        "64:ff9b:1::",
        "64:ff9b:1::34d8:6401",
        "64:ff9b:1:ffff:ffff:ffff:ffff:ffff",
        "64:ff9b:1:0:34:d864:100:0",
    ] {
        assert!(is_prohibited(ip(s)), "{s} must be refused");
    }
}
