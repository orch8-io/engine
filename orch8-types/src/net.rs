//! Shared IP-address classification for outbound SSRF guards.
//!
//! Every crate that sends requests to user- or workflow-supplied URLs (engine
//! handlers, API config validation, the mobile SDK) must agree on which
//! addresses are off-limits. Keeping one classifier here — dependency-free,
//! `std::net` only — stops the per-crate blocklists from drifting apart.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// `true` if `ip` is not a publicly routable unicast address and outbound
/// requests to user-supplied URLs must never reach it.
///
/// Covers loopback, RFC 1918 private, CGNAT, link-local (incl. the
/// 169.254.169.254 cloud-metadata endpoint), "this network", IETF protocol
/// assignments, benchmarking, documentation, multicast, reserved/broadcast,
/// and — for IPv6 — ULA, link/site-local, multicast, discard, documentation,
/// Teredo, local-use NAT64, and every transition form that embeds an IPv4
/// address (mapped, compatible, translated, NAT64, 6to4), which is unwrapped
/// and classified as IPv4.
#[must_use]
pub fn is_non_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_non_public_ipv4(v4),
        IpAddr::V6(v6) => is_non_public_ipv6(v6),
    }
}

/// IPv4 half of [`is_non_public_ip`].
#[must_use]
pub fn is_non_public_ipv4(v4: Ipv4Addr) -> bool {
    let [a, b, c, _] = v4.octets();
    a == 0                                   // 0.0.0.0/8 "this network"
        || v4.is_loopback()                  // 127.0.0.0/8
        || v4.is_private()                   // 10/8, 172.16/12, 192.168/16
        || v4.is_link_local()                // 169.254.0.0/16 (metadata)
        || (a == 100 && (b & 0xc0) == 64)    // 100.64.0.0/10 CGNAT
        || (a == 192 && b == 0 && c == 0)    // 192.0.0.0/24 IETF assignments
        || (a == 192 && b == 88 && c == 99)  // 192.88.99.0/24 6to4 relay anycast
        || (a == 198 && (b & 0xfe) == 18)    // 198.18.0.0/15 benchmarking
        || v4.is_documentation()             // TEST-NET-1/2/3
        || v4.is_multicast()                 // 224.0.0.0/4
        || a >= 240 // 240.0.0.0/4 reserved, incl. 255.255.255.255 broadcast
}

/// IPv6 half of [`is_non_public_ip`].
#[must_use]
pub fn is_non_public_ipv6(v6: Ipv6Addr) -> bool {
    if let Some(v4) = embedded_ipv4(v6) {
        return is_non_public_ipv4(v4);
    }
    let s = v6.segments();
    // `::/96` covers `::`, `::1` and the deprecated IPv4-compatible form
    // `::a.b.c.d` — none of which is a legitimate public destination.
    (s[0..6] == [0; 6])
        || v6.is_multicast()                              // ff00::/8
        || (s[0] & 0xfe00) == 0xfc00                      // fc00::/7 ULA
        || (s[0] & 0xffc0) == 0xfe80                      // fe80::/10 link-local
        || (s[0] & 0xffc0) == 0xfec0                      // fec0::/10 site-local (deprecated)
        || (s[0] == 0x0100 && s[1..4] == [0; 3])          // 100::/64 discard
        || (s[0] == 0x2001 && s[1] == 0x0db8)             // 2001:db8::/32 documentation
        || (s[0] == 0x2001 && s[1] == 0)                  // 2001::/32 Teredo
        || (s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 1) // 64:ff9b:1::/48 local-use NAT64
}

/// Extract an IPv4 address embedded by a v4-in-v6 transition mechanism that
/// would route to that IPv4 host: v4-mapped `::ffff:a.b.c.d`, SIIT-translated
/// `::ffff:0:a.b.c.d`, well-known NAT64 `64:ff9b::a.b.c.d`, and 6to4
/// `2002:AABB:CCDD::/48`.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = v6.segments();
    let o = v6.octets();
    let tail = Ipv4Addr::new(o[12], o[13], o[14], o[15]);
    if s[0..5] == [0; 5] && s[5] == 0xffff {
        return Some(tail); // ::ffff:0:0/96 mapped
    }
    if s[0..4] == [0; 4] && s[4] == 0xffff && s[5] == 0 {
        return Some(tail); // ::ffff:0:0:0/96 translated
    }
    if s[0] == 0x0064 && s[1] == 0xff9b && s[2..6] == [0; 4] {
        return Some(tail); // 64:ff9b::/96 NAT64
    }
    if s[0] == 0x2002 {
        return Some(Ipv4Addr::new(o[2], o[3], o[4], o[5])); // 2002::/16 6to4
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocked(s: &str) -> bool {
        is_non_public_ip(s.parse().expect("ip literal"))
    }

    #[test]
    fn blocks_non_public_ipv4_ranges() {
        for ip in [
            "0.0.0.0",
            "0.1.2.3",
            "10.0.0.1",
            "100.64.0.1",
            "100.127.255.254",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.0.0.170",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.255.254",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(blocked(ip), "{ip} must be blocked");
        }
    }

    #[test]
    fn allows_public_ipv4() {
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "100.128.0.1",
            "198.20.0.1",
            "93.184.216.34",
        ] {
            assert!(!blocked(ip), "{ip} must be allowed");
        }
    }

    #[test]
    fn blocks_non_public_ipv6_ranges() {
        for ip in [
            "::",
            "::1",
            "::127.0.0.1",
            "::8.8.8.8",
            "::ffff:127.0.0.1",
            "::ffff:a9fe:a9fe",
            "::ffff:0:10.0.0.1",
            "64:ff9b::7f00:1",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::1",
            "2002:7f00:1::1",
            "2002:a9fe:a9fe::",
            "2001::1",
            "2001:db8::1",
            "100::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
        ] {
            assert!(blocked(ip), "{ip} must be blocked");
        }
    }

    #[test]
    fn allows_public_ipv6_and_public_embedded_v4() {
        for ip in [
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
        ] {
            assert!(!blocked(ip), "{ip} must be allowed");
        }
    }
}
