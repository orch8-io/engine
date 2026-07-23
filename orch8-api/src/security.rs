//! Security helpers shared by API handlers.

use std::net::IpAddr;

/// Rejects URLs that target loopback, private, link-local, unspecified, or
/// multicast addresses. This is a synchronous, DNS-free guard suitable for
/// API-layer validation of user-supplied URLs (e.g. `webhook_url`, `push_url`).
///
/// Hostnames that are not literal IPs are allowed through — the engine's
/// outbound HTTP clients run an async DNS-based SSRF check before calling
/// them. This helper still catches the common direct-IP and metadata-service
/// vectors at configuration time.
pub fn validate_public_url(url: &str) -> Result<(), ApiUrlValidationError> {
    let parsed = url::Url::parse(url).map_err(ApiUrlValidationError::Parse)?;

    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(ApiUrlValidationError::Scheme(other.to_string()));
        }
    }

    let host = parsed
        .host_str()
        .ok_or(ApiUrlValidationError::MissingHost)?
        .trim()
        .to_lowercase();
    if host.is_empty() {
        return Err(ApiUrlValidationError::MissingHost);
    }

    // Reject the most common metadata-service endpoint even when written as a
    // hostname. Many cloud metadata APIs answer on link-local IPs; blocking the
    // literal hostname closes one more bypass vector at config time.
    if host == "169.254.169.254"
        || host.ends_with(".internal")
            && (host.starts_with("metadata.") || host.starts_with("instance-metadata."))
    {
        return Err(ApiUrlValidationError::InternalAddress(host));
    }

    // If the host is a literal IP, classify it. `host_str` keeps the
    // brackets around IPv6 literals ("[::1]"), so strip them before parsing —
    // otherwise every literal IPv6 target would skip classification entirely.
    let ip_str = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host.as_str());
    if let Ok(ip) = ip_str.parse::<IpAddr>()
        && is_non_public_ip(ip)
    {
        return Err(ApiUrlValidationError::InternalAddress(host));
    }

    Ok(())
}

fn is_non_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.is_documentation()
                // 100.64.0.0/10 CGNAT space is not publicly routable.
                || v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64
                // 192.0.0.0/24 contains IANA reserved addresses including
                // 192.0.0.170/171 (NAT64/DNS64 well-known prefixes).
                || v4.octets()[0] == 192 && v4.octets()[1] == 0 && v4.octets()[2] == 0
        }
        IpAddr::V6(v6) => {
            // IPv4-mapped IPv6 (`::ffff:a.b.c.d`) must pass the same checks
            // as the literal V4 form — otherwise `::ffff:127.0.0.1` or
            // `::ffff:a9fe:a9fe` (169.254.169.254) bypass the guard.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_non_public_ip(IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // Unique local addresses (fc00::/7).
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // Link-local unicast (fe80::/10).
                || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApiUrlValidationError {
    #[error("not a valid URL: {0}")]
    Parse(#[from] url::ParseError),
    #[error("URL scheme must be http or https, got {0}")]
    Scheme(String),
    #[error("URL must have a host")]
    MissingHost,
    #[error("URL targets an internal or non-public address: {0}")]
    InternalAddress(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_public_https() {
        assert!(validate_public_url("https://example.com/webhook").is_ok());
    }

    #[test]
    fn rejects_non_http_scheme() {
        assert!(matches!(
            validate_public_url("ftp://example.com"),
            Err(ApiUrlValidationError::Scheme(_))
        ));
    }

    #[test]
    fn rejects_loopback() {
        assert!(matches!(
            validate_public_url("http://127.0.0.1/hook"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
    }

    #[test]
    fn rejects_metadata_ip() {
        assert!(matches!(
            validate_public_url("http://169.254.169.254/latest/meta-data/"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
    }

    #[test]
    fn rejects_private_range() {
        assert!(matches!(
            validate_public_url("http://10.0.0.1/hook"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
    }

    #[test]
    fn rejects_missing_host() {
        // `http://` / `http:///` produce an empty host at parse time.
        assert!(matches!(
            validate_public_url("http://"),
            Err(ApiUrlValidationError::Parse(_))
        ));
        assert!(matches!(
            validate_public_url("http:///"),
            Err(ApiUrlValidationError::Parse(_))
        ));
    }

    #[test]
    fn rejects_ipv4_mapped_ipv6_loopback_and_metadata() {
        // Literal IPv6 loopback (brackets included by `host_str`).
        assert!(matches!(
            validate_public_url("http://[::1]/hook"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
        assert!(matches!(
            validate_public_url("http://[::ffff:127.0.0.1]/hook"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
        // ::ffff:a9fe:a9fe == 169.254.169.254 (cloud metadata endpoint).
        assert!(matches!(
            validate_public_url("http://[::ffff:a9fe:a9fe]/latest/meta-data/"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
        // ::ffff:10.0.0.1 — private range in mapped form.
        assert!(matches!(
            validate_public_url("http://[::ffff:10.0.0.1]/hook"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
    }

    #[test]
    fn rejects_cgnat_shared_space() {
        assert!(matches!(
            validate_public_url("http://100.64.0.1/hook"),
            Err(ApiUrlValidationError::InternalAddress(_))
        ));
    }

    #[test]
    fn allows_public_ipv6_literal() {
        // Cloudflare DNS — a public global-unicast V6 literal must pass.
        assert!(validate_public_url("https://[2606:4700:4700::1111]/hook").is_ok());
    }

    #[test]
    fn allows_public_hostname() {
        assert!(validate_public_url("https://hooks.example.com/path").is_ok());
    }
}
