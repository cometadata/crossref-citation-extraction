//! Shared DOI and landing-page URL normalization.
//!
//! This module is the single authoritative normalization used by both
//! DataCite index construction and Crossref reference matching, so that a
//! registry value and an extracted candidate compare equal exactly when they
//! identify the same DOI or landing page.
//!
//! It never removes arXiv version suffixes; only `crate::extract::arxiv`
//! applies arXiv version rules. General DOI version suffixes such as
//! `10.1234/data.v1` and `10.1234/data.v2` identify distinct DOIs.

use std::borrow::Cow;

use url::Url;

/// Normalize a complete DOI value to its canonical lowercase
/// `10.<registrant>/<suffix>` form.
///
/// Accepts bare DOIs, `doi:` wrappers, and DOI resolver URLs (`doi.org`,
/// `dx.doi.org`, `www.doi.org`, with or without an explicit scheme).
/// Resolver URLs are parsed as URLs: the DOI is the resolver path,
/// percent-decoded exactly once, and the query and fragment are not part of
/// it. Bare DOIs are taken verbatim and never percent-decoded.
///
/// The value is treated as one complete identifier, not scanned as text:
/// surrounding whitespace is trimmed, but any remaining whitespace or
/// control character makes the value invalid. Only ASCII letters are
/// case-folded, following the DOI handbook's ASCII case-insensitivity;
/// non-ASCII suffix characters are preserved verbatim.
///
/// Returns `None` when the value is not a single, syntactically valid DOI.
pub fn normalize_doi(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.chars().any(char::is_control) {
        return None;
    }
    let unwrapped = if is_resolver_reference(raw) {
        Cow::Owned(resolver_doi_path(raw)?)
    } else {
        Cow::Borrowed(raw)
    };
    let doi = strip_doi_scheme(&unwrapped).to_ascii_lowercase();
    is_valid_doi(&doi).then_some(doi)
}

/// Canonicalize an explicit HTTP or HTTPS landing-page URL for exact lookup.
///
/// Normalizes scheme and host case, drops default ports, decodes
/// percent-encoded unreserved characters, and uppercases the hex digits of
/// the encodings that remain. Preserves the explicit scheme, explicit
/// non-default ports, path case, non-root trailing slashes, query parameters
/// and their order, and nonempty fragments; `www` and tracking parameters
/// are never stripped. An empty path becomes `/`; an empty query or fragment
/// (`?` or `#` with nothing after it) is dropped.
///
/// Surrounding whitespace is trimmed, but an embedded control character
/// makes the value invalid: `Url::parse` would silently strip tabs and
/// newlines, and the key must not diverge from the evidence. Returns `None`
/// for anything that is not a valid HTTP/HTTPS URL with a host.
pub fn normalize_landing_url(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.chars().any(char::is_control) {
        return None;
    }
    let mut url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    url.host_str().filter(|host| !host.is_empty())?;

    let path = canonicalize_percent_encoding(url.path());
    url.set_path(&path);
    let query = url.query().map(canonicalize_percent_encoding);
    url.set_query(query.as_deref().filter(|query| !query.is_empty()));
    let fragment = url.fragment().map(canonicalize_percent_encoding);
    url.set_fragment(fragment.as_deref().filter(|fragment| !fragment.is_empty()));
    Some(url.to_string())
}

/// Produce the exact lookup keys for a URL candidate.
///
/// A value with an explicit scheme yields at most its one normalized key. A
/// scheme-less host/path reference is checked as both `http://` and
/// `https://`, parsed separately so an explicit port keeps its meaning
/// under each scheme. The caller decides whether a scheme-less match is
/// ambiguous.
pub fn url_lookup_keys(value: &str) -> Vec<String> {
    let raw = value.trim();
    if raw.contains("://") {
        return normalize_landing_url(raw).into_iter().collect();
    }
    let raw = raw.strip_prefix("//").unwrap_or(raw);
    ["http://", "https://"]
        .into_iter()
        .filter_map(|scheme| normalize_landing_url(&format!("{scheme}{raw}")))
        .collect()
}

/// Hosts of the DOI resolver proxy, matched after `Url` lowercases the host.
const RESOLVER_HOSTS: [&str; 3] = ["doi.org", "dx.doi.org", "www.doi.org"];

/// Whether a value should be parsed as a resolver URL rather than as a bare
/// DOI: anything with an explicit scheme, or a scheme-less reference that
/// starts with a resolver host. A bare DOI (`10.…`) is never reparsed, so a
/// suffix that happens to contain `://` stays intact.
fn is_resolver_reference(raw: &str) -> bool {
    if raw.starts_with("10.") {
        return false;
    }
    // Compare bytes so a multibyte character at the prefix boundary cannot
    // split a char; ASCII hosts never match inside a multibyte sequence.
    let bytes = raw.as_bytes();
    raw.contains("://")
        || RESOLVER_HOSTS.iter().any(|host| {
            bytes
                .get(..host.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(host.as_bytes()))
                && bytes.get(host.len()) == Some(&b'/')
        })
}

/// Extract the percent-decoded DOI path from a resolver URL, or `None` if
/// the value is not an HTTP/HTTPS URL on a resolver host.
fn resolver_doi_path(raw: &str) -> Option<String> {
    let url = if raw.contains("://") {
        Url::parse(raw)
    } else {
        Url::parse(&format!("https://{raw}"))
    }
    .ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    if !RESOLVER_HOSTS.contains(&url.host_str()?) {
        return None;
    }
    let path = url.path().strip_prefix('/').unwrap_or(url.path());
    percent_decode(path)
}

/// Remove a leading `doi:` scheme, case-insensitively, and any whitespace
/// that followed it (`DOI: 10.…`).
fn strip_doi_scheme(value: &str) -> &str {
    value
        .get(..4)
        .filter(|prefix| prefix.eq_ignore_ascii_case("doi:"))
        .map_or(value, |_| &value[4..])
        .trim()
}

/// Check the canonical `10.<registrant>/<suffix>` shape.
///
/// The registrant code is one or more digits, optionally divided into
/// dot-separated numeric sub-elements (`10.1000.10/…`), which the DOI
/// handbook permits "for administrative convenience". There is no allowlist
/// of prefixes. The suffix must be nonempty and free of whitespace and
/// control characters; no other suffix characters are restricted.
fn is_valid_doi(doi: &str) -> bool {
    let Some(rest) = doi.strip_prefix("10.") else {
        return false;
    };
    let Some((registrant, suffix)) = rest.split_once('/') else {
        return false;
    };
    let registrant_ok = registrant
        .split('.')
        .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()));
    let suffix_ok =
        !suffix.is_empty() && !suffix.chars().any(|c| c.is_whitespace() || c.is_control());
    registrant_ok && suffix_ok
}

/// Decode every valid `%XX` sequence exactly once. Malformed sequences are
/// kept verbatim; a result that is not valid UTF-8 yields `None`.
fn percent_decode(component: &str) -> Option<String> {
    let bytes = component.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if let Some(decoded) = hex_pair(bytes, i + 1) {
                out.push(decoded);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).ok()
}

/// Decode percent-encoded RFC 3986 unreserved characters
/// (`ALPHA / DIGIT / "-" / "." / "_" / "~"`) and uppercase the hex digits of
/// every other valid `%XX` sequence, so equivalent encodings compare equal
/// (RFC 3986 section 6.2.2). Reserved characters stay encoded because
/// decoding them would change the URL's structure. Malformed `%` sequences
/// are kept verbatim.
fn canonicalize_percent_encoding(component: &str) -> String {
    let bytes = component.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if let Some(decoded) = hex_pair(bytes, i + 1) {
                if is_unreserved(decoded) {
                    out.push(decoded);
                } else {
                    out.push(b'%');
                    out.push(bytes[i + 1].to_ascii_uppercase());
                    out.push(bytes[i + 2].to_ascii_uppercase());
                }
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    // Only ASCII bytes were substituted, so the input's UTF-8 validity is preserved.
    String::from_utf8(out).unwrap_or_else(|_| component.to_string())
}

/// Decode the two hex digits starting at `at`, if both are present and valid.
fn hex_pair(bytes: &[u8], at: usize) -> Option<u8> {
    let hex = |b: &u8| (*b as char).to_digit(16).map(|d| d as u8);
    let high = hex(bytes.get(at)?)?;
    let low = hex(bytes.get(at + 1)?)?;
    Some((high << 4) | low)
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}
