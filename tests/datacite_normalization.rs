use crossref_citation_extraction::common::identifiers::{
    normalize_doi, normalize_landing_url, url_lookup_keys,
};

#[test]
fn normalizes_doi_wrappers_without_merging_versions() {
    assert_eq!(
        normalize_doi("https://doi.org/10.1234/AbC"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("doi:10.1234/data.v2"),
        Some("10.1234/data.v2".into())
    );
    assert_ne!(
        normalize_doi("10.1234/data.v1"),
        normalize_doi("10.1234/data.v2")
    );
}

#[test]
fn preserves_meaningful_url_components() {
    assert_eq!(
        normalize_landing_url("https://EXAMPLE.org:443/%7Eme?x=1#part"),
        Some("https://example.org/~me?x=1#part".into())
    );
    assert_ne!(
        normalize_landing_url("https://example.org/A"),
        normalize_landing_url("https://example.org/a")
    );
    assert_eq!(
        url_lookup_keys("example.org/A"),
        vec!["http://example.org/A", "https://example.org/A"]
    );
}

#[test]
fn accepts_dx_www_and_scheme_less_resolver_hosts() {
    assert_eq!(
        normalize_doi("http://dx.doi.org/10.1234/AbC"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("https://DX.DOI.ORG/10.1234/abc"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("https://www.doi.org/10.1234/abc"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("doi.org/10.1234/AbC"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("dx.doi.org/10.1234/abc"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("https://doi.org:443/10.1234/abc"),
        Some("10.1234/abc".into())
    );
}

#[test]
fn decodes_resolver_paths_exactly_once() {
    assert_eq!(
        normalize_doi("https://doi.org/10.1234%2FAbC"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("https://doi.org/10.1234/a%28b%29"),
        Some("10.1234/a(b)".into())
    );
    assert_eq!(
        normalize_doi("https://doi.org/10.1234/a%C3%BCb"),
        Some("10.1234/aüb".into())
    );
    // A double-encoded slash decodes to a literal `%2F`, not to a slash.
    assert_eq!(
        normalize_doi("https://doi.org/10.1234/a%252Fb"),
        Some("10.1234/a%2fb".into())
    );
    // Decoded whitespace and invalid UTF-8 are rejected, not silently kept.
    assert_eq!(normalize_doi("https://doi.org/10.1234/a%20b"), None);
    assert_eq!(normalize_doi("https://doi.org/10.1234/a%FFb"), None);
    // Bare DOIs are never percent-decoded.
    assert_eq!(normalize_doi("10.1234/a%2Fb"), Some("10.1234/a%2fb".into()));
}

#[test]
fn ignores_resolver_query_and_fragment() {
    assert_eq!(
        normalize_doi("https://doi.org/10.1234/abc?via=x&y=1#frag"),
        Some("10.1234/abc".into())
    );
    assert_eq!(
        normalize_doi("https://doi.org/10.1234/abc#"),
        Some("10.1234/abc".into())
    );
}

#[test]
fn rejects_urls_that_are_not_doi_resolvers() {
    assert_eq!(normalize_doi("https://example.org/10.1234/abc"), None);
    assert_eq!(normalize_doi("https://notdoi.org/10.1234/abc"), None);
    assert_eq!(normalize_doi("ftp://doi.org/10.1234/abc"), None);
    assert_eq!(normalize_doi("https://doi.org/"), None);
    assert_eq!(normalize_doi("https://doi.org/abc"), None);
    assert_eq!(normalize_doi("https://doi.org//10.1234/abc"), None);
    assert_eq!(normalize_doi("https://"), None);
}

#[test]
fn preserves_unicode_suffixes_and_balanced_punctuation() {
    // Only ASCII letters fold; non-ASCII suffix characters are kept verbatim.
    assert_eq!(
        normalize_doi("10.1234/Ünï(cøde)"),
        Some("10.1234/Ünï(cøde)".into())
    );
    assert_eq!(
        normalize_doi("10.1234/日本語"),
        Some("10.1234/日本語".into())
    );
    assert_eq!(
        normalize_doi("10.1002/(SICI)1097-4636(199612)33:4<231::AID-JBM1>3.0.CO;2-I"),
        Some("10.1002/(sici)1097-4636(199612)33:4<231::aid-jbm1>3.0.co;2-i".into())
    );
    assert_eq!(normalize_doi("10.1234/a(b"), Some("10.1234/a(b".into()));
    assert_eq!(normalize_doi("10.1234/abc."), Some("10.1234/abc.".into()));
    assert_eq!(
        normalize_doi("10.1234/abc/def/"),
        Some("10.1234/abc/def/".into())
    );
}

#[test]
fn rejects_empty_suffixes_and_malformed_prefixes() {
    assert_eq!(normalize_doi("10.1234/"), None);
    assert_eq!(normalize_doi("10.1234"), None);
    assert_eq!(normalize_doi("10./abc"), None);
    assert_eq!(normalize_doi("10.12a4/abc"), None);
    assert_eq!(normalize_doi("11.1234/abc"), None);
    assert_eq!(normalize_doi("doi:"), None);
    assert_eq!(normalize_doi("doi:10.1234/"), None);
    assert_eq!(normalize_doi(""), None);
    assert_eq!(normalize_doi("   "), None);
}

#[test]
fn accepts_dotted_registrant_sub_elements() {
    assert_eq!(
        normalize_doi("10.1000.10/123456"),
        Some("10.1000.10/123456".into())
    );
    assert_eq!(normalize_doi("10.1000./x"), None);
    assert_eq!(normalize_doi("10..1000/x"), None);
    assert_eq!(normalize_doi("10.1000.a/x"), None);
}

#[test]
fn rejects_whitespace_and_control_characters_inside_dois() {
    assert_eq!(normalize_doi("10.1234/ab c"), None);
    assert_eq!(normalize_doi("10.1234/ab\tc"), None);
    assert_eq!(normalize_doi("10.1234/ab\u{0}c"), None);
    assert_eq!(normalize_doi("10.1234/ab\u{7f}c"), None);
    assert_eq!(normalize_doi("10.1234/ab\u{85}c"), None);
    assert_eq!(normalize_doi("10.1234/ab\u{a0}c"), None);
    assert_eq!(normalize_doi("https://doi.org/10.1234/ab\u{1}c"), None);
    // Surrounding whitespace is trimmed; an embedded phrase is not a DOI value.
    assert_eq!(normalize_doi("  10.1234/abc\n"), Some("10.1234/abc".into()));
    assert_eq!(normalize_doi("see 10.1234/abc"), None);
}

#[test]
fn strips_doi_scheme_case_insensitively() {
    assert_eq!(normalize_doi("DOI:10.1234/ABC"), Some("10.1234/abc".into()));
    assert_eq!(
        normalize_doi("Doi: 10.1234/abc"),
        Some("10.1234/abc".into())
    );
    assert_eq!(normalize_doi("doi:doi:10.1234/abc"), None);
}

#[test]
fn rejects_multibyte_near_misses_of_resolver_hosts_without_panicking() {
    // A non-ASCII character where the host prefix check ends must not split a char.
    assert_eq!(normalize_doi("doi.oré/10.1234/abc"), None);
    assert_eq!(normalize_doi("dx.doi.or\u{e9}/10.1234/abc"), None);
    assert_eq!(normalize_doi("doi.orgé"), None);
}

#[test]
fn keeps_arxiv_version_suffixes_distinct() {
    assert_eq!(
        normalize_doi("10.48550/arXiv.2403.03542v2"),
        Some("10.48550/arxiv.2403.03542v2".into())
    );
    assert_ne!(
        normalize_doi("10.48550/arXiv.2403.03542v2"),
        normalize_doi("10.48550/arXiv.2403.03542")
    );
}

#[test]
fn keeps_explicit_ports_and_drops_only_default_ports() {
    assert_eq!(
        normalize_landing_url("https://example.org:8443/a"),
        Some("https://example.org:8443/a".into())
    );
    assert_eq!(
        normalize_landing_url("http://example.org:80/a"),
        Some("http://example.org/a".into())
    );
    assert_eq!(
        normalize_landing_url("https://example.org:80/a"),
        Some("https://example.org:80/a".into())
    );
    assert_eq!(
        normalize_landing_url("http://example.org:443/a"),
        Some("http://example.org:443/a".into())
    );
    assert_ne!(
        normalize_landing_url("https://example.org:8443/a"),
        normalize_landing_url("https://example.org/a")
    );
}

#[test]
fn preserves_query_order_and_tracking_parameters() {
    assert_ne!(
        normalize_landing_url("https://example.org/a?a=1&b=2"),
        normalize_landing_url("https://example.org/a?b=2&a=1")
    );
    assert_eq!(
        normalize_landing_url("https://example.org/a?utm_source=x&utm_medium=y"),
        Some("https://example.org/a?utm_source=x&utm_medium=y".into())
    );
    assert_ne!(
        normalize_landing_url("https://example.org/a?utm_source=x"),
        normalize_landing_url("https://example.org/a")
    );
    assert_eq!(
        normalize_landing_url("https://example.org/a?q=%7Eb"),
        Some("https://example.org/a?q=~b".into())
    );
    // An empty query carries no parameters and is not part of the key.
    assert_eq!(
        normalize_landing_url("https://example.org/a?"),
        Some("https://example.org/a".into())
    );
}

#[test]
fn preserves_nonempty_fragments_and_drops_empty_ones() {
    assert_eq!(
        normalize_landing_url("https://example.org/a#section-2"),
        Some("https://example.org/a#section-2".into())
    );
    assert_ne!(
        normalize_landing_url("https://example.org/a#section-2"),
        normalize_landing_url("https://example.org/a")
    );
    assert_eq!(
        normalize_landing_url("https://example.org/a#%7Ex"),
        Some("https://example.org/a#~x".into())
    );
    assert_eq!(
        normalize_landing_url("https://example.org/a#"),
        Some("https://example.org/a".into())
    );
}

#[test]
fn keeps_www_scheme_path_case_and_trailing_slashes() {
    assert_ne!(
        normalize_landing_url("https://www.example.org/a"),
        normalize_landing_url("https://example.org/a")
    );
    assert_eq!(
        normalize_landing_url("https://WWW.Example.ORG/Path/To"),
        Some("https://www.example.org/Path/To".into())
    );
    assert_ne!(
        normalize_landing_url("https://example.org/a/"),
        normalize_landing_url("https://example.org/a")
    );
    assert_ne!(
        normalize_landing_url("http://example.org/a"),
        normalize_landing_url("https://example.org/a")
    );
    assert_eq!(
        normalize_landing_url("https://example.org"),
        Some("https://example.org/".into())
    );
    assert_eq!(
        normalize_landing_url("HTTPS://EXAMPLE.ORG/"),
        Some("https://example.org/".into())
    );
}

#[test]
fn canonicalizes_percent_encodings_without_decoding_reserved_characters() {
    assert_eq!(
        normalize_landing_url("https://example.org/%41%2d%5f%2e%7e"),
        Some("https://example.org/A-_.~".into())
    );
    // Retained encodings use uppercase hex so `%2f` and `%2F` compare equal.
    assert_eq!(
        normalize_landing_url("https://example.org/a%2fb"),
        Some("https://example.org/a%2Fb".into())
    );
    assert_eq!(
        normalize_landing_url("https://example.org/a%20b"),
        Some("https://example.org/a%20b".into())
    );
    assert_eq!(
        normalize_landing_url("https://example.org/a b"),
        normalize_landing_url("https://example.org/a%20b")
    );
    assert_eq!(
        normalize_landing_url("https://example.org/a%3Fb"),
        Some("https://example.org/a%3Fb".into())
    );
    assert_ne!(
        normalize_landing_url("https://example.org/a%3Fb"),
        normalize_landing_url("https://example.org/a?b")
    );
    assert_eq!(
        normalize_landing_url("https://example.org/100%"),
        Some("https://example.org/100%".into())
    );
    assert_eq!(
        normalize_landing_url("https://example.org/π"),
        normalize_landing_url("https://example.org/%CF%80")
    );
}

#[test]
fn rejects_malformed_and_non_http_urls() {
    assert_eq!(normalize_landing_url("https://"), None);
    assert_eq!(normalize_landing_url("https:///"), None);
    assert_eq!(normalize_landing_url("ftp://example.org/a"), None);
    assert_eq!(normalize_landing_url("mailto:me@example.org"), None);
    assert_eq!(normalize_landing_url("example.org/a"), None);
    assert_eq!(normalize_landing_url("not a url"), None);
    assert_eq!(normalize_landing_url("https://exa mple.org/"), None);
    assert_eq!(normalize_landing_url("https://example.org:99999/"), None);
    assert_eq!(normalize_landing_url("http://[::1/"), None);
    assert_eq!(normalize_landing_url("https://user:pw@/a"), None);
    assert_eq!(normalize_landing_url(""), None);
}

#[test]
fn rejects_embedded_control_characters_in_urls() {
    // `Url::parse` would silently strip these; the key must not differ from the evidence.
    assert_eq!(normalize_landing_url("https://exa\tmple.org/a"), None);
    assert_eq!(normalize_landing_url("https://example.org/a\nb"), None);
    assert_eq!(normalize_landing_url("https://example.org/a\u{0}b"), None);
    assert_eq!(normalize_landing_url("https://example.org/a\u{7f}b"), None);
    assert_eq!(
        normalize_landing_url("  https://example.org/a\n"),
        Some("https://example.org/a".into())
    );
}

#[test]
fn lookup_keys_use_one_key_for_explicit_schemes_and_two_otherwise() {
    assert_eq!(
        url_lookup_keys("https://EXAMPLE.org/A"),
        vec!["https://example.org/A"]
    );
    assert_eq!(
        url_lookup_keys("http://example.org/A"),
        vec!["http://example.org/A"]
    );
    assert_eq!(
        url_lookup_keys("//example.org/A"),
        vec!["http://example.org/A", "https://example.org/A"]
    );
    assert_eq!(
        url_lookup_keys("example.org"),
        vec!["http://example.org/", "https://example.org/"]
    );
    assert_eq!(
        url_lookup_keys("  example.org/A?b=2&a=1#f  "),
        vec![
            "http://example.org/A?b=2&a=1#f",
            "https://example.org/A?b=2&a=1#f"
        ]
    );
}

#[test]
fn lookup_keys_keep_explicit_port_semantics_per_scheme() {
    assert_eq!(
        url_lookup_keys("example.org:443/A"),
        vec!["http://example.org:443/A", "https://example.org/A"]
    );
    assert_eq!(
        url_lookup_keys("example.org:80/A"),
        vec!["http://example.org/A", "https://example.org:80/A"]
    );
    assert_eq!(
        url_lookup_keys("example.org:8443/A"),
        vec!["http://example.org:8443/A", "https://example.org:8443/A"]
    );
}

#[test]
fn lookup_keys_are_empty_for_unusable_candidates() {
    assert!(url_lookup_keys("ftp://example.org/A").is_empty());
    assert!(url_lookup_keys("not a url").is_empty());
    assert!(url_lookup_keys("").is_empty());
    assert!(url_lookup_keys("https://").is_empty());
    assert!(url_lookup_keys("exa\tmple.org/A").is_empty());
}
