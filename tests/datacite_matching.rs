//! Integration tests for general DataCite reference matching: membership,
//! shared-URL ambiguity, evidence selection, and the diagnostic counters,
//! all exercised through `match_reference` against an in-memory index.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::ensure;
use crossref_citation_extraction::common::MatchType;
use crossref_citation_extraction::extract::{match_reference, Provenance, ReferenceMatches};
use crossref_citation_extraction::index::datacite::TargetLookup;
use serde_json::{json, Value};
use url::Url;

/// An in-memory registry with the same contract as the memory-mapped
/// bundle: exact canonical DOI membership, exact hosts of the registered
/// landing pages, and sorted distinct DOIs per landing-page key, subject
/// to the same allocation budget as `DataciteIndex::url_dois`.
struct MemoryIndex {
    dois: BTreeSet<String>,
    urls: BTreeMap<String, Vec<String>>,
}

impl TargetLookup for MemoryIndex {
    fn contains_doi(&self, doi: &str) -> bool {
        self.dois.contains(doi)
    }

    fn contains_host(&self, host: &str) -> bool {
        self.urls.keys().any(|key| {
            Url::parse(key)
                .ok()
                .and_then(|url| url.host_str().map(|h| h == host))
                .unwrap_or(false)
        })
    }

    fn url_dois(&self, key: &str, max_bytes: usize) -> anyhow::Result<Vec<String>> {
        let dois = self.urls.get(key).cloned().unwrap_or_default();
        let needed = dois.len() * std::mem::size_of::<String>()
            + dois.iter().map(String::len).sum::<usize>();
        ensure!(needed <= max_bytes, "URL candidate memory budget exceeded");
        Ok(dois)
    }
}

#[test]
fn a_doi_disambiguates_only_its_own_shared_url() -> anyhow::Result<()> {
    let index = MemoryIndex {
        dois: ["10.1234/a", "10.1234/b", "10.1234/c"]
            .map(String::from)
            .into(),
        urls: [(
            "https://example.org/shared".into(),
            vec!["10.1234/a".into(), "10.1234/b".into()],
        )]
        .into(),
    };
    let ambiguous = match_reference(
        &serde_json::json!({"URL":"https://example.org/shared"}),
        &index,
        4096,
    )?;
    assert!(ambiguous.confirmed.is_empty());
    assert_eq!(ambiguous.ambiguous.len(), 1);
    let resolved = match_reference(
        &serde_json::json!({"DOI":"10.1234/a","doi-asserted-by":"publisher","URL":"https://example.org/shared"}),
        &index,
        4096,
    )?;
    assert_eq!(resolved.confirmed.len(), 1);
    assert_eq!(resolved.confirmed[0].provenance, Provenance::Publisher);
    assert!(resolved.ambiguous.is_empty());
    let unrelated = match_reference(
        &serde_json::json!({"DOI":"10.1234/c","URL":"https://example.org/shared"}),
        &index,
        4096,
    )?;
    assert_eq!(unrelated.confirmed.len(), 1);
    assert_eq!(unrelated.ambiguous.len(), 1);
    Ok(())
}

/// Delegates to a `MemoryIndex` while counting landing-page lookups.
struct CountingIndex<'a> {
    inner: &'a MemoryIndex,
    url_lookups: AtomicUsize,
}

impl TargetLookup for CountingIndex<'_> {
    fn contains_doi(&self, doi: &str) -> bool {
        self.inner.contains_doi(doi)
    }

    fn contains_host(&self, host: &str) -> bool {
        self.inner.contains_host(host)
    }

    fn url_dois(&self, key: &str, max_bytes: usize) -> anyhow::Result<Vec<String>> {
        self.url_lookups.fetch_add(1, Ordering::SeqCst);
        self.inner.url_dois(key, max_bytes)
    }
}

fn index(dois: &[&str], urls: &[(&str, &[&str])]) -> MemoryIndex {
    MemoryIndex {
        dois: dois.iter().map(|d| d.to_string()).collect(),
        urls: urls
            .iter()
            .map(|(url, dois)| {
                (
                    url.to_string(),
                    dois.iter().map(|d| d.to_string()).collect(),
                )
            })
            .collect(),
    }
}

fn run(index: &dyn TargetLookup, reference: Value) -> ReferenceMatches {
    match_reference(&reference, index, 4096).expect("reference matches")
}

/// `(doi, provenance, match_type, raw_match)` of every confirmed match.
fn confirmed(matches: &ReferenceMatches) -> Vec<(&str, Provenance, MatchType, &str)> {
    matches
        .confirmed
        .iter()
        .map(|m| {
            (
                m.doi.as_str(),
                m.provenance,
                m.match_type,
                m.raw_match.as_str(),
            )
        })
        .collect()
}

fn counters(matches: &ReferenceMatches) -> (u64, u64, u64, usize) {
    (
        matches.unmatched_dois,
        matches.unmatched_urls,
        matches.malformed_fields,
        matches.failed_arxiv.len(),
    )
}

#[test]
fn lookup_budget_errors_propagate() {
    let index = index(
        &["10.1234/a", "10.1234/b"],
        &[("https://example.org/shared", &["10.1234/a", "10.1234/b"])],
    );
    let error =
        match_reference(&json!({"URL":"https://example.org/shared"}), &index, 1).unwrap_err();
    assert!(format!("{error:#}").contains("budget"), "{error:#}");
    // A reference without landing-page lookups never spends the budget.
    let ok = match_reference(&json!({"DOI":"10.1234/a"}), &index, 0).unwrap();
    assert_eq!(ok.confirmed.len(), 1);
}

#[test]
fn multiple_doi_targets_in_one_reference() {
    let index = index(&["10.1234/a", "10.1234/b", "10.1234/c"], &[]);
    let matches = run(
        &index,
        json!({
            "DOI": "10.1234/a", "doi-asserted-by": "crossref",
            "unstructured": "Data: https://doi.org/10.1234/b and doi:10.1234/c and 10.1234/zz elsewhere"
        }),
    );
    assert_eq!(
        confirmed(&matches),
        vec![
            (
                "10.1234/a",
                Provenance::Crossref,
                MatchType::Doi,
                "10.1234/a"
            ),
            (
                "10.1234/b",
                Provenance::Mined,
                MatchType::Doi,
                "https://doi.org/10.1234/b"
            ),
            ("10.1234/c", Provenance::Mined, MatchType::Doi, "10.1234/c"),
        ]
    );
    assert_eq!(counters(&matches), (1, 0, 0, 0));

    // Unmatched DOIs are counted once per distinct canonical DOI.
    let repeated = run(
        &index,
        json!({"DOI":"10.9/x","URL":"https://doi.org/10.9/X","unstructured":"10.9/x, 10.9/y."}),
    );
    assert!(repeated.confirmed.is_empty());
    assert_eq!(counters(&repeated), (2, 0, 0, 0));

    // A resolver URL owns its DOI: one miss, not a URL miss plus a bare DOI miss.
    let resolver = run(
        &index,
        json!({"unstructured":"https://doi.org/10.9/zz%2Fq."}),
    );
    assert!(resolver.confirmed.is_empty());
    assert_eq!(counters(&resolver), (1, 0, 0, 0));
}

#[test]
fn two_identifying_members_keep_a_shared_url_ambiguous() {
    let index = index(
        &["10.1234/a", "10.1234/b"],
        &[("https://example.org/shared", &["10.1234/a", "10.1234/b"])],
    );
    let matches = run(
        &index,
        json!({
            "DOI": "10.1234/a", "doi-asserted-by": "publisher",
            "URL": "https://example.org/shared",
            "unstructured": "See also 10.1234/b via https://example.org/shared"
        }),
    );
    assert_eq!(
        confirmed(&matches),
        vec![
            (
                "10.1234/a",
                Provenance::Publisher,
                MatchType::Doi,
                "10.1234/a"
            ),
            ("10.1234/b", Provenance::Mined, MatchType::Doi, "10.1234/b"),
        ]
    );
    assert_eq!(matches.ambiguous.len(), 1);
    assert_eq!(matches.ambiguous[0].url, "https://example.org/shared");
    assert_eq!(matches.ambiguous[0].raw_match, "https://example.org/shared");
    assert_eq!(
        matches.ambiguous[0].candidate_dois,
        vec!["10.1234/a", "10.1234/b"]
    );
    assert_eq!(counters(&matches), (0, 0, 0, 0));
}

#[test]
fn scheme_less_urls_are_ambiguous_across_schemes() {
    let index = index(
        &["10.1234/a", "10.1234/b", "10.1234/c"],
        &[
            ("http://example.org/p", &["10.1234/a"]),
            ("https://example.org/p", &["10.1234/b"]),
            ("https://example.org/q", &["10.1234/c"]),
        ],
    );
    let shared = run(&index, json!({"unstructured":"Available at example.org/p"}));
    assert!(shared.confirmed.is_empty());
    assert_eq!(shared.ambiguous.len(), 1);
    assert_eq!(shared.ambiguous[0].url, "http://example.org/p");
    assert_eq!(shared.ambiguous[0].raw_match, "example.org/p");
    assert_eq!(
        shared.ambiguous[0].candidate_dois,
        vec!["10.1234/a", "10.1234/b"]
    );

    let https = run(&index, json!({"URL":"https://example.org/p"}));
    assert_eq!(
        confirmed(&https),
        vec![(
            "10.1234/b",
            Provenance::Mined,
            MatchType::Url,
            "https://example.org/p"
        )]
    );
    let http = run(&index, json!({"URL":"HTTP://Example.org:80/p"}));
    assert_eq!(
        confirmed(&http),
        vec![(
            "10.1234/a",
            Provenance::Mined,
            MatchType::Url,
            "HTTP://Example.org:80/p"
        )]
    );

    let settled = run(
        &index,
        json!({"DOI":"10.1234/b","unstructured":"example.org/p"}),
    );
    assert_eq!(
        confirmed(&settled),
        vec![("10.1234/b", Provenance::Mined, MatchType::Doi, "10.1234/b")]
    );
    assert!(settled.ambiguous.is_empty());

    // Only one scheme registered: the scheme-less reference resolves to it.
    let single = run(&index, json!({"unstructured":"Dataset: example.org/q."}));
    assert_eq!(
        confirmed(&single),
        vec![(
            "10.1234/c",
            Provenance::Mined,
            MatchType::Url,
            "example.org/q"
        )]
    );
    assert_eq!(counters(&single), (0, 0, 0, 0));
}

#[test]
fn path_prefixes_and_suffix_shortening_never_match() {
    let index = index(
        &["10.1234/ab"],
        &[("https://example.org/data/1", &["10.1234/ab"])],
    );
    for url in [
        "https://example.org/data/12",
        "https://example.org/data/1/",
        "https://example.org/data",
        "https://example.org/",
        "https://other.org/data/1",
        "http://example.org/data/1",
    ] {
        let matches = run(&index, json!({"URL": url}));
        assert!(matches.confirmed.is_empty(), "{url}");
        assert!(matches.ambiguous.is_empty(), "{url}");
        assert_eq!(counters(&matches), (0, 1, 0, 0), "{url}");
    }
    for doi in ["10.1234/abc", "10.1234/ab2", "10.1234/a", "10.12345/ab"] {
        let matches = run(
            &index,
            json!({"unstructured": format!("see {doi} for data")}),
        );
        assert!(matches.confirmed.is_empty(), "{doi}");
        assert_eq!(counters(&matches), (1, 0, 0, 0), "{doi}");
    }
}

#[test]
fn trailing_punctuation_is_removed_only_through_registered_variants() {
    let index = index(
        &["10.1234/a", "10.1234/b(1)", "10.1234/c."],
        &[
            ("https://example.org/x", &["10.1234/a"]),
            ("https://example.org/y(1)", &["10.1234/b(1)"]),
        ],
    );
    let cases = [
        ("(10.1234/a).", ("10.1234/a", MatchType::Doi, "10.1234/a")),
        ("\"10.1234/a\",", ("10.1234/a", MatchType::Doi, "10.1234/a")),
        (
            "see 10.1234/b(1).",
            ("10.1234/b(1)", MatchType::Doi, "10.1234/b(1)"),
        ),
        (
            "see 10.1234/c.,",
            ("10.1234/c.", MatchType::Doi, "10.1234/c."),
        ),
        ("10.1234/a.,;:", ("10.1234/a", MatchType::Doi, "10.1234/a")),
        (
            "(https://example.org/x).",
            ("10.1234/a", MatchType::Url, "https://example.org/x"),
        ),
        (
            "<https://example.org/y(1)>",
            ("10.1234/b(1)", MatchType::Url, "https://example.org/y(1)"),
        ),
        (
            "[https://doi.org/10.1234/a];",
            ("10.1234/a", MatchType::Doi, "https://doi.org/10.1234/a"),
        ),
    ];
    for (text, (doi, match_type, raw)) in cases {
        let matches = run(&index, json!({"unstructured": text}));
        assert_eq!(
            confirmed(&matches),
            vec![(doi, Provenance::Mined, match_type, raw)],
            "{text}"
        );
        assert_eq!(counters(&matches), (0, 0, 0, 0), "{text}");
    }
    // Variants are bounded: five trailing characters are too many.
    let bounded = run(&index, json!({"unstructured":"10.1234/a.,;:)"}));
    assert!(bounded.confirmed.is_empty());
    assert_eq!(counters(&bounded), (1, 0, 0, 0));
}

#[test]
fn encoded_resolver_urls_are_doi_evidence() {
    let index = index(&["10.1234/a/b", "10.1234/abc"], &[]);
    let cases = [
        (
            json!({"URL":"https://doi.org/10.1234/a%2Fb"}),
            (
                "10.1234/a/b",
                Provenance::Mined,
                "https://doi.org/10.1234/a%2Fb",
            ),
        ),
        (
            json!({"unstructured":"http://dx.doi.org/10.1234/ABC."}),
            (
                "10.1234/abc",
                Provenance::Mined,
                "http://dx.doi.org/10.1234/ABC",
            ),
        ),
        (
            json!({"unstructured":"doi.org/10.1234/abc"}),
            ("10.1234/abc", Provenance::Mined, "doi.org/10.1234/abc"),
        ),
        (
            json!({"DOI":"https://doi.org/10.1234/ABC","doi-asserted-by":"Publisher"}),
            (
                "10.1234/abc",
                Provenance::Publisher,
                "https://doi.org/10.1234/ABC",
            ),
        ),
        (
            json!({"DOI":"doi:10.1234/ABC","doi-asserted-by":"CROSSREF"}),
            ("10.1234/abc", Provenance::Crossref, "doi:10.1234/ABC"),
        ),
        (
            json!({"DOI":"10.1234/abc","doi-asserted-by":"unknown"}),
            ("10.1234/abc", Provenance::Mined, "10.1234/abc"),
        ),
        (
            json!({"DOI":"10.1234/abc","doi-asserted-by":5}),
            ("10.1234/abc", Provenance::Mined, "10.1234/abc"),
        ),
    ];
    for (reference, (doi, provenance, raw)) in cases {
        let matches = run(&index, reference.clone());
        assert_eq!(
            confirmed(&matches),
            vec![(doi, provenance, MatchType::Doi, raw)],
            "{reference}"
        );
        assert_eq!(counters(&matches), (0, 0, 0, 0), "{reference}");
    }
}

#[test]
fn html_entities_decode_without_changing_raw_evidence() {
    let index = index(
        &["10.1234/a&b", "10.1234/q"],
        &[("https://example.org/q?a=1&b=2", &["10.1234/q"])],
    );
    let cases = [
        (
            json!({"unstructured":"Available at https://example.org/q?a=1&amp;b=2."}),
            (
                "10.1234/q",
                MatchType::Url,
                "https://example.org/q?a=1&amp;b=2",
            ),
        ),
        (
            json!({"unstructured":"https://example.org/q?a=1&#38;b=2"}),
            (
                "10.1234/q",
                MatchType::Url,
                "https://example.org/q?a=1&#38;b=2",
            ),
        ),
        (
            json!({"URL":"https://example.org/q?a=1&amp;b=2"}),
            (
                "10.1234/q",
                MatchType::Url,
                "https://example.org/q?a=1&amp;b=2",
            ),
        ),
        (
            json!({"unstructured":"doi 10.1234/a&amp;b."}),
            ("10.1234/a&b", MatchType::Doi, "10.1234/a&amp;b"),
        ),
        (
            json!({"unstructured":"&lt;10.1234/a&#x26;b&gt;"}),
            ("10.1234/a&b", MatchType::Doi, "10.1234/a&#x26;b"),
        ),
    ];
    for (reference, (doi, match_type, raw)) in cases {
        let matches = run(&index, reference.clone());
        assert_eq!(
            confirmed(&matches),
            vec![(doi, Provenance::Mined, match_type, raw)],
            "{reference}"
        );
        assert_eq!(counters(&matches), (0, 0, 0, 0), "{reference}");
    }
}

#[test]
fn repeated_url_evidence_is_reported_once_and_looked_up_once() {
    let inner = index(
        &["10.1234/a", "10.1234/b", "10.1234/c"],
        &[
            ("https://example.org/shared", &["10.1234/a", "10.1234/b"]),
            ("https://example.org/c", &["10.1234/c"]),
        ],
    );
    let index = CountingIndex {
        inner: &inner,
        url_lookups: AtomicUsize::new(0),
    };
    let matches = run(
        &index,
        json!({
            "URL": "https://example.org/shared",
            "unstructured": "See https://example.org/shared and https://example.org/shared. Also https://example.org/c and https://example.org/c"
        }),
    );
    assert_eq!(
        confirmed(&matches),
        vec![(
            "10.1234/c",
            Provenance::Mined,
            MatchType::Url,
            "https://example.org/c"
        )]
    );
    assert_eq!(matches.ambiguous.len(), 1);
    assert_eq!(matches.ambiguous[0].raw_match, "https://example.org/shared");
    assert_eq!(
        matches.ambiguous[0].candidate_dois,
        vec!["10.1234/a", "10.1234/b"]
    );
    // Three distinct keys were probed (`shared`, `shared.`, `c`); every repeat hit the memo.
    assert_eq!(index.url_lookups.load(Ordering::SeqCst), 3);
}

#[test]
fn legacy_arxiv_patterns_are_confirmed_as_arxiv_evidence() {
    let index = index(
        &[
            "10.48550/arxiv.2403.03542",
            "10.48550/arxiv.hep-ph/9901234",
            "10.48550/arxiv.cs.dm/9910013",
            "10.48550/arxiv.2206.153252",
        ],
        &[(
            "https://arxiv.org/abs/2403.03542",
            &["10.48550/arxiv.2403.03542"],
        )],
    );
    let modern = "10.48550/arxiv.2403.03542";
    let cases = [
        ("arXiv:2403.03542", modern, "arXiv:2403.03542"),
        ("arXiv:2403.03542v2", modern, "arXiv:2403.03542v2"),
        (
            "arXiv:hep-ph/9901234",
            "10.48550/arxiv.hep-ph/9901234",
            "arXiv:hep-ph/9901234",
        ),
        (
            "arXiv:cs.DM/9910013",
            "10.48550/arxiv.cs.dm/9910013",
            "arXiv:cs.DM/9910013",
        ),
        (
            "arXiv:cs.DM/ 9910013",
            "10.48550/arxiv.cs.dm/9910013",
            "arXiv:cs.DM/ 9910013",
        ),
        (
            "ArXiv. 2206.153252",
            "10.48550/arxiv.2206.153252",
            "ArXiv. 2206.153252",
        ),
        (
            "10.48550/arXiv.2403.03542",
            modern,
            "10.48550/arXiv.2403.03542",
        ),
        (
            "https://arxiv.org/abs/2403.03542",
            modern,
            "arxiv.org/abs/2403.03542",
        ),
        // The versioned landing page is not registered; the arXiv parser owns the text.
        (
            "https://arxiv.org/pdf/2403.03542v2.",
            modern,
            "arxiv.org/pdf/2403.03542v2",
        ),
    ];
    for (text, doi, raw) in cases {
        let matches = run(&index, json!({"unstructured": text}));
        assert_eq!(
            confirmed(&matches),
            vec![(doi, Provenance::Mined, MatchType::Arxiv, raw)],
            "{text}"
        );
        assert_eq!(counters(&matches), (0, 0, 0, 0), "{text}");
    }

    // A structured arXiv DOI is structured evidence: by provenance when asserted, by kind otherwise.
    let asserted = run(
        &index,
        json!({"DOI":"10.48550/arXiv.2403.03542","doi-asserted-by":"publisher"}),
    );
    assert_eq!(
        confirmed(&asserted),
        vec![(
            modern,
            Provenance::Publisher,
            MatchType::Doi,
            "10.48550/arXiv.2403.03542"
        )]
    );
    let unasserted = run(&index, json!({"DOI":"10.48550/arXiv.2403.03542"}));
    assert_eq!(
        confirmed(&unasserted),
        vec![(
            modern,
            Provenance::Mined,
            MatchType::Doi,
            "10.48550/arXiv.2403.03542"
        )]
    );
}

#[test]
fn invalid_arxiv_ids_are_reported_once_and_not_counted_as_dois() {
    let index = index(&["10.1234/a"], &[]);
    let matches = run(
        &index,
        json!({
            "article-title": "arXiv:9999.99999",
            "unstructured": "arXiv:9999.99999v3 and 10.48550/arXiv.9999.99999 and arXiv:hep-ph/0000000"
        }),
    );
    assert!(matches.confirmed.is_empty());
    assert_eq!(counters(&matches), (0, 0, 0, 2));
    let failed: Vec<(&str, &str, &str)> = matches
        .failed_arxiv
        .iter()
        .map(|m| (m.id.as_str(), m.raw.as_str(), m.arxiv_doi.as_str()))
        .collect();
    assert_eq!(
        failed,
        vec![
            (
                "9999.99999",
                "arXiv:9999.99999",
                "10.48550/arXiv.9999.99999"
            ),
            (
                "hep-ph/0000000",
                "arXiv:hep-ph/0000000",
                "10.48550/arXiv.hep-ph/0000000"
            ),
        ]
    );
}

#[test]
fn bare_numeric_ids_require_arxiv_context_within_the_field() {
    let index = index(&["10.48550/arxiv.2403.03542"], &[]);
    for reference in [
        json!({"unstructured":"Some paper 2403.03542"}),
        json!({"journal-title":"arXiv","unstructured":"2403.03542"}),
        json!({"article-title":"arXiv:","unstructured":"2403.03542"}),
    ] {
        let matches = run(&index, reference.clone());
        assert!(matches.confirmed.is_empty(), "{reference}");
        assert_eq!(counters(&matches), (0, 0, 0, 0), "{reference}");
    }
}

#[test]
fn non_string_fields_are_counted_and_the_rest_still_mined() {
    let index = index(&["10.1234/a"], &[]);
    let matches = run(
        &index,
        json!({"DOI":42,"doi-asserted-by":"publisher","unstructured":"doi:10.1234/a"}),
    );
    assert_eq!(
        confirmed(&matches),
        vec![("10.1234/a", Provenance::Mined, MatchType::Doi, "10.1234/a")]
    );
    assert_eq!(counters(&matches), (0, 0, 1, 0));

    let all_malformed = run(
        &index,
        json!({"DOI":null,"URL":["https://example.org/x"],"article-title":{"a":1},"journal-title":true,"unstructured":1.5,"author":"ignored"}),
    );
    assert!(all_malformed.confirmed.is_empty());
    assert_eq!(counters(&all_malformed), (0, 0, 5, 0));

    assert!(match_reference(&json!("not an object"), &index, 4096).is_err());
    assert!(match_reference(&json!([{"DOI":"10.1234/a"}]), &index, 4096).is_err());
    assert!(run(&index, json!({})).confirmed.is_empty());
}

#[test]
fn arxiv_version_assertions_stay_mined_against_the_base_doi() {
    let base = "10.48550/arxiv.2403.03542";
    let index = index(&[base], &[]);
    let matches = run(
        &index,
        json!({"DOI":"10.48550/arXiv.2403.03542v2","doi-asserted-by":"publisher"}),
    );
    assert_eq!(
        confirmed(&matches),
        vec![(
            base,
            Provenance::Mined,
            MatchType::Arxiv,
            "10.48550/arXiv.2403.03542v2"
        )]
    );
    assert_eq!(counters(&matches), (0, 0, 0, 0));

    // When the version is registered too, the versions are distinct targets.
    let both = index_with_versions(base);
    let matches = run(
        &both,
        json!({"DOI":"10.48550/arXiv.2403.03542v2","doi-asserted-by":"publisher"}),
    );
    assert_eq!(
        confirmed(&matches),
        vec![
            (
                base,
                Provenance::Mined,
                MatchType::Arxiv,
                "10.48550/arXiv.2403.03542v2"
            ),
            (
                "10.48550/arxiv.2403.03542v2",
                Provenance::Publisher,
                MatchType::Doi,
                "10.48550/arXiv.2403.03542v2"
            ),
        ]
    );
}

fn index_with_versions(base: &str) -> MemoryIndex {
    index(&[base, &format!("{base}v2")], &[])
}

#[test]
fn field_order_then_offset_picks_the_representative() {
    let index = index(&["10.1234/a"], &[("https://example.org/a", &["10.1234/a"])]);
    let by_field = run(
        &index,
        json!({"journal-title":"x 10.1234/a","article-title":"10.1234/A"}),
    );
    assert_eq!(
        confirmed(&by_field),
        vec![("10.1234/a", Provenance::Mined, MatchType::Doi, "10.1234/A")]
    );

    let by_offset = run(
        &index,
        json!({"unstructured":"https://doi.org/10.1234/a (10.1234/a)"}),
    );
    assert_eq!(
        confirmed(&by_offset),
        vec![(
            "10.1234/a",
            Provenance::Mined,
            MatchType::Doi,
            "https://doi.org/10.1234/a"
        )]
    );

    // DOI evidence outranks URL evidence for the same target, whatever the field.
    let doi_over_url = run(
        &index,
        json!({"URL":"https://example.org/a","unstructured":"10.1234/a"}),
    );
    assert_eq!(
        confirmed(&doi_over_url),
        vec![("10.1234/a", Provenance::Mined, MatchType::Doi, "10.1234/a")]
    );
    let url_only = run(&index, json!({"URL":"https://example.org/a"}));
    assert_eq!(
        confirmed(&url_only),
        vec![(
            "10.1234/a",
            Provenance::Mined,
            MatchType::Url,
            "https://example.org/a"
        )]
    );
}
