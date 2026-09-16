//! Per-reference matching cost against a real memory-mapped DataCite bundle,
//! plus the legacy arXiv text scan.
//!
//! The release fixture is written and the bundle built once, outside every
//! timed loop; each case then measures one `match_reference` call on a
//! representative reference object: no identifier at all, a structured
//! registered DOI, a unique registered landing page, a landing page shared
//! by two DOIs that nothing disambiguates, and a structured arXiv DOI whose
//! identifier also appears in the free text.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use crossref_citation_extraction::extract::{extract_arxiv_matches_from_text, match_reference};
use crossref_citation_extraction::index::datacite::DataciteIndex;
use crossref_citation_extraction::index::datacite_builder::build_datacite_index;
use serde_json::{json, Value};

#[path = "../tests/support/mod.rs"]
mod support;

/// Landing-page lookup budget, as the production default (`--max-entry-mb`
/// scaled down: every reference here has at most two candidates).
const LOOKUP_BUDGET_BYTES: usize = 64 * 1024;

fn build_index(root: &std::path::Path) -> DataciteIndex {
    let source = root.join("source");
    support::make_release(
        &source,
        &[
            json!({"id":"10.1234/a","attributes":{"url":"https://example.org/a"}}),
            json!({"id":"10.1234/b","attributes":{"url":"https://example.org/shared"}}),
            json!({"id":"10.1234/c","attributes":{"url":"https://example.org/shared"}}),
            json!({"id":"10.48550/arxiv.2403.03542","attributes":{"url":"https://arxiv.org/abs/2403.03542"}}),
        ],
        &[
            ("10.1234/a", "findable"),
            ("10.1234/b", "findable"),
            ("10.1234/c", "findable"),
            ("10.48550/arxiv.2403.03542", "findable"),
        ],
    )
    .expect("write the benchmark release fixture");
    let bundle = root.join("bundle");
    build_datacite_index(&source, &bundle, 64).expect("build the benchmark bundle");
    DataciteIndex::load(&bundle).expect("load the benchmark bundle")
}

fn reference_cases() -> Vec<(&'static str, Value)> {
    vec![
        (
            "no_match",
            json!({
                "key": "r0",
                "unstructured": "Doe, J. (2019). A study of plain references. Journal of Examples 12, 34-56."
            }),
        ),
        (
            "direct_doi",
            json!({"key": "r1", "DOI": "10.1234/a", "doi-asserted-by": "publisher"}),
        ),
        (
            "registered_url",
            json!({"key": "r2", "URL": "https://example.org/a"}),
        ),
        (
            "shared_url",
            json!({"key": "r3", "URL": "https://example.org/shared"}),
        ),
        (
            "arxiv_overlap",
            json!({
                "key": "r4",
                "DOI": "10.48550/arXiv.2403.03542",
                "doi-asserted-by": "crossref",
                "unstructured": "See arXiv:2403.03542 for the preprint."
            }),
        ),
    ]
}

fn bench_reference_matching(c: &mut Criterion) {
    let tmp = tempfile::tempdir().expect("create a temporary directory");
    let index = build_index(tmp.path());
    let cases = reference_cases();

    let mut group = c.benchmark_group("datacite_match_reference");
    group.throughput(Throughput::Elements(1));
    for (name, reference) in &cases {
        // Sanity: every case must be a well-formed reference object.
        match_reference(reference, &index, LOOKUP_BUDGET_BYTES)
            .unwrap_or_else(|error| panic!("case {name} failed: {error}"));
        group.bench_function(*name, |b| {
            b.iter(|| {
                black_box(
                    match_reference(black_box(reference), &index, LOOKUP_BUDGET_BYTES)
                        .expect("match a benchmark reference"),
                )
            })
        });
    }
    group.finish();
}

fn bench_arxiv_extraction(c: &mut Criterion) {
    let sample_texts = [
        "arXiv:2403.12345",
        "arXiv:hep-ph/9901234 and arXiv:cs.DM/9910013",
        "https://arxiv.org/abs/2403.12345",
        "10.48550/arXiv.2403.12345",
        "No arXiv references here",
    ];

    let mut group = c.benchmark_group("arxiv_extraction");
    group.throughput(Throughput::Elements(sample_texts.len() as u64));

    group.bench_function("extract_arxiv_matches", |b| {
        b.iter(|| {
            for text in &sample_texts {
                black_box(extract_arxiv_matches_from_text(black_box(text)));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_reference_matching, bench_arxiv_extraction);
criterion_main!(benches);
