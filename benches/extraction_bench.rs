use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// Import from the library
use crossref_citation_extraction::extract::{
    extract_arxiv_matches_from_text, extract_doi_matches_from_text, normalize_doi,
};
use crossref_citation_extraction::index::DoiIndex;

fn bench_doi_extraction(c: &mut Criterion) {
    let sample_texts = vec![
        "See 10.1234/example.paper for details",
        "doi:10.5678/another-paper and https://doi.org/10.9999/third",
        "Multiple DOIs: 10.1111/a, 10.2222/b, 10.3333/c, 10.4444/d",
        "No DOIs in this text at all",
        "Mixed content with arXiv:2403.12345 and 10.48550/arXiv.2403.12345",
    ];

    let mut group = c.benchmark_group("doi_extraction");
    group.throughput(Throughput::Elements(sample_texts.len() as u64));

    group.bench_function("extract_doi_matches", |b| {
        b.iter(|| {
            for text in &sample_texts {
                black_box(extract_doi_matches_from_text(text));
            }
        })
    });

    group.finish();
}

fn bench_arxiv_extraction(c: &mut Criterion) {
    let sample_texts = vec![
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
                black_box(extract_arxiv_matches_from_text(text));
            }
        })
    });

    group.finish();
}

fn bench_doi_index_lookup(c: &mut Criterion) {
    // Create an index with 1M DOIs
    let mut index = DoiIndex::with_capacity(1_000_000, 10_000);
    for i in 0..1_000_000u64 {
        index.insert(&format!("10.{}/{}", 1000 + (i % 10000), i));
    }

    let test_dois = vec![
        "10.1234/500000",   // exists
        "10.5678/999999",   // exists
        "10.9999/notfound", // doesn't exist
    ];

    let mut group = c.benchmark_group("index_lookup");

    group.bench_function("contains", |b| {
        b.iter(|| {
            for doi in &test_dois {
                black_box(index.contains(doi));
            }
        })
    });

    group.bench_function("has_prefix", |b| {
        b.iter(|| {
            black_box(index.has_prefix("10.1234"));
            black_box(index.has_prefix("10.9999"));
        })
    });

    group.finish();
}

fn bench_normalize_doi(c: &mut Criterion) {
    let test_dois = vec![
        "10.1234/TEST",
        "10.1234/test.",
        "10.1234%2Fencoded",
        "10.1234/clean",
    ];

    c.bench_function("normalize_doi", |b| {
        b.iter(|| {
            for doi in &test_dois {
                black_box(normalize_doi(doi));
            }
        })
    });
}

criterion_group!(
    benches,
    bench_doi_extraction,
    bench_arxiv_extraction,
    bench_doi_index_lookup,
    bench_normalize_doi,
);
criterion_main!(benches);
