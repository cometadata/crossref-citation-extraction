//! General DataCite reference matching: membership checks against the
//! target index, evidence collection in field order, the shared-URL
//! intersection rule, deduplication by canonical target DOI, and
//! representative selection.
//!
//! [`match_reference`] is the full-mode entry point, called once per
//! reference object. Fields are visited in the fixed order `DOI`, `URL`,
//! `article-title`, `journal-title`, `unstructured`, one at a time. Within
//! a field the structured `DOI`/`URL` value is checked as a whole first,
//! then the existing arXiv parser runs when the field has arXiv context,
//! then embedded URL and DOI tokens are scanned on entity-decoded text
//! with evidence sliced from the original. Direct DOI and arXiv targets
//! are validated before any landing page is resolved, so a shared URL can
//! only be settled by evidence that identifies one of its own DOIs.
//! Degenerate landing-page keys (see [`is_degenerate_landing_url`]) are
//! skipped before any lookup and only counted.

use std::cmp::Reverse;
use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};
use std::ops::Range;

use anyhow::{anyhow, Context, Result};
use serde_json::{Map, Value};
use url::Url;

#[cfg(doc)]
use crate::common::identifiers::is_degenerate_landing_url;
use crate::common::identifiers::{is_degenerate_landing, normalize_doi, url_lookup_keys};
use crate::common::{ArxivMatch, MatchType};
use crate::extract::arxiv::extract_arxiv_matches_from_text;
use crate::extract::reference::{
    boundary_variants, contains_arxiv_context, scan_doi_tokens, scan_url_tokens, search_fields,
    Decoded, EntityDecoder, FieldText, Token, DOI_FIELD, MAX_DOI_TOKEN_BYTES, URL_FIELD,
};
use crate::extract::Provenance;
use crate::index::datacite::TargetLookup;

/// Prefix of every canonical arXiv DOI. Failures of such candidates are
/// reported by the arXiv parser as `failed_arxiv`, not as unmatched DOIs.
const ARXIV_DOI_PREFIX: &str = "10.48550/arxiv.";

/// One confirmed cited target and the evidence chosen to represent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceMatch {
    /// The canonical cited DOI.
    pub doi: String,
    pub provenance: Provenance,
    /// The original substring (or whole structured value) that matched.
    pub raw_match: String,
    pub match_type: MatchType,
}

/// A registered landing page shared by several DOIs that the reference's
/// own DOI and arXiv evidence could not settle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedUrl {
    /// The normalized lookup key. For a scheme-less sighting this is the
    /// first key (`http://` before `https://`) that had candidates, and
    /// `candidate_dois` is the union across both.
    pub url: String,
    /// The first sighting's original substring.
    pub raw_match: String,
    /// Sorted, distinct.
    pub candidate_dois: Vec<String>,
}

/// Everything `match_reference` learned about one reference.
#[derive(Debug, Clone, Default)]
pub struct ReferenceMatches {
    /// One entry per canonical cited DOI, sorted by DOI.
    pub confirmed: Vec<ReferenceMatch>,
    /// One entry per normalized shared URL, sorted by URL.
    pub ambiguous: Vec<UnresolvedUrl>,
    /// arXiv identifiers the parser accepted but the index does not hold,
    /// one per identifier in field order.
    pub failed_arxiv: Vec<ArxivMatch>,
    /// Distinct non-arXiv DOI candidates absent from the index.
    pub unmatched_dois: u64,
    /// Distinct URL candidates with no registered landing page.
    pub unmatched_urls: u64,
    /// Distinct URL candidates whose lookup keys are degenerate (see
    /// [`is_degenerate_landing_url`]) and were never looked up. Counted
    /// like `unmatched_urls`, and never also counted there.
    pub degenerate_urls: u64,
    /// Searchable fields present with a non-string value.
    pub malformed_fields: u64,
}

/// Mine one Crossref reference object against `index`.
///
/// `max_lookup_bytes` bounds every landing-page lookup; a lookup that
/// exceeds it is an error, as is a `reference` that is not a JSON object.
pub fn match_reference(
    reference: &Value,
    index: &dyn TargetLookup,
    max_lookup_bytes: usize,
) -> Result<ReferenceMatches> {
    let object = reference
        .as_object()
        .ok_or_else(|| anyhow!("reference is not a JSON object"))?;
    let (fields, malformed_fields) = search_fields(object);
    let asserted = asserted_provenance(object);
    let mut matcher = Matcher::new(index, max_lookup_bytes);
    let mut scratch = Scratch::default();
    for field in &fields {
        matcher.mine_field(field, asserted, &mut scratch)?;
    }
    Ok(matcher.finish(malformed_fields))
}

/// Buffers reused across the fields of one reference.
#[derive(Default)]
struct Scratch {
    decoder: EntityDecoder,
    tokens: Vec<Token>,
    /// Byte ranges of the original field text already claimed by the
    /// arXiv parser or by a resolver URL; tokens overlapping them are not
    /// evaluated again as general candidates.
    claimed: Vec<Range<usize>>,
}

fn overlaps(claimed: &[Range<usize>], range: &Range<usize>) -> bool {
    claimed
        .iter()
        .any(|span| range.start < span.end && span.start < range.end)
}

/// The provenance a registered structured DOI receives: only a recognized
/// `doi-asserted-by` value lifts it above `mined`.
fn asserted_provenance(reference: &Map<String, Value>) -> Provenance {
    match reference.get("doi-asserted-by").and_then(Value::as_str) {
        Some(value) if value.eq_ignore_ascii_case("publisher") => Provenance::Publisher,
        Some(value) if value.eq_ignore_ascii_case("crossref") => Provenance::Crossref,
        _ => Provenance::Mined,
    }
}

/// Evidence kinds in representative-selection order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EvidenceKind {
    StructuredDoi,
    Arxiv,
    OtherDoi,
    Url,
}

impl EvidenceKind {
    fn match_type(self) -> MatchType {
        match self {
            EvidenceKind::StructuredDoi | EvidenceKind::OtherDoi => MatchType::Doi,
            EvidenceKind::Arxiv => MatchType::Arxiv,
            EvidenceKind::Url => MatchType::Url,
        }
    }
}

/// One validated piece of evidence for one canonical DOI.
#[derive(Debug)]
struct Candidate {
    doi: String,
    provenance: Provenance,
    kind: EvidenceKind,
    field: usize,
    offset: usize,
    raw: String,
}

/// The primary candidate of a scanned token that no variant matched: the
/// canonical form of its shortest normalizable variant, so trailing
/// punctuation does not multiply the count.
#[derive(Debug)]
enum Miss {
    Doi(String),
    Url(String),
    /// A URL whose lookup keys were all degenerate, by its first key.
    DegenerateUrl(String),
}

/// What trying the variants of a DOI-shaped token found.
#[derive(Debug)]
enum DoiOutcome {
    /// A variant is registered and was recorded as evidence.
    Matched,
    /// No variant is registered: the canonical form of the shortest
    /// normalizable variant, if any.
    Unmatched(Option<String>),
}

/// What evaluating a URL-shaped token found.
#[derive(Debug)]
struct UrlOutcome {
    /// The token was a DOI resolver URL (whether or not it matched).
    resolver: bool,
    miss: Option<Miss>,
}

/// One registered landing page seen in the reference, before resolution.
#[derive(Debug)]
struct UrlSighting {
    raw: String,
    candidates: Vec<String>,
    field: usize,
    offset: usize,
}

/// Longest DOI whose failed membership probe is memoized. Longer misses do
/// not repeat in practice and would otherwise keep a copy of every long
/// token a reference contains.
const MAX_MEMOIZED_MISS_BYTES: usize = 256;

/// Per-reference memoization of index lookups, so repeated tokens and
/// variants do not repeat FST probes or posting decodes.
struct LookupCache<'a> {
    index: &'a dyn TargetLookup,
    max_lookup_bytes: usize,
    dois: BTreeMap<String, bool>,
    hosts: BTreeMap<String, bool>,
    urls: BTreeMap<String, Vec<String>>,
}

impl LookupCache<'_> {
    fn contains_doi(&mut self, doi: &str) -> bool {
        self.probe_doi(doi, true)
    }

    /// Membership of `doi`, memoized. A miss is remembered only when
    /// `remember_miss` is set and the DOI is at most
    /// [`MAX_MEMOIZED_MISS_BYTES`] long.
    fn probe_doi(&mut self, doi: &str, remember_miss: bool) -> bool {
        if let Some(&hit) = self.dois.get(doi) {
            return hit;
        }
        let hit = self.index.contains_doi(doi);
        if hit || (remember_miss && doi.len() <= MAX_MEMOIZED_MISS_BYTES) {
            self.dois.insert(doi.to_owned(), hit);
        }
        hit
    }

    /// Whether the host of the parsed normalized URL `key` has any
    /// registered landing page, derived exactly as the index builder
    /// derives hosts.
    fn contains_host_of(&mut self, key: &Url) -> bool {
        let Some(host) = key.host_str() else {
            return false;
        };
        if let Some(&hit) = self.hosts.get(host) {
            return hit;
        }
        let hit = self.index.contains_host(host);
        self.hosts.insert(host.to_owned(), hit);
        hit
    }

    fn url_dois(&mut self, key: &str) -> Result<&[String]> {
        if !self.urls.contains_key(key) {
            let dois = self
                .index
                .url_dois(key, self.max_lookup_bytes)
                .with_context(|| format!("landing-page lookup for {key:?}"))?;
            self.urls.insert(key.to_owned(), dois);
        }
        Ok(self.urls.get(key).map_or(&[], Vec::as_slice))
    }
}

struct Matcher<'a> {
    cache: LookupCache<'a>,
    candidates: Vec<Candidate>,
    urls: BTreeMap<String, UrlSighting>,
    failed_arxiv: Vec<ArxivMatch>,
    failed_arxiv_dois: BTreeSet<String>,
    unmatched_dois: BTreeSet<String>,
    unmatched_urls: BTreeSet<String>,
    degenerate_urls: BTreeSet<String>,
}

impl<'a> Matcher<'a> {
    fn new(index: &'a dyn TargetLookup, max_lookup_bytes: usize) -> Self {
        Self {
            cache: LookupCache {
                index,
                max_lookup_bytes,
                dois: BTreeMap::new(),
                hosts: BTreeMap::new(),
                urls: BTreeMap::new(),
            },
            candidates: Vec::new(),
            urls: BTreeMap::new(),
            failed_arxiv: Vec::new(),
            failed_arxiv_dois: BTreeSet::new(),
            unmatched_dois: BTreeSet::new(),
            unmatched_urls: BTreeSet::new(),
            degenerate_urls: BTreeSet::new(),
        }
    }

    /// Mine one field: the structured whole value first, then the arXiv
    /// parser, then embedded URL tokens, then embedded DOI tokens.
    ///
    /// A DOI token cut short by a later DOI start is first tried whole, up
    /// to the next whitespace, for a registered variant only: a hit is
    /// evidence and claims the whole token, so `10.7777/outer.10.8888/inner`
    /// cites the outer DOI and not also the inner one. Without a hit the
    /// cut token is evaluated as usual. Each DOI start therefore costs at
    /// most two bounded sets of variants, each no longer than
    /// [`MAX_DOI_TOKEN_BYTES`], and the misses of whole-token tries are
    /// neither counted nor memoized.
    ///
    /// Matching runs on the entity-decoded text; evidence is always sliced
    /// from the original value. Text the arXiv parser matched belongs to
    /// it, so `arXiv:cs.DM/9910013` is not also a scheme-less URL and
    /// `arxiv.org/abs/<id>v2` is not also an unregistered landing page;
    /// likewise a resolver URL owns the DOI it carries, which the URL
    /// reading percent-decodes and a bare reading would not.
    ///
    /// Only token scans feed the unmatched and degenerate counters: a
    /// structured value that fails as a whole is scanned as a token of the
    /// same field, which counts it once if no variant matches, and not at
    /// all if one does.
    fn mine_field(
        &mut self,
        field: &FieldText<'_>,
        asserted: Provenance,
        scratch: &mut Scratch,
    ) -> Result<()> {
        let decoded = scratch.decoder.decode(field.text);
        let whole = |_: usize| field.text.to_owned();
        match field.field {
            DOI_FIELD => self.check_doi_value(field.field, decoded.text, whole, asserted),
            URL_FIELD => {
                self.evaluate_url(field.field, 0, std::iter::once(decoded.text), whole)?;
            }
            _ => {}
        }

        scratch.claimed.clear();
        if contains_arxiv_context(field.text) {
            self.mine_arxiv(field, &mut scratch.claimed);
        }

        scratch.tokens.clear();
        scan_url_tokens(decoded.text, &mut scratch.tokens);
        for token in &scratch.tokens {
            let original = decoded.original_range(token.start, token.end);
            if overlaps(&scratch.claimed, &original) {
                continue;
            }
            let text = &decoded.text[token.start..token.end];
            let raw_of = |length: usize| raw_from(field, &decoded, token.start, length);
            let outcome =
                self.evaluate_url(field.field, token.start, boundary_variants(text), raw_of)?;
            if outcome.resolver {
                scratch.claimed.push(original);
            }
            self.note_miss(outcome.miss);
        }

        scratch.tokens.clear();
        scan_doi_tokens(decoded.text, &mut scratch.tokens);
        for token in &scratch.tokens {
            let original = decoded.original_range(token.start, token.end);
            if overlaps(&scratch.claimed, &original) {
                continue;
            }
            let raw_of = |length: usize| raw_from(field, &decoded, token.start, length);
            if token.full_end > token.end && token.full_end - token.start <= MAX_DOI_TOKEN_BYTES {
                let whole = decoded.original_range(token.start, token.full_end);
                if !overlaps(&scratch.claimed, &whole) {
                    let text = &decoded.text[token.start..token.full_end];
                    let outcome = self.evaluate_doi(
                        field.field,
                        token.start,
                        boundary_variants(text),
                        raw_of,
                        false,
                    );
                    if matches!(outcome, DoiOutcome::Matched) {
                        scratch.claimed.push(whole);
                        continue;
                    }
                }
            }
            let text = &decoded.text[token.start..token.end];
            let miss = if token.overlong {
                // Longer than any registrable DOI: counted, never looked up.
                normalize_doi(text).map(Miss::Doi)
            } else {
                match self.evaluate_doi(
                    field.field,
                    token.start,
                    boundary_variants(text),
                    raw_of,
                    true,
                ) {
                    DoiOutcome::Matched => None,
                    DoiOutcome::Unmatched(primary) => primary.map(Miss::Doi),
                }
            };
            self.note_miss(miss);
        }
        Ok(())
    }

    /// The structured `DOI` value as a whole, with its asserted provenance.
    fn check_doi_value(
        &mut self,
        field: usize,
        text: &str,
        raw_of: impl Fn(usize) -> String,
        asserted: Provenance,
    ) {
        let Some(canonical) = normalize_doi(text) else {
            return;
        };
        if self.cache.contains_doi(&canonical) {
            self.candidates.push(Candidate {
                doi: canonical,
                provenance: asserted,
                kind: EvidenceKind::StructuredDoi,
                field,
                offset: 0,
                raw: raw_of(text.len()),
            });
        }
    }

    /// Try the variants of a DOI-shaped token, longest first; the first
    /// registered variant wins and is recorded as evidence. `raw_of` gives
    /// the evidence for a variant of the given length. `remember_misses`
    /// allows failed probes to be memoized.
    fn evaluate_doi<'t>(
        &mut self,
        field: usize,
        offset: usize,
        variants: impl Iterator<Item = &'t str>,
        raw_of: impl Fn(usize) -> String,
        remember_misses: bool,
    ) -> DoiOutcome {
        let mut primary = None;
        for variant in variants {
            let Some(canonical) = normalize_doi(variant) else {
                continue;
            };
            if self.cache.probe_doi(&canonical, remember_misses) {
                self.candidates.push(Candidate {
                    doi: canonical,
                    provenance: Provenance::Mined,
                    kind: EvidenceKind::OtherDoi,
                    field,
                    offset,
                    raw: raw_of(variant.len()),
                });
                return DoiOutcome::Matched;
            }
            primary = Some(canonical);
        }
        DoiOutcome::Unmatched(primary)
    }

    /// Try the variants of a URL-shaped token, longest first. A resolver
    /// URL is checked as a DOI; any other URL is checked against every
    /// lookup key that is not degenerate and whose host is registered, and
    /// the candidates of all keys are unioned. A degenerate key is skipped
    /// before any lookup. The first variant with a registered DOI or at
    /// least one landing-page candidate wins. Reports whether the token was
    /// a resolver URL and the miss to count when nothing matched: a
    /// shortest normalizable variant whose keys are all degenerate is a
    /// degenerate miss rather than an unmatched one.
    fn evaluate_url<'t>(
        &mut self,
        field: usize,
        offset: usize,
        variants: impl Iterator<Item = &'t str>,
        raw_of: impl Fn(usize) -> String,
    ) -> Result<UrlOutcome> {
        let mut primary_doi = None;
        let mut primary_url = None;
        for variant in variants {
            if let Some(canonical) = normalize_doi(variant) {
                if self.cache.contains_doi(&canonical) {
                    self.candidates.push(Candidate {
                        doi: canonical,
                        provenance: Provenance::Mined,
                        kind: EvidenceKind::OtherDoi,
                        field,
                        offset,
                        raw: raw_of(variant.len()),
                    });
                    return Ok(UrlOutcome {
                        resolver: true,
                        miss: None,
                    });
                }
                primary_doi = Some(canonical);
                continue;
            }
            let keys = url_lookup_keys(variant);
            let Some(first) = keys.first() else {
                continue;
            };
            let mut primary = None;
            let mut url = None;
            let mut candidates = Vec::new();
            for key in &keys {
                let parsed = Url::parse(key).ok();
                if parsed.as_ref().is_some_and(is_degenerate_landing) {
                    continue;
                }
                primary.get_or_insert(key);
                if !parsed.is_some_and(|parsed| self.cache.contains_host_of(&parsed)) {
                    continue;
                }
                let dois = self.cache.url_dois(key)?;
                if !dois.is_empty() {
                    url.get_or_insert_with(|| key.clone());
                    candidates.extend(dois.iter().cloned());
                }
            }
            primary_url = Some(match primary {
                Some(key) => Miss::Url(key.clone()),
                None => Miss::DegenerateUrl(first.clone()),
            });
            if let Some(url) = url {
                candidates.sort();
                candidates.dedup();
                self.record_url(url, raw_of(variant.len()), candidates, field, offset);
                return Ok(UrlOutcome {
                    resolver: false,
                    miss: None,
                });
            }
        }
        Ok(UrlOutcome {
            resolver: primary_doi.is_some(),
            miss: primary_doi.map(Miss::Doi).or(primary_url),
        })
    }

    /// Count a scanned token that no variant matched, once per distinct
    /// primary candidate. arXiv DOIs are the arXiv parser's to report.
    fn note_miss(&mut self, miss: Option<Miss>) {
        match miss {
            Some(Miss::Doi(canonical)) => {
                if !canonical.starts_with(ARXIV_DOI_PREFIX) {
                    self.unmatched_dois.insert(canonical);
                }
            }
            Some(Miss::Url(key)) => {
                self.unmatched_urls.insert(key);
            }
            Some(Miss::DegenerateUrl(key)) => {
                self.degenerate_urls.insert(key);
            }
            None => {}
        }
    }

    /// Run the legacy arXiv parser on the original field text. Registered
    /// identifiers become arXiv evidence; the rest are reported once each.
    /// Every occurrence of a matched substring is added to `claimed`.
    fn mine_arxiv(&mut self, field: &FieldText<'_>, claimed: &mut Vec<Range<usize>>) {
        for arxiv in extract_arxiv_matches_from_text(field.text) {
            let mut offset = None;
            for (start, raw) in field.text.match_indices(arxiv.raw.as_str()) {
                offset.get_or_insert(start);
                claimed.push(start..start + raw.len());
            }
            let Some(canonical) = normalize_doi(&arxiv.arxiv_doi) else {
                continue;
            };
            if self.cache.contains_doi(&canonical) {
                self.candidates.push(Candidate {
                    doi: canonical,
                    provenance: Provenance::Mined,
                    kind: EvidenceKind::Arxiv,
                    field: field.field,
                    offset: offset.unwrap_or(0),
                    raw: arxiv.raw,
                });
            } else if self.failed_arxiv_dois.insert(canonical) {
                self.failed_arxiv.push(arxiv);
            }
        }
    }

    /// Record a sighting of a registered landing page. Repeated sightings
    /// of one normalized URL keep the first evidence and union candidates.
    fn record_url(
        &mut self,
        url: String,
        raw: String,
        candidates: Vec<String>,
        field: usize,
        offset: usize,
    ) {
        match self.urls.entry(url) {
            Entry::Vacant(slot) => {
                slot.insert(UrlSighting {
                    raw,
                    candidates,
                    field,
                    offset,
                });
            }
            Entry::Occupied(mut slot) => {
                let seen = &mut slot.get_mut().candidates;
                seen.extend(candidates);
                seen.sort();
                seen.dedup();
            }
        }
    }

    /// Resolve landing pages against the independently identified DOIs,
    /// then choose one representative per canonical target.
    fn finish(mut self, malformed_fields: u64) -> ReferenceMatches {
        let independently_identified: BTreeSet<String> = self
            .candidates
            .iter()
            .map(|candidate| candidate.doi.clone())
            .collect();

        let mut ambiguous = Vec::new();
        for (url, sighting) in std::mem::take(&mut self.urls) {
            let candidates: Vec<String> = sighting.candidates;
            let identified: Vec<&String> = candidates
                .iter()
                .filter(|doi| independently_identified.contains(*doi))
                .collect();
            let resolved = if candidates.len() == 1 {
                candidates.first()
            } else if identified.len() == 1 {
                identified.first().copied()
            } else {
                None
            };
            match resolved {
                Some(doi) => self.candidates.push(Candidate {
                    doi: doi.clone(),
                    provenance: Provenance::Mined,
                    kind: EvidenceKind::Url,
                    field: sighting.field,
                    offset: sighting.offset,
                    raw: sighting.raw,
                }),
                None => ambiguous.push(UnresolvedUrl {
                    url,
                    raw_match: sighting.raw,
                    candidate_dois: candidates,
                }),
            }
        }

        let mut candidates = self.candidates;
        candidates.sort_by(|a, b| {
            (&a.doi, Reverse(a.provenance), a.kind, a.field, a.offset).cmp(&(
                &b.doi,
                Reverse(b.provenance),
                b.kind,
                b.field,
                b.offset,
            ))
        });
        candidates.dedup_by(|later, earlier| later.doi == earlier.doi);
        let confirmed = candidates
            .into_iter()
            .map(|candidate| ReferenceMatch {
                doi: candidate.doi,
                provenance: candidate.provenance,
                raw_match: candidate.raw,
                match_type: candidate.kind.match_type(),
            })
            .collect();

        ReferenceMatches {
            confirmed,
            ambiguous,
            failed_arxiv: self.failed_arxiv,
            unmatched_dois: self.unmatched_dois.len() as u64,
            unmatched_urls: self.unmatched_urls.len() as u64,
            degenerate_urls: self.degenerate_urls.len() as u64,
            malformed_fields,
        }
    }
}

/// The original substring behind a decoded token variant. The span map
/// always yields character boundaries of the original; the decoded slice
/// is a defensive fallback rather than a reachable path.
fn raw_from(field: &FieldText<'_>, decoded: &Decoded<'_>, start: usize, length: usize) -> String {
    let range = decoded.original_range(start, start + length);
    field
        .text
        .get(range)
        .unwrap_or(&decoded.text[start..start + length])
        .to_owned()
}
