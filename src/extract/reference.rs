//! Reference-field walking, candidate token scanning, HTML entity decoding
//! with offset preservation, and bounded boundary variants for general DOI
//! and landing-page URL mining.
//!
//! Nothing here consults an index or applies arXiv rules: it reports which
//! searchable fields hold text, where DOI- and URL-shaped candidates start,
//! how far each whitespace-delimited token runs, and which trailing
//! citation punctuation a caller may try removing. Fields are handled one
//! at a time; text is never joined across fields.

use std::ops::Range;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value};

/// The searchable reference fields, in the fixed order evidence is ranked.
pub const SEARCH_FIELDS: [&str; 5] = [
    "DOI",
    "URL",
    "article-title",
    "journal-title",
    "unstructured",
];

/// Position of the structured `DOI` field in [`SEARCH_FIELDS`].
pub const DOI_FIELD: usize = 0;
/// Position of the structured `URL` field in [`SEARCH_FIELDS`].
pub const URL_FIELD: usize = 1;

/// Most trailing characters removed from one token while generating
/// variants, so a token is tried at most this many plus one times.
pub const MAX_BOUNDARY_STRIPS: usize = 4;

/// Trailing characters that may be citation punctuation rather than part
/// of the identifier.
const BOUNDARY_CHARS: [char; 10] = ['.', ',', ';', ':', ')', ']', '>', '}', '\'', '"'];

/// Longest entity body (between `&` and `;`) considered for decoding.
const MAX_ENTITY_BODY: usize = 8;

/// One searchable field holding a string value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldText<'a> {
    /// Index into [`SEARCH_FIELDS`].
    pub field: usize,
    pub name: &'static str,
    /// The value exactly as stored in the reference.
    pub text: &'a str,
}

/// The string-valued searchable fields of a reference object in field
/// order, and the number of searchable fields present with a non-string
/// value (`null` included), which are skipped. A missing key is neither.
pub fn search_fields(reference: &Map<String, Value>) -> (Vec<FieldText<'_>>, u64) {
    let mut fields = Vec::with_capacity(SEARCH_FIELDS.len());
    let mut malformed = 0;
    for (index, name) in SEARCH_FIELDS.iter().enumerate() {
        match reference.get(*name) {
            None => {}
            Some(Value::String(text)) => fields.push(FieldText {
                field: index,
                name,
                text,
            }),
            Some(_) => malformed += 1,
        }
    }
    (fields, malformed)
}

/// Whether the arXiv parser is worth running on `text`: the legacy
/// pre-check for the literal `arxiv` in any case.
pub fn contains_arxiv_context(text: &str) -> bool {
    text.as_bytes()
        .windows(5)
        .any(|window| window.eq_ignore_ascii_case(b"arxiv"))
}

/// What a token looked like where it started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// A `10.<registrant>/` prefix.
    Doi,
    /// An explicit `http://`/`https://` URL or a scheme-less `host.tld/path`.
    Url,
}

/// A candidate token: byte offsets into the scanned text, ending at the
/// next whitespace or control character.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
}

/// A DOI start: `10.`, a registrant of digit groups, and the slash.
static DOI_START: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b10\.\d+(?:\.\d+)*/").unwrap());

/// A URL start: an explicit HTTP(S) scheme, or a dotted host with an
/// alphabetic top-level label followed by a path slash.
static URL_START: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)https?://|\b(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,}/").unwrap()
});

/// Append every DOI token in `text`. Each start is extended to the end of
/// its whitespace-delimited token; starts inside an earlier token are
/// kept, so `10.1/a,10.2/b` yields both.
pub fn scan_doi_tokens(text: &str, out: &mut Vec<Token>) {
    let mut position = 0;
    while let Some(found) = DOI_START.find_at(text, position) {
        out.push(Token {
            kind: TokenKind::Doi,
            start: found.start(),
            end: token_end(text, found.start()),
        });
        position = found.end();
    }
}

/// Append every URL token in `text`. The scan resumes after each accepted
/// token, so a scheme-less pattern is not found again inside an explicit
/// URL. A scheme-less start directly after `@`, `/`, `.`, or `-` is an
/// e-mail domain, a path segment, or a host fragment, not a reference.
pub fn scan_url_tokens(text: &str, out: &mut Vec<Token>) {
    let mut position = 0;
    while let Some(found) = URL_START.find_at(text, position) {
        let start = found.start();
        let explicit = found.as_str().contains("://");
        if explicit || plausible_scheme_less_start(text, start) {
            let end = token_end(text, start);
            out.push(Token {
                kind: TokenKind::Url,
                start,
                end,
            });
            position = end;
        } else {
            position = found.end();
        }
    }
}

fn plausible_scheme_less_start(text: &str, start: usize) -> bool {
    !matches!(
        text[..start].chars().next_back(),
        Some('@' | '/' | '.' | '-')
    )
}

/// The end of the token starting at `start`: the next whitespace or
/// control character, or the end of the text.
fn token_end(text: &str, start: usize) -> usize {
    text[start..]
        .find(|c: char| c.is_whitespace() || c.is_control())
        .map_or(text.len(), |offset| start + offset)
}

/// The bounded variants of one token, longest first: the token itself,
/// then with one trailing boundary character removed at a time, at most
/// [`MAX_BOUNDARY_STRIPS`] times. A closing bracket that balances an
/// opener inside the token belongs to the identifier and stops the
/// sequence. Nothing else is ever removed.
pub fn boundary_variants(token: &str) -> BoundaryVariants<'_> {
    BoundaryVariants {
        next: Some(token),
        strips_left: MAX_BOUNDARY_STRIPS,
    }
}

/// Iterator returned by [`boundary_variants`].
#[derive(Debug, Clone)]
pub struct BoundaryVariants<'t> {
    next: Option<&'t str>,
    strips_left: usize,
}

impl<'t> Iterator for BoundaryVariants<'t> {
    type Item = &'t str;

    fn next(&mut self) -> Option<&'t str> {
        let current = self.next.take()?;
        if self.strips_left > 0 {
            if let Some(shorter) = strip_boundary(current) {
                self.next = Some(shorter);
                self.strips_left -= 1;
            }
        }
        Some(current)
    }
}

/// `token` without its last character when that character is boundary
/// punctuation and, for a closer, is not balanced by an opener inside.
fn strip_boundary(token: &str) -> Option<&str> {
    let last = token.chars().next_back()?;
    if !BOUNDARY_CHARS.contains(&last) {
        return None;
    }
    let shorter = &token[..token.len() - last.len_utf8()];
    if let Some(open) = opener(last) {
        let opens = shorter.chars().filter(|&c| c == open).count();
        let closes = shorter.chars().filter(|&c| c == last).count();
        if opens > closes {
            return None;
        }
    }
    Some(shorter)
}

fn opener(close: char) -> Option<char> {
    match close {
        ')' => Some('('),
        ']' => Some('['),
        '>' => Some('<'),
        '}' => Some('{'),
        _ => None,
    }
}

/// HTML entity decoding that remembers where every replacement came from,
/// so evidence can be sliced from the original text. Buffers are reused
/// across calls; a text without entities is returned as-is.
///
/// Decodes the XML named entities (`&amp;`, `&lt;`, `&gt;`, `&quot;`,
/// `&apos;`) and decimal or hexadecimal numeric references. Anything else
/// is left verbatim.
#[derive(Debug, Default)]
pub struct EntityDecoder {
    text: String,
    replacements: Vec<Replacement>,
}

/// One decoded entity: its bytes in the decoded text and in the original.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Replacement {
    decoded_start: usize,
    decoded_end: usize,
    original_start: usize,
    original_end: usize,
}

/// A decoded view of one field, borrowing the decoder's buffers.
#[derive(Debug, Clone, Copy)]
pub struct Decoded<'a> {
    pub text: &'a str,
    replacements: &'a [Replacement],
}

impl EntityDecoder {
    pub fn decode<'a>(&'a mut self, original: &'a str) -> Decoded<'a> {
        self.text.clear();
        self.replacements.clear();
        if !original.contains('&') {
            return Decoded {
                text: original,
                replacements: &[],
            };
        }
        let bytes = original.as_bytes();
        let mut copied = 0;
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'&' {
                if let Some((decoded, length)) = parse_entity(&original[i..]) {
                    self.text.push_str(&original[copied..i]);
                    let decoded_start = self.text.len();
                    self.text.push(decoded);
                    self.replacements.push(Replacement {
                        decoded_start,
                        decoded_end: self.text.len(),
                        original_start: i,
                        original_end: i + length,
                    });
                    i += length;
                    copied = i;
                    continue;
                }
            }
            i += 1;
        }
        if self.replacements.is_empty() {
            return Decoded {
                text: original,
                replacements: &[],
            };
        }
        self.text.push_str(&original[copied..]);
        Decoded {
            text: &self.text,
            replacements: &self.replacements,
        }
    }
}

impl Decoded<'_> {
    /// The original byte range behind decoded `start..end`. A boundary at
    /// the start of a decoded entity maps to the entity's first byte; one
    /// at its end maps to the byte after its `;`.
    pub fn original_range(&self, start: usize, end: usize) -> Range<usize> {
        map_offset(self.replacements, start)..map_offset(self.replacements, end)
    }
}

fn map_offset(replacements: &[Replacement], decoded: usize) -> usize {
    let index = replacements.partition_point(|r| r.decoded_end <= decoded);
    if let Some(inside) = replacements.get(index) {
        if inside.decoded_start < decoded {
            return inside.original_start;
        }
    }
    let shift = index.checked_sub(1).map_or(0, |i| {
        replacements[i].original_end - replacements[i].decoded_end
    });
    decoded + shift
}

/// Decode the entity at the start of `text` (which begins with `&`):
/// the character it stands for and the length of the entity, including
/// the `;`.
fn parse_entity(text: &str) -> Option<(char, usize)> {
    let body = &text[1..];
    let end = body
        .bytes()
        .take(MAX_ENTITY_BODY + 1)
        .position(|b| b == b';')?;
    let body = &body[..end];
    let decoded = match body {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        _ => {
            let digits = body.strip_prefix('#')?;
            let value = match digits.strip_prefix(['x', 'X']) {
                Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                None => digits.parse::<u32>().ok()?,
            };
            char::from_u32(value)?
        }
    };
    Some((decoded, end + 2))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(kind: TokenKind, text: &str) -> Vec<&str> {
        let mut out = Vec::new();
        match kind {
            TokenKind::Doi => scan_doi_tokens(text, &mut out),
            TokenKind::Url => scan_url_tokens(text, &mut out),
        }
        out.iter().map(|t| &text[t.start..t.end]).collect()
    }

    #[test]
    fn counts_non_string_fields_and_keeps_field_order() {
        let reference = serde_json::json!({
            "unstructured": "u", "DOI": 42, "journal-title": null,
            "article-title": "a", "URL": ["x"], "author": "ignored"
        });
        let (fields, malformed) = search_fields(reference.as_object().unwrap());
        assert_eq!(malformed, 3);
        assert_eq!(
            fields
                .iter()
                .map(|f| (f.field, f.name, f.text))
                .collect::<Vec<_>>(),
            vec![(2, "article-title", "a"), (4, "unstructured", "u")]
        );
    }

    #[test]
    fn doi_tokens_start_at_word_boundaries_and_run_to_whitespace() {
        assert_eq!(
            tokens(
                TokenKind::Doi,
                "doi:10.1234/abc, then 10.1000.10/x.y) and 10.1/a,10.2/b"
            ),
            vec!["10.1234/abc,", "10.1000.10/x.y)", "10.1/a,10.2/b", "10.2/b"]
        );
        assert!(tokens(TokenKind::Doi, "arXiv:2410.12345 x10.1234/y 10.abc/z").is_empty());
    }

    #[test]
    fn url_tokens_cover_explicit_and_scheme_less_forms() {
        assert_eq!(
            tokens(
                TokenKind::Url,
                "see HTTPS://Example.org/a?x=1&y=2. or example.org/b; not me@example.org/c or /path/example.org/d"
            ),
            vec!["HTTPS://Example.org/a?x=1&y=2.", "example.org/b;"]
        );
        assert!(tokens(TokenKind::Url, "vol.12/3 fig.a/b 10.1234/abc").is_empty());
    }

    #[test]
    fn boundary_variants_are_bounded_and_keep_balanced_closers() {
        let all = |token: &str| {
            boundary_variants(token)
                .map(String::from)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            all("10.1234/a)."),
            vec!["10.1234/a).", "10.1234/a)", "10.1234/a"]
        );
        assert_eq!(all("10.1234/b(1)."), vec!["10.1234/b(1).", "10.1234/b(1)"]);
        assert_eq!(all("10.1234/(a))"), vec!["10.1234/(a))", "10.1234/(a)"]);
        assert_eq!(all("10.1234/abc"), vec!["10.1234/abc"]);
        assert_eq!(
            all("x.,;:)]"),
            vec!["x.,;:)]", "x.,;:)", "x.,;:", "x.,;", "x.,"]
        );
        assert_eq!(all("é."), vec!["é.", "é"]);
    }

    #[test]
    fn entity_decoding_maps_offsets_back_to_the_original() {
        let mut decoder = EntityDecoder::default();
        let original = "a&amp;b&#38;c&#x26;d &lt;x&gt; &bogus; &#1114112; &#233;";
        let decoded = decoder.decode(original);
        assert_eq!(decoded.text, "a&b&c&d <x> &bogus; &#1114112; é");
        assert_eq!(&original[decoded.original_range(0, 3)], "a&amp;b");
        assert_eq!(&original[decoded.original_range(1, 2)], "&amp;");
        assert_eq!(&original[decoded.original_range(2, 5)], "b&#38;c");
        assert_eq!(&original[decoded.original_range(5, 7)], "&#x26;d");
        assert_eq!(&original[decoded.original_range(8, 11)], "&lt;x&gt;");
        assert_eq!(&original[decoded.original_range(12, 19)], "&bogus;");
        let end = decoded.text.len();
        assert_eq!(&original[decoded.original_range(end - 2, end)], "&#233;");
        assert_eq!(decoded.original_range(0, end), 0..original.len());

        let plain = decoder.decode("no entities & here");
        assert_eq!(plain.text, "no entities & here");
        assert_eq!(plain.original_range(3, 8), 3..8);
    }
}
