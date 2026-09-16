//! The DataCite index bundle: manifest types, the lookup contract, and the
//! memory-mapped loader.
//!
//! A bundle is an immutable directory holding four artifacts and a
//! completion marker:
//!
//! - `dois.fst`: an FST set of canonical DOIs (see
//!   [`crate::common::identifiers::normalize_doi`]).
//! - `hosts.fst`: an FST set of the lowercase hosts of every registered
//!   landing page, a cheap pre-filter before full URL lookup.
//! - `urls.fst`: an FST map from canonical landing-page URL (see
//!   [`crate::common::identifiers::normalize_landing_url`]) to a byte
//!   offset in `url-targets.bin`.
//! - `url-targets.bin`: at each offset, a little-endian `u64` candidate
//!   count followed by that many little-endian `u32` byte lengths each
//!   followed by the UTF-8 bytes of one DOI; the candidates of one URL are
//!   sorted and distinct.
//! - `_INDEX_COMPLETE.json`: the [`BundleManifest`], written last. Its
//!   exact bytes define the bundle digest.
//!
//! [`DataciteIndex::load`] verifies the marker and every listed file's size
//! and SHA-256 before mapping anything, so a corrupt or partially written
//! bundle is rejected rather than served. The loaded index is `Sync` and
//! holds only memory maps, so workers share one instance instead of
//! copying the registry.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, ensure, Context, Result};
use fst::{Map, Set};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// `kind` of every DataCite bundle manifest.
pub const BUNDLE_KIND: &str = "datacite-index";
/// Layout version of the bundle files this module reads and writes.
pub const FORMAT_VERSION: u32 = 2;
/// Version of the DOI and URL normalization the keys were built with.
pub const NORMALIZATION_VERSION: u32 = 1;
/// The completion marker and manifest, written last.
pub const MARKER_FILE: &str = "_INDEX_COMPLETE.json";
pub const DOIS_FILE: &str = "dois.fst";
pub const HOSTS_FILE: &str = "hosts.fst";
pub const URLS_FILE: &str = "urls.fst";
pub const URL_TARGETS_FILE: &str = "url-targets.bin";
/// Every bundle file other than the marker, in a fixed order.
pub const BUNDLE_FILES: [&str; 4] = [DOIS_FILE, HOSTS_FILE, URLS_FILE, URL_TARGETS_FILE];

/// Buffer size for hashing bundle files.
const HASH_BUFFER: usize = 1024 * 1024;
/// Bytes of one candidate count in `url-targets.bin`.
const COUNT_BYTES: usize = 8;
/// Bytes of one candidate length in `url-targets.bin`.
const LENGTH_BYTES: usize = 4;

/// Membership and landing-page lookup against a registry of cited targets.
///
/// Keys are canonical: callers normalize DOIs with `normalize_doi`, URLs
/// with `normalize_landing_url` or `url_lookup_keys`, and pass hosts
/// exactly as they appear in a normalized URL (lowercase, no port).
pub trait TargetLookup: Sync {
    /// Whether `doi` is a registered DOI.
    fn contains_doi(&self, doi: &str) -> bool;
    /// Whether any registered landing page lives on `host`.
    fn contains_host(&self, host: &str) -> bool;
    /// The sorted, distinct DOIs registered to the landing page `key`;
    /// empty when the URL is not registered. Fails instead of allocating
    /// more than `max_bytes` for the result.
    fn url_dois(&self, key: &str, max_bytes: usize) -> Result<Vec<String>>;
}

/// Size and lowercase hex SHA-256 of one file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFile {
    pub size: u64,
    pub sha256: String,
}

impl IndexFile {
    /// Measure and hash the file at `path`.
    pub fn of_file(path: &Path) -> Result<Self> {
        let mut file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; HASH_BUFFER];
        let mut size = 0u64;
        loop {
            let read = file
                .read(&mut buffer)
                .with_context(|| format!("failed to read {}", path.display()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            size += read as u64;
        }
        Ok(Self {
            size,
            sha256: hex_digest(&hasher.finalize()),
        })
    }
}

/// The bundle manifest stored as `_INDEX_COMPLETE.json`.
///
/// `files` lists every bundle file except the manifest itself. `counts`
/// holds the build counters: `metadata_records`, `csv_records`, `dois`,
/// `urls`, `hosts`, `url_pairs`, `missing_urls`, `invalid_urls`,
/// `invalid_dois`, and `shared_urls`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleManifest {
    pub kind: String,
    pub format_version: u32,
    pub normalization_version: u32,
    /// The DataCite release month, e.g. `2026-07`.
    pub release: String,
    /// The release's `MonthlyRelease::inventory_sha256`.
    pub source_inventory_sha256: String,
    pub files: BTreeMap<String, IndexFile>,
    pub counts: BTreeMap<String, u64>,
}

impl BundleManifest {
    /// Check the kind and versions this module supports, and that every
    /// required artifact is listed under a plain file name.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.kind == BUNDLE_KIND,
            "manifest kind is {:?}, expected {BUNDLE_KIND:?}",
            self.kind
        );
        ensure!(
            self.format_version == FORMAT_VERSION,
            "manifest format_version is {}, expected {FORMAT_VERSION}",
            self.format_version
        );
        ensure!(
            self.normalization_version == NORMALIZATION_VERSION,
            "manifest normalization_version is {}, expected {NORMALIZATION_VERSION}",
            self.normalization_version
        );
        for name in self.files.keys() {
            ensure!(
                is_plain_file_name(name) && name != MARKER_FILE,
                "manifest lists an invalid bundle file name {name:?}"
            );
        }
        for name in BUNDLE_FILES {
            ensure!(
                self.files.contains_key(name),
                "manifest does not list {name}"
            );
        }
        Ok(())
    }

    /// The exact bytes stored as `_INDEX_COMPLETE.json`.
    pub fn to_marker_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(self).context("failed to serialize manifest")?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

/// A loaded, verified bundle shared read-only across workers.
pub struct DataciteIndex {
    directory: PathBuf,
    manifest: BundleManifest,
    manifest_sha256: String,
    dois: Set<Mmap>,
    hosts: Set<Mmap>,
    urls: Map<Mmap>,
    targets: Mmap,
}

impl DataciteIndex {
    /// Load the bundle at `directory`.
    ///
    /// Reads and validates the marker, then checks that every listed file
    /// exists with the listed size and SHA-256 before memory-mapping the
    /// artifacts. Any mismatch is an error.
    pub fn load(directory: &Path) -> Result<Self> {
        let marker_path = directory.join(MARKER_FILE);
        let marker = fs::read(&marker_path)
            .with_context(|| format!("failed to read {}", marker_path.display()))?;
        let manifest_sha256 = hex_digest(&Sha256::digest(&marker));
        let manifest: BundleManifest = serde_json::from_slice(&marker)
            .with_context(|| format!("malformed {}", marker_path.display()))?;
        manifest
            .validate()
            .with_context(|| format!("{}", marker_path.display()))?;
        for (name, expected) in &manifest.files {
            let path = directory.join(name);
            let actual = IndexFile::of_file(&path)?;
            ensure!(
                actual.size == expected.size,
                "{}: file is {} bytes but the manifest says {}",
                path.display(),
                actual.size,
                expected.size
            );
            ensure!(
                actual.sha256 == expected.sha256,
                "{}: SHA-256 {} does not match the manifest's {}",
                path.display(),
                actual.sha256,
                expected.sha256
            );
        }

        let dois = Set::new(map_file(&directory.join(DOIS_FILE))?)
            .with_context(|| format!("{}: not a valid FST set", DOIS_FILE))?;
        let hosts = Set::new(map_file(&directory.join(HOSTS_FILE))?)
            .with_context(|| format!("{}: not a valid FST set", HOSTS_FILE))?;
        let urls = Map::new(map_file(&directory.join(URLS_FILE))?)
            .with_context(|| format!("{}: not a valid FST map", URLS_FILE))?;
        let targets = map_file(&directory.join(URL_TARGETS_FILE))?;
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest,
            manifest_sha256,
            dois,
            hosts,
            urls,
            targets,
        })
    }

    /// The directory the bundle was loaded from.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn manifest(&self) -> &BundleManifest {
        &self.manifest
    }

    /// The bundle digest: SHA-256 of the exact `_INDEX_COMPLETE.json` bytes.
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }
}

impl TargetLookup for DataciteIndex {
    fn contains_doi(&self, doi: &str) -> bool {
        self.dois.contains(doi.as_bytes())
    }

    fn contains_host(&self, host: &str) -> bool {
        self.hosts.contains(host.as_bytes())
    }

    fn url_dois(&self, key: &str, max_bytes: usize) -> Result<Vec<String>> {
        match self.urls.get(key.as_bytes()) {
            Some(offset) => read_postings(&self.targets, offset, max_bytes)
                .with_context(|| format!("invalid URL posting for {key:?}")),
            None => Ok(Vec::new()),
        }
    }
}

/// Decode the candidate DOIs stored at `offset` in `url-targets.bin`.
///
/// Every offset and length is checked against the file before it is used,
/// and the result's allocation (the `Vec` itself plus every DOI's bytes)
/// is charged against `max_bytes` before it is made.
fn read_postings(bytes: &[u8], offset: u64, max_bytes: usize) -> Result<Vec<String>> {
    let overflow = || anyhow!("URL posting overflow");
    let mut cursor = usize::try_from(offset).map_err(|_| overflow())?;

    let end = cursor.checked_add(COUNT_BYTES).ok_or_else(overflow)?;
    ensure!(end <= bytes.len(), "truncated URL posting");
    let count = u64::from_le_bytes(bytes[cursor..end].try_into()?);
    let count = usize::try_from(count).map_err(|_| overflow())?;
    cursor = end;
    // Each candidate carries at least its length prefix, so a count the
    // file cannot hold is corrupt rather than merely over budget.
    let minimum = count.checked_mul(LENGTH_BYTES).ok_or_else(overflow)?;
    ensure!(
        minimum <= bytes.len() - cursor,
        "URL posting count {count} exceeds the file"
    );
    // The result vector itself is charged before it is allocated.
    let mut used_bytes = count
        .checked_mul(std::mem::size_of::<String>())
        .ok_or_else(|| anyhow!("lookup budget overflow"))?;
    ensure!(
        used_bytes <= max_bytes,
        "URL candidate memory budget exceeded"
    );
    let mut dois = Vec::with_capacity(count);

    for _ in 0..count {
        let end = cursor.checked_add(LENGTH_BYTES).ok_or_else(overflow)?;
        ensure!(end <= bytes.len(), "truncated URL posting");
        let length = u32::from_le_bytes(bytes[cursor..end].try_into()?) as usize;
        cursor = end;

        let end = cursor.checked_add(length).ok_or_else(overflow)?;
        ensure!(end <= bytes.len(), "truncated URL posting");
        used_bytes = used_bytes
            .checked_add(length)
            .ok_or_else(|| anyhow!("lookup budget overflow"))?;
        ensure!(
            used_bytes <= max_bytes,
            "URL candidate memory budget exceeded"
        );
        let doi = std::str::from_utf8(&bytes[cursor..end])?;
        dois.push(doi.to_string());
        cursor = end;
    }
    Ok(dois)
}

/// Memory-map a bundle file read-only.
fn map_file(path: &Path) -> Result<Mmap> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    // SAFETY: bundle files are immutable once their marker exists; the
    // map is read-only and its digest was verified above.
    unsafe { Mmap::map(&file) }.with_context(|| format!("failed to map {}", path.display()))
}

/// A single path component that names a regular file.
fn is_plain_file_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

/// Lowercase hex of a digest.
fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}

impl std::fmt::Debug for DataciteIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataciteIndex")
            .field("directory", &self.directory)
            .field("release", &self.manifest.release)
            .field("manifest_sha256", &self.manifest_sha256)
            .finish_non_exhaustive()
    }
}
